import json
import unittest

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import TushareClient, run_ingestion
from financedb.storage import Store


DAY = "20261124"
NAV = {"ts_code": "000001.OF", "nav_date": DAY, "unit_nav": "1.02"}


class FundNavTest(unittest.TestCase):
    def setUp(self):
        self.profile = load_profiles()["fund_nav"]
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.response_fields = list(self.profile["fields"])
        self.rows = []
        self.calls = []
        self.client = TushareClient("fixture", workers=1, transport=httpx.MockTransport(self.respond))
        self.addCleanup(self.client.close)

    def respond(self, request):
        payload = json.loads(request.content)
        self.assertEqual(payload["api_name"], "fund_nav")
        self.assertIn("ann_date", payload["fields"].split(","))
        params = payload["params"]
        self.assertEqual(params["nav_date"], DAY)
        self.calls.append(params["market"])
        rows = self.rows if params["market"] == "O" else []
        fields = self.response_fields
        return httpx.Response(200, json={"code": 0, "data": {
            "fields": fields, "items": [[row.get(field) for field in fields] for row in rows],
        }})

    def collect(self, profile=None, *, mode="history", today="20261127"):
        result = run_ingestion(self.store, {"fund_nav": profile or self.profile}, ["fund_nav"],
                               client=self.client, mode=mode, start=DAY, end=DAY, today=today)
        return result["datasets"][0]

    def checkpoint(self):
        return next(part for part in self.store.list_partitions("fund_nav") if part["params"]["market"] == "O")

    def test_recovers_failed_partition_and_preserves_announcement_dates(self):
        self.rows = [NAV | {"ann_date": "20261125"}, NAV | {"ann_date": "20261126"},
                     NAV, NAV | {"ann_date": ""}, NAV | {"ann_date": " \t"}, NAV | {"unit_nav": "1.03"}]
        legacy = {name: value for name, value in self.profile.items() if name != "nullable_keys"}
        report = self.collect(legacy)
        self.assertEqual(report["message"], "来源记录缺少业务键 ann_date")
        failed = self.checkpoint()
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(self.store.row_count("fund_nav"), 0)

        self.calls.clear()
        report = self.collect()
        self.assertEqual((report["status"], report["completed"], report["skipped"]), ("complete", 1, 1))
        self.assertEqual(self.calls, ["O"])
        self.assertEqual(self.checkpoint()["partition_id"], failed["partition_id"])
        self.assertEqual(self.checkpoint()["status"], "complete")
        self.assertEqual(self.store.row_count("fund_nav"), 4)

        cursor = None
        result = []
        for _ in range(4):
            page = self.store.query_rows(self.profile, code=NAV["ts_code"], start=DAY, end=DAY,
                                         cursor=cursor, limit=1)
            result.extend(page["rows"])
            cursor = page["next_cursor"]
        self.assertIsNone(cursor)
        self.assertCountEqual(result, [self.rows[0], self.rows[1], NAV, NAV | {"unit_nav": "1.03"}])

    def test_omitted_announcement_column_also_deduplicates_on_refresh(self):
        self.rows = [NAV | {"ann_date": None}]
        self.assertEqual(self.collect()["status"], "complete")
        self.response_fields.remove("ann_date")
        self.assertEqual(self.collect(mode="incremental")["status"], "complete")
        self.assertEqual(self.store.query_rows(self.profile)["rows"], [NAV])
        self.assertEqual(self.store.row_count("fund_nav"), 1)

    def test_fund_code_and_nav_date_remain_required(self):
        for field in ("ts_code", "nav_date"):
            with self.subTest(field=field):
                self.rows = [NAV | {field: None}]
                report = self.collect()
                self.assertEqual(report["message"], f"来源记录缺少业务键 {field}")
                self.assertEqual(self.checkpoint()["status"], "failed")
                self.assertEqual(self.store.row_count("fund_nav"), 0)
