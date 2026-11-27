import json
import unittest

import httpx
from sqlalchemy import select

from financedb.business_tables import business_tables
from financedb.catalog import load_profiles
from financedb.ingest import TushareClient, run_ingestion
from financedb.storage import Store


class ETFAuctionResponseTest(unittest.TestCase):
    def setUp(self):
        self.profile = load_profiles()["etf_auction"]
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.code = "180607.SZ"
        self.day = "20260911"
        self.rows = [{
            "ts_code": self.code, "trade_date": self.day, "vol": 700, "price": 5.05,
            "amount": 3535, "pre_close": 5.05, "turnover_rate": 0.02,
            "volume_ratio": 0.251875, "float_share": 12345.67,
        }]
        self.requested_fields = []
        self.response_fields = list(self.profile["fields"])

        def respond(request):
            payload = json.loads(request.content)
            self.assertEqual(payload["api_name"], "etf_auction")
            self.assertEqual(payload["params"], {"trade_date": self.day})
            self.requested_fields.append(payload["fields"].split(","))
            fields = self.response_fields
            return httpx.Response(200, json={"code": 0, "data": {
                "fields": fields, "items": [[row[field] for field in fields] for row in self.rows],
            }})

        self.client = TushareClient("fixture", workers=1, transport=httpx.MockTransport(respond))
        self.addCleanup(self.client.close)

    def ingest(self, profile=None, **options):
        arguments = {"mode": "history", "today": "2026-11-27"} | options
        return run_ingestion(self.store, {"etf_auction": self.profile if profile is None else profile},
                             ["etf_auction"], client=self.client, start=self.day, end=self.day, **arguments)

    def stored_row(self):
        with self.store.engine.connect() as conn:
            return dict(conn.execute(select(business_tables["etf_auction"])).mappings().one())

    def test_failed_partition_resumes_when_two_optional_columns_are_absent(self):
        missing = {"float_share", "turnover_rate"}
        legacy = self.profile | {"request": {k: v for k, v in self.profile["request"].items()
                                            if k != "optional_response_fields"}}
        self.response_fields = [field for field in self.response_fields if field not in missing]
        report = self.ingest(legacy)["datasets"][0]
        self.assertEqual(report["status"], "failed")
        self.assertIn("float_share, turnover_rate", report["message"])
        before, = self.store.list_partitions("etf_auction")
        self.assertEqual(before["status"], "failed")

        report = self.ingest()["datasets"][0]
        self.assertEqual((report["status"], report["completed"], report["rows"]), ("complete", 1, 1))
        after, = self.store.list_partitions("etf_auction")
        self.assertEqual(after["partition_id"], before["partition_id"])
        self.assertEqual(after["status"], "complete")
        self.assertEqual(self.requested_fields, [self.profile["fields"]] * 2)
        row = self.stored_row()
        self.assertEqual((row["vol"], row["price"], row["amount"], row["volume_ratio"]), (700, "5.05", "3535", "0.251875"))
        self.assertIsNone(row["float_share"])
        self.assertIsNone(row["turnover_rate"])

    def test_refresh_preserves_an_omitted_column_and_applies_an_explicit_null(self):
        self.assertEqual(self.ingest()["status"], "complete")
        self.assertEqual(self.stored_row()["turnover_rate"], "0.02")
        self.response_fields.remove("float_share")
        self.rows[0].update(price=5.08, turnover_rate=None)

        self.assertEqual(self.ingest(mode="incremental")["status"], "complete")
        row = self.stored_row()
        self.assertEqual(row["price"], "5.08")
        self.assertEqual(row["float_share"], "12345.67")
        self.assertIsNone(row["turnover_rate"])

    def test_required_columns_still_reject_a_truncated_response(self):
        self.response_fields = [field for field in self.response_fields if field not in {"amount", "float_share"}]
        report = self.ingest()["datasets"][0]
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["message"], "Tushare 响应缺少已请求字段 amount")
        self.assertEqual(self.store.row_count("etf_auction"), 0)
