import json
import unittest

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import TushareClient, run_ingestion
from financedb.storage import Store


DAY = "20260630"
CODE = "00700.HK"
MISSING_TYPE_CODE = "00005.HK"
INDICATOR = {"ts_code": CODE, "end_date": DAY, "report_type": "Q2", "basic_eps": "2.5"}


class HKFinancialIndicatorTest(unittest.TestCase):
    def setUp(self):
        self.profile = load_profiles()["hk_fina_indicator"]
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.rows = {CODE: [INDICATOR | {"ind_type": kind} for kind in ("Q", "Y")]}
        self.omitted = {}
        self.calls = []
        self.client = TushareClient("fixture", workers=1, transport=httpx.MockTransport(self.respond))
        self.addCleanup(self.client.close)

    def respond(self, request):
        payload = json.loads(request.content)
        self.assertEqual(payload["api_name"], "hk_fina_indicator")
        code = payload["params"]["ts_code"]
        self.calls.append(code)
        fields = payload["fields"].split(",")
        self.assertIn("ind_type", fields)
        fields = [name for name in fields if name not in self.omitted.get(code, ())]
        return httpx.Response(200, json={"code": 0, "data": {
            "fields": fields, "items": [[row.get(name) for name in fields] for row in self.rows[code]],
        }})

    def collect(self, profile=None, *, mode="history"):
        profiles = {"hk_fina_indicator": profile or self.profile}
        report = run_ingestion(self.store, profiles, profiles, client=self.client, mode=mode,
                               start=DAY, end=DAY, today="20261128", codes=list(self.rows))
        return report["datasets"][0]

    def checkpoint(self, code):
        return next(item for item in self.store.list_partitions("hk_fina_indicator")
                    if item["params"]["ts_code"] == code)

    def test_omitted_indicator_type_resumes_the_failed_checkpoint(self):
        self.rows[MISSING_TYPE_CODE] = [INDICATOR | {"ts_code": MISSING_TYPE_CODE}]
        self.omitted[MISSING_TYPE_CODE] = {"ind_type"}
        legacy = self.profile | {"request": {name: value for name, value in self.profile["request"].items()
                                             if name != "optional_response_fields"}}
        report = self.collect(legacy)
        self.assertEqual(report["message"], "Tushare 响应缺少已请求字段 ind_type")
        failed = self.checkpoint(MISSING_TYPE_CODE)
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(self.checkpoint(CODE)["status"], "complete")

        self.calls.clear()
        report = self.collect()
        self.assertEqual((report["status"], report["completed"], report["skipped"]), ("complete", 1, 1))
        self.assertEqual(self.calls, [MISSING_TYPE_CODE])
        recovered = self.checkpoint(MISSING_TYPE_CODE)
        self.assertEqual(recovered["partition_id"], failed["partition_id"])
        self.assertEqual(recovered["status"], "complete")
        self.assertCountEqual(self.store.query_rows(self.profile)["rows"],
                              self.rows[CODE] + self.rows[MISSING_TYPE_CODE])

    def test_blank_indicator_type_deduplicates_without_overwriting_known_types(self):
        self.rows[CODE] += [INDICATOR | {"ind_type": value} for value in (None, "", " \t")]
        self.assertEqual(self.collect()["status"], "complete")
        self.assertEqual(self.collect(mode="incremental")["status"], "complete")
        self.assertCountEqual(self.store.query_rows(self.profile)["rows"], self.rows[CODE][:2] + [INDICATOR])

    def test_report_period_type_remains_required(self):
        self.omitted[CODE] = {"ind_type", "report_type"}
        report = self.collect()
        self.assertEqual(report["message"], "Tushare 响应缺少已请求字段 report_type")
        self.assertEqual(self.checkpoint(CODE)["status"], "failed")
        self.assertEqual(self.store.row_count("hk_fina_indicator"), 0)
