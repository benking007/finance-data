from contextlib import closing
import json
import unittest
from unittest.mock import patch

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import TushareClient, run_ingestion
from financedb.storage import Store


FIRST = "20261116"
SECOND = "20261117"
THIRD = "20261118"


class GatewayRetryTest(unittest.TestCase):
    def collect(self, store, profile, respond, *, end=THIRD, codes=None):
        dataset_id = profile["id"]
        with patch("financedb.ingest._wait"), TushareClient(
            "fixture", workers=1, transport=httpx.MockTransport(respond),
        ) as client:
            result = run_ingestion(store, {dataset_id: profile}, [dataset_id], client=client, mode="history",
                                   today="20261128", start=FIRST, end=end, codes=codes)
        return result["datasets"][0]

    def test_one_gateway_failure_leaves_only_failed_partition_for_resume(self):
        profiles = load_profiles()
        for dataset_id in ("factor_value", "etf_sh_cons"):
            with self.subTest(dataset_id=dataset_id), closing(Store("sqlite://")) as store:
                profile = profiles[dataset_id]
                is_factor = dataset_id == "factor_value"
                if is_factor:
                    profile = profile | {"request": profile["request"] | {"window_days": 1}}
                calls = []
                failing = {SECOND}

                def respond(request):
                    payload = json.loads(request.content)
                    params = payload["params"]
                    stamp = params["start_date"] if is_factor else params["trade_date"]
                    calls.append(stamp)
                    self.assertEqual(payload["api_name"], dataset_id)
                    if stamp in failing:
                        return httpx.Response(502)
                    row = {"ts_code": "600000.SH", "factor_name": "MACD", "factor_value": "1.2",
                           "trade_date": stamp} if is_factor else {
                        "ts_code": "517030.SH", "con_code": "600000.SH", "trade_date": stamp, "qty": 10,
                    }
                    fields = payload["fields"].split(",")
                    return httpx.Response(200, json={"code": 0, "data": {
                        "fields": fields, "items": [[row.get(field) for field in fields]],
                    }})

                codes = ["600000.SH"] if is_factor else None
                report = self.collect(store, profile, respond, codes=codes)
                self.assertEqual((report["status"], report["completed"], report["failed"]), ("partial", 2, 1))
                self.assertEqual(calls, [FIRST, SECOND, SECOND, SECOND, THIRD])
                failed, = [item for item in store.list_partitions(dataset_id) if item["status"] == "failed"]
                self.assertEqual(failed["message"], "Tushare HTTP 502")

                calls.clear()
                failing.clear()
                report = self.collect(store, profile, respond, codes=codes)
                self.assertEqual((report["status"], report["skipped"], report["failed"]), ("complete", 2, 0))
                self.assertEqual(calls, [SECOND])
                self.assertTrue(store.partition_done(dataset_id, failed["partition_id"]))
                self.assertEqual(store.row_count(dataset_id), 3)

    def test_consecutive_gateway_failures_stop_the_dataset(self):
        profile = load_profiles()["etf_sh_cons"]
        calls = []

        def respond(request):
            calls.append(json.loads(request.content)["params"]["trade_date"])
            return httpx.Response(504)

        with closing(Store("sqlite://")) as store:
            report = self.collect(store, profile, respond, end="20261120")
        self.assertEqual((report["status"], report["failed"]), ("failed", 3))
        self.assertEqual(calls, [FIRST] * 3 + [SECOND] * 3 + [THIRD] * 3)

    def test_retry_after_still_stops_other_requests_for_the_dataset(self):
        profile = load_profiles()["etf_sh_cons"]
        calls = []

        def respond(request):
            calls.append(json.loads(request.content)["params"]["trade_date"])
            return httpx.Response(503, headers={"Retry-After": "300"})

        with closing(Store("sqlite://")) as store:
            report = self.collect(store, profile, respond)
        self.assertEqual((report["status"], report["failed"]), ("failed", 1))
        self.assertEqual(calls, [FIRST])
