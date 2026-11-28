import json
import unittest

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import Partition, TushareClient, _partition_id, run_ingestion
from financedb.storage import Store


class ConvertibleBondPriceChangeTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        profiles = load_profiles()
        self.profiles = {name: profiles[name] for name in ("cb_basic", "cb_price_chg")}
        self.directory = [
            {"ts_code": "110059.SH", "exchange": "SSE", "list_date": "20191021"},
            {"ts_code": "123001.SZ", "exchange": "SZSE", "list_date": "20171228", "delist_date": "20260921"},
        ]
        self.calls = []
        self.client = TushareClient("fixture", workers=1, transport=httpx.MockTransport(self.respond))
        self.addCleanup(self.client.close)

    def respond(self, request):
        payload = json.loads(request.content)
        dataset, params = payload["api_name"], payload["params"]
        self.calls.append((dataset, params))
        fields = payload["fields"].split(",")
        if dataset == "cb_basic":
            fields.remove("maturity_put_price")
            rows = [] if "exchange" in params else self.directory
            if "limit" in params:
                rows = rows[:params["limit"]]
        else:
            self.assertEqual(dataset, "cb_price_chg")
            rows = [{"ts_code": params["ts_code"], "change_date": "20260901", "convertprice_aft": "10.12"}]
        return httpx.Response(200, json={"code": 0, "data": {
            "fields": fields, "items": [[row.get(field) for field in fields] for row in rows],
        }})

    def collect(self, profiles=None):
        report = run_ingestion(self.store, profiles or self.profiles, ["cb_price_chg", "cb_basic"],
                               client=self.client, mode="history", recent_history=True, today="20261127")
        return {item["dataset_id"]: item for item in report["datasets"]}

    def test_unfiltered_directory_replaces_empty_exchange_checkpoints_and_resumes(self):
        previous = self.profiles["cb_basic"] | {"request": {
            "strategy": "snapshot", "dimensions": {"exchange": ["SSE", "SZSE"]},
        }}
        report = self.collect(self.profiles | {"cb_basic": previous})
        self.assertEqual(report["cb_basic"]["status"], "complete")
        self.assertEqual(report["cb_price_chg"]["message"], "缺少标的范围，请先同步主数据或明确指定标的")
        self.assertEqual(self.calls, [("cb_basic", {"exchange": "SSE"}), ("cb_basic", {"exchange": "SZSE"})])

        self.calls.clear()
        report = self.collect()
        self.assertEqual((report["cb_basic"]["status"], report["cb_price_chg"]["status"]), ("complete", "complete"))
        self.assertEqual(self.calls, [("cb_basic", {}), ("cb_price_chg", {"ts_code": "110059.SH"}),
                                      ("cb_price_chg", {"ts_code": "123001.SZ"})])
        self.assertEqual(self.store.row_count("cb_price_chg"), 2)

        self.calls.clear()
        report = self.collect()
        self.assertEqual((report["cb_basic"]["skipped"], report["cb_price_chg"]["skipped"]), (1, 2))
        self.assertEqual(self.calls, [])

    def test_empty_directory_is_retried_on_the_same_day(self):
        directory, self.directory = self.directory, []
        report = self.collect()
        self.assertIn("空名录", report["cb_basic"]["message"])
        self.assertEqual(report["cb_price_chg"]["status"], "failed")
        self.assertEqual(self.calls, [("cb_basic", {})])
        failed, = self.store.list_partitions("cb_basic")
        self.assertEqual(failed["status"], "failed")

        self.directory = directory
        self.calls.clear()
        report = self.collect()
        recovered, = self.store.list_partitions("cb_basic")
        self.assertEqual(recovered["partition_id"], failed["partition_id"])
        self.assertEqual(recovered["status"], "complete")
        self.assertEqual(report["cb_price_chg"]["rows"], 2)

    def test_complete_checkpoint_without_stored_codes_refreshes_the_directory(self):
        directory, self.directory = self.directory, []
        self.store.initialize()
        basic = self.profiles["cb_basic"]
        partition_id = _partition_id(basic, Partition({}), "snapshot:2026-11-27")
        self.store.commit_partition(basic, partition_id, {}, [], source_count=len(directory))
        self.assertTrue(self.store.partition_done("cb_basic", partition_id, row_limit=basic["row_limit"]))

        report = self.collect()
        self.assertEqual(self.calls, [("cb_basic", {})])
        self.assertEqual(report["cb_basic"]["status"], "failed")
        self.assertIn("缺少本日完整标的名录，请先成功同步 cb_basic", report["cb_price_chg"]["message"])
        self.assertFalse(self.store.partition_done("cb_basic", partition_id, row_limit=basic["row_limit"]))

        self.directory = directory
        self.calls.clear()
        report = self.collect()
        self.assertEqual((report["cb_basic"]["status"], report["cb_price_chg"]["rows"]), ("complete", 2))
        self.assertEqual(self.calls[0], ("cb_basic", {}))

        self.calls.clear()
        report = self.collect()
        self.assertEqual((report["cb_basic"]["skipped"], report["cb_price_chg"]["skipped"]), (1, 2))
        self.assertEqual(self.calls, [])

    def test_unverified_capped_directory_does_not_authorize_downstream(self):
        capped = self.profiles["cb_basic"] | {"row_limit": 2}
        report = self.collect(self.profiles | {"cb_basic": capped})
        self.assertEqual(report["cb_basic"]["status"], "partial")
        self.assertEqual(report["cb_price_chg"]["status"], "failed")
        self.assertIn("cb_basic", report["cb_price_chg"]["message"])
        self.assertEqual(self.calls, [("cb_basic", {}), ("cb_basic", {"offset": 1, "limit": 2}),
                                      ("cb_basic", {"offset": 2, "limit": 2})])
        self.assertEqual(self.store.row_count("cb_basic"), 2)
        self.assertEqual(self.store.row_count("cb_price_chg"), 0)

    def test_partial_refresh_does_not_reuse_a_checkpoint_that_lost_its_catalog(self):
        basic = self.profiles["cb_basic"] | {"row_limit": 2}
        self.store.initialize()
        partition_id = _partition_id(basic, Partition({}), "snapshot:2026-11-27")
        self.store.commit_partition(basic, partition_id, {}, [], source_count=1)
        report = self.collect(self.profiles | {"cb_basic": basic})
        self.assertEqual((report["cb_basic"]["status"], report["cb_price_chg"]["status"]), ("partial", "failed"))
        self.assertFalse(self.store.partition_done("cb_basic", partition_id, row_limit=basic["row_limit"]))
        self.assertEqual(self.store.row_count("cb_basic"), 2)
        self.assertEqual(self.store.row_count("cb_price_chg"), 0)
