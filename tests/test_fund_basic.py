import unittest

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import Store


class FundClient:
    def __init__(self, rows, *, pagination):
        self.rows = rows
        self.pagination = pagination
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        self.calls.append((profile["id"], params.copy()))
        records = self.rows.get((params.get("market"), params.get("status")), [])
        offset = params.get("offset", 0) if self.pagination else 0
        return records[offset:offset + profile["row_limit"]]


class FundBasicTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profiles = load_profiles()
        self.profile = self.profiles["fund_basic"] | {"row_limit": 4}
        self.profiles["fund_basic"] = self.profile
        self.rows = {
            (market, status): [
                {"ts_code": f"{market}{status}{idx}.OF", "market": market, "status": status}
                for idx in range(7 if (market, status) == ("O", "L") else 1)
            ]
            for market in ("E", "O") for status in ("D", "I", "L")
        }

    def ingest(self, client, *datasets):
        return run_ingestion(self.store, self.profiles, datasets or ["fund_basic"], client=client,
                             mode="history", today="2026-11-26")

    def test_capped_partition_can_continue_from_five_completed_slices(self):
        capped = FundClient(self.rows, pagination=False)
        before = self.ingest(capped)["datasets"][0]
        self.assertEqual(before["status"], "partial")
        self.assertEqual(before["completed"], 5)
        self.assertEqual(self.store.row_count("fund_basic"), 9)

        complete = FundClient(self.rows, pagination=True)
        after = self.ingest(complete)["datasets"][0]
        self.assertEqual(after["status"], "complete")
        self.assertEqual(after["skipped"], 5)
        self.assertEqual(self.store.row_count("fund_basic"), 12)
        self.assertEqual({(call["market"], call["status"]) for _, call in complete.calls}, {("O", "L")})

    def test_partial_master_does_not_authorize_complete_fund_universe(self):
        client = FundClient(self.rows, pagination=False)
        report = self.ingest(client, "fund_basic", "fund_portfolio")
        results = {item["dataset_id"]: item for item in report["datasets"]}
        self.assertEqual(results["fund_basic"]["status"], "partial")
        self.assertEqual(results["fund_portfolio"]["status"], "failed")
        self.assertIn("fund_basic", results["fund_portfolio"]["message"])
        self.assertFalse(any(dataset == "fund_portfolio" for dataset, _ in client.calls))

    def test_rows_from_another_market_or_status_are_rejected(self):
        self.rows[("O", "L")] = [{"ts_code": "wrong.OF", "market": "E", "status": "L"}]
        report = self.ingest(FundClient(self.rows, pagination=True))
        self.assertEqual(report["status"], "partial")
        self.assertEqual(self.store.row_count("fund_basic"), 5)
        self.assertTrue(all(row["ts_code"] != "wrong.OF" for row in self.store.query_rows(self.profile)["rows"]))

    def test_benchmark_splits_capped_level_using_official_benchmark_types(self):
        profile = self.profiles["mkt_idx_bmk"] | {"row_limit": 4}
        self.profiles["mkt_idx_bmk"] = profile
        records = [{"ts_code": f"{idx}.CSI", "bmk_level": "一类库", "bmk_type": bmk_type}
                   for idx, bmk_type in enumerate(["策略", "宽基", "行业主题"] * 2)]
        records.append({"ts_code": "7.CSI", "bmk_level": "二类库", "bmk_type": "宽基"})

        class BenchmarkClient:
            def __init__(self):
                self.calls = []

            def query(self, profile, params, *, cancelled=None):
                self.calls.append(params.copy())
                return [row for row in records if all(row.get(field) == value for field, value in params.items())][:4]

        client = BenchmarkClient()
        result = self.ingest(client, "mkt_idx_bmk")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(self.store.row_count("mkt_idx_bmk"), len(records))
        self.assertEqual({params["bmk_type"] for params in client.calls if "bmk_type" in params},
                         {"策略", "宽基", "行业主题"})

    def test_benchmark_empty_child_does_not_certify_capped_parent(self):
        profile = self.profiles["mkt_idx_bmk"] | {"row_limit": 4}
        self.profiles["mkt_idx_bmk"] = profile
        records = [{"ts_code": f"{idx}.CSI", "bmk_level": "一类库", "bmk_type": bmk_type}
                   for idx, bmk_type in enumerate(["策略", "宽基", "行业主题", "策略"])]

        class BenchmarkClient:
            def query(self, profile, params, *, cancelled=None):
                if params.get("bmk_type") == "行业主题":
                    return []
                return [row for row in records if all(row.get(field) == value for field, value in params.items())]

        report = self.ingest(BenchmarkClient(), "mkt_idx_bmk")
        self.assertEqual(report["status"], "partial")
        parent = next(part for part in self.store.list_partitions("mkt_idx_bmk") if part["params"] == {"bmk_level": "一类库"})
        self.assertNotIn(parent["status"], {"complete", "empty"})


if __name__ == "__main__":
    unittest.main()
