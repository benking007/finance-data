import unittest

from financedb.catalog import load_profiles
from financedb.ingest import Partition, RequestError, _partition_id, run_ingestion
from financedb.storage import PAGINATION_VERIFIED_MESSAGE, Store


DAY = "2025-11-25"
STAMP = DAY.replace("-", "")


def factor(code, exchange="NYS", date=STAMP):
    return {"ts_code": code, "trade_date": date, "exchange": exchange,
            "cum_adjfactor": "1.25", "close_price": "10"}


class FactorClient:
    def __init__(self, rows, *, offset_base=None, reject_paging=False, unavailable_etf=False):
        self.rows = rows
        self.offset_base = offset_base
        self.reject_paging = reject_paging
        self.unavailable_etf = unavailable_etf
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        dataset_id = profile["id"]
        self.calls.append((dataset_id, params.copy()))
        if dataset_id == "us_basic":
            return [{"ts_code": code} for code in ("AAPL", "UNSEEN_STOCK")]
        if dataset_id == "us_etf_basic":
            if self.unavailable_etf:
                raise RequestError("ETF 名录权限不可用", status="blocked", stop_dataset=True)
            if params == {"exchange": "NYSEARCA", "list_status": "L"}:
                return [{"ts_code": code, "exchange": "NYSEARCA", "list_status": "L", "list_date": "20190101"}
                        for code in ("AAAU", "UNSEEN_ETF")]
            return []
        if "offset" in params and self.reject_paging:
            raise RequestError("未知参数 offset", stop_dataset=True)
        rows = [row for row in self.rows if row["trade_date"] == params["trade_date"]
                and ("ts_code" not in params or row["ts_code"] == params["ts_code"])]
        offset = 0 if self.offset_base is None or "offset" not in params else max(0, params["offset"] - self.offset_base)
        return rows[offset:offset + profile["row_limit"]]

    def factor_calls(self):
        return [params for dataset, params in self.calls if dataset == "us_adjfactor"]


class USAdjustmentFactorTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        registry = load_profiles()
        self.profiles = {name: registry[name] for name in ("us_basic", "us_etf_basic", "us_adjfactor")}
        self.profile = self.profiles["us_adjfactor"] = self.profiles["us_adjfactor"] | {"row_limit": 4}
        self.rows = [
            factor("AAPL", "NAS"), factor("AAAU", "ARC"), factor("AAC.U!AA", "OTC"), factor("AAC.WS", "NYS"),
            factor("AAPL", "OTC"), factor("UNSEEN_STOCK"), factor("UNSEEN_ETF", "ARC"), factor("OUTSIDE_OTC", "OTC"),
        ]

    def ingest(self, client, *, targets=None, profiles=None, start=DAY):
        result = run_ingestion(self.store, profiles or self.profiles, targets or self.profiles, client=client,
                               mode="history", start=start, end=DAY, today="2026-11-27")
        return next(report for report in result["datasets"] if report["dataset_id"] == "us_adjfactor")

    def parent(self):
        return next(item for item in self.store.list_partitions("us_adjfactor") if item["params"] == {"trade_date": STAMP})

    def stored_keys(self):
        return {(row["ts_code"], row["trade_date"], row["exchange"])
                for row in self.store.query_rows(self.profile)["rows"]}

    def test_verified_paging_repairs_original_failed_checkpoint_without_rescanning_complete_day(self):
        prior = factor("AAPL", "NAS", "20251124")
        client = FactorClient([prior, *self.rows], offset_base=1)
        old_request = {name: value for name, value in self.profile["request"].items() if name not in {
            "cap_candidate_code_sources", "verify_capped_pagination", "capped_pagination_offsets", "retain_capped_rows",
        }}
        old_profile = self.profile | {"request": old_request | {"cap_code_source": "us_basic"}}
        first = self.ingest(client, profiles=self.profiles | {"us_adjfactor": old_profile}, start="2025-11-24")
        self.assertIn("名录外", first["message"])
        self.assertEqual(self.parent()["status"], "failed")
        self.assertEqual(self.store.row_count("us_adjfactor"), 1)

        client.calls.clear()
        second = self.ingest(client, start="2025-11-24")
        self.assertEqual(second["status"], "complete", second)
        self.assertEqual(second["skipped"], 1)
        self.assertEqual(self.parent()["message"], PAGINATION_VERIFIED_MESSAGE)
        self.assertEqual(self.store.row_count("us_adjfactor"), len(self.rows) + 1)
        calls = client.factor_calls()
        self.assertTrue(any("offset" in params for params in calls))
        self.assertTrue(all(params["trade_date"] == STAMP and "ts_code" not in params for params in calls))

    def test_ignored_paging_saves_sample_and_recovers_missing_candidates_without_certifying_day(self):
        client = FactorClient(self.rows)
        report = self.ingest(client)
        self.assertEqual(report["status"], "partial", report)
        self.assertEqual(self.parent()["status"], "partial")
        self.assertEqual(self.parent()["source_count"], 4)
        self.assertIn("名录外", self.parent()["message"])
        recovered = [*self.rows[:4], self.rows[5], self.rows[6]]
        self.assertEqual(self.stored_keys(), {(row["ts_code"], STAMP, row["exchange"]) for row in recovered})
        code_calls = [params["ts_code"] for params in client.factor_calls() if "ts_code" in params]
        self.assertCountEqual(code_calls, ["UNSEEN_STOCK", "UNSEEN_ETF"])

        client.calls.clear()
        self.assertEqual(self.ingest(client)["status"], "partial")
        self.assertFalse(any("ts_code" in params for params in client.factor_calls()))
        self.assertEqual(self.store.row_count("us_adjfactor"), 6)

    def test_paging_rejection_and_unavailable_etf_directory_still_recover_known_stock_and_capped_rows(self):
        client = FactorClient(self.rows, reject_paging=True, unavailable_etf=True)
        report = self.ingest(client)
        self.assertEqual(report["status"], "partial", report)
        self.assertIn("尚无完整名录", self.parent()["message"])
        self.assertIn("us_etf_basic", self.parent()["message"])
        self.assertIn(("AAAU", STAMP, "ARC"), self.stored_keys())
        self.assertIn(("UNSEEN_STOCK", STAMP, "NYS"), self.stored_keys())
        self.assertIn(("AAC.U!AA", STAMP, "OTC"), self.stored_keys())
        self.assertNotIn(("UNSEEN_ETF", STAMP, "ARC"), self.stored_keys())

    def test_one_page_below_cap_needs_no_directory_or_extra_requests(self):
        client = FactorClient(self.rows[:2])
        report = self.ingest(client, targets=["us_adjfactor"])
        self.assertEqual(report["status"], "complete", report)
        self.assertEqual(client.calls, [("us_adjfactor", {"trade_date": STAMP})])

    def test_no_candidates_still_saves_the_sample_and_reports_the_gap(self):
        client = FactorClient(self.rows, reject_paging=True)
        report = self.ingest(client, targets=["us_adjfactor"])
        self.assertEqual(report["status"], "partial", report)
        self.assertEqual(self.parent()["source_count"], 4)
        self.assertIn("尚无完整名录", self.parent()["message"])
        self.assertIn("名录外", self.parent()["message"])
        self.assertFalse(any("ts_code" in params for params in client.factor_calls()))

    def test_rechecks_old_split_parent_and_explicit_ticker_can_recover_another_listing(self):
        self.store.initialize()
        partition_id = _partition_id(self.profile, Partition({"trade_date": STAMP}), "history")
        self.store.commit_partition(self.profile, partition_id, {"trade_date": STAMP}, [], source_count=0)

        client = FactorClient(self.rows)
        self.assertEqual(self.ingest(client)["status"], "partial")
        self.assertEqual(self.parent()["partition_id"], partition_id)
        self.assertEqual(self.parent()["status"], "partial")
        self.assertNotIn(("AAPL", STAMP, "OTC"), self.stored_keys())

        checked = run_ingestion(self.store, self.profiles, ["us_adjfactor"], client=client, codes=["AAPL"],
                                mode="history", start=DAY, end=DAY, today="2026-11-27")
        self.assertEqual(checked["status"], "complete", checked)
        self.assertIn(("AAPL", STAMP, "OTC"), self.stored_keys())
        self.assertEqual(self.parent()["status"], "partial")
