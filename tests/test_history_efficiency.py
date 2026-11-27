from datetime import date
import unittest

from financedb.catalog import load_profiles
from financedb.ingest import plan_partitions, run_ingestion
from financedb.storage import Store


class HistoryEfficiencyTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.store.initialize()
        self.profiles = load_profiles()
        profile = self.profiles["block_trade"]
        self.profile = profile | {"request": profile["request"] | {"history_empty_probe": {
            "days": 3, "control_start": "2025-01-07", "control_end": "2025-01-08",
        }}}

    def plan(self, profile, **kwargs):
        return list(plan_partitions(self.store, profile, mode="history", today=date(2025, 1, 15), **kwargs))

    def run_history(self, client, profile=None):
        profile = profile or self.profile
        return run_ingestion(self.store, {"block_trade": profile}, ["block_trade"], client=client,
                             mode="history", today="20250115", start="20250101", end="20250106")

    def test_partial_calendar_excludes_only_confirmed_closures_in_the_correct_market(self):
        for dataset_id, fields in [("trade_cal", {"exchange": "SSE"}), ("hk_tradecal", {})]:
            self.store.commit_partition(self.profiles[dataset_id], dataset_id, {}, [
                fields | {"cal_date": "20250102", "is_open": "0"}, fields | {"cal_date": "20250103", "is_open": "1"},
            ])
        hk = self.profiles["hk_adjfactor"]
        hk = hk | {"request": hk["request"] | {"trading_days": True, "calendar_dataset": "hk_tradecal"}}
        for profile in (self.profile, hk):
            partitions = self.plan(profile, start=date(2025, 1, 1), end=date(2025, 1, 4), allow_range_probes=False)
            self.assertEqual([p.params["trade_date"] for p in partitions], ["20250101", "20250103", "20250104"])

    def test_empty_ranges_are_saved_and_ranges_with_data_use_original_daily_checks(self):
        client = DailyFixture()
        report = self.run_history(client)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(client.calls, [
            {"start_date": "20250107", "end_date": "20250108"},
            {"start_date": "20250106", "end_date": "20250108"},
            {"start_date": "20250101", "end_date": "20250103"},
            {"start_date": "20250104", "end_date": "20250106"},
            {"trade_date": "20250104"}, {"trade_date": "20250105"}, {"trade_date": "20250106"},
        ])
        self.assertEqual(self.store.row_count("block_trade"), 1)
        retry = DailyFixture()
        self.assertEqual(self.run_history(retry)["datasets"][0]["skipped"], 2)
        self.assertEqual(retry.calls, [])

    def test_one_day_only_control_falls_back_without_assuming_early_ranges_are_empty(self):
        client = DailyFixture(control_works=False)
        self.assertEqual(self.run_history(client)["status"], "complete")
        self.assertEqual(client.calls[1:], [{"trade_date": f"2025010{n}"} for n in range(1, 7)])
        self.assertEqual(self.store.row_count("block_trade"), 1)

    def test_upstream_returning_empty_for_long_ranges_uses_original_days(self):
        client = DailyFixture(wide_control_works=False)
        self.assertEqual(self.run_history(client)["status"], "complete")
        self.assertEqual(client.calls[2:], [{"trade_date": f"2025010{n}"} for n in range(1, 7)])
        self.assertEqual(self.store.row_count("block_trade"), 1)

    def test_range_plan_reuses_original_day_checkpoints(self):
        original = self.profile | {"request": {k: v for k, v in self.profile["request"].items() if k != "history_empty_probe"}}
        self.assertEqual(self.run_history(DailyFixture(), original)["status"], "complete")
        client = DailyFixture()
        self.assertEqual(self.run_history(client)["datasets"][0]["skipped"], 2)
        self.assertEqual(client.calls, [])
        self.assertEqual(self.store.row_count("block_trade"), 1)

    def test_existing_positive_day_prevents_empty_probe_for_its_remaining_range(self):
        client = DailyFixture()
        run_ingestion(self.store, {"block_trade": self.profile}, ["block_trade"], client=client,
                      mode="history", today="20250115", start="20250105", end="20250105")
        client = DailyFixture()
        self.assertEqual(self.run_history(client)["status"], "complete")
        self.assertNotIn({"start_date": "20250104", "end_date": "20250106"}, client.calls)
        self.assertNotIn({"trade_date": "20250105"}, client.calls)
        self.assertIn({"trade_date": "20250104"}, client.calls)
        self.assertIn({"trade_date": "20250106"}, client.calls)

    def test_date_floor_applies_independently_to_exchange_variants(self):
        profile = self.profiles["opt_daily"]
        request = profile["request"] | {"dimensions": {"exchange": ["SSE", "SZSE"]},
                                        "history_start_by_param": {"exchange": {"SZSE": "2019-12-23"}}}
        partitions = self.plan(profile | {"request": request}, start=date(2019, 12, 22), end=date(2019, 12, 23))
        self.assertEqual([p.params for p in partitions], [
            {"exchange": "SSE", "trade_date": "20191222"},
            {"exchange": "SSE", "trade_date": "20191223"}, {"exchange": "SZSE", "trade_date": "20191223"},
        ])


class DailyFixture:
    def __init__(self, *, control_works=True, wide_control_works=True):
        self.calls = []
        self.control_works = control_works
        self.wide_control_works = wide_control_works

    def query(self, profile, params, *, cancelled=None):
        self.calls.append(params)
        if params.get("start_date") == "20250107":
            dates = ["20250107", "20250108"] if self.control_works else ["20250108"]
        elif params.get("start_date") == "20250106" and self.wide_control_works:
            dates = ["20250108"]
        elif params.get("start_date") == "20250104" or params.get("trade_date") == "20250105":
            dates = ["20250105"]
        else:
            dates = []
        return [{"ts_code": "600000.SH", "trade_date": day, "price": "10"} for day in dates]
