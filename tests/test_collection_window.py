from datetime import date
import unittest

from financedb.catalog import load_profiles
from financedb.ingest import IngestError, _partition_id, plan_partitions, recent_history_range, run_ingestion
from financedb.storage import Store


TEXT_DATASETS = ("anns_d", "idx_anns", "news", "major_news", "cctv_news")


class CollectionWindowTest(unittest.TestCase):
    def setUp(self):
        self.profiles = load_profiles()

    def plan(self, dataset_id, **kwargs):
        return list(plan_partitions(object(), self.profiles[dataset_id], today=date(2026, 11, 26), **kwargs))

    def test_history_only_requests_the_last_calendar_year(self):
        for dataset_id in TEXT_DATASETS:
            with self.subTest(dataset=dataset_id):
                partitions = self.plan(dataset_id, mode="history")
                self.assertEqual(min(part.start.date() for part in partitions), date(2025, 11, 26))
                self.assertEqual(max(part.end.date() for part in partitions), date(2026, 11, 25))

    def test_explicit_dates_cannot_bypass_window_in_any_mode(self):
        for dataset_id in TEXT_DATASETS:
            for mode in ["history", "incremental", "sample"]:
                with self.subTest(dataset=dataset_id, mode=mode):
                    partitions = self.plan(dataset_id, mode=mode, start=date(1999, 1, 1), end=date(2099, 1, 1))
                    self.assertEqual(min(part.start.date() for part in partitions), date(2025, 11, 26))
                    self.assertEqual(max(part.end.date() for part in partitions), date(2026, 11, 26))

    def test_old_explicit_range_sends_no_partitions(self):
        for dataset_id in TEXT_DATASETS:
            with self.subTest(dataset=dataset_id), self.assertRaisesRegex(IngestError, "最近 1 年"):
                self.plan(dataset_id, mode="history", start=date(2020, 1, 1), end=date(2020, 12, 31))

    def test_leap_day_uses_february_28_as_previous_year_boundary(self):
        partitions = list(plan_partitions(object(), self.profiles["anns_d"], mode="history", today=date(2024, 2, 29)))
        self.assertEqual(partitions[0].start.date(), date(2023, 2, 28))
        self.assertEqual(partitions[-1].end.date(), date(2024, 2, 28))

    def test_recent_checkpoints_keep_the_same_identity(self):
        profile = self.profiles["anns_d"]
        legacy = profile | {"request": {key: value for key, value in profile["request"].items() if key != "lookback_years"}}
        for part in self.plan("anns_d", mode="history", start=date(2026, 11, 24), end=date(2026, 11, 25)):
            self.assertEqual(_partition_id(profile, part, "history"), _partition_id(legacy, part, "history"))

    def test_other_financial_data_keeps_its_historical_range(self):
        partitions = self.plan("daily", mode="history", start=date(2000, 1, 1), end=date(2000, 1, 3))
        self.assertEqual(partitions[0].start.date(), date(2000, 1, 1))

    def test_factor_value_caps_default_and_explicit_history_at_thirty_days(self):
        wide = {"start": date(2012, 1, 1), "end": date(2099, 1, 1)}
        for scope in ({}, wide):
            with self.subTest(scope=scope):
                part, = self.plan("factor_value", mode="history", codes=["600000.SH"], **scope)
                self.assertEqual(part.params, {
                    "ts_code": "600000.SH", "start_date": "20261027", "end_date": "20261125",
                })
        with self.assertRaisesRegex(IngestError, "最近 30 天"):
            self.plan("factor_value", mode="history", codes=["600000.SH"],
                      start=date(2026, 10, 1), end=date(2026, 10, 26))

    def test_factor_value_keeps_incremental_planning_and_stock_technical_factor_history(self):
        factor = self.profiles["factor_value"]
        request = {key: value for key, value in factor["request"].items() if key != "history_lookback_days"}
        options = {"mode": "incremental", "today": date(2026, 11, 26), "codes": ["600000.SH"]}
        expected = list(plan_partitions(object(), factor | {"request": request}, **options))
        self.assertEqual(list(plan_partitions(object(), factor, **options)), expected)

        part, = self.plan("stk_factor_pro", mode="history", start=date(2012, 1, 4), end=date(2012, 1, 4))
        self.assertEqual(part.params["trade_date"], "20120104")

    def test_recent_full_history_bounds_periods_preserves_references_and_skips_empty_scope(self):
        calendars = ("trade_cal", "fut_trade_cal", "hk_tradecal", "us_tradecal")
        profiles = self.profiles | {
            dataset_id: self.profiles[dataset_id] | {"history_start": "2023-01-01"}
            for dataset_id in calendars
        }
        calls = {}

        class Client:
            def query(self, profile, params, *, cancelled=None):
                calls.setdefault(profile["id"], []).append(params.copy())
                return []

        store = Store("sqlite://")
        self.addCleanup(store.close)
        today = date(2024, 2, 29)
        self.assertEqual(recent_history_range(today), (date(2023, 2, 28), date(2024, 2, 28)))
        report = run_ingestion(
            store, profiles, [*calendars, "cn_cpi", "cn_gdp", "stock_basic", "stk_account_old"],
            client=Client(), mode="history", today=today, recent_history=True,
        )
        self.assertEqual(report["status"], "complete")
        for dataset_id in calendars:
            with self.subTest(calendar=dataset_id):
                self.assertEqual(min(row["start_date"] for row in calls[dataset_id]), "20230101")
                self.assertEqual(max(row["end_date"] for row in calls[dataset_id]), "20240228")
        self.assertEqual([row["m"] for row in calls["cn_cpi"]],
                         [*(f"2023{month:02}" for month in range(2, 13)), "202401"])
        self.assertEqual([row["q"] for row in calls["cn_gdp"]], ["2023Q1", "2023Q2", "2023Q3", "2023Q4"])
        self.assertTrue(all("start_date" not in row for row in calls["stock_basic"]))
        self.assertNotIn("stk_account_old", calls)
        archived = next(row for row in report["datasets"] if row["dataset_id"] == "stk_account_old")
        self.assertEqual(archived["status"], "complete")
        self.assertIn("没有交集", archived["message"])

