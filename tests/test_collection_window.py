from datetime import date
import unittest

from financedb.catalog import load_profiles
from financedb.ingest import IngestError, _partition_id, plan_partitions


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

