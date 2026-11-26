from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest

from sqlalchemy import inspect

from financedb.catalog import load_profiles
from financedb.ingest import import_tick_csv
from financedb.storage import Store


class TickImportTest(unittest.TestCase):
    def test_resume_with_different_batch_boundaries_preserves_identical_ticks(self):
        with TemporaryDirectory() as directory:
            delivery = Path(directory, "tick.csv")
            delivery.write_text(
                "InstrumentID,TradingDay,UpdateTime,LastPrice\n"
                "cu2310,20230925,10:00:00.500,68210\n"
                "cu2310,20230925,10:00:00.500,68210\n"
                "cu2310,20230925,10:00:01.000,68211\n"
                "cu2310,20230925,10:00:01.500,68212\n"
            )
            store = Store("sqlite://")
            self.addCleanup(store.close)
            profile = load_profiles()["fut_tick"]
            stop = Event()

            def progress(report):
                if report.get("partition_status") == "complete":
                    stop.set()

            result = import_tick_csv(store, profile, [delivery], batch_size=2, progress=progress, cancelled=stop.is_set)
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(store.row_count("fut_tick"), 2)

            result = import_tick_csv(store, profile, [delivery], batch_size=3)
            self.assertEqual(result["status"], "complete")
            self.assertEqual(store.row_count("fut_tick"), 4)
            rows = store.query_rows(profile)["rows"]
            self.assertCountEqual([row["LastPrice"] for row in rows], ["68210", "68210", "68211", "68212"])
            self.assertEqual({column["name"] for column in inspect(store.engine).get_columns("fut_tick")},
                             set(profile["fields"]))

            # The old per-row identity also treated the same file hash as the
            # same delivery if a different encoding was supplied on retry.
            result = import_tick_csv(store, profile, [delivery], batch_size=1, encoding="utf-8")
            self.assertEqual(result["status"], "complete")
            self.assertEqual(store.row_count("fut_tick"), 4)
