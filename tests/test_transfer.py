from hashlib import sha256
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select, text

from financedb.business_tables import business_tables
from financedb.catalog import load_profiles
from financedb.storage import SCHEMA_VERSION, SchemaMismatch, Store, json_text
from financedb.transfer import build_release, migrate


class TransferTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.profiles = load_profiles()
        self.daily = {"ts_code": "000001.SZ", "trade_date": "20260105", "close": "11.0125", "amount": "42000.25"}

    def url(self, name):
        return f"sqlite:///{self.directory / (name + '.sqlite3')}"

    def store(self, name):
        store = Store(self.url(name))
        self.addCleanup(store.close)
        store.initialize()
        return store

    def seed_obsolete_payload(self, store):
        daily = business_tables["daily"]
        with store.engine.begin() as conn:
            conn.execute(daily.update().values(_payload='{"close":"9","amount":"legacy-only"}'))

    def test_same_schema_copy_preserves_business_records_and_resume_state(self):
        source = self.store("source")
        source.commit_partition(self.profiles["daily"], "daily/20260105", {}, [self.daily])
        self.seed_obsolete_payload(source)
        income = {"ts_code": "000001.SZ", "end_date": "20251231", "report_type": "1", "comp_type": "1",
                  "f_ann_date": "20260301", "total_revenue": "811.01"}
        source.commit_partition(self.profiles["income"], "income/first", {}, [income])
        revised = income | {"f_ann_date": "20260322", "total_revenue": "824.33"}
        source.commit_partition(self.profiles["income"], "income/revised", {}, [revised])
        source.write_operation("schedule", {"enabled": True, "time": "21:00"})
        for key in ("database_target", "tushare_token"):
            source.write_operation(key, {"encrypted": "fixture-credential"})

        report = migrate(self.url("source"), self.url("clone"))
        self.assertEqual(report["daily"]["rows"], 1)
        self.assertEqual(report["income"]["rows"], 1)
        self.assertEqual(report["dataset_revisions"]["rows"], 2)
        self.assertEqual(report["ingestion_partitions"]["rows"], 3)
        self.assertEqual(report["operation_state"]["rows"], 1)
        self.assertNotIn("dataset_rows", report)
        clone = self.store("clone")
        self.assertEqual(clone.query_rows(self.profiles["daily"])["rows"], [self.daily])
        self.assertEqual(clone.query_rows(self.profiles["income"])["rows"], [revised])
        with clone.engine.connect() as conn:
            self.assertIn(conn.scalar(select(business_tables["daily"].c._payload)), (None, ""))
        self.assertEqual(clone.dataset_stats("daily")["row_count"], 1)
        self.assertTrue(clone.partition_done("daily", "daily/20260105"))
        self.assertEqual(clone.read_operation("schedule"), {"enabled": True, "time": "21:00"})
        self.assertIsNone(clone.read_operation("database_target"))
        self.assertIsNone(clone.read_operation("tushare_token"))
        with self.assertRaisesRegex(ValueError, "请指定空库"):
            migrate(self.url("source"), self.url("clone"))

    def test_release_copies_only_approved_values_into_business_columns(self):
        source = self.store("source")
        source.commit_partition(self.profiles["daily"], "daily/20260105", {}, [self.daily])
        self.seed_obsolete_payload(source)
        source.write_operation("schedule", {"enabled": True})
        selected = {"daily": ["close"]}

        self.assertEqual(build_release(self.url("source"), self.url("release"), selected), {"daily": 1})
        release = self.store("release")
        with release.engine.connect() as conn:
            row = conn.execute(select(business_tables["daily"])).mappings().one()
        self.assertIn(row["_payload"], (None, ""))
        self.assertEqual(row["_content_hash"], sha256(json_text({"close": "11.0125"}).encode()).hexdigest())
        self.assertEqual(row["close"], "11.0125")
        self.assertIsNone(row["amount"])
        self.assertIsNone(row["ts_code"])
        self.assertIsNone(row["trade_date"])
        self.assertEqual(row["_instrument"], "")
        self.assertEqual(row["_event_date"], "")
        stats = release.dataset_stats("daily")
        self.assertEqual(stats["row_count"], 1)
        self.assertIsNone(stats["min_date"])
        self.assertIsNone(stats["max_date"])
        self.assertIsNone(release.read_operation("schedule"))
        self.assertEqual(release.read_operation("release_manifest")["fields"], selected)

        daily = business_tables["daily"]
        with source.engine.begin() as conn:
            conn.execute(daily.update().values(_instrument="legacy-only", _event_date="1900-01-01"))
        fields = ["ts_code", "trade_date", "close"]
        self.assertEqual(build_release(self.url("source"), self.url("indexed"), {"daily": fields}), {"daily": 1})
        indexed = self.store("indexed")
        rows = indexed.query_rows(self.profiles["daily"], code="000001.SZ", start="20260105", end="20260105")["rows"]
        self.assertEqual(rows, [{field: self.daily[field] for field in fields}])

    def test_alembic_empty_database_and_legacy_refusal(self):
        config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
        with patch.dict(os.environ, {"FINANCEDB_DATABASE_URL": self.url("baseline")}):
            command.upgrade(config, "head")
        store = Store(self.url("baseline"), read_only=True)
        self.addCleanup(store.close)
        store.validate_schema()
        names = set(inspect(store.engine).get_table_names())
        self.assertTrue({"daily", "income", "dataset_counts"} <= names)
        self.assertNotIn("dataset_rows", names)
        with store.engine.connect() as conn:
            self.assertEqual(conn.scalar(text("SELECT version FROM schema_versions")), SCHEMA_VERSION)

        legacy = create_engine(self.url("legacy"))
        self.addCleanup(legacy.dispose)
        with legacy.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE dataset_rows (legacy_data TEXT)")
            conn.exec_driver_sql("CREATE TABLE schema_versions (version INTEGER PRIMARY KEY)")
            conn.exec_driver_sql("INSERT INTO schema_versions VALUES (1)")
            conn.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32))")
            conn.exec_driver_sql("INSERT INTO alembic_version VALUES ('0001')")
        old_tables = set(inspect(legacy).get_table_names())
        with patch.dict(os.environ, {"FINANCEDB_DATABASE_URL": self.url("legacy")}):
            with self.assertRaises(SchemaMismatch):
                command.upgrade(config, "head")
        self.assertEqual(set(inspect(legacy).get_table_names()), old_tables)
