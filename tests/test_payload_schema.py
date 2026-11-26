"""The retained payload column can be skipped on existing installations."""

import unittest

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateTable

from financedb.business_tables import business_tables
from financedb.payload_schema import prepare_payload_defaults


class PayloadSchemaTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)

    @staticmethod
    def old_definition(conn, dataset):
        table = business_tables[dataset]
        return str(CreateTable(table).compile(dialect=conn.dialect)).replace(" DEFAULT ('')", "", 1)

    def test_existing_table_accepts_insert_without_payload_and_keeps_rows(self):
        daily = business_tables["daily"]
        with self.engine.begin() as conn:
            conn.exec_driver_sql(self.old_definition(conn, "daily"))
            conn.exec_driver_sql("CREATE INDEX custom_daily_code ON daily(ts_code)")
            conn.exec_driver_sql(
                "INSERT INTO daily (_record_key, _event_date, _instrument, _payload, _content_hash, _version_order, "
                "_observed_at, ts_code) VALUES ('old', '', '', 'legacy', '', '', '', 'LEGACY')"
            )
            root_page = conn.exec_driver_sql("SELECT rootpage FROM sqlite_master WHERE name = 'daily'").scalar_one()

        with self.engine.begin() as conn:
            prepare_payload_defaults(conn, [daily])
            prepare_payload_defaults(conn, [daily])
            conn.exec_driver_sql(
                "INSERT INTO daily (_record_key, _event_date, _instrument, _content_hash, _version_order, "
                "_observed_at, ts_code) VALUES ('new', '', '', '', '', '', 'CURRENT')"
            )
            self.assertEqual(conn.exec_driver_sql(
                "SELECT _record_key, ts_code, _payload FROM daily ORDER BY _record_key"
            ).all(), [("new", "CURRENT", ""), ("old", "LEGACY", "legacy")])
            self.assertEqual(conn.exec_driver_sql("SELECT rootpage FROM sqlite_master WHERE name = 'daily'").scalar_one(), root_page)
            self.assertEqual(conn.exec_driver_sql(
                "SELECT name FROM pragma_index_list('daily') WHERE name = 'custom_daily_code'"
            ).scalar_one(), "custom_daily_code")
            self.assertEqual(conn.exec_driver_sql("PRAGMA quick_check").scalar_one(), "ok")

    def test_unknown_old_table_definition_is_left_intact(self):
        adj_factor = business_tables["adj_factor"]
        with self.engine.begin() as conn:
            conn.exec_driver_sql(self.old_definition(conn, "adj_factor").replace("_payload TEXT NOT NULL,", "_payload TEXT NOT NULL CHECK (1),"))
            original = conn.exec_driver_sql("SELECT sql FROM sqlite_master WHERE name = 'adj_factor'").scalar_one()

        with self.assertRaisesRegex(RuntimeError, "adj_factor"):
            with self.engine.begin() as conn:
                prepare_payload_defaults(conn, [adj_factor])

        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("SELECT sql FROM sqlite_master WHERE name = 'adj_factor'").scalar_one(), original)
            self.assertEqual(conn.exec_driver_sql("PRAGMA writable_schema").scalar_one(), 0)
