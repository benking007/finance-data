import unittest

from sqlalchemy import BigInteger, Column, Index, MetaData, String, create_engine, inspect, select, text

from financedb.business_schema import assert_business_columns, migrate_business_tables
from financedb.business_tables import LONG_TEXT, business_metadata, business_tables
from financedb.catalog import load_profiles


LEGACY_COLUMNS = (
    "_record_key", "_event_date", "_instrument", "_payload", "_content_hash", "_version_order", "_observed_at",
)
LEGACY_VALUES = {name: "legacy" for name in LEGACY_COLUMNS}


def legacy_business_table(dataset_id, metadata):
    """Reconstruct a v2 table for migration tests."""
    table = business_tables[dataset_id].to_metadata(metadata)
    for index in list(table.indexes):
        if index.name == f"ix_{dataset_id}_business_key":
            table.indexes.remove(index)
    for column in (
        Column("_record_key", String(64), primary_key=True),
        Column("_event_date", String(32), nullable=False),
        Column("_instrument", String(128), nullable=False),
        Column("_payload", LONG_TEXT, nullable=False, server_default=text("('')")),
        Column("_content_hash", String(64), nullable=False),
        Column("_version_order", String(96), nullable=False),
        Column("_observed_at", String(32), nullable=False),
    ):
        table.append_column(column)
    if dataset_id == "fut_tick":
        table.append_column(Column("source_sha256", LONG_TEXT))
        table.append_column(Column("source_row", BigInteger))
    Index(f"ix_{dataset_id}_date", table.c._event_date, table.c._record_key)
    Index(f"ix_{dataset_id}_code_date", table.c._instrument, table.c._event_date, table.c._record_key)
    return table


class BusinessSchemaTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)

    def test_every_registered_table_contains_exactly_the_business_fields(self):
        with self.engine.begin() as conn:
            business_metadata.create_all(conn)
            inspector = inspect(conn)
            reflected = inspector.get_multi_columns()
            profiles = load_profiles()
            self.assertEqual(set(inspector.get_table_names()), set(profiles))
            for name, profile in profiles.items():
                with self.subTest(table=name):
                    actual = [column["name"] for column in reflected[None, name]]
                    self.assertEqual(actual, profile["fields"])
                    self.assertFalse(any(column.startswith("_") for column in actual))
            self.assertNotIn("source_sha256", business_tables["fut_tick"].columns)
            self.assertNotIn("source_row", business_tables["fut_tick"].columns)

    def test_upgrade_removes_additional_system_columns_and_preserves_rows(self):
        old = MetaData()
        daily = legacy_business_table("daily", old)
        tick = legacy_business_table("fut_tick", old)
        company = legacy_business_table("fund_company", old)
        legacy_quote = {"ts_code": "000001.SZ", "trade_date": "20260105", "close": "11.0125"}
        tick_values = {"InstrumentID": "cu2310", "TradingDay": "20230925", "UpdateTime": "10:00:00.500",
                       "LastPrice": "68190.000000", "source_sha256": "example"}
        with self.engine.begin() as conn:
            old.create_all(conn)
            conn.exec_driver_sql("ALTER TABLE daily ADD COLUMN _etl_progress TEXT DEFAULT 'old'")
            conn.exec_driver_sql("CREATE INDEX custom_etl ON daily (_etl_progress)")
            conn.exec_driver_sql("CREATE INDEX custom_close ON daily (close)")
            conn.exec_driver_sql("CREATE VIEW daily_prices AS SELECT ts_code, close FROM daily")
            conn.execute(daily.insert(), LEGACY_VALUES | legacy_quote)
            conn.execute(tick.insert(), [LEGACY_VALUES | tick_values | {"_record_key": str(i), "source_row": i}
                                          for i in (1, 2)])
            conn.execute(company.insert(), LEGACY_VALUES | {"name": "示例公司"})

        with self.engine.begin() as conn:
            tables = inspect(conn).get_table_names()
            migrate_business_tables(conn, tables)
            migrate_business_tables(conn, tables)
            assert_business_columns(conn, tables)
            self.assertEqual(conn.execute(select(business_tables["daily"].c.close)).scalars().all(), ["11.0125"])
            self.assertEqual(conn.execute(select(business_tables["fut_tick"].c.LastPrice)).scalars().all(), ["68190.000000"] * 2)
            self.assertEqual(conn.exec_driver_sql("SELECT * FROM daily_prices").all(), [("000001.SZ", "11.0125")])
            self.assertEqual({index["name"] for index in inspect(conn).get_indexes("daily")},
                             {"ix_daily_source_code", "ix_daily_source_date", "custom_close"})
            self.assertIn("ix_fund_company_business_key", {index["name"] for index in inspect(conn).get_indexes("fund_company")})
            self.assertEqual(conn.exec_driver_sql("PRAGMA quick_check").scalar_one(), "ok")

    def test_unclassified_columns_stop_before_altering_any_table(self):
        old = MetaData()
        legacy_business_table("anns_d", old)
        legacy_business_table("daily", old)
        with self.engine.begin() as conn:
            old.create_all(conn)
            conn.exec_driver_sql("ALTER TABLE daily ADD COLUMN customer_note TEXT")
        with self.assertRaisesRegex(RuntimeError, "customer_note"):
            with self.engine.begin() as conn:
                migrate_business_tables(conn, inspect(conn).get_table_names())
        for name in ("anns_d", "daily"):
            self.assertIn("_record_key", {column["name"] for column in inspect(self.engine).get_columns(name)})

    def test_incompatible_view_rolls_back_the_entire_sqlite_rebuild(self):
        old = MetaData()
        daily = legacy_business_table("daily", old)
        legacy_business_table("anns_d", old)
        with self.engine.begin() as conn:
            old.create_all(conn)
            conn.execute(daily.insert(), LEGACY_VALUES | {"ts_code": "000001.SZ"})
            conn.exec_driver_sql("CREATE VIEW old_payload AS SELECT _payload FROM daily")
        with self.assertRaisesRegex(RuntimeError, "old_payload"):
            with self.engine.begin() as conn:
                migrate_business_tables(conn, inspect(conn).get_table_names())
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("SELECT _payload FROM old_payload").scalar_one(), "legacy")
            self.assertIn("_record_key", {column["name"] for column in inspect(conn).get_columns("anns_d")})
