from collections import Counter
from datetime import date, timedelta
import unittest

from sqlalchemy import event

from financedb.business_tables import business_tables
from financedb.catalog import load_profiles
from financedb.storage import Store, dataset_counts


class BusinessQueriesTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profile = load_profiles()["daily"]
        self.daily = business_tables["daily"]
        with self.store.engine.begin() as conn:
            self.daily.create(conn)
            dataset_counts.create(conn)

    def insert(self, rows):
        with self.store.engine.begin() as conn:
            conn.execute(self.daily.insert(), rows)

    def collect(self, **filters):
        rows = []
        cursor = None
        for _ in range(8):
            result = self.store.query_rows(self.profile, cursor=cursor, limit=1, **filters)
            rows.extend(result["rows"])
            cursor = result["next_cursor"]
            if not cursor:
                return rows
        self.fail("分页没有终止")

    def test_filters_and_pages_keep_mixed_business_date_formats(self):
        self.insert([
            {"ts_code": "000001.SZ", "trade_date": "20261124", "close": "10"},
            {"ts_code": "000002.SZ", "trade_date": "2026-11-24", "close": "11"},
            {"ts_code": "000003.SZ", "trade_date": "2026-11-24 10:00:00", "close": "12"},
            {"ts_code": "000001.SZ", "trade_date": "20261123", "close": "9"},
        ])
        rows = self.collect(start="20261124", end="2026-11-24")
        self.assertEqual([row["close"] for row in rows], ["10", "11", "12"])
        self.assertEqual(self.store.instrument_values("daily", event_date="2026-11-24"),
                         ["000001.SZ", "000002.SZ", "000003.SZ"])

    def test_date_range_uses_raw_index_and_keeps_month_quarter_and_trimmed_values(self):
        valid = ["202611", "2026-11", "2026Q4", "2026q4", "\t20261124\r", "\u300020261124"]
        self.insert([{"ts_code": f"{i:06}.SZ", "trade_date": raw} for i, raw in enumerate([*valid, "202610", "2026Q3", "202612"])])
        self.insert([{"ts_code": "000001.SZ", "trade_date": (date(2000, 1, 1) + timedelta(days=i)).strftime("%Y%m%d")}
                     for i in range(2000)])
        with self.store.engine.begin() as conn:
            conn.exec_driver_sql("ANALYZE daily")
        statements = []

        @event.listens_for(self.store.engine, "before_cursor_execute")
        def record(conn, cursor, statement, params, context, executemany):
            statements.append((statement, params))

        rows = self.collect(start="2026-10-01", end="2026-11-24")
        self.assertCountEqual([row["trade_date"] for row in rows], [*valid, "202610"])
        first, parameters = statements[0]
        with self.store.engine.connect() as conn:
            plan = conn.exec_driver_sql("EXPLAIN QUERY PLAN " + first, parameters).all()
        self.assertTrue(any("SEARCH daily USING INDEX ix_daily_source_date" in step[-1] for step in plan), plan)

    def test_projected_pages_preserve_duplicates_without_reading_hidden_columns(self):
        self.insert([{"close": value} for value in ["20", "10", "10", "20"]])
        statements = []

        @event.listens_for(self.store.engine, "before_cursor_execute")
        def record(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        rows = self.collect(fields=["close"], visible_fields=["close"])
        self.assertEqual(Counter(row["close"] for row in rows), {"10": 2, "20": 2})
        self.assertTrue(statements)
        self.assertTrue(all("trade_date" not in statement and "ts_code" not in statement for statement in statements))

    def test_unbounded_pages_reach_sparse_history_even_with_stale_date_stats(self):
        self.insert([
            {"ts_code": "000001.SZ", "trade_date": "20261124", "close": "10"},
            {"ts_code": "000002.SZ", "trade_date": "20261124", "close": "11"},
            {"ts_code": "000003.SZ", "trade_date": "20260821", "close": "9"},
            {"ts_code": "000004.SZ", "trade_date": None, "close": "8"},
        ])
        with self.store.engine.begin() as conn:
            conn.execute(dataset_counts.insert().values(dataset_id="daily", row_count=2, max_date="2026-11-24"))
        self.assertEqual([row["close"] for row in self.collect()], ["10", "11", "9", "8"])
