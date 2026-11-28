import sqlite3
import unittest
from threading import Event
from unittest.mock import patch

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from financedb.business_tables import business_tables
from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import Store, storage_error_details


def sqlite_error(code):
    driver_error = sqlite3.OperationalError("private driver details")
    driver_error.sqlite_errorcode = code
    return OperationalError("private statement", {"password": "private parameter"}, driver_error)


class FactorDatabaseRetryTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        profile = load_profiles()["factor_value"]
        self.profile = profile | {"request": profile["request"] | {"window_days": 1}}
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        day = params["start_date"]
        self.calls.append(day)
        return [{"factor_name": "test_factor", "ts_code": params["ts_code"], "trade_date": day, "factor_value": "1.25"}]

    def collect(self, *, end="20261124", cancelled=None):
        return run_ingestion(self.store, {"factor_value": self.profile}, ["factor_value"], client=self, mode="history",
                             start="20261124", end=end, today="2026-11-27", codes=["600000.SH"], cancelled=cancelled)

    def test_transient_write_rolls_back_and_reuses_same_response(self):
        save = self.store._save_partition
        attempts = []
        table = business_tables["factor_value"]

        def write(conn, values, **kwargs):
            # Writes and the checkpoint must both roll back before the retry.
            attempts.append(conn.scalar(select(func.count()).select_from(table)))
            save(conn, values, **kwargs)
            if len(attempts) == 1:
                raise sqlite_error(sqlite3.SQLITE_BUSY)

        with patch.object(self.store, "_save_partition", side_effect=write), patch("financedb.ingest._wait") as wait:
            result = self.collect()
        self.assertEqual(result["status"], "complete")
        self.assertEqual(attempts, [1, 1])
        self.assertEqual(self.calls, ["20261124"])
        self.assertEqual(self.store.row_count("factor_value"), 1)
        self.assertEqual([item["status"] for item in self.store.list_partitions("factor_value")], ["complete"])
        wait.assert_called_once()
        self.assertEqual(self.collect()["datasets"][0]["skipped"], 1)
        self.assertEqual(self.calls, ["20261124"])

    def test_disk_full_keeps_prior_checkpoint_without_retry_and_hides_driver_details(self):
        save = self.store._save_partition
        attempts = []

        def write(conn, values, **kwargs):
            attempts.append(values["params"])
            if len(attempts) > 1:
                raise sqlite_error(sqlite3.SQLITE_FULL)
            save(conn, values, **kwargs)

        with patch.object(self.store, "_save_partition", side_effect=write), patch("financedb.ingest._wait") as wait:
            result = self.collect(end="20261125")
        self.assertEqual(result["status"], "partial")
        self.assertEqual(len(attempts), 2)
        wait.assert_not_called()
        self.assertEqual(result["datasets"][0]["failed"], 1)
        self.assertIn("SQLite 13 · 存储空间不足", result["datasets"][0]["message"])
        self.assertNotIn("private", repr(result))
        self.assertEqual(self.store.row_count("factor_value"), 1)
        self.assertEqual(len(self.store.list_partitions("factor_value")), 1)

    def test_transient_retry_stops_on_cancellation(self):
        stopped = Event()

        def write(*args, **kwargs):
            stopped.set()
            raise sqlite_error(sqlite3.SQLITE_LOCKED_SHAREDCACHE)

        with patch.object(self.store, "_save_partition", side_effect=write) as save:
            result = self.collect(cancelled=stopped.is_set)
        save.assert_called_once()
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(self.store.row_count("factor_value"), 0)

    def test_transient_retry_is_bounded(self):
        with patch.object(self.store, "_save_partition", side_effect=sqlite_error(sqlite3.SQLITE_BUSY)) as save, \
             patch("financedb.ingest._wait") as wait:
            result = self.collect()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.calls, ["20261124"])
        self.assertEqual(save.call_count, 3)
        self.assertEqual(wait.call_count, 2)
        self.assertEqual(result["datasets"][0]["failed"], 1)
        self.assertEqual(self.store.row_count("factor_value"), 0)
        self.assertEqual(self.store.list_partitions("factor_value"), [])

    def test_mysql_error_codes_distinguish_transient_and_permanent_failures(self):
        cases = {1205: ("锁等待超时", True), 1213: ("事务死锁", True), 2006: ("连接已断开", True),
                 2013: ("查询时连接中断", True), 1114: ("表空间不足", False), 1153: ("数据包超出服务器限制", False),
                 1146: ("业务表不存在", False)}
        for code, (reason, retryable) in cases.items():
            with self.subTest(code=code):
                error = OperationalError("private statement", {"token": "private parameter"}, Exception(code, "private driver"))
                self.assertEqual(storage_error_details(error, "mysql"),
                                 (f"目标数据库操作失败（MySQL {code} · {reason}）", retryable))


if __name__ == "__main__":
    unittest.main()
