from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from financedb.audit import repair_category
from financedb.catalog import catalog_snapshot, load_profiles
from financedb.ingest import run_ingestion
from financedb.ops import DATA_AUDIT_OPERATION, DEFAULT_SCHEDULE, JobManager
from financedb.storage import Store


MINUTE_DATASETS = {"stk_mins", "idx_mins", "etf_mins", "ft_mins", "opt_mins", "hk_mins", "sw_mins"}


class NoMinuteOfflineTest(unittest.TestCase):
    def test_job_scopes_exclude_minutes_and_catalog_still_lists_them(self):
        with TemporaryDirectory() as directory:
            manager = JobManager(f"sqlite:///{Path(directory, 'finance.sqlite3')}")
            try:
                for mode in ("sample", "history", "incremental"):
                    with self.subTest(mode=mode):
                        selected = manager._selected(None, mode)
                        self.assertTrue(MINUTE_DATASETS.isdisjoint(selected))
                        self.assertIn("daily", selected)
                        with self.assertRaisesRegex(ValueError, "stk_mins"):
                            manager.start_job(mode=mode, datasets=["stk_mins"])
                for category in manager._category_datasets:
                    self.assertTrue(MINUTE_DATASETS.isdisjoint(manager._category_scope(category)))
                stock_audit_targets = manager._category_datasets["股票数据"]
                self.assertIn("daily", stock_audit_targets)
                self.assertNotIn("stk_mins", stock_audit_targets)
            finally:
                manager.close()

        snapshot = catalog_snapshot()
        excluded = {dataset_id for entry in snapshot["entries"] if entry["collection_status"] == "offline_excluded"
                    for dataset_id in entry["profile_ids"]}
        self.assertEqual(excluded, MINUTE_DATASETS)

    def test_direct_ingestion_and_stale_repair_issue_no_upstream_requests(self):
        store = Store("sqlite://")
        self.addCleanup(store.close)
        profiles = load_profiles()
        client = Mock(spec=["query"])
        client.query.side_effect = AssertionError("minute data must not reach Tushare")

        result = run_ingestion(store, profiles, sorted(MINUTE_DATASETS), client=client, mode="history")
        self.assertEqual(result["attempted_partitions"], 0)
        self.assertEqual({item["dataset_id"]: item["status"] for item in result["datasets"]},
                         dict.fromkeys(MINUTE_DATASETS, "unsupported"))

        report = repair_category(store, profiles, [{"dataset_id": "stk_mins"}],
                                 client=client, cancelled=lambda: False, notify=lambda _: None)
        self.assertEqual((report["written"], report["skipped"], report["failed"]), (0, 1, 0))
        client.query.assert_not_called()

    def test_restored_schedule_and_pending_repair_drop_disabled_minutes(self):
        with TemporaryDirectory() as directory:
            url = f"sqlite:///{Path(directory, 'finance.sqlite3')}"
            store = Store(url)
            try:
                store.initialize()
                store.write_operation("schedule", DEFAULT_SCHEDULE | {"enabled": True, "datasets": ["stk_mins"]})
                store.write_operation(DATA_AUDIT_OPERATION, {"report": {"id": "old-audit", "status": "complete"},
                                                             "plan": [{"dataset_id": "stk_mins"}]})
            finally:
                store.close()

            manager = JobManager(url)
            try:
                state = manager.state()
                self.assertFalse(state["schedule"]["enabled"])
                self.assertFalse(state["data_audit"]["can_repair"])
                self.assertIn("重新走查", state["data_audit"]["message"])
            finally:
                manager.close()
