import os
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
from threading import Event
from time import monotonic, sleep
import unittest
from unittest.mock import patch

from financedb.ops import ACTIVE, JobManager


PROFILES = {
    "daily": {"category": "股票数据", "mode": "history", "api_name": "daily", "request": {"strategy": "date"}},
    "weekly": {"category": "股票数据", "mode": "history", "api_name": "weekly", "request": {"strategy": "date"}},
    "index_daily": {"category": "指数专题", "mode": "history", "api_name": "index_daily", "request": {"strategy": "date"}},
}


class CategorySchedulerTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.url = f"sqlite:///{tmp.name}/category_test.sqlite3"
        environment = patch.dict(os.environ, {"TUSHARE_TOKEN": ""})
        environment.start()
        self.addCleanup(environment.stop)
        registry = patch("financedb.ops.load_profiles", return_value=PROFILES)
        registry.start()
        self.addCleanup(registry.stop)

    def manager(self, runner=None):
        manager = JobManager(self.url, client_factory=lambda token: object(), runner=runner or complete)
        self.addCleanup(manager.close)
        return manager

    def plans(self, manager):
        return {item["category"]: item for item in manager.state()["category_schedules"]}

    def await_state(self, manager, predicate):
        deadline = monotonic() + 4
        while monotonic() < deadline:
            state = manager.state()
            if predicate(state):
                return state
            sleep(0.02)
        self.fail(f"Timed out waiting for expected state: {manager.state()}")

    def test_category_controls_migrate_only_the_configured_legacy_scope(self):
        first = self.manager()
        first.set_schedule(enabled=True, time="20:10", datasets=["daily"])
        prior = self.plans(first)
        self.assertEqual((prior["股票数据"]["enabled"], prior["股票数据"]["datasets_total"]), (True, 1))
        self.assertFalse(prior["指数专题"]["enabled"])
        first.set_category_schedule(category="指数专题", enabled=True, time="21:50")
        first.close()
        restored = self.plans(self.manager())
        self.assertEqual((restored["股票数据"]["enabled"], restored["股票数据"]["time"], restored["股票数据"]["datasets_total"]),
                         (True, "20:10", 1))
        self.assertEqual((restored["指数专题"]["enabled"], restored["指数专题"]["time"], restored["指数专题"]["datasets_total"]),
                         (True, "21:50", 1))

    def test_due_categories_serialize_and_each_target_survives_restart(self):
        entered, release = Event(), Event()
        runs = []

        def runner(*args, **kwargs):
            datasets = args[2]
            runs.append(datasets)
            if len(runs) == 1:
                entered.set()
                release.wait(2)
            return complete(*args, **kwargs)

        first = self.manager(runner)
        self.addCleanup(release.set)
        for category, time in (("股票数据", "00:00"), ("指数专题", "00:00")):
            first.set_category_schedule(category=category, enabled=True, time=time)
        with first._lock:
            for plan in first._category_plans["plans"].values():
                plan["enabled_since"] = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        first._check_schedule()
        self.assertEqual(self.plans(first)["股票数据"]["last_status"], "waiting_token")
        first.set_token("synthetic-scheduling-token")
        first._check_schedule()
        self.assertTrue(entered.wait(2))
        during = self.plans(first)
        self.assertIn(during["股票数据"]["last_status"], ACTIVE)
        self.assertTrue(during["指数专题"]["queued"])
        self.assertEqual(len(runs), 1)
        release.set()
        finished = self.await_state(first, lambda state: len(state["history"]) == 2 and state["job"]["status"] == "complete")
        self.assertEqual(runs, [["daily", "weekly"], ["index_daily"]])
        self.assertTrue(all(item["progress"]["partitions_done"] == item["datasets_total"] for item in finished["category_schedules"]))
        first.close()
        restored = self.manager(runner)
        restored.set_token("synthetic-scheduling-token")
        restored._check_schedule()
        self.assertIsNone(restored.state()["job"])
        self.assertEqual(len(runs), 2)

    def test_full_job_remains_visible_after_running_a_category(self):
        first = self.manager()
        first.set_token("synthetic-history-token")
        full = first.start_job(mode="history")
        self.await_state(first, lambda state: state["job"]["status"] == "complete")
        category = first.start_category_job(category="指数专题")
        finished = self.await_state(first, lambda state: state["job"]["status"] == "complete")
        self.assertEqual(finished["job"]["id"], category["id"])
        self.assertEqual(finished["full_job"]["id"], full["id"])
        self.assertEqual(finished["job"]["trigger"], "category_manual")
        first.close()
        restored = self.manager().state()
        self.assertEqual(restored["full_job"]["id"], full["id"])
        index = next(item for item in restored["category_schedules"] if item["category"] == "指数专题")
        self.assertEqual((index["last_status"], index["progress"]["partitions_done"], index["last_trigger"]),
                         ("complete", 1, "category_manual"))

    def test_category_job_includes_external_dependency_without_inflating_scope(self):
        profiles = PROFILES | {"stock_basic": {"category": "股票数据", "mode": "snapshot", "api_name": "stock_basic",
                                               "request": {"strategy": "snapshot"}}}
        profiles["index_daily"] = profiles["index_daily"] | {"request": {"strategy": "date", "cap_code_source": "stock_basic"}}
        calls = []

        def runner(*args, **kwargs):
            calls.append(args[2])
            return complete(*args, **kwargs)

        with patch("financedb.ops.load_profiles", return_value=profiles):
            manager = self.manager(runner)
            manager.set_token("synthetic-snapshot-token")
            manager.start_category_job(category="指数专题")
            state = self.await_state(manager, lambda state: state["job"]["status"] == "complete")
        self.assertEqual(calls, [["stock_basic", "index_daily"]])
        self.assertEqual(state["job"]["datasets_total"], 1)
        self.assertEqual([group["name"] for group in state["job"]["categories"]], ["指数专题"])
        self.assertEqual([row["dataset_id"] for row in state["job"]["datasets"]], ["index_daily"])

    def test_stopped_group_keeps_unfinished_data_out_of_progress(self):
        entered = Event()

        def runner(store, profiles, datasets, *, progress, cancelled, **kwargs):
            completed, current = datasets
            progress({"event": "dataset_finished", "dataset_id": completed, "status": "complete", "planned": 1, "completed": 1})
            progress({"event": "dataset_started", "dataset_id": current})
            entered.set()
            deadline = monotonic() + 2
            while not cancelled() and monotonic() < deadline:
                sleep(0.01)
            progress({"event": "dataset_finished", "dataset_id": current, "status": "cancelled"})
            return {"status": "cancelled"}

        manager = self.manager(runner)
        manager.set_token("synthetic-cancel-token")
        manager.start_category_job(category="股票数据")
        self.assertTrue(entered.wait(1))
        manager.stop_job()
        state = self.await_state(manager, lambda state: state["job"]["status"] == "cancelled")
        group = state["job"]["categories"][0]
        self.assertEqual((group["datasets_finished"], group["datasets_total"], group["status"]), (1, 2, "partial"))


def complete(store, profiles, datasets, *, progress, **kwargs):
    for dataset in datasets:
        progress({"event": "dataset_started", "dataset_id": dataset})
        progress({"event": "dataset_finished", "dataset_id": dataset, "status": "complete", "planned": 1, "completed": 1})
    return {"status": "complete"}


if __name__ == "__main__":
    unittest.main()
