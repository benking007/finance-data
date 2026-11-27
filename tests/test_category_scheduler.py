import json
import os
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
from threading import Barrier, Event, Lock
from time import monotonic, sleep
import unittest
from unittest.mock import patch

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import IngestCancelled, RateLimiter, TushareClient
from financedb.ops import ACTIVE, JobManager


PROFILES = {
    "daily": {"category": "股票数据", "mode": "history", "api_name": "daily", "request": {"strategy": "date"}},
    "weekly": {"category": "股票数据", "mode": "history", "api_name": "weekly", "request": {"strategy": "date"}},
    "index_daily": {"category": "指数专题", "mode": "history", "api_name": "index_daily", "request": {"strategy": "date"}},
}

FULL_DATASETS = (
    "stock_basic", "index_basic", "fund_basic", "etf_basic", "fut_basic", "opt_basic", "cb_basic",
    "sge_basic", "fx_obasic", "hk_basic", "us_basic", "factor_list", "cn_gdp", "anns_d",
)


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

    def manager(self, runner=None, *, client_factory=None):
        if client_factory is None:
            client_factory = lambda token: object()
            runner = runner or complete
        manager = JobManager(self.url, client_factory=client_factory, runner=runner)
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

    def test_due_categories_share_a_run_and_preserve_individual_results_on_restart(self):
        entered, release = Event(), Event()
        runs = []

        def runner(*args, **kwargs):
            datasets = args[2]
            runs.append(datasets)
            progress = kwargs["progress"]
            for dataset in datasets:
                progress({"event": "dataset_started", "dataset_id": dataset})
            entered.set()
            if not release.wait(4):
                raise AssertionError("the scheduled test did not release the job")
            for dataset in datasets:
                succeeded = dataset != "index_daily"
                progress({"event": "dataset_finished", "dataset_id": dataset,
                          "status": "complete" if succeeded else "failed", "planned": 1,
                          "completed": int(succeeded), "failed": int(not succeeded)})
            return {"status": "partial"}

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
        for category in ("股票数据", "指数专题"):
            self.assertIn(during[category]["last_status"], ACTIVE)
            self.assertFalse(during[category]["queued"])
        self.assertEqual(len(runs), 1)
        release.set()
        self.await_state(first, lambda state: state["job"]["status"] == "partial")
        self.assertEqual(len(first.state()["history"]), 1)
        self.assertCountEqual(runs[0], ["daily", "weekly", "index_daily"])
        plans = self.plans(first)
        self.assertEqual(plans["股票数据"]["last_status"], "complete")
        self.assertEqual(plans["股票数据"]["progress"]["partitions_done"], 2)
        self.assertEqual(plans["指数专题"]["last_status"], "failed")
        self.assertEqual(plans["指数专题"]["progress"]["partitions_failed"], 1)
        self.assertTrue(all(plan["last_target"] for plan in plans.values()))
        first.close()
        restored = self.manager(runner)
        restored.set_token("synthetic-scheduling-token")
        restored._check_schedule()
        self.assertIsNone(restored.state()["job"])
        self.assertEqual(len(runs), 1)
        for category, plan in self.plans(restored).items():
            self.assertEqual((plan["last_target"], plan["last_status"]),
                             (plans[category]["last_target"], plans[category]["last_status"]))

    def test_full_job_runs_all_categories_and_resumes_while_one_source_is_stalled(self):
        profiles = snapshot_profiles(*FULL_DATASETS)
        categories = {profile["category"] for profile in profiles.values()}
        self.assertGreaterEqual(len(categories), 12)
        barrier = Barrier(len(profiles), timeout=5)
        stalled = "anns_d"
        release = Event()
        lock = Lock()
        calls = []

        class Client:
            def query(self, profile, params, *, cancelled):
                dataset = profile["id"]
                with lock:
                    calls.append(dataset)
                    first_run = not release.is_set()
                if first_run:
                    barrier.wait()
                if dataset == stalled and first_run:
                    while not release.wait(0.02):
                        if cancelled():
                            raise IngestCancelled("source released by cancellation")
                return []

        with patch("financedb.ops.load_profiles", return_value=profiles):
            manager = self.manager(client_factory=lambda token: Client())
            self.addCleanup(release.set)
            manager.set_token("synthetic-full-token")
            manager.start_job(mode="history")
            state = self.await_state(manager, lambda state: sum(group["status"] == "complete" for group in
                                                               state["job"]["categories"]) == len(categories) - 1)
            groups = {group["name"]: group for group in state["job"]["categories"]}
            active = groups[profiles[stalled]["category"]]
            self.assertEqual((active["status"], active["current_dataset"]), ("running", stalled))
            self.assertEqual(state["full_job"]["id"], state["job"]["id"])
            self.assertCountEqual(calls, profiles.keys())
            manager.stop_job()
            self.await_state(manager, lambda state: state["job"]["status"] == "cancelled")
            self.assertEqual(manager.store.list_partitions(stalled), [])
            release.set()
            manager.close()

            resumed = self.manager(client_factory=lambda token: Client())
            resumed.set_token("synthetic-full-token")
            resumed.start_job(mode="history")
            final = self.await_state(resumed, lambda state: state["job"]["status"] == "complete")["job"]

        self.assertCountEqual(calls, [*profiles, stalled])
        for item in final["datasets"]:
            self.assertEqual((item["completed"], item["skipped"]),
                             (1, 0) if item["dataset_id"] == stalled else (0, 1))

    def test_slow_shared_dependency_does_not_hold_independent_data_in_its_category(self):
        source = "stock_basic"
        independent = "fund_share"
        dependent = {"fund_portfolio", "index_daily"}
        profiles = snapshot_profiles(source, independent, *sorted(dependent))
        for dataset in dependent:
            profiles[dataset]["request"] = {"strategy": "code_snapshot", "code_source": source}
        entered, release = Event(), Event()
        lock = Lock()
        calls = []

        class Client:
            def query(self, profile, params, *, cancelled):
                dataset = profile["id"]
                with lock:
                    calls.append((dataset, params))
                if dataset == source:
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError("source fixture was not released")
                    return [{"ts_code": "000001.SZ"}]
                return []

        with patch("financedb.ops.load_profiles", return_value=profiles):
            manager = self.manager(client_factory=lambda token: Client())
            self.addCleanup(release.set)
            manager.set_token("synthetic-dependencies-token")
            manager.start_job(mode="history")
            self.assertTrue(entered.wait(2))
            self.await_state(manager, lambda state: any(item["dataset_id"] == independent and item["status"] == "complete"
                                                       for item in state["job"]["datasets"]))
            self.assertCountEqual(calls, [(source, {}), (independent, {})])
            release.set()
            self.await_state(manager, lambda state: state["job"]["status"] == "complete")

        self.assertCountEqual(calls, [(source, {}), (independent, {}),
                                    *((dataset, {"ts_code": "000001.SZ"}) for dataset in dependent)])

    def test_simultaneous_categories_share_the_account_budget_and_report_separate_request_counts(self):
        profiles = snapshot_profiles("stock_basic", "etf_basic", "cn_gdp")
        budget_wait, release = Event(), Event()
        calls = []

        def respond(request):
            calls.append(json.loads(request.content)["api_name"])
            return httpx.Response(200, json={"code": 0, "data": {"fields": [], "items": []}})

        def wait_for_budget(seconds, cancelled):
            budget_wait.set()
            if not release.wait(5) or not cancelled():
                raise AssertionError("the category waiting for the account budget was not stopped")
            raise IngestCancelled("fixture stopped")

        def client_factory(token):
            return TushareClient(token, workers=1, limiter=RateLimiter(2), transport=httpx.MockTransport(respond))

        with patch("financedb.ops.load_profiles", return_value=profiles), patch("financedb.ingest._wait", wait_for_budget):
            manager = self.manager(client_factory=client_factory)
            self.addCleanup(release.set)
            manager.set_token("synthetic-shared-budget-token")
            manager.start_job(mode="history")
            self.assertTrue(budget_wait.wait(3))
            self.await_state(manager, lambda state: sum(item["status"] == "complete" for item in
                                                         state["job"]["datasets"]) == 2)
            manager.stop_job()
            release.set()
            final = self.await_state(manager, lambda state: state["job"]["status"] == "cancelled")["job"]

        self.assertEqual(len(calls), 2)
        for dataset in final["datasets"]:
            api_name = profiles[dataset["dataset_id"]]["api_name"]
            self.assertEqual(dataset["metrics"]["requests"], calls.count(api_name))

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


def snapshot_profiles(*datasets):
    registry = load_profiles()
    return {dataset: registry[dataset] | {"request": {"strategy": "snapshot"}} for dataset in datasets}


def complete(store, profiles, datasets, *, progress, **kwargs):
    for dataset in datasets:
        progress({"event": "dataset_started", "dataset_id": dataset})
        progress({"event": "dataset_finished", "dataset_id": dataset, "status": "complete", "planned": 1, "completed": 1})
    return {"status": "complete"}


if __name__ == "__main__":
    unittest.main()
