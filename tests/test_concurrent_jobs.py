from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, Event
from time import monotonic, sleep
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from financedb.catalog import load_profiles
from financedb.ops import ACTIVE, DEFAULT_SCHEDULE, LOCAL_ZONE, JobManager
from financedb.web import create_app


ORIGIN = "http://127.0.0.1:8765"
ADMIN = {"username": "concurrent-admin", "password": "fixture-concurrency-password"}
BACKFILL = {"dataset_id": "daily", "start": "2026-10-12", "end": "2026-10-13"}
CATEGORY = "股票数据"


class ControlledRunner:
    def __init__(self):
        self.entered = {mode: Event() for mode in ("history", "incremental")}
        self.release = {mode: Event() for mode in self.entered}

    def __call__(self, store, profiles, datasets, *, mode, progress, cancelled, **kwargs):
        dataset, = datasets
        rows = 100 if mode == "history" else 2
        progress({"event": "dataset_started", "dataset_id": dataset})
        progress({"event": "partition_progress", "dataset_id": dataset, "planned": 2, "completed": 1, "rows": rows})
        self.entered[mode].set()
        deadline = monotonic() + 10
        while not cancelled() and monotonic() < deadline:
            if self.release[mode].wait(0.02):
                break
        status = "cancelled" if cancelled() else "complete" if self.release[mode].is_set() else "failed"
        progress({"event": "dataset_finished", "dataset_id": dataset, "status": status})
        return {"status": status}

    def release_all(self):
        for event in self.release.values():
            event.set()


class ConcurrentJobsTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        daily = load_profiles()["daily"]
        self.profiles = {"daily": daily | {"request": {"strategy": "date", "date_param": "trade_date"}}}
        self.enterContext(patch.dict("os.environ", {"TUSHARE_TOKEN": ""}))
        self.enterContext(patch("financedb.ops.load_profiles", return_value=self.profiles))

    def manager(self, runner, name, *, configure_token=True, client_factory=None):
        url = f"sqlite:///{self.directory / (name + '.sqlite3')}"
        manager = JobManager(url, client_factory=client_factory or (lambda _: object()), runner=runner)
        self.addCleanup(manager.close)
        if runner is not None:
            self.addCleanup(runner.release_all)
        if configure_token:
            manager.set_token("fixture-job-token")
        return manager, url

    def browser(self, manager, url):
        browser = self.enterContext(TestClient(
            create_app(url, job_manager=manager, accounts_path=str(self.directory / "accounts.sqlite3")),
            base_url=ORIGIN, headers={"Origin": ORIGIN}, client=("127.0.0.1", 49000),
        ))
        registered = browser.post("/auth/register", json=ADMIN)
        self.assertEqual(registered.status_code, 200, registered.text)
        return browser

    def state_until(self, get_state, predicate):
        deadline = monotonic() + 5
        while monotonic() < deadline:
            state = get_state()
            if predicate(state):
                return state
            sleep(0.02)
        self.fail(f"Expected job state was not reached: {get_state()}")

    def test_manual_incremental_and_full_offline_have_independent_progress_and_stop(self):
        runner = ControlledRunner()
        manager, url = self.manager(runner, "manual")
        browser = self.browser(manager, url)

        started = browser.post("/ops/jobs", json={"mode": "history"})
        self.assertEqual(started.status_code, 202, started.text)
        offline_id = started.json()["full_job"]["id"]
        self.assertTrue(runner.entered["history"].wait(3))

        started = browser.post("/ops/category-jobs", json={"category": CATEGORY})
        self.assertEqual(started.status_code, 202, started.text)
        incremental_id = started.json()["incremental_job"]["id"]
        self.assertTrue(runner.entered["incremental"].wait(3))

        def state():
            return browser.get("/ops/state").json()

        active = state()
        self.assertCountEqual([job["id"] for job in active["active_jobs"]], [offline_id, incremental_id])
        self.assertEqual(active["full_job"]["datasets"][0]["rows"], 100)
        self.assertEqual(active["incremental_job"]["datasets"][0]["rows"], 2)
        category = next(plan for plan in active["category_schedules"] if plan["category"] == CATEGORY)
        self.assertEqual(category["progress"]["rows"], 2)
        for path, body in (("/ops/jobs", {"mode": "history"}), ("/ops/category-jobs", {"category": CATEGORY}),
                           ("/ops/jobs/history-backfill", BACKFILL)):
            response = browser.post(path, json=body)
            self.assertEqual(response.status_code, 409, response.text)
        ambiguous = browser.post("/ops/jobs/stop")
        self.assertEqual(ambiguous.status_code, 409, ambiguous.text)

        stopped = browser.post("/ops/jobs/stop", params={"job_id": offline_id})
        self.assertEqual(stopped.status_code, 200, stopped.text)
        after = self.state_until(state, lambda value: value["full_job"]["status"] == "cancelled")
        self.assertEqual([job["id"] for job in after["active_jobs"]], [incremental_id])
        self.assertEqual(after["incremental_job"]["status"], "running")
        stale = browser.post("/ops/jobs/stop", params={"job_id": offline_id})
        self.assertEqual(stale.status_code, 409, stale.text)

        runner.release["incremental"].set()
        final = self.state_until(state, lambda value: value["incremental_job"]["status"] == "complete")
        self.assertFalse(final["active_jobs"])
        self.assertEqual({entry["id"]: entry["status"] for entry in final["history"]},
                         {offline_id: "cancelled", incremental_id: "complete"})

    def test_scheduled_incremental_and_table_backfill_launch_in_either_order(self):
        class ScheduleClock(datetime):
            current = datetime(2026, 11, 28, 12, 0, tzinfo=LOCAL_ZONE)

            @classmethod
            def now(cls, tz=None):
                return cls.current.astimezone(tz) if tz else cls.current.replace(tzinfo=None)

        for backfill_first in (True, False):
            with self.subTest(backfill_first=backfill_first), patch("financedb.ops.datetime", ScheduleClock), patch(
                "financedb.ops.timestamp", new=lambda: ScheduleClock.current.isoformat(),
            ):
                ScheduleClock.current = datetime(2026, 11, 28, 12, 0, tzinfo=LOCAL_ZONE)
                runner = ControlledRunner()
                manager, _ = self.manager(runner, f"scheduled-{backfill_first}")
                manager.set_category_schedule(category=CATEGORY, enabled=True, time="12:10")
                if backfill_first:
                    backfill = manager.start_history_backfill(**BACKFILL)
                    self.assertTrue(runner.entered["history"].wait(3))

                ScheduleClock.current = datetime(2026, 11, 28, 12, 11, tzinfo=LOCAL_ZONE)
                manager._check_schedule()
                self.assertTrue(runner.entered["incremental"].wait(3))
                if not backfill_first:
                    backfill = manager.start_history_backfill(**BACKFILL)
                    self.assertTrue(runner.entered["history"].wait(3))

                manager._check_schedule()
                active = manager.state()
                self.assertEqual(len(active["active_jobs"]), 2)
                self.assertEqual(active["history_backfill"]["id"], backfill["id"])
                self.assertEqual(active["incremental_job"]["trigger"], "category_schedule")
                self.assertEqual(active["incremental_job"]["scheduled_categories"], {CATEGORY: "2026-11-28"})
                self.assertEqual(len(active["history"]), 2)

                runner.release["history"].set()
                completed = self.state_until(manager.state, lambda state: state["history_backfill"]["status"] == "complete")
                self.assertIn(completed["incremental_job"]["status"], ACTIVE)
                runner.release["incremental"].set()
                final = self.state_until(manager.state, lambda state: state["incremental_job"]["status"] == "complete")
                category = next(plan for plan in final["category_schedules"] if plan["category"] == CATEGORY)
                self.assertEqual((category["last_status"], category["last_target"]), ("complete", "2026-11-28"))
                manager.close()

    def test_overlapping_ingestion_writes_one_business_row_and_two_checkpoints(self):
        both_requests = Barrier(2, timeout=4)

        class Client:
            def query(self, profile, params, *, cancelled):
                both_requests.wait()
                return [{"ts_code": "000001.SZ", "trade_date": params["trade_date"], "close": "8.3"}]

        manager, _ = self.manager(None, "writes", client_factory=lambda _: Client())
        for mode in ("history", "incremental"):
            manager.start_job(mode=mode, datasets=["daily"], start=BACKFILL["start"], end=BACKFILL["start"])

        finished = self.state_until(manager.state, lambda state: not state["active_jobs"])
        self.assertEqual(finished["offline_job"]["status"], "complete")
        self.assertEqual(finished["incremental_job"]["status"], "complete")
        self.assertEqual(manager.store.row_count("daily"), 1)
        partitions = manager.store.list_partitions("daily")
        self.assertEqual(len(partitions), 2)
        self.assertTrue(all(item["status"] == "complete" and item["params"] == {"trade_date": "20261012"}
                            for item in partitions))

    def test_restart_marks_both_saved_jobs_interrupted(self):
        runner = ControlledRunner()
        original, _ = self.manager(runner, "restart", configure_token=False)
        fields = {"status": "running", "started_at": "2026-11-28T10:00:00+08:00", "finished_at": None,
                  "message": None, "category": None, "scheduled_categories": {}}
        incremental = fields | {"id": "previous-incremental", "mode": "incremental", "trigger": "schedule", "is_full": False}
        offline = fields | {"id": "previous-full", "mode": "history", "trigger": "manual", "is_full": True,
                            "recent_history": True, "start": "2025-11-28", "end": "2026-11-27"}
        original.store.write_operation("job_history", [incremental, offline])
        original.store.write_operation("schedule", DEFAULT_SCHEDULE | {"last_status": "running", "last_target": "2026-11-28"})
        original.close()

        restored, _ = self.manager(runner, "restart", configure_token=False)
        state = restored.state()
        self.assertFalse(state["active_jobs"])
        self.assertEqual({item["id"]: item["status"] for item in state["history"]},
                         {incremental["id"]: "interrupted", offline["id"]: "interrupted"})
        self.assertTrue(all(item["finished_at"] for item in state["history"]))
        self.assertEqual((state["full_job"]["id"], state["full_job"]["status"]), (offline["id"], "interrupted"))
        self.assertEqual(state["schedule"]["last_status"], "interrupted")
        self.assertIsNone(state["schedule"]["last_target"])
