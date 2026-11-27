from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from time import monotonic, sleep
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from financedb.audit import compare_category, repair_category
from financedb.catalog import load_profiles
from financedb.ingest import SHANGHAI, IngestCancelled, run_ingestion
from financedb.ops import JobManager
from financedb.storage import Store
from financedb.web import create_app


ORIGIN = "http://127.0.0.1:8765"
HEADERS = {"Origin": ORIGIN}


def day_stamp():
    return (datetime.now(SHANGHAI).date() - timedelta(days=1)).strftime("%Y%m%d")


def position(code, close):
    return {"ts_code": code, "trade_date": day_stamp(), "close": close}


class FixtureClient:
    def __init__(self, rows):
        self.rows = rows

    def query(self, profile, params, *, cancelled=None):
        return self.rows if params.get("trade_date") == day_stamp() else []


class DataAuditTest(unittest.TestCase):
    def setUp(self):
        source = load_profiles()["daily"]
        self.profile = source | {"request": source["request"] | {"incremental_days": 1, "incremental_include_today": False}}
        self.profiles = {"daily": self.profile}
        self.targets = ["daily"]

    def observe(self, store, rows):
        events = []
        status, plan = compare_category(store, self.profiles, self.targets, self.targets,
                                        client=FixtureClient(rows), cancelled=lambda: False, notify=events.append)
        return status, plan, events[-1]

    def test_difference_drift_and_cap_leave_existing_rows_untouched(self):
        store = Store("sqlite://")
        self.addCleanup(store.close)
        store.initialize()
        store.commit_partition(self.profile, "prior", {}, [position("000001.SZ", "10")])
        status, plan, audit = self.observe(store, [position("000001.SZ", "15")])
        self.assertEqual((status, audit["changed"], audit["missing"]), ("complete", 1, 0))
        result = repair_category(store, self.profiles, plan, client=FixtureClient([position("000001.SZ", "20")]),
                                 cancelled=lambda: False, notify=lambda _: None)
        self.assertEqual((result["written"], result["failed"]), (0, 1))
        self.assertIn("差异已变化", result["message"])
        self.assertEqual(store.query_rows(self.profile)["rows"][0]["close"], "10")

        disappeared = repair_category(store, self.profiles, plan, client=FixtureClient([]),
                                      cancelled=lambda: False, notify=lambda _: None)
        self.assertEqual((disappeared["written"], disappeared["skipped"], disappeared["failed"]), (0, 0, 1))
        self.assertIn("上游记录已变化", disappeared["message"])

        self.profiles["daily"] = self.profile | {"row_limit": 1, "request": self.profile["request"] | {"cap_split": []}}
        status, plan, audit = self.observe(store, [position("000001.SZ", "15")])
        self.assertEqual((status, audit["checked_partitions"], audit["failed_partitions"], plan), ("failed", 0, 1, []))
        self.assertEqual(len(store.list_partitions("daily")), 1)

    def test_dependency_catalog_is_verified_without_writing_it_or_seeking_its_approval(self):
        store = Store("sqlite://")
        self.addCleanup(store.close)
        store.initialize()
        stock = load_profiles()["stock_basic"] | {"request": {"strategy": "snapshot"}}
        daily = self.profile | {"request": {"strategy": "code_window", "code_source": "stock_basic",
                                             "window_days": 1, "incremental_days": 1}}

        class Client:
            def query(self, profile, params, *, cancelled=None):
                if profile["id"] == "stock_basic":
                    return [{"ts_code": "000001.SZ"}]
                if params.get("ts_code") == "000001.SZ" and params.get("start_date") == day_stamp():
                    return [position("000001.SZ", "10")]
                return []

        events = []
        status, plan = compare_category(store, {"stock_basic": stock, "daily": daily}, ["daily", "stock_basic"], ["daily"],
                                        client=Client(), cancelled=lambda: False, notify=events.append)
        self.assertEqual((status, events[-1]["missing"]), ("complete", 1))
        self.assertEqual([item["dataset_id"] for item in plan], ["daily"])
        self.assertEqual(store.row_count("stock_basic"), 0)
        self.assertEqual(store.list_partitions("stock_basic"), [])

    def wait_for(self, browser, predicate):
        deadline = monotonic() + 5
        while monotonic() < deadline:
            state = browser.get("/ops/state").json()
            if predicate(state):
                return state
            sleep(0.04)
        self.fail("走查未进入预期状态")

    def test_admin_reviews_then_repairs_exact_delta_after_restart(self):
        with TemporaryDirectory() as directory:
            url = f"sqlite:///{directory}/business.sqlite3"
            accounts = str(Path(directory, "accounts.sqlite3"))
            local = [position("000001.SZ", "10"), position("000003.SZ", "30")]
            rows = [position("000001.SZ", "15"), position("000002.SZ", "20")]
            category = self.profile["category"]
            admin = {"username": "audit-operator", "password": "fixture-audit-password"}
            factory = lambda token: FixtureClient(rows)
            with patch("financedb.ops.load_profiles", return_value=self.profiles):
                manager = JobManager(url, client_factory=factory)
                manager.set_token("audit-fixture-token", remember=True)
                run_ingestion(manager.store, self.profiles, self.targets, client=FixtureClient(local), mode="incremental")
                existing = manager.store.list_partitions("daily")
                with TestClient(create_app(url, job_manager=manager, accounts_path=accounts), base_url=ORIGIN,
                                client=("127.0.0.1", 49000)) as browser:
                    self.assertEqual(browser.post("/ops/data-audits", json={"category": category},
                                                  headers=HEADERS).status_code, 401)
                    self.assertEqual(browser.post("/auth/register", json=admin, headers=HEADERS).status_code, 200)
                    remote = browser.post("/ops/data-audits", json={"category": category}, headers={"Origin": "https://elsewhere.test"})
                    self.assertEqual(remote.status_code, 403)
                    started = browser.post("/ops/data-audits", json={"category": category}, headers=HEADERS)
                    self.assertEqual(started.status_code, 202, started.text)
                    audit = self.wait_for(browser, lambda state: state["data_audit"]["status"] == "complete")["data_audit"]
                    self.assertEqual((audit["missing"], audit["changed"], audit["source_rows"]), (1, 1, 2))
                    self.assertNotIn("plan", audit)
                    self.assertNotIn("digest", str(audit))
                    self.assertTrue(audit["can_repair"])
                    self.assertEqual(manager.store.query_rows(self.profile)["rows"], local)
                    self.assertEqual(manager.store.list_partitions("daily"), existing)
                    audit_id = audit["id"]

                restored = JobManager(url, client_factory=factory)
                with TestClient(create_app(url, job_manager=restored, accounts_path=accounts), base_url=ORIGIN,
                                client=("127.0.0.1", 49000)) as browser:
                    self.assertEqual(browser.post("/auth/login", json=admin, headers=HEADERS).status_code, 200)
                    endpoint = f"/ops/data-audits/{audit_id}/repair"
                    reply = browser.post(endpoint, headers=HEADERS)
                    self.assertEqual(reply.status_code, 202, reply.text)
                    repaired = self.wait_for(browser, lambda state: state["data_audit"]["status"] == "repaired")["data_audit"]
                    self.assertEqual(repaired["repair"]["written"], 2)
                    self.assertFalse(repaired["can_repair"])
                    self.assertEqual(restored.store.query_rows(self.profile)["rows"], [*rows, local[1]])
                    self.assertEqual(restored.store.list_partitions("daily"), existing)
                    self.assertEqual(browser.post(endpoint, headers=HEADERS).status_code, 409)

    def test_audit_can_be_cancelled_without_writing_source_rows(self):
        with TemporaryDirectory() as directory:
            entered = Event()

            class SlowClient:
                def query(self, profile, params, *, cancelled):
                    entered.set()
                    deadline = monotonic() + 3
                    while not cancelled() and monotonic() < deadline:
                        sleep(0.02)
                    raise IngestCancelled("fixture cancelled")

            with patch("financedb.ops.load_profiles", return_value=self.profiles):
                manager = JobManager(f"sqlite:///{directory}/business.sqlite3", client_factory=lambda token: SlowClient())
                try:
                    manager.set_token("audit-fixture-token")
                    audit = manager.start_data_audit(category=self.profile["category"])
                    self.assertTrue(entered.wait(2))
                    manager.stop_data_audit(audit["id"])
                    deadline = monotonic() + 4
                    while manager.state()["data_audit"]["status"] != "cancelled" and monotonic() < deadline:
                        sleep(0.04)
                    self.assertEqual(manager.state()["data_audit"]["status"], "cancelled")
                    self.assertEqual(manager.store.row_count("daily"), 0)
                    self.assertEqual(manager.store.list_partitions("daily"), [])
                finally:
                    manager.close()
