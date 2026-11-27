from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.ops import ACTIVE, JobManager
from financedb.web import create_app


ORIGIN = "http://127.0.0.1:8765"
HEADERS = {"Origin": ORIGIN}
ADMIN = {"username": "history-operator", "password": "fixture-history-password"}
MEMBER = {"username": "history-reader", "password": "fixture-reader-password"}
BACKFILL_ENDPOINT = "/ops/jobs/history-backfill"
CATALOG_ENDPOINT = "/ops/history-datasets"


def table_paths(nodes, parents=()):
    for node in nodes:
        path = (*parents, node["title"])
        if "children" in node:
            yield from table_paths(node["children"], path)
        else:
            yield node["dataset_id"], path


class LeapDay(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2028, 2, 29, 12, tzinfo=tz)


class RecentYearBackfillTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database_url = f"sqlite:///{Path(directory.name, 'finance.sqlite3')}"
        self.accounts_path = str(Path(directory.name, "accounts.sqlite3"))
        self.profiles = load_profiles()
        daily = self.profiles["daily"]
        stock = self.profiles["stock_basic"]
        self.daily = daily | {"request": {"strategy": "date", "date_param": "trade_date"}}
        self.stock = stock | {"request": {"strategy": "snapshot"}}

    def manager(self, query, *, profiles=None, runner=None):
        class Client:
            def query(self, profile, params, *, cancelled=None):
                return query(profile["id"], params)

        with patch("financedb.ops.load_profiles", return_value=self.profiles if profiles is None else profiles):
            manager = JobManager(self.database_url, client_factory=lambda _: Client(), runner=runner)
        self.addCleanup(manager.close)
        manager.set_token("fixture-history-token")
        return manager

    def browser(self, manager):
        return TestClient(create_app(self.database_url, job_manager=manager, accounts_path=self.accounts_path),
                          base_url=ORIGIN, client=("127.0.0.1", 49000))

    def register(self, browser, account=ADMIN):
        response = browser.post("/auth/register", headers=HEADERS, json=account)
        self.assertEqual(response.status_code, 200, response.text)

    def completed_job(self, browser, started):
        self.assertEqual(started.status_code, 202, started.text)
        job_id = started.json()["job"]["id"]
        deadline = monotonic() + 7
        while monotonic() < deadline:
            state = browser.get("/ops/state").json()
            job = state["job"]
            if job and job["id"] == job_id and job["status"] not in ACTIVE:
                self.assertEqual(job["status"], "complete", job)
                return state
            sleep(0.03)
        self.fail("历史任务未结束")

    def test_default_job_uses_calendar_year_and_keeps_old_rows_and_snapshots(self):
        requests = []
        runner_options = {}

        def query(dataset_id, params):
            requests.append((dataset_id, params))
            if dataset_id == "stock_basic":
                return [{"ts_code": "000001.SZ", "list_date": "19910403"}]
            if params["trade_date"] == "20280228":
                return [{"ts_code": "000001.SZ", "trade_date": "20280228", "close": "16"}]
            return []

        def runner(store, profiles, datasets, **options):
            runner_options.update(options)
            return run_ingestion(store, profiles, datasets, **options)

        with patch("financedb.ops.datetime", LeapDay):
            manager = self.manager(query, profiles={"daily": self.daily, "stock_basic": self.stock}, runner=runner)
            old_row = {"ts_code": "000001.SZ", "trade_date": "20110104", "close": "10"}
            manager.store.apply_business_rows(self.daily, [old_row])
            with self.browser(manager) as browser:
                self.register(browser)
                state = self.completed_job(browser, browser.post("/ops/jobs", headers=HEADERS, json={"mode": "history"}))
                full_job = state["full_job"]
                self.assertTrue(full_job["is_full"])
                self.assertEqual((full_job["start"], full_job["end"]), ("2027-02-28", "2028-02-28"))
                self.assertEqual(set(item["dataset_id"] for item in full_job["datasets"]), {"stock_basic", "daily"})
                self.assertEqual((runner_options["start"], runner_options["end"]), (date(2027, 2, 28), date(2028, 2, 28)))
                self.assertTrue(runner_options["recent_history"])
                dates = {params["trade_date"] for dataset_id, params in requests if dataset_id == "daily"}
                self.assertEqual((min(dates), max(dates)), ("20270228", "20280228"))
                self.assertIn(("stock_basic", {}), requests)
                self.assertEqual(manager.store.query_rows(self.daily, start="2011-01-04", end="2011-01-04",
                                                          fields=list(old_row))["rows"], [old_row])
                self.assertEqual(manager.store.query_rows(self.stock, fields=["ts_code", "list_date"])["rows"],
                                 [{"ts_code": "000001.SZ", "list_date": "19910403"}])

    def test_table_catalog_and_backfill_validate_permissions_and_parameters(self):
        def query(dataset_id, params):
            self.fail(f"无效操作触发采集 {dataset_id} {params}")

        manager = self.manager(query)
        body = {"dataset_id": "daily", "start": "2020-01-02", "end": "2020-01-03"}
        with self.browser(manager) as browser:
            self.assertEqual(browser.get(CATALOG_ENDPOINT).status_code, 401)
            self.assertEqual(browser.post(BACKFILL_ENDPOINT, headers=HEADERS, json=body).status_code, 401)
            self.register(browser)
            result = browser.get(CATALOG_ENDPOINT)
            self.assertEqual(result.status_code, 200, result.text)
            paths = dict(table_paths(result.json()["tree"]))
            self.assertEqual(paths["daily"], ("股票数据", "行情数据", "历史日线"))
            self.assertEqual(paths["factor_value"], ("量化因子库", "因子数据"))
            self.assertTrue({"stock_basic", "factor_list", "stk_mins", "fut_tick"}.isdisjoint(paths))

            wrong_origin = browser.post(BACKFILL_ENDPOINT, headers={"Origin": "https://elsewhere.test"}, json=body)
            self.assertEqual(wrong_origin.status_code, 403)
            for invalid in ({"dataset_id": "daily", "start": "2020-01-02"},
                            body | {"start": "2020-01-04"}, body | {"end": "2099-01-01"},
                            body | {"start": "2020-02-30"}, body | {"dataset_id": "stock_basic"},
                            body | {"dataset_id": "fut_tick"}, body | {"dataset_id": "stk_mins"},
                            body | {"dataset_id": "unknown_fixture"}):
                with self.subTest(body=invalid):
                    result = browser.post(BACKFILL_ENDPOINT, headers=HEADERS, json=invalid)
                    self.assertEqual(result.status_code, 422, result.text)
            self.assertIsNone(browser.get("/ops/state").json()["job"])

            self.register(browser, MEMBER)
            self.assertEqual(browser.get(CATALOG_ENDPOINT).status_code, 403)
            self.assertEqual(browser.post(BACKFILL_ENDPOINT, headers=HEADERS, json=body).status_code, 403)

    def test_single_table_backfill_reuses_checkpoints_and_keeps_progress_after_restart(self):
        calls = []
        factor = self.profiles["factor_value"]
        profiles = {"daily": self.daily, "stock_basic": self.stock, "factor_value": factor}

        def query(dataset_id, params):
            calls.append((dataset_id, params))
            if dataset_id == "stock_basic":
                return [{"ts_code": "000001.SZ"}]
            if dataset_id == "factor_value":
                return [{"ts_code": "000001.SZ", "factor_name": "fixture_factor",
                         "trade_date": "20120104", "factor_value": "0.5"}]
            return []

        manager = self.manager(query, profiles=profiles)
        body = {"dataset_id": "factor_value", "start": "2012-01-03", "end": "2012-01-05"}
        with self.browser(manager) as browser:
            self.register(browser)
            state = self.completed_job(browser, browser.post(BACKFILL_ENDPOINT, headers=HEADERS, json=body))
            job = state["history_backfill"]
            self.assertEqual(job["id"], state["job"]["id"])
            self.assertEqual(job["trigger"], "history_backfill")
            for key, value in body.items():
                self.assertEqual(job[key], value)
            self.assertEqual(job["datasets_total"], 1)
            self.assertEqual([dataset["dataset_id"] for dataset in job["datasets"]], ["factor_value"])
            self.assertEqual(calls, [("stock_basic", {}), ("factor_value", {"ts_code": "000001.SZ",
                                                                          "start_date": "20120103", "end_date": "20120105"})])
            self.assertEqual(manager.store.row_count("factor_value"), 1)
            second = self.completed_job(browser, browser.post(BACKFILL_ENDPOINT, headers=HEADERS, json=body))
            self.assertEqual(second["history_backfill"]["datasets"][0]["skipped"], 1)
            self.assertEqual(len(calls), 2)

            latest_id = second["history_backfill"]["id"]
            unrelated = browser.post("/ops/jobs", headers=HEADERS, json={"mode": "sample", "datasets": ["daily"],
                                                                        "start": "2012-01-03", "end": "2012-01-03"})
            self.assertEqual(self.completed_job(browser, unrelated)["history_backfill"]["id"], latest_id)

        restored = self.manager(query, profiles=profiles)
        with self.browser(restored) as browser:
            self.assertEqual(browser.post("/auth/login", headers=HEADERS, json=ADMIN).status_code, 200)
            state = browser.get("/ops/state").json()
            self.assertEqual(state["history_backfill"]["id"], latest_id)
            self.assertEqual(state["history_backfill"]["datasets"][0]["skipped"], 1)
