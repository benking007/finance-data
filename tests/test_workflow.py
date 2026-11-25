import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from time import monotonic, sleep
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import select

from financedb.catalog import catalog_snapshot, load_profiles
from financedb.ingest import plan_partitions, run_ingestion
from financedb.ops import JobManager, runnable
from financedb.storage import Store, rows_table
from financedb.transfer import build_release, migrate
from financedb.web import create_app


ORIGIN = "http://127.0.0.1:8765"
ORIGIN_HEADER = {"Origin": ORIGIN}
ROW = {"ts_code": "000001.SZ", "trade_date": "20261124", "open": "10.2500", "close": "11.0200", "amount": "12003.12"}


class Client:
    def __init__(self, token):
        super().__init__()

    def query(self, profile, params, *, cancelled=None):
        trade_date = params["trade_date"]
        return [ROW] if trade_date == ROW["trade_date"] else []

    def close(self):
        pass


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database_url = f"sqlite:///{self.temp.name}/测试 私有.sqlite3"
        self.profile = load_profiles()["daily"]
        self.manager = JobManager(self.database_url, client_factory=Client)
        self.addCleanup(self.manager.close)

    def session(self, app):
        return TestClient(app, base_url=ORIGIN, client=("127.0.0.1", 49000))

    def test_official_inventory(self):
        snapshot = catalog_snapshot()
        self.assertEqual(len(snapshot["entries"]), 274)
        self.assertEqual(len({entry["doc_id"] for entry in snapshot["entries"]}), 274)
        self.assertEqual(len(snapshot["categories"]), 15)
        self.assertEqual(snapshot["configured_count"], 255)
        self.assertTrue(all(item["pending_review"] == 0 for item in snapshot["coverage"].values()))

        def ordered_leaves(nodes):
            for node in nodes:
                if "doc_id" in node:
                    yield node["doc_id"]
                else:
                    yield from ordered_leaves(node["children"])

        self.assertEqual(list(ordered_leaves(snapshot["tree"])), [entry["doc_id"] for entry in snapshot["entries"]])

    def test_fund_catalog_keeps_all_official_subsets_and_queryable_profiles(self):
        expected = ["基金列表", "基金管理人", "基金经理", "基金业绩基准", "基金规模", "基金净值", "基金分红", "基金持仓", "新发基金申报数据"]
        with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            response = browser.get("/v1/source-catalog")
            self.assertEqual(response.status_code, 200, response.text)
            snapshot = response.json()
            funds = [entry for entry in snapshot["entries"] if entry["category"] == "公募基金"]
            self.assertEqual([entry["title"] for entry in funds], expected)
            self.assertEqual(snapshot["coverage"]["公募基金"], {"total": 9, "configured": 9, "pending_review": 0, "other": 0})
            for entry in funds:
                for dataset in entry["profile_ids"]:
                    self.assertEqual(browser.get(f"/v1/datasets/{dataset}").status_code, 200, dataset)
                    self.assertEqual(browser.get(f"/v1/datasets/{dataset}/rows").status_code, 200, dataset)

    def test_api_reference_renders_and_describes_dataset_hierarchy(self):
        with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            page = browser.get("/docs")
            self.assertEqual(page.status_code, 200)
            self.assertIn("API 文档", page.text)
            self.assertIn('href="/operations"', page.text)
            self.assertIn('class="nav-item" href="/docs">', browser.get("/").text)
            self.assertEqual(browser.get("/assets/api-docs.js").status_code, 200)
            self.assertEqual(browser.get("/docs/openapi").status_code, 200)
            self.assertIn("/v1/datasets/{dataset_id}/rows", browser.get("/openapi.json").json()["paths"])

            contents = browser.get("/v1/api-reference")
            self.assertEqual(contents.status_code, 200, contents.text)
            metadata = contents.json()
            self.assertEqual(metadata["authentication"]["header"], "X-API-Key")
            self.assertFalse(metadata["authentication"]["required"])
            index = {item["id"]: item for item in metadata["datasets"]}
            self.assertEqual(index["daily"]["path"], ["股票数据", "行情数据", "历史日线"])
            self.assertEqual(index["fund_company"]["path"], ["公募基金", "基金管理人"])
            self.assertEqual(len([item for item in index.values() if item["category"] == "公募基金"]), 9)

            dataset = browser.get("/v1/api-reference/datasets/daily")
            self.assertEqual(dataset.status_code, 200, dataset.text)
            daily = dataset.json()
            self.assertIn("close", {item["name"] for item in daily["fields"]})
            operation = next(item for item in daily["operations"] if item["path"] == "/v1/datasets/daily/rows")
            params = {item["name"]: item for item in operation["parameters"]}
            self.assertTrue({"code", "start", "end", "limit", "cursor", "fields"} <= params.keys())
            self.assertEqual(params["limit"]["default"], 100)
            self.assertEqual(params["cursor"]["location"], "query")
            self.assertIn("next_cursor", {item["name"] for item in operation["response_fields"]})

    def test_history_plans_every_configured_dataset_without_dates(self):
        class InstrumentStore:
            def instrument_codes(self, dataset_id):
                return ["TEST", "801003.SI"]

            def instrument_values(self, dataset_id, field=None, *, event_date=None):
                return self.instrument_codes(dataset_id)

        profiles = {name: profile for name, profile in load_profiles().items() if runnable(profile)}
        self.assertGreater(len(profiles), 1)
        today = date(2026, 11, 25)
        for dataset_id, profile in profiles.items():
            with self.subTest(dataset=dataset_id):
                sizes = []
                first = next(plan_partitions(InstrumentStore(), profile, mode="history", today=today, report_size=sizes.append))
                if not profile["request"].get("date_code_source"):
                    self.assertGreater(sizes[0], 0)
                if profile.get("request", {}).get("history_snapshot"):
                    self.assertIsNone(first.start)

    def test_historical_constituents_use_each_dates_confirmed_membership_universe(self):
        profiles = load_profiles()
        responses = []

        class Upstream:
            def query(self, profile, params, *, cancelled=None):
                name, trade_date = profile["api_name"], params["trade_date"]
                board = "BK0001.DC" if trade_date == "20261123" else "BK0002.DC"
                responses.append((name, trade_date, params.get("ts_code")))
                if name == "dc_index":
                    if params["idx_type"] != "行业板块":
                        return []
                    return [{"ts_code": board, "trade_date": trade_date, "name": board, "idx_type": "行业板块"}]
                if params["ts_code"] == board:
                    return [{"ts_code": board, "trade_date": trade_date, "con_code": "000001.SZ", "name": "测试"}]
                self.fail("Board code from a different day was requested")

            fail = self.fail

        result = run_ingestion(self.manager.store, profiles, ["dc_member"], client=Upstream(), mode="history",
                               start="2026-11-23", end="2026-11-24", today="2026-11-25")
        self.assertEqual(result["status"], "complete", result)
        selected = [item for item in responses if item[0] == "dc_member"]
        self.assertEqual(selected, [("dc_member", "20261123", "BK0001.DC"), ("dc_member", "20261124", "BK0002.DC")])

    def test_full_job_posts_no_scope_and_uses_all_configured_datasets(self):
        calls = []

        def runner(store, profiles, datasets, **kwargs):
            calls.append((datasets, kwargs["mode"], kwargs["start"], kwargs["end"]))
            return {"status": "complete"}

        self.manager._runner = runner
        self.manager.set_token("synthetic-full-token")
        with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            response = browser.post("/ops/jobs", json={"mode": "history"}, headers=ORIGIN_HEADER)
            self.assertEqual(response.status_code, 202, response.text)
            stop_at = monotonic() + 5
            while not calls and monotonic() < stop_at:
                sleep(0.01)
            self.assertTrue(calls)
            datasets, mode, start, end = calls[0]
            self.assertEqual(set(datasets), {name for name, profile in self.manager.profiles.items() if runnable(profile)})
            self.assertEqual((mode, start, end), ("history", None, None))

    def test_category_http_controls_are_scoped_and_require_local_origin(self):
        self.manager.set_token("synthetic-category-token")
        self.manager._runner = lambda *args, **kwargs: {"status": "complete"}
        with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            plan = {"category": "股票数据", "enabled": False, "time": "18:15"}
            denied = browser.put("/ops/category-schedules", json=plan, headers={"Origin": "https://example.net"})
            self.assertEqual(denied.status_code, 403)
            saved = browser.put("/ops/category-schedules", json=plan, headers=ORIGIN_HEADER)
            self.assertEqual(saved.status_code, 200, saved.text)
            categories = {item["category"]: item for item in saved.json()["category_schedules"]}
            self.assertEqual(categories["股票数据"]["time"], "18:15")
            self.assertEqual(categories["ETF专题"]["time"], "21:30")
            job = browser.post("/ops/category-jobs", json={"category": "ETF专题"}, headers=ORIGIN_HEADER)
            self.assertEqual(job.status_code, 202, job.text)
            self.assertEqual(job.json()["job"]["category"], "ETF专题")
            self.assertEqual(job.json()["job"]["mode"], "incremental")

    def test_database_target_local_initialization_and_active_job_guard(self):
        started, release = Event(), Event()

        def runner(*args, **kwargs):
            started.set()
            release.wait(3)
            return {"status": "complete"}

        self.manager._runner = runner
        self.manager.set_token("synthetic-target-token")
        with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            expected = Path(self.temp.name, "测试 私有.sqlite3").resolve()
            initial = browser.get("/ops/state").json()["target"]
            self.assertEqual((initial["kind"], initial["status"]), ("sqlite", "ready"))
            self.assertEqual(Path(initial["sqlite_path"]), expected)
            forbidden = browser.post("/ops/database-target", json={"kind": "sqlite"}, headers={"Origin": "https://example.net"})
            self.assertEqual(forbidden.status_code, 403)
            result = browser.post("/ops/database-target", json={"kind": "sqlite"}, headers=ORIGIN_HEADER)
            self.assertEqual(result.status_code, 200, result.text)
            self.assertTrue(result.json()["has_token"])
            invalid = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": {"password": "fixture-password"}}, headers=ORIGIN_HEADER)
            self.assertEqual(invalid.status_code, 422)
            self.assertNotIn("fixture-password", invalid.text)
            try:
                job = browser.post("/ops/jobs", json={"mode": "sample", "datasets": ["daily"]}, headers=ORIGIN_HEADER)
                self.assertEqual(job.status_code, 202, job.text)
                self.assertTrue(started.wait(2))
                blocked = browser.post("/ops/database-target", json={"kind": "sqlite"}, headers=ORIGIN_HEADER)
                self.assertEqual(blocked.status_code, 409)
            finally:
                release.set()

    def test_database_target_switch_isolated_data_and_reconnect(self):
        mock_url = f"sqlite:///{self.temp.name}/simulated_mysql.sqlite3"
        password = "synthetic-mysql-@-password"
        mysql = {"host": "127.0.0.1", "port": 3306, "database": "finance", "username": "collector", "password": password}

        def simulated_mysql(url, **options):
            if url.password != password:
                raise RuntimeError("MySQL 登录失败")
            store = Store(mock_url)
            store.initialize()
            return store

        self.manager.store.commit_partition(self.profile, "local", {}, [ROW])
        with patch("financedb.targets.open_mysql_store", side_effect=simulated_mysql):
            with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
                invalid = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": mysql | {"password": "bad"}}, headers=ORIGIN_HEADER)
                self.assertEqual(invalid.status_code, 409, invalid.text)
                self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [ROW])
                saved = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": mysql}, headers=ORIGIN_HEADER)
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertEqual(saved.json()["target"]["kind"], "mysql")
                self.assertNotIn(password, saved.text)
                copied = ROW | {"close": "21.50"}
                self.manager.query_store().commit_partition(self.profile, "remote", {}, [copied])
                self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [copied])
                switched = browser.post("/ops/database-target", json={"kind": "sqlite"}, headers=ORIGIN_HEADER)
                self.assertEqual(switched.status_code, 200, switched.text)
                self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [ROW])
                saved = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": mysql}, headers=ORIGIN_HEADER)
                self.assertEqual(saved.status_code, 200, saved.text)
            self.assertNotIn(password.encode(), Path(self.temp.name, "测试 私有.sqlite3").read_bytes())
            with patch.dict(os.environ):
                os.environ.pop("FINANCEDB_MYSQL_PASSWORD", None)
                with self.session(create_app(self.database_url)) as browser:
                    self.assertEqual(browser.get("/ops/state").json()["target"]["status"], "needs_password")
                    self.assertEqual(browser.get("/v1/datasets/daily/rows").status_code, 503)
                    self.assertEqual(browser.get("/v1/api-reference/datasets/daily").status_code, 200)
                os.environ["FINANCEDB_MYSQL_PASSWORD"] = password
                with self.session(create_app(self.database_url)) as browser:
                    self.assertEqual(browser.get("/ops/state").json()["target"]["status"], "ready")
                    self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [copied])

    def test_private_ui_job_schedule_and_resume(self):
        app = create_app(self.database_url, job_manager=self.manager)
        with self.session(app) as browser:
            home = browser.get("/")
            self.assertEqual(home.status_code, 200)
            self.assertIn('href="/operations"', home.text)
            operations = browser.get("/operations")
            self.assertEqual(operations.status_code, 200)
            self.assertIn('id="operations-view"', operations.text)
            self.assertEqual(browser.get("/operations", headers={"Host": "example.net"}).status_code, 403)
            self.assertEqual(browser.get("/assets/app.js").status_code, 200)
            self.assertEqual(browser.get("/ops/state").status_code, 200)
            secret = "test-token-kept-in-memory"
            denied = browser.post("/ops/token", headers={"Origin": "https://example.net"}, json={"token": secret})
            self.assertIn(denied.status_code, (400, 403, 422))
            response = browser.post("/ops/token", headers=ORIGIN_HEADER, json={"token": secret})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn(secret, response.text)
            params = {"mode": "history", "datasets": ["daily"], "start": "2026-11-24", "end": "2026-11-24"}
            for step in range(2):
                response = browser.post("/ops/jobs", headers=ORIGIN_HEADER, json=params)
                self.assertEqual(response.status_code, 202, response.text)
                deadline = monotonic() + 5
                while monotonic() < deadline:
                    result = browser.get("/ops/state").json()
                    if result["job"]["status"] in {"complete", "failed", "partial", "cancelled"}:
                        break
                    sleep(0.05)
                self.assertEqual(result["job"]["status"], "complete", result)
                self.assertEqual(result["job"]["categories"][0]["partitions_done"], 1)
                self.assertEqual(result["job"]["datasets"][0]["skipped"], step)
                self.assertNotIn(secret, str(result))
            response = browser.get("/v1/datasets/daily/rows", params={"code": "000001.SZ", "start": "2026-11-24", "limit": 1})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["rows"], [ROW])
            schedule = browser.put("/ops/schedule", headers=ORIGIN_HEADER, json={"enabled": True, "time": "21:30"})
            self.assertEqual(schedule.status_code, 200, schedule.text)
            self.assertEqual(schedule.json()["schedule"]["timezone"], "Asia/Shanghai")
        self.assertTrue(self.manager.store.read_operation("schedule")["enabled"])
        self.assertNotIn(secret.encode(), Path(self.temp.name, "测试 私有.sqlite3").read_bytes())

    def test_operator_login_protects_operations_with_separate_session(self):
        password = "fixture-operator-password"
        query_key = "fixture-admin-only-for-query"
        settings = {
            "FINANCEDB_ADMIN_PASSWORD": password,
            "FINANCEDB_API_KEYS_JSON": '{"reader":{"key":"fixture-admin-only-for-query","datasets":["daily"],"admin":true}}',
        }
        with patch.dict(os.environ, settings), self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            login_page = browser.get("/operations")
            self.assertIn('id="admin-login-form"', login_page.text)
            self.assertEqual(login_page.headers["cache-control"], "no-store")
            self.assertTrue(browser.get("/app-config").json()["operations_login_required"])
            query_headers = {"X-API-Key": query_key}
            self.assertEqual(browser.get("/v1/datasets", headers=query_headers).status_code, 200)
            self.assertEqual(browser.get("/ops/state", headers=query_headers).status_code, 401)
            self.assertEqual(browser.post("/ops/token", json={"token": "fixture-tushare-token"},
                                          headers=ORIGIN_HEADER).status_code, 401)
            self.assertEqual(browser.post("/ops/admin/login", json={"password": password},
                                          headers={"Origin": "https://example.net"}).status_code, 403)
            self.assertEqual(browser.post("/ops/admin/login", json={"password": "incorrect"},
                                          headers=ORIGIN_HEADER).status_code, 401)
            session = browser.post("/ops/admin/login", json={"password": password}, headers=ORIGIN_HEADER)
            self.assertEqual(session.status_code, 204)
            self.assertIn("HttpOnly", session.headers["set-cookie"])
            self.assertIn("SameSite=strict", session.headers["set-cookie"])
            self.assertNotIn(password, str(session.headers))
            self.assertIn('id="operations-view"', browser.get("/operations").text)
            self.assertEqual(browser.get("/ops/state").status_code, 200)
            self.assertEqual(browser.get("/v1/datasets").status_code, 401)
            self.assertEqual(browser.post("/ops/admin/logout", headers=ORIGIN_HEADER).status_code, 204)
            self.assertEqual(browser.get("/ops/state").status_code, 401)
            self.assertIn('id="admin-login-form"', browser.get("/operations").text)

    def test_snapshot_migration_and_public_api_field_projection(self):
        self.manager.store.commit_partition(self.profile, "sample", {}, [ROW])
        clone = f"sqlite:///{self.temp.name}/clone.sqlite3"
        self.assertEqual(migrate(self.database_url, clone)["dataset_rows"]["rows"], 1)
        with self.assertRaises(ValueError):
            migrate(self.database_url, self.database_url)
        released = f"sqlite:///{self.temp.name}/release.sqlite3"
        self.assertEqual(build_release(self.database_url, released, {"daily": ["trade_date", "close"]}), {"daily": 1})
        read = Store(released, read_only=True)
        self.addCleanup(read.close)
        with read.engine.connect() as conn:
            row = conn.execute(select(rows_table)).mappings().one()
        self.assertEqual(row["instrument"], "")
        self.assertNotIn("amount", row["payload"])
        published = {"FINANCEDB_WEB_MODE": "published", "FINANCEDB_PUBLISH_DATABASE_URL": released,
                     "FINANCEDB_PUBLISHED_DATASETS": "daily", "FINANCEDB_PUBLISHED_FIELDS_JSON": '{"daily":["trade_date","close"]}',
                     "FINANCEDB_API_KEYS_JSON": '{"reader":{"key":"demo-api-key-for-contract-test","datasets":["daily"],"csv":false}}'}
        with patch.dict(os.environ, published), self.session(create_app(self.database_url)) as browser:
            self.assertEqual(browser.get("/operations").status_code, 404)
            self.assertEqual(browser.get("/ops/state").status_code, 404)
            self.assertEqual(browser.get("/v1/datasets").status_code, 401)
            response = browser.get("/v1/datasets/daily/rows", headers={"X-API-Key": "demo-api-key-for-contract-test"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["rows"], [{"trade_date": "20261124", "close": "11.0200"}])
            headers = {"X-API-Key": "demo-api-key-for-contract-test"}
            self.assertEqual(browser.get("/v1/api-reference").status_code, 401)
            reference = browser.get("/v1/api-reference", headers=headers)
            self.assertEqual(reference.status_code, 200, reference.text)
            self.assertEqual([item["id"] for item in reference.json()["datasets"]], ["daily"])
            self.assertNotIn(headers["X-API-Key"], reference.text)
            detail = browser.get("/v1/api-reference/datasets/daily", headers=headers).json()
            self.assertEqual({item["name"] for item in detail["fields"]}, {"trade_date", "close"})
            self.assertIsNone(detail.get("source_url"))
            rows = next(item for item in detail["operations"] if item["path"] == "/v1/datasets/daily/rows")
            self.assertNotIn("code", {param["name"] for param in rows["parameters"]})
            self.assertEqual(browser.get("/v1/api-reference/datasets/fund_company", headers=headers).status_code, 404)
        with patch.dict(os.environ, published | {"FINANCEDB_PUBLISH_DATABASE_URL": clone}):
            with self.assertRaisesRegex(RuntimeError, "发布清单"):
                with self.session(create_app(self.database_url)):
                    pass

    def test_due_schedule_waits_for_token_and_runs_once(self):
        self.manager.clear_token()
        self.manager.set_schedule(enabled=True, time="00:00", datasets=["daily"])
        with self.manager._lock:
            self.manager._schedule["enabled_since"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        self.manager._check_schedule()
        self.assertIsNone(self.manager.state()["job"])
        self.assertEqual(self.manager.state()["schedule"]["last_status"], "waiting_token")
        self.manager.set_token("synthetic-scheduled-token")
        self.manager._check_schedule()
        until = monotonic() + 5
        while monotonic() < until:
            state = self.manager.state()
            if state["schedule"]["last_status"] == "complete":
                break
            sleep(0.05)
        self.manager._check_schedule()
        state = self.manager.state()
        self.assertEqual(state["schedule"]["last_status"], "complete", state)
        self.assertEqual(state["job"]["trigger"], "schedule")
        self.assertEqual(state["job"]["mode"], "incremental")
        self.assertEqual(len(state["history"]), 1)

    def test_history_write_failure_leaves_no_active_job(self):
        self.manager.set_token("synthetic-start-token")
        with patch.object(self.manager.store, "write_operation", side_effect=OSError("unwritable test")):
            with self.assertRaises(OSError):
                self.manager.start_job(mode="sample", datasets=["daily"])
        self.assertIsNone(self.manager.state()["job"])
        self.assertFalse(self.manager.clear_token()["has_token"])

    def test_cursor_and_revision_deduplication(self):
        store = self.manager.store
        profiles = load_profiles()
        daily = {**profiles["daily"], "versioned": True}
        store.commit_partition(daily, "before", {}, [ROW, {**ROW, "trade_date": "20261123"}])
        store.commit_partition(daily, "after", {}, [{**ROW, "amount": "12"}])
        first = store.query_rows(daily, limit=1)
        self.assertEqual(first["rows"][0]["amount"], "12")
        self.assertEqual(store.query_rows(daily, limit=1, cursor=first["next_cursor"])["rows"][0]["trade_date"], "20261123")
        with self.assertRaises(ValueError):
            store.query_rows(daily, code="another instrument", cursor=first["next_cursor"])
        self.assertEqual(store.dataset_stats("daily")["row_count"], 2)


if __name__ == "__main__":
    unittest.main()
