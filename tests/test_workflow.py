import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from time import monotonic, sleep
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import event, inspect, select

from financedb.catalog import catalog_snapshot, load_profiles
from financedb.ingest import plan_partitions, run_ingestion
from financedb.ops import JobManager, runnable
from financedb.storage import Store, business_tables, revisions
from financedb.transfer import build_release, migrate
from financedb.web import create_app


ORIGIN = "http://127.0.0.1:8765"
ORIGIN_HEADER = {"Origin": ORIGIN}
ROW = {"ts_code": "000001.SZ", "trade_date": "20261124", "open": "10.2500", "close": "11.0200", "amount": "12003.12"}
BUSINESS_ROW = ROW | {"open": "10.25", "close": "11.02"}
ADMIN = {"username": "finance-admin", "password": "fixture-admin-password"}


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
        environment = patch.dict(os.environ, {"FINANCEDB_ACCOUNTS_PATH": str(Path(self.temp.name, "accounts.sqlite3"))})
        environment.start()
        self.addCleanup(environment.stop)
        self.database_url = f"sqlite:///{self.temp.name}/测试 私有.sqlite3"
        self.profile = load_profiles()["daily"]
        self.manager = JobManager(self.database_url, client_factory=Client)
        self.addCleanup(self.manager.close)

    @contextmanager
    def session(self, app, *, authenticated=True):
        with TestClient(app, base_url=ORIGIN, client=("127.0.0.1", 49000)) as browser:
            if authenticated and browser.get("/app-config").json()["mode"] == "local":
                login = browser.post("/auth/login", json=ADMIN, headers=ORIGIN_HEADER)
                if login.status_code == 401:
                    login = browser.post("/auth/register", json=ADMIN, headers=ORIGIN_HEADER)
                self.assertEqual(login.status_code, 200, login.text)
            yield browser

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

    def test_business_tables_store_indexed_fields_and_preserve_revisions(self):
        store = self.manager.store
        profiles = load_profiles()
        names = set(inspect(store.engine).get_table_names())
        self.assertEqual(set(profiles), set(business_tables))
        self.assertEqual(len(names), len(profiles) + 5)
        self.assertNotIn("dataset_rows", names)

        nav = {"ts_code": "000001.OF", "nav_date": "20261124", "ann_date": "20261125", "unit_nav": "1.02130000"}
        statement = {"ts_code": "000001.SZ", "end_date": "20260930", "report_type": "1", "comp_type": "1",
                     "f_ann_date": "20261031", "revenue": "248.1300000"}
        store.commit_partition(self.profile, "daily-day", {}, [ROW])
        store.commit_partition(profiles["fund_nav"], "nav-day", {}, [nav])
        store.commit_partition(profiles["income"], "income-new", {}, [statement])
        older = statement | {"f_ann_date": "20261030", "revenue": "200.10"}
        store.commit_partition(profiles["income"], "income-old", {}, [older])

        with store.engine.connect() as conn:
            daily = business_tables["daily"]
            self.assertEqual(conn.execute(select(daily.c.ts_code, daily.c.trade_date, daily.c.close)).one(),
                             ("000001.SZ", "20261124", "11.02"))
            self.assertEqual(conn.scalar(select(business_tables["fund_nav"].c.unit_nav)), "1.0213")
            self.assertEqual(conn.scalar(select(business_tables["income"].c.revenue)), "248.13")
            self.assertEqual(len(conn.execute(select(revisions).where(revisions.c.dataset_id == "income")).all()), 2)
            plan = conn.exec_driver_sql(
                "EXPLAIN QUERY PLAN SELECT close FROM daily WHERE ts_code = ? AND trade_date BETWEEN ? AND ?",
                ("000001.SZ", "20261101", "20261130"),
            ).one()
            self.assertIn("ix_daily_source_code", plan[3])
        self.assertEqual(store.query_rows(profiles["fund_nav"])["rows"], [nav | {"unit_nav": "1.0213"}])
        self.assertEqual(store.query_rows(profiles["income"])["rows"], [statement | {"revenue": "248.13"}])
        self.assertEqual(store.row_count("income"), 1)
        self.assertTrue(store.partition_done("income", "income-old"))

    def test_business_ingestion_and_queries_omit_legacy_payload(self):
        store = self.manager.store
        profiles = self.manager.profiles
        daily = business_tables["daily"]
        store.commit_partition(self.profile, "first", {}, [ROW])
        legacy = '{"close":"999","ts_code":"OLD"}'
        with store.engine.begin() as conn:
            conn.execute(daily.update().values(_payload=legacy))

        statements = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(store.engine, "before_cursor_execute", capture)
        try:
            store.commit_partition(self.profile, "next", {}, [ROW, ROW | {"trade_date": "20261125"}])
            self.assertEqual(store.query_rows(self.profile, limit=1, fields=["trade_date", "close"])["rows"],
                             [{"trade_date": "20261125", "close": "11.02"}])
            self.assertEqual(store.query_rows(self.profile, end="2026-11-24")["rows"], [BUSINESS_ROW])
            self.assertEqual(store.instrument_values("daily", "ts_code", event_date="20261124"), ["000001.SZ"])
            self.assertEqual(store.instrument_values("daily", "_payload"), [])

            stock = {"ts_code": "000001.SZ", "list_date": "20020101", "delist_date": "20261124"}
            store.commit_partition(profiles["stock_basic"], "listings", {}, [stock])
            self.assertEqual(store.instrument_lifetimes("stock_basic")["000001.SZ"], ("2002-01-01", "2026-11-24"))
            calendar = [
                {"exchange": "SSE", "cal_date": "20261124", "is_open": 1},
                {"exchange": "SSE", "cal_date": "20261125", "is_open": 0},
            ]
            store.commit_partition(profiles["trade_cal"], "calendar", {}, calendar)
            self.assertEqual(store.trading_days("2026-11-24", "2026-11-25", exchange="SSE"), ["2026-11-24"])
        finally:
            event.remove(store.engine, "before_cursor_execute", capture)
        self.assertTrue(statements)
        self.assertTrue(all("_payload" not in statement for statement in statements))
        with store.engine.connect() as conn:
            payloads = dict(conn.execute(select(daily.c.trade_date, daily.c._payload)).tuples().all())
        self.assertEqual(payloads, {"20261124": legacy, "20261125": ""})

    def test_unrepresentable_business_field_leaves_failed_partition(self):
        class OverflowClient(Client):
            def query(self, profile, params, *, cancelled=None):
                return [ROW | {"amount": "1.1234567890123456789"}]

        store = self.manager.store
        run = run_ingestion(store, self.manager.profiles, ["daily"], client=OverflowClient("fixture"),
                            mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-25")
        self.assertEqual(run["status"], "failed")
        self.assertEqual(store.row_count("daily"), 0)
        failure = store.list_partitions("daily")[0]
        self.assertEqual(failure["status"], "failed")
        self.assertIn("daily.amount", failure["message"])

    def test_index_basic_verifies_pagination_and_reuses_market_checkpoints(self):
        profile = self.manager.profiles["index_basic"]
        markets = {
            market: [{"ts_code": f"{idx:06d}.{market}", "market": market, "category": "主题指数"}
                     for idx in range(count)]
            for market, count in {"CSI": 7, "SSE": 1, "SZSE": 1, "SW": 1}.items()
        }

        class IndexClient:
            def __init__(self):
                self.calls = []

            def query(self, profile, params, *, cancelled=None):
                self.calls.append(params)
                start = params.get("offset", 0)
                return markets[params["market"]][start:start + profile["row_limit"]]

        client = IndexClient()
        active = profile | {"row_limit": 4}
        legacy = active | {"request": {"strategy": "snapshot", "dimensions": profile["request"]["dimensions"]}}
        options = {"client": client, "mode": "history", "today": "2026-11-26"}
        store = self.manager.store
        first = run_ingestion(store, {"index_basic": legacy}, ["index_basic"], **options)
        self.assertEqual(first["status"], "partial")
        self.assertEqual(store.row_count("index_basic"), 3)

        client.calls.clear()
        retry = run_ingestion(store, {"index_basic": active}, ["index_basic"], **options)
        self.assertEqual(retry["status"], "complete")
        self.assertEqual(retry["datasets"][0]["skipped"], 3)
        self.assertEqual(client.calls, [{"market": "CSI"}, {"market": "CSI", "offset": 2, "limit": 4},
                                        {"market": "CSI", "offset": 4, "limit": 4}])
        self.assertEqual(store.row_count("index_basic"), 10)
        client.calls.clear()
        self.assertEqual(run_ingestion(store, {"index_basic": active}, ["index_basic"], **options)["status"], "complete")
        self.assertFalse(client.calls)

    def test_index_basic_backfills_categories_but_rejects_pagination_of_only_capped_rows(self):
        profile = self.manager.profiles["index_basic"]
        categories = ["主题指数", "规模指数", "一级行业指数"]
        rows = [{"ts_code": f"{idx:06d}.CSI", "market": "CSI", "category": categories[idx % len(categories)]}
                for idx in range(7)]
        active = profile | {"row_limit": 4, "request": profile["request"] | {
            "dimensions": {"market": ["CSI"]}, "cap_category_values": categories[:2],
        }}

        class CappedFirstClient:
            def query(self, profile, params, *, cancelled=None):
                if "category" in params:
                    return [row for row in rows if row["category"] == params["category"]]
                return rows[:4][params.get("offset", 0):]

        store = self.manager.store
        retry = run_ingestion(store, {"index_basic": active}, ["index_basic"], client=CappedFirstClient(),
                              mode="history", today="2026-11-26")
        self.assertEqual(retry["status"], "partial")
        self.assertIn("整市场覆盖待核对", retry["datasets"][0]["message"])
        self.assertEqual(store.row_count("index_basic"), 7)
        marks = {item["params"].get("category"): item["status"] for item in store.list_partitions("index_basic")}
        self.assertEqual(marks, {None: "failed", **dict.fromkeys(categories, "complete")})

    def test_index_basic_discards_rows_outside_the_category_filter(self):
        profile = self.manager.profiles["index_basic"]
        active = profile | {"row_limit": 3, "request": profile["request"] | {
            "dimensions": {"market": ["CSI"]}, "cap_category_values": ["主题指数", "规模指数"],
        }}
        rows = [{"ts_code": f"{idx:06d}.CSI", "market": "CSI", "category": "主题指数"} for idx in range(3)]

        class IgnoredFiltersClient:
            def query(self, profile, params, *, cancelled=None):
                return rows[:1] if "category" in params else rows

        store = self.manager.store
        report = run_ingestion(store, {"index_basic": active}, ["index_basic"], client=IgnoredFiltersClient(),
                               mode="history", today="2026-11-26")
        self.assertEqual(report["status"], "failed")
        self.assertEqual(store.row_count("index_basic"), 0)
        failures = {item["params"].get("category"): item["message"] for item in store.list_partitions("index_basic")}
        self.assertIn("已出现的记录", failures["主题指数"])
        self.assertIn("筛选范围以外", failures["规模指数"])

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
            self.assertTrue(metadata["authentication"]["required"])
            self.assertEqual(metadata["limits"]["requests_per_day"], 3000)
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
                self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [BUSINESS_ROW])
                saved = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": mysql}, headers=ORIGIN_HEADER)
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertEqual(saved.json()["target"]["kind"], "mysql")
                self.assertTrue(saved.json()["target"]["password_saved"])
                self.assertNotIn(password, saved.text)
                copied = ROW | {"close": "21.50"}
                self.manager.query_store().commit_partition(self.profile, "remote", {}, [copied])
                self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [BUSINESS_ROW | {"close": "21.5"}])
                switched = browser.post("/ops/database-target", json={"kind": "sqlite"}, headers=ORIGIN_HEADER)
                self.assertEqual(switched.status_code, 200, switched.text)
                self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [BUSINESS_ROW])
                saved = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": mysql}, headers=ORIGIN_HEADER)
                self.assertEqual(saved.status_code, 200, saved.text)
            self.assertNotIn(password.encode(), Path(self.temp.name, "测试 私有.sqlite3").read_bytes())
            key_file = Path(self.temp.name, "测试 私有.sqlite3.credentials.key")
            self.assertEqual(key_file.stat().st_mode & 0o777, 0o600)
            with patch.dict(os.environ):
                os.environ.pop("FINANCEDB_MYSQL_PASSWORD", None)
                with self.session(create_app(self.database_url)) as browser:
                    self.assertEqual(browser.get("/ops/state").json()["target"]["status"], "ready")
                    self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [BUSINESS_ROW | {"close": "21.5"}])
                    transient = {key: value for key, value in mysql.items() if key != "password"}
                    transient["remember_password"] = False
                    cleared = browser.post("/ops/database-target", json={"kind": "mysql", "mysql": transient}, headers=ORIGIN_HEADER)
                    self.assertEqual(cleared.status_code, 200, cleared.text)
                    self.assertFalse(cleared.json()["target"]["password_saved"])
                with self.session(create_app(self.database_url)) as browser:
                    self.assertEqual(browser.get("/ops/state").json()["target"]["status"], "needs_password")
                    self.assertEqual(browser.get("/v1/datasets/daily/rows").status_code, 503)
                    self.assertEqual(browser.get("/v1/api-reference/datasets/daily").status_code, 200)
                os.environ["FINANCEDB_MYSQL_PASSWORD"] = password
                with self.session(create_app(self.database_url)) as browser:
                    self.assertEqual(browser.get("/ops/state").json()["target"]["status"], "ready")
                    self.assertEqual(browser.get("/v1/datasets/daily/rows").json()["rows"], [BUSINESS_ROW | {"close": "21.5"}])

    def test_interrupted_full_job_can_restart_with_remembered_token(self):
        token = "fixture-tushare-remembered-after-restart"
        seed = run_ingestion(self.manager.store, self.manager.profiles, ["daily"], client=Client(token), mode="history",
                             start="2026-11-24", end="2026-11-24", today="2026-11-25")
        self.assertEqual(seed["status"], "complete")
        self.manager.store.write_operation("job_history", [{
            "id": "interrupted-previous", "mode": "history", "status": "running", "is_full": True,
            "trigger": "manual", "started_at": "2026-11-25T00:00:00Z",
        }])
        with self.session(create_app(self.database_url, job_manager=self.manager)) as browser:
            response = browser.post("/ops/token", json={"token": token, "remember": True}, headers=ORIGIN_HEADER)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()["token_saved"])
            self.assertNotIn(token, response.text)
        self.assertNotIn(token.encode(), Path(self.temp.name, "测试 私有.sqlite3").read_bytes())

        def run_one_day(store, profiles, selected, **kwargs):
            self.assertIn("daily", selected)
            bounded = kwargs | {"start": "2026-11-24", "end": "2026-11-24", "today": "2026-11-25"}
            return run_ingestion(store, profiles, ["daily"], **bounded)

        with patch.dict(os.environ):
            os.environ.pop("TUSHARE_TOKEN", None)
            restarted = JobManager(self.database_url, client_factory=Client, runner=run_one_day)
            with self.session(create_app(self.database_url, job_manager=restarted)) as browser:
                state = browser.get("/ops/state").json()
                self.assertTrue(state["has_token"])
                self.assertEqual(state["full_job"]["status"], "interrupted")
                resumed = browser.post("/ops/jobs", json={"mode": "history"}, headers=ORIGIN_HEADER)
                self.assertEqual(resumed.status_code, 202, resumed.text)
                until = monotonic() + 5
                while monotonic() < until:
                    job = restarted.state()["job"]
                    if job["status"] == "complete":
                        break
                    sleep(0.05)
                self.assertEqual(job["status"], "complete", job)
                self.assertEqual(next(row for row in job["datasets"] if row["dataset_id"] == "daily")["skipped"], 1)
                cleared = browser.delete("/ops/token", headers=ORIGIN_HEADER)
                self.assertFalse(cleared.json()["token_saved"])
            with self.session(create_app(self.database_url)) as browser:
                self.assertFalse(browser.get("/ops/state").json()["has_token"])

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
            self.assertEqual(response.json()["rows"], [BUSINESS_ROW])
            schedule = browser.put("/ops/schedule", headers=ORIGIN_HEADER, json={"enabled": True, "time": "21:30"})
            self.assertEqual(schedule.status_code, 200, schedule.text)
            self.assertEqual(schedule.json()["schedule"]["timezone"], "Asia/Shanghai")
        self.assertTrue(self.manager.store.read_operation("schedule")["enabled"])
        self.assertNotIn(secret.encode(), Path(self.temp.name, "测试 私有.sqlite3").read_bytes())

    def test_account_roles_api_key_and_daily_limit_on_the_same_site(self):
        member = {"username": "finance-reader", "password": "fixture-reader-password"}
        with self.session(create_app(self.database_url, job_manager=self.manager), authenticated=False) as browser:
            self.assertEqual(browser.get("/operations", follow_redirects=False).headers["location"], "/login?next=operations")
            self.assertFalse(browser.get("/app-config").json()["operations"])
            self.assertEqual(browser.get("/ops/state").status_code, 401)
            self.assertEqual(browser.post("/auth/register", json=ADMIN, headers={"Origin": "https://example.net"}).status_code, 403)

            admin = browser.post("/auth/register", json=ADMIN, headers=ORIGIN_HEADER)
            self.assertEqual(admin.status_code, 200, admin.text)
            self.assertTrue(admin.json()["user"]["is_admin"])
            self.assertIn("HttpOnly", admin.headers["set-cookie"])
            self.assertIn("SameSite=strict", admin.headers["set-cookie"])
            self.assertTrue(browser.get("/app-config").json()["operations"])
            self.assertEqual(browser.get("/operations").headers["cache-control"], "no-store")
            self.assertEqual(browser.get("/ops/state").status_code, 200)
            first_key = browser.post("/account/api-key", headers=ORIGIN_HEADER).json()["api_key"]

            registered = browser.post("/auth/register", json=member, headers=ORIGIN_HEADER).json()["user"]
            self.assertFalse(registered["is_admin"])
            self.assertEqual(registered["daily_limit"], 3000)
            self.assertFalse(browser.get("/app-config").json()["operations"])
            for path in ("/operations", "/ops/state", "/admin/users", "/auth/admin/users"):
                self.assertEqual(browser.get(path).status_code, 403)
            self.assertEqual(browser.patch(f"/auth/admin/users/{registered['id']}", json={"daily_limit": 1}, headers=ORIGIN_HEADER).status_code, 403)
            self.assertEqual(browser.post("/ops/token", json={"token": "fixture-tushare-token"}, headers=ORIGIN_HEADER).status_code, 403)

            generated = browser.post("/account/api-key", headers=ORIGIN_HEADER).json()["api_key"]
            member_key = browser.post("/account/api-key", headers=ORIGIN_HEADER).json()["api_key"]
            self.assertNotEqual(generated, member_key)
            self.assertNotIn(member_key, browser.get("/auth/session").text)
            self.assertEqual(browser.get("/v1/datasets", headers={"X-API-Key": generated}).status_code, 401)
            self.assertEqual(browser.post("/auth/logout", headers=ORIGIN_HEADER).status_code, 204)
            self.assertEqual(browser.get("/ops/state", headers={"X-API-Key": first_key}).status_code, 401)

            signed_in = browser.post("/auth/login", json=ADMIN, headers=ORIGIN_HEADER)
            self.assertEqual(signed_in.status_code, 200, signed_in.text)
            self.assertEqual(len(browser.get("/auth/admin/users").json()["users"]), 2)
            limit = browser.patch(f"/auth/admin/users/{registered['id']}", json={"daily_limit": 2}, headers=ORIGIN_HEADER)
            self.assertEqual(limit.json()["user"]["daily_limit"], 2)
            self.assertEqual(browser.get("/v1/datasets", headers={"X-API-Key": member_key}).status_code, 200)
            self.assertEqual(browser.post("/auth/login", json=member, headers=ORIGIN_HEADER).status_code, 200)
            self.assertEqual(browser.get("/v1/api-reference").status_code, 200)
            exceeded = browser.get("/v1/datasets/daily/rows", headers={"X-API-Key": member_key})
            self.assertEqual(exceeded.status_code, 429)
            self.assertGreater(int(exceeded.headers["Retry-After"]), 0)
            self.assertEqual(browser.get("/auth/session").json()["user"]["remaining_today"], 0)

    def test_reverse_proxy_same_site_auth_enforces_origin(self):
        origin = "https://finance.example.test"
        with patch.dict(os.environ, {"FINANCEDB_SITE_ORIGIN": origin}):
            app = create_app(self.database_url, job_manager=self.manager)
            with TestClient(app, base_url=origin, client=("192.0.2.15", 49000)) as browser:
                self.assertEqual(browser.get("/app-config", headers={"Host": "other.example.test"}).status_code, 403)
                self.assertEqual(browser.post("/auth/register", json=ADMIN,
                                              headers={"Origin": "https://other.example.test"}).status_code, 403)
                signed_in = browser.post("/auth/register", json=ADMIN, headers={"Origin": origin})
                self.assertEqual(signed_in.status_code, 200, signed_in.text)
                self.assertIn("Secure", signed_in.headers["set-cookie"])
                self.assertEqual(browser.get("/operations").status_code, 200)
                self.assertEqual(browser.post("/account/api-key", headers={"Origin": origin}).status_code, 200)

    def test_snapshot_migration_and_public_api_field_projection(self):
        self.manager.store.commit_partition(self.profile, "sample", {}, [ROW])
        clone = f"sqlite:///{self.temp.name}/clone.sqlite3"
        self.assertEqual(migrate(self.database_url, clone)["daily"]["rows"], 1)
        with self.assertRaises(ValueError):
            migrate(self.database_url, self.database_url)
        released = f"sqlite:///{self.temp.name}/release.sqlite3"
        self.assertEqual(build_release(self.database_url, released, {"daily": ["trade_date", "close"]}), {"daily": 1})
        read = Store(released, read_only=True)
        self.addCleanup(read.close)
        with read.engine.connect() as conn:
            row = conn.execute(select(business_tables["daily"])).mappings().one()
        self.assertEqual(row["_instrument"], "")
        self.assertIsNone(row["amount"])
        self.assertEqual(row["_payload"], "")
        published = {"FINANCEDB_WEB_MODE": "published", "FINANCEDB_PUBLISH_DATABASE_URL": released,
                     "FINANCEDB_PUBLISHED_DATASETS": "daily", "FINANCEDB_PUBLISHED_FIELDS_JSON": '{"daily":["trade_date","close"]}',
                     "FINANCEDB_API_KEYS_JSON": '{"reader":{"key":"demo-api-key-for-contract-test","datasets":["daily"],"csv":false}}'}
        with patch.dict(os.environ, published), self.session(create_app(self.database_url)) as browser:
            self.assertEqual(browser.get("/operations").status_code, 404)
            self.assertEqual(browser.get("/ops/state").status_code, 404)
            self.assertEqual(browser.get("/v1/datasets").status_code, 401)
            response = browser.get("/v1/datasets/daily/rows", headers={"X-API-Key": "demo-api-key-for-contract-test"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["rows"], [{"trade_date": "20261124", "close": "11.02"}])
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
        with patch.dict(os.environ, published | {"FINANCEDB_API_KEYS_JSON": "{}"}), \
                self.session(create_app(self.database_url)) as browser:
            signup = browser.post("/auth/register", json=ADMIN, headers=ORIGIN_HEADER)
            self.assertTrue(signup.json()["user"]["is_admin"])
            self.assertEqual(browser.get("/operations").status_code, 404)
            key = browser.post("/account/api-key", headers=ORIGIN_HEADER).json()["api_key"]
            browser.post("/auth/logout", headers=ORIGIN_HEADER)
            headers = {"X-API-Key": key}
            self.assertEqual(browser.get("/v1/datasets/daily/rows", headers=headers).json()["rows"],
                             [{"trade_date": "20261124", "close": "11.02"}])
            self.assertEqual(browser.get("/v1/datasets/fund_company", headers=headers).status_code, 404)
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
