"""Read-only data API, browser workspace, and local operations routes."""
from __future__ import annotations

import csv
import hmac
import io
import ipaddress
import json
import logging
import os
import re
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, SecretStr
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from .accounts import (
    SESSION_SECONDS, AccountConflict, AccountForbidden, AccountNotFound, Accounts, InvalidCredentials, QuotaExceeded,
)
from .api_reference import (
    API_KEY_HEADER, API_TITLE, API_VERSION, MAX_QUERY_YEARS, dataset_entry, dataset_paths, reference_dataset, reference_index,
)
from .business_tables import business_tables
from .catalog import catalog_snapshot, load_profiles
from .storage import DEFAULT_DATABASE_URL, Store


STATIC = Path(__file__).with_name("static")
API_KEY = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)
LOCAL_HOSTS = {"127.0.0.1", "localhost", "[::1]"}
SESSION_COOKIE = "financedb_session"
AUDIT = logging.getLogger("uvicorn.error.financedb_api")


@dataclass(frozen=True)
class KeyPolicy:
    label: str
    key: str
    datasets: frozenset[str]
    csv: bool
    admin: bool
    requests_per_minute: int
    requests_per_day: int | None = None


def read_policies(value: str, known_datasets: set[str]) -> list[KeyPolicy]:
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        raise ValueError("FINANCEDB_API_KEYS_JSON 必须为 JSON 对象") from None
    if not isinstance(data, dict):
        raise ValueError("FINANCEDB_API_KEYS_JSON 必须为 JSON 对象")
    policies = []
    for label, item in data.items():
        if not isinstance(item, dict):
            raise ValueError("API 密钥配置必须使用对象")
        key = item.get("key")
        datasets = item.get("datasets", [])
        rate = item.get("requests_per_minute", 60)
        csv_allowed = item.get("csv", False)
        admin = item.get("admin", False)
        if not isinstance(key, str) or not key.isascii() or len(key) < 16 or not all(
            33 <= ord(character) <= 126 for character in key
        ):
            raise ValueError("每个 API 密钥须至少 16 位可见 ASCII 字符")
        if any(hmac.compare_digest(key, policy.key) for policy in policies):
            raise ValueError("API 密钥不得重复")
        if not isinstance(datasets, list) or not all(
            isinstance(dataset, str) and dataset in known_datasets for dataset in datasets
        ):
            raise ValueError("API 密钥的 datasets 必须列出已登记的数据集")
        if type(rate) is not int or not 1 <= rate <= 10000:
            raise ValueError("requests_per_minute 必须为 1 至 10000 的整数")
        if type(csv_allowed) is not bool or type(admin) is not bool:
            raise ValueError("API 密钥的 csv 和 admin 必须为布尔值")
        policies.append(KeyPolicy(label, key, frozenset(datasets), csv_allowed, admin, rate))
    return policies


class TokenInput(BaseModel):
    token: SecretStr = Field(min_length=1, max_length=512)
    remember: bool = Field(default=False, strict=True)


class AccountInput(BaseModel):
    username: str = Field(min_length=1, max_length=32)
    password: SecretStr = Field(min_length=1, max_length=256)


class DailyLimitInput(BaseModel):
    daily_limit: int = Field(ge=0, le=2147483647, strict=True)


class JobInput(BaseModel):
    mode: Literal["sample", "history", "incremental"]
    datasets: list[str] | None = Field(default=None, max_length=300)
    start: str | None = Field(default=None, max_length=10)
    end: str | None = Field(default=None, max_length=10)


class ScheduleInput(BaseModel):
    enabled: bool
    time: str = Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")


class CategoryJobInput(BaseModel):
    category: str = Field(min_length=1, max_length=40)


class CategoryScheduleInput(ScheduleInput, CategoryJobInput):
    pass


class MySQLTargetInput(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=3306, ge=1, le=65535, strict=True)
    database: str = Field(min_length=1, max_length=64)
    username: str = Field(min_length=1, max_length=128)
    password: SecretStr | None = Field(default=None, max_length=1024)
    remember_password: bool = Field(default=True, strict=True)
    tls: bool = Field(default=False, strict=True)


class DatabaseTargetInput(BaseModel):
    kind: Literal["sqlite", "mysql"]
    mysql: MySQLTargetInput | None = None


def require_loopback(request: Request) -> None:
    host = request.headers.get("host", "")
    matched = re.fullmatch(r"(localhost|127\.0\.0\.1|\[::1\])(?::\d+)?", host)
    try:
        local = ipaddress.ip_address(request.client.host).is_loopback if request.client else False
    except ValueError:
        local = False
    if not local or not matched or matched[1] not in LOCAL_HOSTS:
        raise HTTPException(403, "本地服务仅接受环回连接")


def fields_for(profile: dict) -> list[dict]:
    specification = profile.get("fields") or []
    field_info = profile.get("field_info") or {}
    if isinstance(specification, dict):
        specification = [{"name": name, **(meta if isinstance(meta, dict) else {"description": str(meta)})}
                         for name, meta in specification.items()]
    fields = []
    for field in specification:
        if isinstance(field, str):
            field = {"name": field, **field_info.get(field, {})}
        if isinstance(field, dict):
            name = field.get("name") or field.get("field")
            if name:
                fields.append({"name": name, "type": field.get("type"), "unit": field.get("unit"),
                               "description": field.get("description") or field.get("desc")})
    return fields


def parse_date_filter(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return date.fromisoformat(value)
        if re.fullmatch(r"\d{8}", value):
            return date(int(value[:4]), int(value[4:6]), int(value[6:]))
        if re.fullmatch(r"\d{6}", value):
            return date(int(value[:4]), int(value[4:]), 1)
        if re.fullmatch(r"\d{4}[Qq][1-4]", value):
            return date(int(value[:4]), 1 + 3 * (int(value[5]) - 1), 1)
    except ValueError:
        pass
    raise HTTPException(422, "日期应为 YYYY-MM-DD、YYYYMMDD、YYYYMM 或 YYYYQn")


def csv_cell(value):
    if isinstance(value, (list, dict)):
        value = json.dumps(value, ensure_ascii=False)
    if isinstance(value, str) and (value.startswith(("\t", "\r", "\n")) or value.lstrip().startswith(("=", "+", "-", "@"))):
        try:
            float(value)
        except ValueError:
            return "'" + value
    return value


def create_app(database_url: str | None = None, *, job_manager=None, accounts_path=None) -> FastAPI:
    profiles = load_profiles()
    mode = os.getenv("FINANCEDB_WEB_MODE", "local").strip().lower()
    if mode not in {"local", "published"}:
        raise ValueError("FINANCEDB_WEB_MODE 必须为 local 或 published")
    published = mode == "published"
    site_origin = os.getenv("FINANCEDB_SITE_ORIGIN", "").strip().rstrip("/")
    site_url = urlsplit(site_origin)
    if site_origin and (site_url.scheme not in {"http", "https"} or not site_url.hostname or site_url.username
                        or site_url.path or site_url.query or site_url.fragment):
        raise ValueError("FINANCEDB_SITE_ORIGIN 必须是站点域名及协议，例如 https://finance.example.com")
    accounts = Accounts(accounts_path)
    private_url = database_url or os.getenv("FINANCEDB_DATABASE_URL") or DEFAULT_DATABASE_URL
    private_address = make_url(private_url)
    demo = (not published and private_address.get_backend_name() == "sqlite"
            and "demo" in Path(private_address.database or "").name.lower())
    fixed_store = None
    configured = os.getenv("FINANCEDB_PUBLISHED_DATASETS", "")
    allowed_datasets = {value.strip() for value in configured.split(",") if value.strip()}
    published_fields: dict[str, list[str]] = {}
    policies = read_policies(os.getenv("FINANCEDB_API_KEYS_JSON", "{}"), set(profiles))
    if published:
        publish_url = os.getenv("FINANCEDB_PUBLISH_DATABASE_URL")
        if not publish_url or publish_url == private_url:
            raise ValueError("发布服务必须配置独立的 FINANCEDB_PUBLISH_DATABASE_URL")
        if not allowed_datasets or not allowed_datasets <= profiles.keys():
            raise ValueError("发布服务必须配置有效的 FINANCEDB_PUBLISHED_DATASETS")
        try:
            published_fields = json.loads(os.getenv("FINANCEDB_PUBLISHED_FIELDS_JSON", "{}"))
        except json.JSONDecodeError:
            raise ValueError("FINANCEDB_PUBLISHED_FIELDS_JSON 必须为 JSON 对象") from None
        if not isinstance(published_fields, dict) or set(published_fields) != allowed_datasets:
            raise ValueError("必须为每个开放数据集配置对外字段")
        for dataset_id, names in published_fields.items():
            known = {field["name"] for field in fields_for(profiles[dataset_id])}
            if not isinstance(names, list) or not names or not all(
                isinstance(name, str) and name in known for name in names
            ) or len(names) != len(set(names)):
                raise ValueError("对外字段须使用该数据集的唯一登记字段")
        fixed_store = Store(publish_url, read_only=True)
        private_store = Store(private_url, read_only=True)
        try:
            if fixed_store.identity == private_store.identity:
                raise ValueError("发布服务的数据库必须与采集库物理隔离")
        finally:
            private_store.close()
    else:
        allowed_datasets = set(profiles)
        if demo:
            fixed_store = Store(private_url)
    source_inventory = catalog_snapshot(profiles)
    api_paths = dataset_paths(source_inventory)
    directory = None if published else source_inventory
    coverage = directory["coverage"] if directory else {}

    def require_site(request: Request) -> None:
        if site_origin:
            if request.headers.get("host", "").lower() != site_url.netloc.lower():
                raise HTTPException(403, "请使用配置的本站域名访问")
        elif not published:
            require_loopback(request)

    def require_browser_origin(request: Request) -> None:
        require_site(request)
        if request.headers.get("origin") != (site_origin or str(request.base_url).rstrip("/")):
            raise HTTPException(403, "请从本站页面提交操作")

    def secure_cookie(request: Request) -> bool:
        return (site_url.scheme or request.url.scheme) == "https"

    def session_token(request: Request) -> str:
        return request.cookies.get(SESSION_COOKIE, "")

    def require_user(request: Request, *, admin=False) -> dict:
        require_site(request)
        user = accounts.session_user(session_token(request))
        if user is None:
            raise HTTPException(401, "请先登录")
        if admin and not user["is_admin"]:
            raise HTTPException(403, "仅管理员可以访问")
        return user

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if published:
            fixed_store.validate_schema()
            manifest = fixed_store.read_operation("release_manifest", {})
            released_fields = manifest.get("fields", {})
            if manifest.get("type") != "financedb-release-v1" or any(
                not set(names) <= set(released_fields.get(dataset_id, ()))
                for dataset_id, names in published_fields.items()
            ):
                raise RuntimeError("发布库缺少完整的发布清单，或配置超出已发布的字段范围")
        elif demo:
            fixed_store.initialize()
            app.state.jobs = None
        elif job_manager is None:
            from .ops import JobManager

            app.state.jobs = JobManager(private_url)
        else:
            app.state.jobs = job_manager
        try:
            yield
        finally:
            if fixed_store is not None:
                fixed_store.close()
            else:
                close = getattr(app.state.jobs, "close", None)
                if close:
                    close()

    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        description="按数据集查询已入库记录。发布模式使用 X-API-Key 请求头。",
        lifespan=lifespan,
        docs_url="/docs/openapi",
        redoc_url=None,
        swagger_ui_parameters={"tryItOutEnabled": False},
    )

    @app.middleware("http")
    async def response_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        if request.url.path in {"/operations", "/account", "/admin/users", "/login", "/register", "/app-config"} or request.url.path.startswith(("/v1/", "/ops/", "/auth/", "/account/")):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error: RequestValidationError):
        if request.url.path.startswith("/ops/"):
            return JSONResponse(status_code=422, content={"detail": "操作参数格式有误"})
        if request.url.path.startswith(("/auth/", "/account/")):
            return JSONResponse(status_code=422, content={"detail": "用户名、密码或设置格式有误"})
        return JSONResponse(status_code=422, content={"detail": "查询参数格式有误"})

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request: Request, error: SQLAlchemyError):
        return JSONResponse(status_code=503, content={"detail": "数据库暂时无法查询"})

    request_lock = threading.Lock()
    usage: dict[str, deque] = defaultdict(deque)
    credential_usage: dict[str, deque] = {}

    def limit_credential_attempts(request: Request):
        host = request.client.host if request.client else "unknown"
        now = time.monotonic()
        with request_lock:
            if len(credential_usage) >= 4096:
                for address, attempts in list(credential_usage.items()):
                    if not attempts or now - attempts[-1] >= 60:
                        del credential_usage[address]
            if host not in credential_usage and len(credential_usage) >= 4096:
                raise HTTPException(429, "请求过多，请稍后重试", headers={"Retry-After": "60"})
            attempts = credential_usage.setdefault(host, deque())
            while attempts and now - attempts[0] >= 60:
                attempts.popleft()
            if len(attempts) >= 12:
                wait = max(1, int(61 - (now - attempts[0])))
                raise HTTPException(429, "登录或注册尝试过多，请稍后重试", headers={"Retry-After": str(wait)})
            attempts.append(now)

    def resolve_access(request: Request, key: str | None) -> KeyPolicy:
        require_site(request)
        policy = next(
            (candidate for candidate in policies if key and key.isascii()
             and hmac.compare_digest(candidate.key, key)),
            None,
        )
        if policy is not None:
            return policy
        try:
            user = accounts.api_user(key, consume=True) if key else accounts.session_user(session_token(request), consume=True)
        except QuotaExceeded as error:
            raise HTTPException(429, "已达到今日 API 调用上限，北京时间 00:00 重置",
                                headers={"Retry-After": str(error.retry_after)}) from None
        if user is not None:
            return KeyPolicy(f"user:{user['id']}", "", frozenset(allowed_datasets), False, user["is_admin"], 0, user["daily_limit"])
        raise HTTPException(401, "请先登录或使用有效的 API 密钥", headers={"WWW-Authenticate": API_KEY_HEADER})

    def access(request: Request, key: str | None = Security(API_KEY)) -> KeyPolicy:
        policy = resolve_access(request, key)
        if not policy.key:
            return policy
        now = time.monotonic()
        with request_lock:
            history = usage[policy.label]
            while history and now - history[0] >= 60:
                history.popleft()
            if len(history) >= policy.requests_per_minute:
                wait = max(1, int(61 - (now - history[0])))
                raise HTTPException(429, "请求频率超限", headers={"Retry-After": str(wait)})
            history.append(now)
        return policy

    def reference_access(request: Request, key: str | None = Security(API_KEY)) -> KeyPolicy:
        return resolve_access(request, key)

    def accessible_profile(dataset_id: str, policy: KeyPolicy) -> dict:
        if dataset_id not in policy.datasets or dataset_id not in allowed_datasets:
            raise HTTPException(404, "数据集不存在或未开放")
        return profiles[dataset_id]

    def active_store(request: Request) -> Store:
        if fixed_store is not None:
            return fixed_store
        try:
            return request.app.state.jobs.query_store()
        except RuntimeError:
            raise HTTPException(503, "目标数据库未连接，请在数据运营页面初始化") from None

    def describe(dataset_id: str, profile: dict, stats: dict, policy: KeyPolicy) -> dict:
        columns = published_fields.get(dataset_id)
        result = {
            "id": dataset_id,
            "title": profile.get("title") or dataset_id,
            "category": profile.get("category") or "其他",
            "description": profile.get("description") or "",
            "mode": profile.get("mode") or "",
            "history_start": profile.get("history_start"),
            "date_field": profile.get("date_field") if columns is None or profile.get("date_field") in columns else None,
            "code_field": profile.get("code_field") if columns is None or profile.get("code_field") in columns else None,
            "row_count": stats.get("row_count", 0),
            "queryable": True,
            "min_date": stats.get("min_date"),
            "max_date": stats.get("max_date"),
            "last_sync": stats.get("last_sync"),
            "can_export": policy.csv,
            "complete_partitions": stats.get("complete_partitions", 0),
            "pending_partitions": stats.get("pending_partitions", 0),
        }
        if not published:
            result["source_url"] = profile.get("doc_url") or profile.get("source_url")
            if dataset_id in business_tables:
                result["storage_table"] = business_tables[dataset_id].name
        elif not result["date_field"]:
            result["min_date"] = None
            result["max_date"] = None
        return result

    def static_page(filename: str) -> FileResponse:
        return FileResponse(STATIC / filename, headers={
            "Cache-Control": "no-cache",
            "Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'",
        })

    @app.get("/", include_in_schema=False)
    def interface(request: Request):
        require_site(request)
        return static_page("index.html")

    if not published:
        @app.get("/operations", include_in_schema=False)
        def operations_page(request: Request):
            require_site(request)
            if accounts.session_user(session_token(request)) is None:
                return RedirectResponse("/login?next=operations", status_code=303)
            require_user(request, admin=True)
            return static_page("index.html")

    @app.get("/docs", include_in_schema=False)
    def reference_page(request: Request):
        require_site(request)
        return static_page("api-docs.html")

    app.mount("/assets", StaticFiles(directory=STATIC), name="assets")

    @app.get("/login", include_in_schema=False)
    @app.get("/register", include_in_schema=False)
    def auth_page(request: Request):
        require_site(request)
        return static_page("account.html")

    @app.get("/account", include_in_schema=False)
    @app.get("/admin/users", include_in_schema=False)
    def account_page(request: Request):
        require_site(request)
        user = accounts.session_user(session_token(request))
        if user is None:
            return RedirectResponse("/login", status_code=303)
        if request.url.path == "/admin/users" and not user["is_admin"]:
            raise HTTPException(403, "仅管理员可以访问")
        return static_page("account.html")

    @app.get("/auth/session", include_in_schema=False)
    def account_session(request: Request):
        require_site(request)
        user = accounts.session_user(session_token(request))
        return {"user": user, "registration_enabled": True, "operations": bool(user and user["is_admin"] and not published)}

    def issue_session(request, authenticate, form):
        require_browser_origin(request)
        limit_credential_attempts(request)
        try:
            user, token = authenticate(form.username, form.password.get_secret_value())
        except AccountConflict as error:
            raise HTTPException(409, str(error)) from None
        except InvalidCredentials as error:
            raise HTTPException(401, str(error)) from None
        except ValueError as error:
            raise HTTPException(422, str(error)) from None
        accounts.logout(session_token(request))
        response = JSONResponse({"user": user})
        response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_SECONDS, path="/", httponly=True,
                            secure=secure_cookie(request), samesite="strict")
        return response

    @app.post("/auth/register", include_in_schema=False)
    def register(form: AccountInput, request: Request):
        return issue_session(request, accounts.register, form)

    @app.post("/auth/login", include_in_schema=False)
    def login(form: AccountInput, request: Request):
        return issue_session(request, accounts.login, form)

    @app.post("/auth/logout", status_code=204, include_in_schema=False)
    def logout(request: Request):
        require_browser_origin(request)
        accounts.logout(session_token(request))
        response = Response(status_code=204)
        response.delete_cookie(SESSION_COOKIE, path="/", httponly=True, secure=secure_cookie(request), samesite="strict")
        return response

    @app.post("/account/api-key", include_in_schema=False)
    def create_api_key(request: Request):
        require_browser_origin(request)
        user = require_user(request)
        key, updated = accounts.generate_api_key(user["id"])
        return {"api_key": key, "user": updated}

    @app.get("/auth/admin/users", include_in_schema=False)
    def list_users(request: Request):
        user = require_user(request, admin=True)
        return {"users": accounts.list_users(user["id"])}

    @app.patch("/auth/admin/users/{user_id}", include_in_schema=False)
    def set_user_limit(user_id: int, form: DailyLimitInput, request: Request):
        require_browser_origin(request)
        user = require_user(request, admin=True)
        try:
            updated = accounts.set_daily_limit(user["id"], user_id, form.daily_limit)
        except AccountNotFound as error:
            raise HTTPException(404, str(error)) from None
        except AccountForbidden as error:
            raise HTTPException(403, str(error)) from None
        return {"user": updated}

    @app.get("/app-config", include_in_schema=False)
    def app_config(request: Request):
        state = account_session(request)
        user = state["user"]
        return {"mode": mode, "demo": demo, "api_key_required": user is None, "operations": state["operations"],
                "user": {name: user[name] for name in ("id", "username", "is_admin")} if user else None}

    def reference_entry(dataset_id: str, profile: dict, policy: KeyPolicy) -> dict:
        return dataset_entry(profile, api_paths.get(dataset_id), columns=published_fields.get(dataset_id),
                             csv_allowed=policy.csv, show_source=not published and policy.admin)

    @app.get("/v1/api-reference", summary="查看当前权限下的 API 文档索引", tags=["API 参考"])
    def api_reference(policy: KeyPolicy = Depends(reference_access)):
        allowed = allowed_datasets & policy.datasets
        entries = [reference_entry(dataset_id, profile, policy)
                   for dataset_id, profile in profiles.items() if dataset_id in allowed]
        return reference_index(entries, auth_required=True, daily_limit=policy.requests_per_day,
                               rate_limit=policy.requests_per_minute if policy.key else None, local=not published,
                               admin=not published and policy.admin, csv_allowed=policy.csv)

    @app.get("/v1/api-reference/datasets/{dataset_id}", summary="查看数据集的调用参数和字段文档", tags=["API 参考"])
    def api_reference_dataset(dataset_id: str, policy: KeyPolicy = Depends(reference_access)):
        profile = accessible_profile(dataset_id, policy)
        columns = published_fields.get(dataset_id)
        fields = [field for field in fields_for(profile) if columns is None or field["name"] in columns]
        return reference_dataset(reference_entry(dataset_id, profile, policy), fields,
                                 local=not published, admin=not published and policy.admin)

    @app.get("/v1/datasets", summary="查看已开放的数据目录", tags=["目录与定义"])
    def datasets(policy: KeyPolicy = Depends(access), store: Store = Depends(active_store)):
        stats = {entry["dataset_id"]: entry for entry in store.list_stats()}
        return {"datasets": [
            describe(dataset_id, profile, stats.get(dataset_id, {}), policy)
            for dataset_id, profile in profiles.items() if dataset_id in allowed_datasets & policy.datasets
        ]}

    @app.get("/v1/datasets/{dataset_id}", summary="查看数据定义与覆盖", tags=["目录与定义"])
    def dataset(dataset_id: str, policy: KeyPolicy = Depends(access), store: Store = Depends(active_store)):
        profile = accessible_profile(dataset_id, policy)
        stats = store.dataset_stats(dataset_id)
        columns = published_fields.get(dataset_id)
        return {
            **describe(dataset_id, profile, stats, policy),
            "fields": [field for field in fields_for(profile) if columns is None or field["name"] in columns],
            "key": [field for field in profile.get("key", []) if columns is None or field in columns],
        }

    @app.get("/v1/datasets/{dataset_id}/rows", summary="筛选并分页读取已入库数据", tags=["数据查询"])
    def query_rows(
        dataset_id: str,
        code: str | None = Query(default=None, max_length=40, pattern=r"^[\w.\-]+$"),
        start: str | None = Query(default=None, max_length=10),
        end: str | None = Query(default=None, max_length=10),
        fields: str | None = Query(default=None, max_length=4000),
        cursor: str | None = Query(default=None, max_length=1000),
        limit: int = Query(default=100, ge=1, le=1000),
        file_format: Literal["json", "csv"] = Query(default="json", alias="format"),
        policy: KeyPolicy = Depends(access),
        store: Store = Depends(active_store),
    ):
        profile = accessible_profile(dataset_id, policy)
        columns = published_fields.get(dataset_id)
        if code and (not profile.get("code_field") or columns is not None and profile.get("code_field") not in columns):
            raise HTTPException(422, "该数据集不支持指定证券代码")
        if (start or end) and (not profile.get("date_field") or columns is not None and profile.get("date_field") not in columns):
            raise HTTPException(422, "该数据集不支持日期筛选")
        first = parse_date_filter(start)
        last = parse_date_filter(end)
        if first and last and (last < first or (last - first).days > MAX_QUERY_YEARS * 366):
            raise HTTPException(422, f"请使用正确的日期顺序，日期跨度最多 {MAX_QUERY_YEARS} 年")
        if file_format == "csv" and not policy.csv:
            raise HTTPException(403, "此 API 密钥未授权 CSV 导出")
        known_fields = columns or [item["name"] for item in fields_for(profile)]
        picked = list(dict.fromkeys(value.strip() for value in fields.split(",") if value.strip())) if fields else []
        if fields and (not picked or not set(picked) <= set(known_fields)):
            raise HTTPException(422, "fields 包含未登记的列")
        try:
            result = store.query_rows(profile, code=code, start=first, end=last, cursor=cursor, limit=limit, fields=picked or columns)
        except (ValueError, TypeError):
            raise HTTPException(422, "筛选参数或游标无效") from None
        rows = result["rows"]
        if published:
            rows = [{name: row.get(name) for name in picked or known_fields} for row in rows]
            AUDIT.info("Data query key=%s dataset=%s rows=%d format=%s", policy.label, dataset_id, len(rows), file_format)
        elif picked:
            rows = [{name: row.get(name) for name in picked} for row in rows]
        columns = picked or list(dict.fromkeys([*known_fields, *(key for row in rows for key in row)]))
        if file_format == "csv":
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(columns)
            writer.writerows([csv_cell(row.get(key)) for key in columns] for row in rows)
            return Response(
                "\ufeff" + buffer.getvalue(), media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{dataset_id}.csv"'},
            )
        return {"dataset_id": dataset_id, "rows": rows, "fields": columns,
                "next_cursor": result.get("next_cursor"), "page_count": len(rows)}

    if not published:
        @app.get("/v1/source-catalog", summary="浏览官方文档目录与采集配置覆盖", tags=["本地管理"])
        def source_catalog(policy: KeyPolicy = Depends(access)):
            if not policy.admin:
                raise HTTPException(403, "此 API 密钥未授权查看完整来源目录")
            return directory

        @app.get("/v1/datasets/{dataset_id}/partitions", summary="查看本地导入分片", tags=["本地管理"])
        def partitions(dataset_id: str, limit: int = Query(default=50, ge=1, le=100),
                       policy: KeyPolicy = Depends(access), store: Store = Depends(active_store)):
            accessible_profile(dataset_id, policy)
            if not policy.admin:
                raise HTTPException(403, "此 API 密钥未授权查看导入分片")
            return {"partitions": store.list_partitions(dataset_id, limit)}

        def run_operation(method, *args, **kwargs):
            try:
                return method(*args, **kwargs)
            except ValueError as error:
                raise HTTPException(422, str(error)) from None
            except RuntimeError as error:
                raise HTTPException(409, str(error)) from None

        @app.get("/ops/state", include_in_schema=False)
        def ops_state(request: Request):
            require_user(request, admin=True)
            if request.app.state.jobs is None:
                target = {"kind": "sqlite", "status": "ready", "sqlite_path": fixed_store.engine.url.database,
                          "mysql": None, "read_only": True}
                state = {"has_token": False, "job": None, "schedule": {}, "history": [],
                         "database": fixed_store.list_stats(), "target": target}
            else:
                state = request.app.state.jobs.state()
            return {**state, "catalog_coverage": coverage}

        def editable_operations(request: Request):
            require_browser_origin(request)
            require_user(request, admin=True)
            if demo:
                raise HTTPException(403, "模拟数据演示库仅支持查看运营页面")
            return request.app.state.jobs

        @app.post("/ops/database-target", include_in_schema=False)
        def ops_database_target(input_data: DatabaseTargetInput, request: Request):
            jobs = editable_operations(request)
            mysql = None
            if input_data.kind == "mysql":
                if input_data.mysql is None:
                    raise HTTPException(422, "请填写 MySQL 连接配置")
                mysql = input_data.mysql.model_dump(exclude={"password"})
                if input_data.mysql.password is not None:
                    mysql["password"] = input_data.mysql.password.get_secret_value()
            run_operation(jobs.initialize_target, kind=input_data.kind, mysql=mysql)
            return ops_state(request)

        @app.post("/ops/token", include_in_schema=False)
        def ops_token(input_data: TokenInput, request: Request):
            jobs = editable_operations(request)
            run_operation(jobs.set_token, input_data.token.get_secret_value(), remember=input_data.remember)
            return ops_state(request)

        @app.delete("/ops/token", include_in_schema=False)
        def ops_clear_token(request: Request):
            run_operation(editable_operations(request).clear_token)
            return ops_state(request)

        @app.post("/ops/jobs", status_code=202, include_in_schema=False)
        def ops_start_job(input_data: JobInput, request: Request):
            run_operation(editable_operations(request).start_job, **input_data.model_dump())
            return ops_state(request)

        @app.post("/ops/jobs/stop", include_in_schema=False)
        def ops_stop_job(request: Request):
            run_operation(editable_operations(request).stop_job)
            return ops_state(request)

        @app.put("/ops/schedule", include_in_schema=False)
        def ops_schedule(input_data: ScheduleInput, request: Request):
            run_operation(editable_operations(request).set_schedule, **input_data.model_dump())
            return ops_state(request)

        @app.put("/ops/category-schedules", include_in_schema=False)
        def ops_category_schedule(input_data: CategoryScheduleInput, request: Request):
            run_operation(editable_operations(request).set_category_schedule, **input_data.model_dump())
            return ops_state(request)

        @app.post("/ops/category-jobs", status_code=202, include_in_schema=False)
        def ops_category_job(input_data: CategoryJobInput, request: Request):
            run_operation(editable_operations(request).start_category_job, **input_data.model_dump())
            return ops_state(request)

    return app
