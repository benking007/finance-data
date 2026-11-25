"""Choose a collection database without persisting its credentials."""

import ipaddress
import os
import re
import ssl
from dataclasses import asdict, dataclass

from sqlalchemy import create_engine, event, inspect
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import SQLAlchemyError

from .storage import DEFAULT_DATABASE_URL, Store, operations


TARGET_OPERATION = "database_target"
SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


@dataclass(frozen=True)
class MysqlSettings:
    host: str
    port: int
    database: str
    username: str
    tls: bool

    @classmethod
    def from_values(cls, values):
        if any(not isinstance(values.get(field), str) for field in ("host", "database", "username")):
            raise ValueError("请填写 MySQL 主机名、数据库名称和用户名")
        host = values["host"].strip()
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        try:
            ipaddress.ip_address(host)
        except ValueError:
            try:
                host = host.encode("idna").decode("ascii")
            except UnicodeError:
                raise ValueError("请输入有效的 MySQL 主机名或 IP 地址") from None
            if not re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.?", host) or len(host) > 253:
                raise ValueError("请输入有效的 MySQL 主机名或 IP 地址") from None
        port = values.get("port", 3306)
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("MySQL 端口必须在 1 至 65535 之间")
        database = values["database"].strip()
        if not (1 <= len(database) <= 64 and database.lower() not in SYSTEM_DATABASES
                and all(ch not in "/\\." and ch.isprintable() and ord(ch) <= 0xFFFF for ch in database)):
            raise ValueError("请输入有效的 MySQL 业务数据库名称")
        username = values["username"].strip()
        if not (1 <= len(username) <= 128 and username.isprintable()):
            raise ValueError("请输入有效的 MySQL 用户名")
        tls = values.get("tls", False)
        if type(tls) is not bool:
            raise ValueError("MySQL TLS 设置无效")
        return cls(host, port, database, username, tls)

    @classmethod
    def from_url(cls, url):
        tls = any(key.startswith("ssl_") and key != "ssl_disabled" for key in url.query)
        return cls.from_values({"host": url.host or "localhost", "port": url.port or 3306,
                                "database": url.database, "username": url.username, "tls": tls})

    def url(self, password):
        return URL.create("mysql+pymysql", username=self.username, password=password,
                          host=self.host, port=self.port, database=self.database,
                          query={"charset": "utf8mb4", "connect_timeout": "5",
                                 "read_timeout": "12", "write_timeout": "12"})


def _initialize_private(store):
    if inspect(store.engine).has_table(operations.name) and store.read_operation("release_manifest") is not None:
        raise ValueError("发布数据库不能作为采集目标")
    store.initialize()


def _mysql_error_code(error):
    args = getattr(getattr(error, "orig", None), "args", ())
    return args[0] if args and isinstance(args[0], int) else None


def _mysql_failure(error):
    code = _mysql_error_code(error)
    if code in {1044, 1142, 1227}:
        return RuntimeError("MySQL 账户没有初始化该数据库的权限")
    if code in {1045, 1698, 3118}:
        return RuntimeError("MySQL 登录失败，请检查用户名、密码和账户访问范围")
    if code == 1049:
        return RuntimeError("MySQL 数据库不存在，请检查数据库名称")
    if code == 2026:
        return RuntimeError("MySQL TLS 验证失败，请检查服务器证书和主机名")
    if code in {2002, 2003, 2005, 2006, 2013}:
        return RuntimeError("无法连接 MySQL，请检查主机、端口和网络")
    if isinstance(error, ImportError):
        return RuntimeError("MySQL 驱动或认证依赖缺失，请安装项目依赖后重试")
    return RuntimeError("MySQL 初始化失败，请检查连接配置、权限和数据库表结构")


def _create_mysql_database(url, connect_args):
    admin_url = url.set(database="")
    admin = create_engine(admin_url, connect_args=connect_args)
    try:
        name = admin.dialect.identifier_preparer.quote_identifier(url.database)
        with admin.begin() as connection:
            connection.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {name} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
    finally:
        admin.dispose()


def open_mysql_store(url, *, verify_tls=False, create_missing=False):
    """Open an initialized private destination, returning only sanitized errors."""
    if url.drivername == "mysql":
        url = url.set(drivername="mysql+pymysql")
    try:
        connect_args = {"ssl": ssl.create_default_context()} if verify_tls else {}
        for attempt in range(2 if create_missing else 1):
            store = Store(url)
            if connect_args:
                def verified_connect(dialect, record, args, params):
                    params.update(connect_args)

                event.listen(store.engine, "do_connect", verified_connect)
            try:
                _initialize_private(store)
            except Exception as error:
                store.close()
                if isinstance(error, SQLAlchemyError) and _mysql_error_code(error) == 1049 and create_missing and attempt == 0:
                    _create_mysql_database(url, connect_args)
                    continue
                raise
            return store
    except (SQLAlchemyError, ImportError, OSError, RuntimeError) as error:
        raise _mysql_failure(error) from None


class DatabaseTargets:
    def __init__(self, database_url):
        configured = make_url(database_url)
        backend = configured.get_backend_name()
        if backend not in {"sqlite", "mysql"}:
            raise ValueError("采集目标只支持 SQLite 和 MySQL")
        self.local = Store(configured if backend == "sqlite" else DEFAULT_DATABASE_URL)
        _initialize_private(self.local)
        saved = self.local.read_operation(TARGET_OPERATION, {})
        self.kind = saved.get("kind", backend)
        self.settings = None
        if saved.get("mysql"):
            self.settings = MysqlSettings.from_values(saved["mysql"])
        elif backend == "mysql":
            self.settings = MysqlSettings.from_url(configured)
        self.store = self.local
        self.status = "ready"
        if self.kind != "mysql":
            return

        self.store = None
        password = os.environ.get("FINANCEDB_MYSQL_PASSWORD")
        if self.settings is None:
            self.status = "unavailable"
            return
        if backend == "mysql" and self.settings == MysqlSettings.from_url(configured):
            # Keep socket and transport options for compatible CLI environments.
            explicit_url = configured if password is None else configured.set(password=password)
            try:
                self.store = open_mysql_store(explicit_url)
            except (RuntimeError, ValueError):
                self.status = "unavailable"
            return
        if password is None:
            self.status = "needs_password"
            return
        try:
            self.store = open_mysql_store(self.settings.url(password), verify_tls=self.settings.tls)
        except (RuntimeError, ValueError):
            self.status = "unavailable"

    def prepare(self, kind, mysql):
        if kind == "sqlite":
            _initialize_private(self.local)
            return self.local, self.settings
        if kind != "mysql" or not isinstance(mysql, dict):
            raise ValueError("请选择数据库类型并填写相应的连接信息")
        settings = MysqlSettings.from_values(mysql)
        password = mysql.get("password", "")
        if not isinstance(password, str) or len(password) > 1024 or "\x00" in password:
            raise ValueError("MySQL 密码格式无效")
        store = open_mysql_store(settings.url(password), verify_tls=settings.tls, create_missing=True)
        return store, settings

    def activate(self, kind, store, settings):
        self.local.write_operation(TARGET_OPERATION, {"kind": kind, "mysql": asdict(settings) if settings else None})
        previous = self.store
        self.kind, self.store, self.settings, self.status = kind, store, settings, "ready"
        return previous

    def state(self):
        return {"kind": self.kind, "status": self.status,
                "sqlite_path": self.local.engine.url.database or ":memory:",
                "mysql": asdict(self.settings) if self.settings else None}

    def close(self):
        if self.store is not None and self.store is not self.local:
            self.store.close()
        self.local.close()
