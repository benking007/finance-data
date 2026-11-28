"""Shared SQL storage for SQLite and MySQL."""

import base64
from collections import defaultdict
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from threading import RLock

from sqlalchemy import (
    BigInteger, Column, Index, Integer, MetaData, String, Table, Text,
    and_, cast, create_engine, event, func, inspect, literal, null, or_, select, tuple_,
)
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.exc import DBAPIError, OperationalError, ProgrammingError, TimeoutError as PoolTimeoutError
from sqlalchemy.engine import make_url
from sqlalchemy.pool import StaticPool

from .business_query import business_row_order, event_date_column, indexed_date_filter
from .business_schema import (
    FactorValueStorageMismatch, check_mysql_business_schema, ensure_factor_value_storage, migrate_business_tables,
)
from .business_tables import BusinessFieldError, business_columns, business_metadata, business_row, business_tables, project_fields


DEFAULT_DATABASE_URL = "sqlite:///data/finance-v2.sqlite3"
SCHEMA_VERSION = 3
PAGINATION_VERIFIED_MESSAGE = "分页完整性已核验"
metadata = MetaData()
long_text = Text().with_variant(mysql.LONGTEXT(), "mysql")
mysql_options = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_bin"}

schema_versions = Table(
    "schema_versions", metadata,
    Column("version", Integer, primary_key=True), Column("applied_at", String(32), nullable=False),
    **mysql_options,
)
dataset_counts = Table(
    "dataset_counts", metadata,
    Column("dataset_id", String(80), primary_key=True),
    Column("row_count", BigInteger, nullable=False),
    Column("min_date", String(32)),
    Column("max_date", String(32)),
    **mysql_options,
)

revisions = Table(
    "dataset_revisions", metadata,
    Column("dataset_id", String(80), primary_key=True),
    Column("record_key", String(64), primary_key=True),
    Column("content_hash", String(64), primary_key=True),
    Column("payload", long_text, nullable=False),
    Column("event_date", String(32), nullable=False),
    Column("instrument", String(128), nullable=False),
    Column("first_seen_at", String(32), nullable=False),
    **mysql_options,
)

partitions = Table(
    "ingestion_partitions", metadata,
    Column("dataset_id", String(80), primary_key=True),
    Column("partition_id", String(128), primary_key=True),
    Column("params", long_text, nullable=False),
    Column("status", String(24), nullable=False),
    Column("source_count", BigInteger, nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("message", Text, nullable=False),
    Column("updated_at", String(32), nullable=False),
    **mysql_options,
)
Index("ix_partitions_status", partitions.c.dataset_id, partitions.c.status, partitions.c.updated_at)
Index("ix_partitions_updated", partitions.c.updated_at)

operations = Table(
    "operation_state", metadata,
    Column("name", String(80), primary_key=True),
    Column("payload", long_text, nullable=False),
    **mysql_options,
)


class SchemaMismatch(RuntimeError):
    """An application and database schema must agree before access."""


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


_MYSQL_TRANSIENT_ERRORS = {
    1205: "锁等待超时", 1213: "事务死锁", 2006: "连接已断开", 2013: "查询时连接中断", 2055: "与服务器的连接中断",
}
_MYSQL_STORAGE_ERRORS = {
    1021: "存储空间不足", 1044: "数据库访问权限不足", 1045: "登录验证失败",
    1054: "业务表字段不存在", 1114: "表空间不足", 1142: "操作权限不足",
    1146: "业务表不存在", 1153: "数据包超出服务器限制",
    2002: "无法连接服务器", 2003: "无法连接服务器", 2020: "数据包超出客户端限制",
}
_SQLITE_TRANSIENT_ERRORS = {sqlite3.SQLITE_BUSY: "数据库繁忙", sqlite3.SQLITE_LOCKED: "数据库锁冲突"}
_SQLITE_STORAGE_ERRORS = {
    sqlite3.SQLITE_FULL: "存储空间不足", sqlite3.SQLITE_READONLY: "数据库为只读",
    sqlite3.SQLITE_IOERR: "数据库文件读写异常", sqlite3.SQLITE_CANTOPEN: "无法打开数据库文件",
}


def storage_error_details(error, backend):
    """Return an operator-safe message and whether a fresh transaction can be retried."""
    if isinstance(error, PoolTimeoutError):
        return "目标数据库操作失败（等待数据库连接超时）", False
    if isinstance(error, DBAPIError):
        if backend == "mysql":
            args = getattr(error.orig, "args", ())
            if args and type(args[0]) is int:
                code = args[0]
                retryable = code in _MYSQL_TRANSIENT_ERRORS
                detail = _MYSQL_TRANSIENT_ERRORS.get(code) or _MYSQL_STORAGE_ERRORS.get(code, type(error).__name__)
                return f"目标数据库操作失败（MySQL {code} · {detail}）", retryable
        elif backend == "sqlite":
            code = getattr(error.orig, "sqlite_errorcode", None)
            if type(code) is int:
                primary = code & 0xFF
                retryable = primary in _SQLITE_TRANSIENT_ERRORS
                detail = _SQLITE_TRANSIENT_ERRORS.get(primary) or _SQLITE_STORAGE_ERRORS.get(primary, type(error).__name__)
                return f"目标数据库操作失败（SQLite {code} · {detail}）", retryable
    return f"目标数据库操作失败（{type(error).__name__}）", False


def reject_legacy_schema(conn, *, allow_upgrade=False):
    existing = set(inspect(conn).get_table_names())
    if {"account_users", "account_sessions"} & existing:
        raise SchemaMismatch("账号库必须与业务数据库分开，请选择业务数据库")
    if "dataset_rows" in existing:
        raise SchemaMismatch("检测到旧版单表存储，请使用新的空数据库初始化业务表")
    if schema_versions.name in existing:
        version = conn.scalar(select(func.max(schema_versions.c.version)))
        if version == 2 and not allow_upgrade:
            raise SchemaMismatch("业务库需要升级。请停止采集和服务，再使用同一业务库连接运行 financedb init")
        if version is not None and version not in {2, SCHEMA_VERSION}:
            raise SchemaMismatch(f"不支持数据库结构版本 {version}，请使用新的空数据库初始化业务表")
    return existing


def create_schema(conn, *, upgrade=False):
    existing = reject_legacy_schema(conn, allow_upgrade=upgrade)
    expected = {**metadata.tables, **business_tables}
    if existing - expected.keys() - {"alembic_version"}:
        raise SchemaMismatch("数据库含有其他应用的表，请使用独立的业务数据库")
    version = conn.scalar(select(func.max(schema_versions.c.version))) if schema_versions.name in existing else None
    missing = [table for table in business_tables.values() if table.name not in existing]
    if upgrade or missing:
        check_mysql_business_schema(conn)
    if upgrade:
        migrate_business_tables(conn, existing)
    if existing and version is None:
        inspector = inspect(conn)
        for name in existing & expected.keys():
            actual = {column["name"].lower() for column in inspector.get_columns(name)}
            if actual != {column.name.lower() for column in expected[name].columns}:
                raise SchemaMismatch(f"数据库中的表 {name} 与业务结构不一致，请使用新的空数据库")
    try:
        ensure_factor_value_storage(conn, existing, upgrade=upgrade)
    except FactorValueStorageMismatch as exc:
        raise SchemaMismatch(str(exc)) from None
    metadata.create_all(conn, tables=[table for table in metadata.sorted_tables if table.name not in existing])
    business_metadata.create_all(conn, tables=missing)
    if version != SCHEMA_VERSION:
        conn.execute(schema_versions.insert().values(version=SCHEMA_VERSION, applied_at=timestamp()))


def _json_default(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported value: {type(value).__name__}")


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default, allow_nan=False)


def _digest(value):
    return sha256(value.encode("utf-8")).hexdigest()


def normalize_date(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    if len(text) == 8 and text.isascii() and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    if len(text) == 6 and text.isascii() and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-01"
    if len(text) == 6 and text[4] in "Qq" and text[5] in "1234":
        return f"{text[:4]}-{(int(text[5]) - 1) * 3 + 1:02d}-01"
    if len(text) == 7 and text[4] == "-":
        return f"{text}-01"
    return ""


def _profile_id(profile):
    return profile.get("id") or profile["api_name"]


def split_scope_message(profile, params):
    request = profile.get("request", {})
    source = request.get("cap_code_supplement")
    if request.get("cap_candidate_code_sources"):
        # The candidate list cannot certify a capped parent. Recheck older
        # synthetic "complete" markers while keeping ordinary daily checkpoints.
        source = "candidate_code_sources"
    if source and request.get("code_param", "ts_code") not in params:
        return f"拆分范围已核验:{source}:1"
    return None


def _publication_order(profile, row):
    field = profile["revision_field"]
    value = row.get(field)
    if value in (None, ""):
        value = normalize_date(row.get(profile.get("date_field")))
    try:
        published = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).strip())
    except ValueError:
        raise BusinessFieldError(f"业务字段 {_profile_id(profile)}.{field} 的发布时间无效") from None
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone(timedelta(hours=8)))
    return published.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _key_fields(profile):
    keys = profile.get("key", ())
    return (keys,) if isinstance(keys, str) else tuple(keys)


def _revision_key(profile, row, content_hash):
    keys = _key_fields(profile)
    if keys and all(row.get(key) not in (None, "") for key in keys):
        return _digest(json_text([row[key] for key in keys]))
    if keys:
        return _digest(json_text([row.get(key) for key in keys] + [content_hash]))
    return content_hash


def _identity(key_fields, row):
    key = tuple(row[field] for field in key_fields)
    if key and all(value not in (None, "") for value in key):
        return True, key
    # No reliable natural key means only an exact business row is a duplicate.
    return False, tuple(row.values())


def _version_priority(profile, row):
    dataset_id = _profile_id(profile)
    revision_field = profile.get("revision_field")
    if revision_field and profile.get("field_info", {}).get(revision_field, {}).get("type") == "datetime":
        return _publication_order(profile, row), _digest(json_text(row))
    if revision_field or dataset_id in {"income", "balancesheet", "cashflow", "fina_indicator"}:
        release = normalize_date(row.get(revision_field or "f_ann_date") or row.get("ann_date"))
        return release, str(row.get("update_flag")) == "1"
    return ()


def _incoming_rows(profile, table, key_fields, rows):
    optional_fields = set(profile.get("request", {}).get("optional_response_fields", ())).difference(key_fields)
    for row in rows:
        if key_fields and (not all(key in row for key in key_fields)
                           or not any(row[key] not in (None, "") for key in key_fields)):
            raise BusinessFieldError(f"{table.name} 返回的记录缺少业务键")
        values = project_fields(table, row)
        identity = _identity(key_fields, values)
        priority = _version_priority(profile, values)
        if identity[0] and optional_fields:
            # A missing upstream column leaves existing data untouched.
            for field in optional_fields.difference(row):
                values.pop(field, None)
        yield row, identity, priority, values


def _prepare_business_write(profile, table, key_fields, rows, observed_at):
    current, history = {}, {}
    versioned = profile.get("versioned") or profile.get("revision_field") or table.name in {
        "income", "balancesheet", "cashflow", "fina_indicator", "cn_cpi",
    }
    for row, identity, priority, values in _incoming_rows(profile, table, key_fields, rows):
        if identity not in current or priority > current[identity][0]:
            current[identity] = (priority, values)
        if versioned:
            source_json = json_text(row)
            content_hash = _digest(source_json)
            record_key = _revision_key(profile, row, content_hash)
            history[(record_key, content_hash)] = {
                "dataset_id": table.name, "record_key": record_key, "content_hash": content_hash,
                "event_date": normalize_date(values.get(profile.get("date_field"))),
                "instrument": str(values.get(profile.get("code_field")) or ""),
                "payload": source_json, "first_seen_at": observed_at,
            }
    return current, history


def _matching_rows(conn, table, fields, values, *, columns=None, lock=False):
    """Read bounded batches using indexed business values as selectors."""
    statement = select(*(columns if columns is not None else table.c)).select_from(table)
    if lock:
        statement = statement.with_for_update()
    selectors = [table.c[field] for field in fields]
    if not selectors:
        yield from conn.execute(statement).mappings()
        return
    complete = []
    nullable = []
    for values_tuple in dict.fromkeys(values):
        if None in values_tuple:
            nullable.append(values_tuple)
        else:
            complete.append(values_tuple)
    step = min(250, 900 // len(selectors))
    for offset in range(0, len(complete), step):
        batch = complete[offset:offset + step]
        condition = (selectors[0].in_([value[0] for value in batch]) if len(selectors) == 1
                     else tuple_(*selectors).in_(batch))
        yield from _exact_matches(conn, statement.where(condition), fields, batch)
    for offset in range(0, len(nullable), step):
        batch = nullable[offset:offset + step]
        conditions = [
            and_(*(column == value for column, value in zip(selectors, entry, strict=True)))
            for entry in batch
        ]
        yield from _exact_matches(conn, statement.where(or_(*conditions)), fields, batch)


def _exact_matches(conn, statement, fields, batch):
    # Some MySQL string collations ignore trailing whitespace in equality.
    expected = set(batch)
    for row in conn.execute(statement).mappings():
        if tuple(row[field] for field in fields) in expected:
            yield row


def _cursor(parts):
    return base64.urlsafe_b64encode(json_text(parts).encode()).decode().rstrip("=")


def _parse_cursor(value, query_id):
    try:
        if len(value) > 512:
            raise ValueError
        revision, event_date, offset = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if revision != query_id or not isinstance(event_date, str) or type(offset) is not int:
            raise ValueError
        if len(event_date) not in {0, 10} or not 1 <= offset < 2**63:
            raise ValueError
        return event_date, offset
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("Invalid cursor for this query") from exc


class Store:
    def __init__(self, database_url=DEFAULT_DATABASE_URL, *, read_only=False):
        url = make_url(database_url)
        backend = url.get_backend_name()
        if backend == "mysql" and url.drivername == "mysql":
            url = url.set(drivername="mysql+pymysql")
        if backend not in {"sqlite", "mysql"}:
            raise ValueError("Database URL must use sqlite or mysql")
        self.read_only = read_only
        self.backend = backend
        self.identity = (backend, url.host, url.port or 3306, url.database)
        self._read_lock = nullcontext()
        self._write_lock = nullcontext()
        if backend == "sqlite":
            self._write_lock = RLock()
            engine_options = {"connect_args": {"timeout": 15, "check_same_thread": False}}
            if not url.database or url.database == ":memory:":
                # The in-memory database lives on a single shared connection.
                engine_options["poolclass"] = StaticPool
                self._read_lock = self._write_lock
            else:
                sqlite_path = Path(url.database).expanduser().resolve()
                self.identity = (backend, sqlite_path)
                if read_only:
                    url = url.set(database=f"{sqlite_path.as_uri()}?mode=ro", query={"uri": "true"})
                else:
                    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
                    url = url.set(database=str(sqlite_path))
            self.engine = create_engine(url, **engine_options)
            event.listen(self.engine, "connect", self._configure_sqlite)
        else:
            self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=1800)

    def _configure_sqlite(self, connection, _):
        cursor = connection.cursor()
        cursor.execute("PRAGMA busy_timeout = 15000")
        cursor.execute("PRAGMA foreign_keys = ON")
        if self.read_only:
            cursor.execute("PRAGMA query_only = ON")
        cursor.close()

    @contextmanager
    def _read_connection(self):
        with self._read_lock, self.engine.connect() as conn:
            yield conn

    @contextmanager
    def _write_transaction(self):
        # SQLite permits one writer. Queue writes before checking out a connection.
        with self._write_lock, self.engine.begin() as conn:
            yield conn

    def initialize(self, *, upgrade=False):
        if self.read_only:
            self.validate_schema()
            return
        with self._write_transaction() as conn:
            create_schema(conn, upgrade=upgrade)

    def validate_schema(self):
        try:
            with self._read_connection() as conn:
                reject_legacy_schema(conn)
                version = conn.scalar(select(func.max(schema_versions.c.version)))
        except (OperationalError, ProgrammingError) as exc:
            raise SchemaMismatch("Initialize the finance database first") from exc
        if version != SCHEMA_VERSION:
            raise SchemaMismatch("请先初始化业务数据库")

    def close(self):
        with self._write_lock:
            self.engine.dispose()

    def _upsert(self, conn, table, values, update_columns, *, guard=None):
        if self.read_only:
            raise RuntimeError("The database connection is read-only")
        if not values:
            return
        if self.backend == "sqlite":
            stmt = sqlite.insert(table)
            if update_columns:
                where = guard(stmt.excluded) if guard else None
                stmt = stmt.on_conflict_do_update(
                    index_elements=list(table.primary_key),
                    set_={name: stmt.excluded[name] for name in update_columns}, where=where,
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=list(table.primary_key))
        else:
            stmt = mysql.insert(table)
            if update_columns:
                if guard:
                    from sqlalchemy import case
                    condition = guard(stmt.inserted)
                    updates = [(name, case((condition, stmt.inserted[name]), else_=table.c[name])) for name in update_columns]
                else:
                    updates = [(name, stmt.inserted[name]) for name in update_columns]
            else:
                key = next(iter(table.primary_key))
                updates = [(key.name, key)]
            stmt = stmt.on_duplicate_key_update(updates)
        for offset in range(0, len(values), 1000):
            conn.execute(stmt, values[offset:offset + 1000])

    @staticmethod
    def _completed_partition(row_limit, split_scope=None):
        completed = partitions.c.status.in_(("complete", "empty"))
        if row_limit:
            completed = completed & or_(partitions.c.status == "empty", partitions.c.source_count < row_limit,
                                         partitions.c.message == PAGINATION_VERIFIED_MESSAGE)
        if split_scope:
            completed = completed & or_(partitions.c.status != "complete", partitions.c.source_count != 0,
                                         partitions.c.message == split_scope)
        return completed

    def _save_partition(self, conn, values, *, keep_complete=False, row_limit=None, split_scope=None):
        update_columns = ["params", "source_count", "row_count", "message", "updated_at", "status"]
        if self.backend == "mysql" and keep_complete and (row_limit or split_scope):
            # MySQL evaluates upsert assignments left to right. Use a WHERE
            # clause so completeness is checked before any field is updated.
            # Insert first because an UPDATE of a missing key can lock the gap
            # and deadlock other categories trying to insert their checkpoints.
            self._upsert(conn, partitions, [values], [])
            conn.execute(partitions.update().where(
                partitions.c.dataset_id == values["dataset_id"],
                partitions.c.partition_id == values["partition_id"],
                ~self._completed_partition(row_limit, split_scope),
            ).values({name: values[name] for name in update_columns}))
            return
        guard = (lambda incoming: ~self._completed_partition(row_limit, split_scope)) if keep_complete else None
        self._upsert(conn, partitions, [values], update_columns, guard=guard)

    def partition_done(self, dataset_id, partition_id, *, row_limit=None, expected_keys=(), expected_rows=(),
                       split_scope=None):
        with self._read_connection() as conn:
            result = conn.scalar(select(partitions.c.partition_id).where(
                partitions.c.dataset_id == dataset_id, partitions.c.partition_id == partition_id,
                self._completed_partition(row_limit, split_scope),
            ))
            if result is None:
                return False
            if not expected_keys and not expected_rows:
                return True
            table = business_tables[dataset_id]
            profile = table.info["profile"]
            if expected_rows:
                fields = [column.name for column in business_columns(table)]
                if any(len(row) != len(fields) for row in expected_rows):
                    return False
                indexed = tuple(dict.fromkeys(field for field in (
                    profile.get("code_field"), profile.get("date_field"),
                ) if field))
                positions = [fields.index(field) for field in indexed]
                values = {tuple(row[position] for position in positions) for row in expected_rows}
                found = _matching_rows(conn, table, indexed, values)
                if not set(expected_rows).issubset(tuple(row[field] for field in fields) for row in found):
                    return False
            if not expected_keys:
                return True
            key_fields = _key_fields(profile)
            if not key_fields or any(len(key) != len(key_fields) or any(value in (None, "") for value in key)
                                     for key in expected_keys):
                return False
            try:
                projected = (project_fields(table, dict(zip(key_fields, key, strict=True))) for key in expected_keys)
                expected = {tuple(row[field] for field in key_fields) for row in projected}
            except BusinessFieldError:
                return False
            found = _matching_rows(conn, table, key_fields, expected, columns=[table.c[field] for field in key_fields])
            return expected.issubset(tuple(row.values()) for row in found)

    def date_probe_checkpoints(self, dataset_id, partition_ids, *, row_limit=None):
        with self._read_connection() as conn:
            rows = conn.execute(select(partitions.c.status, partitions.c.source_count,
                                       self._completed_partition(row_limit).label("done")).where(
                partitions.c.dataset_id == dataset_id, partitions.c.partition_id.in_(partition_ids),
            )).all()
        return sum(bool(done) for _, _, done in rows), any(status == "complete" or count for status, count, _ in rows)

    def compare_business_rows(self, profile, rows):
        """Return the supplied rows that would insert or update a business record."""
        table = business_tables[_profile_id(profile)]
        key_fields = _key_fields(profile)
        current, source = {}, {}
        for row, identity, priority, values in _incoming_rows(profile, table, key_fields, rows):
            if identity not in current or priority > current[identity][0]:
                current[identity] = (priority, values)
                source[identity] = row

        result = {"missing": [], "changed": []}
        if current:
            with self._read_connection() as conn:
                for identity, old, _ in self._business_row_deltas(conn, table, profile, key_fields, current):
                    result["missing" if old is None else "changed"].append(dict(source[identity]))
        return result

    def commit_partition(self, profile, partition_id, params, rows, *, status="complete", source_count=None, message=""):
        dataset_id = _profile_id(profile)
        table = business_tables[dataset_id]
        observed_at = timestamp()
        key_fields = _key_fields(profile)
        current, history = _prepare_business_write(profile, table, key_fields, rows, observed_at)
        batch = {
            "dataset_id": dataset_id, "partition_id": partition_id, "params": json_text(params),
            "status": status, "source_count": len(rows) if source_count is None else int(source_count),
            "row_count": len(current), "message": message[:1024], "updated_at": observed_at,
        }
        with self._write_transaction() as conn:
            self._write_business_rows(conn, table, profile, key_fields, current, history)
            self._save_partition(conn, batch, keep_complete=status == "partial", row_limit=profile.get("row_limit"),
                                 split_scope=split_scope_message(profile, params))
        return {**batch, "params": params}

    def apply_business_rows(self, profile, rows):
        """Apply verified changes in one transaction without advancing ingest checkpoints."""
        table = business_tables[_profile_id(profile)]
        key_fields = _key_fields(profile)
        current, history = _prepare_business_write(profile, table, key_fields, rows, timestamp())
        with self._write_transaction() as conn:
            inserted, updated = self._write_business_rows(conn, table, profile, key_fields, current, history)
        return {"inserted": inserted, "updated": updated}

    def _write_business_rows(self, conn, table, profile, key_fields, current, history):
        counts = self._save_dataset_rows(conn, table, profile, key_fields, current)
        self._upsert(conn, revisions, list(history.values()), [])
        return counts

    def business_summary(self, table):
        event_date = func.nullif(event_date_column(table, table.info["profile"], self.backend), "")
        return select(func.count().label("row_count"), func.min(event_date).label("min_date"),
                      func.max(event_date).label("max_date")).select_from(table)

    def _lock_totals(self, conn, dataset_id):
        self._upsert(conn, dataset_counts, [{"dataset_id": dataset_id, "row_count": 0, "min_date": None, "max_date": None}], [])
        query = select(dataset_counts).where(dataset_counts.c.dataset_id == dataset_id).with_for_update()
        return dict(conn.execute(query).mappings().one())

    def _update_totals(self, conn, table, totals, added, changes):
        totals["row_count"] += added
        changed_extreme = False
        for old_date, new_date in changes:
            if old_date and old_date != new_date and old_date in {totals["min_date"], totals["max_date"]}:
                changed_extreme = True
            if new_date:
                totals["min_date"] = min(totals["min_date"] or new_date, new_date)
                totals["max_date"] = max(totals["max_date"] or new_date, new_date)
        if changed_extreme:
            totals.update(conn.execute(self.business_summary(table)).mappings().one())
        self._upsert(conn, dataset_counts, [totals], ["row_count", "min_date", "max_date"])

    def _insert_rows(self, conn, table, rows):
        for offset in range(0, len(rows), 1000):
            conn.execute(table.insert(), rows[offset:offset + 1000])

    @staticmethod
    def _business_row_deltas(conn, table, profile, key_fields, current, *, lock=False):
        date_field = profile.get("date_field")
        fallback = tuple(dict.fromkeys(field for field in (profile.get("code_field"), date_field) if field))
        candidates = defaultdict(set)
        for (keyed, key), (_, values) in current.items():
            fields = key_fields if keyed else fallback
            candidates[keyed, fields].add(key if keyed else tuple(values[name] for name in fields))
        before = defaultdict(list)
        for (keyed, fields), selectors in candidates.items():
            for existing in _matching_rows(conn, table, fields, selectors, lock=lock):
                identity = _identity(key_fields, existing)
                if identity[0] == keyed and identity in current:
                    before[identity].append(dict(existing))
        for identity, (priority, new) in current.items():
            old_rows = before.get(identity, ())
            sparse = len(new) < len(table.c)
            if not old_rows:
                yield identity, None, new
                continue
            if not identity[0] or any(
                all(old[field] == value for field, value in new.items()) if sparse else old == new
                for old in old_rows
            ):
                continue
            try:
                older = any(priority < _version_priority(profile, old) for old in old_rows)
            except BusinessFieldError:
                older = False  # An upgraded row can have an invalid legacy timestamp.
            if older:
                continue
            if len(old_rows) != 1:
                raise BusinessFieldError(f"{table.name} 已有多条记录共用业务键，无法明确更新目标")
            yield identity, old_rows[0], new

    def _save_dataset_rows(self, conn, table, profile, key_fields, current):
        if not current:
            return 0, 0
        # Serialize lookups and writes through the existing per-dataset count row.
        totals = self._lock_totals(conn, table.name)
        date_field = profile.get("date_field")
        added, changes = [], []
        for identity, old, new in self._business_row_deltas(conn, table, profile, key_fields, current, lock=True):
            if old is None:
                added.append({column.name: new.get(column.name) for column in table.c} if len(new) < len(table.c) else new)
                changes.append(("", normalize_date(new.get(date_field))))
                continue
            filters = []
            for field, value in zip(key_fields, identity[1], strict=True):
                column = table.c[field]
                filters.append(column == value)
                if self.backend == "mysql" and isinstance(column.type, String):
                    filters.append(cast(column, mysql.BINARY) == cast(literal(value), mysql.BINARY))
            conn.execute(table.update().where(*filters).values(
                {field: value for field, value in new.items() if old[field] != value}
            ))
            changes.append((normalize_date(old.get(date_field)), normalize_date(new.get(date_field))))
        self._insert_rows(conn, table, added)
        self._update_totals(conn, table, totals, len(added), changes)
        return len(added), len(changes) - len(added)

    def commit_tick_batch(self, profile, partition_id, params, rows):
        table = business_tables[_profile_id(profile)]
        first, last = params["first_row"], params["last_row"]
        if first < 1 or last != first + len(rows) - 1:
            raise ValueError("Tick 导入行范围与数据不符")
        with self._write_transaction() as conn:
            totals = self._lock_totals(conn, table.name)
            digest = func.json_extract(partitions.c.params, "$.sha256")
            if self.backend == "mysql":
                digest = func.json_unquote(digest)
            checkpoints = select(partitions.c.params).where(
                partitions.c.dataset_id == table.name, self._completed_partition(None), digest == params["sha256"],
            ).with_for_update()
            ranges = []
            for saved in conn.scalars(checkpoints):
                saved = json.loads(saved)
                begin, end = saved.get("first_row"), saved.get("last_row")
                if begin is None and end is None:
                    ranges.append((first, last))  # The entire file has already been imported.
                elif isinstance(begin, int) and isinstance(end, int) and begin <= last and end >= first:
                    ranges.append((max(first, begin), min(last, end)))
            pending = []
            next_row = first
            for begin, end in sorted(ranges):
                if begin > next_row:
                    pending.extend(rows[next_row - first:begin - first])
                next_row = max(next_row, end + 1)
            pending.extend(rows[next_row - first:])
            added = [project_fields(table, row) for row in pending]
            self._insert_rows(conn, table, added)
            date_field = profile.get("date_field")
            self._update_totals(conn, table, totals, len(added),
                                (("", normalize_date(row.get(date_field))) for row in added))
            checkpoint = {"dataset_id": table.name, "partition_id": partition_id, "params": json_text(params),
                          "status": "complete", "source_count": len(rows), "row_count": len(added),
                          "message": "", "updated_at": timestamp()}
            self._save_partition(conn, checkpoint)
            return len(added)

    def record_failure(self, profile, partition_id, params, message, *, status="failed"):
        batch = {
            "dataset_id": _profile_id(profile), "partition_id": partition_id, "params": json_text(params),
            "status": status, "source_count": 0, "row_count": 0, "message": str(message)[:1024], "updated_at": timestamp(),
        }
        with self._write_transaction() as conn:
            self._save_partition(conn, batch, keep_complete=True, row_limit=profile.get("row_limit"),
                                 split_scope=split_scope_message(profile, params))

    def list_stats(self):
        summaries = {}
        dataset_query = select(dataset_counts)
        partition_query = select(
            partitions.c.dataset_id, partitions.c.status, func.count().label("count"),
            func.max(partitions.c.updated_at).label("last_sync"),
        ).group_by(partitions.c.dataset_id, partitions.c.status)
        with self._read_connection() as conn:
            for row in conn.execute(dataset_query).mappings():
                summaries[row["dataset_id"]] = self._empty_stats(row["dataset_id"]) | dict(row)
            for row in conn.execute(partition_query).mappings():
                dataset_id = row["dataset_id"]
                item = summaries.setdefault(dataset_id, self._empty_stats(dataset_id))
                status = row["status"]
                if status in {"complete", "empty"}:
                    item["complete_partitions"] += row["count"]
                    item["last_sync"] = max(item["last_sync"] or "", row["last_sync"])
                elif status != "split":
                    item["pending_partitions"] += row["count"]
                item["partition_statuses"][status] = row["count"]
        return [summaries[key] for key in sorted(summaries)]

    @staticmethod
    def _empty_stats(dataset_id):
        return {"dataset_id": dataset_id, "row_count": 0, "min_date": None, "max_date": None, "last_sync": None,
                "complete_partitions": 0, "pending_partitions": 0, "partition_statuses": {}}

    def dataset_stats(self, dataset_id):
        return self.list_stats_for(dataset_id)

    def dataset_date_coverage(self, dataset_id):
        """Read the first stored business date without scanning managed tables."""
        totals = select(dataset_counts.c.row_count, dataset_counts.c.min_date).where(
            dataset_counts.c.dataset_id == dataset_id
        )
        with self._read_connection() as conn:
            coverage = conn.execute(totals).mappings().one_or_none()
            if not coverage or not coverage["row_count"] or not coverage["min_date"]:
                # An imported table can contain business rows before it has a coverage summary.
                coverage = conn.execute(self.business_summary(business_tables[dataset_id])).mappings().one()
        return {"min_date": coverage["min_date"] or None, "has_rows": bool(coverage["row_count"])}

    def list_stats_for(self, dataset_id):
        item = self._empty_stats(dataset_id)
        query = select(dataset_counts).where(dataset_counts.c.dataset_id == dataset_id)
        by_status = select(partitions.c.status, func.count(), func.max(partitions.c.updated_at)).where(
            partitions.c.dataset_id == dataset_id).group_by(partitions.c.status)
        with self._read_connection() as conn:
            item.update(conn.execute(query).mappings().one_or_none() or {})
            for status, count, updated_at in conn.execute(by_status):
                item["partition_statuses"][status] = count
                if status in {"complete", "empty"}:
                    item["complete_partitions"] += count
                    item["last_sync"] = max(item["last_sync"] or "", updated_at)
                elif status != "split":
                    item["pending_partitions"] += count
        return item

    def row_count(self, dataset_id):
        return self.dataset_stats(dataset_id)["row_count"]

    def list_partitions(self, dataset_id=None, limit=100):
        query = select(partitions).order_by(partitions.c.updated_at.desc()).limit(limit)
        if dataset_id:
            query = query.where(partitions.c.dataset_id == dataset_id)
        with self._read_connection() as conn:
            return [dict(row) | {"params": json.loads(row["params"])} for row in conn.execute(query).mappings()]

    def query_rows(self, profile, *, code=None, start=None, end=None, cursor=None, limit=100, fields=None, visible_fields=None):
        dataset_id = _profile_id(profile)
        table = business_tables[dataset_id]
        date_start, date_end = normalize_date(start), normalize_date(end)
        query_id = _digest(json_text([dataset_id, code, date_start, date_end, visible_fields]))[:20]
        columns = [column for column in business_columns(table)
                   if (fields is None or column.name in fields) and (visible_fields is None or column.name in visible_fields)]
        event_date = event_date_column(table, profile, self.backend, visible_fields)
        query = select(*columns, event_date).select_from(table)
        if code is not None:
            instrument = table.c.get(profile.get("code_field"))
            query = query.where(instrument == code if instrument is not None else code == "")
        if date_start:
            query = query.where(event_date >= date_start)
        if date_end:
            query = query.where(event_date <= date_end)
        offset = 0
        cursor_date = None
        if cursor:
            cursor_date, offset = _parse_cursor(cursor, query_id)
            query = query.where(event_date <= cursor_date).offset(offset)
        upper = min(date_end, cursor_date) if date_end and cursor_date else date_end or cursor_date
        candidates = indexed_date_filter(table, profile, date_start, upper, visible_fields)
        if candidates is not None:
            query = query.where(candidates)
        ordering = business_row_order(table, profile, self.backend, visible_fields)
        query = query.order_by(event_date.desc(), *ordering).limit(limit + 1)
        with self._read_connection() as conn:
            batch = []
            date_field = profile.get("date_field")
            if not date_start and date_field and (visible_fields is None or date_field in visible_fields):
                hint = upper if cursor else date_end or conn.scalar(select(dataset_counts.c.max_date).where(
                    dataset_counts.c.dataset_id == dataset_id))
                try:
                    lower = (date.fromisoformat(hint) - timedelta(days=31)).isoformat() if hint else None
                except (ValueError, OverflowError):
                    lower = None
                recent = indexed_date_filter(table, profile, lower, upper, visible_fields)
                if recent is not None:
                    # A recent slice can answer a full page without sorting the
                    # entire history. A short result falls back to all dates.
                    batch = conn.execute(query.where(recent, event_date >= lower)).all()
            if len(batch) <= limit:
                batch = conn.execute(query).all()
        token = None
        if len(batch) > limit:
            batch = batch[:limit]
            last_date = batch[-1][-1]
            consumed = sum(row[-1] == last_date for row in batch)
            token = _cursor([query_id, last_date, consumed + (offset if last_date == cursor_date else 0)])
        return {"rows": [business_row(table, row._mapping) for row in batch], "next_cursor": token}

    def instrument_codes(self, dataset_id):
        return self.instrument_values(dataset_id)

    def instrument_values(self, dataset_id, field=None, *, event_date=None):
        table = business_tables.get(dataset_id)
        if table is None:
            return []
        column = table.c.get(field or table.info["code_field"])
        if column is None:
            return []
        query = select(column).distinct()
        if event_date is not None:
            stored_date = event_date_column(table, table.info, self.backend)
            normalized = normalize_date(event_date)
            query = query.where(stored_date == normalized)
            candidates = indexed_date_filter(table, table.info, normalized, normalized)
            if candidates is not None:
                query = query.where(candidates)
        with self._read_connection() as conn:
            return sorted({str(item) for item in conn.scalars(query) if item not in (None, "")})

    def instrument_lifetimes(self, dataset_id):
        table = business_tables.get(dataset_id)
        if table is None or not table.info["code_field"]:
            return {}
        if "list_date" not in table.c and "delist_date" not in table.c:
            return {}
        list_date = table.c.list_date if "list_date" in table.c else null()
        delist_date = table.c.delist_date if "delist_date" in table.c else null()
        instrument = table.c[table.info["code_field"]]
        query = select(instrument, list_date, delist_date).where(instrument != "")
        with self._read_connection() as conn:
            lifetimes = {}
            for instrument, listed, delisted in conn.execute(query):
                first = normalize_date(listed) or None
                last = normalize_date(delisted) or None
                if instrument in lifetimes:
                    previous_first, previous_last = lifetimes[instrument]
                    first = min(previous_first, first) if previous_first and first else None
                    last = max(previous_last, last) if previous_last and last else None
                lifetimes[instrument] = (first, last)
            return {instrument: bounds for instrument, bounds in lifetimes.items() if any(bounds)}

    def trading_days(self, start, end, *, exchange=None):
        table = business_tables["trade_cal"]
        event_date = event_date_column(table, table.info, self.backend)
        first, last = normalize_date(start), normalize_date(end)
        query = select(event_date).where(
            table.c.is_open == "1", event_date.between(first, last),
        ).distinct().order_by(event_date)
        candidates = indexed_date_filter(table, table.info, first, last)
        if candidates is not None:
            query = query.where(candidates)
        if exchange:
            query = query.where(table.c.exchange == exchange)
        with self._read_connection() as conn:
            return list(conn.scalars(query))

    def calendar_sessions(self, start, end, *, exchange=None, dataset_id="trade_cal"):
        """Return only dates with an explicit open/closed status in the selected market."""
        table = business_tables[dataset_id]
        event_date = event_date_column(table, table.info, self.backend)
        first, last = normalize_date(start), normalize_date(end)
        query = select(event_date, func.max(table.c.is_open)).where(
            event_date.between(first, last), table.c.is_open.in_(("0", "1")),
        ).group_by(event_date)
        candidates = indexed_date_filter(table, table.info, first, last)
        if candidates is not None:
            query = query.where(candidates)
        if exchange is not None:
            query = query.where(table.c.exchange == exchange)
        with self._read_connection() as conn:
            sessions = {}
            for day, status in conn.execute(query):
                try:
                    sessions[date.fromisoformat(day)] = str(status) == "1"
                except ValueError:
                    continue
            return sessions

    def calendar_covers(self, start, end, *, exchange=None):
        first = date.fromisoformat(normalize_date(start))
        last = date.fromisoformat(normalize_date(end))
        count = (last - first) // timedelta(days=1) + 1
        if count <= 0 or not exchange:
            return False
        table = business_tables["trade_cal"]
        event_date = event_date_column(table, table.info, self.backend)
        query = select(func.count(func.distinct(event_date))).where(
            event_date.between(first.isoformat(), last.isoformat()), table.c.exchange == exchange,
        )
        candidates = indexed_date_filter(table, table.info, first.isoformat(), last.isoformat())
        if candidates is not None:
            query = query.where(candidates)
        with self._read_connection() as conn:
            return conn.scalar(query) == count

    def read_operation(self, name, default=None):
        with self._read_connection() as conn:
            payload = conn.scalar(select(operations.c.payload).where(operations.c.name == name))
        return default if payload is None else json.loads(payload)

    def write_operation(self, name, payload):
        with self._write_transaction() as conn:
            self._upsert(conn, operations, [{"name": name, "payload": json_text(payload)}], ["payload"])
