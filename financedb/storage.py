"""Shared SQL storage for SQLite and MySQL."""

import base64
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path

from sqlalchemy import (
    BigInteger, Column, Index, Integer, MetaData, String, Table, Text,
    and_, create_engine, event, func, inspect, or_, select,
)
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.engine import make_url

from .business_tables import business_metadata, business_tables, project_fields


DEFAULT_DATABASE_URL = "sqlite:///data/finance-v2.sqlite3"
SCHEMA_VERSION = 2
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


def reject_legacy_schema(conn):
    existing = set(inspect(conn).get_table_names())
    if {"account_users", "account_sessions"} & existing:
        raise SchemaMismatch("账号库必须与业务数据库分开，请选择业务数据库")
    if "dataset_rows" in existing:
        raise SchemaMismatch("检测到旧版单表存储，请使用新的空数据库初始化业务表")
    if schema_versions.name in existing:
        version = conn.scalar(select(func.max(schema_versions.c.version)))
        if version is not None and version != SCHEMA_VERSION:
            raise SchemaMismatch(f"不支持数据库结构版本 {version}，请使用新的空数据库初始化业务表")
    return existing


def create_schema(conn):
    existing = reject_legacy_schema(conn)
    expected = {**metadata.tables, **business_tables}
    if existing - expected.keys() - {"alembic_version"}:
        raise SchemaMismatch("数据库含有其他应用的表，请使用独立的业务数据库")
    if existing and (schema_versions.name not in existing or conn.scalar(select(func.max(schema_versions.c.version))) is None):
        inspector = inspect(conn)
        for name in existing & expected.keys():
            actual = {column["name"].lower() for column in inspector.get_columns(name)}
            if actual != {column.name.lower() for column in expected[name].columns}:
                raise SchemaMismatch(f"数据库中的表 {name} 与业务结构不一致，请使用新的空数据库")
    metadata.create_all(conn, tables=[table for table in metadata.sorted_tables if table.name not in existing])
    business_metadata.create_all(conn, tables=[table for table in business_tables.values() if table.name not in existing])
    if conn.scalar(select(schema_versions.c.version).where(schema_versions.c.version == SCHEMA_VERSION)) is None:
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


def _row_values(table, profile, row, observed_at):
    dataset_id = _profile_id(profile)
    payload = json_text(row)
    content_hash = _digest(payload)
    keys = profile.get("key", [])
    if isinstance(keys, str):
        keys = [keys]
    if keys and all(key in row and row[key] not in (None, "") for key in keys):
        record_key = _digest(json_text([row[key] for key in keys]))
    elif keys and all(key in row for key in keys) and any(row[key] not in (None, "") for key in keys):
        record_key = _digest(json_text([row[key] for key in keys] + [content_hash]))
    elif keys:
        raise ValueError(f"{dataset_id} returned a row without its business key")
    else:
        record_key = content_hash
    event_date = normalize_date(row.get(profile.get("date_field")))
    instrument = str(row.get(profile.get("code_field")) or "")
    revision_field = profile.get("revision_field")
    if revision_field or dataset_id in {"income", "balancesheet", "cashflow", "fina_indicator"}:
        release_date = normalize_date(row.get(revision_field or "f_ann_date") or row.get("ann_date"))
        priority = f"{release_date:10}:{int(str(row.get('update_flag')) == '1')}:{observed_at}"
    else:
        priority = observed_at
    return {
        **project_fields(table, row),
        "_record_key": record_key, "_event_date": event_date, "_instrument": instrument,
        "_payload": payload, "_content_hash": content_hash, "_version_order": priority, "_observed_at": observed_at,
    }


def _cursor(parts):
    return base64.urlsafe_b64encode(json_text(parts).encode()).decode().rstrip("=")


def _parse_cursor(value, query_id):
    try:
        if len(value) > 512:
            raise ValueError
        revision, event_date, record_key = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if revision != query_id or not isinstance(event_date, str) or not isinstance(record_key, str):
            raise ValueError
        if len(event_date) > 32 or len(record_key) != 64:
            raise ValueError
        return event_date, record_key
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
        if backend == "sqlite":
            if url.database and url.database != ":memory:":
                sqlite_path = Path(url.database).expanduser().resolve()
                self.identity = (backend, sqlite_path)
                if read_only:
                    url = url.set(database=f"{sqlite_path.as_uri()}?mode=ro", query={"uri": "true"})
                else:
                    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
                    url = url.set(database=str(sqlite_path))
            self.engine = create_engine(url, connect_args={"timeout": 15, "check_same_thread": False})
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

    def initialize(self):
        if self.read_only:
            self.validate_schema()
            return
        with self.engine.begin() as conn:
            create_schema(conn)

    def validate_schema(self):
        try:
            with self.engine.connect() as conn:
                reject_legacy_schema(conn)
                version = conn.scalar(select(func.max(schema_versions.c.version)))
        except (OperationalError, ProgrammingError) as exc:
            raise SchemaMismatch("Initialize the finance database first") from exc
        if version != SCHEMA_VERSION:
            raise SchemaMismatch("请先初始化业务数据库")

    def close(self):
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

    def _save_partition(self, conn, values, *, keep_complete=False):
        update_columns = ["params", "source_count", "row_count", "message", "updated_at", "status"]
        guard = (lambda incoming: ~partitions.c.status.in_(("complete", "empty"))) if keep_complete else None
        self._upsert(conn, partitions, [values], update_columns, guard=guard)

    def partition_done(self, dataset_id, partition_id):
        with self.engine.connect() as conn:
            result = conn.scalar(select(partitions.c.status).where(
                partitions.c.dataset_id == dataset_id, partitions.c.partition_id == partition_id,
            ))
        return result in {"complete", "empty"}

    def commit_partition(self, profile, partition_id, params, rows, *, status="complete", source_count=None):
        dataset_id = _profile_id(profile)
        table = business_tables[dataset_id]
        observed_at = timestamp()
        current = {}
        history = {}
        versioned = profile.get("versioned") or profile.get("revision_field") or dataset_id in {
            "income", "balancesheet", "cashflow", "fina_indicator", "cn_cpi",
        }
        for row in rows:
            values = _row_values(table, profile, row, observed_at)
            key = values["_record_key"]
            if key not in current or values["_version_order"] > current[key]["_version_order"]:
                current[key] = values
            if versioned:
                revision = {field: values[f"_{field}"] for field in ("record_key", "content_hash", "payload", "event_date", "instrument")}
                revision.update(dataset_id=dataset_id, first_seen_at=observed_at)
                history[(key, values["_content_hash"])] = revision
        batch = {
            "dataset_id": dataset_id, "partition_id": partition_id, "params": json_text(params),
            "status": status, "source_count": len(rows) if source_count is None else int(source_count),
            "row_count": len(current), "message": "", "updated_at": observed_at,
        }
        with self.engine.begin() as conn:
            self._save_dataset_rows(conn, table, dataset_id, current)
            self._upsert(conn, revisions, list(history.values()), [])
            self._save_partition(conn, batch)
        return {**batch, "params": params}

    @staticmethod
    def business_summary(table):
        event_date = func.nullif(table.c._event_date, "")
        return select(func.count().label("row_count"), func.min(event_date).label("min_date"),
                      func.max(event_date).label("max_date")).select_from(table)

    def _save_dataset_rows(self, conn, table, dataset_id, current):
        if not current:
            return
        self._upsert(conn, dataset_counts, [{"dataset_id": dataset_id, "row_count": 0, "min_date": None, "max_date": None}], [])
        totals = dict(conn.execute(select(dataset_counts).where(dataset_counts.c.dataset_id == dataset_id).with_for_update()).mappings().one())
        before = {}
        keys = list(current)
        for offset in range(0, len(keys), 500):
            query = select(table.c._record_key, table.c._event_date, table.c._version_order).where(
                table.c._record_key.in_(keys[offset:offset + 500]))
            before.update({row._record_key: row for row in conn.execute(query)})
        update_columns = [col.name for col in table.columns if col.name not in {"_record_key", "_version_order"}]
        # MySQL evaluates ON DUPLICATE KEY assignments left to right.
        self._upsert(conn, table, list(current.values()), [*update_columns, "_version_order"],
                     guard=lambda incoming: incoming._version_order >= table.c._version_order)
        totals["row_count"] += len(current) - len(before)
        changed_extreme = False
        for key, new in current.items():
            old = before.get(key)
            if old and new["_version_order"] < old._version_order:
                continue
            old_date, new_date = old._event_date if old else "", new["_event_date"]
            if old_date and old_date != new_date and old_date in {totals["min_date"], totals["max_date"]}:
                changed_extreme = True
            if new_date:
                totals["min_date"] = min(totals["min_date"] or new_date, new_date)
                totals["max_date"] = max(totals["max_date"] or new_date, new_date)
        if changed_extreme:
            totals = {"dataset_id": dataset_id, **conn.execute(self.business_summary(table)).mappings().one()}
        self._upsert(conn, dataset_counts, [totals], ["row_count", "min_date", "max_date"])

    def record_failure(self, profile, partition_id, params, message, *, status="failed"):
        batch = {
            "dataset_id": _profile_id(profile), "partition_id": partition_id, "params": json_text(params),
            "status": status, "source_count": 0, "row_count": 0, "message": str(message)[:1024], "updated_at": timestamp(),
        }
        with self.engine.begin() as conn:
            self._save_partition(conn, batch, keep_complete=True)

    def list_stats(self):
        summaries = {}
        dataset_query = select(dataset_counts)
        partition_query = select(
            partitions.c.dataset_id, partitions.c.status, func.count().label("count"),
            func.max(partitions.c.updated_at).label("last_sync"),
        ).group_by(partitions.c.dataset_id, partitions.c.status)
        with self.engine.connect() as conn:
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

    def list_stats_for(self, dataset_id):
        item = self._empty_stats(dataset_id)
        query = select(dataset_counts).where(dataset_counts.c.dataset_id == dataset_id)
        by_status = select(partitions.c.status, func.count(), func.max(partitions.c.updated_at)).where(
            partitions.c.dataset_id == dataset_id).group_by(partitions.c.status)
        with self.engine.connect() as conn:
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
        with self.engine.connect() as conn:
            return [dict(row) | {"params": json.loads(row["params"])} for row in conn.execute(query).mappings()]

    def query_rows(self, profile, *, code=None, start=None, end=None, cursor=None, limit=100):
        dataset_id = _profile_id(profile)
        table = business_tables[dataset_id]
        date_start, date_end = normalize_date(start), normalize_date(end)
        query_id = _digest(json_text([dataset_id, code, date_start, date_end]))[:20]
        query = select(table.c._payload, table.c._event_date, table.c._record_key)
        if code is not None:
            query = query.where(table.c._instrument == code)
        if date_start:
            query = query.where(table.c._event_date >= date_start)
        if date_end:
            query = query.where(table.c._event_date <= date_end)
        if cursor:
            cursor_date, cursor_key = _parse_cursor(cursor, query_id)
            query = query.where(or_(table.c._event_date < cursor_date,
                                    and_(table.c._event_date == cursor_date, table.c._record_key > cursor_key)))
        query = query.order_by(table.c._event_date.desc(), table.c._record_key).limit(limit + 1)
        with self.engine.connect() as conn:
            batch = conn.execute(query).all()
        token = None
        if len(batch) > limit:
            batch = batch[:limit]
            token = _cursor([query_id, batch[-1]._event_date, batch[-1]._record_key])
        return {"rows": [json.loads(row._payload) for row in batch], "next_cursor": token}

    def instrument_codes(self, dataset_id):
        table = business_tables.get(dataset_id)
        if table is None:
            return []
        with self.engine.connect() as conn:
            return list(conn.scalars(select(table.c._instrument).distinct().where(
                table.c._instrument != "",
            ).order_by(table.c._instrument)))

    def instrument_values(self, dataset_id, field=None, *, event_date=None):
        if field is None and event_date is None:
            return self.instrument_codes(dataset_id)
        table = business_tables.get(dataset_id)
        if table is None:
            return []
        query = select(table.c._payload if field else table.c._instrument)
        if event_date is not None:
            query = query.where(table.c._event_date == normalize_date(event_date))
        if not field:
            query = query.distinct()
        with self.engine.connect() as conn:
            values = conn.scalars(query)
            if field:
                values = (json.loads(payload).get(field) for payload in values)
            return sorted({str(item) for item in values if item not in (None, "")})

    def instrument_lifetimes(self, dataset_id):
        table = business_tables.get(dataset_id)
        if table is None:
            return {}
        query = select(table.c._instrument, table.c._payload).where(table.c._instrument != "")
        with self.engine.connect() as conn:
            lifetimes = {}
            for instrument, payload in conn.execute(query):
                row = json.loads(payload)
                first = normalize_date(row.get("list_date")) or None
                last = normalize_date(row.get("delist_date")) or None
                if first or last:
                    lifetimes[instrument] = (first, last)
            return lifetimes

    def trading_days(self, start, end, *, exchange=None):
        table = business_tables["trade_cal"]
        query = select(table.c._event_date, table.c._payload).where(
            table.c._event_date >= normalize_date(start), table.c._event_date <= normalize_date(end),
        )
        if exchange:
            query = query.where(table.c._instrument == exchange)
        with self.engine.connect() as conn:
            return sorted({event_date for event_date, payload in conn.execute(query) if str(json.loads(payload).get("is_open")) == "1"})

    def calendar_covers(self, start, end, *, exchange=None):
        first = date.fromisoformat(normalize_date(start))
        last = date.fromisoformat(normalize_date(end))
        count = (last - first) // timedelta(days=1) + 1
        if count <= 0 or not exchange:
            return False
        table = business_tables["trade_cal"]
        query = select(func.count(func.distinct(table.c._event_date))).where(
            table.c._event_date >= first.isoformat(), table.c._event_date <= last.isoformat(), table.c._instrument == exchange,
        )
        with self.engine.connect() as conn:
            return conn.scalar(query) == count

    def read_operation(self, name, default=None):
        with self.engine.connect() as conn:
            payload = conn.scalar(select(operations.c.payload).where(operations.c.name == name))
        return default if payload is None else json.loads(payload)

    def write_operation(self, name, payload):
        with self.engine.begin() as conn:
            self._upsert(conn, operations, [{"name": name, "payload": json_text(payload)}], ["payload"])
