"""Shared SQL storage for SQLite and MySQL."""

import base64
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path

from sqlalchemy import (
    BigInteger, Column, Index, Integer, MetaData, String, Table, Text,
    and_, create_engine, event, func, or_, select,
)
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.engine import make_url


DEFAULT_DATABASE_URL = "sqlite:///data/finance.sqlite3"
metadata = MetaData()
long_text = Text().with_variant(mysql.LONGTEXT(), "mysql")
mysql_options = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_bin"}

schema_versions = Table(
    "schema_versions", metadata,
    Column("version", Integer, primary_key=True), Column("applied_at", String(32), nullable=False),
    **mysql_options,
)
rows_table = Table(
    "dataset_rows", metadata,
    Column("dataset_id", String(80), primary_key=True),
    Column("record_key", String(64), primary_key=True),
    Column("event_date", String(32), nullable=False),
    Column("instrument", String(128), nullable=False),
    Column("payload", long_text, nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("version_order", String(96), nullable=False),
    Column("observed_at", String(32), nullable=False),
    **mysql_options,
)
Index("ix_rows_dataset_date", rows_table.c.dataset_id, rows_table.c.event_date, rows_table.c.record_key)
Index("ix_rows_dataset_code_date", rows_table.c.dataset_id, rows_table.c.instrument, rows_table.c.event_date, rows_table.c.record_key)

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


def _row_values(profile, row, observed_at):
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
        "dataset_id": dataset_id, "record_key": record_key, "event_date": event_date,
        "instrument": instrument, "payload": payload, "content_hash": content_hash,
        "version_order": priority, "observed_at": observed_at,
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
        metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            self._upsert(conn, schema_versions, [{"version": 1, "applied_at": timestamp()}], [])
            count_ready = conn.scalar(select(operations.c.payload).where(operations.c.name == "dataset_counts_v1"))
            if count_ready is None:
                summary = self._aggregate_rows().group_by(rows_table.c.dataset_id)
                self._upsert(conn, dataset_counts, [dict(row) for row in conn.execute(summary).mappings()], [])
                self._upsert(conn, operations, [{"name": "dataset_counts_v1", "payload": "true"}], [])
        self.validate_schema()

    def validate_schema(self):
        try:
            with self.engine.connect() as conn:
                version = conn.scalar(select(func.max(schema_versions.c.version)))
        except (OperationalError, ProgrammingError) as exc:
            raise SchemaMismatch("Initialize the finance database first") from exc
        if version != 1:
            raise SchemaMismatch(f"Expected database schema 1, found {version}")

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
        observed_at = timestamp()
        current = {}
        history = {}
        for row in rows:
            values = _row_values(profile, row, observed_at)
            key = values["record_key"]
            if key not in current or values["version_order"] > current[key]["version_order"]:
                current[key] = values
            if profile.get("versioned") or profile.get("revision_field") or _profile_id(profile) in {"income", "balancesheet", "cashflow", "fina_indicator", "cn_cpi"}:
                revision = {field: values[field] for field in ("dataset_id", "record_key", "content_hash", "payload", "event_date", "instrument")}
                revision["first_seen_at"] = observed_at
                history[(key, values["content_hash"])] = revision
        batch = {
            "dataset_id": _profile_id(profile), "partition_id": partition_id, "params": json_text(params),
            "status": status, "source_count": len(rows) if source_count is None else int(source_count),
            "row_count": len(current), "message": "", "updated_at": observed_at,
        }
        with self.engine.begin() as conn:
            self._save_dataset_rows(conn, batch["dataset_id"], current)
            self._upsert(conn, revisions, list(history.values()), [])
            self._save_partition(conn, batch)
        return {**batch, "params": params}

    @staticmethod
    def _aggregate_rows():
        return select(rows_table.c.dataset_id, func.count().label("row_count"),
                      func.min(func.nullif(rows_table.c.event_date, "")).label("min_date"),
                      func.max(func.nullif(rows_table.c.event_date, "")).label("max_date"))

    def _save_dataset_rows(self, conn, dataset_id, current):
        if not current:
            return
        self._upsert(conn, dataset_counts, [{"dataset_id": dataset_id, "row_count": 0, "min_date": None, "max_date": None}], [])
        totals = dict(conn.execute(select(dataset_counts).where(dataset_counts.c.dataset_id == dataset_id).with_for_update()).mappings().one())
        before = {}
        keys = list(current)
        for offset in range(0, len(keys), 500):
            query = select(rows_table.c.record_key, rows_table.c.event_date, rows_table.c.version_order).where(
                rows_table.c.dataset_id == dataset_id, rows_table.c.record_key.in_(keys[offset:offset + 500]),
            )
            before.update({row.record_key: row for row in conn.execute(query)})
        self._upsert(conn, rows_table, list(current.values()),
                     ["event_date", "instrument", "payload", "content_hash", "observed_at", "version_order"],
                     guard=lambda incoming: incoming.version_order >= rows_table.c.version_order)
        totals["row_count"] += len(current) - len(before)
        changed_extreme = False
        for key, new in current.items():
            old = before.get(key)
            if old and new["version_order"] < old.version_order:
                continue
            old_date, new_date = old.event_date if old else "", new["event_date"]
            if old_date and old_date != new_date and old_date in {totals["min_date"], totals["max_date"]}:
                changed_extreme = True
            if new_date:
                totals["min_date"] = min(totals["min_date"] or new_date, new_date)
                totals["max_date"] = max(totals["max_date"] or new_date, new_date)
        if changed_extreme:
            extrema = self._aggregate_rows().where(rows_table.c.dataset_id == dataset_id).group_by(rows_table.c.dataset_id)
            totals = dict(conn.execute(extrema).mappings().one())
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
        date_start, date_end = normalize_date(start), normalize_date(end)
        query_id = _digest(json_text([dataset_id, code, date_start, date_end]))[:20]
        query = select(rows_table.c.payload, rows_table.c.event_date, rows_table.c.record_key).where(rows_table.c.dataset_id == dataset_id)
        if code is not None:
            query = query.where(rows_table.c.instrument == code)
        if date_start:
            query = query.where(rows_table.c.event_date >= date_start)
        if date_end:
            query = query.where(rows_table.c.event_date <= date_end)
        if cursor:
            cursor_date, cursor_key = _parse_cursor(cursor, query_id)
            query = query.where(or_(rows_table.c.event_date < cursor_date,
                                    and_(rows_table.c.event_date == cursor_date, rows_table.c.record_key > cursor_key)))
        query = query.order_by(rows_table.c.event_date.desc(), rows_table.c.record_key).limit(limit + 1)
        with self.engine.connect() as conn:
            batch = conn.execute(query).all()
        token = None
        if len(batch) > limit:
            batch = batch[:limit]
            token = _cursor([query_id, batch[-1].event_date, batch[-1].record_key])
        return {"rows": [json.loads(row.payload) for row in batch], "next_cursor": token}

    def instrument_codes(self, dataset_id):
        with self.engine.connect() as conn:
            return list(conn.scalars(select(rows_table.c.instrument).distinct().where(
                rows_table.c.dataset_id == dataset_id, rows_table.c.instrument != "",
            ).order_by(rows_table.c.instrument)))

    def instrument_values(self, dataset_id, field=None, *, event_date=None):
        if field is None and event_date is None:
            return self.instrument_codes(dataset_id)
        value = rows_table.c.payload if field else rows_table.c.instrument
        query = select(value).where(rows_table.c.dataset_id == dataset_id)
        if event_date is not None:
            query = query.where(rows_table.c.event_date == normalize_date(event_date))
        if not field:
            query = query.distinct()
        with self.engine.connect() as conn:
            values = conn.scalars(query)
            if field:
                values = (json.loads(payload).get(field) for payload in values)
            return sorted({str(item) for item in values if item not in (None, "")})

    def instrument_lifetimes(self, dataset_id):
        query = select(rows_table.c.instrument, rows_table.c.payload).where(
            rows_table.c.dataset_id == dataset_id, rows_table.c.instrument != "",
        )
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
        query = select(rows_table.c.event_date, rows_table.c.payload).where(
            rows_table.c.dataset_id == "trade_cal", rows_table.c.event_date >= normalize_date(start),
            rows_table.c.event_date <= normalize_date(end),
        )
        if exchange:
            query = query.where(rows_table.c.instrument == exchange)
        with self.engine.connect() as conn:
            return sorted({row.event_date for row in conn.execute(query) if str(json.loads(row.payload).get("is_open")) == "1"})

    def calendar_covers(self, start, end, *, exchange=None):
        first = date.fromisoformat(normalize_date(start))
        last = date.fromisoformat(normalize_date(end))
        count = (last - first) // timedelta(days=1) + 1
        if count <= 0 or not exchange:
            return False
        query = select(func.count(func.distinct(rows_table.c.event_date))).where(
            rows_table.c.dataset_id == "trade_cal", rows_table.c.event_date >= first.isoformat(),
            rows_table.c.event_date <= last.isoformat(), rows_table.c.instrument == exchange,
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
