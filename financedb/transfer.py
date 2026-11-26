"""Transfer an offline database or build a field-limited release database."""

from contextlib import contextmanager
from hashlib import sha256

from sqlalchemy import select

from .business_tables import business_tables
from .catalog import load_profiles
from .storage import Store, dataset_counts, json_text, metadata, normalize_date, operations, schema_versions, timestamp


LOCAL_CONFIGURATION = ("database_target", "tushare_token")


@contextmanager
def snapshot(store):
    with store.engine.connect() as conn:
        if store.backend == "mysql":
            conn = conn.execution_options(isolation_level="REPEATABLE READ")
        with conn.begin():
            if store.backend == "sqlite":
                conn.exec_driver_sql("BEGIN")
            yield conn


def _batches(conn, table, *, columns=None, chunk_size=1000):
    if columns is None:
        columns = [column for column in table.columns if column.name != "_payload"]
    statement = select(*columns).select_from(table).order_by(*table.primary_key.columns)
    if table is operations:
        statement = statement.where(operations.c.name.not_in(LOCAL_CONFIGURATION))
    with conn.execution_options(stream_results=True).execute(statement) as result:
        for batch in result.mappings().partitions(chunk_size):
            yield [dict(row) for row in batch]


def _signature(conn, table):
    digest = sha256()
    count = 0
    for batch in _batches(conn, table):
        count += len(batch)
        for row in batch:
            digest.update(json_text(row).encode())
            digest.update(b"\n")
    return count, digest.hexdigest()


def _check_new_database(store):
    with store.engine.connect() as conn:
        for table in _data_tables():
            if conn.scalar(select(1).select_from(table).limit(1)):
                raise ValueError("目标库已有数据，请指定空库")


def _data_tables():
    yield from business_tables.values()
    yield from (table for table in metadata.sorted_tables if table is not schema_versions)


def _assert_distinct(source, target):
    if source.identity == target.identity:
        raise ValueError("源库与目标库必须不同")


def migrate(source_url, target_url):
    source, target = Store(source_url, read_only=True), Store(target_url)
    try:
        _assert_distinct(source, target)
        source.validate_schema()
        target.initialize()
        _check_new_database(target)
        report = {}
        with snapshot(source) as read_conn:
            for table in _data_tables():
                digest = sha256()
                count = 0
                for batch in _batches(read_conn, table):
                    with target.engine.begin() as write_conn:
                        write_conn.execute(table.insert(), batch)
                    count += len(batch)
                    for row in batch:
                        digest.update(json_text(row).encode())
                        digest.update(b"\n")
                with snapshot(target) as check_conn:
                    actual = _signature(check_conn, table)
                if actual != (count, digest.hexdigest()):
                    raise RuntimeError(f"迁移校验失败：{table.name}")
                report[table.name] = {"rows": count, "sha256": actual[1]}
        return report
    finally:
        source.close()
        target.close()


def build_release(source_url, target_url, allowed_fields):
    if not isinstance(allowed_fields, dict) or not allowed_fields:
        raise ValueError("发布范围应为数据集到字段列表的映射")
    if any(not isinstance(key, str) or not key or not isinstance(fields, list) or not fields or
           not all(isinstance(field, str) and field for field in fields) for key, fields in allowed_fields.items()):
        raise ValueError("每个发布数据集至少要指定一个有效字段")
    profiles = load_profiles()
    for dataset_id, fields in allowed_fields.items():
        profile = profiles.get(dataset_id)
        if not profile or len(fields) != len(set(fields)) or not set(fields) <= set(profile["fields"]):
            raise ValueError(f"发布范围含未登记的数据集或字段：{dataset_id}")
    source, target = Store(source_url, read_only=True), Store(target_url)
    try:
        _assert_distinct(source, target)
        source.validate_schema()
        target.initialize()
        _check_new_database(target)
        count_by_dataset = {}
        released_at = timestamp()
        summaries = []
        with snapshot(source) as read_conn:
            for dataset_id, fields in allowed_fields.items():
                count = 0
                profile = profiles[dataset_id]
                table = business_tables[dataset_id]
                columns = [table.c[field] for field in fields]
                date_field, code_field = profile.get("date_field"), profile.get("code_field")
                for batch in _batches(read_conn, table, columns=columns):
                    released = []
                    for ordinal, row in enumerate(batch, count + 1):
                        public = {field: row[field] for field in fields}
                        content_hash = sha256(json_text(public).encode()).hexdigest()
                        released.append(public | {
                            "_content_hash": content_hash,
                            "_record_key": sha256(json_text([dataset_id, ordinal, content_hash]).encode()).hexdigest(),
                            "_event_date": normalize_date(public.get(date_field)),
                            "_instrument": str(public.get(code_field) or ""),
                            "_observed_at": released_at,
                            "_version_order": released_at,
                        })
                    with target.engine.begin() as write_conn:
                        write_conn.execute(table.insert(), released)
                    count += len(batch)
                count_by_dataset[dataset_id] = count
                if count:
                    with target.engine.connect() as conn:
                        summary = dict(conn.execute(target.business_summary(table)).mappings().one())
                    summaries.append({"dataset_id": dataset_id, **summary})
        with target.engine.begin() as conn:
            if summaries:
                conn.execute(dataset_counts.insert(), summaries)
            manifest = {"type": "financedb-release-v1", "created_at": released_at,
                        "fields": allowed_fields, "rows": count_by_dataset}
            conn.execute(operations.insert().values(name="release_manifest", payload=json_text(manifest)))
        return count_by_dataset
    finally:
        source.close()
        target.close()
