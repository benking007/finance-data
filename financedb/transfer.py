"""Transfer an offline database or build a field-limited release database."""

from contextlib import contextmanager
from hashlib import sha256
import json

from sqlalchemy import func, select

from .catalog import load_profiles
from .storage import Store, dataset_counts, json_text, metadata, operations, rows_table, schema_versions, timestamp


@contextmanager
def snapshot(store):
    with store.engine.connect() as conn:
        if store.backend == "mysql":
            conn = conn.execution_options(isolation_level="REPEATABLE READ")
        with conn.begin():
            if store.backend == "sqlite":
                conn.exec_driver_sql("BEGIN")
            yield conn


def _batches(conn, table, conditions=(), *, chunk_size=1000):
    statement = select(table).where(*conditions).order_by(*table.primary_key.columns)
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
        for table in metadata.sorted_tables:
            rows = select(table).limit(1)
            if table is operations:
                rows = rows.where(operations.c.name != "dataset_counts_v1")
            if table is not schema_versions and conn.scalar(select(func.count()).select_from(rows.subquery())):
                raise ValueError("目标库已有数据，请指定空库")


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
            for table in metadata.sorted_tables:
                if table is schema_versions:
                    continue
                digest = sha256()
                count = 0
                for batch in _batches(read_conn, table):
                    with target.engine.begin() as write_conn:
                        if table is operations:
                            target._upsert(write_conn, table, batch, [])
                        else:
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
        if not profile or not set(fields) <= set(profile["fields"]):
            raise ValueError(f"发布范围含未登记的数据集或字段：{dataset_id}")
    source, target = Store(source_url, read_only=True), Store(target_url)
    try:
        _assert_distinct(source, target)
        source.validate_schema()
        target.initialize()
        _check_new_database(target)
        count_by_dataset = {}
        released_at = timestamp()
        with snapshot(source) as read_conn:
            for dataset_id, fields in allowed_fields.items():
                count = 0
                profile = profiles[dataset_id]
                for batch in _batches(read_conn, rows_table, [rows_table.c.dataset_id == dataset_id]):
                    for ordinal, row in enumerate(batch, count + 1):
                        original = json.loads(row["payload"])
                        row["payload"] = json_text({key: original[key] for key in fields if key in original})
                        row["content_hash"] = sha256(row["payload"].encode()).hexdigest()
                        row["record_key"] = sha256(json_text([dataset_id, ordinal, row["content_hash"]]).encode()).hexdigest()
                        row["event_date"] = row["event_date"] if profile.get("date_field") in fields else ""
                        row["instrument"] = row["instrument"] if profile.get("code_field") in fields else ""
                        row["observed_at"] = row["version_order"] = released_at
                    with target.engine.begin() as write_conn:
                        write_conn.execute(rows_table.insert(), batch)
                    count += len(batch)
                count_by_dataset[dataset_id] = count
        with target.engine.begin() as conn:
            summaries = target._aggregate_rows().group_by(rows_table.c.dataset_id)
            target._upsert(conn, dataset_counts, [dict(row) for row in conn.execute(summaries).mappings()], [])
            manifest = {"type": "financedb-release-v1", "created_at": released_at,
                        "fields": allowed_fields, "rows": count_by_dataset}
            target._upsert(conn, operations, [{"name": "release_manifest", "payload": json_text(manifest)}], [])
        return count_by_dataset
    finally:
        source.close()
        target.close()
