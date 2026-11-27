"""Remove ingestion metadata from existing physical business tables."""

from sqlalchemy import MetaData, inspect
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import CreateIndex, CreateTable

from .business_tables import business_tables


LEGACY_DATASET_FIELDS = {"fut_tick": ("source_sha256", "source_row")}


class FactorValueStorageMismatch(RuntimeError):
    """The existing MySQL factor value cannot store the current representation."""


def ensure_factor_value_storage(conn, existing, *, upgrade):
    """Widen the legacy MySQL column without changing rows or checkpoints."""
    if conn.dialect.name not in {"mysql", "mariadb"} or "factor_value" not in existing:
        return
    columns = inspect(conn).get_columns("factor_value")
    actual = next((column["type"] for column in columns if column["name"].lower() == "factor_value"), None)
    if isinstance(actual, mysql.LONGTEXT):
        return
    if not isinstance(actual, mysql.DECIMAL):
        raise FactorValueStorageMismatch("MySQL 业务表 factor_value 的因子值列类型不符合业务结构")
    if not upgrade:
        raise FactorValueStorageMismatch("MySQL 因子值列需要升级。请停止采集和服务，使用同一目标库连接运行 financedb init")
    conn.exec_driver_sql("ALTER TABLE `factor_value` MODIFY COLUMN `factor_value` LONGTEXT NULL")


def check_mysql_business_schema(conn):
    if conn.dialect.name not in {"mysql", "mariadb"}:
        return
    forced = conn.exec_driver_sql(
        "SHOW VARIABLES WHERE Variable_name IN "
        "('sql_require_primary_key', 'sql_generate_invisible_primary_key', 'innodb_force_primary_key')"
    ).all()
    enabled = [name for name, value in forced if str(value).upper() in {"ON", "1"}]
    if enabled:
        raise RuntimeError(f"MySQL 设置 {', '.join(enabled)} 要求或自动生成主键，请关闭后再初始化或迁移业务表")


def _column_plans(conn, existing, *, upgrade):
    names = sorted(set(existing) & business_tables.keys())
    if not names:
        return {}
    reflected = inspect(conn).get_multi_columns(filter_names=names)
    plans = {}
    for name in names:
        expected = {column.name.lower() for column in business_tables[name].columns}
        actual = {column["name"].lower(): column["name"] for column in reflected[None, name]}
        missing = expected - actual.keys()
        extra = actual.keys() - expected
        legacy = {column for column in extra if column.startswith("_") or column in LEGACY_DATASET_FIELDS.get(name, ())}
        if missing or extra - legacy:
            details = []
            if missing:
                details.append(f"缺少业务列 {', '.join(sorted(missing))}")
            if extra - legacy:
                details.append(f"存在未识别列 {', '.join(sorted(extra - legacy))}")
            raise RuntimeError(f"业务表 {name} 结构不符合目录定义，{'，'.join(details)}")
        if legacy and not upgrade:
            raise RuntimeError(f"业务表 {name} 含旧版非业务列，请先迁移数据库")
        if legacy:
            plans[name] = {actual[column] for column in legacy}
    return plans


def assert_business_columns(conn, existing):
    """Check every existing business table against its registered field list."""
    _column_plans(conn, existing, upgrade=False)


def _sqlite_prepare(conn, names):
    # The sqlite3 driver may defer BEGIN until DML. Acquire the write lock
    # before any table is rebuilt so a failure rolls back every DDL change.
    if not conn.connection.driver_connection.in_transaction:
        conn.exec_driver_sql("BEGIN IMMEDIATE")
    references = conn.exec_driver_sql(
        'SELECT m.name, f."table" FROM sqlite_master AS m, pragma_foreign_key_list(m.name) AS f '
        "WHERE m.type = 'table'"
    ).all()
    lower_names = {name.lower() for name in names}
    for source, target in references:
        if source.lower() in lower_names or target.lower() in lower_names:
            raise RuntimeError(f"SQLite 自定义外键 {source} → {target} 与业务表迁移冲突")
    for trigger, table in conn.exec_driver_sql("SELECT name, tbl_name FROM sqlite_master WHERE type = 'trigger'"):
        if table.lower() in lower_names:
            raise RuntimeError(f"SQLite 自定义触发器 {trigger} 与业务表 {table} 迁移冲突")


def _sqlite_rebuild(conn, table, removed):
    quote = conn.dialect.identifier_preparer.quote_identifier
    name = quote(table.name)
    staged = table.to_metadata(MetaData(), name=f"{table.name}__fdb_schema3")
    staged_name = quote(staged.name)
    columns = ", ".join(quote(column.name) for column in table.columns)
    current = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {name}").scalar_one()
    conn.execute(CreateTable(staged))
    conn.exec_driver_sql(f"INSERT INTO {staged_name} ({columns}) SELECT {columns} FROM {name}")
    if conn.exec_driver_sql(f"SELECT COUNT(*) FROM {staged_name}").scalar_one() != current:
        raise RuntimeError(f"业务表 {table.name} 迁移前后行数不一致")

    expected_indexes = {index.name for index in table.indexes}
    removed_lower = {column.lower() for column in removed}
    for _, index, _, origin, _ in conn.exec_driver_sql(f"PRAGMA index_list({name})"):
        if origin == "u":
            raise RuntimeError(f"SQLite 自定义唯一约束 {index} 与业务表 {table.name} 迁移冲突")
    custom_indexes = []
    for index, ddl in conn.exec_driver_sql(
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL", (table.name,)
    ).all():
        if index in expected_indexes:
            continue
        index_columns = conn.exec_driver_sql(f"PRAGMA index_info({quote(index)})").all()
        if not any(column is not None and column.lower() in removed_lower for _, _, column in index_columns):
            custom_indexes.append(ddl)

    conn.exec_driver_sql(f"DROP TABLE {name}")
    conn.exec_driver_sql(f"ALTER TABLE {staged_name} RENAME TO {name}")
    for index in sorted(table.indexes, key=lambda index: index.name):
        conn.execute(CreateIndex(index))
    for ddl in custom_indexes:
        conn.exec_driver_sql(ddl)


def _sqlite_migrate(conn, plans):
    _sqlite_prepare(conn, plans)
    legacy_alter = conn.exec_driver_sql("PRAGMA legacy_alter_table").scalar_one()
    conn.exec_driver_sql("PRAGMA legacy_alter_table = ON")
    try:
        for name, removed in plans.items():
            _sqlite_rebuild(conn, business_tables[name], removed)
        quote = conn.dialect.identifier_preparer.quote_identifier
        views = conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'view'").scalars().all()
        for name in views:
            try:
                conn.exec_driver_sql(f"SELECT * FROM {quote(name)} LIMIT 0")
            except DBAPIError as exc:
                raise RuntimeError(f"SQLite 视图 {name} 与清理后的业务表结构不兼容") from exc
    finally:
        conn.exec_driver_sql(f"PRAGMA legacy_alter_table = {int(legacy_alter)}")


def _mysql_migrate(conn, plans):
    check_mysql_business_schema(conn)
    quote = conn.dialect.identifier_preparer.quote_identifier
    inspector = inspect(conn)
    for name, removed in plans.items():
        legacy_columns = {column.lower() for column in removed}
        indexes = inspector.get_indexes(name)
        primary_key = inspector.get_pk_constraint(name).get("constrained_columns", ())
        clauses = []
        if any(column.lower() in legacy_columns for column in primary_key):
            clauses.append("DROP PRIMARY KEY")
        current_indexes = set()
        for index in indexes:
            if any(column is not None and column.lower() in legacy_columns for column in index["column_names"]):
                clauses.append(f"DROP INDEX {quote(index['name'])}")
            else:
                current_indexes.add(index["name"])
        clauses.extend(f"DROP COLUMN {quote(column)}" for column in sorted(removed))
        for index in sorted(business_tables[name].indexes, key=lambda index: index.name):
            if index.name not in current_indexes:
                ddl = str(CreateIndex(index).compile(dialect=conn.dialect))
                # Compile the column bindings and prefix lengths using the dialect.
                head, body = ddl.split(" ON ", 1)
                clause = body.split(" (", 1)[1]
                clauses.append(f"ADD {head.removeprefix('CREATE ')} ({clause}")
        # MySQL commits DDL independently. A single ALTER per table leaves a
        # fully upgraded or fully old table, allowing safe retry after failure.
        conn.exec_driver_sql(f"ALTER TABLE {quote(name)} {', '.join(clauses)}")


def migrate_business_tables(conn, existing):
    """Preserve the business projection of every row, including duplicates."""
    plans = _column_plans(conn, existing, upgrade=True)
    if not plans:
        return
    if conn.dialect.name == "sqlite":
        _sqlite_migrate(conn, plans)
    else:
        _mysql_migrate(conn, plans)
    assert_business_columns(conn, existing)
