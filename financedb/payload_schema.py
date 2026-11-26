"""Make the retained legacy payload column optional in business inserts."""

from contextlib import closing
import sqlite3

from sqlalchemy.schema import CreateTable


def _sqlite_columns(conn):
    return conn.exec_driver_sql(
        "SELECT m.name, m.sql, p.dflt_value, p.\"notnull\" "
        "FROM sqlite_master AS m, pragma_table_info(m.name) AS p "
        "WHERE m.type = 'table' AND p.name = '_payload'"
    ).all()


def _sqlite_defaults(conn, tables):
    needed = [(name, ddl) for name, ddl, default, required in _sqlite_columns(conn)
              if name in tables and required and default is None]
    if not needed:
        return

    if not conn.connection.driver_connection.in_transaction:
        conn.exec_driver_sql("BEGIN IMMEDIATE")
    changes = []
    with closing(sqlite3.connect(":memory:")) as validator:
        for name, current in needed:
            proposed = str(CreateTable(tables[name]).compile(dialect=conn.dialect)).strip()
            legacy = proposed.replace("_payload TEXT DEFAULT ('') NOT NULL,", "_payload TEXT NOT NULL,", 1)
            if legacy == proposed or current != legacy:
                raise RuntimeError(f"业务表 {name} 的结构不符合已有版本，请将兼容列 _payload 设为空默认值后重试")
            validator.execute(proposed)
            changes.append((proposed, name, current))

    # SQLite's metadata-only procedure for changing an existing default.
    # Every changed CREATE statement has been matched and parsed above.
    with conn.begin_nested():
        version = conn.exec_driver_sql("PRAGMA schema_version").scalar_one()
        conn.exec_driver_sql("PRAGMA writable_schema = ON")
        try:
            result = conn.exec_driver_sql(
                "UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = ? AND sql = ?", changes
            )
            if result.rowcount != len(changes):
                raise RuntimeError("业务表结构已变更，请重试初始化")
            conn.exec_driver_sql(f"PRAGMA schema_version = {version + 1}")
        finally:
            conn.exec_driver_sql("PRAGMA writable_schema = OFF")
        pending = {name for name, _, default, required in _sqlite_columns(conn) if required and default is None}
        if any(name in pending for _, name, _ in changes):
            raise RuntimeError("兼容列的默认值校验失败")


def _mysql_defaults(conn, tables):
    needs_default = conn.exec_driver_sql(
        "SELECT TABLE_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME = '_payload' "
        "AND IS_NULLABLE = 'NO' AND COLUMN_DEFAULT IS NULL"
    ).scalars().all()
    for name in needs_default:
        if name in tables:
            quoted = conn.dialect.identifier_preparer.quote_identifier(name)
            # MySQL treats a bare parenthesized literal as a literal in SET DEFAULT.
            conn.exec_driver_sql(f"ALTER TABLE {quoted} ALTER COLUMN `_payload` SET DEFAULT (CONCAT('')), ALGORITHM=INSTANT")


def prepare_payload_defaults(conn, tables):
    """Release legacy NOT NULL requirements without rebuilding business tables."""
    tables = {table.name: table for table in tables}
    if not tables:
        return
    if conn.dialect.name == "sqlite":
        _sqlite_defaults(conn, tables)
    elif conn.dialect.name in {"mysql", "mariadb"}:
        _mysql_defaults(conn, tables)
