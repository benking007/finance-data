"""Physical tables for the business datasets registered in the catalog."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import partial
import json
import re

from sqlalchemy import BigInteger, Column, Index, MetaData, String, Table, Text
from sqlalchemy.dialects import mysql
from sqlalchemy.types import TypeDecorator

from .catalog import load_profiles


# The largest profile has 259 numeric columns. This precision fits the width
# of its InnoDB record and allows 27 integer digits and 18 fractional digits.
DECIMAL_PRECISION = 45
DECIMAL_SCALE = 18
SOURCE_CODE_LENGTH = 128
SOURCE_DATE_LENGTH = 48
LONG_TEXT = Text().with_variant(mysql.LONGTEXT(), "mysql")
SYSTEM_COLUMNS = (
    "_record_key", "_event_date", "_instrument", "_payload", "_content_hash", "_version_order", "_observed_at",
)
MANAGEMENT_TABLES = {
    "alembic_version", "dataset_counts", "dataset_revisions", "dataset_rows", "ingestion_partitions",
    "operation_state", "schema_versions",
}
MYSQL_OPTIONS = {
    "mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_bin", "mysql_row_format": "DYNAMIC",
}


def _number(value):
    if not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        value = Decimal(int(value)) if isinstance(value, bool) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() else None


def _decimal_text(value):
    number = _number(value)
    if number is None:
        return None
    if number.is_zero():
        return "0"

    sign, digits, exponent = number.as_tuple()
    while digits[-1] == 0:
        digits = digits[:-1]
        exponent += 1
    if exponent < -DECIMAL_SCALE or len(digits) + exponent > DECIMAL_PRECISION - DECIMAL_SCALE:
        return None

    return format(Decimal((sign, digits, exponent)), "f")


def _integer(value):
    number = _number(value)
    if number is None or not -(1 << 63) <= number < (1 << 63):
        return None
    integer = int(number)
    return integer if integer == number else None


def _text(value):
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, bool)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return str(value)


def _indexed_text(value, *, length):
    text = _text(value)
    return text if text is None or len(text) <= length else None


class ExactDecimal(TypeDecorator):
    """Expose exact decimal text on SQLite and native DECIMAL on MySQL."""

    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "mysql":
            return dialect.type_descriptor(mysql.DECIMAL(DECIMAL_PRECISION, DECIMAL_SCALE))
        return dialect.type_descriptor(Text())

    def process_bind_param(self, value, dialect):
        if value is not None and dialect.name == "mysql":
            return Decimal(value)
        return value

    def process_result_value(self, value, dialect):
        # MySQL returns fixed-scale Decimal objects. A shared Python
        # representation keeps SQLite/MySQL copy checksums interchangeable.
        return _decimal_text(value) if dialect.name == "mysql" else value


FIELD_PROJECTIONS = {
    "float": (ExactDecimal, _decimal_text),
    "int": (BigInteger, _integer),
}


def _define_table(metadata, dataset_id, profile):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", dataset_id) or dataset_id in MANAGEMENT_TABLES:
        raise ValueError(f"Invalid business table name: {dataset_id}")

    columns = [
        Column("_record_key", String(64), primary_key=True),
        Column("_event_date", String(32), nullable=False),
        Column("_instrument", String(128), nullable=False),
        Column("_payload", LONG_TEXT, nullable=False),
        Column("_content_hash", String(64), nullable=False),
        Column("_version_order", String(96), nullable=False),
        Column("_observed_at", String(32), nullable=False),
    ]
    projections = []
    used_names = set(SYSTEM_COLUMNS)
    code_field = profile.get("code_field")
    date_field = profile.get("date_field")
    indexed_fields = {name: width for name, width in (
        (date_field, SOURCE_DATE_LENGTH), (code_field, SOURCE_CODE_LENGTH),
    ) if name}
    for name in profile["fields"]:
        if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", name) or name.lower() in used_names:
            raise ValueError(f"Invalid business column: {dataset_id}.{name}")
        used_names.add(name.lower())
        field_type = profile.get("field_info", {}).get(name, {}).get("type")
        if name in indexed_fields:
            length = indexed_fields[name]
            sql_type, project = String(length), partial(_indexed_text, length=length)
        else:
            sql_type, project = FIELD_PROJECTIONS.get(field_type, (LONG_TEXT, _text))
        columns.append(Column(name, sql_type))
        projections.append((name, project))

    table = Table(dataset_id, metadata, *columns, info={"field_projections": tuple(projections)}, **MYSQL_OPTIONS)
    Index(f"ix_{dataset_id}_date", table.c._event_date, table.c._record_key)
    Index(f"ix_{dataset_id}_code_date", table.c._instrument, table.c._event_date, table.c._record_key)
    if date_field:
        Index(f"ix_{dataset_id}_source_date", table.c[date_field])
    if code_field and code_field != date_field:
        fields = [table.c[code_field]]
        if date_field:
            fields.append(table.c[date_field])
        Index(f"ix_{dataset_id}_source_code", *fields)
    return table


business_metadata = MetaData()
business_tables = {
    dataset_id: _define_table(business_metadata, dataset_id, profile)
    for dataset_id, profile in load_profiles().items()
}


def project_fields(table, row):
    """Project the registered fields without changing the source payload."""
    return {name: project(row.get(name)) for name, project in table.info["field_projections"]}
