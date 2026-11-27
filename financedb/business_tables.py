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
_UNDEFINED_FACTOR = object()
MANAGEMENT_TABLES = {
    "alembic_version", "dataset_counts", "dataset_revisions", "dataset_rows", "ingestion_partitions",
    "operation_state", "schema_versions",
}
MYSQL_OPTIONS = {
    "mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_bin", "mysql_row_format": "DYNAMIC",
}


def _parse_decimal(value):
    if not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        return Decimal(int(value)) if isinstance(value, bool) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _number(value):
    number = _parse_decimal(value)
    return number if number is not None and number.is_finite() else None


def _trim_decimal(number):
    sign, digits, exponent = number.as_tuple()
    end = len(digits)
    while end > 1 and digits[end - 1] == 0:
        end -= 1
    return sign, digits[:end], exponent + len(digits) - end


def _decimal_text(value):
    number = _number(value)
    if number is None:
        return None
    if number.is_zero():
        return "0"

    sign, digits, exponent = _trim_decimal(number)
    if exponent < -DECIMAL_SCALE or len(digits) + exponent > DECIMAL_PRECISION - DECIMAL_SCALE:
        return None

    return format(Decimal((sign, digits, exponent)), "f")


def _factor_decimal_text(value):
    number = None if isinstance(value, bool) else _parse_decimal(value)
    if number is None:
        return None
    if not number.is_finite():
        return _UNDEFINED_FACTOR
    if number.is_zero():
        return "0"
    parts = _trim_decimal(number)
    _, digits, exponent = parts
    normalized = Decimal(parts)
    if exponent >= -DECIMAL_SCALE and len(digits) + exponent <= DECIMAL_PRECISION - DECIMAL_SCALE:
        return format(normalized, "f")
    return str(normalized)


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


class FactorDecimal(TypeDecorator):
    """Store finite factor values as exact decimal text across SQL backends."""

    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(mysql.LONGTEXT() if dialect.name in {"mysql", "mariadb"} else Text())

    def process_result_value(self, value, dialect):
        # MySQL's in-place DECIMAL-to-text upgrade may leave trailing zeros.
        stored = _factor_decimal_text(value)
        return None if stored is _UNDEFINED_FACTOR else stored


FIELD_PROJECTIONS = {
    "float": (ExactDecimal, _decimal_text),
    "int": (BigInteger, _integer),
}


def _define_table(metadata, dataset_id, profile):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", dataset_id) or dataset_id in MANAGEMENT_TABLES:
        raise ValueError(f"Invalid business table name: {dataset_id}")

    columns = []
    projections = []
    used_names = set()
    code_field = profile.get("code_field")
    date_field = profile.get("date_field")
    indexed_fields = {name: width for name, width in (
        (date_field, SOURCE_DATE_LENGTH), (code_field, SOURCE_CODE_LENGTH),
    ) if name}
    for name in profile["fields"]:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_]{0,63}", name) or name.lower() in used_names:
            raise ValueError(f"Invalid business column: {dataset_id}.{name}")
        used_names.add(name.lower())
        field_type = profile.get("field_info", {}).get(name, {}).get("type")
        if name in indexed_fields:
            length = indexed_fields[name]
            sql_type, project = String(length), partial(_indexed_text, length=length)
        elif (dataset_id, name) == ("factor_value", "factor_value"):
            sql_type, project = FactorDecimal, _factor_decimal_text
        else:
            sql_type, project = FIELD_PROJECTIONS.get(field_type, (LONG_TEXT, _text))
        columns.append(Column(name, sql_type))
        projections.append((name, project))

    key = profile.get("key", ())
    key_fields = (key,) if isinstance(key, str) else tuple(key)
    table = Table(dataset_id, metadata, *columns, info={
        "field_projections": tuple(projections), "profile": profile, "key_fields": key_fields,
        "date_field": date_field, "code_field": code_field,
    }, **MYSQL_OPTIONS)
    if date_field:
        Index(f"ix_{dataset_id}_source_date", table.c[date_field])
    if code_field and code_field != date_field:
        fields = [table.c[code_field]]
        if date_field:
            fields.append(table.c[date_field])
        Index(f"ix_{dataset_id}_source_code", *fields)
    if key_fields and not {code_field, date_field}.intersection(key_fields):
        # Keep lookup bounded even when a business key uses long provider text.
        key_columns = [table.c[name] for name in key_fields]
        prefixes = {column.name: 64 for column in key_columns if isinstance(column.type, Text)}
        Index(f"ix_{dataset_id}_business_key", *key_columns, mysql_length=prefixes, mariadb_length=prefixes)
    return table


business_metadata = MetaData()
business_tables = {
    dataset_id: _define_table(business_metadata, dataset_id, profile)
    for dataset_id, profile in load_profiles().items()
}


class BusinessFieldError(ValueError):
    """A source field cannot be stored without losing its value."""


def business_columns(table):
    return tuple(table.c[name] for name, _ in table.info["field_projections"])


def business_row(table, values):
    return {name: values[name] for name, _ in table.info["field_projections"]
            if name in values and values[name] is not None}


def project_fields(table, row):
    """Store registered fields in their SQL types without silent truncation."""
    fields = {}
    for name, project in table.info["field_projections"]:
        value = row.get(name)
        stored = project(value)
        if stored is _UNDEFINED_FACTOR:
            fields[name] = None
            continue
        if stored is None and value not in (None, "") and not (isinstance(value, str) and not value.strip()):
            raise BusinessFieldError(f"业务字段 {table.name}.{name} 的值超出存储范围或类型不匹配")
        fields[name] = stored
    return fields
