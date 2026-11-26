"""SQL expressions for querying existing business columns."""

from datetime import date, timedelta

from sqlalchemy import String, Text, case, func, literal, or_


# Match Python's str.strip(), including non-ASCII whitespace.
DATE_WHITESPACE = (" \t\n\r\v\f\x1c\x1d\x1e\x1f\x85\xa0\u1680"
                   "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
                   "\u2028\u2029\u202f\u205f\u3000")


def indexed_date_filter(table, profile, start, end=None, visible_fields=None):
    """Restrict via the source-date index before exact normalized comparisons.

    Both compact and ISO dates can occur in one table, alongside monthly and
    quarterly periods. Include whitespace-prefixed outliers in the candidates
    and leave exact date interpretation to event_date_column.
    """
    field = profile.get("date_field")
    column = table.c.get(field)
    if column is None or not start or (visible_fields is not None and field not in visible_fields):
        return None
    try:
        date.fromisoformat(start)
        next_day = (date.fromisoformat(end) + timedelta(days=1)).isoformat() if end else None
    except (ValueError, OverflowError):
        return None
    if next_day is None:
        return or_(column >= start[:7], column < "0")
    first_year, last_year = start[:4], end[:4]
    return or_(
        (column >= start[:7]) & (column < next_day),
        (column >= start[:7].replace("-", "")) & (column < next_day.replace("-", "")),
        (column >= first_year + "Q") & (column < last_year + "R"),
        (column >= first_year + "q") & (column < last_year + "r"),
        column < "0", column >= ":",
    )


def event_date_column(table, profile, backend, visible_fields=None):
    """Match the date normalization used by the ingestion planner."""
    column = table.c.get(profile.get("date_field"))
    if column is None or (visible_fields is not None and column.name not in visible_fields):
        return literal("")
    if backend == "mysql":
        raw = func.regexp_replace(column, f"^[{DATE_WHITESPACE}]+|[{DATE_WHITESPACE}]+$", "")
    else:
        raw = func.trim(column, DATE_WHITESPACE)
    length = func.char_length(raw) if backend == "mysql" else func.length(raw)

    def part(start, count, *, value=raw):
        return func.substr(value, start, count, type_=String())

    year, month, day = part(1, 4), part(5, 2), part(7, 2)
    dashed = part(5, 1) == "-"
    quarter = part(6, 1)
    quarter_month = case({"1": "01", "2": "04", "3": "07", "4": "10"}, value=quarter)
    return case(
        (column.regexp_match(r"^[0-9]{8}$"),
         part(1, 4, value=column) + "-" + part(5, 2, value=column) + "-" + part(7, 2, value=column)),
        ((length >= 10) & dashed & (part(8, 1) == "-"), part(1, 10)),
        (raw.regexp_match(r"^[0-9]{8}$"), year + "-" + month + "-" + day),
        (raw.regexp_match(r"^[0-9]{6}$"), year + "-" + month + "-01"),
        ((length == 6) & part(5, 1).in_(("Q", "q")) & quarter.in_(("1", "2", "3", "4")),
         year + "-" + quarter_month + "-01"),
        ((length == 7) & dashed, part(1, 7) + "-01"),
        else_="",
    )


def _row_order(columns, backend):
    if backend == "mysql":
        # MySQL sorts only a prefix of a TEXT value. Hash the complete row
        # when a projected or keyless table needs a stable tie breaker.
        return [func.sha2(func.json_array(*columns), 256)]
    return list(columns)


def business_row_order(table, profile, backend, visible_fields=None):
    columns = tuple(table.c)
    if visible_fields is not None:
        columns = tuple(column for column in columns if column.name in visible_fields)
        return _row_order(columns, backend)

    keys = tuple(table.c[name] for name in table.info["key_fields"])
    if not keys:
        return _row_order(columns, backend)
    ordering = [func.sha2(key, 256) if backend == "mysql" and isinstance(key.type, Text) else key for key in keys]
    nullable = [key for key in keys if key.name in profile.get("nullable_keys", ())]
    if nullable:
        missing = [key.is_(None) | (key == "") if isinstance(key.type, String) else key.is_(None)
                   for key in nullable]
        incomplete = or_(*missing)
        ordering.extend(case((incomplete, value)) for value in _row_order(columns, backend))
    return ordering
