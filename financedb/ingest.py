"""Plan and run restartable, read-only pulls from the Tushare Pro API."""

from __future__ import annotations

import csv
import hashlib
import io
import itertools
import json
import math
import os
import random
import tempfile
import time
from collections import defaultdict, deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as time_of_day, timedelta
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from threading import Lock
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from .business_tables import BusinessFieldError
from .announcement_scope import AnnouncementScopeError, SCOPE_OPERATION, SCOPE_SOURCE, SCOPE_VERSION, fetch_exchange_codes, valid_codes
from .storage import PAGINATION_VERIFIED_MESSAGE, json_text, split_scope_message

API_URL = "https://api.tushare.pro"
SHANGHAI = ZoneInfo("Asia/Shanghai")
DAY = timedelta(days=1)
SECOND = timedelta(seconds=1)
Cancelled = Callable[[], bool]
Progress = Callable[[dict[str, Any]], None]
Universe = Callable[..., list[str]]
DatedUniverse = Callable[[str, date, str | None], list[str]]
SNAPSHOT_STRATEGIES = {"snapshot", "parameter_grid", "code_snapshot"}
DEPENDENCY_FIELDS = ("code_source", "cap_code_source", "date_code_source", "date_cap_code_source")


def request_dependencies(request: Mapping[str, Any]) -> Iterator[str]:
    for field in DEPENDENCY_FIELDS:
        source = request.get(field)
        if source:
            yield source
    yield from request.get("cap_code_sources", ())


class IngestError(Exception):
    """An error that can be recorded without embedding an upstream request."""


class IngestCancelled(IngestError):
    """The caller requested cooperative cancellation."""


class PaginationError(IngestError):
    """The response did not verify that pagination advanced without gaps."""


class RequestError(IngestError):
    def __init__(
        self,
        message: str,
        *,
        status: str = "failed",
        retryable: bool = False,
        stop_dataset: bool = False,
        invalid_token: bool = False,
        network_unavailable: bool = False,
        retry_after: float = 0,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.retryable = retryable
        self.stop_dataset = stop_dataset
        self.invalid_token = invalid_token
        self.network_unavailable = network_unavailable
        self.retry_after = retry_after


def _check_cancelled(cancelled: Cancelled | None) -> None:
    if cancelled and cancelled():
        raise IngestCancelled("已停止，已提交的数据可续跑")


def _wait(seconds: float, cancelled: Cancelled | None) -> None:
    deadline = time.monotonic() + seconds
    while True:
        _check_cancelled(cancelled)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.25))


class RateLimiter:
    """Concurrent callers share rolling request budgets, including retries."""

    def __init__(self, per_minute: int = 200, per_day: int | None = None, *, endpoint_per_minute: int = 50) -> None:
        if per_minute < 1 or endpoint_per_minute < 1 or (per_day is not None and per_day < 1):
            raise ValueError("request quotas must be positive")
        self.account = (per_minute, per_day)
        self.endpoint_per_minute = endpoint_per_minute
        self._calls: dict[tuple[str, int], deque[float]] = defaultdict(deque)
        self._lock = Lock()

    @classmethod
    def from_environment(cls) -> RateLimiter:
        def positive(name, default):
            try:
                value = int(os.environ.get(name, str(default)))
                if value > 0:
                    return value
            except ValueError:
                pass
            raise ValueError(f"{name} 必须为正整数")

        return cls(
            positive("FINANCEDB_TUSHARE_REQUESTS_PER_MINUTE", 200),
            endpoint_per_minute=positive("FINANCEDB_TUSHARE_ENDPOINT_REQUESTS_PER_MINUTE", 50),
        )

    def acquire(
        self, profile: Mapping[str, Any], cancelled: Cancelled | None = None
    ) -> None:
        quota = profile.get("quota", {})
        group = quota.get("group", profile["api_name"])
        buckets = [
            ("account", 60, self.account[0]),
            ("account", 86400, self.account[1]),
            (f"endpoint:{group}", 60, min(quota.get("per_minute", self.endpoint_per_minute), self.endpoint_per_minute)),
            (f"endpoint:{group}", 86400, quota.get("per_day")),
        ]
        buckets = [(name, period, int(cap)) for name, period, cap in buckets if cap]
        while True:
            _check_cancelled(cancelled)
            with self._lock:
                now = time.monotonic()
                wait = 0.0
                for name, period, cap in buckets:
                    calls = self._calls[name, period]
                    while calls and calls[0] <= now - period:
                        calls.popleft()
                    if len(calls) >= cap:
                        wait = max(wait, period - (now - calls[len(calls) - cap]))
                if wait <= 0:
                    for name, period, _ in buckets:
                        self._calls[name, period].append(now)
                    return
            _wait(wait, cancelled)


class TushareClient:
    def __init__(
        self,
        token: str,
        *,
        timeout: float = 25,
        attempts: int = 3,
        limiter: RateLimiter | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._token = token.strip()
        if not self._token:
            raise ValueError("请配置 Tushare Token")
        if attempts < 1:
            raise ValueError("attempts must be positive")
        self._attempts = attempts
        self._limiter = limiter or RateLimiter.from_environment()
        self.metrics = {"requests": 0, "response_rows": 0, "retries": 0,
                        "http_seconds": 0.0, "rate_wait_seconds": 0.0, "retry_wait_seconds": 0.0,
                        "scope_requests": 0}
        self._http = httpx.Client(
            timeout=timeout, follow_redirects=False, transport=transport
        )

    def __enter__(self) -> TushareClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def announcement_codes(self, *, cancelled: Cancelled | None = None) -> list[str]:
        def get_json(url, params, referer):
            _check_cancelled(cancelled)
            started = time.monotonic()
            self.metrics["scope_requests"] += 1
            try:
                response = self._http.get(url, params=params, headers={"Referer": referer, "User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
                _check_cancelled(cancelled)
                return response.json()
            except (httpx.HTTPError, ValueError):
                raise AnnouncementScopeError("交易所公告补充名录连接或响应异常") from None
            finally:
                self.metrics["http_seconds"] += time.monotonic() - started

        return fetch_exchange_codes(get_json)

    def query(
        self,
        profile: Mapping[str, Any],
        params: Mapping[str, Any],
        *,
        cancelled: Cancelled | None = None,
    ) -> list[dict[str, Any]]:
        for attempt in range(self._attempts):
            started = time.monotonic()
            try:
                self._limiter.acquire(profile, cancelled)
            finally:
                self.metrics["rate_wait_seconds"] += time.monotonic() - started
            try:
                started = time.monotonic()
                self.metrics["requests"] += 1
                try:
                    rows = self._query(profile, params)
                    self.metrics["response_rows"] += len(rows)
                    return rows
                finally:
                    self.metrics["http_seconds"] += time.monotonic() - started
            except RequestError as exc:
                if not exc.retryable or attempt + 1 == self._attempts:
                    raise
                if exc.retry_after > 120:
                    raise
                backoff = (2 ** attempt) + random.uniform(0, 0.5)
                self.metrics["retries"] += 1
                started = time.monotonic()
                try:
                    _wait(max(backoff, exc.retry_after), cancelled)
                finally:
                    self.metrics["retry_wait_seconds"] += time.monotonic() - started
        raise AssertionError("at least one request attempt")

    def _query(
        self, profile: Mapping[str, Any], params: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        request = profile.get("request", {})
        injected = request.get("inject_params", {})
        fields = request.get("wire_fields", [name for name in profile.get("fields", []) if name not in injected])
        try:
            response = self._http.post(
                API_URL,
                json={
                    "api_name": profile["api_name"],
                    "token": self._token,
                    "params": dict(params),
                    "fields": ",".join(fields),
                },
            )
        except httpx.RequestError as exc:
            raise RequestError(
                f"Tushare 连接失败（{type(exc).__name__}）",
                retryable=True,
                stop_dataset=True,
                network_unavailable=isinstance(exc, httpx.ConnectError),
            ) from None

        status = response.status_code
        if status != 200:
            retryable = status in (408, 425, 429) or 500 <= status < 600
            raise RequestError(
                f"Tushare HTTP {status}",
                status="blocked" if status in (401, 403, 429) else "failed",
                retryable=retryable,
                stop_dataset=True,
                invalid_token=status == 401,
                retry_after=_retry_after(response.headers.get("Retry-After")),
            )

        try:
            result = response.json(parse_float=Decimal)
        except ValueError:
            raise RequestError("Tushare 响应格式异常", retryable=True) from None
        if not isinstance(result, dict) or "code" not in result:
            raise RequestError("Tushare 响应缺少业务状态", retryable=True)
        if str(result["code"]) != "0":
            raise self._business_error(result["code"], result.get("msg"))

        data = result.get("data")
        if not isinstance(data, dict):
            raise RequestError("Tushare 成功响应缺少 data")
        names = data.get("fields")
        items = data.get("items")
        if (
            not isinstance(names, list)
            or not all(isinstance(name, str) for name in names)
            or len(names) != len(set(names))
            or not isinstance(items, list)
            or any(not isinstance(row, list) or len(row) != len(names) for row in items)
        ):
            raise RequestError("Tushare 响应字段或行结构不符合预期")
        if items and not set(fields).issubset(names):
            missing = ", ".join(sorted(set(fields).difference(names)))
            raise RequestError(f"Tushare 响应缺少已请求字段 {missing}", stop_dataset=True)
        return [dict(zip(names, row, strict=True)) for row in items]

    def _business_error(self, code: Any, value: Any) -> RequestError:
        message = " ".join(str(value or "上游拒绝请求").replace(self._token, "[已隐藏]").split())
        code_text = str(code)
        display_code = code_text if len(code_text) < 8 and code_text.lstrip("-").isdigit() else "未知"
        detail = f"Tushare 业务状态 {display_code}，{message[:350]}"
        lowered = message.casefold()
        invalid_token = (
            "token" in lowered
            and any(word in lowered for word in ("不正确", "无效", "错误", "invalid", "expired"))
        )
        quota = any(word in lowered for word in ("频次", "限流", "频率", "rate limit", "too many"))
        if quota:
            return RequestError(detail, status="blocked", retryable=True, stop_dataset=True)
        if invalid_token or str(code) == "2002" or any(
            word in lowered for word in ("无权", "权限", "permission")
        ):
            return RequestError(
                detail, status="blocked", stop_dataset=True, invalid_token=invalid_token
            )
        if any(word in lowered for word in ("系统内部", "系统繁忙", "temporar", "timeout")):
            return RequestError(detail, retryable=True, stop_dataset=True)
        return RequestError(detail, stop_dataset=True)


def _retry_after(value: str | None) -> float:
    if not value:
        return 0
    try:
        delay = float(value)
        return max(0, delay) if math.isfinite(delay) else 0
    except ValueError:
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 0


def _date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _source_datetime(value: Any) -> datetime:
    text = str(value)
    if len(text) == 6 and text.isdigit():
        return datetime(int(text[:4]), int(text[4:]), 1)
    if len(text) == 7 and text[4] == "-":
        return datetime.fromisoformat(f"{text}-01")
    if len(text) == 6 and text[4] in "Qq" and text[5] in "1234":
        return datetime(int(text[:4]), 3 * (int(text[5]) - 1) + 1, 1)
    if len(text) == 4 and text.isdigit():
        return datetime(int(text), 1, 1)
    stamp = datetime.fromisoformat(text)
    return stamp.astimezone(SHANGHAI).replace(tzinfo=None) if stamp.tzinfo else stamp


def _period_floor(value: date, unit: str) -> date:
    if unit == "day":
        return value
    if unit == "week":
        return value - timedelta(days=value.weekday())
    if unit == "month":
        return value.replace(day=1)
    if unit == "quarter":
        return value.replace(month=((value.month - 1) // 3) * 3 + 1, day=1)
    if unit == "year":
        return value.replace(month=1, day=1)
    raise IngestError(f"未知分期单位 {unit}")


def _next_period(value: date, unit: str) -> date:
    if unit in ("day", "week"):
        return value + timedelta(days=7 if unit == "week" else 1)
    months = {"month": 1, "quarter": 3, "year": 12}[unit]
    year, month = divmod(value.month - 1 + months, 12)
    return date(value.year + year, month + 1, 1)


def _format_date(value: datetime, format_string: str) -> str:
    quarter = str(((value.month - 1) // 3) + 1)
    return value.strftime(format_string.replace("%q", quarter))


@dataclass(frozen=True)
class Partition:
    params: dict[str, Any]
    start: datetime | None = None
    end: datetime | None = None
    code: str | None = None
    api_name: str | None = None
    expected_keys: frozenset[tuple[Any, ...]] = frozenset()


def _monthly_span_days(unit: str, mode: str) -> int:
    days = {"week": 14, "month": 110, "quarter": 370, "year": 740}
    return days.get(unit, 30 if mode == "sample" else 7)


def _range(
    profile: Mapping[str, Any],
    mode: str,
    today: date,
    start: date | None,
    end: date | None,
    current_time: time_of_day,
) -> tuple[date, date]:
    request = profile["request"]
    if end is None:
        include_today = (
            mode == "incremental"
            and request.get("incremental_include_today")
            and current_time >= time_of_day.fromisoformat(request.get("incremental_release_time", "21:30"))
        )
        end = today if include_today else today - DAY
    if profile.get("history_end"):
        end = min(end, _date(profile["history_end"]))
    unit = request.get("unit", "day")
    first = profile.get("history_start")
    if start is None:
        if mode == "history":
            lower_bound = first or profile.get("history_probe_start")
            if not lower_bound:
                raise IngestError("这个接口的历史回补范围还没有配置")
            start = _date(lower_bound)
        else:
            default = _monthly_span_days(unit, mode)
            if unit == "day" and request["strategy"] == "code_window":
                default = 5 if mode == "sample" else 2
            days = int(request.get(f"{mode}_days", default))
            start = end - timedelta(days=max(days, 1) - 1)
    if first:
        start = max(start, _date(first))
    lookback = request.get("lookback_years")
    if lookback:
        try:
            earliest = today.replace(year=today.year - lookback)
        except ValueError:
            earliest = today.replace(year=today.year - lookback, day=28)
        start = max(start, earliest)
        end = min(end, today)
    if start > end:
        if lookback:
            raise IngestError(f"指定范围与最近 {lookback} 年采集窗口没有交集")
        raise IngestError("指定范围与可采集范围没有交集")
    return start, end


def _grid(request: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
    dimensions = request.get("dimensions", {})
    combinations = itertools.product(*dimensions.values())
    for values in combinations:
        yield {**request.get("static_params", {}), **dict(zip(dimensions, values, strict=True))}


def _bounds(partition: Partition, request: Mapping[str, Any]) -> Partition:
    assert partition.start is not None and partition.end is not None
    fmt = request.get("date_format", "%Y%m%d")
    upper = partition.end
    if not request.get("end_inclusive", True):
        upper += SECOND
    return replace(
        partition,
        params={
            **partition.params,
            request.get("start_param", "start_date"): _format_date(partition.start, fmt),
            request.get("end_param", "end_date"): _format_date(upper, fmt),
        },
    )


def _dates(
    store: Any,
    request: Mapping[str, Any],
    start: date,
    end: date,
    mode: str,
) -> Iterator[tuple[date, date]]:
    unit = request.get("unit", "day")
    exchange = request.get("exchange")
    calendar_covers = getattr(store, "calendar_covers", None)
    if (
        unit == "day"
        and request.get("trading_days")
        and calendar_covers
        and calendar_covers(start.isoformat(), end.isoformat(), exchange=exchange)
    ):
        dates = store.trading_days(start.isoformat(), end.isoformat(), exchange=exchange)
        if mode == "sample":
            dates = dates[-int(request.get("sample_sessions", 20)) :]
        yield from ((_date(value), _date(value)) for value in dates)
        return

    cursor = _period_floor(start, unit)
    while cursor <= end:
        following = _next_period(cursor, unit)
        period_end = following - DAY
        if (unit in ("day", "week") or period_end <= end or request.get("partial_period")):
            yield cursor, period_end
        cursor = following


def _codes(
    store: Any,
    request: Mapping[str, Any],
    explicit: Sequence[str],
    mode: str,
    universe: Universe | None,
) -> list[str]:
    if explicit:
        return list(dict.fromkeys(explicit))
    if mode == "sample" and request.get("sample_codes"):
        return list(request["sample_codes"])
    source = request.get("code_source")
    field = request.get("code_source_field")
    lookup = universe or store.instrument_codes
    values = []
    if source:
        if field:
            values = universe(source, field) if universe else store.instrument_values(source, field)
        else:
            values = lookup(source)
    suffix = request.get("code_suffix")
    if suffix:
        values = [value for value in values if value.endswith(suffix)]
    if not values:
        raise IngestError("缺少标的范围，请先同步主数据或明确指定标的")
    return values[: int(request.get("sample_code_count", 3))] if mode == "sample" else values


def plan_partitions(
    store: Any,
    profile: Mapping[str, Any],
    *,
    mode: str,
    today: date,
    start: date | None = None,
    end: date | None = None,
    codes: Sequence[str] = (),
    current_time: time_of_day | None = None,
    universe: Universe | None = None,
    dated_universe: DatedUniverse | None = None,
    report_size: Callable[[int], None] | None = None,
) -> Iterator[Partition]:
    request = profile["request"]
    strategy = request["strategy"]
    variants = math.prod(len(values) for values in request.get("dimensions", {}).values())
    unfiltered_history = mode == "history" and start is None and end is None and not codes and request.get("history_snapshot")
    if strategy in ("snapshot", "parameter_grid") or unfiltered_history:
        if report_size:
            report_size(variants)
        for params in _grid(request):
            yield Partition(params)
        return
    if strategy == "code_snapshot":
        selected_codes = _codes(store, request, codes, mode, universe)
        code_param = request.get("code_param", "ts_code")
        if report_size:
            report_size(variants * len(selected_codes))
        for code in selected_codes:
            for params in _grid(request):
                yield Partition({**params, code_param: code}, code=code)
        return
    start, end = _range(
        profile, mode, today, start, end, current_time or datetime.now(SHANGHAI).time()
    )
    if strategy == "date":
        unit = request.get("unit", "day")
        fmt = request.get("date_format", "%Y%m%d")
        date_param = request.get("date_param", "trade_date")
        periods = _dates(store, request, start, end, mode)
        date_source = request.get("date_code_source")
        if report_size and not date_source:
            periods = list(periods)
            count = len(codes) if codes and request.get("code_param") else 1
            report_size(len(periods) * variants * count)
        for lower, upper in periods:
            value = upper if request.get("period_anchor", "end") == "end" else lower
            stamp = _format_date(datetime.combine(value, time_of_day.min), fmt)
            selected_codes = codes
            if date_source and not selected_codes:
                field = request.get("date_code_source_field")
                selected_codes = (dated_universe(date_source, value, field) if dated_universe
                                  else store.instrument_values(date_source, field, event_date=value))
            for variant in _grid(request):
                params = {**variant, date_param: stamp}
                if (date_source or selected_codes) and request.get("code_param"):
                    for code in selected_codes:
                        yield Partition(
                            {**params, request["code_param"]: code},
                            datetime.combine(lower, time_of_day.min),
                            datetime.combine(upper, time_of_day.max),
                            code,
                        )
                else:
                    yield Partition(
                        params,
                        datetime.combine(lower, time_of_day.min),
                        datetime.combine(upper, time_of_day.max),
                    )
        return
    if strategy not in ("window", "code_window"):
        raise IngestError(f"未知请求分片方式 {strategy}")

    code_param = request.get("code_param", "ts_code")
    selected_codes = (
        _codes(store, request, codes, mode, universe)
        if strategy == "code_window"
        else (list(codes) if codes and request.get("code_param") else [None])
    )
    lifetimes = {}
    if strategy == "code_window" and request.get("code_source"):
        lookup = getattr(store, "instrument_lifetimes", None)
        if lookup:
            lifetimes = lookup(request["code_source"])
    window = max(int(request.get("window_days", 31)), 1)
    code_ranges = []
    for code in selected_codes:
        first, last = lifetimes.get(code, (None, None))
        lower = max(start, _date(first)) if first else start
        upper = min(end, _date(last)) if last else end
        if lower <= upper:
            code_ranges.append((code, lower, upper))
    if report_size:
        windows = sum(((upper - lower).days // window) + 1 for _, lower, upper in code_ranges)
        report_size(windows * variants)
    for code, cursor, effective_end in code_ranges:
        while cursor <= effective_end:
            upper = min(effective_end, cursor + timedelta(days=window - 1))
            for variant in _grid(request):
                params = {**variant, code_param: code} if code else variant
                yield _bounds(
                    Partition(
                        params,
                        datetime.combine(cursor, time_of_day.min),
                        datetime.combine(upper, time_of_day(23, 59, 59)),
                        code,
                    ),
                    request,
                )
            cursor = upper + DAY


def _split(
    profile: Mapping[str, Any],
    partition: Partition,
    universe: Universe,
    dated_universe: DatedUniverse | None = None,
    raw: Sequence[Mapping[str, Any]] = (),
    *,
    coverage_gaps: list[str] | None = None,
) -> Iterator[Partition]:
    request = profile["request"]
    strategy = request["strategy"]
    if strategy in ("window", "code_window") and partition.start is not None:
        assert partition.end is not None
        if partition.end.date() > partition.start.date():
            elapsed = (partition.end.date() - partition.start.date()).days
            split_date = partition.start.date() + timedelta(days=elapsed // 2)
            midpoint = datetime.combine(split_date, time_of_day(23, 59, 59))
        elif "%H" in request.get("date_format", "") and partition.end > partition.start:
            elapsed = int((partition.end - partition.start).total_seconds())
            midpoint = partition.start + timedelta(seconds=elapsed // 2)
        else:
            midpoint = None
        if midpoint is not None:
            yield _bounds(replace(partition, end=midpoint), request)
            yield _bounds(replace(partition, start=midpoint + SECOND), request)
            return

    for name, values in request.get("cap_dimensions", {}).items():
        if name not in partition.params:
            response_field = request.get("validate_parameters", {}).get(name, name)
            observed = _cap_groups(profile, partition, raw, response_field)
            if not set(observed) <= set(values):
                raise IngestError(f"来源出现未配置的 {name} 值，无法验证拆分覆盖范围")
            for value in values:
                yield replace(partition, params={**partition.params, name: value},
                              expected_keys=observed.get(value, frozenset()))
            return

    categories = request.get("cap_category_values")
    if categories and "category" not in partition.params:
        observed = {category: keys for category, keys in _cap_groups(profile, partition, raw, "category").items()
                    if isinstance(category, str) and category.strip()}
        for category in dict.fromkeys([*observed, *categories]):
            yield replace(partition, params={**partition.params, "category": category},
                          expected_keys=observed.get(category, frozenset()))
        return

    if partition.code is None and request.get("cap_split"):
        code_param = request.get("cap_code_param", request.get("code_param", "ts_code"))
        if code_param in request["cap_split"]:
            date_source = request.get("date_cap_code_source")
            source = request.get("cap_code_source", request.get("code_source"))
            field = request.get("cap_code_source_field")
            sources = request.get("cap_code_sources", ())
            if date_source and dated_universe and partition.start:
                values = dated_universe(date_source, partition.start.date(), field)
            elif sources:
                values = list(dict.fromkeys(code for source_id in sources for code in universe(source_id)))
            elif source:
                values = universe(source, field) if field else universe(source)
            else:
                values = []
            supplement = request.get("cap_code_supplement")
            if supplement:
                values = list(dict.fromkeys([*values, *universe(supplement)]))
            observed = {}
            response_field = request.get("partition_code_field", profile.get("code_field"))
            if response_field and request.get("validate_code", True):
                observed = _cap_groups(profile, partition, raw, response_field)
                outside = set(observed) - set(values)
                if outside:
                    example = ", ".join(str(code)[:128] for code in sorted(outside, key=str)[:5])
                    detail = f"名录外 {code_param} 共 {len(outside)} 个（{example}）"
                    if not request.get("cap_include_observed") or coverage_gaps is None:
                        raise IngestError(f"触顶样本含{detail}，无法验证拆分覆盖范围")
                    coverage_gaps.append(detail)
                    # The observed codes are valid source selectors, but the
                    # capped sample cannot prove that no other codes exist.
                    values = list(dict.fromkeys([*observed, *values]))
            for code in values:
                yield replace(
                    partition,
                    params={**partition.params, code_param: code},
                    code=code,
                    api_name=request.get("split_api_name"),
                    expected_keys=observed.get(code, frozenset()),
                )


def _cap_groups(profile, partition, raw, response_field):
    groups = defaultdict(set)
    keys = profile.get("key", ())
    for row in _validated_rows(profile, partition, raw):
        value = row.get(response_field)
        if isinstance(value, (dict, list)):
            raise IngestError(f"来源字段 {response_field} 含非标量值，无法验证拆分覆盖范围")
        group = groups[value]
        if keys:
            group.add(tuple(row[name] for name in keys))
    return {value: frozenset(group) for value, group in groups.items()}


def _partition_id(profile: Mapping[str, Any], partition: Partition, scope: str) -> str:
    data = {
        "api_name": partition.api_name or profile["api_name"],
        "fields": profile.get("fields", []),
        "params": partition.params,
        "request_version": profile.get("request", {}).get("version", 1),
        "scope": scope,
    }
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _validated_rows(
    profile: Mapping[str, Any], partition: Partition, rows: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    request = profile.get("request", {})
    date_field = profile.get("date_field")
    code_field = request.get("partition_code_field", profile.get("code_field"))
    injected = request.get("inject_params", {})
    end_exclusive = None
    if partition.end:
        end_exclusive = partition.end + (
            timedelta(microseconds=1) if partition.end.microsecond else SECOND
        )
    result = []
    keys = profile.get("key", [])
    required_keys = set(keys) - set(profile.get("nullable_keys", []))
    versioned = profile.get("versioned") or profile.get("revision_field")
    selectors = {name: name for name in request.get("cap_dimensions", {})} | request.get("validate_parameters", {})
    if request.get("cap_category_values"):
        selectors = {"market": "market", "category": "category", **selectors}
    filters = [(name, response_field, partition.params[name]) for name, response_field in selectors.items()
               if name in partition.params]
    by_identity = {}
    for original in rows:
        row = dict(original)
        for name, alternate in request.get("field_aliases", {}).items():
            if name not in row and alternate in row:
                row[name] = row[alternate]
        for name, param in injected.items():
            if param not in partition.params:
                raise IngestError(f"无法从分片参数补齐字段 {name}")
            row.setdefault(name, partition.params[param])
        missing_keys = [name for name in keys if name not in row or (
            name in required_keys and row[name] in (None, "")
        )]
        if missing_keys:
            raise IngestError(f"来源记录缺少业务键 {', '.join(missing_keys)}")
        if request.get("validate_code", True) and partition.code and code_field and row.get(code_field) != partition.code:
            raise IngestError("来源返回了分片标的以外的记录")
        for name, response_field, value in filters:
            if row.get(response_field) != value:
                raise IngestError(f"来源返回了 {name}={value} 筛选范围以外的记录")
        if request.get("validate_date", True) and date_field and partition.start and end_exclusive:
            try:
                when = _source_datetime(row.get(date_field))
            except ValueError:
                raise IngestError(f"无法解析来源日期字段 {date_field}") from None
            if not partition.start <= when < end_exclusive:
                if request.get("clip_out_of_window"):
                    continue
                raise IngestError(f"来源返回了分片日期以外的记录（{date_field}）")
        if keys and not versioned:
            identity = tuple(row[name] for name in keys)
            if any(isinstance(value, (dict, list)) for value in identity):
                raise IngestError("来源业务键出现非标量字段")
            if all(value not in (None, "") for value in identity):
                previous = by_identity.setdefault(identity, row)
                if previous != row:
                    raise IngestError("同一分片内有内容不同且业务键相同的记录")
        result.append(row)
    if partition.expected_keys and not partition.expected_keys.issubset(tuple(row[name] for name in keys) for row in result):
        raise IngestError("子分片缺少上限响应中已出现的记录，无法核验完整性")
    return result


def _fetch_partition(
    client: Any,
    profile: Mapping[str, Any],
    params: Mapping[str, Any],
    cancelled: Cancelled | None,
) -> list[dict[str, Any]]:
    pagination = profile.get("request", {}).get("pagination")
    if not pagination:
        return client.query(profile, params, cancelled=cancelled)
    return _collect_pages(client, profile, params, pagination, cancelled)


def _page_fingerprints(rows):
    try:
        return [hashlib.sha256(json_text(dict(row)).encode()).digest() for row in rows]
    except (TypeError, ValueError):
        raise PaginationError("来源记录无法生成分页校验指纹") from None


def _collect_pages(client, profile, params, pagination, cancelled, *, capped_first=None):
    configured = pagination["page_size"]
    size = min(configured, profile.get("row_limit") or configured)
    overlap = min(pagination.get("overlap", 2), size - 1)
    if overlap < 1:
        raise PaginationError("分页参数无法校验相邻批次边界")
    offset = pagination.get("initial_offset", 0)
    offset_name = pagination.get("offset_param", "offset")
    limit_name = pagination.get("limit_param", "limit")

    def fetch(position):
        _check_cancelled(cancelled)
        page = client.query(profile, {**params, offset_name: position, limit_name: size}, cancelled=cancelled)
        _check_cancelled(cancelled)
        if len(page) > size:
            raise PaginationError(f"分页 {offset_name}={position} 返回 {len(page)} 行，超过请求的 {size} 行")
        return page

    rows = list(fetch(offset) if capped_first is None else capped_first)
    if len(rows) < size:
        return rows
    previous = _page_fingerprints(rows)
    seen = set(previous)
    for page_number in range(1, pagination.get("max_pages", 1000)):
        position = offset + len(rows) - overlap
        page = fetch(position)
        markers = _page_fingerprints(page)
        if len(markers) < overlap or markers[:overlap] != previous[-overlap:]:
            raise PaginationError(f"分页 {offset_name}={position} 未接上前页边界，无法核验分页")
        fresh = set(markers[overlap:]) - seen
        if not fresh and (len(page) == size or (capped_first is not None and page_number == 1)):
            raise PaginationError(f"分页 {offset_name}={position} 未提供可核验的新记录，无法核验分页")
        rows.extend(page[overlap:])
        if len(page) < size:
            return rows
        seen.update(fresh)
        previous = markers
    raise PaginationError("达到分页安全上限，无法核验完整性")


def _verified_capped_pages(client, profile, partition, first_page, cancelled):
    for initial_offset in profile["request"].get("capped_pagination_offsets", (0,)):
        options = {"page_size": profile["row_limit"], "max_pages": 101, "initial_offset": initial_offset}
        try:
            rows = _collect_pages(client, profile, partition.params, options, cancelled, capped_first=first_page)
        except RequestError as exc:
            if exc.retryable or exc.status == "blocked" or exc.network_unavailable:
                raise
            return None
        except PaginationError:
            continue
        try:
            _validated_rows(profile, partition, rows)
        except IngestError:
            continue
        return rows
    return None


@dataclass
class DatasetProgress:
    dataset_id: str
    callback: Progress | None = field(repr=False)
    planned: int | None = None
    completed: int = 0
    skipped: int = 0
    failed: int = 0
    rows: int = 0
    status: str = "running"
    message: str | None = None
    empty_partitions: int = 0
    split_partitions: int = 0
    storage_seconds: float = 0.0
    client_metrics: Mapping[str, int | float] = field(default_factory=dict, repr=False)
    _baseline: dict[str, int | float] = field(default_factory=dict, init=False, repr=False)
    _started: float = field(default_factory=time.monotonic, init=False, repr=False)

    def __post_init__(self):
        self._baseline = dict(self.client_metrics)

    def emit(self, event: str, **details: Any) -> None:
        if self.callback:
            self.callback({"event": event, **self.summary(), **details})

    def summary(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "status": self.status,
            "planned": self.planned,
            "completed": self.completed,
            "skipped": self.skipped,
            "failed": self.failed,
            "rows": self.rows,
            "message": self.message,
            "metrics": {
                **{name: round(value - self._baseline.get(name, 0), 3) for name, value in self.client_metrics.items()},
                "elapsed_seconds": round(time.monotonic() - self._started, 3),
                "storage_seconds": round(self.storage_seconds, 3),
                "empty_partitions": self.empty_partitions, "split_partitions": self.split_partitions,
            },
        }

    def set_planned(self, count: int) -> None:
        self.planned = count
        self.emit("plan_ready")


@dataclass
class _RunLimits:
    max_partitions: int | None
    attempted: int = 0
    stopped: bool = False

    def use_partition(self) -> None:
        if self.max_partitions is not None and self.attempted >= self.max_partitions:
            self.stopped = True
            raise IngestCancelled("已达到本次运行的分片上限，可再次运行以继续")
        self.attempted += 1


class _DatasetPull:
    def __init__(
        self,
        store: Any,
        client: Any,
        profile: Mapping[str, Any],
        universe: Universe,
        scope: str,
        meter: DatasetProgress,
        limits: _RunLimits,
        cancelled: Cancelled | None,
        dated_universe: DatedUniverse | None = None,
    ) -> None:
        self.store = store
        self.client = client
        self.profile = profile
        self.universe = universe
        self.scope = scope
        self.meter = meter
        self.limits = limits
        self.cancelled = cancelled
        self.dated_universe = dated_universe

    def _stored(self, operation, *args, **kwargs):
        started = time.monotonic()
        try:
            return operation(*args, **kwargs)
        finally:
            self.meter.storage_seconds += time.monotonic() - started

    def pull(self, partition: Partition) -> bool:
        _check_cancelled(self.cancelled)
        partition_id = _partition_id(self.profile, partition, self.scope)
        details = {"partition_id": partition_id, "params": partition.params}
        cap = self.profile.get("row_limit")
        split_scope = split_scope_message(self.profile, partition.params)
        if self._stored(self.store.partition_done, self.meter.dataset_id, partition_id, row_limit=cap,
                        expected_keys=partition.expected_keys, split_scope=split_scope):
            self.meter.skipped += 1
            self.meter.emit("partition_progress", partition_status="skipped", row_count=0, **details)
            return True

        self.limits.use_partition()
        request_profile = self.profile
        if partition.api_name:
            request_profile = {**self.profile, "api_name": partition.api_name}
        request = self.profile["request"]
        paginated = bool(request.get("pagination"))
        try:
            raw = _fetch_partition(self.client, request_profile, partition.params, self.cancelled)
            _check_cancelled(self.cancelled)
            capped = cap and len(raw) >= cap and not paginated
            if capped and request.get("verify_capped_pagination"):
                verified = _verified_capped_pages(self.client, request_profile, partition, raw, self.cancelled)
                if verified is not None:
                    raw, capped = verified, False
                    paginated = True
            rows = [] if capped else _validated_rows(self.profile, partition, raw)
        except IngestCancelled:
            raise
        except IngestError as exc:
            self._failure(partition_id, partition, exc)
            if isinstance(exc, RequestError) and exc.stop_dataset:
                raise
            return False

        if capped:
            coverage_gaps = []
            try:
                children = list(_split(self.profile, partition, self.universe, self.dated_universe, raw,
                                       coverage_gaps=coverage_gaps))
            except IngestCancelled:
                raise
            except IngestError as exc:
                if request.get("retain_capped_rows"):
                    self._save_partial(partition_id, partition, raw, str(exc))
                else:
                    self._failure(partition_id, partition, exc)
                return False
            if not children:
                selectors = ", ".join(f"{name}={value}" for name, value in partition.params.items())
                scope = f"（{selectors}）" if selectors else ""
                if request.get("retain_capped_rows"):
                    self._save_partial(partition_id, partition, raw, f"达到单次行数上限 {cap}{scope}，已保存可验证记录，剩余数据待补齐")
                else:
                    self._failure(partition_id, partition, IngestError(f"达到单次行数上限 {cap}{scope}，缺少可验证的续页或更细分片"))
                return False
            if self.meter.planned is not None:
                self.meter.planned += len(children) - 1
            self.meter.split_partitions += 1
            self.meter.emit("partition_progress", partition_status="split", row_count=0, **details)
            if coverage_gaps:
                self._save_partial(partition_id, partition, raw,
                                   f"{'；'.join(coverage_gaps)}，分页未通过核验；已保留样本并按代码补采，整日覆盖仍待核验")
            children_complete = True
            for child in children:
                children_complete = self.pull(child) and children_complete
            if coverage_gaps:
                return False
            if request.get("cap_category_values") and "category" not in partition.params:
                market = partition.params.get("market", "未知市场")
                detail = "已补采可识别分类" if children_complete else "部分分类仍有缺口"
                self._failure(partition_id, partition, IngestError(
                    f"市场 {market} 达到 {cap} 行上限，{detail}，分页未通过验证，整市场覆盖待核对"
                ))
                return False
            if children_complete:
                self._stored(self.store.commit_partition,
                    self.profile, partition_id, partition.params, [], status="complete", source_count=0,
                    message=split_scope or "",
                )
            elif request.get("retain_capped_rows"):
                self._save_partial(partition_id, partition, raw, "已保留触顶样本，拆分后仍有未完成的子分片")
            else:
                self._stored(self.store.record_failure,
                    self.profile, partition_id, partition.params, "拆分后仍有未完成的子分片"
                )
            return children_complete

        state = "complete" if rows else "empty"
        try:
            saved = self._stored(self.store.commit_partition,
                self.profile, partition_id, partition.params, rows, status=state, source_count=len(raw),
                message=PAGINATION_VERIFIED_MESSAGE if paginated else "",
            )
        except BusinessFieldError as exc:
            self._failure(partition_id, partition, IngestError(str(exc)))
            return False
        count = saved.get("row_count", len(rows))
        self.meter.completed += 1
        self.meter.empty_partitions += int(not rows)
        self.meter.rows += count
        self.meter.emit("partition_progress", partition_status=state, row_count=count, **details)
        return True

    def _save_partial(self, partition_id, partition, raw, message):
        try:
            rows = _validated_rows(self.profile, partition, raw)
            saved = self._stored(self.store.commit_partition, self.profile, partition_id, partition.params, rows,
                                 status="partial", source_count=len(raw), message=message)
        except (IngestError, BusinessFieldError) as exc:
            self._failure(partition_id, partition, IngestError(str(exc)))
            return
        self._mark_issue(partition_id, partition, "partial", message, saved["row_count"])

    def _failure(self, partition_id: str, partition: Partition, exc: IngestError) -> None:
        status = exc.status if isinstance(exc, RequestError) else "failed"
        self._stored(self.store.record_failure, self.profile, partition_id, partition.params, str(exc), status=status)
        self._mark_issue(partition_id, partition, status, str(exc), 0)

    def _mark_issue(self, partition_id, partition, status, message, rows):
        self.meter.failed += 1
        self.meter.rows += rows
        self.meter.message = message
        self.meter.emit(
            "partition_progress", partition_status=status, row_count=rows, message=message,
            partition_id=partition_id, params=partition.params,
        )


def _ordered_targets(
    profiles: Mapping[str, Mapping[str, Any]], dataset_ids: Iterable[str]
) -> list[str]:
    remaining = dict.fromkeys(dataset_ids)
    for dataset_id in tuple(remaining):
        request = profiles.get(dataset_id, {}).get("request", {})
        for field in ("date_code_source", "date_cap_code_source"):
            source_id = request.get(field)
            if source_id in profiles:
                remaining.setdefault(source_id, None)
    ordered = []

    def visit(dataset_id: str) -> None:
        if dataset_id not in remaining:
            return
        del remaining[dataset_id]
        request = profiles.get(dataset_id, {}).get("request", {})
        for source_id in request_dependencies(request):
            visit(source_id)
        ordered.append(dataset_id)

    visit("trade_cal")
    visit("stock_basic")
    for dataset_id in list(remaining):
        visit(dataset_id)
    return ordered


def run_ingestion(
    store: Any,
    profiles: Mapping[str, Mapping[str, Any]],
    dataset_ids: Iterable[str],
    *,
    client: Any,
    mode: str = "sample",
    start: str | date | None = None,
    end: str | date | None = None,
    codes: Sequence[str] | None = None,
    progress: Progress | None = None,
    cancelled: Cancelled | None = None,
    today: str | date | None = None,
    max_partitions: int | None = None,
) -> dict[str, Any]:
    """Run configured datasets sequentially; checkpoints survive failures and cancellation."""
    if mode not in ("sample", "history", "incremental"):
        raise ValueError(f"未知采集模式 {mode}")
    start_date = _date(start) if start else None
    end_date = _date(end) if end else None
    clock = datetime.now(SHANGHAI)
    today = _date(today) if today else clock.date()
    if start_date and end_date and start_date > end_date:
        raise ValueError("开始日期必须早于或等于结束日期")
    if max_partitions is not None and max_partitions <= 0:
        raise ValueError("分片上限须大于零")

    store.initialize()
    limits = _RunLimits(max_partitions)
    reports = []
    unavailable = None
    stopped = False
    unique_codes = list(dict.fromkeys(codes or []))
    scope = f"incremental:{today.isoformat()}" if mode == "incremental" else "history"
    snapshot_scope = f"snapshot:{today.isoformat()}"
    universes: dict[tuple[str, str | None], list[str]] = {}
    universe_failures: dict[tuple[str, str | None], str] = {}

    def verified_universe(source: str, field: str | None = None) -> list[str]:
        key = source, field
        if key in universe_failures:
            raise IngestError(universe_failures[key])
        if key not in universes:
            if source == SCOPE_SOURCE:
                cached = store.read_operation(SCOPE_OPERATION, {})
                if (isinstance(cached, dict) and cached.get("version") == SCOPE_VERSION
                        and cached.get("checked_on") == today.isoformat() and valid_codes(cached.get("codes"))):
                    universes[key] = cached["codes"]
                else:
                    provider = getattr(client, "announcement_codes", None)
                    if not callable(provider):
                        universe_failures[key] = "公告拆分需要沪深交易所完整补充名录，当前采集客户端不支持"
                        raise IngestError(universe_failures[key])
                    try:
                        values = provider(cancelled=cancelled)
                    except AnnouncementScopeError as exc:
                        universe_failures[key] = str(exc)
                        raise IngestError(universe_failures[key]) from None
                    if not valid_codes(values):
                        universe_failures[key] = "交易所公告补充名录代码范围无效"
                        raise IngestError(universe_failures[key])
                    store.write_operation(SCOPE_OPERATION, {"version": SCOPE_VERSION,
                                          "checked_on": today.isoformat(), "codes": values})
                    universes[key] = values
                return universes[key]
            source_profile = profiles.get(source, {})
            source_request = source_profile.get("request", {})
            strategy = source_request.get("strategy")
            if strategy not in ("snapshot", "parameter_grid") or not all(
                store.partition_done(
                    source,
                    _partition_id(source_profile, Partition(params), snapshot_scope),
                    row_limit=source_profile.get("row_limit"),
                )
                for params in _grid(source_request)
            ):
                raise IngestError(f"缺少本日完整标的名录，请先成功同步 {source}")
            universes[key] = store.instrument_values(source, field) if field else store.instrument_codes(source)
        return universes[key]

    def verified_date_universe(source: str, day: date, field: str | None = None) -> list[str]:
        source_profile = profiles.get(source, {})
        source_request = source_profile.get("request", {})
        if source_request.get("strategy") != "date" or source_request.get("unit", "day") != "day":
            raise IngestError(f"历史标的来源 {source} 缺少日度配置")
        date_param = source_request.get("date_param", "trade_date")
        date_value = day.strftime(source_request.get("date_format", "%Y%m%d"))
        for params in _grid(source_request):
            partition = Partition({**params, date_param: date_value})
            if not any(store.partition_done(source, _partition_id(source_profile, partition, candidate),
                                            row_limit=source_profile.get("row_limit"))
                       for candidate in {scope, "history"}):
                raise IngestError(f"{source} 的 {day.isoformat()} 标的目录尚未完整采集")
        return store.instrument_values(source, field, event_date=day)

    for dataset_id in _ordered_targets(profiles, dataset_ids):
        meter = DatasetProgress(dataset_id, progress, client_metrics=getattr(client, "metrics", {}))
        meter.emit("dataset_started")
        profile = profiles.get(dataset_id)
        request = profile.get("request", {}) if profile else {}
        try:
            if stopped:
                raise IngestCancelled("本次采集已停止")
            _check_cancelled(cancelled)
            if unavailable:
                meter.status = "blocked"
                meter.message = unavailable
            elif not profile:
                meter.status = "unsupported"
                meter.message = "该数据集没有已核验的采集配置"
            elif mode == "incremental" and profile.get("archived_source"):
                meter.status = "complete"
                meter.message = "来源已停更，历史数据保留在本地。"
            elif (
                not profile.get("api_name")
                or request.get("strategy", "unsupported") == "unsupported"
                or profile.get("mode") in ("file", "realtime", "derived", "write", "private")
            ):
                meter.status = "unsupported"
                meter.message = "该数据集通过文件、实时、派生或账户专用流程处理"
            else:
                is_snapshot = request["strategy"] in SNAPSHOT_STRATEGIES
                runner = _DatasetPull(
                    store, client, profile, verified_universe,
                    snapshot_scope if is_snapshot else scope, meter, limits, cancelled, verified_date_universe,
                )
                partitions = plan_partitions(
                    store, profile, mode=mode, today=today,
                    start=start_date, end=end_date, codes=unique_codes, current_time=clock.time(),
                    universe=verified_universe, dated_universe=verified_date_universe, report_size=meter.set_planned,
                )
                planned = 0
                for partition in partitions:
                    planned += 1
                    runner.pull(partition)
                meter.planned = meter.completed + meter.skipped + meter.failed
                if meter.failed:
                    meter.status = "partial" if meter.completed or meter.skipped or meter.rows else "failed"
                elif planned or request.get("date_code_source"):
                    meter.status = "complete"
                else:
                    meter.status = "unsupported"
                    meter.message = "指定时间内没有可请求的交易日或已完成报告期"
        except IngestCancelled as exc:
            meter.status = "cancelled"
            meter.message = str(exc)
            stopped = True
        except RequestError as exc:
            if exc.invalid_token:
                unavailable = "Tushare Token 验证失败"
            elif exc.network_unavailable:
                unavailable = "Tushare 网络不可达"
            meter.status = exc.status
            if meter.completed or meter.skipped:
                meter.status = "partial"
            meter.message = str(exc)
        except IngestError as exc:
            meter.status = "failed"
            meter.message = str(exc)
        meter.emit("dataset_finished")
        reports.append(meter.summary())
    statuses = {report["status"] for report in reports}
    if stopped:
        status = "cancelled"
    elif statuses == {"complete"}:
        status = "complete"
    elif statuses.intersection({"complete", "partial"}):
        status = "partial"
    else:
        status = "failed"
    return {
        "mode": mode,
        "status": status,
        "datasets": reports,
        "cancelled": stopped,
        "attempted_partitions": limits.attempted,
    }


def _csv_identity(digest: str, encoding: str, start: int = 0, end: int = 0) -> str:
    return hashlib.sha256(f"tick-csv-v1:{digest}:{encoding}:{start}:{end}".encode()).hexdigest()


def _tick_rows(
    file: Any,
    encoding: str,
    digest: str,
    profile: Mapping[str, Any],
    cancelled: Cancelled | None,
) -> Iterator[dict[str, Any]]:
    file.seek(0)
    source = io.TextIOWrapper(file, encoding=encoding, newline="")
    try:
        reader = csv.DictReader(source, strict=True)
        fields = reader.fieldnames or []
        required = {
            *profile.get("key", ()),
            profile.get("date_field"),
            profile.get("code_field"),
            "UpdateTime",
        }.difference({"source_sha256", "source_row", None})
        if not required.issubset(fields) or len(fields) != len(set(fields)):
            raise IngestError("期货 Tick CSV 缺少必需列或有同名列")
        for number, row in enumerate(reader, 1):
            _check_cancelled(cancelled)
            if None in row or any(value is None for value in row.values()):
                raise IngestError(f"期货 Tick CSV 数据行 {number} 与字段数量不一致")
            if any(not row[column].strip() for column in required):
                raise IngestError(f"期货 Tick CSV 数据行 {number} 缺少业务标识")
            try:
                _date(row[profile["date_field"]])
                time_of_day.fromisoformat(row["UpdateTime"])
            except ValueError:
                raise IngestError(f"期货 Tick CSV 数据行 {number} 的交易日期或时间无效") from None
            yield {**row, "source_sha256": digest, "source_row": number}
    finally:
        source.detach()


def import_tick_csv(
    store: Any,
    profile: Mapping[str, Any],
    paths: Iterable[str | Path],
    *,
    batch_size: int = 5000,
    encoding: str = "utf-8-sig",
    progress: Progress | None = None,
    cancelled: Cancelled | None = None,
) -> dict[str, Any]:
    """Stream registered Tick files into bounded transactions and resume by content hash."""
    if batch_size < 1:
        raise ValueError("CSV 批次大小须大于零")
    if profile.get("mode") != "file":
        raise ValueError("请选择文件交付数据集")
    store.initialize()
    meter = DatasetProgress(profile["id"], progress)
    meter.emit("dataset_started")
    files = []
    for item in paths:
        path = Path(item).expanduser()
        digest = None
        file_id = None
        params = {"filename": path.name}
        try:
            _check_cancelled(cancelled)
            with path.open("rb") as original, tempfile.TemporaryFile() as staged:
                hasher = hashlib.sha256()
                for block in iter(lambda: original.read(1024 * 1024), b""):
                    _check_cancelled(cancelled)
                    hasher.update(block)
                    staged.write(block)
                digest = hasher.hexdigest()
                file_id = _csv_identity(digest, encoding)
                params = {**params, "sha256": digest, "encoding": encoding}
                if store.partition_done(meter.dataset_id, file_id):
                    meter.skipped += 1
                    files.append({**params, "status": "skipped"})
                    meter.emit(
                        "partition_progress", partition_status="skipped", row_count=0,
                        partition_id=file_id, params=params,
                    )
                    continue
                rows = _tick_rows(staged, encoding, digest, profile, cancelled)
                total = 0
                try:
                    while batch := list(itertools.islice(rows, batch_size)):
                        first = batch[0]["source_row"]
                        last = batch[-1]["source_row"]
                        partition_id = _csv_identity(digest, encoding, first, last)
                        batch_params = {**params, "first_row": first, "last_row": last}
                        total += len(batch)
                        if store.partition_done(meter.dataset_id, partition_id):
                            meter.skipped += 1
                            state, count = "skipped", 0
                        else:
                            saved = store.commit_partition(
                                profile, partition_id, batch_params, batch, source_count=len(batch)
                            )
                            count = saved.get("row_count", len(batch))
                            meter.completed += 1
                            meter.rows += count
                            state = "complete"
                        meter.emit(
                            "partition_progress", partition_status=state, row_count=count,
                            partition_id=partition_id, params=batch_params,
                        )
                finally:
                    rows.close()
                _check_cancelled(cancelled)
                store.commit_partition(profile, file_id, params, [], status="complete", source_count=total)
                files.append({**params, "status": "complete", "source_count": total})
        except IngestCancelled as exc:
            meter.status = "cancelled"
            meter.message = str(exc)
            break
        except (OSError, LookupError, UnicodeError, csv.Error, IngestError, BusinessFieldError) as exc:
            if isinstance(exc, (IngestError, BusinessFieldError)):
                message = str(exc)
            else:
                message = f"无法读取期货 Tick CSV（{type(exc).__name__}）"
            if digest and file_id:
                store.record_failure(profile, file_id, params, message)
            meter.failed += 1
            files.append({**params, "status": "failed", "message": message})
            meter.emit(
                "partition_progress", partition_status="failed", row_count=0,
                partition_id=file_id, params=params, message=message,
            )
    if meter.status == "running":
        if meter.failed:
            meter.status = "partial" if meter.completed or meter.skipped else "failed"
        else:
            meter.status = "complete"
    meter.emit("dataset_finished")
    return {**meter.summary(), "files": files}
