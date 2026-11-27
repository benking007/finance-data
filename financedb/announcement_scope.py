"""Validated exchange directories supplement Tushare's A-share stock master."""

import math
import re


SCOPE_SOURCE = "exchange_announcements"
SCOPE_OPERATION = "announcement_exchange_scope"
SCOPE_VERSION = 1
SCOPE_RETRY_COOLDOWN = 60
SSE_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SSE_REFERER = "https://www.sse.com.cn/assortment/stock/list/share/"
SZSE_URL = "https://www.szse.cn/api/report/ShowReport/data"
SZSE_REFERER = "https://www.szse.cn/market/product/stock/list/index.html"
SZSE_INACTIVE_REFERER = "https://www.szse.cn/market/stock/suspend/index.html"
MAX_PAGES = 100


class AnnouncementScopeError(ValueError):
    """An exchange directory did not establish its complete code range."""

    def __init__(self, message, *, retryable=False, retry_after=0):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


def directory_label(url, params):
    if url == SSE_URL:
        return f"上交所 B 股第 {params['pageHelp.pageNo']} 页"
    if params.get("CATALOGID") == "1110":
        name = "深交所 B 股"
    else:
        name = "深交所暂停上市" if params.get("TABKEY") == "tab1" else "深交所终止上市"
    return f"{name}第 {params['PAGENO']} 页"


def _integer(value, name, *, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AnnouncementScopeError(f"交易所名录的 {name} 无效")
    return value


def _code(value, suffix):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{6}", value):
        raise AnnouncementScopeError("交易所名录含无效证券代码")
    return f"{value}.{suffix}"


def valid_codes(codes):
    return (isinstance(codes, list) and bool(codes)
            and all(isinstance(code, str) and re.fullmatch(r"[0-9]{6}\.(?:SH|SZ)", code) for code in codes)
            and len(codes) == len(set(codes)))


def _sse_codes(get_json):
    codes = []
    total = None
    page = 1
    size = 100
    while True:
        payload = get_json(SSE_URL, {
            "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L", "STOCK_TYPE": "2", "isPagination": "true",
            "pageHelp.pageSize": str(size), "pageHelp.pageNo": str(page),
        }, SSE_REFERER)
        if not isinstance(payload, dict) or payload.get("actionErrors") or payload.get("fieldErrors"):
            raise AnnouncementScopeError("上交所 B 股名录响应异常")
        meta, rows = payload.get("pageHelp"), payload.get("result")
        if not isinstance(meta, dict) or not isinstance(rows, list):
            raise AnnouncementScopeError("上交所 B 股名录缺少分页信息")
        current_total = _integer(meta.get("total"), "total", minimum=1)
        pages = _integer(meta.get("pageCount"), "pageCount", minimum=1)
        if (meta.get("pageNo") != page or meta.get("pageSize") != size
                or pages != math.ceil(current_total / size) or pages > MAX_PAGES
                or (total is not None and current_total != total)):
            raise AnnouncementScopeError("上交所 B 股名录分页范围发生变化")
        total = current_total
        if len(rows) != min(size, total - (page - 1) * size):
            raise AnnouncementScopeError("上交所 B 股名录返回行数与总数不符")
        for row in rows:
            if not isinstance(row, dict) or row.get("STOCK_TYPE") != "2":
                raise AnnouncementScopeError("上交所 B 股名录返回了其他证券类型")
            codes.append(_code(row.get("B_STOCK_CODE"), "SH"))
        if len(set(codes)) != len(codes):
            raise AnnouncementScopeError("上交所 B 股名录分页重复代码")
        if page == pages:
            return codes
        page += 1


def _szse_codes(get_json, catalog, tabkey, code_field, *, allow_empty=False):
    codes = []
    total = size = None
    page = 1
    referer = SZSE_REFERER if catalog == "1110" else SZSE_INACTIVE_REFERER
    while True:
        payload = get_json(SZSE_URL, {
            "SHOWTYPE": "JSON", "CATALOGID": catalog, "TABKEY": tabkey, "PAGENO": str(page),
        }, referer)
        if not isinstance(payload, list):
            raise AnnouncementScopeError("深交所名录响应格式异常")
        matches = [tab for tab in payload if isinstance(tab, dict) and isinstance(tab.get("metadata"), dict)
                   and tab["metadata"].get("catalogid") == catalog and tab["metadata"].get("tabkey") == tabkey]
        if len(matches) != 1 or matches[0].get("error"):
            raise AnnouncementScopeError("深交所名录缺少指定报表或返回了错误")
        meta, rows = matches[0]["metadata"], matches[0].get("data")
        current_total = _integer(meta.get("recordcount"), "recordcount", minimum=0 if allow_empty else 1)
        current_size = _integer(meta.get("pagesize"), "pagesize", minimum=1)
        pages = _integer(meta.get("pagecount"), "pagecount")
        if (meta.get("pageno") != page or pages != math.ceil(current_total / current_size)
                or pages > MAX_PAGES or current_size > 1000
                or (total is not None and (current_total, current_size) != (total, size))):
            raise AnnouncementScopeError("深交所名录分页范围发生变化")
        total, size = current_total, current_size
        if not isinstance(rows, list) or len(rows) != min(size, max(0, total - (page - 1) * size)):
            raise AnnouncementScopeError("深交所名录返回行数与总数不符")
        for row in rows:
            if not isinstance(row, dict):
                raise AnnouncementScopeError("深交所名录的证券记录无效")
            codes.append(_code(row.get(code_field), "SZ"))
        if len(set(codes)) != len(codes):
            raise AnnouncementScopeError("深交所名录分页重复代码")
        if page >= pages:
            return codes
        page += 1


def fetch_exchange_codes(get_json):
    """Include all SSE B shares and SZSE B shares, suspended and delisted codes.

    The SZSE inactive directories include A shares too. Retain those original
    security codes: they can also differ from today's Tushare stock master.
    Publish the union only after every directory and page has been validated.
    """
    sources = [
        _sse_codes(get_json),
        _szse_codes(get_json, "1110", "tab2", "bgdm"),
        _szse_codes(get_json, "1793_ssgs", "tab1", "zqdm", allow_empty=True),
        _szse_codes(get_json, "1793_ssgs", "tab2", "zqdm"),
    ]
    return sorted({code for codes in sources for code in codes})
