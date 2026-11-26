from copy import deepcopy
import unittest

from financedb.announcement_scope import AnnouncementScopeError, fetch_exchange_codes, valid_codes


class DirectoryFixture:
    def __init__(self):
        self.calls = []

    def __call__(self, url, params, referer):
        self.calls.append((url, params.copy(), referer))
        if "sseQuery" in url:
            codes = ["900939", "900952"]  # Include a delisted B share.
            return {"pageHelp": {"total": 2, "pageCount": 1, "pageNo": 1, "pageSize": 100},
                    "result": [{"B_STOCK_CODE": code, "STOCK_TYPE": "2"} for code in codes]}
        catalog, tab, page = params["CATALOGID"], params["TABKEY"], int(params["PAGENO"])
        if catalog == "1110":
            codes, field = ["200992", "201872", "200002"], "bgdm"
        elif tab == "tab1":
            codes, field = [], "zqdm"
        else:
            codes, field = ["200003", "000003", "200005"], "zqdm"
        size = 2
        chosen = {"metadata": {"catalogid": catalog, "tabkey": tab, "recordcount": len(codes),
                               "pagecount": (len(codes) + size - 1) // size, "pagesize": size, "pageno": page},
                  "data": [{field: code} for code in codes[(page - 1) * size:page * size]], "error": None}
        # Other tabs may be returned even when one tab was requested.
        return [{"metadata": {"catalogid": "1110", "tabkey": "unused"}, "data": []}, chosen]


class AnnouncementScopeTest(unittest.TestCase):
    def test_all_pages_current_and_inactive_codes_are_included(self):
        fixture = DirectoryFixture()
        codes = fetch_exchange_codes(fixture)
        self.assertEqual(set(codes), {"900939.SH", "900952.SH", "200992.SZ", "201872.SZ", "200002.SZ",
                                      "200003.SZ", "000003.SZ", "200005.SZ"})
        self.assertTrue(valid_codes(codes))
        self.assertEqual(len(fixture.calls), 6)
        self.assertTrue(all(url.startswith("https://") and "token" not in params for url, params, _ in fixture.calls))

    def mutate(self, modify):
        fixture = DirectoryFixture()

        def get(url, params, referer):
            payload = deepcopy(fixture(url, params, referer))
            modify(payload, params)
            return payload

        return get

    def test_repeated_szse_page_cannot_certify_the_directory(self):
        def repeated(payload, params):
            if params.get("CATALOGID") == "1110" and params["PAGENO"] == "2":
                payload[-1]["data"] = [{"bgdm": "200992"}]
        with self.assertRaisesRegex(AnnouncementScopeError, "重复代码"):
            fetch_exchange_codes(self.mutate(repeated))

    def test_changing_count_and_wrong_page_are_rejected(self):
        for key, value in [("recordcount", 4), ("pageno", 1)]:
            with self.subTest(key=key):
                def changed(payload, params):
                    if params.get("CATALOGID") == "1110" and params["PAGENO"] == "2":
                        payload[-1]["metadata"][key] = value
                with self.assertRaises(AnnouncementScopeError):
                    fetch_exchange_codes(self.mutate(changed))

    def test_wrong_tab_errors_and_truncated_pages_are_rejected(self):
        for failure in ["tab", "error", "truncated", "invalid_code"]:
            with self.subTest(failure=failure):
                def broken(payload, params):
                    if params.get("CATALOGID") == "1110":
                        if failure == "tab":
                            payload[-1]["metadata"]["tabkey"] = "tab1"
                        elif failure == "error":
                            payload[-1]["error"] = "report failed"
                        elif failure == "truncated":
                            payload[-1]["data"] = []
                        else:
                            payload[-1]["data"][0]["bgdm"] = "bad-code"
                with self.assertRaises(AnnouncementScopeError):
                    fetch_exchange_codes(self.mutate(broken))

    def test_sse_totals_and_security_type_are_checked(self):
        for failure in ["count", "type", "duplicate"]:
            with self.subTest(failure=failure):
                def broken(payload, params):
                    if "sqlId" in params:
                        if failure == "count":
                            payload["pageHelp"]["total"] = 3
                        elif failure == "type":
                            payload["result"][0]["STOCK_TYPE"] = "1"
                        else:
                            payload["result"][1] = payload["result"][0].copy()
                with self.assertRaises(AnnouncementScopeError):
                    fetch_exchange_codes(self.mutate(broken))

    def test_malformed_cached_codes_are_rejected(self):
        for codes in [None, [], ["200992.SZ", "200992.SZ"], [{}], ["invalid"], ["AAPL.US"]]:
            with self.subTest(codes=codes):
                self.assertFalse(valid_codes(codes))
