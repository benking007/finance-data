import unittest

from financedb.catalog import load_profiles
from financedb.ingest import RequestError, run_ingestion
from financedb.storage import Store


DAY = "2025-06-25"


def quote(bank, code, price):
    return {
        "trade_date": DAY.replace("-", ""),
        "qt_time": "10:15:00",
        "bank": bank,
        "ts_code": code,
        "buy_price": str(price),
    }


class QuoteClient:
    def __init__(self, rows, *, offset_base=None, invalid_bank=False, move_quote=False, missing_quote=False):
        self.rows = rows
        self.offset_base = offset_base
        self.invalid_bank = invalid_bank
        self.move_quote = move_quote
        self.missing_quote = missing_quote
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        self.calls.append(params.copy())
        if self.invalid_bank and params.get("bank") == "甲银行":
            return [quote("未发现银行", "250003.BC", 111)]
        rows = [row for row in self.rows
                if all(row.get(field) == params[field]
                       for field in ("trade_date", "bank", "ts_code") if field in params)]
        if params.get("bank") == "甲银行":
            if self.missing_quote:
                rows = [row for row in rows if row["buy_price"] not in {"103", "104"}]
            if self.move_quote and "ts_code" not in params:
                rows.sort(key=lambda row: row["buy_price"] == "103")
        offset = 0
        if "offset" in params and self.offset_base is not None:
            offset = max(0, params["offset"] - self.offset_base)
        return rows[offset:offset + profile["row_limit"]]


class OTCQuoteCapTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profile = load_profiles()["bc_otcqt"] | {"row_limit": 4}
        self.rows = [
            quote("甲银行", "250001.BC", 100),
            quote("乙银行", "250001.BC", 101),
            quote("甲银行", "250002.BC", 102),
            quote("甲银行", "250001.BC", 103),
            quote("甲银行", "250002.BC", 104),
            quote("甲银行", "250001.BC", 105),
            quote("乙银行", "250002.BC", 106),
            quote("未发现银行", "250003.BC", 107),
        ]

    def ingest(self, client):
        return run_ingestion(
            self.store, {"bc_otcqt": self.profile}, ["bc_otcqt"], client=client,
            mode="history", start=DAY, end=DAY, today="2026-11-27",
        )

    def test_verified_pagination_accepts_one_based_offsets_and_preserves_quote_updates(self):
        client = QuoteClient(self.rows, offset_base=1)
        self.assertEqual(self.ingest(client)["status"], "complete")
        self.assertEqual(self.store.row_count("bc_otcqt"), len(self.rows))
        self.assertEqual([call["offset"] for call in client.calls if "offset" in call], [2, 3, 5, 7])
        self.assertFalse(any("bank" in call for call in client.calls))
        retry = QuoteClient(self.rows, offset_base=1)
        self.assertEqual(self.ingest(retry)["datasets"][0]["skipped"], 1)
        self.assertFalse(retry.calls)

    def test_ignored_offset_recovers_observed_banks_and_bonds_without_certifying_the_day(self):
        client = QuoteClient(self.rows)
        result = self.ingest(client)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(self.store.row_count("bc_otcqt"), 7)
        self.assertTrue(any(call.get("bank") == "甲银行" and call.get("ts_code") == "250001.BC"
                            for call in client.calls))
        self.assertTrue(any(call.get("bank") == "乙银行" for call in client.calls))
        self.assertFalse(any(call.get("bank") == "未发现银行" for call in client.calls))
        partition = next(row for row in self.store.list_partitions("bc_otcqt")
                         if row["params"] == {"trade_date": DAY.replace("-", "")})
        self.assertEqual(partition["status"], "partial")

        retry = QuoteClient(self.rows)
        self.assertEqual(self.ingest(retry)["status"], "partial")
        self.assertFalse(any("ts_code" in call or call.get("bank") == "乙银行" for call in retry.calls))
        self.assertEqual(self.store.row_count("bc_otcqt"), 7)

        complete = QuoteClient(self.rows, offset_base=0)
        self.assertEqual(self.ingest(complete)["status"], "complete")
        self.assertEqual(self.store.row_count("bc_otcqt"), len(self.rows))

    def test_response_outside_observed_bank_is_never_stored(self):
        client = QuoteClient(self.rows, invalid_bank=True)
        result = self.ingest(client)
        self.assertEqual(result["status"], "partial")
        self.assertFalse(self.store.query_rows(self.profile, code="250003.BC")["rows"])
        failed = next(row for row in self.store.list_partitions("bc_otcqt")
                      if row["params"].get("bank") == "甲银行")
        self.assertEqual(failed["status"], "failed")
        self.assertIn("bank", failed["message"])

    def test_child_must_cover_earlier_quote_even_when_its_code_and_time_are_present(self):
        client = QuoteClient(self.rows, missing_quote=True)
        self.assertEqual(self.ingest(client)["status"], "partial")
        failed = next(row for row in self.store.list_partitions("bc_otcqt")
                      if row["params"].get("bank") == "甲银行")
        self.assertEqual(failed["status"], "failed")
        self.assertIn("缺少上限响应中已出现的记录", failed["message"])
        retained = self.store.query_rows(self.profile, code="250001.BC")["rows"]
        self.assertIn("103", [row["buy_price"] for row in retained])

    def test_capped_bank_can_be_split_when_a_known_quote_is_not_on_its_first_page(self):
        client = QuoteClient(self.rows, move_quote=True)
        self.assertEqual(self.ingest(client)["status"], "partial")
        self.assertEqual(self.store.row_count("bc_otcqt"), 7)
        states = {tuple(row["params"].get(name, "") for name in ("bank", "ts_code")): row["status"]
                  for row in self.store.list_partitions("bc_otcqt")}
        self.assertEqual(states["甲银行", ""], "partial")
        self.assertEqual(states["甲银行", "250001.BC"], "complete")

    def test_rate_limit_during_offset_check_retains_sample_for_retry(self):
        class RateLimitedClient(QuoteClient):
            def query(self, profile, params, *, cancelled=None):
                if "offset" in params:
                    raise RequestError("HTTP 429", status="blocked", stop_dataset=True)
                return super().query(profile, params, cancelled=cancelled)

        blocked = self.ingest(RateLimitedClient(self.rows))
        self.assertEqual(blocked["status"], "partial")
        self.assertEqual(self.store.row_count("bc_otcqt"), self.profile["row_limit"])
        partition, = self.store.list_partitions("bc_otcqt")
        self.assertEqual(partition["status"], "partial")
        self.assertIn("HTTP 429", partition["message"])

        self.assertEqual(self.ingest(QuoteClient(self.rows, offset_base=0))["status"], "complete")
        self.assertEqual(self.store.row_count("bc_otcqt"), len(self.rows))
