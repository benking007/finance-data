import unittest

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import PAGINATION_VERIFIED_MESSAGE, Store


DAY = "2026-10-30"
NEXT_DAY = "2026-10-31"


def event(index, country="美国", currency="USD", day=DAY):
    return {
        "date": day.replace("-", ""),
        "time": "15:30",
        "country": country,
        "currency": currency,
        "event": f"测试财经事件 {index}",
    }


class CalendarClient:
    def __init__(self, rows, *, offset_base=None, ignored_country_filter=False):
        self.rows = rows
        self.offset_base = offset_base
        self.ignored_country_filter = ignored_country_filter
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        self.calls.append(params.copy())
        if self.ignored_country_filter and params.get("country") == "美国":
            return [event("超出范围", country="加拿大", currency="CAD")]
        rows = [
            row for row in self.rows
            if params["start_date"] <= row["date"] <= params["end_date"]
            and all(row[name] == params[name] for name in ("country", "currency") if name in params)
        ]
        start = 0
        if self.offset_base is not None and "offset" in params:
            start = max(0, params["offset"] - self.offset_base)
        size = params.get("limit", profile["row_limit"])
        return rows[start:start + size]


class EconomicCalendarCapTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profile = load_profiles()["eco_cal"]

    def ingest(self, client, end=DAY):
        return run_ingestion(
            self.store, {"eco_cal": self.profile}, ["eco_cal"], client=client,
            mode="history", start=DAY, end=end, today="2026-11-28",
        )

    def test_splits_multi_day_windows_before_verifying_capped_single_day_pagination(self):
        rows = [event(index) for index in range(126)] + [event(126, day=NEXT_DAY)]
        client = CalendarClient(rows, offset_base=1)
        self.assertEqual(self.ingest(client, end=NEXT_DAY)["status"], "complete")
        self.assertEqual(self.store.row_count("eco_cal"), 127)
        self.assertEqual(client.calls[0], {"start_date": "20261030", "end_date": "20261031"})
        page_calls = [call for call in client.calls if "offset" in call]
        self.assertEqual([call["offset"] for call in page_calls], [98, 99])
        self.assertTrue(all(call["start_date"] == call["end_date"] == "20261030" for call in page_calls))
        partitions = self.store.list_partitions("eco_cal")
        self.assertIn(PAGINATION_VERIFIED_MESSAGE, [item["message"] for item in partitions])
        retry = CalendarClient(rows, offset_base=1)
        self.assertEqual(self.ingest(retry, end=NEXT_DAY)["datasets"][0]["skipped"], 1)
        self.assertEqual(retry.calls, [])

    def test_ignored_offsets_salvage_known_countries_and_currencies_without_certifying_day(self):
        rows = ([event(index) for index in range(75)]
                + [event(index, currency="EUR") for index in range(75, 100)]
                + [event(index) for index in range(100, 103)]
                + [event(103, country="加拿大", currency="CAD")])
        client = CalendarClient(rows)
        self.assertEqual(self.ingest(client)["status"], "partial")
        self.assertEqual(self.store.row_count("eco_cal"), 103)
        self.assertTrue(any(call.get("country") == "美国" for call in client.calls))
        self.assertEqual({call["currency"] for call in client.calls if "currency" in call}, {"USD", "EUR"})
        partition = next(item for item in self.store.list_partitions("eco_cal")
                         if "country" not in item["params"])
        self.assertEqual(partition["status"], "partial")
        self.assertIn("整日覆盖仍待核验", partition["message"])

        recovered = CalendarClient(rows, offset_base=0)
        self.assertEqual(self.ingest(recovered)["status"], "complete")
        self.assertEqual(self.store.row_count("eco_cal"), 104)

    def test_ignored_country_filter_does_not_write_out_of_scope_rows(self):
        rows = [event(index) for index in range(100)]
        client = CalendarClient(rows, ignored_country_filter=True)
        self.assertEqual(self.ingest(client)["status"], "partial")
        self.assertEqual(self.store.row_count("eco_cal"), 100)
        partition = next(item for item in self.store.list_partitions("eco_cal")
                         if item["params"].get("country") == "美国")
        self.assertEqual(partition["status"], "failed")
        self.assertIn("筛选范围以外", partition["message"])
