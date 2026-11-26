import unittest

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import Store


class USBasicFixture:
    def __init__(self, rows, *, ignore_offset=False):
        self.rows = rows
        self.ignore_offset = ignore_offset
        self.params = []

    def query(self, profile, params, *, cancelled=None):
        self.params.append(params)
        start = 0 if self.ignore_offset else int(params["offset"]) - 1
        size = int(params["limit"])
        return self.rows[start:start + size]


class USBasicPaginationTest(unittest.TestCase):
    def setUp(self):
        profile = load_profiles()["us_basic"]
        self.profile = profile | {"request": profile["request"] | {
            "pagination": profile["request"]["pagination"] | {"page_size": 4},
        }}
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.rows = [
            {"ts_code": "REUSE", "exchange": "NASDAQ", "list_date": "19920406", "list_status": "D"},
            {"ts_code": "AAPL", "exchange": "NASDAQ", "list_date": "19801212", "list_status": "L"},
            {"ts_code": "DUAL", "exchange": "NASDAQ", "list_date": "20011029", "list_status": "L"},
            {"ts_code": "ZBRA", "exchange": "NASDAQ", "list_date": "19910815", "list_status": "L"},
            {"ts_code": "REUSE", "exchange": "NYSE", "list_date": "20080313", "list_status": "L"},
            {"ts_code": "DUAL", "exchange": "NYSE", "list_date": "20011029", "list_status": "L"},
            {"ts_code": "YPF", "exchange": "NYSE", "list_date": "19930628", "list_status": "L"},
        ]

    def run_fixture(self, client):
        return run_ingestion(self.store, {"us_basic": self.profile}, ["us_basic"], client=client,
                             mode="history", today="2026-11-26")

    def test_verified_pages_keep_reused_and_cross_listed_codes(self):
        client = USBasicFixture(self.rows)
        result = self.run_fixture(client)

        self.assertEqual(result["status"], "complete")
        self.assertEqual([int(params["offset"]) for params in client.params], [1, 3, 5])
        self.assertEqual(self.store.row_count("us_basic"), len(self.rows))
        self.assertEqual(len(self.store.query_rows(self.profile, code="REUSE")["rows"]), 2)
        self.assertEqual(len(self.store.query_rows(self.profile, code="DUAL")["rows"]), 2)

        client.params.clear()
        retry = self.run_fixture(client)
        self.assertEqual(retry["datasets"][0]["skipped"], 1)
        self.assertEqual(client.params, [])

    def test_verified_pages_allow_identical_source_rows_among_new_records(self):
        rows = [*self.rows[:4], self.rows[2], *self.rows[4:]]
        client = USBasicFixture(rows)

        result = self.run_fixture(client)

        self.assertEqual(result["status"], "complete")
        self.assertEqual([int(params["offset"]) for params in client.params], [1, 3, 5, 7])
        self.assertEqual(self.store.row_count("us_basic"), len(self.rows))

    def test_repeated_page_leaves_snapshot_incomplete_and_retries(self):
        client = USBasicFixture(self.rows, ignore_offset=True)
        result = self.run_fixture(client)

        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.store.row_count("us_basic"), 0)
        self.assertEqual(self.store.list_partitions("us_basic")[0]["status"], "failed")

        valid = USBasicFixture(self.rows)
        self.assertEqual(self.run_fixture(valid)["status"], "complete")
        self.assertEqual(self.store.row_count("us_basic"), len(self.rows))

    def test_reused_codes_cover_all_known_listing_periods(self):
        rows = [
            {"ts_code": "REUSE", "exchange": "NASDAQ", "list_date": "19920406", "delist_date": "20001110"},
            {"ts_code": "REUSE", "exchange": "NYSE", "list_date": "20080313", "delist_date": "20260601"},
            {"ts_code": "LIVE", "exchange": "NASDAQ", "list_date": "19920406", "delist_date": "20001110"},
            {"ts_code": "LIVE", "exchange": "NYSE", "list_date": "20080313"},
            {"ts_code": "UNKNOWN", "exchange": "NASDAQ", "delist_date": "20001110"},
            {"ts_code": "UNKNOWN", "exchange": "NYSE", "list_date": "20080313", "delist_date": "20260601"},
        ]
        self.store.initialize()
        self.store.commit_partition(self.profile, "listing_history", {}, rows)

        self.assertEqual(self.store.instrument_lifetimes("us_basic"), {
            "REUSE": ("1992-04-06", "2026-06-01"),
            "LIVE": ("1992-04-06", None),
            "UNKNOWN": (None, "2026-06-01"),
        })
