import json
import unittest

from sqlalchemy import select

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import Store, business_tables, revisions


ANNOUNCEMENT = {
    "url": "https://example.test/announcement.pdf", "ts_code": "000001.SZ",
    "ann_date": "20261124", "name": "测试公司", "title": "董事会决议公告",
    "rec_time": "2026-11-24 09:00:00",
}


class AnnouncementClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        self.calls.append(params.copy())
        return self.rows if params["ann_date"] == "20261124" else []


class AnnouncementTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profiles = load_profiles()
        self.profile = self.profiles["anns_d"]

    def ingest(self, client, *, start="2026-11-24", end="2026-11-24"):
        return run_ingestion(self.store, self.profiles, ["anns_d"], client=client,
                             mode="history", start=start, end=end, today="2026-11-26")

    def test_conflicting_metadata_is_preserved_and_latest_publication_is_current(self):
        revised = ANNOUNCEMENT | {"title": "董事会决议公告（更正）", "rec_time": "2026-11-24 15:30:00"}
        result = self.ingest(AnnouncementClient([revised, ANNOUNCEMENT, revised]))

        self.assertEqual(result["status"], "complete", result)
        self.assertEqual(self.store.query_rows(self.profile)["rows"], [revised])
        self.assertEqual(self.store.row_count("anns_d"), 1)
        with self.store.engine.connect() as conn:
            variants = [json.loads(value) for value in conn.scalars(
                select(revisions.c.payload).where(revisions.c.dataset_id == "anns_d"))]
        self.assertCountEqual(variants, [ANNOUNCEMENT, revised])

    def test_resume_retries_failed_day_and_keeps_completed_day(self):
        incomplete = ANNOUNCEMENT | {"url": ""}
        first = self.ingest(AnnouncementClient([incomplete]), start="2026-11-23")
        self.assertEqual(first["status"], "partial")
        client = AnnouncementClient([ANNOUNCEMENT, ANNOUNCEMENT | {"name": "历史名称"}])
        second = self.ingest(client, start="2026-11-23")

        self.assertEqual(second["status"], "complete", second)
        self.assertEqual(second["datasets"][0]["skipped"], 1)
        self.assertEqual(client.calls, [{"ann_date": "20261124"}])
        self.assertEqual(self.store.row_count("anns_d"), 1)

    def test_timestamp_order_is_stable_across_batches_and_legacy_rows(self):
        self.store.initialize()
        legacy = {key: value for key, value in self.profile.items() if key not in {"versioned", "revision_field"}}
        self.store.commit_partition(legacy, "legacy", {}, [ANNOUNCEMENT])
        newer = ANNOUNCEMENT | {"title": "修订标题", "rec_time": "2026-11-24T15:00:00+08:00"}
        self.store.commit_partition(self.profile, "newer", {}, [newer])
        self.store.commit_partition(self.profile, "older", {}, [ANNOUNCEMENT])
        self.assertEqual(self.store.query_rows(self.profile)["rows"], [newer])
        self.assertEqual(self.store.row_count("anns_d"), 1)
        with self.store.engine.connect() as conn:
            self.assertEqual(conn.scalar(select(business_tables["anns_d"].c._record_key)),
                             conn.scalar(select(revisions.c.record_key).where(revisions.c.dataset_id == "anns_d")))

    def test_same_timestamp_variants_have_a_stable_current_row(self):
        other = ANNOUNCEMENT | {"title": "来源中的另一标题"}
        self.store.initialize()
        self.store.commit_partition(self.profile, "first", {}, [ANNOUNCEMENT, other])
        first = self.store.query_rows(self.profile)["rows"]
        self.store.commit_partition(self.profile, "second", {}, [other, ANNOUNCEMENT])
        self.assertEqual(self.store.query_rows(self.profile)["rows"], first)

    def test_missing_publication_time_still_preserves_metadata_variants(self):
        row = ANNOUNCEMENT | {"rec_time": None}
        result = self.ingest(AnnouncementClient([row, row | {"name": "历史名称"}]))
        self.assertEqual(result["status"], "complete", result)
        self.assertEqual(self.store.row_count("anns_d"), 1)

    def test_capped_response_with_variants_splits_without_losing_records(self):
        profile = self.profile | {"row_limit": 4}
        self.profiles["anns_d"] = profile
        rows = [ANNOUNCEMENT, ANNOUNCEMENT | {"name": "历史名称"},
                ANNOUNCEMENT | {"title": "修订标题", "rec_time": "2026-11-24 10:00:00"},
                ANNOUNCEMENT | {"ts_code": "000002.SZ", "url": "https://example.test/another.pdf"}]

        class CappedClient:
            def query(self, profile, params, *, cancelled=None):
                if profile["id"] == "stock_basic":
                    return [{"ts_code": code} for code in ["000001.SZ", "000002.SZ"]] if params["list_status"] == "L" else []
                return [row for row in rows if not params.get("ts_code") or row["ts_code"] == params["ts_code"]][:4]

        result = run_ingestion(self.store, self.profiles, ["stock_basic", "anns_d"], client=CappedClient(),
                               mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-26")
        self.assertEqual(result["status"], "complete", result)
        self.assertEqual(self.store.row_count("anns_d"), 2)
        self.assertEqual(result["datasets"][-1]["metrics"]["split_partitions"], 1)
        with self.store.engine.connect() as conn:
            self.assertEqual(len(conn.scalars(select(revisions.c.payload).where(revisions.c.dataset_id == "anns_d")).all()), 4)

    def test_invalid_publication_time_keeps_partition_failed(self):
        result = self.ingest(AnnouncementClient([ANNOUNCEMENT | {"rec_time": "not-a-date"}]))
        self.assertEqual(result["status"], "failed", result)
        self.assertIn("发布时间无效", result["datasets"][0]["message"])
        self.assertEqual(self.store.row_count("anns_d"), 0)

    def test_collecting_recent_data_preserves_existing_older_history(self):
        self.store.initialize()
        older = ANNOUNCEMENT | {"ann_date": "20200101", "rec_time": "2020-01-01 09:00:00",
                                "url": "https://example.test/older.pdf"}
        self.store.commit_partition(self.profile, "older-history", {}, [older])
        self.assertEqual(self.ingest(AnnouncementClient([ANNOUNCEMENT]))["status"], "complete")
        self.assertEqual(self.store.row_count("anns_d"), 2)
        self.assertEqual(self.store.dataset_stats("anns_d")["min_date"], "2020-01-01")

    def test_unversioned_market_data_still_rejects_conflicting_rows(self):
        class ConflictingClient:
            def query(self, profile, params, *, cancelled=None):
                return [{"ts_code": "000001.SZ", "trade_date": "20261124", "close": value}
                        for value in ["10", "11"]]

        result = run_ingestion(self.store, self.profiles, ["daily"], client=ConflictingClient(),
                               mode="history", start="2026-11-24", end="2026-11-24")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.store.row_count("daily"), 0)
