import json
import unittest
from unittest.mock import patch

from sqlalchemy import select

from financedb.catalog import load_profiles
from financedb.announcement_scope import AnnouncementScopeError, SCOPE_OPERATION
from financedb.ingest import IngestCancelled, run_ingestion
from financedb.storage import PAGINATION_VERIFIED_MESSAGE, Store, revisions


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


class CappedAnnouncementClient:
    def __init__(self, rows, *, pagination=False, initial_offset=0, omit_code=None):
        self.rows = rows
        self.pagination = pagination
        self.initial_offset = initial_offset
        self.omit_code = omit_code
        self.calls = []
        self.scope_calls = 0

    def announcement_codes(self, *, cancelled=None):
        self.scope_calls += 1
        return ["200992.SZ", "900939.SH", "201872.SZ"]

    def query(self, profile, params, *, cancelled=None):
        self.calls.append((profile["id"], params.copy()))
        if profile["id"] == "stock_basic":
            return [{"ts_code": "000001.SZ"}] if params["list_status"] == "L" else []
        if params.get("ann_date") != "20261124":
            return []
        if params.get("ts_code") == self.omit_code and self.omit_code is not None:
            return []
        rows = [row for row in self.rows if not params.get("ts_code") or row["ts_code"] == params["ts_code"]]
        offset = max(0, int(params.get("offset", self.initial_offset)) - self.initial_offset) if self.pagination else 0
        return rows[offset:offset + profile["row_limit"]]


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
            identities = set(conn.scalars(select(revisions.c.record_key).where(revisions.c.dataset_id == "anns_d")))
            self.assertEqual(len(identities), 1)

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
            def announcement_codes(self, *, cancelled=None):
                return ["200992.SZ", "900939.SH"]

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

    def capped_fixture(self):
        self.profiles["anns_d"] = self.profile | {"row_limit": 4}
        return [ANNOUNCEMENT | {"ts_code": code, "url": f"https://example.test/{index}.pdf"}
                for index, code in enumerate(["000001.SZ", "830001.BJ", "830001.BJ", "830001.BJ", "430001.BJ"])]

    def ingest_capped(self, client):
        return run_ingestion(self.store, self.profiles, ["stock_basic", "anns_d"], client=client,
                             mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-26")

    def test_b_share_scope_includes_codes_not_present_in_capped_sample(self):
        self.profiles["anns_d"] = self.profile | {"row_limit": 4}
        rows = [ANNOUNCEMENT | {"ts_code": code, "url": f"https://example.test/bshare-{index}.pdf"}
                for index, code in enumerate(["000001.SZ", "200992.SZ", "900939.SH", "000001.SZ", "201872.SZ"])]
        client = CappedAnnouncementClient(rows)
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "complete", result)
        self.assertEqual(self.store.row_count("anns_d"), 5)
        self.assertEqual(client.scope_calls, 1)
        self.assertTrue(any(params.get("ts_code") == "201872.SZ" for _, params in client.calls))
        parent = next(item for item in self.store.list_partitions("anns_d") if "ts_code" not in item["params"])
        self.assertEqual(parent["status"], "complete")
        self.assertIn("exchange_announcements", parent["message"])
        cached = self.store.read_operation(SCOPE_OPERATION)
        self.assertEqual(cached["checked_on"], "2026-11-26")

        client.calls.clear()
        self.assertEqual(self.ingest_capped(client)["status"], "complete")
        self.assertEqual(client.calls, [])
        self.assertEqual(client.scope_calls, 1)

    def legacy_a_share_split(self):
        self.profiles["anns_d"] = self.profile | {"row_limit": 4}
        rows = [ANNOUNCEMENT | {"ts_code": code, "url": f"https://example.test/legacy-{index}.pdf"}
                for index, code in enumerate(["000001.SZ", "000001.SZ", "000002.SZ", "000002.SZ", "200992.SZ", "900939.SH"])]

        class TwoStockClient(CappedAnnouncementClient):
            def query(self, profile, params, *, cancelled=None):
                if profile["id"] == "stock_basic":
                    return [{"ts_code": code} for code in ["000001.SZ", "000002.SZ"]] if params["list_status"] == "L" else []
                return super().query(profile, params, cancelled=cancelled)

        legacy = self.profiles | {"anns_d": self.profiles["anns_d"] | {"request": {
            key: value for key, value in self.profile["request"].items() if key != "cap_code_supplement"}}}
        before = run_ingestion(self.store, legacy, ["stock_basic", "anns_d"], client=TwoStockClient(rows),
                               mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-26")
        self.assertEqual(before["status"], "complete", before)
        self.assertEqual(self.store.row_count("anns_d"), 4)
        return TwoStockClient(rows)

    def test_old_a_only_parent_is_rechecked_while_completed_children_are_reused(self):
        client = self.legacy_a_share_split()
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "complete", result)
        self.assertEqual(result["datasets"][-1]["skipped"], 2)
        self.assertEqual(self.store.row_count("anns_d"), 6)
        self.assertFalse(any(params.get("ts_code") in {"000001.SZ", "000002.SZ"} for _, params in client.calls))

    def test_exchange_failure_reopens_unverified_old_parent_and_keeps_sample(self):
        client = self.legacy_a_share_split()

        def unavailable(*, cancelled=None):
            raise AnnouncementScopeError("交易所名录不可用")

        client.announcement_codes = unavailable
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(self.store.row_count("anns_d"), 4)
        parent = next(item for item in self.store.list_partitions("anns_d") if "ts_code" not in item["params"])
        self.assertEqual(parent["status"], "partial")
        self.assertIsNone(self.store.read_operation(SCOPE_OPERATION))

    def test_supplement_cache_is_shared_with_same_day_resume_and_refreshed_when_stale(self):
        rows = self.capped_fixture()
        first = CappedAnnouncementClient(rows)
        self.ingest_capped(first)
        self.assertEqual(first.scope_calls, 1)
        second = CappedAnnouncementClient(rows)
        self.ingest_capped(second)
        self.assertEqual(second.scope_calls, 0)
        cached = self.store.read_operation(SCOPE_OPERATION) | {"checked_on": "2026-11-25"}
        self.store.write_operation(SCOPE_OPERATION, cached)
        third = CappedAnnouncementClient(rows)
        self.ingest_capped(third)
        self.assertEqual(third.scope_calls, 1)

    def test_cancellation_during_directory_fetch_is_not_reported_as_a_data_failure(self):
        client = CappedAnnouncementClient(self.capped_fixture())

        def cancelled(*, cancelled=None):
            raise IngestCancelled("已停止")

        client.announcement_codes = cancelled
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "cancelled", result)
        self.assertEqual(result["datasets"][-1]["failed"], 0)
        self.assertEqual(self.store.list_partitions("anns_d"), [])

    def test_unavailable_directory_is_attempted_only_once_per_job(self):
        rows = self.capped_fixture()

        class UnavailableClient(CappedAnnouncementClient):
            def announcement_codes(self, *, cancelled=None):
                self.scope_calls += 1
                raise AnnouncementScopeError("交易所不可达")

            def query(self, profile, params, *, cancelled=None):
                if profile["id"] == "anns_d" and params.get("ann_date") == "20261125":
                    rows = super().query(profile, params | {"ann_date": "20261124"}, cancelled=cancelled)
                    return [row | {"ann_date": "20261125", "rec_time": "2026-11-25 09:00:00"} for row in rows]
                return super().query(profile, params, cancelled=cancelled)

        client = UnavailableClient(rows)
        result = run_ingestion(self.store, self.profiles, ["stock_basic", "anns_d"], client=client,
                               mode="history", start="2026-11-24", end="2026-11-25", today="2026-11-26")
        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(client.scope_calls, 1)
        self.assertEqual(self.store.row_count("anns_d"), 8)
        self.assertIsNone(self.store.read_operation(SCOPE_OPERATION))

    def test_temporary_directory_failure_retries_after_cooldown_in_same_job(self):
        self.profiles["anns_d"] = self.profile | {"row_limit": 4}
        rows = [ANNOUNCEMENT | {"ts_code": code, "url": f"https://example.test/{index}.pdf"}
                for index, code in enumerate(["000001.SZ", "200992.SZ", "900939.SH", "000001.SZ", "201872.SZ"])]
        current_time = 0
        scope_attempt_times = []
        day_times = {"20261123": 0, "20261124": 90, "20261125": 180}

        class RecoveringClient(CappedAnnouncementClient):
            def announcement_codes(self, *, cancelled=None):
                scope_attempt_times.append(current_time)
                if len(scope_attempt_times) == 1:
                    raise AnnouncementScopeError("交易所暂不可达", retryable=True, retry_after=180)
                return super().announcement_codes(cancelled=cancelled)

            def query(self, profile, params, *, cancelled=None):
                nonlocal current_time
                if profile["id"] != "anns_d":
                    return super().query(profile, params, cancelled=cancelled)
                day = params["ann_date"]
                current_time = day_times[day]
                selected = super().query(profile, params | {"ann_date": "20261124"}, cancelled=cancelled)
                return [row | {"ann_date": day, "rec_time": f"2026-11-{day[-2:]} 09:00:00",
                               "url": f"{row['url']}?day={day}"} for row in selected]

        client = RecoveringClient(rows)
        with patch("financedb.ingest.time.monotonic", side_effect=lambda: current_time):
            result = run_ingestion(self.store, self.profiles, ["stock_basic", "anns_d"], client=client,
                                   mode="history", start="2026-11-23", end="2026-11-25", today="2026-11-26")

        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(scope_attempt_times, [0, 180])
        self.assertEqual(self.store.row_count("anns_d"), 13)
        self.assertIsNotNone(self.store.read_operation(SCOPE_OPERATION))
        parents = {item["params"]["ann_date"]: item["status"] for item in self.store.list_partitions("anns_d")
                   if "ts_code" not in item["params"]}
        self.assertEqual(parents, {"20261123": "partial", "20261124": "partial", "20261125": "complete"})

    def test_verified_pagination_covers_codes_missing_from_stock_master(self):
        rows = self.capped_fixture()
        for initial_offset in [0, 1]:
            with self.subTest(initial_offset=initial_offset):
                store = Store("sqlite://")
                self.addCleanup(store.close)
                client = CappedAnnouncementClient(rows, pagination=True, initial_offset=initial_offset)
                result = run_ingestion(store, self.profiles, ["stock_basic", "anns_d"], client=client,
                                       mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-26")
                self.assertEqual(result["status"], "complete", result)
                self.assertEqual(store.row_count("anns_d"), len(rows))
                checkpoint = store.list_partitions("anns_d")[0]
                self.assertEqual(checkpoint["message"], PAGINATION_VERIFIED_MESSAGE)
                self.assertFalse(any(params.get("ts_code") for dataset, params in client.calls if dataset == "anns_d"))
                client.calls.clear()
                resumed = run_ingestion(store, self.profiles, ["anns_d"], client=client,
                                        mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-26")
                self.assertEqual(resumed["status"], "complete")
                self.assertEqual(client.calls, [])

    def test_unverified_pages_preserve_sample_and_pull_observed_codes(self):
        rows = self.capped_fixture()
        client = CappedAnnouncementClient(rows, omit_code="830001.BJ")
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(self.store.row_count("anns_d"), 4)
        self.assertEqual({params.get("ts_code") for dataset, params in client.calls
                          if dataset == "anns_d" and params.get("ts_code")},
                         {"000001.SZ", "830001.BJ", "200992.SZ", "900939.SH", "201872.SZ"})
        parent = next(item for item in self.store.list_partitions("anns_d") if "ts_code" not in item["params"])
        self.assertEqual(parent["status"], "partial")
        self.assertIn("830001.BJ", parent["message"])
        self.assertIn("覆盖", parent["message"])
        self.assertEqual(parent["source_count"], 4)

    def test_observed_union_does_not_certify_unseen_outside_codes(self):
        rows = self.capped_fixture()
        client = CappedAnnouncementClient(rows)
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(self.store.row_count("anns_d"), 4)
        parent = next(item for item in self.store.list_partitions("anns_d") if "ts_code" not in item["params"])
        self.assertEqual(parent["status"], "partial")
        self.assertFalse(any(params.get("ts_code") == "430001.BJ" for _, params in client.calls))

        complete = CappedAnnouncementClient(rows, pagination=True)
        retry = self.ingest_capped(complete)
        self.assertEqual(retry["status"], "complete", retry)
        self.assertEqual(self.store.row_count("anns_d"), len(rows))
        self.assertTrue(all(item["status"] in {"complete", "empty"} for item in self.store.list_partitions("anns_d")))

    def test_capped_single_code_preserves_rows_and_remains_incomplete(self):
        rows = self.capped_fixture()
        rows[0] = rows[0] | {"ts_code": "830001.BJ"}
        result = self.ingest_capped(CappedAnnouncementClient(rows))
        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(self.store.row_count("anns_d"), 4)
        checkpoint = next(item for item in self.store.list_partitions("anns_d") if item["params"].get("ts_code") == "830001.BJ")
        self.assertEqual(checkpoint["status"], "partial")

    def test_advancing_pages_without_matching_boundaries_cannot_certify_coverage(self):
        rows = self.capped_fixture()
        client = CappedAnnouncementClient(rows, pagination=True, initial_offset=-1)
        result = self.ingest_capped(client)
        self.assertEqual(result["status"], "partial", result)
        self.assertEqual(self.store.row_count("anns_d"), 4)
        parent = next(item for item in self.store.list_partitions("anns_d") if "ts_code" not in item["params"])
        self.assertEqual(parent["status"], "partial")

    def test_market_data_keeps_strict_universe_validation(self):
        self.profiles["daily"] = self.profiles["daily"] | {"row_limit": 4}

        class MarketClient:
            def query(self, profile, params, *, cancelled=None):
                if profile["id"] == "stock_basic":
                    return [{"ts_code": "000001.SZ"}] if params["list_status"] == "L" else []
                return [{"ts_code": code, "trade_date": "20261124", "close": "10"}
                        for code in ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]]

        result = run_ingestion(self.store, self.profiles, ["stock_basic", "daily"], client=MarketClient(),
                               mode="history", start="2026-11-24", end="2026-11-24", today="2026-11-26")
        daily = result["datasets"][-1]
        self.assertEqual(daily["status"], "failed")
        self.assertIn("000002.SZ", daily["message"])
        self.assertEqual(self.store.row_count("daily"), 0)

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
