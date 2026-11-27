import json
from threading import Barrier, Event, Lock, get_ident
import unittest
from unittest.mock import patch

import httpx
from sqlalchemy import event

from financedb.catalog import load_profiles
from financedb.ingest import IngestCancelled, IngestError, Partition, TushareClient, run_ingestion
from financedb.storage import Store


DAYS = ("20261123", "20261124", "20261125")


class ParallelIngestionTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        profile = load_profiles()["daily"]
        self.profile = profile | {"fields": ["ts_code", "trade_date"]}

    def run_daily(self, client, **kwargs):
        return run_ingestion(self.store, {"daily": self.profile}, ["daily"], client=client,
                             mode="history", start=DAYS[0], end=DAYS[-1], today="2026-11-26", **kwargs)

    def test_http_requests_overlap_and_store_commits_on_the_caller_thread(self):
        barrier = Barrier(3, timeout=4)
        request_threads = set()
        database_threads = set()
        lock = Lock()

        def respond(request):
            with lock:
                request_threads.add(get_ident())
            barrier.wait()
            day = json.loads(request.content)["params"]["trade_date"]
            return httpx.Response(200, json={"code": 0, "data": {
                "fields": ["ts_code", "trade_date"], "items": [["000001.SZ", day]],
            }})

        def record_thread(*_):
            database_threads.add(get_ident())

        event.listen(self.store.engine, "before_cursor_execute", record_thread)
        with TushareClient("fixture", workers=3, transport=httpx.MockTransport(respond)) as client:
            report = self.run_daily(client)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["datasets"][0]["metrics"]["requests"], 3)
        self.assertEqual(len(request_threads), 3)
        self.assertEqual(database_threads, {get_ident()})
        self.assertEqual(self.store.row_count("daily"), 3)

    def test_cancelled_workers_cannot_commit_and_incomplete_dates_can_resume(self):
        stop = Event()

        class CancelClient:
            def query(self, profile, params, *, cancelled):
                day = params["trade_date"]
                if day != DAYS[0]:
                    if not stop.wait(4):
                        raise AssertionError("pending request was not cancelled")
                    if cancelled():
                        raise IngestCancelled("fixture stopped")
                return [{"ts_code": "000001.SZ", "trade_date": day}]

        def stop_after_first_commit(progress):
            if progress.get("partition_status") == "complete":
                stop.set()

        report = self.run_daily(CancelClient(), workers=3, cancelled=stop.is_set, progress=stop_after_first_commit)
        self.assertEqual(report["status"], "cancelled")
        self.assertEqual(self.store.row_count("daily"), 1)
        self.assertEqual(len(self.store.list_partitions("daily")), 1)

        requests = []

        class ResumeClient:
            def query(self, profile, params, *, cancelled):
                day = params["trade_date"]
                requests.append(day)
                return [{"ts_code": "000001.SZ", "trade_date": day}]

        report = self.run_daily(ResumeClient(), workers=3)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["datasets"][0]["skipped"], 1)
        self.assertCountEqual(requests, DAYS[1:])
        self.assertEqual(self.store.row_count("daily"), 3)

    def test_planning_failure_preserves_valid_partitions_already_submitted(self):
        def plan(*args, **kwargs):
            yield Partition({"trade_date": DAYS[0]})
            raise IngestError("第二个交易日缺少标的名录")

        class Client:
            def query(self, profile, params, *, cancelled):
                return [{"ts_code": "000001.SZ", "trade_date": params["trade_date"]}]

        with patch("financedb.ingest.plan_partitions", plan):
            report = self.run_daily(Client(), workers=3)
        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["datasets"][0]["completed"], 1)
        self.assertIn("缺少标的名录", report["datasets"][0]["message"])
        self.assertEqual(self.store.row_count("daily"), 1)
