import json
import os
import unittest
from unittest.mock import patch

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import IngestCancelled, RateLimiter, TushareClient, run_ingestion
from financedb.storage import Store


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def wait(self, seconds, cancelled=None):
        self.now += seconds


class IngestionMetricsTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        for target, replacement in [("financedb.ingest.time.monotonic", self.clock.monotonic),
                                    ("financedb.ingest._wait", self.clock.wait)]:
            mocked = patch(target, replacement)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.profiles = load_profiles()

    def test_high_tier_budget_honors_account_and_special_endpoint_limits(self):
        environment = {"FINANCEDB_TUSHARE_REQUESTS_PER_MINUTE": "500",
                       "FINANCEDB_TUSHARE_ENDPOINT_REQUESTS_PER_MINUTE": "500"}
        with patch.dict(os.environ, environment):
            limiter = RateLimiter.from_environment()
        for _ in range(500):
            limiter.acquire(self.profiles["anns_d"])
        self.assertEqual(self.clock.now, 0)
        limiter.acquire(self.profiles["anns_d"])
        self.assertEqual(self.clock.now, 60)

        limiter = RateLimiter(500, endpoint_per_minute=500)
        for _ in range(30):
            limiter.acquire(self.profiles["stk_factor_pro"])
        before = self.clock.now
        limiter.acquire(self.profiles["stk_factor_pro"])
        self.assertEqual(self.clock.now - before, 60)

    def test_lower_configured_endpoint_budget_and_daily_cap_are_preserved(self):
        limiter = RateLimiter(500, per_day=2, endpoint_per_minute=1)
        for api_name in ["one", "two"]:
            limiter.acquire({"api_name": api_name})
        limiter.acquire({"api_name": "three"})
        self.assertEqual(self.clock.now, 86400)
        limiter.acquire({"api_name": "three", "quota": {"per_minute": 500}})
        self.assertEqual(self.clock.now, 86460)

    def test_invalid_environment_budget_fails_clearly(self):
        for value in ["0", "-1", "not-an-integer"]:
            with self.subTest(value=value), patch.dict(os.environ, {"FINANCEDB_TUSHARE_REQUESTS_PER_MINUTE": value}):
                with self.assertRaisesRegex(ValueError, "FINANCEDB_TUSHARE_REQUESTS_PER_MINUTE"):
                    RateLimiter.from_environment()

    def test_cancellation_during_rate_wait_does_not_consume_another_request(self):
        limiter = RateLimiter(1)
        profile = self.profiles["anns_d"]
        limiter.acquire(profile)
        with patch("financedb.ingest._wait", side_effect=IngestCancelled("stopped")):
            with self.assertRaises(IngestCancelled):
                limiter.acquire(profile)
        self.assertEqual(len(limiter._calls["account", 60]), 1)

    def test_request_retry_http_and_rate_wait_are_measured_separately(self):
        responses = [503, 200]

        def respond(request):
            self.clock.now += 0.25
            return httpx.Response(responses.pop(0), json={"code": 0, "data": {"fields": [], "items": []}})

        client = TushareClient("fixture", limiter=RateLimiter(1), transport=httpx.MockTransport(respond))
        self.addCleanup(client.close)
        with patch("financedb.ingest.random.uniform", return_value=0):
            self.assertEqual(client.query({"api_name": "fixture", "fields": []}, {}), [])
        self.assertEqual(client.metrics["requests"], 2)
        self.assertEqual(client.metrics["retries"], 1)
        self.assertEqual(client.metrics["http_seconds"], 0.5)
        self.assertEqual(client.metrics["retry_wait_seconds"], 1)
        self.assertEqual(client.metrics["rate_wait_seconds"], 58.75)

    def test_dataset_metrics_are_isolated_and_resume_sends_no_requests(self):
        store = Store("sqlite://")
        self.addCleanup(store.close)
        profiles = {key: self.profiles[key] for key in ["daily", "adj_factor"]}

        def respond(request):
            self.clock.now += 0.2
            payload = json.loads(request.content)
            fields = payload["fields"].split(",")
            row = {"ts_code": "000001.SZ", "trade_date": "20261124", "close": "10"}
            items = [[row.get(name) for name in fields]] if payload["api_name"] == "daily" else []
            return httpx.Response(200, json={"code": 0, "data": {"fields": fields, "items": items}})

        client = TushareClient("fixture", limiter=RateLimiter(), transport=httpx.MockTransport(respond))
        self.addCleanup(client.close)
        events = []
        options = dict(client=client, mode="history", start="2026-11-24", end="2026-11-24", progress=events.append)
        first = run_ingestion(store, profiles, profiles, **options)
        self.assertEqual(first["status"], "complete", first)
        daily, factors = first["datasets"]
        self.assertEqual(daily["metrics"]["requests"], 1)
        self.assertEqual(factors["metrics"]["requests"], 1)
        self.assertEqual(daily["metrics"]["empty_partitions"], 0)
        self.assertEqual(factors["metrics"]["empty_partitions"], 1)
        self.assertEqual(factors["metrics"]["response_rows"], 0)
        second = run_ingestion(store, profiles, profiles, **options)
        self.assertTrue(all(item["metrics"]["requests"] == 0 and item["skipped"] == 1 for item in second["datasets"]))
        self.assertEqual(client.metrics["requests"], 2)
        self.assertTrue(all("metrics" in item for item in events))
