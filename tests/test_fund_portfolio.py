import unittest

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import Store


CODE = "001753.OF"
START, END = "20260301", "20260530"
QUARTER, OUTSIDE = "20260331", "20251231"


def holding(day, symbol="600845.SH"):
    return {"ts_code": CODE, "end_date": day, "ann_date": "20260425", "symbol": symbol, "mkv": "1000"}


class HoldingsClient:
    def __init__(self, wide, exact=None):
        self.wide = wide
        self.exact = exact or {}
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        self.calls.append(params.copy())
        return self.exact.get(params["period"], []) if "period" in params else self.wide


class FundHoldingsRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profile = load_profiles()["fund_portfolio"]

    def collect(self, client, *, profile=None, start=START, end=END):
        profile = profile or self.profile
        result = run_ingestion(self.store, {"fund_portfolio": profile}, ["fund_portfolio"],
                               client=client, mode="history", start=start, end=end, codes=[CODE], today="20261128")
        return result["datasets"][0]

    def test_healthy_window_keeps_one_request(self):
        client = HoldingsClient([holding(QUARTER)])
        self.assertEqual(self.collect(client)["status"], "complete")
        self.assertEqual(client.calls, [{"ts_code": CODE, "start_date": START, "end_date": END}])

    def test_recovers_same_checkpoint_and_fetches_every_quarter_in_window(self):
        second = "20260630"
        request = self.profile["request"] | {"window_days": 185}
        profile = self.profile | {"request": request}
        legacy = profile | {"request": {name: value for name, value in request.items() if name != "fallback_report_period"}}
        client = HoldingsClient([holding(OUTSIDE), holding(QUARTER)], {
            QUARTER: [holding(QUARTER), holding(QUARTER, "600596.SH")], second: [holding(second)],
        })
        failed = self.collect(client, profile=legacy, end=second)
        self.assertIn("分片日期以外", failed["message"])
        before, = self.store.list_partitions("fund_portfolio")
        self.assertEqual(before["status"], "failed")
        self.assertEqual(self.store.row_count("fund_portfolio"), 0)

        client.calls.clear()
        completed = self.collect(client, profile=profile, end=second)
        self.assertEqual((completed["status"], completed["rows"]), ("complete", 3))
        self.assertEqual(client.calls, [{"ts_code": CODE, "start_date": START, "end_date": second},
                                        {"ts_code": CODE, "period": QUARTER}, {"ts_code": CODE, "period": second}])
        after, = self.store.list_partitions("fund_portfolio")
        self.assertEqual((after["partition_id"], after["status"]), (before["partition_id"], "complete"))
        self.assertCountEqual(self.store.query_rows(profile)["rows"],
                              [holding(QUARTER), holding(QUARTER, "600596.SH"), holding(second)])

    def test_period_request_must_respect_date_and_keep_observed_holdings(self):
        for rows, message in (([holding(OUTSIDE)], "仍返回了分片日期以外"),
                              ([holding(QUARTER, "600596.SH")], "缺少原分片已返回的记录")):
            with self.subTest(rows=rows):
                client = HoldingsClient([holding(OUTSIDE), holding(QUARTER)], {QUARTER: rows})
                report = self.collect(client)
                self.assertEqual(report["status"], "failed")
                self.assertIn(message, report["message"])
                self.assertEqual(self.store.row_count("fund_portfolio"), 0)

    def test_empty_quarter_needs_positive_control(self):
        outside = holding(OUTSIDE)
        rejected = self.collect(HoldingsClient([outside]))
        self.assertEqual(rejected["status"], "failed")
        self.assertIn("缺少原分片已返回的记录", rejected["message"])

        client = HoldingsClient([outside], {OUTSIDE: [outside]})
        accepted = self.collect(client)
        self.assertEqual((accepted["status"], accepted["rows"]), ("complete", 0))
        self.assertEqual(client.calls[1:], [{"ts_code": CODE, "period": QUARTER}, {"ts_code": CODE, "period": OUTSIDE}])
        checkpoint, = self.store.list_partitions("fund_portfolio")
        self.assertEqual(checkpoint["status"], "empty")

    def test_nonquarter_source_dates_do_not_certify_an_empty_window(self):
        client = HoldingsClient([holding("20260330")])
        report = self.collect(client, start="20260401", end="20260530")
        self.assertEqual(report["status"], "failed")
        self.assertIn("非季度末的报告日期", report["message"])
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(self.store.row_count("fund_portfolio"), 0)
