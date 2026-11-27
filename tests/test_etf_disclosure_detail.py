from decimal import Decimal
import unittest

from financedb.catalog import load_profiles
from financedb.ingest import run_ingestion
from financedb.storage import Store


FIRST_DAY = "20261119"
SECOND_DAY = "20261120"


def disclosure(code, trade_date, ann_date, nav):
    return {"ts_code": code, "trade_date": trade_date, "ann_date": ann_date, "nav": Decimal(nav)}


class DisclosureClient:
    def __init__(self, by_day, *, codes=(), incomplete_code=None):
        self.by_day = by_day
        self.codes = codes
        self.incomplete_code = incomplete_code
        self.calls = []

    def query(self, profile, params, *, cancelled=None):
        if profile["id"] == "etf_basic":
            return [{"ts_code": code} for code in self.codes]
        self.calls.append(params)
        rows = self.by_day[params["trade_date"]]
        if code := params.get("ts_code"):
            rows = [row for row in rows if row["ts_code"] == code]
            if code == self.incomplete_code:
                rows = rows[:1]
        return rows[:profile["row_limit"]]


class ETFDisclosureDetailTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.profiles = load_profiles()

    def collect(self, profile, client, *, start=FIRST_DAY, use_directory=False):
        dataset = profile["id"]
        profiles = {dataset: profile}
        targets = [dataset]
        if use_directory:
            profiles["etf_basic"] = self.profiles["etf_basic"] | {"request": {"strategy": "snapshot"}}
            targets.append("etf_basic")
        result = run_ingestion(self.store, profiles, targets, client=client, mode="history",
                               start=start, end=SECOND_DAY, today="20261127")
        return next(report for report in result["datasets"] if report["dataset_id"] == dataset)

    def test_both_markets_preserve_distinct_disclosures_and_resume_failed_days(self):
        for dataset, code in (("etf_bs_sh_detail", "510130.SH"), ("etf_bs_sz_detail", "159977.SZ")):
            with self.subTest(dataset=dataset):
                profile = self.profiles[dataset]
                legacy = profile | {"key": ["ts_code", "trade_date"]}
                first = disclosure(code, SECOND_DAY, "20261120", "1.1000")
                same = disclosure(code, SECOND_DAY, "20261120", "1.10")
                later_date = disclosure(code, SECOND_DAY, "20261123", "1.10")
                same_date_variant = disclosure(code, SECOND_DAY, "20261120", "1.11")
                by_day = {FIRST_DAY: [disclosure(code, FIRST_DAY, "20261119", "1.09")],
                          SECOND_DAY: [first, same, later_date, same_date_variant]}
                report = self.collect(legacy, DisclosureClient(by_day))
                self.assertEqual(report["completed"], 1)
                self.assertEqual(report["failed"], 1)
                self.assertIn("业务键相同", report["message"])
                checkpoint_ids = {row["partition_id"] for row in self.store.list_partitions(dataset)}

                client = DisclosureClient(by_day)
                report = self.collect(profile, client)
                self.assertEqual((report["status"], report["completed"], report["skipped"]), ("complete", 1, 1))
                self.assertEqual(client.calls, [{"trade_date": SECOND_DAY}])
                self.assertEqual({row["partition_id"] for row in self.store.list_partitions(dataset)}, checkpoint_ids)
                rows = self.store.query_rows(profile, start=SECOND_DAY, end=SECOND_DAY)["rows"]
                self.assertCountEqual([(row["ann_date"], row["nav"]) for row in rows],
                                      [("20261120", "1.1"), ("20261123", "1.1"), ("20261120", "1.11")])

    def test_cap_split_requires_every_distinct_source_row(self):
        profile = self.profiles["etf_bs_sh_detail"] | {"row_limit": 3}
        code_a, code_b = "510130.SH", "510100.SH"
        rows = [disclosure(code_a, SECOND_DAY, "20261120", "1.1"),
                disclosure(code_a, SECOND_DAY, "20261120", "1.2"),
                disclosure(code_b, SECOND_DAY, "20261120", "1.3")]
        partial = DisclosureClient({SECOND_DAY: rows}, codes=(code_a, code_b), incomplete_code=code_a)
        report = self.collect(profile, partial, start=SECOND_DAY, use_directory=True)
        self.assertEqual(report["status"], "partial")
        self.assertIn("缺少上限响应中已出现的记录", report["message"])
        self.assertEqual(self.store.row_count(profile["id"]), 1)

        intact = DisclosureClient({SECOND_DAY: rows}, codes=(code_a, code_b))
        report = self.collect(profile, intact, start=SECOND_DAY, use_directory=True)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(self.store.row_count(profile["id"]), 3)

    def test_source_still_requires_trade_date_and_code(self):
        profile = self.profiles["etf_bs_sh_detail"]
        row = disclosure("", SECOND_DAY, "20261120", "1.11")
        client = DisclosureClient({SECOND_DAY: [row]})
        report = self.collect(profile, client, start=SECOND_DAY)
        self.assertEqual(report["status"], "failed")
        self.assertIn("缺少有效字段 ts_code", report["message"])
        self.assertEqual(self.store.row_count(profile["id"]), 0)
