from decimal import Decimal
import unittest

from sqlalchemy import select

from financedb.business_tables import BusinessFieldError, business_tables
from financedb.catalog import load_profiles
from financedb.storage import Store, revisions


class AuditStorageTest(unittest.TestCase):
    def setUp(self):
        self.store = Store("sqlite://")
        self.addCleanup(self.store.close)
        self.store.initialize()
        self.profiles = load_profiles()

    def commit(self, dataset, partition, rows):
        self.store.commit_partition(self.profiles[dataset], partition, {}, rows)

    def test_compare_deduplicates_effective_rows_and_leaves_local_data_unchanged(self):
        profile = self.profiles["daily"]
        day = {"ts_code": "000001.SZ", "trade_date": "20261123", "close": "10.00"}
        changed = {**day, "trade_date": "20261124", "close": "11"}
        added = {**day, "trade_date": "20261125", "close": "13"}
        self.commit("daily", "first", [day, {**changed, "close": "12.00"}])

        comparison = self.store.compare_business_rows(profile, [{**day, "close": Decimal("10.000")},
                                                                 changed, added, added])
        self.assertEqual(comparison, {"missing": [added], "changed": [changed]})
        self.assertEqual(self.store.row_count("daily"), 2)
        self.assertEqual({row["close"] for row in self.store.query_rows(profile)["rows"]}, {"10", "12"})
        self.assertEqual(len(self.store.list_partitions("daily")), 1)

        self.assertEqual(self.store.apply_business_rows(profile, comparison["missing"] + comparison["changed"]),
                         {"inserted": 1, "updated": 1})
        self.assertEqual(self.store.compare_business_rows(profile, [day, changed, added]), {"missing": [], "changed": []})
        self.assertEqual(self.store.apply_business_rows(profile, [day, changed, added]), {"inserted": 0, "updated": 0})
        self.assertEqual(len(self.store.list_partitions("daily")), 1)

    def test_sparse_optional_fields_keep_existing_values(self):
        profile = self.profiles["etf_auction"]
        row = {"ts_code": "180607.SZ", "trade_date": "20261124", "price": "5.10", "float_share": "42"}
        self.commit("etf_auction", "first", [row])
        partial = {key: value for key, value in row.items() if key != "float_share"}
        self.assertEqual(self.store.compare_business_rows(profile, [partial]), {"missing": [], "changed": []})

        updated = {**partial, "price": "5.11"}
        diff = self.store.compare_business_rows(profile, [updated])
        self.assertEqual(diff, {"missing": [], "changed": [updated]})
        self.store.apply_business_rows(profile, diff["changed"])
        saved, = self.store.query_rows(profile)["rows"]
        self.assertEqual((saved["price"], saved["float_share"]), ("5.11", "42"))
        self.assertEqual(self.store.compare_business_rows(profile, [updated | {"float_share": None}])["changed"],
                         [updated | {"float_share": None}])

    def test_keyless_records_add_distinct_rows_and_do_not_replace_older_variants(self):
        profile = self.profiles["block_trade"]
        row = {"ts_code": "000001.SZ", "trade_date": "20261124", "price": "10.00", "buyer": "杭州"}
        different = row | {"buyer": "宁波"}
        self.commit("block_trade", "first", [row])
        self.assertEqual(self.store.compare_business_rows(profile, [row, different, different]),
                         {"missing": [different], "changed": []})

    def test_stale_source_revision_cannot_replace_local_version(self):
        profile = self.profiles["income"]
        row = {"ts_code": "000001.SZ", "end_date": "20260930", "report_type": "1", "comp_type": "1",
               "f_ann_date": "20261031", "revenue": "248.13"}
        old = row | {"f_ann_date": "20261030", "revenue": "220"}
        new = row | {"f_ann_date": "20261101", "revenue": "250"}
        self.commit("income", "first", [row])
        self.assertEqual(self.store.compare_business_rows(profile, [old]), {"missing": [], "changed": []})
        self.assertEqual(self.store.compare_business_rows(profile, [old, new]), {"missing": [], "changed": [new]})
        self.assertEqual(self.store.apply_business_rows(profile, [new]), {"inserted": 0, "updated": 1})
        self.assertEqual(self.store.query_rows(profile)["rows"], [new])
        with self.store.engine.connect() as conn:
            self.assertEqual(len(conn.execute(select(revisions).where(revisions.c.dataset_id == "income")).all()), 2)
        self.assertEqual(len(self.store.list_partitions("income")), 1)

    def test_ambiguous_local_business_key_cannot_be_repaired_implicitly(self):
        profile = self.profiles["daily"]
        table = business_tables["daily"]
        row = {"ts_code": "000001.SZ", "trade_date": "20261124", "close": "10"}
        with self.store.engine.begin() as conn:
            conn.execute(table.insert(), [row, row | {"close": "11"}])
        with self.assertRaisesRegex(BusinessFieldError, "多条记录共用业务键"):
            self.store.compare_business_rows(profile, [row | {"close": "12"}])
        with self.store.engine.connect() as conn:
            self.assertEqual(set(conn.scalars(select(table.c.close))), {"10", "11"})

    def test_repair_rolls_back_earlier_business_changes_after_ambiguous_key(self):
        profile = self.profiles["daily"]
        table = business_tables["daily"]
        day = {"ts_code": "000001.SZ", "trade_date": "20261123", "close": "10"}
        duplicate = day | {"trade_date": "20261124"}
        self.commit("daily", "first", [day, duplicate])
        with self.store.engine.begin() as conn:
            conn.execute(table.insert(), duplicate | {"close": "11"})

        with self.assertRaisesRegex(BusinessFieldError, "多条记录共用业务键"):
            self.store.apply_business_rows(profile, [day | {"close": "12"}, duplicate | {"close": "13"}])
        with self.store.engine.connect() as conn:
            self.assertEqual(conn.scalar(select(table.c.close).where(table.c.trade_date == "20261123")), "10")
        self.assertEqual(len(self.store.list_partitions("daily")), 1)
