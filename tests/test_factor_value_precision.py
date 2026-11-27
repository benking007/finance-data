import json
from decimal import Decimal
import unittest

import httpx

from financedb.catalog import load_profiles
from financedb.ingest import TushareClient, run_ingestion
from financedb.storage import Store


class FactorValuePrecisionTest(unittest.TestCase):
    def test_wire_precision_and_missing_values_survive_resume_without_rewriting_completed_dates(self):
        store = Store("sqlite://")
        self.addCleanup(store.close)
        profile = load_profiles()["factor_value"]
        profile = profile | {"request": profile["request"] | {"window_days": 1}}
        tiny = "-1.23456789012345678901234567891e-42"
        large = "1.23e+105"
        precise = "0.0002331183123459874"
        first_day, failed_day = "20261124", "20261125"
        calls = []
        corrected = False

        def respond(request):
            body = json.loads(request.content)
            day = body["params"]["start_date"]
            code = body["params"]["ts_code"]
            calls.append(day)
            if day == first_day:
                values = {"normal": "12.3400"}
            elif not corrected:
                values = {"bad": '"bad value"'}
            else:
                values = {"tiny": tiny, "large": large, "precise": precise, "missing": "null", "undefined": "NaN",
                          "undefined_text": '"-Infinity"'}
            items = ",".join(f"[{json.dumps(name)},{json.dumps(code)},{json.dumps(day)},{value}]"
                             for name, value in values.items())
            fields = json.dumps(profile["fields"])
            return httpx.Response(200, content=f'{{"code":0,"data":{{"fields":{fields},"items":[{items}]}}}}')

        def collect(client):
            return run_ingestion(store, {profile["id"]: profile}, [profile["id"]], client=client,
                                 mode="history", start=first_day, end=failed_day, today="2026-11-27",
                                 codes=["600000.SH"])

        with TushareClient("fixture", workers=1, transport=httpx.MockTransport(respond)) as client:
            initial = collect(client)
            self.assertEqual(initial["status"], "partial")
            self.assertIn("factor_value.factor_value", initial["datasets"][0]["message"])
            self.assertEqual(calls, [first_day, failed_day])
            self.assertEqual(store.row_count("factor_value"), 1)

            corrected = True
            resumed = collect(client)
            self.assertEqual(resumed["status"], "complete")
            self.assertEqual(resumed["datasets"][0]["skipped"], 1)
            self.assertEqual(calls, [first_day, failed_day, failed_day])
            self.assertEqual(collect(client)["datasets"][0]["skipped"], 2)
            self.assertEqual(calls, [first_day, failed_day, failed_day])

        rows = {row["factor_name"]: row.get("factor_value") for row in store.query_rows(profile, limit=20)["rows"]}
        self.assertEqual(set(rows), {"normal", "tiny", "large", "precise", "missing", "undefined", "undefined_text"})
        self.assertEqual(rows["normal"], "12.34")
        self.assertEqual(rows["precise"], precise)
        self.assertEqual(Decimal(rows["tiny"]), Decimal(tiny))
        self.assertEqual(Decimal(rows["large"]), Decimal(large))
        self.assertTrue(all(rows[name] is None for name in ("missing", "undefined", "undefined_text")))
