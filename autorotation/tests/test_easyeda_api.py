"""Tests for ``autorotation.easyeda_api``.

Uses the offline fixture at ``fixtures/C2132.json`` (a real EasyEDA response
for an SOT-23-3 transistor) so the suite never touches the network. Set the
``EASYEDA_LIVE_TEST`` environment variable to additionally exercise the real
HTTP endpoint.
"""

from __future__ import annotations

import json
import math
import os
import unittest
from pathlib import Path

from autorotation.easyeda_api import (
    FootprintResult,
    fetch_easyeda_pads,
    parse_easyeda_response,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"
C2132_FIXTURE = FIXTURE_DIR / "C2132.json"


def _load_fixture(name: str):
    with open(FIXTURE_DIR / name, "r", encoding="utf-8") as f:
        return json.load(f)


class ParseEasyedaResponseTests(unittest.TestCase):
    def test_parses_c2132_sot23_fixture(self):
        data = _load_fixture("C2132.json")
        result = parse_easyeda_response(data, lcsc_code="C2132")

        self.assertTrue(result.success, msg=f"parse failed: {result.error!r}")
        self.assertIsNone(result.error)
        self.assertEqual(result.lcsc_code, "C2132")
        # c_para.package for C2132 is "SOT-23-3_L2.9-W1.6-P1.90-LS2.8-BR".
        self.assertIn("SOT-23", result.package)
        self.assertTrue(result.title)

        self.assertEqual(len(result.pads), 3)
        numbers = [p["number"] for p in result.pads]
        self.assertEqual(sorted(numbers), ["1", "2", "3"])

        by_num = {p["number"]: p for p in result.pads}
        # Expected footprint-local coords after subtracting head origin
        # (4000.0005, 3000) and scaling by 0.254 mm/unit.
        self.assertAlmostEqual(by_num["1"]["x"], 1.235, delta=0.01)
        self.assertAlmostEqual(by_num["1"]["y"], 0.950, delta=0.01)
        self.assertAlmostEqual(by_num["2"]["x"], 1.235, delta=0.01)
        self.assertAlmostEqual(by_num["2"]["y"], -0.950, delta=0.01)
        self.assertAlmostEqual(by_num["3"]["x"], -1.235, delta=0.01)
        self.assertAlmostEqual(by_num["3"]["y"], 0.0, delta=0.01)

        # Same-side pitch between pads 1 and 2 should be ~1.9 mm (SOT-23).
        pitch = abs(by_num["1"]["y"] - by_num["2"]["y"])
        self.assertAlmostEqual(pitch, 1.9, delta=0.02)

        # Lead span between pads 1 and 3 should be ~2.47 mm.
        span = abs(by_num["1"]["x"] - by_num["3"]["x"])
        self.assertAlmostEqual(span, 2.47, delta=0.02)

        # Width and height fields must be present on all pads.
        # Raw PAD fields 4/5 for C2132 are 4.2126 and 2.3622, giving
        # 4.2126 * 0.254 ≈ 1.070 mm and 2.3622 * 0.254 ≈ 0.600 mm.
        for pad in result.pads:
            self.assertIn("width", pad)
            self.assertIn("height", pad)
        self.assertAlmostEqual(by_num["1"]["width"], 1.070, delta=0.005)
        self.assertAlmostEqual(by_num["1"]["height"], 0.600, delta=0.005)
        self.assertAlmostEqual(by_num["2"]["width"], 1.070, delta=0.005)
        self.assertAlmostEqual(by_num["2"]["height"], 0.600, delta=0.005)
        self.assertAlmostEqual(by_num["3"]["width"], 1.070, delta=0.005)
        self.assertAlmostEqual(by_num["3"]["height"], 0.600, delta=0.005)

    def test_empty_dict_returns_failure(self):
        result = parse_easyeda_response({})
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)
        self.assertEqual(result.pads, [])

    def test_success_false_returns_failure(self):
        result = parse_easyeda_response({"success": False, "message": "not found"})
        self.assertFalse(result.success)
        self.assertIn("not found", (result.error or ""))

    def test_missing_result_returns_failure(self):
        result = parse_easyeda_response({"success": True})
        self.assertFalse(result.success)
        self.assertIn("result", (result.error or ""))

    def test_missing_package_detail_returns_failure(self):
        result = parse_easyeda_response({"success": True, "result": {}})
        self.assertFalse(result.success)
        self.assertIn("packageDetail", (result.error or ""))

    def test_missing_data_str_returns_failure(self):
        payload = {"success": True, "result": {"packageDetail": {}}}
        result = parse_easyeda_response(payload)
        self.assertFalse(result.success)
        self.assertIn("dataStr", (result.error or ""))

    def test_missing_head_returns_failure(self):
        payload = {
            "success": True,
            "result": {"packageDetail": {"dataStr": {"shape": []}}},
        }
        result = parse_easyeda_response(payload)
        self.assertFalse(result.success)
        self.assertIn("head", (result.error or ""))

    def test_missing_shape_returns_failure(self):
        payload = {
            "success": True,
            "result": {
                "packageDetail": {
                    "dataStr": {"head": {"x": 0, "y": 0, "c_para": {"package": "X"}}}
                }
            },
        }
        result = parse_easyeda_response(payload)
        self.assertFalse(result.success)
        self.assertIn("shape", (result.error or ""))

    def test_no_pad_entries_returns_failure(self):
        payload = {
            "success": True,
            "result": {
                "packageDetail": {
                    "title": "x",
                    "dataStr": {
                        "head": {"x": 0, "y": 0, "c_para": {"package": "X"}},
                        "shape": ["TRACK~1~2~3", "CIRCLE~a~b"],
                    },
                }
            },
        }
        result = parse_easyeda_response(payload)
        self.assertFalse(result.success)
        self.assertIn("PAD", (result.error or ""))

    def test_non_dict_input_returns_failure(self):
        for bad in (None, [], "string", 42):
            result = parse_easyeda_response(bad)
            self.assertFalse(result.success)
            self.assertIsNotNone(result.error)

    def test_malformed_pad_entries_are_skipped(self):
        # One valid PAD and one truncated PAD; parser should return the valid one.
        payload = {
            "success": True,
            "result": {
                "packageDetail": {
                    "title": "t",
                    "dataStr": {
                        "head": {"x": 0, "y": 0, "c_para": {"package": "P"}},
                        "shape": [
                            "PAD~RECT~10~20~1~1~TOP~~A",
                            "PAD~too~short",
                        ],
                    },
                }
            },
        }
        result = parse_easyeda_response(payload)
        self.assertTrue(result.success, msg=result.error)
        self.assertEqual(len(result.pads), 1)
        self.assertEqual(result.pads[0]["number"], "A")
        self.assertTrue(math.isclose(result.pads[0]["x"], 10 * 0.254, rel_tol=1e-9))
        self.assertTrue(math.isclose(result.pads[0]["y"], 20 * 0.254, rel_tol=1e-9))
        # Width (field 4 = 1) and height (field 5 = 1) should be present.
        self.assertTrue(math.isclose(result.pads[0]["width"], 1 * 0.254, rel_tol=1e-9))
        self.assertTrue(math.isclose(result.pads[0]["height"], 1 * 0.254, rel_tol=1e-9))


class FetchEasyedaPadsTests(unittest.TestCase):
    def test_empty_lcsc_code_returns_failure(self):
        result = fetch_easyeda_pads("")
        self.assertIsInstance(result, FootprintResult)
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)

    @unittest.skipUnless(
        os.environ.get("EASYEDA_LIVE_TEST"),
        "set EASYEDA_LIVE_TEST=1 to exercise the live API",
    )
    def test_live_fetch_c2132(self):  # pragma: no cover - network
        result = fetch_easyeda_pads("C2132", timeout=15.0)
        self.assertTrue(result.success, msg=result.error)
        self.assertEqual(len(result.pads), 3)


if __name__ == "__main__":
    unittest.main()
