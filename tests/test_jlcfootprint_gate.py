"""The M0 gate as a test: the committed corner-case board must reproduce JLC's observed rotations."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BOARD = ROOT / "scripts" / "corner_case" / "corner_case.kicad_pcb"
TRUTH = ROOT / "tests" / "fixtures" / "jlcfootprint" / "truth.csv"
BOARD_M3 = ROOT / "scripts" / "corner_case_m3" / "corner_case_m3.kicad_pcb"
TRUTH_M3 = ROOT / "tests" / "fixtures" / "jlcfootprint" / "truth_m3.csv"
FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"
PRO_FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda_pro"


def load_validator():
    """Import scripts/validate_board.py as a module."""
    spec = importlib.util.spec_from_file_location(
        "validate_board", ROOT / "scripts" / "validate_board.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_corner_case_board_matches_jlc():
    """Every observed rotation is reproduced and the wrong picks derive nothing."""
    if not TRUTH.exists():
        pytest.skip("truth.csv not recorded yet (plan Task 12)")
    validator = load_validator()
    rows = validator.evaluate(BOARD, FIXTURES)
    truth = validator.load_truth(TRUTH)
    assert len(truth) >= 40
    assert validator.compare(rows, truth) == []
    by_reference = {row["reference"]: row["verdict"] for row in rows}
    assert by_reference["U7"].fit == "count"
    assert by_reference["R4"].fit == "fits_tight"
    assert by_reference["D9"].polarity_light == "yellow"


def test_corner_case_board_matches_jlc_from_the_pro_fixtures():
    """Read the way the plugin fetches live (spec 15), the board reproduces the same truth."""
    if not TRUTH.exists():
        pytest.skip("truth.csv not recorded yet (plan Task 12)")
    validator = load_validator()
    rows = validator.evaluate(BOARD, FIXTURES, pro_fixtures=PRO_FIXTURES)
    truth = validator.load_truth(TRUTH)
    assert validator.compare(rows, truth) == []
    by_reference = {row["reference"]: row["verdict"] for row in rows}
    assert all(row["verdict"] is not None for row in rows), "every part is recorded"
    assert by_reference["U7"].fit == "count"
    assert by_reference["R4"].fit == "fits_tight"
    assert by_reference["D9"].polarity_light == "yellow"
    classic = {
        row["reference"]: row["verdict"] for row in validator.evaluate(BOARD, FIXTURES)
    }
    assert {
        ref: (v.status if v.status != "yellow" else "green", v.rotation)
        for ref, v in by_reference.items()
    } == {
        ref: (v.status if v.status != "yellow" else "green", v.rotation)
        for ref, v in classic.items()
    }


def test_m3_board_matches_jlc_from_the_pro_fixtures():
    """The M3 board (spec 16.6) reproduces its truth: function pairing, the drawings' marks, the body caveat."""
    validator = load_validator()
    rows = validator.evaluate(BOARD_M3, FIXTURES, pro_fixtures=PRO_FIXTURES)
    truth = validator.load_truth(TRUTH_M3)
    assert len(truth) == 15  # C8 (C308913) waits for JLC's preview
    assert validator.compare(rows, truth) == []
    by_reference = {row["reference"]: row["verdict"] for row in rows}
    assert all(verdict is not None for verdict in by_reference.values())
    assert (
        by_reference["U1"].rotation == 270
        and "paired by pin function" in by_reference["U1"].note_text
    )
    assert "by elimination" in by_reference["U2"].note_text
    assert "pad counts differ" not in by_reference["U4"].note_text
    assert by_reference["U5"].fit == "numbering"
    assert {by_reference[ref].polarity_source for ref in ("C1", "C2", "C3", "C4")} == {
        "drawing"
    }
    assert {by_reference[ref].rotation for ref in ("C5", "C6", "C7")} == {180}
    assert by_reference["C8"].rotation == 0
    assert by_reference["C9"].body_excess_mm == pytest.approx(0.9, abs=0.01)
    assert by_reference["C10"].body_excess_mm is None
    assert by_reference["C11"].rotation == 0
