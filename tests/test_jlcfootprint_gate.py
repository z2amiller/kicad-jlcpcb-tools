"""The M0 gate as a test: the committed corner-case board must reproduce JLC's observed rotations."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BOARD = ROOT / "scripts" / "corner_case" / "corner_case.kicad_pcb"
TRUTH = ROOT / "tests" / "fixtures" / "jlcfootprint" / "truth.csv"
FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"


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
    assert by_reference["R4"].fit == "pitch"
    assert by_reference["D9"].polarity_light == "yellow"
