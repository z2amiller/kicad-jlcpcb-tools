"""Tests for scripts/validate_board.py: the gate's evaluate and compare plumbing."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"


def load_script():
    """Import the validator script as a module without running its CLI."""
    spec = importlib.util.spec_from_file_location(
        "validate_board", ROOT / "scripts" / "validate_board.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def footprint_text(reference, layer, placed, mirror=False):
    """Return one SOT-23 footprint (C2132) as KiCad stores it: absolute pad angles, mirrored Y on the back."""
    sign = -1 if mirror else 1
    at = f"(at 10 10 {placed})" if placed else "(at 10 10)"
    rot = f" {placed}" if placed else ""
    pads = [
        ("1", -0.9375, -0.95 * sign),
        ("2", -0.9375, 0.95 * sign),
        ("3", 0.9375, 0.0),
    ]
    pad_lines = "".join(
        f'    (pad "{number}" smd roundrect (at {x} {y}{rot}) (size 1.475 0.6) (layers "{layer}"))\n'
        for number, x, y in pads
    )
    return (
        f'  (footprint "Package_TO_SOT_SMD:SOT-23" (layer "{layer}") {at}\n'
        f'    (property "Reference" "{reference}" (at 0 0) (layer "F.SilkS"))\n'
        f'    (property "LCSC" "C2132" (at 0 0) (layer "F.Fab"))\n'
        f"{pad_lines}  )\n"
    )


def board_file(tmp_path, body):
    """Write a minimal board around ``body`` and return its path."""
    path = tmp_path / "board.kicad_pcb"
    path.write_text(
        f'(kicad_pcb (version 20240108) (generator "pcbnew")\n{body})\n',
        encoding="utf-8",
    )
    return path


def test_sot23_at_every_placement_emits_the_same_correction(tmp_path):
    """Four top placements and two bottom ones all derive 180, and the CPL check agrees with itself."""
    validator = load_script()
    body = "".join(
        footprint_text(f"Q{i}", "F.Cu", r) for i, r in enumerate((0, 90, 180, 270), 1)
    )
    body += footprint_text("Q5", "B.Cu", 0, mirror=True)
    body += footprint_text("Q6", "B.Cu", 90, mirror=True)
    rows = validator.evaluate(board_file(tmp_path, body), FIXTURES)
    assert [row["verdict"].rotation for row in rows] == [180] * 6
    truth = {
        row["reference"]: str(
            validator.expected_cpl_rotation(row["placed"], row["bottom"], 180)
        )
        for row in rows
    }
    assert validator.compare(rows, truth) == []
    wrong = dict(truth, Q2="0")
    assert [reason for _, reason in validator.compare(rows, wrong)] == [
        "would emit 270, JLC shows 0"
    ]


def test_expected_cpl_rotation_matches_upstream():
    """Upstream mirrors bottom parts as 180 minus the angle, then adds the correction."""
    validator = load_script()
    assert validator.expected_cpl_rotation(90, False, 180) == 270
    assert validator.expected_cpl_rotation(90, True, 180) == 270
    assert validator.expected_cpl_rotation(0, True, 180) == 0
    assert validator.expected_cpl_rotation(270, False, 90) == 0


def test_missing_fixture_and_blank_truth(tmp_path):
    """No recorded response is reported; a blank truth means 'must not derive'."""
    validator = load_script()
    body = footprint_text("Q1", "F.Cu", 0).replace("C2132", "C999999999")
    rows = validator.evaluate(board_file(tmp_path, body), FIXTURES)
    assert rows[0]["verdict"] is None
    assert validator.compare(rows, {"Q1": "90"})[0][1] == "no recorded EasyEDA response"
    assert validator.compare(rows, {}) == []
    typo = validator.compare(rows, {"Q1": "90", "Q99": "0"})
    assert [(row["reference"], reason) for row, reason in typo] == [
        ("Q99", "not on the board (typo in truth.csv?)"),
        ("Q1", "no recorded EasyEDA response"),
    ]
    good = validator.evaluate(
        board_file(tmp_path, footprint_text("Q1", "F.Cu", 0)), FIXTURES
    )
    assert (
        validator.compare(good, {"Q1": ""})[0][1]
        == "expected no derived rotation, got 180"
    )


def test_rows_come_out_in_natural_reference_order(tmp_path):
    """Q2 sorts before Q10, whatever order pcbnew wrote the footprints in."""
    validator = load_script()
    body = "".join(footprint_text(ref, "F.Cu", 0) for ref in ("Q10", "Q2", "Q1"))
    rows = validator.evaluate(board_file(tmp_path, body), FIXTURES)
    assert [row["reference"] for row in rows] == ["Q1", "Q2", "Q10"]
    assert validator.reference_key("LED3") < validator.reference_key("Q1")
