"""Tests for scripts/validate_board.py: the gate's evaluate and compare plumbing."""

import importlib.util
from pathlib import Path

import pytest

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


def test_main_end_to_end_reports_and_exits(tmp_path, capsys):
    """The gate is main's exit code: 0 when JLC agrees, 1 with one reason line per disagreement."""
    validator = load_script()
    body = footprint_text("Q1", "F.Cu", 0) + footprint_text(
        "Q2", "B.Cu", 90, mirror=True
    )
    board = board_file(tmp_path, body)
    truth = tmp_path / "truth.csv"
    report = tmp_path / "report.txt"
    truth.write_text("reference,observed_rotation\nQ1,180\nQ2,270\n", encoding="utf-8")
    argv = [str(board), "--fixtures", str(FIXTURES), "--truth", str(truth)]
    assert validator.main([*argv, "--report", str(report)]) == 0
    out = capsys.readouterr().out
    assert "2 parts checked against JLC, 0 disagree" in out
    assert (
        report.read_text(encoding="utf-8")
        == validator.format_rows(validator.evaluate(board, FIXTURES)) + "\n"
    )
    truth.write_text("reference,observed_rotation\nQ1,180\nQ2,0\n", encoding="utf-8")
    assert validator.main(argv) == 1
    out = capsys.readouterr().out
    assert "2 parts checked against JLC, 1 disagree" in out
    assert "Q2" in out and "placed 90 bottom: would emit 270, JLC shows 0" in out


def test_load_truth_tolerates_spreadsheet_output(tmp_path):
    """A BOM, capitalised headers and a missing cell are read; a missing column stops the run."""
    validator = load_script()
    path = tmp_path / "truth.csv"
    path.write_text("﻿Reference,Observed_Rotation\nQ1,90\nQ2\n\n", encoding="utf-8")
    assert validator.load_truth(path) == {"Q1": "90", "Q2": ""}
    path.write_text("ref,observed_rotation\nQ1,90\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="no 'reference' column"):
        validator.load_truth(path)


def test_refused_part_with_a_recorded_angle_is_a_disagreement(tmp_path):
    """A red verdict against a JLC angle is reported with the status, never silently skipped."""
    validator = load_script()
    body = footprint_text("U1", "F.Cu", 0).replace("C2132", "C3014306")
    rows = validator.evaluate(board_file(tmp_path, body), FIXTURES)
    assert rows[0]["verdict"].status == "red"
    assert validator.compare(rows, {"U1": "180"})[0][1] == (
        "no derived rotation (red); JLC shows 180"
    )
    good = validator.evaluate(
        board_file(tmp_path, footprint_text("Q1", "F.Cu", 0)), FIXTURES
    )
    assert validator.compare(good, {"Q1": "180 deg"})[0][1] == (
        "truth value '180 deg' is not a number"
    )


def test_evaluate_uses_the_plugin_flip_constant_by_default(tmp_path, monkeypatch):
    """With no --flip-y the gate tests FLIP_EASYEDA_Y itself, so changing the constant changes the gate."""
    validator = load_script()
    board = board_file(tmp_path, footprint_text("Q1", "F.Cu", 0))
    as_shipped = validator.evaluate(board, FIXTURES)[0]["verdict"]
    geometry = importlib.import_module("jlcfootprint.geometry")
    monkeypatch.setattr(geometry, "FLIP_EASYEDA_Y", not geometry.FLIP_EASYEDA_Y)
    flipped = validator.evaluate(board, FIXTURES)[0]["verdict"]
    assert (as_shipped.rotation, as_shipped.status) != (
        flipped.rotation,
        flipped.status,
    )
    assert validator.evaluate(board, FIXTURES, flip_y=False)[0]["verdict"].rotation == (
        as_shipped.rotation
    )


def test_corrupt_fixture_names_the_file(tmp_path):
    """A truncated recording stops the run with its path instead of a bare JSON error."""
    validator = load_script()
    fixtures = tmp_path / "fixtures"
    fixtures.mkdir()
    (fixtures / "C2132.json").write_text('{"success": tru', encoding="utf-8")
    board = board_file(tmp_path, footprint_text("Q1", "F.Cu", 0))
    with pytest.raises(SystemExit, match="C2132.json: not valid JSON"):
        validator.evaluate(board, fixtures)
