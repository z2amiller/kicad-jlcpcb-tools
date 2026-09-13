"""Tests for scripts/truth_from_presses.py: preview presses plus the uploaded CPL give JLC's angle."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_script():
    """Import the converter script as a module without running its CLI."""
    spec = importlib.util.spec_from_file_location(
        "truth_from_presses", ROOT / "scripts" / "truth_from_presses.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_presses_add_to_the_uploaded_angle(tmp_path):
    """Comments and notes are tolerated, blanks stay blank, and a reference not in the CPL stops."""
    converter = load_script()
    cpl = tmp_path / "cpl.csv"
    cpl.write_text(
        "Designator,Val,Package,Mid X,Mid Y,Rotation,Layer\n"
        "U1,x,SOIC,1,1,270.0,top\nQ1,x,SOT,1,1,-90.0,top\nU7,x,SOT,1,1,0.0,top\n",
        encoding="utf-8",
    )
    presses = tmp_path / "presses.csv"
    presses.write_text(
        '### CCW turns pressed in the preview\nU1,0\nQ1,180,"band at the box"\nU7,,wrong pick\n',
        encoding="utf-8",
    )
    out = tmp_path / "truth.csv"
    assert converter.main([str(presses), str(cpl), "--out", str(out)]) == 0
    assert out.read_text(encoding="utf-8").splitlines() == [
        "reference,observed_rotation,note",
        "U1,270,",
        "Q1,90,band at the box",
        "U7,,wrong pick",
    ]
    presses.write_text("R9,0\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="not in the CPL: R9"):
        converter.main([str(presses), str(cpl), "--out", str(out)])
