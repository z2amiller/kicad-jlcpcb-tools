"""The live board reader agrees with the board-file parser on both gate boards, in real KiCad."""

import importlib.util
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from .native_kicad_support import native_bindings

__all__ = ["native_bindings"]

ROOT = Path(__file__).resolve().parents[1]
BOARDS = ROOT / "tests" / "fixtures" / "jlcfootprint" / "boards"
PRO_FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda_pro"


@pytest.fixture
def checker(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import tools/jlcfootprint/check_live_adapter.py as a module for one test.

    Its dataclasses look their module up in sys.modules, so it is registered
    there while the test runs.
    """
    spec = importlib.util.spec_from_file_location(
        "check_live_adapter", ROOT / "tools" / "jlcfootprint" / "check_live_adapter.py"
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.native_kicad
# The gate boards are KiCad 10 files, which KiCad 7 to 9 cannot open, so the test
# runs in the KiCad 10 lane only, as the other KiCad 10 fixtures' tests do.
@pytest.mark.kicad10_impedance
@pytest.mark.skipif(
    os.environ.get("KICAD_JLCPCB_NATIVE_TESTS") != "1",
    reason="Native KiCad tests require an explicit opt-in",
)
@pytest.mark.parametrize(
    ("board", "pro", "footprints"),
    [
        ("corner_case/corner_case.kicad_pcb", False, 47),
        ("corner_case/corner_case.kicad_pcb", True, 47),
        ("pin_functions_and_marks/pin_functions_and_marks.kicad_pcb", True, 16),
    ],
    ids=["corner-case", "corner-case-pro", "pin-functions-and-marks"],
)
def test_the_live_reader_agrees_with_the_board_file(
    native_bindings: SimpleNamespace,
    checker: ModuleType,
    board: str,
    pro: bool,
    footprints: int,
) -> None:
    """Every footprint's pads, courtyard, key and verdict agree between the two readers."""
    pcbnew = native_bindings.pcbnew
    path = BOARDS / board
    result = checker.compare(
        path,
        checker.read_components(pcbnew, path),
        pro_fixtures=PRO_FIXTURES if pro else None,
    )
    assert result.agrees, "\n".join([*result.lines, result.summary()])
    assert (result.footprints, result.resolved) == (footprints, footprints)
