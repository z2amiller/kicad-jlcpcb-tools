"""Headless integration tests for KiCad pcbnew SWIG bindings.

These tests are intentionally small sanity checks:
- load/save a board via pcbnew
- enumerate footprints
- assign/read LCSC-like fields on a real SWIG footprint
- persist a fake enrichment field for later plugin-side use
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.kicad_integration

pcbnew = pytest.importorskip("pcbnew")
helpers = pytest.importorskip("helpers")

get_lcsc_value = helpers.get_lcsc_value
set_lcsc_value = helpers.set_lcsc_value


def _new_board():
    """Create an empty board using whichever constructor the KiCad version exposes."""
    board_cls = getattr(pcbnew, "BOARD", None)
    if board_cls is not None:
        return board_cls()

    create_empty_board = getattr(pcbnew, "CreateEmptyBoard", None)
    if create_empty_board is not None:
        return create_empty_board()

    pytest.skip("No BOARD/CreateEmptyBoard API in this pcbnew build")
    raise AssertionError("unreachable")


def _new_footprint(board):
    """Create a footprint object across supported KiCad SWIG class names."""
    for class_name in ("FOOTPRINT", "PCB_FOOTPRINT", "MODULE"):
        cls = getattr(pcbnew, class_name, None)
        if cls is None:
            continue
        try:
            return cls(board)
        except TypeError:
            return cls()

    pytest.skip("No FOOTPRINT/PCB_FOOTPRINT/MODULE API in this pcbnew build")
    raise AssertionError("unreachable")


def _board_footprints(board):
    """Return footprints list across SWIG API variants."""
    if hasattr(board, "GetFootprints"):
        return list(board.GetFootprints())
    if hasattr(board, "GetModules"):
        return list(board.GetModules())
    pytest.skip("No GetFootprints/GetModules API in this pcbnew build")
    raise AssertionError("unreachable")


def _add_footprint(board, footprint):
    """Attach footprint to board across SWIG API variants."""
    if hasattr(board, "Add"):
        board.Add(footprint)
        return
    if hasattr(board, "AddFootprint"):
        board.AddFootprint(footprint)
        return
    pytest.skip("No Add/AddFootprint API in this pcbnew build")
    raise AssertionError("unreachable")


def _save_board(path: Path, board):
    """Persist board to disk across SWIG API variants."""
    if hasattr(pcbnew, "SaveBoard"):
        pcbnew.SaveBoard(str(path), board)
        return
    if hasattr(board, "Save"):
        board.Save(str(path))
        return
    pytest.skip("No SaveBoard/board.Save API in this pcbnew build")
    raise AssertionError("unreachable")


def _set_reference_value(footprint, reference: str = "R1", value: str = "10k"):
    """Populate common footprint identity fields when available."""
    if hasattr(footprint, "SetReference"):
        footprint.SetReference(reference)
    if hasattr(footprint, "SetValue"):
        footprint.SetValue(value)


def _get_named_field_text(footprint, name: str) -> str:
    """Read a footprint field/property by name across SWIG API variants."""
    if hasattr(footprint, "GetFields"):
        for field in footprint.GetFields():
            if field.GetName() == name:
                return field.GetText()
    if hasattr(footprint, "GetProperties"):
        return str(footprint.GetProperties().get(name, ""))
    return ""


def _create_round_trip_board(tmp_path: Path):
    """Create, save, and reload a board with one footprint."""
    board = _new_board()
    footprint = _new_footprint(board)
    _set_reference_value(footprint)
    _add_footprint(board, footprint)

    board_path = tmp_path / "swig_roundtrip.kicad_pcb"
    _save_board(board_path, board)

    loaded = pcbnew.LoadBoard(str(board_path))
    return loaded, _board_footprints(loaded)


def _fixture_board_path(*parts: str) -> Path:
    """Build an absolute path to a checked-in board fixture."""
    return Path(__file__).parent / "fixtures" / Path(*parts)


def test_load_board_and_list_footprints(tmp_path):
    """A board created via SWIG can be saved, reloaded, and enumerated."""
    loaded_board, footprints = _create_round_trip_board(tmp_path)
    assert loaded_board is not None
    assert len(footprints) >= 1


def test_load_checked_in_k9_fixture_board():
    """A real KiCad 9 fixture board can be loaded and enumerated headlessly."""
    board_path = _fixture_board_path("k9_smoke_ok", "fx-Full125B.kicad_pcb")
    assert board_path.exists(), f"Missing fixture board: {board_path}"

    loaded_board = pcbnew.LoadBoard(str(board_path))
    footprints = _board_footprints(loaded_board)
    assert loaded_board is not None
    assert len(footprints) >= 1


def test_assign_lcsc_to_real_footprint_field(tmp_path):
    """LCSC assignment helper round-trips on a real pcbnew footprint object."""
    _, footprints = _create_round_trip_board(tmp_path)
    footprint = footprints[0]

    set_lcsc_value(footprint, "C123456")
    assert get_lcsc_value(footprint) == "C123456"


def test_fake_enrichment_field_can_be_persisted(tmp_path):
    """A fake enrichment status field can be stored for plugin-side experimentation."""
    loaded_board, footprints = _create_round_trip_board(tmp_path)
    footprint = footprints[0]

    if not hasattr(footprint, "SetField"):
        pytest.skip("Footprint SetField API not available in this pcbnew build")

    footprint.SetField("JLC_ENRICHMENT", "Pending")

    board_path = tmp_path / "swig_enrichment_roundtrip.kicad_pcb"
    _save_board(board_path, loaded_board)

    loaded = pcbnew.LoadBoard(str(board_path))
    loaded_footprint = _board_footprints(loaded)[0]
    assert _get_named_field_text(loaded_footprint, "JLC_ENRICHMENT") == "Pending"
