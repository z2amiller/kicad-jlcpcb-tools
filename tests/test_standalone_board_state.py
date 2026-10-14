"""Standalone board mutations survive subsequent native-style reads."""

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from .wx_harness import load_siblings, wx_stubs


@pytest.fixture
def modules() -> Iterator[dict[str, ModuleType]]:
    """Exercise production stubs and edit helpers together without a GUI."""
    with load_siblings(
        "_standalone_board_state_tests",
        (
            "standalone_impl",
            "store",
            "kicad_backend",
            "kicad_backend.swig",
            "app.assignments",
        ),
        wx_stubs(),
    ) as loaded:
        yield loaded


def _fields(footprint: object) -> dict[str, str]:
    """Read a stub footprint's own fields by name, after Reference and Value."""
    return {field.GetName(): field.GetText() for field in footprint.GetFields()[2:]}


def _assign(
    modules: dict[str, ModuleType],
    pcbnew: object,
    assignments: dict[str, str],
    **flags: bool,
) -> None:
    """Assign numbers by reference through the SWIG backend, as standalone mode does."""
    backend = modules["kicad_backend.swig"].SwigBackend(pcbnew)
    board = backend.snapshot()
    edits = []
    for reference, lcsc in assignments.items():
        (component,) = board.by_reference(reference)
        edit = modules["app.assignments"].assignment_edit(component, lcsc)
        edits.append(replace(edit, flags=tuple(flags.items())))
    backend.apply("apply LCSC assignments", edits)


def test_the_store_reads_the_standalone_board(
    modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    """Standalone mode lists its demo part: the SWIG backend reads the stubs."""
    backend = modules["kicad_backend.swig"].SwigBackend(
        modules["standalone_impl"].PcbnewStub()
    )
    parent = SimpleNamespace(
        settings={}, _read_board=backend.snapshot, _held_board=backend.snapshot
    )
    store = modules["store"].Store(parent, str(tmp_path))

    (row,) = store.read_all()

    assert (row["reference"], row["value"], row["footprint"]) == (
        "R1",
        "100",
        "resistors",
    )
    assert (row["lcsc"], row["is_dnp"], row["pad_count"]) == ("", False, 0)
    assert store.get_part("R1") == row


def test_assignment_and_clear_survive_board_lookup(
    modules: dict[str, ModuleType],
) -> None:
    """Assignment writes update the same footprint returned on each board read."""
    pcbnew = modules["standalone_impl"].PcbnewStub()
    board = pcbnew.GetBoard()
    footprint = board.GetFootprints()[0]
    assert board.FindFootprintByReference("R1") is footprint
    assert board.FindFootprintByReference("missing") is None
    assert footprint.GetFieldByName("LCSC") is None

    _assign(modules, pcbnew, {"R1": " c123 "})

    assert _fields(board.GetFootprints()[0])["LCSC"] == "C123"
    field = footprint.GetFieldByName("LCSC")
    assert field.GetText() == "C123"
    assert field.IsVisible() is False
    assert footprint.GetProperties() == {"LCSC": "C123"}
    assert footprint.modified is True

    _assign(modules, pcbnew, {"R1": ""})

    assert _fields(footprint)["LCSC"] == ""
    assert footprint.GetFields()[2:] == [field]
    assert field.GetText() == ""
    assert field.IsVisible() is False
    assert footprint.GetProperties() == {"LCSC": ""}


def test_alias_clear_preserves_identity_visibility_and_unrelated_metadata(
    modules: dict[str, ModuleType],
) -> None:
    """Clearing all assignment aliases preserves descriptive fields and flags."""
    pcbnew = modules["standalone_impl"].PcbnewStub()
    board = pcbnew.GetBoard()
    footprint = board.GetFootprints()[0]
    footprint.SetField("LCSC", "C100")
    footprint.SetField("JLCPCB", "C200")
    footprint.SetField("JLC description", "Keep this description")
    original_fields = footprint.GetFields()
    original_fields[3].SetVisible(False)
    footprint.SetAttributes(65)

    _assign(modules, pcbnew, {"R1": ""}, exclude_from_bom=True, exclude_from_pos=True)

    assert footprint.GetFields() == original_fields
    assert footprint.GetProperties() == {
        "LCSC": "",
        "JLCPCB": "",
        "JLC description": "Keep this description",
    }
    assert original_fields[2].IsVisible() is True
    assert original_fields[3].IsVisible() is False
    assert footprint.GetAttributes() == 77
    assert _fields(board.FindFootprintByReference("R1"))["LCSC"] == ""
    assert board.FindFootprintByReference("R1").GetAttributes() & 0b1100 == 0b1100


def test_failed_batch_removes_new_field_and_restores_existing_aliases(
    modules: dict[str, ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later failed setter rolls back new metadata and preceding native edits."""
    standalone = modules["standalone_impl"]
    pcbnew = standalone.PcbnewStub()
    board = pcbnew.GetBoard()
    first = board.GetFootprints()[0]
    first.SetField("Manufacturer", "Acme")
    first.SetAttributes(1)
    second = standalone.Footprint_Stub("R2", "100", standalone.LIB_ID_Stub("R"))
    board.footprints.append(second)
    second.SetField("LCSC", "C200")
    second.SetField("JLCPCB", "C201")
    second.GetFieldByName("LCSC").SetVisible(False)
    setter = second.SetField

    def fail_after_alias_write(name: str, text: str) -> None:
        setter(name, text)
        if name == "JLCPCB" and text == "C900":
            raise RuntimeError("standalone native write failed")

    monkeypatch.setattr(second, "SetField", fail_after_alias_write)
    backend = modules["kicad_backend.swig"].SwigBackend(pcbnew)
    assignment_edit = modules["app.assignments"].assignment_edit
    r1, r2 = (backend.snapshot().by_reference(ref)[0] for ref in ("R1", "R2"))
    edits = [
        replace(assignment_edit(r1, "C900"), flags=(("exclude_from_bom", True),)),
        assignment_edit(r2, "C900"),
    ]
    with pytest.raises(
        modules["kicad_backend"].EditRejected, match="standalone native write failed"
    ):
        backend.apply("apply LCSC assignments", edits)

    assert first.GetProperties() == {"Manufacturer": "Acme"}
    assert first.GetFieldByName("LCSC") is None
    assert first.GetAttributes() == 1
    assert second.GetProperties() == {"LCSC": "C200", "JLCPCB": "C201"}
    assert second.GetFieldByName("LCSC").IsVisible() is False
    assert second.GetFieldByName("JLCPCB").IsVisible() is True
    assert first.modified is False
    assert second.modified is False


def test_board_filename_survives_readback(modules: dict[str, ModuleType]) -> None:
    """A selected standalone board path remains available to project storage."""
    board = modules["standalone_impl"].BoardStub()
    assert board.GetFileName() == "fake_test_board.kicad_pcb"
    board.SetFileName("project/assembly.kicad_pcb")
    assert board.GetFileName() == "project/assembly.kicad_pcb"
