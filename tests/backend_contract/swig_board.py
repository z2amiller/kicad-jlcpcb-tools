"""Build a described board in real KiCad and read it through the SWIG backend."""

from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ..native_kicad_support import add_native_field, set_native_field
from .support import Board, Part, uuid_owner

_PAD_LAYOUTS = {
    "smd": ("PAD_ATTRIB_SMD", "SMDMask", False),
    "pth": ("PAD_ATTRIB_PTH", "PTHMask", True),
    "npth": ("PAD_ATTRIB_NPTH", "UnplatedHoleMask", True),
    "conn": ("PAD_ATTRIB_CONN", "ConnSMDMask", False),
}


class _FailingFootprint:
    """A real footprint whose field writes change it and then fail.

    The first write fails after changing the footprint, as a KiCad setter
    can. Later writes and removals, which put the footprint back, work unless
    ``restore`` is false; then they fail without changing anything. KiCad 7
    writes its property map whole, KiCad 8 and later one field at a time.
    """

    def __init__(self, footprint: Any, restore: bool) -> None:
        self._footprint = footprint
        self._restore = restore
        self._failed = False

    def __getattr__(self, name: str) -> Any:
        """Reach every other footprint method."""
        return getattr(self._footprint, name)

    def _write(self, write: Callable[[], None]) -> None:
        """Write, then fail the first time; fail later writes unless restoring."""
        if self._failed and not self._restore:
            raise RuntimeError("KiCad refused to put the footprint back")
        write()
        if not self._failed:
            self._failed = True
            raise RuntimeError("KiCad refused the write")

    def SetField(self, name: str, text: str) -> None:
        """Write the field, then fail the first time."""
        self._write(lambda: self._footprint.SetField(name, text))

    def SetProperties(self, properties: dict[str, str]) -> None:
        """Write KiCad 7's property map, then fail the first time."""
        self._write(lambda: self._footprint.SetProperties(properties))

    def Remove(self, item: Any) -> None:
        """Remove a field, unless putting the footprint back is to fail."""
        if self._failed and not self._restore:
            raise RuntimeError("KiCad refused to put the footprint back")
        self._footprint.Remove(item)


class _Board:
    """The real board, with some footprints, by place, standing in for themselves."""

    def __init__(self, board: Any, footprints: dict[int, Any]) -> None:
        self._board = board
        self.footprints = footprints

    def __getattr__(self, name: str) -> Any:
        """Reach every other board method."""
        return getattr(self._board, name)

    def GetFootprints(self) -> list[Any]:
        """Return the board's footprints, in its order, with the stand-ins."""
        return [
            self.footprints.get(place, footprint)
            for place, footprint in enumerate(self._board.GetFootprints())
        ]


class _Editor:
    """The pcbnew module, with this board open in its editor.

    ``running`` stands for KiCad's action wrapper being open.
    """

    def __init__(self, pcbnew: Any, board: Any) -> None:
        self._pcbnew = pcbnew
        self.board = board
        self.running = False

    def GetBoard(self) -> Any:
        """Return the open board, or None once it is closed."""
        return self.board

    def IsActionRunning(self) -> bool:
        """Report whether KiCad's action wrapper is open."""
        return self.running

    def __getattr__(self, name: str) -> Any:
        """Reach every other pcbnew function."""
        return getattr(self._pcbnew, name)


def _footprint(pcbnew: Any, dialect: Any, board: Any, part: Part) -> Any:
    """Create one footprint the way the PCB editor stores it.

    A repeated name is added after the others, as KiCad's footprint dialog
    adds it, where this KiCad keeps two fields with one name; elsewhere the
    later text replaces the earlier one.
    """
    mm = pcbnew.FromMM
    footprint = pcbnew.FOOTPRINT(board)
    footprint.SetReference(part.reference)
    footprint.SetValue(part.value)
    library, name = part.footprint.split(":", 1)
    footprint.SetFPID(pcbnew.LIB_ID(library, name))
    named = set()
    for name, text in part.fields:
        if name in named and dialect.keeps_repeated_fields:
            add_native_field(pcbnew, footprint, name, text)
        else:
            named.add(name)
            set_native_field(footprint, name, text)
    # Where a KiCad can show no field but Reference and Value, the others
    # are hidden already.
    if dialect.shows_fields:
        for field in footprint.GetFields():
            if field.GetName() in part.hidden:
                field.SetVisible(False)
    if part.side == "bottom":
        footprint.SetLayerAndFlip(pcbnew.B_Cu)
    kinds = {pad.kind for pad in part.pads}
    footprint.SetAttributes(
        (0x01 if "pth" in kinds else 0)
        | (0x02 if "smd" in kinds else 0)
        | (0x04 if part.exclude_from_pos else 0)
        | (0x08 if part.exclude_from_bom else 0)
    )
    if dialect.holds_dnp:
        footprint.SetDNP(part.dnp)
    # FOOTPRINT.Add puts each pad first, so add them last to first.
    for spec in reversed(part.pads):
        attribute, mask, drilled = _PAD_LAYOUTS[spec.kind]
        pad = pcbnew.PAD(footprint)
        pad.SetNumber(spec.number)
        pad.SetAttribute(getattr(pcbnew, attribute))
        pad.SetShape(pcbnew.PAD_SHAPE_RECT)
        size = pcbnew.VECTOR2I(mm(1.0), mm(1.0))
        pad.SetSize(size)
        if drilled:
            pad.SetDrillSize(
                size if spec.kind == "npth" else pcbnew.VECTOR2I(mm(0.8), mm(0.8))
            )
        pad.SetLayerSet(getattr(pcbnew.PAD, mask)())
        footprint.Add(pad)
    board.Add(footprint)
    return footprint


def kicad_editor(
    modules: SimpleNamespace, parts: Sequence[Part], request: pytest.FixtureRequest
) -> Any:
    """Create a saved KiCad board holding the parts, open in a stand-in editor.

    It decides how to build the board from the facts of the dialect the
    backend speaks.
    """
    pcbnew = request.getfixturevalue("native_bindings").pcbnew
    dialect = modules.dialects.select(pcbnew)
    filename = str(request.getfixturevalue("tmp_path") / "board.kicad_pcb")
    board = pcbnew.NewBoard(filename)
    board.SetFileName(filename)
    # BOARD.Add puts each footprint first, so add them last to first.
    built = [_footprint(pcbnew, dialect, board, part) for part in reversed(parts)]
    footprints = built[::-1]
    uuids = [str(footprint.m_Uuid.AsString()) for footprint in footprints]
    shared = {
        uuids[index]: uuids[uuid_owner(parts, index)]
        for index in range(len(parts))
        if uuid_owner(parts, index) != index
    }
    if shared:
        # KiCad never gives two footprints one UUID, but a board file can
        # hold them, and opening the file keeps both.
        pcbnew.SaveBoard(filename, board)
        path = Path(filename)
        text = path.read_text(encoding="utf-8")
        for uuid, owner in shared.items():
            text = text.replace(uuid, owner)
        path.write_text(text, encoding="utf-8")
        board = pcbnew.LoadBoard(filename)
    return _Editor(pcbnew, _Board(board, {}))


def swig_board(
    modules: SimpleNamespace, parts: Sequence[Part], request: pytest.FixtureRequest
) -> Board:
    """Read the parts' board in real KiCad through the SWIG backend.

    What this KiCad holds is taken from the backend's dialect.
    """
    editor = kicad_editor(modules, parts, request)
    dialect = modules.dialects.select(editor)
    board = editor.board

    def close() -> None:
        editor.board = None

    def fail_writes(reference: str, restore: bool = True) -> None:
        ((place, footprint),) = (
            (place, fp)
            for place, fp in enumerate(board.GetFootprints())
            if fp.GetReference() == reference
        )
        board.footprints[place] = _FailingFootprint(footprint, restore)

    return Board(
        modules.swig.SwigBackend(editor),
        close,
        fail_writes,
        builtin_fields=dialect.builtin_fields,
        holds_dnp=dialect.holds_dnp,
        keeps_repeated_fields=dialect.keeps_repeated_fields,
        shows_fields=dialect.shows_fields,
        sorts_fields=dialect.sorts_fields,
    )
