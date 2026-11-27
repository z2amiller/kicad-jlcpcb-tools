"""Read the board open in KiCad's PCB editor into records.

The snapshot's reads of the board and its footprints are made here, and
nothing returned holds a SWIG object. What differs between KiCad majors is
read through the caller's dialect. Code outside the backend still makes its
own SWIG reads for what the records do not hold yet.
"""

from contextlib import suppress
import hashlib
from inspect import getattr_static
from typing import Any

from .. import BoardChanged
from ..records import (
    BoardInfo,
    BoardSnapshot,
    ComponentRecord,
    CourtyardShape,
    PadRecord,
    VariantDefinition,
    component_state,
)
from .dialects import Dialect

# FOOTPRINT_ATTR_T bits, the same in KiCad 7 to 10. KiCad 10 moved FP_DNP,
# so the dialect reads DNP instead.
_EXCLUDE_FROM_POS = 0x04
_EXCLUDE_FROM_BOM = 0x08
# PAD_ATTRIB values: PTH=0, SMD=1, CONN=2, NPTH=3.
_PAD_KINDS = ("pth", "smd", "conn", "npth")
# PAD_SHAPE values, the same in KiCad 7 to 10; any other reads as "".
_PAD_SHAPES = (
    "circle",
    "rect",
    "oval",
    "trapezoid",
    "roundrect",
    "chamfered_rect",
    "custom",
)
# KiCad's own names for the courtyard layers, the same in KiCad 7 to 10. Their
# IDs changed in KiCad 9 (47 and 46 before, 31 and 29 after), so the layers are
# found by name, never by number.
_COURTYARD_NAMES = frozenset({"F.Courtyard", "B.Courtyard"})


def board_identity(board: Any) -> str:
    """Identify an open board without keeping a native handle.

    The identity includes the native address, the board UUID and the file
    name. KiCad can return several SWIG proxies for one board. Its saved UUID
    alone cannot distinguish a board reopened in the editor, and Save As
    changes the project whose settings and output paths the plugin must use.
    Objects that are not SWIG proxies fall back to Python identity.
    """
    if board is None:
        raise BoardChanged("The PCB editor no longer has an open board")
    pointer = board
    seen: set[int] = set()
    for _ in range(8):
        if getattr_static(pointer, "this", None) is None or id(pointer) in seen:
            break
        seen.add(id(pointer))
        pointer = pointer.this
    address = id(board)
    if seen:
        with suppress(TypeError, ValueError):
            address = int(pointer)
    uuid = getattr(board, "m_Uuid", None)
    uuid_text = uuid.AsString() if callable(getattr(uuid, "AsString", None)) else ""
    parts = (address, str(uuid_text), str(board.GetFileName()))
    return hashlib.sha256(repr(parts).encode("utf-8")).hexdigest()


def read_info(board: Any) -> BoardInfo:
    """Identify the open board and its file."""
    return BoardInfo(board_identity(board), str(board.GetFileName()))


def _pad_shape(value: int) -> str:
    """Return a PAD_SHAPE value's name, or "" for a value this reader does not know."""
    return _PAD_SHAPES[value] if 0 <= value < len(_PAD_SHAPES) else ""


def _xy(point: Any) -> tuple[int, int]:
    """Return a KiCad point's coordinates."""
    return int(point.x), int(point.y)


def _pad(pad: Any) -> PadRecord:
    """Read one pad as KiCad has it on the board."""
    return PadRecord(
        _PAD_KINDS[int(pad.GetAttribute())],
        bool(pad.HasHole()),
        str(pad.GetNumber()),
        _xy(pad.GetPosition()),
        _xy(pad.GetSize()),
        float(pad.GetOrientation().AsDegrees()),
        _pad_shape(int(pad.GetShape())),
        str(pad.GetPinFunction()),
        bool(pad.IsOnCopperLayer()),
    )


class _CourtyardLayers:
    """Tell a board's courtyard layers by KiCad's own name for each layer.

    ``GetStandardLayerName`` gives KiCad's name, which a user's layer names
    never replace, and is asked once per layer.
    """

    def __init__(self, board: Any) -> None:
        self._board = board
        self._known: dict[int, bool] = {}

    def __contains__(self, layer: int) -> bool:
        """Return whether ``layer`` is a courtyard layer."""
        if layer not in self._known:
            name = str(self._board.GetStandardLayerName(layer))
            self._known[layer] = name in _COURTYARD_NAMES
        return self._known[layer]


def _courtyard(
    footprint: Any, layers: _CourtyardLayers, shape_class: str
) -> tuple[CourtyardShape, ...]:
    """Read the shapes on the footprint's courtyard layers, in KiCad's order.

    Only drawn shapes count, the items whose class is ``shape_class``: a
    footprint's graphic items also hold texts, text boxes, dimensions, tables
    and barcodes, and a text on a courtyard layer bounds nothing and has no
    stroke width.
    """
    shapes = []
    for item in footprint.GraphicalItems():
        if int(item.GetLayer()) not in layers:
            continue
        if str(item.GetClass()) != shape_class:
            continue
        box = item.GetBoundingBox()
        shapes.append(
            CourtyardShape(
                (
                    int(box.GetLeft()),
                    int(box.GetTop()),
                    int(box.GetRight()),
                    int(box.GetBottom()),
                ),
                int(item.GetWidth()),
            )
        )
    return tuple(shapes)


def _component(
    footprint: Any, dialect: Dialect, courtyard_layers: _CourtyardLayers
) -> ComponentRecord:
    """Read one footprint with its Default state."""
    reference = str(footprint.GetReference())
    attributes = int(footprint.GetAttributes())
    state = component_state(
        dialect.fields(footprint),
        dnp=dialect.is_dnp(footprint),
        exclude_from_bom=bool(attributes & _EXCLUDE_FROM_BOM),
        exclude_from_pos=bool(attributes & _EXCLUDE_FROM_POS),
    )
    return ComponentRecord(
        str(footprint.m_Uuid.AsString()),
        reference,
        str(footprint.GetFPIDAsString()),
        str(footprint.GetPath().AsString()),
        "bottom" if footprint.IsFlipped() else "top",
        _xy(footprint.GetPosition()),
        float(footprint.GetOrientation().AsDegrees()),
        tuple(_pad(pad) for pad in footprint.Pads()),
        _courtyard(footprint, courtyard_layers, dialect.footprint_shape_class),
        {"": state},
    )


def read_snapshot(board: Any, dialect: Dialect) -> BoardSnapshot:
    """Read every footprint of the open board in its Default state.

    ``dialect`` is the running KiCad's, from ``dialects.select``.
    """
    info = read_info(board)
    names = getattr(board, "GetVariantNamesForUI", None)
    # KiCad 10 lists its translated label for Default first.
    label = str(tuple(names())[0]) if callable(names) else "Default"
    layers = _CourtyardLayers(board)
    components = tuple(
        _component(footprint, dialect, layers) for footprint in board.GetFootprints()
    )
    return BoardSnapshot(info, (VariantDefinition("", label),), components)
