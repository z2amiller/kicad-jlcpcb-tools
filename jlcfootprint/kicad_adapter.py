"""Read a live pcbnew board into resolver inputs (spec sections 4 and 6).

Pads come out in the footprint's own frame with the placement removed:
``GetFPRelativePosition`` where pcbnew has it, otherwise the board position
un-rotated by the footprint's angle.  pcbnew stores a bottom-side footprint's
pads mirrored, exactly as the board file does, so bottom parts are un-mirrored
with ``mirror_y`` like the validator un-mirrors file pads (checked on the
corner-case board, 2026-09-16).  pcbnew is never imported at module load, so
the module works in tests and standalone tools with duck-typed footprints.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
import math
import re
from typing import Any

from .geometry import Pad, mirror_y, pad_hash

# pcbnew's PAD_SHAPE and PAD_ATTRIB enumerations (KiCad 7 to 10) for boards read
# without the module (tests, tools); the live module's values win when present.
SHAPE_NAMES = {
    0: "circle",
    1: "rect",
    2: "oval",
    3: "trapezoid",
    4: "roundrect",
    5: "chamfered_rect",
    6: "custom",
}
CUSTOM_SHAPE = 6
NPTH_ATTRIBUTE = 3
FRONT_COPPER = 0

_LCSC_FIELD = re.compile(r"lcsc|jlc", re.IGNORECASE)
_LCSC_VALUE = re.compile(r"^C\d+$")


@dataclass
class BoardPart:
    """One placed footprint as the resolver and the CPL path need it."""

    reference: str
    lcsc: str
    footprint_name: str
    is_bottom: bool
    placed_rotation: float
    pads: list[Pad] = field(default_factory=list)
    footprint_hash: str = ""
    value: str = ""


def lcsc_value(footprint: Any) -> str:
    """Return the footprint's LCSC field (upstream's rule: an lcsc/jlc field holding C<digits>)."""
    try:
        fields = [(f.GetName(), f.GetText()) for f in footprint.GetFields()]
    except AttributeError:
        fields = list(footprint.GetProperties().items())
    for name, text in fields:
        if _LCSC_FIELD.match(str(name)) and _LCSC_VALUE.match(str(text).strip()):
            return str(text).strip()
    return ""


def _degrees(angle: Any) -> float:
    """Return degrees from an EDA_ANGLE or a number (tenths of a degree on old pcbnew)."""
    as_degrees = getattr(angle, "AsDegrees", None)
    if callable(as_degrees):
        return float(as_degrees())
    return float(angle) / 10.0


def counts_as_pad(pad: Any, pcbnew: Any = None) -> bool:
    """Return True for copper pads that are soldered: not NPTH, not paste-only."""
    npth = getattr(pcbnew, "PAD_ATTRIB_NPTH", NPTH_ATTRIBUTE)
    attribute = getattr(pad, "GetAttribute", None)
    if callable(attribute) and attribute() == npth:
        return False
    is_npth = getattr(pad, "IsNPTH", None)
    if callable(is_npth) and is_npth():
        return False
    on_copper = getattr(pad, "IsOnCopperLayer", None)
    return not (callable(on_copper) and not on_copper())


def _relative_position(pad: Any, footprint: Any) -> tuple[float, float]:
    """Return the pad centre relative to the footprint with its placement removed."""
    relative = getattr(pad, "GetFPRelativePosition", None)
    if callable(relative):
        point = relative()
        return float(point.x), float(point.y)
    origin = footprint.GetPosition()
    point = pad.GetPosition()
    dx, dy = float(point.x - origin.x), float(point.y - origin.y)
    theta = math.radians(_degrees(footprint.GetOrientation()))
    # pcbnew rotates a footprint CCW on its Y-down canvas: board = R(theta) * local.
    return (
        dx * math.cos(theta) - dy * math.sin(theta),
        dx * math.sin(theta) + dy * math.cos(theta),
    )


def _relative_rotation(pad: Any, footprint: Any) -> float:
    """Return the pad's own angle relative to the footprint, in degrees."""
    relative = getattr(pad, "GetFPRelativeOrientation", None)
    if callable(relative):
        return _degrees(relative()) % 360
    return (_degrees(pad.GetOrientation()) - _degrees(footprint.GetOrientation())) % 360


def footprint_pads(
    footprint: Any,
    to_mm: Callable[[float], float],
    pcbnew: Any = None,
    counts: Callable[[Any], bool] | None = None,
) -> list[Pad]:
    """Return the footprint's solderable pads in its own frame, un-mirrored on the bottom."""
    custom = getattr(pcbnew, "PAD_SHAPE_CUSTOM", CUSTOM_SHAPE)
    pads_fn = getattr(footprint, "Pads", None) or getattr(footprint, "GetPads")
    pads: list[Pad] = []
    for pad in pads_fn():
        if not counts_as_pad(pad, pcbnew) or (counts is not None and not counts(pad)):
            continue
        x, y = _relative_position(pad, footprint)
        size = pad.GetSize()
        shape_id = pad.GetShape()
        function = getattr(pad, "GetPinFunction", None)
        pads.append(
            Pad(
                number=str(pad.GetNumber()),
                x=to_mm(x),
                y=to_mm(y),
                width=to_mm(float(size.x)),
                height=to_mm(float(size.y)),
                rotation=_relative_rotation(pad, footprint),
                pin_function=str(function()) if callable(function) else "",
                shape="custom" if shape_id == custom else SHAPE_NAMES.get(shape_id, ""),
            )
        )
    if footprint.GetLayer() != FRONT_COPPER:
        pads = mirror_y(pads)
    return pads


def board_part(
    footprint: Any,
    to_mm: Callable[[float], float],
    pcbnew: Any = None,
    counts: Callable[[Any], bool] | None = None,
    lcsc_of: Callable[[Any], str] = lcsc_value,
) -> BoardPart:
    """Read one placed footprint."""
    pads = footprint_pads(footprint, to_mm, pcbnew, counts)
    return BoardPart(
        reference=str(footprint.GetReference()),
        lcsc=lcsc_of(footprint),
        footprint_name=str(footprint.GetFPID().GetLibItemName()),
        is_bottom=footprint.GetLayer() != FRONT_COPPER,
        placed_rotation=_degrees(footprint.GetOrientation()) % 360,
        pads=pads,
        footprint_hash=pad_hash(pads),
        value=str(footprint.GetValue()),
    )


def board_parts(
    board: Any,
    pcbnew: Any = None,
    to_mm: Callable[[float], float] | None = None,
    counts: Callable[[Any], bool] | None = None,
    lcsc_of: Callable[[Any], str] = lcsc_value,
) -> list[BoardPart]:
    """Read every footprint on the board.

    ``pcbnew`` is imported here when not supplied; ``to_mm`` defaults to its
    ``ToMM``, or to the identity when pcbnew is absent (test doubles in millimetres).
    ``counts`` is upstream's ``count_pad`` when the plugin supplies it and ``lcsc_of``
    upstream's ``get_lcsc_value``; the defaults apply the same rules.
    """
    if pcbnew is None:
        try:
            import pcbnew as pcbnew_module  # noqa: PLC0415

            pcbnew = pcbnew_module
        except ImportError:
            pcbnew = None
    if to_mm is None:
        to_mm = getattr(pcbnew, "ToMM", None) or (lambda value: float(value))
    return [
        board_part(footprint, to_mm, pcbnew, counts, lcsc_of)
        for footprint in board.GetFootprints()
        if re.match(r"[\w\d-]+", str(footprint.GetReference()))
    ]
