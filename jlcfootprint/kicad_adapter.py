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
import hashlib
import json
import math
import re
from typing import Any

from .geometry import Pad, mirror_box, mirror_y, pad_hash
from .polarity import normalise_function

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
# Courtyard layer names as pcbnew reports them (KiCad 7 to 10) and as files spell them.
COURTYARD_LAYER_NAMES = ("F.Courtyard", "B.Courtyard", "F.CrtYd", "B.CrtYd")

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
    footprint_hash: str = ""  # verdict_key(pads): geometry plus pin functions
    value: str = ""
    # The courtyard's box in the footprint frame (mm, un-mirrored on the bottom).
    courtyard: tuple[float, float, float, float] | None = None


def verdict_key(pads: list[Pad]) -> str:
    """Return the key a verdict is stored under: the pad geometry plus the pin functions.

    Two footprints with the same pads but swapped K/A functions need opposite
    rotations (the corner-case board's D4 and D5 share C81598 on D_SOD-123), so the
    functions are part of the key even though ``pad_hash`` leaves them out.  Without
    any pin function the key is the pad hash itself, so a plain footprint on the top
    and on the bottom share one verdict.
    """
    functions = sorted(
        (pad.number, normalise_function(pad.pin_function))
        for pad in pads
        if normalise_function(pad.pin_function)
    )
    if not functions:
        return pad_hash(pads)
    payload = json.dumps([pad_hash(pads), functions]).encode("utf-8")
    return hashlib.blake2b(payload, digest_size=8).hexdigest()


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
    # A footprint's orientation turns its pads counter-clockwise as drawn, which on
    # pcbnew's Y-down canvas is board = R(-theta) * local, so removing the placement
    # is local = R(theta) * (board - origin), the matrix below.  Checked against
    # GetFPRelativePosition on 47 of 47 and 16 of 16 footprints, bottom side included.
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


def _on_courtyard(item: Any, pcbnew: Any = None) -> bool:
    """Return True for a graphic item on a courtyard layer."""
    ids = tuple(
        getattr(pcbnew, name)
        for name in ("F_CrtYd", "B_CrtYd")
        if hasattr(pcbnew, name)
    )
    if ids:
        return item.GetLayer() in ids
    name = getattr(item, "GetLayerName", None)
    return callable(name) and str(name()) in COURTYARD_LAYER_NAMES


def footprint_courtyard(
    footprint: Any, to_mm: Callable[[float], float], pcbnew: Any = None
) -> tuple[float, float, float, float] | None:
    """Return the courtyard's box in the footprint's own frame, un-mirrored on the bottom.

    pcbnew gives the graphics in board coordinates with the stroke in their
    bounding boxes; the box is shrunk by half the stroke to the centrelines (what
    the file parser reads) and its corners are taken back into the footprint frame
    with the pad fallback's rotation.  A placement off a multiple of 90 degrees
    gives a box on the large side, which only hides a caveat.  None without a
    courtyard or on a footprint double without graphics.
    """
    items = getattr(footprint, "GraphicalItems", None)
    if not callable(items):
        return None
    origin = footprint.GetPosition()
    theta = math.radians(_degrees(footprint.GetOrientation()))
    cos, sin = math.cos(theta), math.sin(theta)
    xs: list[float] = []
    ys: list[float] = []
    for item in items():
        if not _on_courtyard(item, pcbnew):
            continue
        box = item.GetBoundingBox()
        width = getattr(item, "GetWidth", None)
        half = float(width()) / 2.0 if callable(width) else 0.0
        left, top = float(box.GetLeft()) + half, float(box.GetTop()) + half
        right, bottom = float(box.GetRight()) - half, float(box.GetBottom()) - half
        for bx, by in ((left, top), (right, top), (right, bottom), (left, bottom)):
            dx, dy = bx - float(origin.x), by - float(origin.y)
            xs.append(dx * cos - dy * sin)
            ys.append(dx * sin + dy * cos)
    if not xs:
        return None
    result = (to_mm(min(xs)), to_mm(min(ys)), to_mm(max(xs)), to_mm(max(ys)))
    if footprint.GetLayer() != FRONT_COPPER:
        result = mirror_box(result)
    return result


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
        footprint_hash=verdict_key(pads),
        value=str(footprint.GetValue()),
        courtyard=footprint_courtyard(footprint, to_mm, pcbnew),
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
            import pcbnew as pcbnew_module

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
