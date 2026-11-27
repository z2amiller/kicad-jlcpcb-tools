"""Read a live board's footprint records into resolver inputs: one ``BoardPart`` each.

The records are ``kicad_backend``'s: a footprint's placement, pads and courtyard
as KiCad reports them on the board, in nanometres.  Pads come out in the
footprint's own frame with the placement removed.  KiCad stores a bottom-side
footprint's pads mirrored, exactly as the board file does, so bottom parts are
un-mirrored with ``mirror_y`` as the validator un-mirrors file pads;
``tools/jlcfootprint/check_live_adapter.py`` checks that both readings agree on
the gate boards.  The records are read by their attributes, so the module needs
nothing from the plugin and its tests build records of their own.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from .boardfile import lcsc_assignment
from .geometry import Pad, mirror_box, mirror_y, normal_number, rotate
from .verdicts import verdict_key

if TYPE_CHECKING:
    from kicad_backend.records import ComponentRecord

NM_PER_MM = 1_000_000.0


@dataclass
class BoardPart:
    """One placed footprint as the resolver and the CPL path need it."""

    reference: str
    lcsc: str
    footprint_name: str
    is_bottom: bool
    placed_rotation: float
    pads: list[Pad] = field(default_factory=list)
    footprint_hash: str = ""  # verdicts.verdict_key(pads): geometry plus pin functions
    value: str = ""
    # The courtyard's box in the footprint frame (mm, un-mirrored on the bottom).
    courtyard: Optional[tuple[float, float, float, float]] = None


def lcsc_value(component: ComponentRecord) -> str:
    """Return the LCSC part number the footprint's own fields assign, or "" for none.

    ``boardfile.lcsc_assignment`` reads the fields, so a live board and its file
    give the same number, and the same one the plugin reads as a footprint's
    default assignment.
    """
    return lcsc_assignment(
        (name, record.text) for name, record in component.default.fields.items()
    )


def _to_local(
    x_nm: float, y_nm: float, component: ComponentRecord
) -> tuple[float, float]:
    """Return a board point in the footprint's frame, in mm, with the placement removed.

    A footprint's orientation turns its pads counter-clockwise as drawn, which on
    KiCad's Y-down canvas is board = R(-theta) * local + anchor, so removing the
    placement is local = R(theta) * (board - anchor).  The result is rounded to
    whole nanometres, as pcbnew's ``GetFPRelativePosition`` rounds it: a turn by
    a multiple of 90 degrees is then exact, so a pad on a quantisation boundary
    of ``pad_hash`` (a SOT-23's 0.9375 mm) keys the same as in the board file.
    It equals ``GetFPRelativePosition`` on every pad of the two gate boards' 47
    and 16 footprints, the bottom side included.
    """
    anchor_x, anchor_y = component.position_nm
    local_x, local_y = rotate(
        x_nm - anchor_x, y_nm - anchor_y, component.orientation_deg
    )
    return round(local_x) / NM_PER_MM, round(local_y) / NM_PER_MM


def is_soldered(pad: Any) -> bool:
    """Return True for a pad that is soldered: on copper and not an NPTH hole."""
    return pad.kind != "npth" and pad.on_copper


def footprint_pads(component: ComponentRecord) -> list[Pad]:
    """Return the footprint's soldered pads in its own frame, un-mirrored on the bottom."""
    pads: list[Pad] = []
    for pad in component.pads:
        if not is_soldered(pad):
            continue
        x, y = _to_local(*pad.position_nm, component)
        width, height = pad.size_nm
        pads.append(
            Pad(
                number=normal_number(pad.number),
                x=x,
                y=y,
                width=width / NM_PER_MM,
                height=height / NM_PER_MM,
                rotation=(pad.orientation_deg - component.orientation_deg) % 360,
                pin_function=pad.pin_function,
                shape=pad.shape,
            )
        )
    if component.side == "bottom":
        pads = mirror_y(pads)
    return pads


def footprint_courtyard(
    component: ComponentRecord,
) -> Optional[tuple[float, float, float, float]]:
    """Return the courtyard's box in the footprint's own frame, un-mirrored on the bottom.

    KiCad gives each courtyard shape's box on the board with the stroke in it;
    the box is shrunk by half the stroke to the centrelines (what the file parser
    reads) and its corners are taken back into the footprint frame.  A placement
    off a multiple of 90 degrees gives a box on the large side, which only hides a
    caveat.  None without a courtyard.
    """
    xs: list[float] = []
    ys: list[float] = []
    for shape in component.courtyard:
        half = shape.stroke_nm / 2.0
        left, top, right, bottom = shape.bbox_nm
        left, top, right, bottom = left + half, top + half, right - half, bottom - half
        for corner_x, corner_y in (
            (left, top),
            (right, top),
            (right, bottom),
            (left, bottom),
        ):
            x, y = _to_local(corner_x, corner_y, component)
            xs.append(x)
            ys.append(y)
    if not xs:
        return None
    box = (min(xs), min(ys), max(xs), max(ys))
    if component.side == "bottom":
        box = mirror_box(box)
    return box


def _value(component: ComponentRecord) -> str:
    """Return the footprint's value: its first Value field, KiCad's own.

    A user can add a second field named Value; KiCad shows the first.
    """
    state = component.default
    for record in (*state.replaced_fields, *state.fields.values()):
        if record.name == "Value":
            return record.text
    return ""


def board_part(
    component: ComponentRecord,
    lcsc_of: Callable[[ComponentRecord], str] = lcsc_value,
) -> BoardPart:
    """Read one placed footprint."""
    pads = footprint_pads(component)
    return BoardPart(
        reference=component.reference,
        lcsc=lcsc_of(component),
        # "Lib:Name", as boardfile gives it; the resolver reads the name part.
        footprint_name=component.footprint_lib_id,
        is_bottom=component.side == "bottom",
        placed_rotation=component.orientation_deg % 360,
        pads=pads,
        footprint_hash=verdict_key(pads),
        value=_value(component),
        courtyard=footprint_courtyard(component),
    )


def board_parts(
    components: Iterable[ComponentRecord],
    lcsc_of: Callable[[ComponentRecord], str] = lcsc_value,
) -> list[BoardPart]:
    """Read the given footprints, in order.

    Which footprints of a board count is the caller's choice: the plugin's parts
    list shows those whose reference ``board_parts.is_listed`` accepts, and a tool
    reading a whole board passes every component of its snapshot.  ``lcsc_of``
    reads a footprint's part number; the default, ``lcsc_value``, reads its own
    fields as the plugin reads a footprint's default assignment.
    """
    return [board_part(component, lcsc_of) for component in components]
