"""The detail dialog's pad overlay, as primitives with coordinates (spec 16.4).

Pure and stdlib only: given one part's detail and a canvas size in pixels, this
returns the rectangles, circles, lines and labels the canvas draws, in canvas
pixels, so the whole layout -- the scale that fits both pad sets, the grid and its
scale bar, the pin-1 marks, the ``+`` marks, the angle labels -- is testable
without wx.  The dialog only walks the list and sets a pen and a brush per role.

Frames.  Everything is drawn in the KiCad footprint's own frame (millimetres,
Y down, bottom-side parts already un-mirrored by the adapter, which is what
"bottom, mirrored" in the label refers to).  The resolver's placement maps KiCad
pads onto the JLC drawing, so the transformed JLC pads are drawn through its
inverse; the raw JLC pads are drawn in their own frame, which is what a part with
no placement can still show.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math

from .fit import Placement, package_origin, transformed
from .geometry import Pad, named_pads, pad_geom
from .model import PartDetail
from .polarity import terminal_of

# What the canvas can show; the dialog's three checkboxes switch these.
KICAD = "kicad"
JLC_PLACED = "jlc_placed"
JLC_RAW = "jlc_raw"
DEFAULT_LAYERS = (KICAD, JLC_PLACED)

MARGIN_FRACTION = 0.15  # spec 16.4: the pads plus a 15 % margin
SCALE_BAR_LENGTHS_MM = (0.5, 1.0, 2.0, 5.0, 10.0, 20.0)
SCALE_BAR_TARGET = 0.25  # about a quarter of the canvas
PIN1_DOT_PX = 3.0  # fixed in DIP whatever the scale
PIN1_RING_PX = 5.0
PLUS_ARM_PX = 4.0
PLUS_GAP_PX = 5.0  # between a pad's edge and the "+" that names it
ORIGIN_ARM_PX = 5.0  # half a diagonal of the cross on JLC's package origin
EDGE_PX = 8.0
LINE_HEIGHT_PX = 14.0


@dataclass
class Primitive:
    """One thing to draw.

    ``kind`` is ``rect``, ``circle``, ``line`` or ``text``; ``role`` says what it
    means, which is how the dialog picks a pen, a brush and a colour and how a test
    finds it.  Coordinates are canvas pixels, with the text anchored at its top left
    unless ``align`` says ``right``.
    """

    kind: str
    role: str
    x: float = 0.0
    y: float = 0.0
    width: float = 0.0
    height: float = 0.0
    x2: float = 0.0
    y2: float = 0.0
    radius: float = 0.0
    text: str = ""
    align: str = "left"
    number: str = ""


@dataclass
class Overlay:
    """The drawing plus the numbers the dialog puts beside it."""

    primitives: list[Primitive] = field(default_factory=list)
    scale_px_per_mm: float = 1.0
    grid_mm: float = 1.0
    layers: tuple = DEFAULT_LAYERS
    bounds_mm: tuple = (0.0, 0.0, 0.0, 0.0)
    note: str = ""  # why there is no transform, when there is none

    def by_role(self, role: str) -> list:
        """Return the primitives of one role, in draw order."""
        return [item for item in self.primitives if item.role == role]


def inverse(placement: Placement) -> Placement:
    """Return the placement that maps JLC pads back into the KiCad footprint frame.

    ``Placement`` turns KiCad pads onto the JLC drawing (``fit.transformed``); the
    canvas needs the other direction, which is the same rotation negated with the
    offset turned with it.  That turned offset is where JLC's drawing origin lands
    in the footprint frame, so it is :func:`fit.package_origin` itself.
    """
    offset_x, offset_y = package_origin(placement)
    return Placement(
        rotation_deg=(-placement.rotation_deg) % 360,
        offset_x=offset_x,
        offset_y=offset_y,
        is_mirrored=placement.is_mirrored,
        is_underdetermined=placement.is_underdetermined,
        residual=placement.residual,
        angular_rms=placement.angular_rms,
        matched=placement.matched,
    )


def placed(pad: Pad, placement: Placement | None) -> tuple:
    """Return ``(x, y, w, h)`` for one pad after a placement (or as it is, for None)."""
    if placement is None:
        _, _, width, height = pad_geom(pad)
        return (pad.x, pad.y, width, height)
    return transformed(pad, placement)


def _boxes(pads: list[Pad], placement: Placement | None) -> list:
    """Return ``(number, x, y, w, h)`` per named pad after a placement."""
    return [(pad.number, *placed(pad, placement)) for pad in named_pads(pads)]


def _bounds(groups: list) -> tuple:
    """Return the millimetre box around every pad of every group, or a unit box."""
    xs: list = []
    ys: list = []
    for boxes in groups:
        for _number, x, y, width, height in boxes:
            xs.extend((x - width / 2.0, x + width / 2.0))
            ys.extend((y - height / 2.0, y + height / 2.0))
    if not xs:
        return (-0.5, -0.5, 0.5, 0.5)
    if max(xs) - min(xs) <= 0:
        xs = [min(xs) - 0.5, max(xs) + 0.5]
    if max(ys) - min(ys) <= 0:
        ys = [min(ys) - 0.5, max(ys) + 0.5]
    return (min(xs), min(ys), max(xs), max(ys))


def grid_mm(scale: float, width_px: float) -> float:
    """Return the round grid pitch whose spacing is nearest a quarter of the canvas.

    The scale bar uses the same length, so the bar always spans a whole number of
    grid squares; 0.5 mm on an 0201 and 20 mm on a DIP-40 bound the choice.
    """
    target = max(width_px * SCALE_BAR_TARGET, 1.0) / max(scale, 1e-9)
    return min(SCALE_BAR_LENGTHS_MM, key=lambda length: abs(length - target))


def overlay(
    detail: PartDetail,
    size_px: tuple,
    layers: tuple = DEFAULT_LAYERS,
) -> Overlay:
    """Return the canvas primitives for one part's detail (spec 16.4).

    ``layers`` is any of ``kicad``, ``jlc_placed`` and ``jlc_raw``; a layer with
    nothing to draw is skipped silently, and a part with no placement draws its raw
    JLC pads instead of the transformed ones with a note saying why.  ``layers``
    chooses only which primitives are emitted: the scale, the centre, the grid pitch
    and the scale bar come from every pad set the part has, so the drawing does not
    move when a checkbox is toggled.
    """
    width_px, height_px = (float(size_px[0]), float(size_px[1]))
    placement = detail.placement
    note = ""
    wanted = list(layers)
    if JLC_PLACED in wanted and placement is None:
        wanted = [layer for layer in wanted if layer != JLC_PLACED]
        if detail.jlc_pads:
            if JLC_RAW not in wanted:
                wanted.append(JLC_RAW)
            note = "no placement: the JLC pads are drawn as the drawing has them"
        else:
            note = "no JLC pads cached for this part yet"
    # The frame is measured from every pad set the part has, shown or not, so that
    # toggling a checkbox never rescales the drawing (spec 16.4, amended 2026-09-18:
    # the frame is fixed across the toggles).  Only a real resize rescales.
    kicad_boxes = _boxes(detail.kicad_pads, None)
    placed_boxes = (
        _boxes(detail.jlc_pads, inverse(placement)) if placement is not None else []
    )
    raw_boxes = _boxes(detail.jlc_pads, None)
    x1, y1, x2, y2 = _bounds([kicad_boxes, placed_boxes, raw_boxes])
    span_x = (x2 - x1) * (1.0 + 2 * MARGIN_FRACTION)
    span_y = (y2 - y1) * (1.0 + 2 * MARGIN_FRACTION)
    scale = min(width_px / span_x, height_px / span_y)
    centre_x, centre_y = (x1 + x2) / 2.0, (y1 + y2) / 2.0

    def to_px(x: float, y: float) -> tuple:
        """Map millimetres in the footprint frame to canvas pixels."""
        return (
            width_px / 2.0 + (x - centre_x) * scale,
            height_px / 2.0 + (y - centre_y) * scale,
        )

    pitch = grid_mm(scale, width_px)
    if KICAD not in wanted:
        kicad_boxes = []
    if JLC_PLACED not in wanted:
        placed_boxes = []
    if JLC_RAW not in wanted:
        raw_boxes = []
    result = Overlay(
        scale_px_per_mm=scale,
        grid_mm=pitch,
        layers=tuple(wanted),
        bounds_mm=(x1, y1, x2, y2),
        note=note,
    )
    items = result.primitives
    # The grid, on the scale bar's pitch, drawn from the centre outwards.
    steps_x = int(math.floor((width_px / 2.0) / (pitch * scale)))
    steps_y = int(math.floor((height_px / 2.0) / (pitch * scale)))
    for step in range(-steps_x, steps_x + 1):
        x = width_px / 2.0 + step * pitch * scale
        items.append(Primitive("line", "grid", x=x, y=0.0, x2=x, y2=height_px))
    for step in range(-steps_y, steps_y + 1):
        y = height_px / 2.0 + step * pitch * scale
        items.append(Primitive("line", "grid", x=0.0, y=y, x2=width_px, y2=y))
    for role, boxes in (
        ("jlc_raw_pad", raw_boxes),
        ("jlc_pad", placed_boxes),
        ("kicad_pad", kicad_boxes),
    ):
        for number, x, y, box_width, box_height in boxes:
            left, top = to_px(x - box_width / 2.0, y - box_height / 2.0)
            items.append(
                Primitive(
                    "rect",
                    role,
                    x=left,
                    y=top,
                    width=box_width * scale,
                    height=box_height * scale,
                    number=number,
                )
            )
    # Pin 1: a dot on KiCad's, a ring on JLC's, both a fixed size in DIP.
    for role, boxes, radius in (
        ("kicad_pin1", kicad_boxes, PIN1_DOT_PX),
        ("jlc_pin1", placed_boxes or raw_boxes, PIN1_RING_PX),
    ):
        for number, x, y, _width, _height in boxes:
            if number != "1":
                continue
            centre = to_px(x, y)
            items.append(
                Primitive("circle", role, x=centre[0], y=centre[1], radius=radius)
            )
    # The "+" beside each positive terminal, as the vote settled it: KiCad's above its
    # pad and JLC's below its own, both clear of the pad, so a filled JLC pad cannot
    # hide either and a correct alignment reads as two marks on one terminal.
    for role, boxes, pad_number, direction in (
        ("kicad_plus", kicad_boxes, _kicad_positive(detail), -1.0),
        ("jlc_plus", placed_boxes or raw_boxes, _jlc_positive(detail), 1.0),
    ):
        if pad_number is None:
            continue
        for number, x, y, _width, height in boxes:
            if number != pad_number:
                continue
            centre = to_px(x, y)
            edge = height * scale / 2.0 + PLUS_GAP_PX + PLUS_ARM_PX
            items.extend(_plus(centre[0], centre[1] + direction * edge, role))
    # JLC's package origin: a small diagonal cross in the JLC colour, drawn over the
    # pads (spec 17.5).  It belongs to the transformed layer, because the origin is
    # only meaningful once the placement has put the drawing on the footprint; an X
    # rather than a "+" so it cannot be read as a polarity mark.
    if JLC_PLACED in wanted and placement is not None:
        items.extend(_cross(*to_px(*package_origin(placement)), "jlc_origin"))
    # The scale bar in the bottom left corner, one grid square long.
    bar_y = height_px - EDGE_PX
    items.append(
        Primitive(
            "line",
            "scale_bar",
            x=EDGE_PX,
            y=bar_y,
            x2=EDGE_PX + pitch * scale,
            y2=bar_y,
        )
    )
    items.append(
        Primitive(
            "text",
            "scale_label",
            x=EDGE_PX,
            y=bar_y - LINE_HEIGHT_PX,
            text=f"{pitch:g} mm",
        )
    )
    for index, text in enumerate(_labels(detail, placement)):
        items.append(
            Primitive(
                "text",
                "angle_label",
                x=EDGE_PX,
                y=EDGE_PX + index * LINE_HEIGHT_PX,
                text=text,
            )
        )
    return result


def _plus(x: float, y: float, role: str) -> list:
    """Return the two bars of a ``+`` centred on a point, a fixed size in DIP."""
    return [
        Primitive("line", role, x=x - PLUS_ARM_PX, y=y, x2=x + PLUS_ARM_PX, y2=y),
        Primitive("line", role, x=x, y=y - PLUS_ARM_PX, x2=x, y2=y + PLUS_ARM_PX),
    ]


def _cross(x: float, y: float, role: str) -> list:
    """Return the two diagonals of an X centred on a point, a fixed size in DIP."""
    arm = ORIGIN_ARM_PX
    return [
        Primitive("line", role, x=x - arm, y=y - arm, x2=x + arm, y2=y + arm),
        Primitive("line", role, x=x - arm, y=y + arm, x2=x + arm, y2=y - arm),
    ]


def _kicad_positive(detail: PartDetail) -> str | None:
    """Return the KiCad pad number carrying the positive terminal, from the schematic."""
    for pad in named_pads(detail.kicad_pads):
        if terminal_of(pad) in ("anode", "positive"):
            return pad.number
    return None


def _jlc_positive(detail: PartDetail) -> str | None:
    """Return the JLC pad number the drawings mark positive, when they mark one."""
    marks = detail.marks
    return None if marks is None else marks.positive_pad


def _labels(detail: PartDetail, placement: Placement | None) -> list:
    """Return the canvas's corner labels: both angles, and the side when it is the bottom."""
    labels = [f"KiCad raw {detail.placed_rotation:g}°"]
    stored = detail.stored
    if stored is not None and stored.override_rotation is not None:
        labels.append(f"JLC {stored.override_rotation:+d}° override")
    elif placement is None or detail.verdict is None or detail.verdict.rotation is None:
        labels.append("JLC no fit")
    else:
        labels.append(f"JLC {detail.verdict.rotation:+d}° derived")
    if detail.is_bottom:
        labels.append("bottom, mirrored")
    return labels
