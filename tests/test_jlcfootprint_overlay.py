"""Tests for the detail dialog's pad overlay: scale, grid, marks and labels (spec 16.4), no wx."""

import pytest

from jlcfootprint.controller import PartDetail
from jlcfootprint.drawing import DrawingMarks
from jlcfootprint.easyeda_parse import SymbolPin
from jlcfootprint.fit import Placement, align, pair_by_name
from jlcfootprint.geometry import Pad
from jlcfootprint.overlay import (
    JLC_PLACED,
    JLC_RAW,
    KICAD,
    MARGIN_FRACTION,
    grid_mm,
    inverse,
    overlay,
    placed,
)
from jlcfootprint.resolver import resolve
from jlcfootprint.verdicts import StoredVerdict

CANVAS = (420, 300)
KICAD_0805 = [
    Pad("1", -1.0, 0.0, 1.2, 1.4, 0.0, "+"),
    Pad("2", 1.0, 0.0, 1.2, 1.4, 0.0, "-"),
]
JLC_0805 = [Pad("1", -1.0, 0.0, 1.3, 1.5), Pad("2", 1.0, 0.0, 1.3, 1.5)]
SOT23_KICAD = [
    Pad("1", -0.95, 1.1, 0.6, 1.0, 0.0, "B"),
    Pad("2", 0.95, 1.1, 0.6, 1.0, 0.0, "E"),
    Pad("3", 0.0, -1.1, 0.6, 1.0, 0.0, "C"),
]


def detail(**fields):
    """Return a part detail with the 0805 pads unless the test says otherwise."""
    fields.setdefault("kicad_pads", list(KICAD_0805))
    fields.setdefault("jlc_pads", list(JLC_0805))
    fields.setdefault("kicad_footprint", "Capacitor_SMD:C_0805_2012Metric")
    return PartDetail("C1", fields.pop("lcsc", "C7192"), **fields)


def polarized(marks=None, **fields):
    """Return a detail whose verdict comes from the resolver, so it carries a placement."""
    marks = marks or DrawingMarks(positive_pad="1")
    verdict = resolve(
        KICAD_0805,
        "Capacitor_SMD:C_0805_2012Metric",
        "ok",
        "CAP-SMD_L2.0-W1.3-FD",
        JLC_0805,
        [SymbolPin("1", "1"), SymbolPin("2", "2")],
        marks=marks,
    )
    return detail(verdict=verdict, marks=marks, **fields)


def turned(**fields):
    """Return a detail whose JLC drawing is turned 90 degrees from the footprint.

    The raw and the transformed JLC pads then sit in visibly different places, which
    is what makes a rescale on a checkbox toggle visible.
    """
    jlc = [
        Pad("1", 1.1, 0.95, 1.0, 0.6),
        Pad("2", 1.1, -0.95, 1.0, 0.6),
        Pad("3", -1.1, 0.0, 1.0, 0.6),
    ]
    verdict = resolve(
        SOT23_KICAD,
        "Package_TO_SOT_SMD:SOT-23",
        "ok",
        "SOT-23_L2.9-W1.3-P1.90-LS2.4-BR",
        jlc,
        [SymbolPin("1", "B"), SymbolPin("2", "E"), SymbolPin("3", "C")],
    )
    return detail(
        kicad_pads=list(SOT23_KICAD),
        jlc_pads=jlc,
        kicad_footprint="Package_TO_SOT_SMD:SOT-23",
        verdict=verdict,
        **fields,
    )


def test_the_scale_fits_both_pad_sets_with_a_margin_and_centres_them():
    """Spec 16.4: both pad sets plus a 15 % margin, centred on the canvas."""
    drawing = overlay(polarized(), CANVAS)
    x1, y1, x2, y2 = drawing.bounds_mm
    assert (round(x1, 3), round(x2, 3)) == (-1.65, 1.65)  # the wider JLC pads
    span = (x2 - x1) * (1.0 + 2 * MARGIN_FRACTION)
    assert drawing.scale_px_per_mm == pytest.approx(
        min(420 / span, 300 / ((y2 - y1) * (1.0 + 2 * MARGIN_FRACTION)))
    )
    pads = drawing.by_role("kicad_pad")
    centre_x = sum(pad.x + pad.width / 2.0 for pad in pads) / len(pads)
    assert centre_x == pytest.approx(210.0, abs=0.5)
    # Every pad stays inside the canvas, margin included.
    for pad in drawing.primitives:
        if pad.kind != "rect":
            continue
        assert pad.x >= 0 and pad.x + pad.width <= 420
        assert pad.y >= 0 and pad.y + pad.height <= 300


def test_a_larger_part_scales_down_and_takes_a_larger_grid():
    """An 0201 and a 40-pin DIP both fit; the grid and the scale bar follow the scale."""
    small = overlay(
        detail(
            kicad_pads=[Pad("1", -0.3, 0.0, 0.3, 0.3), Pad("2", 0.3, 0.0, 0.3, 0.3)],
            jlc_pads=[],
        ),
        CANVAS,
        (KICAD,),
    )
    big = overlay(
        detail(
            kicad_pads=[
                Pad(
                    str(index + 1),
                    -7.62 if index < 20 else 7.62,
                    -11.43 + 2.54 * (index % 20),
                    1.6,
                    1.6,
                )
                for index in range(40)
            ],
            jlc_pads=[],
        ),
        CANVAS,
        (KICAD,),
    )
    assert small.scale_px_per_mm > big.scale_px_per_mm
    assert small.grid_mm == 0.5
    assert big.grid_mm == 20.0  # a 48 mm package: a quarter of the canvas is ~23 mm
    for drawing in (small, big):
        bar = drawing.by_role("scale_bar")[0]
        assert bar.x2 - bar.x == pytest.approx(
            drawing.grid_mm * drawing.scale_px_per_mm
        )
        assert drawing.by_role("scale_label")[0].text == f"{drawing.grid_mm:g} mm"
        # The bar spans about a quarter of the canvas.
        assert 0.1 < (bar.x2 - bar.x) / 420 < 0.6


def test_the_grid_pitch_choices_are_the_round_lengths():
    """Only 0.5, 1, 2, 5, 10 and 20 mm are offered, whatever the scale."""
    assert {grid_mm(scale, 420.0) for scale in (5, 20, 50, 100, 200, 400, 1000)} <= {
        0.5,
        1.0,
        2.0,
        5.0,
        10.0,
        20.0,
    }
    assert grid_mm(1000.0, 420.0) == 0.5
    assert grid_mm(1.0, 420.0) == 20.0


def test_the_inverse_placement_puts_each_jlc_pad_on_its_kicad_partner():
    """The canvas draws in the footprint frame, so the placement is used backwards."""
    kicad, jlc, _rest = pair_by_name(
        SOT23_KICAD,
        [
            Pad("1", 1.1, 0.95, 0.6, 1.0),
            Pad("2", 1.1, -0.95, 0.6, 1.0),
            Pad("3", -1.1, 0.0, 0.6, 1.0),
        ],
    )
    placement = align(kicad, jlc)
    assert placement.rotation_deg in (90, 270)
    for key, pad in jlc.items():
        x, y, _w, _h = placed(pad, inverse(placement))
        partner = kicad[key]
        assert (x, y) == pytest.approx((partner.x, partner.y), abs=1e-6)


def test_pin_one_is_a_dot_on_kicad_s_pad_and_a_ring_on_jlc_s():
    """Both marks are drawn at a fixed size in DIP, whatever the scale."""
    drawing = overlay(polarized(), CANVAS)
    (dot,) = drawing.by_role("kicad_pin1")
    (ring,) = drawing.by_role("jlc_pin1")
    assert (dot.kind, ring.kind) == ("circle", "circle")
    assert dot.radius < ring.radius
    zoomed = overlay(polarized(), (840, 600))
    assert zoomed.by_role("kicad_pin1")[0].radius == dot.radius
    assert zoomed.scale_px_per_mm > drawing.scale_px_per_mm


def test_the_plus_marks_sit_on_both_positive_terminals_without_overlapping():
    """KiCad's + above its pad, JLC's below its own, so an agreement reads as two marks."""
    drawing = overlay(polarized(), CANVAS)
    kicad_plus = drawing.by_role("kicad_plus")
    jlc_plus = drawing.by_role("jlc_plus")
    assert len(kicad_plus) == len(jlc_plus) == 2  # two bars each
    assert {item.kind for item in kicad_plus + jlc_plus} == {"line"}
    assert kicad_plus[0].y < jlc_plus[0].y
    # Both name pad 1 here, so they share a column of the canvas.
    assert kicad_plus[0].x == pytest.approx(jlc_plus[0].x)
    # A non-polar part draws neither.
    plain = overlay(
        detail(
            kicad_pads=[Pad("1", -1.0, 0.0, 1.2, 1.4), Pad("2", 1.0, 0.0, 1.2, 1.4)],
            marks=DrawingMarks(),
        ),
        CANVAS,
        (KICAD,),
    )
    assert plain.by_role("kicad_plus") == [] and plain.by_role("jlc_plus") == []


def test_the_labels_say_both_angles_the_override_and_the_bottom_side():
    """Spec 16.4's corner labels, in each of their four shapes."""
    derived = overlay(polarized(placed_rotation=90.0), CANVAS)
    assert [item.text for item in derived.by_role("angle_label")] == [
        "KiCad raw 90°",
        "JLC +0° derived",
    ]
    overridden = overlay(
        polarized(
            placed_rotation=0.0,
            stored=StoredVerdict(
                "C1", "h", status="green", rotation=0, override_rotation=180
            ),
        ),
        CANVAS,
    )
    assert overridden.by_role("angle_label")[1].text == "JLC +180° override"
    nothing = overlay(detail(), CANVAS, (KICAD, JLC_PLACED))
    assert nothing.by_role("angle_label")[1].text == "JLC no fit"
    bottom = overlay(polarized(is_bottom=True), CANVAS)
    assert bottom.by_role("angle_label")[2].text == "bottom, mirrored"


def test_a_part_with_no_placement_draws_its_raw_jlc_pads_and_says_why():
    """Spec 16.4: unknown, red-count and pending parts draw what they have."""
    pending = overlay(detail(), CANVAS)
    assert pending.layers == (KICAD, JLC_RAW)
    assert (
        pending.note == "no placement: the JLC pads are drawn as the drawing has them"
    )
    assert len(pending.by_role("jlc_raw_pad")) == 2
    assert pending.by_role("jlc_pad") == []
    nothing_cached = overlay(detail(jlc_pads=[]), CANVAS)
    assert nothing_cached.layers == (KICAD,)
    assert nothing_cached.note == "no JLC pads cached for this part yet"
    assert len(nothing_cached.by_role("kicad_pad")) == 2


def test_the_layers_switch_what_is_drawn():
    """The dialog's three checkboxes; the transformed and the raw JLC pads can both show."""
    both = overlay(polarized(), CANVAS, (KICAD, JLC_PLACED, JLC_RAW))
    assert len(both.by_role("kicad_pad")) == 2
    assert len(both.by_role("jlc_pad")) == 2
    assert len(both.by_role("jlc_raw_pad")) == 2
    kicad_only = overlay(polarized(), CANVAS, (KICAD,))
    assert (
        kicad_only.by_role("jlc_pad") == [] and kicad_only.by_role("jlc_raw_pad") == []
    )
    jlc_only = overlay(polarized(), CANVAS, (JLC_PLACED,))
    assert jlc_only.by_role("kicad_pad") == [] and len(jlc_only.by_role("jlc_pad")) == 2
    assert (
        jlc_only.by_role("kicad_pin1") == [] and len(jlc_only.by_role("jlc_pin1")) == 1
    )


def _drawn(drawing, role):
    """Return one role's rectangles as comparable pixel tuples."""
    return {
        (
            item.number,
            round(item.x, 6),
            round(item.y, 6),
            round(item.width, 6),
            round(item.height, 6),
        )
        for item in drawing.by_role(role)
    }


def test_the_frame_does_not_move_when_a_layer_is_switched():
    """Spec 16.4 (amended 2026-09-18): the checkboxes change what is drawn, not the scale.

    The frame comes from every pad set the part has, so a pad that is drawn lands on
    exactly the same pixel whichever other layers are on, and the scale bar and the
    grid never change with a toggle.
    """
    part = turned()
    full = overlay(part, CANVAS, (KICAD, JLC_PLACED, JLC_RAW))
    # This part really does place its raw and its transformed JLC pads apart.
    assert _drawn(full, "jlc_pad") != _drawn(full, "jlc_raw_pad")
    reference_bar = full.by_role("scale_bar")[0]
    for layers in (
        (),
        (KICAD,),
        (JLC_PLACED,),
        (JLC_RAW,),
        (KICAD, JLC_PLACED),
        (KICAD, JLC_RAW),
        (JLC_PLACED, JLC_RAW),
        (KICAD, JLC_PLACED, JLC_RAW),
    ):
        drawing = overlay(part, CANVAS, layers)
        assert drawing.bounds_mm == full.bounds_mm
        assert drawing.scale_px_per_mm == full.scale_px_per_mm
        assert drawing.grid_mm == full.grid_mm
        bar = drawing.by_role("scale_bar")[0]
        assert (bar.x, bar.y, bar.x2, bar.y2) == (
            reference_bar.x,
            reference_bar.y,
            reference_bar.x2,
            reference_bar.y2,
        )
        assert drawing.by_role("scale_label")[0].text == f"{full.grid_mm:g} mm"
        for role, layer in (
            ("kicad_pad", KICAD),
            ("jlc_pad", JLC_PLACED),
            ("jlc_raw_pad", JLC_RAW),
        ):
            expected = _drawn(full, role) if layer in layers else set()
            assert _drawn(drawing, role) == expected, (role, layers)
    # A real resize still rescales.
    assert overlay(part, (840, 600)).scale_px_per_mm > full.scale_px_per_mm


def test_a_degenerate_or_empty_part_still_produces_a_drawing():
    """One pad, coincident pads or no pads at all must not divide by zero."""
    single = overlay(
        detail(kicad_pads=[Pad("1", 0.0, 0.0, 0.0, 0.0)], jlc_pads=[]), CANVAS
    )
    assert single.scale_px_per_mm > 0
    assert len(single.by_role("kicad_pad")) == 1
    empty = overlay(detail(kicad_pads=[], jlc_pads=[]), CANVAS)
    assert empty.bounds_mm == (-0.5, -0.5, 0.5, 0.5)
    assert empty.by_role("grid")
    tiny = overlay(detail(kicad_pads=[], jlc_pads=[]), (1, 1))
    assert tiny.scale_px_per_mm > 0


def test_the_grid_and_the_scale_bar_share_one_pitch():
    """Spec 16.4: the grid uses the scale bar's length."""
    drawing = overlay(polarized(), CANVAS)
    lines = drawing.by_role("grid")
    verticals = sorted(item.x for item in lines if item.x == item.x2)
    spacing = [round(b - a, 6) for a, b in zip(verticals, verticals[1:])]
    assert spacing and set(spacing) == {
        round(drawing.grid_mm * drawing.scale_px_per_mm, 6)
    }


def test_the_inverse_of_a_turned_placement_is_its_own_inverse():
    """inverse(inverse(p)) is p, so the canvas cannot drift with the angle."""
    for rotation in (0, 90, 180, 270):
        placement = Placement(rotation, 1.5, -2.5, False, False, 0.0, 0.0, 3)
        back = inverse(inverse(placement))
        assert back.rotation_deg == placement.rotation_deg
        assert (back.offset_x, back.offset_y) == pytest.approx(
            (placement.offset_x, placement.offset_y), abs=1e-9
        )
