"""Tests for jlcfootprint.drawing: the + marks of symbols and footprints, and the body box."""

import json

import pytest

from jlcfootprint.drawing import (
    DrawingMarks,
    Segment,
    drawing_marks,
    footprint_bars,
    footprint_body_box,
    footprint_edges,
    footprint_pads,
    footprint_positive_pad,
    is_pro,
    plus_marks,
    symbol_bars,
    symbol_edges,
    symbol_pins,
    symbol_positive_pin,
)
from tests.jlcfootprint_support import pro_record, recorded


def pro_pad(number, x, y, w=59.0, h=53.0):
    """Return one Pro PAD record line in mils."""
    return json.dumps(
        [
            "PAD",
            f"p{number}",
            0,
            "",
            1,
            number,
            x,
            y,
            0,
            None,
            ["RECT", w, h, 0],
            [],
            0,
            0,
            0,
            1,
            0,
        ]
    )


def pro_fill(layer, x1, y1, x2, y2, ident="f"):
    """Return one filled rectangle on ``layer`` as a Pro FILL record line."""
    return json.dumps(
        [
            "FILL",
            ident,
            0,
            "",
            layer,
            0.2,
            0,
            [[x1, y1, "L", x1, y2, x2, y2, x2, y1, x1, y1]],
            0,
        ]
    )


def pro_line(layer, x1, y1, x2, y2, ident="l"):
    """Return one stroke on ``layer`` as a Pro POLY record line."""
    return json.dumps(["POLY", ident, 0, "", layer, 6, [x1, y1, "L", x2, y2], 0])


def plus_at(layer, x, y, arm=15.0, thick=8.0, ident="p"):
    """Return a + of two filled bars centred on (x, y)."""
    return [
        pro_fill(layer, x - arm, y - thick / 2, x + arm, y + thick / 2, ident + "h"),
        pro_fill(layer, x - thick / 2, y - arm, x + thick / 2, y + arm, ident + "v"),
    ]


TWO_PADS = [pro_pad("1", -80, 0), pro_pad("2", 80, 0)]


def test_format_detection():
    """Pro drawings are JSON array lines; anything else reads as classic shapes."""
    assert is_pro(['["DOCTYPE","FOOTPRINT","1.8"]'])
    assert is_pro([["PAD", "e1"]])
    assert not is_pro(["PAD~RECT~1~2~3~4~1~~1~0~~0~gge1"])
    assert not is_pro([])


def test_pro_symbol_plus_names_the_pin_beside_it():
    """C7175's Pro symbol draws its + as two short strokes beside pin 1."""
    record = pro_record("C7175")
    assert symbol_pins(record.symbol_shapes) == [("1", 15.0, 0.0), ("2", -15.0, 0.0)]
    assert symbol_positive_pin(record.symbol_shapes) == "1"


def test_pro_symbol_plus_drawn_as_thin_rectangles():
    """C140418's symbol draws its + as two thin RECT records; the plates and arcs are not bars of a +."""
    record = pro_record("C140418")
    assert symbol_positive_pin(record.symbol_shapes) == "1"


def test_classic_symbol_plus_names_the_pin_beside_it():
    """The classic C16133 symbol draws the + with two PL~ strokes beside pin 1."""
    record = recorded("C16133")
    assert symbol_pins(record.symbol_shapes) == [("1", 30.0, 0.0), ("2", 0.0, 0.0)]
    assert symbol_positive_pin(record.symbol_shapes) == "1"


def test_led_and_transistor_symbols_have_no_plus():
    """An LED's arrows and cathode bar and a transistor's pins name no positive pin."""
    assert symbol_positive_pin(recorded("C2286").symbol_shapes) is None
    assert symbol_positive_pin(pro_record("C2286").symbol_shapes) is None
    assert symbol_positive_pin(pro_record("C2132").symbol_shapes) is None


def test_symbol_plus_must_be_clearly_nearer_one_pin():
    """A + midway between the pins names nothing; two + on different pins name nothing."""
    pins = [
        '["PIN","a",1,null,-15,0,10,0,null,0,0,1]',
        '["ATTR","a1","a","NUMBER","1"]',
        '["PIN","b",1,null,15,0,10,180,null,0,0,1]',
        '["ATTR","b1","b","NUMBER","2"]',
    ]
    midway = ['["POLY","h",[-2,0,2,0],0,"st1",0]', '["POLY","v",[0,-2,0,2],0,"st1",0]']
    assert symbol_positive_pin(pins + midway) is None
    near_one = [
        '["POLY","h",[-12,-4,-8,-4],0,"st1",0]',
        '["POLY","v",[-10,-6,-10,-2],0,"st1",0]',
    ]
    near_two = [
        '["POLY","h2",[8,-4,12,-4],0,"st1",0]',
        '["POLY","v2",[10,-6,10,-2],0,"st1",0]',
    ]
    assert symbol_positive_pin(pins + near_one) == "1"
    assert symbol_positive_pin(pins + near_one + near_two) is None
    assert symbol_positive_pin(pins) is None
    assert symbol_positive_pin(near_one) is None  # no pins at all


def test_pro_footprint_plus_beside_the_pad():
    """C7175 draws + on the document layer beside pad 1; C140418 beside pad 1 on the right."""
    assert footprint_positive_pad(pro_record("C7175").footprint_shapes) == "1"
    assert footprint_positive_pad(pro_record("C140418").footprint_shapes) == "1"
    assert footprint_pads(pro_record("C140418").footprint_shapes) == [
        ("2", -119.0, 0.0),
        ("1", 119.0, 0.0),
    ]


def test_diode_footprint_plus_marks_the_anode_and_its_silk_symbol_is_ignored():
    """C2480's silk diode symbol crosses at its bar but the arrow touches the crossing; the document + names pad 2."""
    shapes = pro_record("C2480").footprint_shapes
    assert footprint_positive_pad(shapes) == "2"
    silk = [bar for bar in footprint_bars(shapes) if bar.layer == "silk"]
    assert plus_marks(silk, 154.7, (0.05, 0.7)) == []


def test_classic_footprint_plus_with_strokes_and_run_together_path_tokens():
    """C7171 draws its silk + with TRACK strokes; C50494's document + path writes ``L3996,3002``."""
    assert footprint_positive_pad(recorded("C7171").footprint_shapes) == "1"
    assert footprint_positive_pad(recorded("C50494").footprint_shapes) == "1"
    assert footprint_positive_pad(recorded("C88744").footprint_shapes) == "1"


def test_footprints_without_a_plus():
    """Transistors, resistors and a plain 0603 capacitor name no pad; so does a three-pad drawing."""
    assert footprint_positive_pad(pro_record("C2132").footprint_shapes) is None
    assert footprint_positive_pad(pro_record("C25804").footprint_shapes) is None
    assert footprint_positive_pad(pro_record("C14663").footprint_shapes) is None
    assert footprint_positive_pad(pro_record("C42441713").footprint_shapes) is None


def test_a_plus_near_the_centre_is_ignored_and_conflicting_sides_name_nothing():
    """An origin cross names no pad; marks on both sides cancel; two marks on one side agree."""
    assert footprint_positive_pad(TWO_PADS + plus_at(13, 0, 0)) is None
    assert (
        footprint_positive_pad(TWO_PADS + plus_at(13, 0, 0) + plus_at(3, -60, 0)) == "1"
    )
    assert (
        footprint_positive_pad(
            TWO_PADS + plus_at(13, -60, 0) + plus_at(3, 60, 0, ident="q")
        )
        is None
    )
    assert (
        footprint_positive_pad(
            TWO_PADS + plus_at(13, 60, 0) + plus_at(3, 60, -20, ident="q")
        )
        == "2"
    )


def test_outline_corners_t_junctions_and_lone_bars_are_not_a_plus():
    """Bars meeting at their ends (an outline corner), a T and a stripe alone name nothing."""
    corner = [pro_line(3, -100, -60, 100, -60), pro_line(3, 100, -60, 100, 60)]
    assert footprint_positive_pad(TWO_PADS + corner) is None
    tee = [pro_fill(13, -80, -4, -40, 4), pro_fill(13, -84, -30, -76, 30)]
    assert footprint_positive_pad(TWO_PADS + tee) is None
    stripe = [pro_fill(13, -64, -30, -56, 30)]
    assert footprint_positive_pad(TWO_PADS + stripe) is None


def test_a_polyline_through_the_crossing_rejects_it():
    """A diode's silk arrow drawn as one polyline ends on its bar where the lead crosses it.

    The classic C2480 silk shows that crossing (the bar and the lead alone form a
    geometric +, 6 % off centre); the arrow's edges reject it.  A polygon beside a
    bar's end, as pad outlines are drawn on the document layer, rejects nothing.
    """
    shapes = recorded("C2480").footprint_shapes
    silk = [bar for bar in footprint_bars(shapes) if bar.layer == "silk"]
    silk_edges = [edge for edge in footprint_edges(shapes) if edge.layer == "silk"]
    assert len(plus_marks(silk, 15.47, (0.05, 0.7))) == 1
    assert plus_marks(silk, 15.47, (0.05, 0.7), silk_edges) == []
    assert footprint_positive_pad(shapes) == "2"
    arrow = ['["POLY","arrow",0,"",13,6,[-45,-15,"L",-60,0,-45,15,-45,-15],0]']
    assert footprint_positive_pad(TWO_PADS + plus_at(13, -60, 0) + arrow) is None
    beside = [
        '["POLY","pad1",0,"",13,6,[-70,-25,"L",-50,-25,-50,-16,-70,-16,-70,-25],0]'
    ]
    assert footprint_positive_pad(TWO_PADS + plus_at(13, -60, 0) + beside) == "1"
    triangle = pro_record("C2480").symbol_shapes
    assert any(not (e.horizontal or e.vertical) for e in symbol_edges(triangle))


def test_edges_skip_arcs_and_circles():
    """An arc breaks the edge chain and a circle gives no edge; a wide filled region gives its sides, not a bar."""
    arc = ['["POLY","e9",0,"",3,15.7,[-134,-68.7,"ARC",359.27,-134,-68.8],0]']
    assert footprint_edges(arc) == []
    circle = ['["POLY","e2",0,"",13,15.7,["CIRCLE",-90,-125,7.87],0]']
    assert footprint_edges(circle) == []
    square = ['["FILL","f",0,"",13,0.2,0,[[0,0,"L",10,0,10,10,0,10,0,0]],0]']
    assert len(footprint_edges(square)) == 4 and footprint_bars(square) == []
    classic = [
        "TRACK~1~3~~0 0 10 0 10 10~gge1~0",
        "SOLIDREGION~12~~M 0 0 L 10 0 L 10 10 Z ~solid~gge2~~~~0",
    ]
    assert [
        len([e for e in footprint_edges(classic) if e.layer == layer])
        for layer in ("silk", "document")
    ] == [2, 2]


def test_a_plus_must_stand_alone_and_its_bars_must_be_alike():
    """A third bar through the crossing rejects it; a long bar with a short one is no +."""
    crowded = plus_at(13, -60, 0) + [pro_line(13, -75, -15, -45, 15, "diag")]
    assert footprint_positive_pad(TWO_PADS + crowded) is None
    unlike = [pro_fill(13, -100, -4, -20, 4), pro_fill(13, -64, -8, -56, 8)]
    assert footprint_positive_pad(TWO_PADS + unlike) is None


def test_vertical_pad_axis():
    """With the pads drawn vertical the + is judged along that axis."""
    pads = [pro_pad("1", 0, 80), pro_pad("2", 0, -80)]
    assert footprint_positive_pad(pads + plus_at(13, 0, 60)) == "1"
    assert footprint_positive_pad(pads + plus_at(13, 0, -60)) == "2"
    assert footprint_positive_pad(pads + plus_at(13, 40, 0)) is None


def test_body_box_pro_and_classic_agree():
    """C88744's layer-48 octagon is 6.6 mm square about the origin in both formats (mm, Y down)."""
    pro = footprint_body_box(pro_record("C88744").footprint_shapes)
    classic_record = recorded("C88744")
    classic = footprint_body_box(
        classic_record.footprint_shapes, classic_record.footprint_origin
    )
    assert pro == pytest.approx((-3.3, -3.3, 3.3, 3.3), abs=0.01)
    assert classic == pytest.approx((-3.3, -3.3, 3.3, 3.3), abs=0.01)
    tab = footprint_body_box(pro_record("C42441713").footprint_shapes)
    assert tab == pytest.approx((-3.3, -4.92, 3.3, 1.13), abs=0.01)


def test_body_box_absent_or_curved():
    """No layer-48 record, or one drawn with arcs, gives no box; malformed lines are skipped."""
    assert footprint_body_box(TWO_PADS) is None
    arc = ['["POLY","e4",0,"",48,2,[-10,-10,"L",10,-10,"A",10,10,0,0,1,-10,10],0]']
    assert footprint_body_box(arc) is None
    assert (
        footprint_body_box(
            ['["POLY"', "not json", '["FILL","x",0,"",48,0.2,0,"points?",0]']
        )
        is None
    )
    assert footprint_positive_pad(['{"a":1}', "PAD~broken"]) is None


def test_drawing_marks_reads_both_drawings():
    """The combined reading carries the pin, the pad and the body box."""
    record = pro_record("C7175")
    marks = drawing_marks(record.symbol_shapes, record.footprint_shapes)
    assert marks == DrawingMarks(
        positive_pin="1",
        positive_pad="1",
        body_box=pytest.approx((-1.6, -0.8, 1.6, 0.8), abs=0.01),
    )
    assert drawing_marks([], []) == DrawingMarks(None, None, None)


def test_segment_helpers_and_symbol_bars():
    """Segment properties, and the classic R~ rectangle read as a bar."""
    bar = Segment(0.0, -2.0, 0.0, 2.0, "symbol")
    assert bar.vertical and not bar.horizontal and bar.length == 4.0
    assert bar.midpoint == (0.0, 0.0)
    rect = ["R~10~20~0~0~0.2~4~#880000~1~0~none~r1~0"]
    (segment,) = symbol_bars(rect)
    assert segment == Segment(10.1, 20.0, 10.1, 24.0, "symbol")
