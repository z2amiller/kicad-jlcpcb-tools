"""Tests for spec 19.8: a ``+`` beside the pads of a large part, under the absolute bar floor."""

import pytest

from jlcfootprint.drawing import (
    drawing_marks,
    footprint_bar_range,
    footprint_bars,
    footprint_pads,
    footprint_positive_pad,
)
from jlcfootprint.easyeda_parse import (
    DeviceHit,
    assemble_record,
    parse_puuid_response,
    parse_symbol_response,
)
from jlcfootprint.geometry import Pad
from jlcfootprint.resolving import resolve_record

from .jlcfootprint_support import recorded_document
from .test_jlcfootprint_drawing import TWO_PADS, plus_at, pro_pad

# A CR123A holder's pads in a Pro drawing (mils): 1,481.88 mil (37.6 mm) apart, so
# 5 % of the spacing is 74.1 mil (1.88 mm).
LARGE_PRO_PADS = [pro_pad("1", -740.94, 0), pro_pad("2", 740.94, 0)]
# The same pads in a classic drawing (10-mil canvas units, origin 4000/3000).
LARGE_CLASSIC_PADS = [
    "PAD~ELLIPSE~3925.906~3000~7.0866~7.0866~11~~1~1.1811~~0~gge1~0~~Y~0~0~0~",
    "PAD~ELLIPSE~4074.094~3000~7.0866~7.0866~11~~2~1.1811~~0~gge2~0~~Y~0~0~0~",
]

# A CR123A holder on a real board (MYOUNG BH-123A, C5290177), placed on the bottom
# side: its KiCad pads as the resolver receives them (mirrored the way
# scripts/validate_board.py mirrors a bottom-side part, pin functions as KiCad
# stores them for Device:Battery), and the Pro host's uuids as the plugin's batch
# lookup returned them.
CR123A_FOOTPRINT = "LowPower:BatteryHolder_MYOUNG_BH-123A-A1CJ002_1xCR123A"
CR123A_PADS = [
    Pad("1", 0.0, 0.0, 3.0, 3.0, 0.0, "+_1", "roundrect"),
    Pad("2", 37.5, 0.0, 3.0, 3.0, 0.0, "-_2", "circle"),
]
CR123A_COURTYARD = (-3.15, -9.65, 40.65, 9.35)
CR123A_HIT = DeviceHit(
    "C5290177",
    "8dbed34387a44c64b8dc17b81b021710",
    "daade0089f9e4abfb5edd463ca341595",
    "BAT-TH_BH-123A-A5BJ002",
)


def classic_plus_at(x, y, arm):
    """Return a classic silk + of two TRACK strokes, each ``2 * arm`` long, centred on (x, y)."""
    return [
        f"TRACK~0.6~3~~{x - arm} {y} {x + arm} {y}~ggh~0",
        f"TRACK~0.6~3~~{x} {y - arm} {x} {y + arm}~ggv~0",
    ]


def cr123a_record():
    """Assemble C5290177 from its recorded Pro footprint and symbol, as the cache holds it."""
    footprint = parse_puuid_response(
        recorded_document("footprint", CR123A_HIT.puuid), CR123A_HIT.puuid
    )
    symbol = parse_symbol_response(
        recorded_document("symbol", CR123A_HIT.symbol_uuid), CR123A_HIT.symbol_uuid
    )
    return assemble_record(CR123A_HIT.lcsc, CR123A_HIT, footprint, symbol)


def test_a_plus_beside_a_large_part_is_read_below_five_percent_of_the_spacing():
    """A 1.6 mm + beside pads 37.6 mm apart (4.3 % of the spacing) names its pad (spec 19.8)."""
    assert footprint_positive_pad(LARGE_PRO_PADS + plus_at(13, -740, -77, 31.5)) == "1"
    assert footprint_positive_pad(LARGE_PRO_PADS + plus_at(13, 740, -77, 31.5)) == "2"


def test_a_cross_well_below_the_absolute_floor_is_not_read_on_the_same_pads():
    """A 0.4 mm cross beside the same pads stays too short to be a mark (spec 19.8)."""
    tiny = plus_at(13, -740, -77, 8.0, thick=3.0)
    assert footprint_positive_pad(LARGE_PRO_PADS + tiny) is None


@pytest.mark.parametrize(
    ("arm", "expected"),
    [(15.0, "1"), (14.5, None)],
    ids=["30_mil_read", "29_mil_not_read"],
)
def test_the_absolute_floor_is_three_quarters_of_a_millimetre(arm, expected):
    """The floor is 0.75 mm (29.5 mil): a 30-mil + is read on a large part, a 29-mil one is not."""
    plus = plus_at(13, -740, -77, arm)
    assert footprint_positive_pad(LARGE_PRO_PADS + plus) == expected


@pytest.mark.parametrize(
    ("arm", "expected"),
    [(5.0, "1"), (3.5, None)],
    ids=["six_percent_read", "four_percent_not_read"],
)
def test_a_small_part_keeps_the_five_percent_floor(arm, expected):
    """On pads 160 mil apart 5 % (8 mil) is below the absolute floor and stays the bound."""
    plus = plus_at(13, -60, 0, arm, thick=2.0)
    assert footprint_positive_pad(TWO_PADS + plus) == expected


@pytest.mark.parametrize(
    ("arm", "expected"),
    [(3.15, "1"), (0.8, None), (1.5, "1"), (1.45, None)],
    ids=["1.6_mm_read", "0.4_mm_not_read", "30_mil_read", "29_mil_not_read"],
)
def test_a_classic_drawing_takes_the_floor_in_its_own_units(arm, expected):
    """Classic canvas units are 10 mil: the same marks on the same pads read the same way."""
    plus = classic_plus_at(3926, 2992.3, arm)
    assert footprint_positive_pad(LARGE_CLASSIC_PADS + plus) == expected


def test_the_bar_range_converts_the_floor_per_format():
    """0.75 mm is 29.53 mil in a Pro drawing and 2.953 canvas units in a classic one."""
    low, high = footprint_bar_range(LARGE_PRO_PADS, 1481.88)
    assert low * 1481.88 == pytest.approx(0.75 / 0.0254)
    assert high == 0.70
    low, high = footprint_bar_range(LARGE_CLASSIC_PADS, 148.188)
    assert low * 148.188 == pytest.approx(0.75 / 0.254)
    assert high == 0.70
    assert footprint_bar_range(TWO_PADS, 160.0) == (0.05, 0.70)
    assert footprint_bar_range(TWO_PADS, 0.0) == (0.05, 0.70)


def test_a_cr123a_holder_reads_its_footprint_plus_and_resolves_green():
    """A real CR123A holder's 63-mil + on 1,482-mil pads is read: green, 0 degrees (spec 19.8).

    Its Pro footprint draws the + beside pad 1 as two filled bars of 63 mil, 4.3 %
    of the pad spacing, which the 5 % bound alone dropped: the part then read
    "polarity unknown" under decision 4 of spec 19.2.  The raw angle was right.
    """
    record = cr123a_record()
    pads = footprint_pads(record.footprint_shapes)
    assert [number for number, _x, _y in pads] == ["1", "2"]
    assert pads[1][1] - pads[0][1] == pytest.approx(1481.88)
    plus = [
        bar
        for bar in footprint_bars(record.footprint_shapes)
        if bar.layer == "document" and abs(bar.midpoint[0] + 740.0) < 2.0
    ]
    assert sorted(round(bar.length, 3) for bar in plus) == [62.992, 62.992]
    marks = drawing_marks(
        record.symbol_shapes, record.footprint_shapes, record.footprint_origin
    )
    assert (marks.positive_pin, marks.positive_pad) == (None, "1")
    verdict = resolve_record(
        CR123A_PADS, CR123A_FOOTPRINT, record, "symbol", CR123A_COURTYARD
    )
    assert (verdict.status, verdict.rotation, verdict.method) == (
        "green",
        0,
        "polarity",
    )
    assert verdict.confidence == "medium"
    assert verdict.fit == "fits_larger_pads"
    assert verdict.overlap_min == pytest.approx(1.0)
    assert verdict.polarity_source == "drawing"
    assert verdict.polarity_light == "green"
    assert verdict.notes == ["polarity from the footprint's + mark"]
