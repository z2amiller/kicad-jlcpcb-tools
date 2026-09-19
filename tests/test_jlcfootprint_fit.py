"""Tests for jlcfootprint.fit: group grading, pairing by pin function, the courtyard overhang."""

import pytest

from jlcfootprint.easyeda_parse import SymbolPin
from jlcfootprint.fit import (
    Placement,
    align,
    courtyard_excess,
    group_fit,
    pad_fit,
    pair_by_function,
    transformed_box,
)
from jlcfootprint.geometry import Pad

# KiCad's TO-252-3_TabPin2: the tab and the stub of the cut lead are both pad 2.
TABPIN2 = [
    Pad("1", -5.04, -2.28, 2.2, 1.2, 0.0, "G"),
    Pad("2", -5.04, 0.0, 2.2, 1.2, 0.0, "D"),
    Pad("2", 1.26, 0.0, 6.4, 5.8, 0.0, "D"),
    Pad("3", -5.04, 2.28, 2.2, 1.2, 0.0, "S"),
]
# JLC's TO-252-2 drawing numbers the tab 3 and the leads 1 and 2 (already placed on TABPIN2).
JLC_DPAK = [
    Pad("3", 1.26, 0.0, 6.0, 6.5),
    Pad("1", -5.04, -2.28, 1.6, 3.0),
    Pad("2", -5.04, 2.28, 1.6, 3.0),
]
DPAK_PINS = [SymbolPin("3", "D"), SymbolPin("1", "G"), SymbolPin("2", "S")]


def identity():
    """Return a placement that moves nothing."""
    return Placement(0, 0.0, 0.0, False, False, 0.0, 0.0, 3)


def geom(pad):
    """Return a pad's box as the fit functions take it."""
    return (pad.x, pad.y, pad.width, pad.height)


def test_group_fit_grades_a_jlc_pad_against_the_union_of_its_group():
    """A tab lands on the KiCad tab of its group; the stub adds nothing but takes nothing away."""
    tab, stub = TABPIN2[2], TABPIN2[1]
    grade, overlap = group_fit([geom(stub), geom(tab)], JLC_DPAK[0])
    assert grade == 2 and overlap > 0.85
    assert group_fit([geom(tab)], JLC_DPAK[0]) == pad_fit(geom(tab), JLC_DPAK[0])


def test_group_fit_sums_the_copper_of_adjacent_members():
    """A JLC pad straddling two KiCad pads of one number overlaps their sum but is centred on neither."""
    members = [(-1.0, 0.0, 2.0, 1.0), (1.0, 0.0, 2.0, 1.0)]
    straddling = Pad("2", 0.0, 0.0, 3.0, 1.0)
    grade, overlap = group_fit(members, straddling)
    assert grade == 1
    assert overlap == pytest.approx(1.0)
    assert group_fit([], straddling) == (0, 0.0)


def test_pair_by_function_pairs_unique_names_and_reports_the_number_differences():
    """G, D and S pair across the two numberings; the tab meets the tab by closest area."""
    pairing = pair_by_function(TABPIN2, JLC_DPAK, DPAK_PINS)
    assert pairing is not None
    assert set(pairing.kicad) == {"1", "2", "3"}
    assert pairing.kicad["3"] is TABPIN2[2]  # JLC pin 3 (D) is the KiCad tab
    assert pairing.kicad["2"] is TABPIN2[3]  # JLC pin 2 (S) is KiCad pad 3
    assert pairing.jlc["3"] is JLC_DPAK[0]
    assert pairing.leftovers == []
    assert sorted(pairing.differences) == [("D", "2", "3"), ("S", "3", "2")]
    assert pairing.eliminated is None


def test_pair_by_function_pairs_the_last_pad_by_elimination():
    """IN on the gate pad matches no JLC name; with D and S paired it pairs with the one pad left."""
    renamed = [TABPIN2[0]._replace(pin_function="IN")] + TABPIN2[1:]
    pairing = pair_by_function(renamed, JLC_DPAK, DPAK_PINS)
    assert pairing is not None
    assert pairing.eliminated == ("1", "1")
    assert pairing.kicad["1"] is renamed[0]


def test_pair_by_function_needs_two_names_and_every_jlc_pad_paired():
    """One matching name, or a JLC pad nothing pairs with, gives no pairing; numeric labels count for nothing."""
    only_d = (
        [TABPIN2[0]._replace(pin_function="")]
        + TABPIN2[1:3]
        + [TABPIN2[3]._replace(pin_function="")]
    )
    assert pair_by_function(only_d, JLC_DPAK, DPAK_PINS) is None
    extra = JLC_DPAK + [Pad("4", 0.0, 4.0, 1.0, 1.0)]
    assert pair_by_function(TABPIN2, extra, DPAK_PINS) is None
    numbered = [SymbolPin("1", "1"), SymbolPin("2", "2"), SymbolPin("3", "3")]
    assert pair_by_function(TABPIN2, JLC_DPAK, numbered) is None
    twice = DPAK_PINS + [SymbolPin("4", "D")]
    assert (
        pair_by_function(TABPIN2, JLC_DPAK + [Pad("4", 0.0, 4.0, 1.0, 1.0)], twice)
        is None
    )


def test_pair_by_function_ignores_no_connection_names():
    """NC on both sides names no pin: the two pads pair by elimination, never as a name match."""
    kicad = [TABPIN2[0]._replace(pin_function="NC")] + TABPIN2[1:]
    pins = [SymbolPin("3", "D"), SymbolPin("1", "NC"), SymbolPin("2", "S")]
    pairing = pair_by_function(kicad, JLC_DPAK, pins)
    assert pairing is not None
    assert pairing.eliminated == ("1", "1")
    assert sorted(pairing.differences) == [("D", "2", "3"), ("S", "3", "2")]


def test_an_underdetermined_solve_reports_no_angular_error():
    """A non-finite pad coordinate leaves the placement's angular RMS at zero, not NaN.

    ``solve_transform`` answers "underdetermined" for a NaN coordinate instead of
    raising, and the resolver copies the placement into the verdict before it
    refuses on that flag, so a NaN here would reach the stored ``angular_rms``
    column and the detail dialog.  The quality assessment ``align`` used to call
    guarded exactly this case; ``angular_rms`` on its own cannot see the flag.
    """
    nan = float("nan")
    kicad = {"1": Pad("1", nan, 0.0, 1.0, 1.0), "2": Pad("2", 1.0, 0.0, 1.0, 1.0)}
    jlc = {"1": Pad("1", 0.0, 0.0, 1.0, 1.0), "2": Pad("2", 1.0, 0.0, 1.0, 1.0)}
    placement = align(kicad, jlc)
    assert placement.is_underdetermined
    assert placement.angular_rms == 0.0


def test_transformed_box_rotates_and_moves_the_corners():
    """A quarter turn swaps the box's extents; the offset moves it."""
    placement = Placement(90, 10.0, 0.0, False, False, 0.0, 0.0, 2)
    assert transformed_box((-1.0, -2.0, 1.0, 2.0), placement) == pytest.approx(
        (8.0, -1.0, 12.0, 1.0)
    )
    assert transformed_box((-1.0, -2.0, 1.0, 2.0), identity()) == (-1.0, -2.0, 1.0, 2.0)


def test_courtyard_excess_is_the_largest_overhang():
    """A body inside the placed courtyard overhangs nothing; one poking out reports the largest side."""
    courtyard = (-3.35, -2.4, 3.35, 2.4)
    assert courtyard_excess(courtyard, (-3.0, -2.0, 3.0, 2.0), identity()) == 0.0
    assert courtyard_excess(
        courtyard, (-3.3, -3.3, 3.3, 3.3), identity()
    ) == pytest.approx(0.9)
    turned = Placement(90, 0.0, 0.0, False, False, 0.0, 0.0, 2)
    assert courtyard_excess(courtyard, (-3.3, -3.3, 3.3, 3.3), turned) == pytest.approx(
        0.9
    )
