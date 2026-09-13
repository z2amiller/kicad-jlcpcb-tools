"""Tests for jlcfootprint.geometry: units, frames, hashing and the CCW conversion."""

import pytest

from jlcfootprint.geometry import (
    Pad,
    ccw_correction,
    centroid,
    easyeda_pads_to_mm,
    mirror_y,
    pad_geom,
    pad_geoms,
    pad_hash,
    pad_points,
    signal_pads,
)


def test_easyeda_pads_to_mm_scales_and_keeps_y_down_by_default():
    """One canvas unit is 0.254 mm and Y is not flipped unless asked."""
    raw = [{"number": "1", "x": 10, "y": -20, "w": 4, "h": 2, "rotation": 90}]
    (pad,) = easyeda_pads_to_mm(raw)
    assert pad.number == "1"
    assert pad.x == pytest.approx(2.54)
    assert pad.y == pytest.approx(-5.08)
    assert pad.width == pytest.approx(1.016)
    assert pad.height == pytest.approx(0.508)
    assert pad.rotation == 90.0
    (flipped,) = easyeda_pads_to_mm(raw, flip_y=True)
    assert flipped.y == pytest.approx(5.08)


def test_easyeda_pads_to_mm_defaults_rotation_to_zero():
    """Raw pads without a rotation field get 0 degrees."""
    (pad,) = easyeda_pads_to_mm([{"number": "2", "x": 0, "y": 0, "w": 1, "h": 1}])
    assert pad.rotation == 0.0


def test_pad_hash_ignores_order_and_pin_function():
    """The hash covers number, position and size only, so wiring changes keep it stable."""
    a = [Pad("1", 0.0, 0.0, 1.0, 1.0), Pad("2", 1.0, 0.0, 1.0, 1.0, 0.0, "K")]
    b = [Pad("2", 1.0, 0.0, 1.0, 1.0), Pad("1", 0.0, 0.0, 1.0, 1.0)]
    assert pad_hash(a) == pad_hash(b)
    assert len(pad_hash(a)) == 16
    moved = [Pad("1", 0.0, 0.0, 1.0, 1.0), Pad("2", 1.5, 0.0, 1.0, 1.0)]
    assert pad_hash(a) != pad_hash(moved)


def test_signal_pads_keeps_only_numeric_numbers():
    """EP, MH and lettered pads are excluded from alignment."""
    pads = [
        Pad("1", 0, 0, 1, 1),
        Pad("EP", 0, 0, 2, 2),
        Pad("A1", 0, 0, 1, 1),
        Pad("12", 0, 0, 1, 1),
    ]
    assert [p.number for p in signal_pads(pads)] == ["1", "12"]


def test_mirror_y_negates_y_only():
    """Bottom-side un-mirroring flips Y and leaves X, size and function alone."""
    (pad,) = mirror_y([Pad("1", 1.0, 2.0, 3.0, 4.0, 90.0, "K")])
    assert pad == Pad("1", 1.0, -2.0, 3.0, 4.0, 90.0, "K")


def test_pad_geom_swaps_width_and_height_for_rotated_pads():
    """A pad rotated 90 or 270 degrees presents its height along X."""
    assert pad_geom(Pad("1", 0, 0, 2.0, 1.0, 90.0)) == (0, 0, 1.0, 2.0)
    assert pad_geom(Pad("1", 0, 0, 2.0, 1.0, 270.0)) == (0, 0, 1.0, 2.0)
    assert pad_geom(Pad("2", 1, 0, 2.0, 1.0, 180.0)) == (1, 0, 2.0, 1.0)
    assert pad_geoms([Pad("3", 0, 0, 2.0, 1.0)]) == {"3": (0, 0, 2.0, 1.0)}


def test_pad_points_and_centroid():
    """Points feed the solver; the centroid is the plain mean."""
    pads = [Pad("1", -1.0, 0.0, 1, 1), Pad("2", 1.0, 2.0, 1, 1)]
    assert pad_points(pads) == {"1": (-1.0, 0.0), "2": (1.0, 2.0)}
    assert centroid(pads) == (0.0, 1.0)


def test_ccw_correction_flips_sign_and_snaps():
    """Math-frame angles in a Y-down frame become KiCad CCW degrees with the sign flipped."""
    assert [ccw_correction(a) for a in (0, 90, 180, 270)] == [0, 270, 180, 90]
    assert ccw_correction(-92.0) == 90
    assert ccw_correction(268.0) == 90
    assert ccw_correction(44.0) == 0
