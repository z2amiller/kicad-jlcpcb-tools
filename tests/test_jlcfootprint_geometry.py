"""Tests for jlcfootprint.geometry: units, frames, hashing and the CCW conversion."""

import pytest

from jlcfootprint.geometry import (
    FLIP_EASYEDA_Y,
    Pad,
    ccw_correction,
    centroid,
    easyeda_pads_to_mm,
    mirror_y,
    named_pads,
    pad_geom,
    pad_hash,
)


def test_easyeda_pads_to_mm_scales_and_flips_only_when_asked():
    """One canvas unit is 0.254 mm; Y follows the flip argument, defaulting to the module constant."""
    raw = [
        {
            "number": "1",
            "x": 10,
            "y": -20,
            "w": 4,
            "h": 2,
            "rotation": 90,
            "shape": "RECT",
        }
    ]
    (unflipped,) = easyeda_pads_to_mm(raw, flip_y=False)
    assert unflipped.number == "1"
    assert unflipped.x == pytest.approx(2.54)
    assert unflipped.y == pytest.approx(-5.08)
    assert unflipped.width == pytest.approx(1.016)
    assert unflipped.height == pytest.approx(0.508)
    assert unflipped.rotation == 90.0
    assert unflipped.shape == "RECT"
    (flipped,) = easyeda_pads_to_mm(raw, flip_y=True)
    assert flipped.y == pytest.approx(5.08)
    (default,) = easyeda_pads_to_mm(raw)
    assert default == easyeda_pads_to_mm(raw, flip_y=FLIP_EASYEDA_Y)[0]


def test_easyeda_pads_to_mm_defaults_rotation_and_shape():
    """Raw pads without a rotation or shape field get 0 degrees and an empty shape."""
    (pad,) = easyeda_pads_to_mm(
        [{"number": "2", "x": 0, "y": 0, "w": 1, "h": 1, "rotation": None}]
    )
    assert (pad.rotation, pad.shape) == (0.0, "")


def test_pad_hash_is_golden_and_order_independent():
    """The hash is a persistent key: a fixed input must always give this value."""
    pads = [Pad("1", 0.0, 0.0, 1.0, 1.0), Pad("2", 1.0, 0.0, 1.0, 1.0, 0.0, "K")]
    assert pad_hash(pads) == "8198e4768b124619"
    assert pad_hash(list(reversed(pads))) == pad_hash(pads)
    assert len(pad_hash(pads)) == 16


def test_pad_hash_ignores_pin_function_and_sign_of_zero_but_not_size_or_turn():
    """Wiring and -0.0 do not change the hash; moving, resizing or turning a pad does."""
    base = [Pad("1", 0.0, 0.0, 2.0, 1.0), Pad("2", 1.0, 0.0, 1.0, 1.0)]
    assert pad_hash(
        [Pad("1", -0.0, 0.0, 2, 1, 0, "K"), Pad("2", 1.0, -0.0, 1.0, 1.0)]
    ) == pad_hash(base)
    assert pad_hash(
        [Pad("1", 0.0, 0.0, 2.0, 1.0), Pad("2", 1.5, 0.0, 1.0, 1.0)]
    ) != pad_hash(base)
    assert pad_hash(
        [Pad("1", 0.0, 0.0, 2.0, 1.0, 90.0), Pad("2", 1.0, 0.0, 1.0, 1.0)]
    ) != pad_hash(base)
    assert pad_hash(
        [Pad("1", 0.0004, 0.0, 2.0, 1.0), Pad("2", 1.0, 0.0, 1.0, 1.0)]
    ) == pad_hash(base)
    assert pad_hash(
        [Pad("1", 0.0006, 0.0, 2.0, 1.0), Pad("2", 1.0, 0.0, 1.0, 1.0)]
    ) != pad_hash(base)


def test_named_pads_keeps_every_named_pad():
    """Unnamed pads (NPTH holes, paste-only copper) are dropped; lettered names and EP stay."""
    pads = [
        Pad("1", 0, 0, 1, 1),
        Pad("", 0, 0, 2, 2),
        Pad("A1", 0, 0, 1, 1),
        Pad("EP", 0, 0, 1, 1),
    ]
    assert [p.number for p in named_pads(pads)] == ["1", "A1", "EP"]


def test_mirror_y_negates_y_only():
    """Bottom-side un-mirroring flips Y and leaves X, size, function and shape alone."""
    (pad,) = mirror_y([Pad("1", 1.0, 2.0, 3.0, 4.0, 90.0, "K", "custom")])
    assert pad == Pad("1", 1.0, -2.0, 3.0, 4.0, 90.0, "K", "custom")


def test_pad_geom_swaps_width_and_height_for_turned_pads():
    """A pad turned 90 or 270 degrees presents its height along X; other angles give the bounding box."""
    assert pad_geom(Pad("1", 0, 0, 2.0, 1.0, 90.0)) == (0, 0, 1.0, 2.0)
    assert pad_geom(Pad("1", 0, 0, 2.0, 1.0, -90.0)) == (0, 0, 1.0, 2.0)
    assert pad_geom(Pad("2", 1, 0, 2.0, 1.0, 180.0)) == (1, 0, 2.0, 1.0)
    _, _, width, height = pad_geom(Pad("3", 0, 0, 2.0, 1.0, 45.0))
    assert width == pytest.approx(height) and width == pytest.approx(3 / 2**0.5)


def test_centroid_is_the_mean_and_rejects_no_pads():
    """The centroid is the plain mean; an empty list is a caller error."""
    pads = [Pad("1", -1.0, 0.0, 1, 1), Pad("2", 1.0, 2.0, 1, 1)]
    assert centroid(pads) == pytest.approx((0.0, 1.0))
    with pytest.raises(ValueError):
        centroid([])


def test_ccw_correction_flips_sign_snaps_and_wraps():
    """Math-frame angles in a Y-down frame become KiCad CCW degrees with the sign flipped."""
    assert [ccw_correction(a) for a in (0, 90, 180, 270)] == [0, 270, 180, 90]
    assert ccw_correction(-92.0) == 90
    assert ccw_correction(268.0) == 90
    assert ccw_correction(44.0) == 0
    assert ccw_correction(-270.0) == 270
    assert ccw_correction(450.0) == 270
