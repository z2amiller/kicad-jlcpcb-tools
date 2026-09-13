"""Regressions for the solver and quality checker found in review: centroid pads, off-origin footprints, NaN."""

import math

from jlcfootprint.quality import assess_quality
from jlcfootprint.solver import solve_transform


def _rotated(points, degrees):
    """Rotate points about their centroid by a small angle (math frame)."""
    cx = sum(x for x, _ in points.values()) / len(points)
    cy = sum(y for _, y in points.values()) / len(points)
    theta = math.radians(degrees)
    out = {}
    for key, (x, y) in points.items():
        dx, dy = x - cx, y - cy
        out[key] = (
            cx + dx * math.cos(theta) - dy * math.sin(theta),
            cy + dx * math.sin(theta) + dy * math.cos(theta),
        )
    return out


def test_slightly_off_centre_exposed_pad_does_not_fake_a_rotation_mismatch():
    """A pad at the centroid has no usable bearing; it must not drive the angular RMS."""
    kicad = {
        str(i): (x, y, 0.3, 0.7)
        for i, (x, y) in enumerate(
            [
                (-0.75, 1.5),
                (-0.25, 1.5),
                (0.25, 1.5),
                (0.75, 1.5),
                (0.75, -1.5),
                (0.25, -1.5),
                (-0.25, -1.5),
                (-0.75, -1.5),
            ],
            1,
        )
    }
    jlc = dict(kicad)
    kicad["9"] = (0.02, 0.0, 1.7, 1.7)
    jlc["9"] = (0.0, 0.01, 1.7, 1.7)
    transform = solve_transform(
        {k: v[:2] for k, v in kicad.items()}, {k: v[:2] for k, v in jlc.items()}
    )
    quality = assess_quality(kicad, jlc, transform)
    assert quality.tier == "ok"
    assert quality.angular_rms_deg < 1.0


def test_pin1_origin_footprint_with_a_small_raw_angle_still_overlaps():
    """The translation must belong to the snapped angle, or a far-from-origin footprint drifts."""
    kicad = {str(i): (i * 2.54, 0.0, 1.7, 1.7) for i in range(4)}
    centred = {k: (v[0] - 3.81, v[1]) for k, v in kicad.items()}
    jlc_points = _rotated(centred, 3.0)
    jlc = {k: (x, y, 1.7, 1.7) for k, (x, y) in jlc_points.items()}
    transform = solve_transform({k: v[:2] for k, v in kicad.items()}, jlc_points)
    assert transform.rotation_deg == 0
    quality = assess_quality(kicad, jlc, transform)
    assert quality.tier == "ok"
    assert quality.min_overlap_ratio > 0.85


def test_nan_coordinates_are_underdetermined_not_an_exception():
    """A NaN pad must not raise out of the solver."""
    result = solve_transform(
        {"1": (float("nan"), 0.0), "2": (1.0, 0.0)}, {"1": (0.0, 0.0), "2": (1.0, 0.0)}
    )
    assert result.is_underdetermined
    assert result.quality_flag == "underdetermined"
