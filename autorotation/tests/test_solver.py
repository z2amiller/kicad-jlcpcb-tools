"""Tests for the 2D rigid-transform solver."""
from __future__ import annotations

import math
import random

import pytest

from autorotation.solver import solve_transform, TransformResult


# ---------------------------------------------------------------------------
# helpers


def rotate(pt, deg):
    t = math.radians(deg)
    c, s = math.cos(t), math.sin(t)
    x, y = pt
    return (c * x - s * y, s * x + c * y)


def translate(pt, dx, dy):
    return (pt[0] + dx, pt[1] + dy)


def transform_set(pads, deg=0.0, dx=0.0, dy=0.0, mirror_y=False, noise=0.0,
                  rng=None):
    out = {}
    for k, p in pads.items():
        x, y = p
        if mirror_y:
            y = -y
        x, y = rotate((x, y), deg)
        x += dx
        y += dy
        if noise and rng is not None:
            x += rng.gauss(0.0, noise)
            y += rng.gauss(0.0, noise)
        out[k] = (x, y)
    return out


SOT23 = {
    1: (-0.95, 0.95),
    2: (0.95, 0.95),
    3: (0.0, -0.95),
}

QFP4 = {
    1: (-1.0, 1.0),
    2: (1.0, 1.0),
    3: (1.0, -1.0),
    4: (-1.0, -1.0),
}


CLEAN_TOL = 1e-10


# ---------------------------------------------------------------------------
# tests


def test_identity():
    r = solve_transform(QFP4, dict(QFP4))
    assert r.matched_pad_count == 4
    assert r.quality_flag == "ok"
    assert abs(r.rotation_deg_raw) < 1e-9
    assert r.rotation_deg == 0
    assert abs(r.offset_x) < CLEAN_TOL
    assert abs(r.offset_y) < CLEAN_TOL
    assert r.residual < CLEAN_TOL
    assert not r.is_mirrored
    assert not r.is_underdetermined


def test_pure_translation():
    b = transform_set(QFP4, dx=3.0, dy=5.0)
    r = solve_transform(QFP4, b)
    assert r.rotation_deg == 0
    assert abs(r.rotation_deg_raw) < 1e-9
    assert r.offset_x == pytest.approx(3.0, abs=1e-9)
    assert r.offset_y == pytest.approx(5.0, abs=1e-9)
    assert r.residual < CLEAN_TOL


@pytest.mark.parametrize("deg", [90, 180, 270])
def test_pure_rotation(deg):
    b = transform_set(QFP4, deg=deg)
    r = solve_transform(QFP4, b)
    assert r.rotation_deg == deg % 360
    assert abs(r.offset_x) < 1e-9
    assert abs(r.offset_y) < 1e-9
    assert r.residual < CLEAN_TOL


def test_rotation_plus_translation():
    b = transform_set(QFP4, deg=90, dx=10.0, dy=-4.0)
    r = solve_transform(QFP4, b)
    assert r.rotation_deg == 90
    assert r.offset_x == pytest.approx(10.0, abs=1e-9)
    assert r.offset_y == pytest.approx(-4.0, abs=1e-9)
    assert r.residual < CLEAN_TOL


def test_non_axis_aligned():
    b = transform_set(QFP4, deg=37.0)
    r = solve_transform(QFP4, b)
    assert r.rotation_deg_raw == pytest.approx(37.0, abs=1e-9)
    assert r.rotation_deg == 0  # nearest 90
    assert r.residual < CLEAN_TOL


def test_non_axis_aligned_snaps_to_90():
    b = transform_set(QFP4, deg=80.0)
    r = solve_transform(QFP4, b)
    assert r.rotation_deg_raw == pytest.approx(80.0, abs=1e-9)
    assert r.rotation_deg == 90


def test_noise_robustness():
    rng = random.Random(42)
    b = transform_set(QFP4, deg=90, dx=2.0, dy=2.0, noise=0.01, rng=rng)
    r = solve_transform(QFP4, b)
    assert r.rotation_deg == 90
    assert abs(r.rotation_deg_raw - 90.0) < 1.0
    # residual is small but nonzero
    assert 0.0 < r.residual < 0.05


def test_sot23_rotated_90():
    b = transform_set(SOT23, deg=90)
    r = solve_transform(SOT23, b)
    assert r.matched_pad_count == 3
    assert r.rotation_deg == 90
    assert r.rotation_deg_raw == pytest.approx(90.0, abs=1e-9)
    assert r.residual < CLEAN_TOL


def test_mirrored_detection():
    # B = A mirrored across the x-axis (flip y sign), no rotation.
    b = transform_set(QFP4, mirror_y=True)
    r = solve_transform(QFP4, b)
    assert r.is_mirrored is True
    assert r.quality_flag == "mirrored"


def test_mirrored_sot23():
    b = transform_set(SOT23, mirror_y=True, deg=45.0)
    r = solve_transform(SOT23, b)
    assert r.is_mirrored is True


def test_missing_pads_in_b():
    a = dict(QFP4)
    b = transform_set({k: v for k, v in QFP4.items() if k != 4}, deg=90)
    r = solve_transform(a, b)
    assert r.matched_pad_count == 3
    assert r.rotation_deg == 90
    assert r.residual < CLEAN_TOL


def test_no_matching_pads():
    a = {1: (0, 0), 2: (1, 0), 3: (0, 1)}
    b = {4: (0, 0), 5: (1, 0), 6: (0, 1)}
    r = solve_transform(a, b)
    assert r.matched_pad_count == 0
    assert r.quality_flag == "no_pads"
    assert r.is_underdetermined
    assert math.isinf(r.residual)


def test_string_pad_names_bga():
    a = {"A1": (0.0, 0.0), "B2": (1.0, 0.0), "C3": (0.0, 1.0)}
    b = transform_set(a, deg=90, dx=5.0, dy=-2.0)
    r = solve_transform(a, b)
    assert r.matched_pad_count == 3
    assert r.rotation_deg == 90
    assert r.offset_x == pytest.approx(5.0, abs=1e-9)
    assert r.offset_y == pytest.approx(-2.0, abs=1e-9)
    assert r.residual < CLEAN_TOL


def test_single_pad_underdetermined():
    a = {1: (1.0, 2.0)}
    b = {1: (4.0, 7.0)}
    r = solve_transform(a, b)
    assert r.matched_pad_count == 1
    assert r.is_underdetermined
    assert r.quality_flag == "underdetermined"
    assert r.offset_x == pytest.approx(3.0)
    assert r.offset_y == pytest.approx(5.0)
    assert r.rotation_deg == 0


def test_two_pads_uniquely_determined():
    a = {1: (0.0, 0.0), 2: (2.0, 0.0)}
    b = transform_set(a, deg=90, dx=1.0, dy=1.0)
    r = solve_transform(a, b)
    assert r.matched_pad_count == 2
    assert not r.is_underdetermined
    assert r.rotation_deg == 90
    assert r.residual < CLEAN_TOL


def test_two_pads_coincident_is_underdetermined():
    a = {1: (1.0, 1.0), 2: (1.0, 1.0)}
    b = {1: (2.0, 3.0), 2: (2.0, 3.0)}
    r = solve_transform(a, b)
    assert r.is_underdetermined
    assert r.quality_flag == "underdetermined"


def test_applied_transform_matches_b():
    """Sanity-check: applying the returned transform to A should yield B."""
    b = transform_set(QFP4, deg=45.0, dx=7.0, dy=-3.0)
    r = solve_transform(QFP4, b)
    theta = math.radians(r.rotation_deg_raw)
    c, s = math.cos(theta), math.sin(theta)
    for k, (ax, ay) in QFP4.items():
        rx = c * ax - s * ay + r.offset_x
        ry = s * ax + c * ay + r.offset_y
        bx, by = b[k]
        assert rx == pytest.approx(bx, abs=1e-9)
        assert ry == pytest.approx(by, abs=1e-9)


def test_result_type():
    r = solve_transform(QFP4, dict(QFP4))
    assert isinstance(r, TransformResult)
