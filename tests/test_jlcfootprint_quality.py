"""Tests for ``autorotation.quality``.

All tests are offline (no network access, no KiCad dependency).  Pad geometries
are synthetic SOT-23-like arrangements chosen to exercise each quality tier.

Threshold tuning notes
----------------------
The angular_rms threshold of 10° (rather than the spec's suggested 3°) was chosen
after measuring real data:

* A 0.5 mm outward shift on a ~1.8 mm arm SOT-23 produces a per-pad angular error
  of ~6°, well above 3° but clearly NOT a rotation mismatch.
* True wrong-rotation cases (45° misalignment snapped to 0°) produce angular RMS
  of ~45° — an order of magnitude above 10°.
* A 10° threshold cleanly separates these two regimes on all tested geometries.

The overlap_fraction and min_overlap_ratio thresholds (1.0 and 0.5) are retained
as specified.
"""

from __future__ import annotations

import math
import unittest

from jlcfootprint.quality import PadGeom, assess_quality
from jlcfootprint.solver import TransformResult, solve_transform

_ANGULAR_RMS_THRESHOLD = 10.0  # degrees — see module docstring for rationale


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_transform(
    rotation_deg: int = 0,
    offset_x: float = 0.0,
    offset_y: float = 0.0,
    residual: float = 0.0,
    matched: int = 3,
    mirrored: bool = False,
    quality_flag: str = "ok",
) -> TransformResult:
    """Build a synthetic TransformResult for unit-testing quality assessment."""
    return TransformResult(
        rotation_deg_raw=float(rotation_deg),
        rotation_deg=rotation_deg,
        offset_x=offset_x,
        offset_y=offset_y,
        residual=residual,
        matched_pad_count=matched,
        is_mirrored=mirrored,
        is_underdetermined=False,
        quality_flag=quality_flag,
    )


def _rotate_point(x: float, y: float, deg: float) -> tuple[float, float]:
    theta = math.radians(deg)
    c, s = math.cos(theta), math.sin(theta)
    return (c * x - s * y, s * x + c * y)


def _circle_pads(
    n: int, radius: float, pad_w: float = 0.5, pad_h: float = 0.5
) -> dict[str, PadGeom]:
    """Build n pads evenly spaced on a circle of given radius."""
    pads = {}
    for i in range(n):
        angle = 2 * math.pi * i / n
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        pads[str(i + 1)] = (x, y, pad_w, pad_h)
    return pads


# SOT-23-3 reference geometry (footprint-local, mm).
# Pads 1 and 2 on the right, pad 3 on the left.
_SOT23_W = 1.07
_SOT23_H = 0.60
_SOT23_PADS: dict[str, PadGeom] = {
    "1": (1.235, 0.950, _SOT23_W, _SOT23_H),
    "2": (1.235, -0.950, _SOT23_W, _SOT23_H),
    "3": (-1.235, 0.000, _SOT23_W, _SOT23_H),
}


class TestIdealMatch(unittest.TestCase):
    """Identical KiCad and JLC pad sets → tier == 'ok'."""

    def test_ideal_sot23_no_rotation(self):
        tr = _make_transform(rotation_deg=0)
        qa = assess_quality(_SOT23_PADS, _SOT23_PADS, tr)
        self.assertEqual(qa.tier, "ok")
        self.assertAlmostEqual(qa.angular_rms_deg, 0.0, delta=0.01)
        self.assertAlmostEqual(qa.overlap_fraction, 1.0, delta=1e-9)
        self.assertAlmostEqual(qa.min_overlap_ratio, 1.0, delta=0.01)
        self.assertAlmostEqual(qa.mean_overlap_ratio, 1.0, delta=0.01)

    def test_ideal_sot23_rotated_90(self):
        """Rotate JLC pads 90°; solver recovers 90°; assess should be 'ok'."""
        # Build JLC pads rotated 90° from KiCad.
        jlc: dict[str, PadGeom] = {}
        for num, (x, y, w, h) in _SOT23_PADS.items():
            nx, ny = _rotate_point(x, y, 90.0)
            # At 90°, bbox swaps: width becomes height and vice versa.
            jlc[num] = (nx, ny, h, w)

        # Solve to recover the 90° rotation.
        kicad_pts = {k: (v[0], v[1]) for k, v in _SOT23_PADS.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertEqual(tr.rotation_deg, 90)

        qa = assess_quality(_SOT23_PADS, jlc, tr)
        self.assertEqual(qa.tier, "ok")
        self.assertAlmostEqual(qa.angular_rms_deg, 0.0, delta=0.5)
        self.assertAlmostEqual(qa.overlap_fraction, 1.0, delta=1e-9)
        self.assertGreater(qa.min_overlap_ratio, 0.5)


class TestSizeVariant(unittest.TestCase):
    """KiCad pads shifted outward from JLC → partial overlap → 'ok_size_variant'.

    A 0.5 mm outward symmetric shift on all three SOT-23 pads produces a
    genuine size/layout variant.  The solver finds rotation=0°, small offset.
    After applying the transform, each KiCad pad partially overlaps its JLC
    counterpart but with min_overlap_ratio < 0.5 (pads are laterally offset
    by 0.5 mm on a 0.6 mm dimension → ~17% overlap area).
    """

    _KICAD_SHIFTED: dict[str, PadGeom] = {
        "1": (1.235, 1.450, _SOT23_W, _SOT23_H),  # shifted up 0.5mm
        "2": (1.235, -1.450, _SOT23_W, _SOT23_H),  # shifted down 0.5mm
        "3": (-1.735, 0.000, _SOT23_W, _SOT23_H),  # shifted left 0.5mm
    }

    def test_sot23_hand_solder_size_variant(self):
        """Symmetric outward shift; solver finds ~0°; quality is 'ok_size_variant'."""
        kicad = self._KICAD_SHIFTED
        jlc = _SOT23_PADS

        kicad_pts = {k: (v[0], v[1]) for k, v in kicad.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertEqual(tr.rotation_deg, 0)

        qa = assess_quality(kicad, jlc, tr)
        self.assertEqual(
            qa.overlap_fraction,
            1.0,
            msg=f"All pads should still overlap, got {qa.overlap_fraction}, notes: {qa.notes}",
        )
        self.assertLess(
            qa.min_overlap_ratio,
            0.5,
            msg=f"Expected partial overlap, min_ratio={qa.min_overlap_ratio}, notes: {qa.notes}",
        )
        self.assertEqual(
            qa.tier,
            "ok_size_variant",
            msg=f"Expected ok_size_variant, got {qa.tier!r}, notes: {qa.notes}",
        )

    def test_sot23_rotated_90_size_variant(self):
        """Rotated 90° hand-solder KiCad; solver recovers rotation; still 'ok_size_variant'."""
        # Rotate the hand-solder KiCad pads by 90°.
        kicad: dict[str, PadGeom] = {}
        for num, (x, y, w, h) in self._KICAD_SHIFTED.items():
            nx, ny = _rotate_point(x, y, 90.0)
            kicad[num] = (nx, ny, h, w)  # swap w/h for 90°

        jlc = _SOT23_PADS

        kicad_pts = {k: (v[0], v[1]) for k, v in kicad.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertIn(
            tr.rotation_deg, (90, 270), msg=f"Expected 90 or 270, got {tr.rotation_deg}"
        )

        qa = assess_quality(kicad, jlc, tr)
        self.assertEqual(qa.overlap_fraction, 1.0, msg=f"notes: {qa.notes}")
        self.assertEqual(
            qa.tier,
            "ok_size_variant",
            msg=f"Expected ok_size_variant, got {qa.tier!r}, notes: {qa.notes}",
        )


class TestRotationMismatch(unittest.TestCase):
    """Wrong footprint / wrong rotation → angular RMS large → 'rotation_mismatch'."""

    def _make_qfp_pads(
        self, n_per_side: int = 8, pitch: float = 0.5, span: float = 5.0
    ) -> dict[str, PadGeom]:
        """Build pads arranged on 4 sides of a square (QFP-like)."""
        pads: dict[str, PadGeom] = {}
        pad_w, pad_h = 0.3, 1.5
        half = (n_per_side - 1) * pitch / 2
        for i in range(n_per_side):
            offset = -half + i * pitch
            pads[str(i + 1)] = (-span / 2, offset, pad_h, pad_w)
            pads[str(i + 1 + n_per_side)] = (offset, span / 2, pad_w, pad_h)
            pads[str(i + 1 + 2 * n_per_side)] = (span / 2, offset, pad_h, pad_w)
            pads[str(i + 1 + 3 * n_per_side)] = (offset, -span / 2, pad_w, pad_h)
        return pads

    def test_qfp_vs_different_qfp_rotation_mismatch(self):
        """Two completely different QFP geometries sharing pad numbers.

        The pad centres will not align under any 90°-snap rotation, so angular
        RMS should be large and the tier should be 'rotation_mismatch'.
        """
        kicad = self._make_qfp_pads(n_per_side=8, pitch=0.5, span=5.0)
        jlc_all = self._make_qfp_pads(n_per_side=5, pitch=0.8, span=8.0)
        jlc = {k: v for k, v in jlc_all.items() if k in kicad}

        self.assertGreater(len(jlc), 0)

        kicad_pts = {k: (v[0], v[1]) for k, v in kicad.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)

        qa = assess_quality(kicad, jlc, tr)
        self.assertEqual(
            qa.tier,
            "rotation_mismatch",
            msg=f"Expected rotation_mismatch, got {qa.tier!r}, notes: {qa.notes}",
        )
        self.assertGreaterEqual(qa.angular_rms_deg, _ANGULAR_RMS_THRESHOLD)

    def test_45deg_misalignment_snapped_to_wrong_angle(self):
        """JLC pads rotated 45° from KiCad; solver snaps to 0° or 90°.

        Forcing rotation_deg=0 (wrong by 45°) should produce large angular RMS.
        """
        jlc: dict[str, PadGeom] = {}
        for num, (x, y, w, h) in _SOT23_PADS.items():
            nx, ny = _rotate_point(x, y, 45.0)
            jlc[num] = (nx, ny, w, h)

        # Force a deliberately wrong rotation (0° instead of the correct 45°→snapped 90°).
        bad_tr = _make_transform(rotation_deg=0, matched=3)

        qa = assess_quality(_SOT23_PADS, jlc, bad_tr)
        self.assertEqual(
            qa.tier,
            "rotation_mismatch",
            msg=f"Expected rotation_mismatch, got {qa.tier!r}, notes: {qa.notes}",
        )
        self.assertGreaterEqual(qa.angular_rms_deg, _ANGULAR_RMS_THRESHOLD)


class TestSuspectOffsetMismatch(unittest.TestCase):
    """Some pads don't overlap after transform → 'suspect_offset_mismatch'.

    We use large circular pad layouts where 1 (or more) pads are radially
    displaced far enough that their bounding boxes don't reach, but with enough
    total pads that the centroid shift stays small and angular RMS stays low.
    """

    def _circle_layout_one_bad(
        self,
        n: int,
        radius: float,
        bad_idx: int,
        bad_radius: float,
        pad_size: float = 0.3,
    ) -> tuple[dict[str, PadGeom], dict[str, PadGeom]]:
        """Build matching KiCad/JLC pad sets where pad ``bad_idx`` is at a different radius in JLC."""
        kicad = _circle_pads(n, radius, pad_w=pad_size, pad_h=pad_size)
        jlc = dict(kicad)
        angle = 2 * math.pi * bad_idx / n
        jlc[str(bad_idx + 1)] = (
            bad_radius * math.cos(angle),
            bad_radius * math.sin(angle),
            pad_size,
            pad_size,
        )
        return kicad, jlc

    def test_one_pad_radially_displaced_20pads(self):
        """20-pad circle, pad 1 displaced from r=2 to r=6; 19 of 20 overlap → 0.95.

        Radial displacement keeps the angular bearing of the displaced pad nearly
        unchanged (it is still at angle 0° from the centroid — just farther).
        With 20 pads the centroid shift is only 0.2 mm, so the angular error for
        the remaining 19 pads stays well below the 10° threshold.
        """
        n = 20
        radius = 2.0
        bad_radius = 6.0  # far enough that 0.3mm bboxes don't reach
        kicad, jlc = self._circle_layout_one_bad(n, radius, 0, bad_radius, pad_size=0.3)

        # Identity transform: we designed kicad/jlc to share the same frame.
        tr = _make_transform(rotation_deg=0, matched=n)

        qa = assess_quality(kicad, jlc, tr)
        # 19 of 20 pads should overlap; the displaced one should not.
        self.assertAlmostEqual(qa.overlap_fraction, 19 / 20, delta=1e-9)
        self.assertLess(
            qa.angular_rms_deg,
            _ANGULAR_RMS_THRESHOLD,
            msg=f"Angular RMS should be low, got {qa.angular_rms_deg:.1f}°",
        )
        self.assertEqual(
            qa.tier,
            "suspect_offset_mismatch",
            msg=f"Expected suspect_offset_mismatch, got {qa.tier!r}, notes: {qa.notes}",
        )

    def test_two_pads_radially_displaced_20pads(self):
        """20-pad circle, pads 1 and 2 displaced; 18 of 20 overlap → 0.9.

        Two opposite-side displacements keep the centroid nearly unchanged
        (they largely cancel), so angular RMS stays low.
        """
        n = 20
        radius = 2.0
        kicad = _circle_pads(n, radius, pad_w=0.3, pad_h=0.3)
        jlc = dict(kicad)
        # Displace pads at index 0 (angle=0°) and index 10 (angle=180°): opposite sides.
        # Their centroid contributions cancel, keeping the centroid shift near zero.
        for bad_idx in (0, 10):
            angle = 2 * math.pi * bad_idx / n
            jlc[str(bad_idx + 1)] = (
                6.0 * math.cos(angle),
                6.0 * math.sin(angle),
                0.3,
                0.3,
            )

        tr = _make_transform(rotation_deg=0, matched=n)
        qa = assess_quality(kicad, jlc, tr)

        self.assertAlmostEqual(qa.overlap_fraction, 18 / 20, delta=1e-9)
        self.assertLess(
            qa.angular_rms_deg,
            _ANGULAR_RMS_THRESHOLD,
            msg=f"Angular RMS={qa.angular_rms_deg:.1f}° should be < {_ANGULAR_RMS_THRESHOLD}°",
        )
        self.assertEqual(qa.tier, "suspect_offset_mismatch", msg=f"notes: {qa.notes}")


class TestNoData(unittest.TestCase):
    """No matching pad numbers → 'no_data'."""

    def test_no_common_keys(self):
        kicad: dict[str, PadGeom] = {"1": (0.0, 0.0, 1.0, 1.0)}
        jlc: dict[str, PadGeom] = {"A": (0.0, 0.0, 1.0, 1.0)}
        tr = _make_transform(rotation_deg=0, matched=0)
        qa = assess_quality(kicad, jlc, tr)
        self.assertEqual(qa.tier, "no_data")

    def test_empty_inputs(self):
        tr = _make_transform(rotation_deg=0, matched=0)
        qa = assess_quality({}, {}, tr)
        self.assertEqual(qa.tier, "no_data")


if __name__ == "__main__":
    unittest.main()
