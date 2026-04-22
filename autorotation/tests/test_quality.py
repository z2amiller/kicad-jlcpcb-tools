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

import json
import math
import unittest
from pathlib import Path
from typing import Dict, List

from autorotation.solver import TransformResult, solve_transform
from autorotation.quality import PadGeom, QualityAssessment, assess_quality

FIXTURE_DIR = Path(__file__).parent / "fixtures"

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


def _circle_pads(n: int, radius: float, pad_w: float = 0.5,
                 pad_h: float = 0.5) -> Dict[str, PadGeom]:
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
_SOT23_PADS: Dict[str, PadGeom] = {
    "1": (1.235,  0.950, _SOT23_W, _SOT23_H),
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
        jlc: Dict[str, PadGeom] = {}
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

    _KICAD_SHIFTED: Dict[str, PadGeom] = {
        "1": (1.235,  1.450, _SOT23_W, _SOT23_H),   # shifted up 0.5mm
        "2": (1.235, -1.450, _SOT23_W, _SOT23_H),   # shifted down 0.5mm
        "3": (-1.735, 0.000, _SOT23_W, _SOT23_H),   # shifted left 0.5mm
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
        self.assertEqual(qa.overlap_fraction, 1.0,
                         msg=f"All pads should still overlap, got {qa.overlap_fraction}, notes: {qa.notes}")
        self.assertLess(qa.min_overlap_ratio, 0.5,
                        msg=f"Expected partial overlap, min_ratio={qa.min_overlap_ratio}, notes: {qa.notes}")
        self.assertEqual(qa.tier, "ok_size_variant",
                         msg=f"Expected ok_size_variant, got {qa.tier!r}, notes: {qa.notes}")

    def test_sot23_rotated_90_size_variant(self):
        """Rotated 90° hand-solder KiCad; solver recovers rotation; still 'ok_size_variant'."""
        # Rotate the hand-solder KiCad pads by 90°.
        kicad: Dict[str, PadGeom] = {}
        for num, (x, y, w, h) in self._KICAD_SHIFTED.items():
            nx, ny = _rotate_point(x, y, 90.0)
            kicad[num] = (nx, ny, h, w)  # swap w/h for 90°

        jlc = _SOT23_PADS

        kicad_pts = {k: (v[0], v[1]) for k, v in kicad.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertIn(tr.rotation_deg, (90, 270),
                      msg=f"Expected 90 or 270, got {tr.rotation_deg}")

        qa = assess_quality(kicad, jlc, tr)
        self.assertEqual(qa.overlap_fraction, 1.0,
                         msg=f"notes: {qa.notes}")
        self.assertEqual(qa.tier, "ok_size_variant",
                         msg=f"Expected ok_size_variant, got {qa.tier!r}, notes: {qa.notes}")


class TestRotationMismatch(unittest.TestCase):
    """Wrong footprint / wrong rotation → angular RMS large → 'rotation_mismatch'."""

    def _make_qfp_pads(self, n_per_side: int = 8, pitch: float = 0.5,
                       span: float = 5.0) -> Dict[str, PadGeom]:
        """Build pads arranged on 4 sides of a square (QFP-like)."""
        pads: Dict[str, PadGeom] = {}
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
        self.assertEqual(qa.tier, "rotation_mismatch",
                         msg=f"Expected rotation_mismatch, got {qa.tier!r}, notes: {qa.notes}")
        self.assertGreaterEqual(qa.angular_rms_deg, _ANGULAR_RMS_THRESHOLD)

    def test_45deg_misalignment_snapped_to_wrong_angle(self):
        """JLC pads rotated 45° from KiCad; solver snaps to 0° or 90°.

        Forcing rotation_deg=0 (wrong by 45°) should produce large angular RMS.
        """
        jlc: Dict[str, PadGeom] = {}
        for num, (x, y, w, h) in _SOT23_PADS.items():
            nx, ny = _rotate_point(x, y, 45.0)
            jlc[num] = (nx, ny, w, h)

        # Force a deliberately wrong rotation (0° instead of the correct 45°→snapped 90°).
        bad_tr = _make_transform(rotation_deg=0, matched=3)

        qa = assess_quality(_SOT23_PADS, jlc, bad_tr)
        self.assertEqual(qa.tier, "rotation_mismatch",
                         msg=f"Expected rotation_mismatch, got {qa.tier!r}, notes: {qa.notes}")
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
    ) -> tuple[Dict[str, PadGeom], Dict[str, PadGeom]]:
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
        self.assertLess(qa.angular_rms_deg, _ANGULAR_RMS_THRESHOLD,
                        msg=f"Angular RMS should be low, got {qa.angular_rms_deg:.1f}°")
        self.assertEqual(qa.tier, "suspect_offset_mismatch",
                         msg=f"Expected suspect_offset_mismatch, got {qa.tier!r}, notes: {qa.notes}")

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
            jlc[str(bad_idx + 1)] = (6.0 * math.cos(angle), 6.0 * math.sin(angle), 0.3, 0.3)

        tr = _make_transform(rotation_deg=0, matched=n)
        qa = assess_quality(kicad, jlc, tr)

        self.assertAlmostEqual(qa.overlap_fraction, 18 / 20, delta=1e-9)
        self.assertLess(qa.angular_rms_deg, _ANGULAR_RMS_THRESHOLD,
                        msg=f"Angular RMS={qa.angular_rms_deg:.1f}° should be < {_ANGULAR_RMS_THRESHOLD}°")
        self.assertEqual(qa.tier, "suspect_offset_mismatch",
                         msg=f"notes: {qa.notes}")


class TestNoData(unittest.TestCase):
    """No matching pad numbers → 'no_data'."""

    def test_no_common_keys(self):
        kicad: Dict[str, PadGeom] = {"1": (0.0, 0.0, 1.0, 1.0)}
        jlc: Dict[str, PadGeom] = {"A": (0.0, 0.0, 1.0, 1.0)}
        tr = _make_transform(rotation_deg=0, matched=0)
        qa = assess_quality(kicad, jlc, tr)
        self.assertEqual(qa.tier, "no_data")

    def test_empty_inputs(self):
        tr = _make_transform(rotation_deg=0, matched=0)
        qa = assess_quality({}, {}, tr)
        self.assertEqual(qa.tier, "no_data")


class TestRealC2132Fixture(unittest.TestCase):
    """Load C2132 fixture and verify width/height fields are present and correct."""

    @classmethod
    def setUpClass(cls):
        from autorotation.easyeda_api import parse_easyeda_response
        with open(FIXTURE_DIR / "C2132.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        cls.result = parse_easyeda_response(data, lcsc_code="C2132")

    def test_parse_succeeds(self):
        self.assertTrue(self.result.success, msg=self.result.error)

    def test_width_height_present(self):
        for pad in self.result.pads:
            self.assertIn("width", pad, msg=f"pad {pad['number']} missing 'width'")
            self.assertIn("height", pad, msg=f"pad {pad['number']} missing 'height'")

    def test_width_height_values(self):
        # Raw PAD fields 4=4.2126, 5=2.3622 → 1.070mm × 0.600mm.
        by_num = {p["number"]: p for p in self.result.pads}
        for num in ("1", "2", "3"):
            self.assertAlmostEqual(by_num[num]["width"], 1.070, delta=0.005,
                                   msg=f"pad {num} width")
            self.assertAlmostEqual(by_num[num]["height"], 0.600, delta=0.005,
                                   msg=f"pad {num} height")


class TestEndToEndC2132(unittest.TestCase):
    """Integration: load C2132 fixture, build synthetic KiCad variants, run solver + quality."""

    @classmethod
    def setUpClass(cls):
        from autorotation.easyeda_api import parse_easyeda_response
        with open(FIXTURE_DIR / "C2132.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        result = parse_easyeda_response(data, lcsc_code="C2132")
        assert result.success, result.error

        # JLC pads as PadGeom.
        cls.jlc: Dict[str, PadGeom] = {
            p["number"]: (p["x"], p["y"], p["width"], p["height"])
            for p in result.pads
        }

        # KiCad hand-solder variant: same positions, pads shifted outward 0.5mm
        # symmetrically so the centroid barely moves.
        # Pad 1 is above center (+y), pad 2 below (-y), pad 3 left (-x).
        by_num = {p["number"]: p for p in result.pads}
        shift = 0.5
        cls.kicad_handsolder: Dict[str, PadGeom] = {
            "1": (by_num["1"]["x"], by_num["1"]["y"] + shift,
                  by_num["1"]["width"], by_num["1"]["height"]),
            "2": (by_num["2"]["x"], by_num["2"]["y"] - shift,
                  by_num["2"]["width"], by_num["2"]["height"]),
            "3": (by_num["3"]["x"] - shift, by_num["3"]["y"],
                  by_num["3"]["width"], by_num["3"]["height"]),
        }

    def test_handsolder_is_ok_size_variant(self):
        """Symmetric outward shift → solver finds ~0° rotation → 'ok_size_variant'."""
        kicad_pts = {k: (v[0], v[1]) for k, v in self.kicad_handsolder.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in self.jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertEqual(tr.rotation_deg, 0,
                         msg=f"Expected 0° rotation, got {tr.rotation_deg}")

        qa = assess_quality(self.kicad_handsolder, self.jlc, tr)
        self.assertEqual(qa.tier, "ok_size_variant",
                         msg=f"Expected ok_size_variant, got {qa.tier!r}, notes: {qa.notes}")

    def test_handsolder_rotated_90_still_ok_size_variant(self):
        """Hand-solder KiCad footprint rotated 90° → solver recovers rotation → 'ok_size_variant'."""
        kicad_rot: Dict[str, PadGeom] = {}
        for num, (x, y, w, h) in self.kicad_handsolder.items():
            nx, ny = _rotate_point(x, y, 90.0)
            kicad_rot[num] = (nx, ny, h, w)  # swap w/h for 90°

        kicad_pts = {k: (v[0], v[1]) for k, v in kicad_rot.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in self.jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertIn(tr.rotation_deg, (90, 270),
                      msg=f"Expected 90 or 270, got {tr.rotation_deg}")

        qa = assess_quality(kicad_rot, self.jlc, tr)
        self.assertEqual(qa.tier, "ok_size_variant",
                         msg=f"Expected ok_size_variant after rotation, got {qa.tier!r}, notes: {qa.notes}")

    def test_handsolder_mirrored_mirror_flag_set(self):
        """Mirror the KiCad footprint (not just the shifted variant).

        SOT-23 is asymmetric enough that mirroring is detectable by the solver.
        We mirror the JLC pads and verify the solver correctly flags is_mirrored.
        The quality assessment still runs without error.
        """
        # Use the standard SOT-23 JLC pads (asymmetric; detectable mirror).
        jlc = self.jlc
        kicad = {k: (v[0], v[1], v[2], v[3]) for k, v in jlc.items()}

        # Mirror KiCad across y-axis (x → -x).
        kicad_mir: Dict[str, PadGeom] = {k: (-v[0], v[1], v[2], v[3]) for k, v in kicad.items()}

        kicad_pts = {k: (v[0], v[1]) for k, v in kicad_mir.items()}
        jlc_pts = {k: (v[0], v[1]) for k, v in jlc.items()}
        tr = solve_transform(kicad_pts, jlc_pts)
        self.assertTrue(tr.is_mirrored,
                        msg=f"Expected is_mirrored=True for mirrored SOT-23; quality_flag={tr.quality_flag}")

        # Quality assessment runs without error regardless of tier.
        qa = assess_quality(kicad_mir, jlc, tr)
        self.assertIsInstance(qa, QualityAssessment)
        self.assertIsNotNone(qa.notes)


if __name__ == "__main__":
    unittest.main()
