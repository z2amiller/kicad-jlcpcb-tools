"""Quality-assessment layer for rigid-transform pad alignment.

Computes pad-overlap fraction and angular consistency on top of the raw
linear residual already provided by ``autorotation.solver``.  Designed as
a pure function with no I/O so it can be tested offline without fixtures.

Python 3.9 compatible; stdlib only.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Literal, Tuple

from autorotation.solver import TransformResult

# (x_centre, y_centre, width, height) in mm — all footprint-local.
PadGeom = Tuple[float, float, float, float]


@dataclass
class QualityAssessment:
    """Result of the quality-assessment step.

    Attributes:
        tier: Categorical verdict — one of:
            * ``"ok"``                   – rotation + size both look right.
            * ``"ok_size_variant"``      – rotation OK, pads overlap but sizes differ.
            * ``"suspect_offset_mismatch"`` – rotation OK but at least one pad
                                              centre misses its target after transform.
            * ``"rotation_mismatch"``    – angular RMS too large; rotation likely wrong.
            * ``"no_data"``              – fewer than 1 matching pad.
        angular_rms_deg:   RMS of per-pad bearing errors after removing the
                           expected rotation (degrees).
        overlap_fraction:  Fraction of matched pads whose bounding boxes
                           overlap after applying the transform (0.0–1.0).
        min_overlap_ratio: Smallest per-pad intersection_area/min(a_area, b_area).
                           0.0 when any pad has zero overlap.
        mean_overlap_ratio: Mean of per-pad overlap ratios.
        notes: Human-readable diagnostic string.
    """

    tier: Literal[
        "ok",
        "ok_size_variant",
        "suspect_offset_mismatch",
        "rotation_mismatch",
        "no_data",
    ]
    angular_rms_deg: float
    overlap_fraction: float
    min_overlap_ratio: float
    mean_overlap_ratio: float
    notes: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _rotate_2d(x: float, y: float, theta_deg: float) -> Tuple[float, float]:
    """Rotate point (x, y) by theta_deg degrees around the origin."""
    theta = math.radians(theta_deg)
    c = math.cos(theta)
    s = math.sin(theta)
    return (c * x - s * y, s * x + c * y)


def _swap_wh_for_rotation(w: float, h: float, rotation_deg: int) -> Tuple[float, float]:
    """Return (effective_w, effective_h) after a 90°-snapped rotation.

    For 0°/180° the bbox dimensions are unchanged.
    For 90°/270° width and height are swapped (the rectangle tips onto its side).
    """
    rot = rotation_deg % 180  # 0 or 90
    if rot == 90:
        return h, w
    return w, h


def _normalize_angle(deg: float) -> float:
    """Normalize angle to the half-open interval (-180, 180]."""
    while deg <= -180.0:
        deg += 360.0
    while deg > 180.0:
        deg -= 360.0
    return deg


def _pad_overlap_ratio(
    ax: float, ay: float, aw: float, ah: float,
    bx: float, by: float, bw: float, bh: float,
) -> float:
    """Return intersection_area / min(a_area, b_area) for two axis-aligned rectangles.

    Returns 0.0 if either area is zero or the boxes do not intersect.
    """
    a_area = aw * ah
    b_area = bw * bh
    min_area = min(a_area, b_area)
    if min_area <= 0.0:
        return 0.0

    ix = max(0.0, min(ax + aw / 2, bx + bw / 2) - max(ax - aw / 2, bx - bw / 2))
    iy = max(0.0, min(ay + ah / 2, by + bh / 2) - max(ay - ah / 2, by - bh / 2))
    intersection_area = ix * iy
    return intersection_area / min_area


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def assess_quality(
    kicad_pads: Dict[str, PadGeom],
    jlc_pads: Dict[str, PadGeom],
    transform: TransformResult,
) -> QualityAssessment:
    """Assess alignment quality between two pad sets given a solved transform.

    Parameters
    ----------
    kicad_pads:
        Mapping from pad number (str) to ``(x, y, width, height)`` in mm,
        in the KiCad footprint's local frame.
    jlc_pads:
        Same format, in the JLC/EasyEDA footprint's local frame.
    transform:
        Result from ``autorotation.solver.solve_transform``.  The snapped
        rotation and offset fields are used; the raw rotation is ignored.

    Returns
    -------
    QualityAssessment
    """
    common_keys = sorted(set(kicad_pads.keys()) & set(jlc_pads.keys()))
    n = len(common_keys)

    if n == 0:
        return QualityAssessment(
            tier="no_data",
            angular_rms_deg=0.0,
            overlap_fraction=0.0,
            min_overlap_ratio=0.0,
            mean_overlap_ratio=0.0,
            notes="no matching pad numbers between KiCad and JLC footprints",
        )

    rot_deg = transform.rotation_deg  # snapped: 0/90/180/270
    tx = transform.offset_x
    ty = transform.offset_y

    # ------------------------------------------------------------------
    # Transform KiCad pad centres into JLC frame.
    # ------------------------------------------------------------------
    transformed_kicad: list[tuple[float, float, float, float]] = []
    for key in common_keys:
        kx, ky, kw, kh = kicad_pads[key]
        # Rotate centre.
        new_cx, new_cy = _rotate_2d(kx, ky, rot_deg)
        new_cx += tx
        new_cy += ty
        # Swap dimensions for 90°/270° rotations.
        new_w, new_h = _swap_wh_for_rotation(kw, kh, rot_deg)
        transformed_kicad.append((new_cx, new_cy, new_w, new_h))

    jlc_geoms: list[tuple[float, float, float, float]] = [jlc_pads[key] for key in common_keys]

    # ------------------------------------------------------------------
    # Per-pad overlap ratios.
    # ------------------------------------------------------------------
    overlap_ratios = []
    for (ax, ay, aw, ah), (bx, by, bw, bh) in zip(transformed_kicad, jlc_geoms):
        ratio = _pad_overlap_ratio(ax, ay, aw, ah, bx, by, bw, bh)
        overlap_ratios.append(ratio)

    overlap_fraction = sum(1 for r in overlap_ratios if r > 0) / n
    min_overlap_ratio = min(overlap_ratios)
    mean_overlap_ratio = sum(overlap_ratios) / n

    # ------------------------------------------------------------------
    # Angular RMS.
    # ------------------------------------------------------------------
    # Centroids in each frame (using *original* KiCad centres, not transformed).
    kicad_centres = [(kicad_pads[k][0], kicad_pads[k][1]) for k in common_keys]
    jlc_centres = [(jlc_pads[k][0], jlc_pads[k][1]) for k in common_keys]

    ck_x = sum(p[0] for p in kicad_centres) / n
    ck_y = sum(p[1] for p in kicad_centres) / n
    cj_x = sum(p[0] for p in jlc_centres) / n
    cj_y = sum(p[1] for p in jlc_centres) / n

    angular_errs_sq = []
    for (kx, ky), (jx, jy) in zip(kicad_centres, jlc_centres):
        dk_x = kx - ck_x
        dk_y = ky - ck_y
        dj_x = jx - cj_x
        dj_y = jy - cj_y
        # Skip pads that are at (or very near) the centroid to avoid atan2(0,0).
        if abs(dk_x) < 1e-9 and abs(dk_y) < 1e-9:
            continue
        if abs(dj_x) < 1e-9 and abs(dj_y) < 1e-9:
            continue
        bearing_kicad = math.degrees(math.atan2(dk_y, dk_x))
        bearing_jlc = math.degrees(math.atan2(dj_y, dj_x))
        err = _normalize_angle(bearing_jlc - bearing_kicad - rot_deg)
        angular_errs_sq.append(err * err)

    if angular_errs_sq:
        angular_rms_deg = math.sqrt(sum(angular_errs_sq) / len(angular_errs_sq))
    else:
        # All pads coincide with centroids — underdetermined, treat as 0 error.
        angular_rms_deg = 0.0

    # ------------------------------------------------------------------
    # Classify.
    # ------------------------------------------------------------------
    overlapping_count = sum(1 for r in overlap_ratios if r > 0)

    if angular_rms_deg >= 10.0:
        tier: Literal[
            "ok", "ok_size_variant", "suspect_offset_mismatch",
            "rotation_mismatch", "no_data"
        ] = "rotation_mismatch"
        notes = (
            f"{overlapping_count} of {n} pads overlap; "
            f"angular rms {angular_rms_deg:.1f}° (threshold 10°) — rotation likely wrong"
        )
    elif overlap_fraction < 1.0:
        tier = "suspect_offset_mismatch"
        notes = (
            f"{overlapping_count} of {n} pads overlap; "
            f"angular rms {angular_rms_deg:.1f}° — rotation OK but offset suspect"
        )
    elif min_overlap_ratio >= 0.5:
        tier = "ok"
        notes = (
            f"all {n} pads overlap; "
            f"min overlap ratio {min_overlap_ratio:.2f}; "
            f"angular rms {angular_rms_deg:.1f}°"
        )
    else:
        tier = "ok_size_variant"
        notes = (
            f"all {n} pads overlap but size differs "
            f"(min overlap ratio {min_overlap_ratio:.2f} < 0.5); "
            f"angular rms {angular_rms_deg:.1f}°"
        )

    return QualityAssessment(
        tier=tier,
        angular_rms_deg=angular_rms_deg,
        overlap_fraction=overlap_fraction,
        min_overlap_ratio=min_overlap_ratio,
        mean_overlap_ratio=mean_overlap_ratio,
        notes=notes,
    )
