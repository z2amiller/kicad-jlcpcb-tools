"""2D rigid-transform solver (Kabsch/Umeyama, specialised for 2D).

Given two pad dicts ``{pad_number: (x, y)}`` from the same physical part
(one from KiCad, one from JLCPCB/EasyEDA), compute the rotation and
translation that best aligns ``A`` to ``B``.

No numpy dependency: the 2D case admits a closed-form rotation from the
cross-covariance via ``atan2``, which is numerically well-behaved and
avoids the need for a 2x2 SVD.  Stdlib-only so this works inside the
KiCad embedded Python interpreter.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Hashable, Iterable, List, Tuple

Point = Tuple[float, float]
PadDict = Dict[Hashable, Point]


@dataclass
class TransformResult:
    """Result of aligning pad set A onto pad set B.

    The transform maps a point ``a`` in A's frame to B's frame via
    ``R @ a + t`` where ``R`` is rotation by ``rotation_deg_raw``
    degrees and ``t = (offset_x, offset_y)``.
    """

    rotation_deg_raw: float      # unsnapped, for diagnostics
    rotation_deg: int            # snapped to nearest 90
    offset_x: float              # translation, same units as input
    offset_y: float
    residual: float              # RMS per-pad error, same units as input
    matched_pad_count: int
    is_mirrored: bool            # true if Kabsch indicates reflection (flag, don't apply)
    is_underdetermined: bool     # true if <2 pads or pads coincident
    quality_flag: str            # "ok" | "mirrored" | "underdetermined" | "no_pads"


def _identity(matched: int, flag: str) -> TransformResult:
    return TransformResult(
        rotation_deg_raw=0.0,
        rotation_deg=0,
        offset_x=0.0,
        offset_y=0.0,
        residual=float("inf") if flag == "no_pads" else 0.0,
        matched_pad_count=matched,
        is_mirrored=False,
        is_underdetermined=flag in ("no_pads", "underdetermined"),
        quality_flag=flag,
    )


def _apply(theta: float, tx: float, ty: float, p: Point) -> Point:
    c = math.cos(theta)
    s = math.sin(theta)
    x, y = p
    return (c * x - s * y + tx, s * x + c * y + ty)


def _rms(theta: float, tx: float, ty: float,
         a_pts: List[Point], b_pts: List[Point]) -> float:
    if not a_pts:
        return 0.0
    total = 0.0
    for a, b in zip(a_pts, b_pts):
        rx, ry = _apply(theta, tx, ty, a)
        dx = rx - b[0]
        dy = ry - b[1]
        total += dx * dx + dy * dy
    return math.sqrt(total / len(a_pts))


def _solve_rotation_translation(a_pts: List[Point],
                                b_pts: List[Point],
                                mirror_y: bool = False
                                ) -> Tuple[float, float, float, float]:
    """Return (theta, tx, ty, residual) aligning a_pts onto b_pts.

    If ``mirror_y`` is True, mirror the A set across the x-axis first
    (i.e. apply y -> -y).  Used to test the reflection hypothesis.
    """
    if mirror_y:
        a_pts = [(x, -y) for (x, y) in a_pts]

    n = len(a_pts)
    ax = sum(p[0] for p in a_pts) / n
    ay = sum(p[1] for p in a_pts) / n
    bx = sum(p[0] for p in b_pts) / n
    by = sum(p[1] for p in b_pts) / n

    # Cross-covariance H = sum_i a'_i outer b'_i.
    h11 = h12 = h21 = h22 = 0.0
    for (xa, ya), (xb, yb) in zip(a_pts, b_pts):
        dxa = xa - ax
        dya = ya - ay
        dxb = xb - bx
        dyb = yb - by
        h11 += dxa * dxb
        h12 += dxa * dyb
        h21 += dya * dxb
        h22 += dya * dyb

    # Closed-form 2D optimal proper rotation.
    # Derivation: we want R minimising sum ||R a' - b'||^2.
    # Expand and maximise trace(R^T H).  For R(theta) the trace is
    # (h11 + h22) cos(theta) + (h12 - h21) sin(theta), maximised at
    # theta = atan2(h12 - h21, h11 + h22).
    theta = math.atan2(h12 - h21, h11 + h22)

    c = math.cos(theta)
    s = math.sin(theta)
    # t = cb - R @ ca
    tx = bx - (c * ax - s * ay)
    ty = by - (s * ax + c * ay)

    residual = _rms(theta, tx, ty, a_pts, b_pts)
    return theta, tx, ty, residual


def solve_transform(a: PadDict, b: PadDict) -> TransformResult:
    """Compute the rigid transform aligning pad set A onto pad set B.

    Pad numbers (dict keys) are matched by equality — pin 1 in A pairs
    with pin 1 in B.  Keys may be ints, strings ("A1", "B2"), or any
    hashable.  Pads present in only one side are ignored.
    """
    common = sorted(set(a.keys()) & set(b.keys()), key=lambda k: (str(type(k)), str(k)))
    matched = len(common)

    if matched == 0:
        return _identity(0, "no_pads")

    a_pts: List[Point] = [(float(a[k][0]), float(a[k][1])) for k in common]
    b_pts: List[Point] = [(float(b[k][0]), float(b[k][1])) for k in common]

    if matched == 1:
        # Only translation recoverable.
        tx = b_pts[0][0] - a_pts[0][0]
        ty = b_pts[0][1] - a_pts[0][1]
        return TransformResult(
            rotation_deg_raw=0.0,
            rotation_deg=0,
            offset_x=tx,
            offset_y=ty,
            residual=0.0,
            matched_pad_count=1,
            is_mirrored=False,
            is_underdetermined=True,
            quality_flag="underdetermined",
        )

    # Degenerate cases: all A points coincident or all B points coincident.
    def _all_coincident(pts: List[Point]) -> bool:
        x0, y0 = pts[0]
        eps = 1e-12
        return all(abs(x - x0) < eps and abs(y - y0) < eps for (x, y) in pts)

    if _all_coincident(a_pts) or _all_coincident(b_pts):
        return _identity(matched, "underdetermined")

    # Proper-rotation solve.
    theta, tx, ty, residual = _solve_rotation_translation(a_pts, b_pts, mirror_y=False)

    # Reflection hypothesis: mirror A across x-axis then solve.
    _, _, _, res_mirror = _solve_rotation_translation(a_pts, b_pts, mirror_y=True)

    is_mirrored = False
    # "10x better" = reflection residual is <= 1/10 of proper residual.
    # Use a small floor so two near-zero residuals don't ratio-explode.
    floor = 1e-9
    if res_mirror + floor < residual / 10.0:
        is_mirrored = True

    theta_deg = math.degrees(theta)
    # Normalise raw to (-180, 180].
    while theta_deg <= -180.0:
        theta_deg += 360.0
    while theta_deg > 180.0:
        theta_deg -= 360.0

    # Snap to nearest 90.  Keep in (-180, 180] range, except we emit
    # the canonical 0/90/180/270 convention mod 360 for the snapped
    # value — callers generally want a non-negative integer.
    snapped = int(round(theta_deg / 90.0)) * 90
    snapped %= 360  # 0, 90, 180, 270

    if is_mirrored:
        flag = "mirrored"
    else:
        flag = "ok"

    return TransformResult(
        rotation_deg_raw=theta_deg,
        rotation_deg=snapped,
        offset_x=tx,
        offset_y=ty,
        residual=residual,
        matched_pad_count=matched,
        is_mirrored=is_mirrored,
        is_underdetermined=False,
        quality_flag=flag,
    )
