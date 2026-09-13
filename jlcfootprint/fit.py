"""Pad pairing, placement and fit grading (spec section 7.2).

Pure geometry: pair pads by name, solve the placement, transform KiCad pads into
JLC's frame, grade how each JLC pad lands on KiCad copper, and align by shape
alone when the names cannot be trusted.  Everything is in the solver's math
frame (KiCad's Y-down data); ``geometry.ccw_correction`` converts angles for the
CPL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math

from .geometry import Pad, centroid, named_pads, pad_geom
from .quality import assess_quality
from .solver import solve_transform

# A JLC pad lands on copper when its centre sits inside the inner part of the KiCad pad
# (this fraction of the pad's half-size each way) and the overlap is at least MIN_OVERLAP
# of the smaller pad.  The centre rule makes 0402 pads under an 0603 part "tight"; the
# overlap rule tolerates land patterns that differ in size, such as KiCad's small SOD-323
# pads.
CENTRE_TOLERANCE = 0.8
MIN_OVERLAP = 0.5
# Pads this much bigger on one side are reported as "fits, KiCad pads larger/smaller".
SIZE_RATIO = 1.5


@dataclass
class Placement:
    """A solved alignment in the solver's math frame.

    ``rotation_deg`` is snapped to 90 and ``offset_x/y`` is the translation that keeps
    the matched centroids together under that snapped angle, which is what a CPL
    position means and what keeps a pin-1-origin footprint from drifting.
    """

    rotation_deg: int
    offset_x: float
    offset_y: float
    is_mirrored: bool
    is_underdetermined: bool
    residual: float
    angular_rms: float
    matched: int


@dataclass
class FitReport:
    """What the fit pass found: the grade plus the notes and metrics that explain it."""

    fit: str  # fits | fits_larger_pads | fits_tight | count | pitch
    notes: list[str] = field(default_factory=list)
    missing_central_pad: bool = False
    overlap_min: float = 0.0
    overlap_mean: float = 0.0


def _area(pad: Pad) -> float:
    """Return the pad's area."""
    return pad.width * pad.height


def pair_by_name(
    kicad_pads: list[Pad], jlc_pads: list[Pad]
) -> tuple[dict[str, Pad], dict[str, Pad], list[Pad]]:
    """Pair pads by name; return the matched KiCad and JLC dicts and the unmatched JLC pads.

    When a name covers several pads on one side (KiCad numbers a SOT-223 tab as a
    second pad ``2``, and a DPAK's tab and its stub pin both as ``2``), the pads
    are paired by closest area, so a pin meets a pin and a tab meets a tab; the
    leftovers go to the position pass.  Keys are ``name`` then ``name#1``, ...
    """
    groups_k: dict[str, list[Pad]] = {}
    for pad in named_pads(kicad_pads):
        groups_k.setdefault(pad.number, []).append(pad)
    groups_j: dict[str, list[Pad]] = {}
    for pad in named_pads(jlc_pads):
        groups_j.setdefault(pad.number, []).append(pad)
    kicad: dict[str, Pad] = {}
    jlc: dict[str, Pad] = {}
    leftovers: list[Pad] = []
    for name, group_j in groups_j.items():
        group_k = list(groups_k.get(name, []))
        for index, pad_j in enumerate(sorted(group_j, key=_area)):
            if not group_k:
                leftovers.append(pad_j)
                continue
            pad_k = min(
                group_k, key=lambda p, target=_area(pad_j): abs(_area(p) - target)
            )
            group_k.remove(pad_k)
            key = name if index == 0 else f"{name}#{index}"
            kicad[key] = pad_k
            jlc[key] = pad_j
    return kicad, jlc, leftovers


def align(kicad: dict[str, Pad], jlc: dict[str, Pad]) -> Placement:
    """Solve the rigid alignment of pads matched by dict key and return the placement."""
    transform = solve_transform(
        {key: (pad.x, pad.y) for key, pad in kicad.items()},
        {key: (pad.x, pad.y) for key, pad in jlc.items()},
    )
    quality = assess_quality(
        {key: pad_geom(pad) for key, pad in kicad.items()},
        {key: pad_geom(pad) for key, pad in jlc.items()},
        transform,
    )
    kx, ky = centroid(list(kicad.values()))
    jx, jy = centroid(list(jlc.values()))
    theta = math.radians(transform.rotation_deg)
    return Placement(
        rotation_deg=transform.rotation_deg,
        offset_x=jx - (kx * math.cos(theta) - ky * math.sin(theta)),
        offset_y=jy - (kx * math.sin(theta) + ky * math.cos(theta)),
        is_mirrored=transform.is_mirrored,
        is_underdetermined=transform.is_underdetermined,
        residual=transform.residual,
        angular_rms=quality.angular_rms_deg,
        matched=len(kicad),
    )


def transformed(pad: Pad, placement: Placement) -> tuple[float, float, float, float]:
    """Return a KiCad pad's ``(x, y, w, h)`` after the placement (solver's math frame)."""
    theta = math.radians(placement.rotation_deg)
    cos, sin = math.cos(theta), math.sin(theta)
    x = pad.x * cos - pad.y * sin + placement.offset_x
    y = pad.x * sin + pad.y * cos + placement.offset_y
    _, _, width, height = pad_geom(pad)
    if placement.rotation_deg % 180 == 90:
        width, height = height, width
    return x, y, width, height


def pad_fit(
    kicad_geom: tuple[float, float, float, float], jlc_pad: Pad
) -> tuple[int, float]:
    """Grade one JLC pad against a transformed KiCad pad: 2 fits, 1 tight, 0 misses.

    Returns the grade and the overlap ratio (intersection over the smaller pad's
    area).  Tight means the pads overlap well but the JLC pad's centre falls outside
    the inner part of the KiCad pad, so the part sits at the edge of its copper.
    """
    ax, ay, aw, ah = kicad_geom
    bx, by, bw, bh = pad_geom(jlc_pad)
    ix = max(0.0, min(ax + aw / 2, bx + bw / 2) - max(ax - aw / 2, bx - bw / 2))
    iy = max(0.0, min(ay + ah / 2, by + bh / 2) - max(ay - ah / 2, by - bh / 2))
    smaller = min(aw * ah, bw * bh)
    overlap = (ix * iy) / smaller if smaller > 0 else 0.0
    if overlap < MIN_OVERLAP:
        return 0, overlap
    centred = (
        abs(bx - ax) <= CENTRE_TOLERANCE * aw / 2
        and abs(by - ay) <= CENTRE_TOLERANCE * ah / 2
    )
    return (2 if centred else 1), overlap


def assess_fit(
    kicad: dict[str, Pad],
    jlc: dict[str, Pad],
    kicad_all: list[Pad],
    jlc_rest: list[Pad],
    placement: Placement,
    count_kicad: int,
    count_jlc: int,
) -> FitReport:
    """Grade the fit: every JLC pad must land on KiCad copper after the placement.

    Matched pads pair by key.  Each remaining JLC pad (a tab numbered differently, a
    merged connector pin) pairs with the nearest transformed KiCad pad, which may be
    reused; extra KiCad copper is never a misfit.  An unmatched JLC pad that lands on
    nothing is a misfit when it sits at the periphery (a pin the footprint lacks) but
    only a warning when it contains the JLC pad centroid (an exposed pad the footprint
    lacks).  Custom-shaped KiCad pads have an unknown extent and are not judged.
    """
    report = FitReport(fit="fits")
    placed = [(pad, transformed(pad, placement)) for pad in kicad_all]
    pairs = [
        (kicad[key], transformed(kicad[key], placement), jlc[key], True)
        for key in kicad
    ]
    for jlc_pad in jlc_rest:
        if not placed:
            break
        pad, geom = min(
            placed,
            key=lambda item: math.hypot(item[1][0] - jlc_pad.x, item[1][1] - jlc_pad.y),
        )
        pairs.append((pad, geom, jlc_pad, False))
    grades = [
        (2, 1.0) if kicad_pad.shape == "custom" else pad_fit(geom, jlc_pad)
        for kicad_pad, geom, jlc_pad, _ in pairs
    ]
    if any(kicad_pad.shape == "custom" for kicad_pad, _, _, _ in pairs):
        report.notes.append("custom-shaped KiCad pad not checked for fit")
    report.overlap_min = min(overlap for _, overlap in grades)
    report.overlap_mean = sum(overlap for _, overlap in grades) / len(grades)
    jlc_pads = [jlc_pad for _, _, jlc_pad, _ in pairs]
    cx, cy = centroid(jlc_pads)
    mismatch = "count" if count_kicad != count_jlc else "pitch"
    missing_pins: list[str] = []
    missing_central: list[str] = []
    tight: list[str] = []
    for (_, _, jlc_pad, matched), (grade, _) in zip(pairs, grades):
        if grade == 2:
            continue
        if grade == 1:
            tight.append(jlc_pad.number)
            continue
        if matched:
            report.fit = mismatch
            return report
        _, _, width, height = pad_geom(jlc_pad)
        central = abs(cx - jlc_pad.x) <= width / 2 and abs(cy - jlc_pad.y) <= height / 2
        (missing_central if central else missing_pins).append(jlc_pad.number)
    if missing_pins:
        report.notes.append(f"JLC pads {', '.join(missing_pins)} land on no copper")
        report.fit = mismatch
        return report
    if missing_central:
        report.notes.append(
            f"JLC part has a central pad ({', '.join(missing_central)}) your footprint lacks"
        )
        report.missing_central_pad = True
    if tight:
        report.notes.append(
            f"JLC pads {', '.join(tight)} sit at the edge of your pads; check the placement preview"
        )
        report.fit = "fits_tight"
        return report
    matched_pairs = [(k, j) for k, _, j, matched in pairs if matched]
    if any(_area(k) >= SIZE_RATIO * _area(j) for k, j in matched_pairs):
        report.fit = "fits_larger_pads"
    elif any(_area(j) >= SIZE_RATIO * _area(k) for k, j in matched_pairs):
        report.notes.append("KiCad pads are smaller than JLC's land pattern")
    return report


def shape_alignment(kicad: list[Pad], jlc: list[Pad]) -> tuple[int, int]:
    """Align the two pad patterns ignoring names; return (math-frame rotation, JLC pads that land).

    Used only after a by-name alignment failed with equal pad counts: when the
    package fits at some angle but the numbers do not line up, the KiCad footprint's
    pin numbering differs from JLC's part (the multi-pin form of "correct by accident").
    """
    kx, ky = centroid(kicad)
    jx, jy = centroid(jlc)
    best: tuple[int, int, float] | None = None
    for rotation in (0, 90, 180, 270):
        theta = math.radians(rotation)
        cos, sin = math.cos(theta), math.sin(theta)
        placed = []
        for pad in kicad:
            x, y = pad.x - kx, pad.y - ky
            _, _, width, height = pad_geom(pad)
            if rotation % 180 == 90:
                width, height = height, width
            placed.append(
                (x * cos - y * sin + jx, x * sin + y * cos + jy, width, height)
            )
        landed = 0
        distance = 0.0
        for jlc_pad in jlc:
            geom = min(
                placed, key=lambda g: math.hypot(g[0] - jlc_pad.x, g[1] - jlc_pad.y)
            )
            landed += pad_fit(geom, jlc_pad)[0] > 0
            distance += math.hypot(geom[0] - jlc_pad.x, geom[1] - jlc_pad.y)
        if best is None or (landed, -distance) > (best[1], -best[2]):
            best = (rotation, landed, distance)
    return best[0], best[1]
