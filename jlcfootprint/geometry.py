"""Pad geometry primitives shared by the resolver, the solver and the tools.

Everything here is stdlib and frame-explicit.  A ``Pad`` is always in
millimetres, in the footprint's own frame, with KiCad's Y-down axis.  EasyEDA
raw pads are converted exactly once, by :func:`easyeda_pads_to_mm`.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import NamedTuple

# One EasyEDA canvas unit (the classic per-LCSC response format) is 10 mil.
EASYEDA_UNIT_MM = 0.254

# Whether EasyEDA's Y axis must be flipped to match KiCad's Y-down frame for the
# classic per-LCSC response format.  Spec section 6: fixed by the validation gate,
# not assumed; the shadow run reproduced SOT-23 (180) and SOIC-8 -BL (90) against
# recorded responses with no flip, and Task 12 confirms it against JLC's preview.
# (EasyEDA Pro footprint text, used by the crawl and the per-uuid endpoint, is in
# mils with Y up; its parser flips.)
FLIP_EASYEDA_Y = False
# Confirmed against JLC's placement preview on 2026-09-12 with scripts/corner_case (plan
# Task 12): no mirror findings on either side, and the bottom rows (D2, Q5, Q6, U3)
# reproduce upstream's mirror formula, so EasyEDA's Y is read as it comes.


class Pad(NamedTuple):
    """One pad in millimetres in the footprint's own frame (KiCad Y-down).

    ``rotation`` is the pad's own angle relative to the footprint.  ``pin_function``
    is the schematic pin name KiCad stored on the pad ("" when unknown).  ``shape``
    is the KiCad pad shape name when known; a ``custom`` pad's stored size is only
    its anchor, so its extent cannot be judged.
    """

    number: str
    x: float
    y: float
    width: float
    height: float
    rotation: float = 0.0
    pin_function: str = ""
    shape: str = ""


def named_pads(pads: list[Pad]) -> list[Pad]:
    """Return the pads that carry a name (spec section 7.1).

    Unnamed pads are NPTH holes and paste-only copper.  Names are matched as
    strings by the resolver, so lettered connector pads (A1, B12) align too.
    """
    return [pad for pad in pads if pad.number]


def _quantised(value: float) -> float:
    """Round to 0.001 mm and normalise -0.0 to 0.0 so equal geometry hashes equally."""
    return round(float(value), 3) + 0.0


def pad_hash(pads: list[Pad]) -> str:
    """Return a 16-hex-character fingerprint of the pads' names, positions and effective sizes.

    The effective size is the pad's box after its own 90-degree rotation, so
    turning a rectangular pad on its side changes the hash; pin functions do not,
    because polarity is re-derived at resolve time rather than read from a stored
    verdict.  Order independent, 0.001 mm quantised, stable across processes and
    Python versions (blake2b, 8-byte digest).  It is half the key of every stored
    verdict and override, so changing it is a migration.
    """
    rows = []
    for pad in pads:
        _, _, width, height = pad_geom(pad)
        rows.append(
            (
                pad.number,
                _quantised(pad.x),
                _quantised(pad.y),
                _quantised(width),
                _quantised(height),
            )
        )
    payload = json.dumps(sorted(rows)).encode("utf-8")
    return hashlib.blake2b(payload, digest_size=8).hexdigest()


def easyeda_pads_to_mm(raw_pads: list[dict], flip_y: bool | None = None) -> list[Pad]:
    """Convert raw classic-format EasyEDA pads (origin-subtracted canvas units) to millimetre pads.

    A flip negates Y only; the pad's own rotation is left as stored because
    consumers only use it modulo 180 (to swap width and height).
    """
    flip = FLIP_EASYEDA_Y if flip_y is None else flip_y
    sign = -1.0 if flip else 1.0
    return [
        Pad(
            number=str(raw["number"]),
            x=float(raw["x"]) * EASYEDA_UNIT_MM,
            y=sign * float(raw["y"]) * EASYEDA_UNIT_MM,
            width=float(raw["w"]) * EASYEDA_UNIT_MM,
            height=float(raw["h"]) * EASYEDA_UNIT_MM,
            rotation=float(raw.get("rotation", 0.0) or 0.0),
            shape=str(raw.get("shape", "") or ""),
        )
        for raw in raw_pads
    ]


def mirror_y(pads: list[Pad]) -> list[Pad]:
    """Return the pads mirrored across the X axis (y -> -y), for bottom-side footprints.

    The pad's own rotation is left as stored; consumers only use it modulo 180.
    """
    return [pad._replace(y=-pad.y) for pad in pads]


def centroid(pads: list[Pad]) -> tuple[float, float]:
    """Return the mean pad centre; raise ValueError for no pads."""
    if not pads:
        raise ValueError("centroid of no pads")
    count = len(pads)
    return (sum(p.x for p in pads) / count, sum(p.y for p in pads) / count)


def pad_geom(pad: Pad) -> tuple[float, float, float, float]:
    """Return ``(x, y, w, h)`` for the quality checker: the pad's axis-aligned box after its own rotation.

    Exact for multiples of 90 degrees (a swap of width and height at 90 and 270);
    for other angles it is the bounding box of the rotated rectangle.
    """
    theta = math.radians(pad.rotation)
    cos, sin = abs(math.cos(theta)), abs(math.sin(theta))
    width = pad.width * cos + pad.height * sin
    height = pad.width * sin + pad.height * cos
    return (pad.x, pad.y, round(width, 9), round(height, 9))


def ccw_correction(math_rotation_deg: float) -> int:
    """Convert the solver's Y-down math-frame angle to the CPL correction, snapped to 90.

    The solver's angle turns the KiCad pads onto JLC's drawing in the Y-down frame;
    the CPL correction is the same turn as JLC counts it.  JLC's placement preview
    settled the sign on 2026-09-12 (plan Task 12, pass 1): every -BL/-TL multi-pin
    part on the corner-case board needed 270 where the negated angle gave 90, and
    every 180-degree part agreed either way, so the angle is taken as it is.
    Feed it ``TransformResult.rotation_deg`` (or the raw angle; both snap here).
    Exact 45-degree ties round to the even multiple, which is deterministic and
    irrelevant in practice because such a solve fails a pad anyway.  Never feed
    the result back into ``assess_quality``, which works in the math frame.
    """
    return (round(math_rotation_deg / 90.0) * 90) % 360


def crawl_to_cpl(rotation: int) -> int:
    """Convert a crawl-table rotation (the (family, token) table's sense) to the CPL's.

    The crawler's table and ``scripts/sweep_crawl.py`` count the turn that brings
    JLC's drawing onto KiCad's footprint; the CPL counts the turn of KiCad's part
    onto JLC's drawing.  They are negatives of each other (SOIC-8 -BL: 90 in the
    table, 270 in the CPL, confirmed by the preview).
    """
    return (-rotation) % 360
