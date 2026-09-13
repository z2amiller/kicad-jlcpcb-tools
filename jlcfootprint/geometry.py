"""Pad geometry primitives shared by the resolver, the solver and the tools.

Everything here is stdlib and frame-explicit.  A ``Pad`` is always in
millimetres, in the footprint's own frame, with KiCad's Y-down axis.  EasyEDA
raw pads are converted exactly once, by :func:`easyeda_pads_to_mm`.
"""

from __future__ import annotations

import hashlib
import json
from typing import NamedTuple

# One EasyEDA canvas unit is 10 mil.
EASYEDA_UNIT_MM = 0.254

# Whether EasyEDA's Y axis must be flipped to match KiCad's Y-down frame.
# Spec section 6: fixed by the validation gate, not assumed.  Task 12 confirms it.
FLIP_EASYEDA_Y = False


class Pad(NamedTuple):
    """One pad in millimetres in the footprint's own frame (KiCad Y-down)."""

    number: str
    x: float
    y: float
    width: float
    height: float
    rotation: float = 0.0
    pin_function: str = ""


def is_signal_pad(pad: Pad) -> bool:
    """Return True for pads whose number is all digits (spec section 7.1)."""
    return pad.number.isdigit()


def signal_pads(pads: list[Pad]) -> list[Pad]:
    """Return only the numeric pads, in their original order."""
    return [pad for pad in pads if is_signal_pad(pad)]


def pad_hash(pads: list[Pad]) -> str:
    """Return a 16-hex-character hash of number, position and size, 0.001 mm quantised."""
    rows = sorted(
        (
            pad.number,
            round(pad.x, 3),
            round(pad.y, 3),
            round(pad.width, 3),
            round(pad.height, 3),
        )
        for pad in pads
    )
    return hashlib.sha1(json.dumps(rows).encode("utf-8")).hexdigest()[:16]  # noqa: S324


def easyeda_pads_to_mm(raw_pads: list[dict], flip_y: bool | None = None) -> list[Pad]:
    """Convert raw EasyEDA pads (origin-subtracted canvas units) to millimetre pads."""
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
        )
        for raw in raw_pads
    ]


def mirror_y(pads: list[Pad]) -> list[Pad]:
    """Return the pads mirrored across the X axis (y -> -y), for bottom-side footprints."""
    return [pad._replace(y=-pad.y) for pad in pads]


def centroid(pads: list[Pad]) -> tuple[float, float]:
    """Return the mean pad centre."""
    count = len(pads)
    return (sum(p.x for p in pads) / count, sum(p.y for p in pads) / count)


def pad_points(pads: list[Pad]) -> dict[str, tuple[float, float]]:
    """Return ``{number: (x, y)}`` for the solver."""
    return {pad.number: (pad.x, pad.y) for pad in pads}


def pad_geom(pad: Pad) -> tuple[float, float, float, float]:
    """Return ``(x, y, w, h)`` for the quality checker, with w/h swapped for 90 degree pads."""
    width, height = pad.width, pad.height
    if round(pad.rotation) % 180 == 90:
        width, height = height, width
    return (pad.x, pad.y, width, height)


def pad_geoms(pads: list[Pad]) -> dict[str, tuple[float, float, float, float]]:
    """Return ``{number: (x, y, w, h)}`` for the quality checker."""
    return {pad.number: pad_geom(pad) for pad in pads}


def ccw_correction(math_rotation_deg: float) -> int:
    """Convert the solver's Y-down math-frame angle to KiCad's CCW CPL degrees, snapped to 90.

    In a Y-down frame a positive mathematical rotation appears clockwise on screen,
    and KiCad's CPL rotation is counter-clockwise on screen, so the sign flips.
    Spec section 6; the validation gate confirms the sign on SOIC-8 (-BL must give 90).
    """
    return (int(round(-math_rotation_deg / 90.0)) * 90) % 360
