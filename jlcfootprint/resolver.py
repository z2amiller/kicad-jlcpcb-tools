"""Per-part rotation, fit and polarity verdicts (spec section 7).

Pure and stdlib only.  The caller supplies KiCad pads in the footprint's own
frame, EasyEDA pads already converted to millimetres, and the symbol pins.
Multi-pin parts align by pad name; polarized two-pad parts align by terminal
meaning and never by pad number; non-polar two-pad parts align by axis.  Fit
is the fraction of each JLC pad that lands on KiCad copper after alignment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math

from .easyeda_parse import (
    PIN1_ANODE_LABELS,
    PIN1_CATHODE_LABELS,
    SymbolPin,
    pin1_polarity,
)
from .geometry import Pad, ccw_correction, centroid, named_pads, pad_geom
from .naming import (
    CATHODE_PIN1_FAMILIES,
    POLARIZED_CAP_FAMILIES,
    extract_family,
    extract_orientation_tokens,
    parse_package_name,
)
from .quality import assess_quality
from .solver import solve_transform

_CATHODE_TOKENS = frozenset({"K", "C", "CATHODE", "CAT", "NEG"})
_ANODE_TOKENS = frozenset({"A", "ANODE", "AN", "POS"})
_CAP_LABELS = frozenset({"+", "-", "POS", "NEG"})

# A JLC pad lands on copper when its centre sits inside the inner part of the KiCad pad
# (this fraction of the pad's half-size each way) and the overlap is at least MIN_OVERLAP
# of the smaller pad.  The centre rule fails 0402 pads under an 0603 part; the overlap
# rule tolerates land patterns that differ in size, such as KiCad's small SOD-323 pads.
CENTRE_TOLERANCE = 0.8
MIN_OVERLAP = 0.5
# Pads this much bigger on one side are reported as "fits, KiCad pads larger/smaller".
SIZE_RATIO = 1.5
# Angular RMS at or above this means the pad pattern does not match under any rotation.
RED_ANGULAR_RMS = 10.0

CHECKERBOARD_NOTE = (
    "no JLC footprint data for this part (expect a checkerboard in the preview)"
)
YELLOW_NOTE = (
    "JLC's pin-1 marker will sit on the other terminal; numbering difference, "
    "not a rotation error; do not renumber the footprint"
)


@dataclass
class Verdict:
    """One part's verdict; mirrors the footprint_verdict columns in the spec."""

    status: str = "unknown"  # green | yellow | red | unknown
    fit: str = "no_data"  # fits | fits_larger_pads | fits_tight | count | pitch | numbering | mirror | no_data
    rotation: int | None = None
    method: str = "none"  # geometry | polarity | axis | none
    confidence: str = "none"  # high | medium | low | none
    name_rotation: int | None = None
    polarity_light: str | None = None  # green | yellow | unknown; None for non-polar
    pad_count_kicad: int = 0
    pad_count_jlc: int = 0
    matched_pads: int = 0
    missing_central_pad: bool = False
    overlap_min: float = 0.0
    overlap_mean: float = 0.0
    angular_rms: float = 0.0
    residual_mm: float = 0.0
    notes: list[str] = field(default_factory=list)

    @property
    def note_text(self) -> str:
        """Return the notes joined for storage and display."""
        return "; ".join(self.notes)

    def unresolved(self, status: str, fit: str, note: str) -> Verdict:
        """Mark the verdict as carrying no derived rotation and return it."""
        self.status = status
        self.fit = fit
        self.rotation = None
        self.method = "none"
        self.notes.append(note)
        return self


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


def normalise_function(text: str) -> str:
    """Reduce a KiCad pin function such as ``K_1`` or ``anode`` to a bare upper-case token."""
    token = text.strip().upper()
    if token in ("+", "-"):
        return token
    letters = ""
    for char in token:
        if not char.isalpha():
            break
        letters += char
    return letters


def terminal_of(pad: Pad) -> str:
    """Return ``cathode``, ``anode``, ``positive``, ``negative`` or ``""`` from the pin function."""
    token = normalise_function(pad.pin_function)
    if token in _CATHODE_TOKENS:
        return "cathode"
    if token in _ANODE_TOKENS:
        return "anode"
    if token == "+":
        return "positive"
    if token == "-":
        return "negative"
    return ""


def part_kind(
    package_name: str,
    kicad_footprint_name: str,
    kicad_pads: list[Pad],
    symbol_pins: list[SymbolPin],
) -> str:
    """Return ``polar_cap``, ``diode`` or ``other`` (spec section 7.1)."""
    family = extract_family(package_name).upper()
    footprint = kicad_footprint_name.rsplit(":", 1)[-1].upper()
    terminals = {terminal_of(pad) for pad in kicad_pads}
    labels = {pin.label.strip().upper() for pin in symbol_pins}
    polarity_tokens = {
        t for t in extract_orientation_tokens(package_name) if t in ("FD", "RD")
    }
    if (
        family in POLARIZED_CAP_FAMILIES
        or (family.startswith("CAP") and polarity_tokens)
        or footprint.startswith(("CP_", "C_ELEC", "TANTALUM"))
        or terminals & {"positive", "negative"}
        or labels & _CAP_LABELS
    ):
        return "polar_cap"
    diode_labels = (PIN1_CATHODE_LABELS | PIN1_ANODE_LABELS) - _CAP_LABELS
    if (
        family in CATHODE_PIN1_FAMILIES
        or footprint.startswith(("D_", "LED_"))
        or terminals & {"cathode", "anode"}
        or labels & diode_labels
    ):
        return "diode"
    return "other"


def _transformed(pad: Pad, transform) -> tuple[float, float, float, float]:
    """Return a KiCad pad's ``(x, y, w, h)`` after the solved transform (solver's math frame)."""
    theta = math.radians(transform.rotation_deg)
    cos, sin = math.cos(theta), math.sin(theta)
    x = pad.x * cos - pad.y * sin + transform.offset_x
    y = pad.x * sin + pad.y * cos + transform.offset_y
    _, _, width, height = pad_geom(pad)
    if transform.rotation_deg % 180 == 90:
        width, height = height, width
    return x, y, width, height


def _pad_fit(
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


def _align(kicad: dict[str, Pad], jlc: dict[str, Pad], verdict: Verdict):
    """Solve and assess on pads matched by dict key, filling the verdict's metrics."""
    transform = solve_transform(
        {key: (pad.x, pad.y) for key, pad in kicad.items()},
        {key: (pad.x, pad.y) for key, pad in jlc.items()},
    )
    quality = assess_quality(
        {key: pad_geom(pad) for key, pad in kicad.items()},
        {key: pad_geom(pad) for key, pad in jlc.items()},
        transform,
    )
    verdict.residual_mm = transform.residual
    verdict.angular_rms = quality.angular_rms_deg
    verdict.matched_pads = len(kicad)
    return transform, quality


def _assess_fit(
    kicad: dict[str, Pad],
    jlc: dict[str, Pad],
    kicad_all: list[Pad],
    jlc_rest: list[Pad],
    transform,
    verdict: Verdict,
) -> str:
    """Return the fit: every JLC pad must land on KiCad copper after the transform.

    Matched pads pair by key.  Each remaining JLC pad (a tab numbered differently, a
    merged connector pin) pairs with the nearest transformed KiCad pad, which may be
    reused; extra KiCad copper is never a misfit.  An unmatched JLC pad that lands on
    nothing is a misfit when it sits at the periphery (a pin the footprint lacks) but
    only a warning when it sits at the centre (an exposed pad the footprint lacks).
    """
    placed = [(pad, _transformed(pad, transform)) for pad in kicad_all]
    pairs = [
        (kicad[key], _transformed(kicad[key], transform), jlc[key], True)
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
    fits = [
        (2, 1.0) if kicad_pad.shape == "custom" else _pad_fit(geom, jlc_pad)
        for kicad_pad, geom, jlc_pad, _ in pairs
    ]
    if any(kicad_pad.shape == "custom" for kicad_pad, _, _, _ in pairs):
        verdict.notes.append("custom-shaped KiCad pad not checked for fit")
    verdict.overlap_min = min(overlap for _, overlap in fits)
    verdict.overlap_mean = sum(overlap for _, overlap in fits) / len(fits)
    jlc_pads = [jlc_pad for _, _, jlc_pad, _ in pairs]
    cx = sum(p.x for p in jlc_pads) / len(jlc_pads)
    cy = sum(p.y for p in jlc_pads) / len(jlc_pads)
    missing_pins: list[str] = []
    missing_central: list[str] = []
    tight: list[str] = []
    for (_kicad_pad, _, jlc_pad, matched), (grade, _) in zip(pairs, fits):
        if grade == 2:
            continue
        if grade == 1:
            tight.append(jlc_pad.number)
            continue
        if matched:
            return (
                "count" if verdict.pad_count_kicad != verdict.pad_count_jlc else "pitch"
            )
        central = (
            abs(cx - jlc_pad.x) <= jlc_pad.width / 2
            and abs(cy - jlc_pad.y) <= jlc_pad.height / 2
        )
        (missing_central if central else missing_pins).append(jlc_pad.number)
    if missing_pins:
        verdict.notes.append(f"JLC pads {', '.join(missing_pins)} land on no copper")
        return "count" if verdict.pad_count_kicad != verdict.pad_count_jlc else "pitch"
    if missing_central:
        verdict.notes.append(
            f"JLC part has a central pad ({', '.join(missing_central)}) your footprint lacks"
        )
        verdict.missing_central_pad = True
    if tight:
        verdict.notes.append(
            f"JLC pads {', '.join(tight)} sit at the edge of your pads; check the placement preview"
        )
        return "fits_tight"
    if any(_area(k) >= SIZE_RATIO * _area(j) for k, _, j, _ in pairs):
        return "fits_larger_pads"
    if any(_area(j) >= SIZE_RATIO * _area(k) for k, _, j, _ in pairs):
        verdict.notes.append("KiCad pads are smaller than JLC's land pattern")
    return "fits"


def _shape_alignment(kicad: list[Pad], jlc: list[Pad]) -> tuple[int, int]:
    """Align the two pad patterns ignoring names; return (CCW rotation, JLC pads that land).

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
            landed += _pad_fit(geom, jlc_pad)[0] > 0
            distance += math.hypot(geom[0] - jlc_pad.x, geom[1] - jlc_pad.y)
        if best is None or (landed, -distance) > (best[1], -best[2]):
            best = (rotation, landed, distance)
    return ccw_correction(best[0]), best[1]


def _mismatch_note(verdict: Verdict) -> str:
    """Describe a red fit in the UI vocabulary."""
    if verdict.pad_count_kicad != verdict.pad_count_jlc:
        return (
            f"does not fit: {verdict.pad_count_kicad} vs {verdict.pad_count_jlc} pads"
        )
    return (
        f"does not fit: pitch (worst JLC pad only {verdict.overlap_min:.0%} on copper)"
    )


def _resolve_multi_pin(
    kicad_pads: list[Pad], jlc_pads: list[Pad], verdict: Verdict
) -> Verdict:
    """Align by pad name (spec section 7.2)."""
    kicad, jlc, jlc_rest = pair_by_name(kicad_pads, jlc_pads)
    common = sorted(kicad)
    if len(common) < 2:
        return verdict.unresolved(
            "unknown", "no_data", "fewer than two matching pad names"
        )
    transform, quality = _align(kicad, jlc, verdict)
    if transform.is_mirrored:
        return verdict.unresolved(
            "red",
            "mirror",
            "pin order reversed: the KiCad footprint's pin numbering runs the opposite way to JLC's part",
        )
    verdict.fit = _assess_fit(
        kicad, jlc, named_pads(kicad_pads), jlc_rest, transform, verdict
    )
    if quality.angular_rms_deg >= RED_ANGULAR_RMS or verdict.fit in ("count", "pitch"):
        if verdict.fit not in ("count", "pitch"):
            verdict.fit = (
                "count" if verdict.pad_count_kicad != verdict.pad_count_jlc else "pitch"
            )
        if verdict.pad_count_kicad == verdict.pad_count_jlc:
            angle, landed = _shape_alignment(
                named_pads(kicad_pads), named_pads(jlc_pads)
            )
            if landed == verdict.pad_count_jlc:
                return verdict.unresolved(
                    "red",
                    "numbering",
                    f"pin numbering differs from JLC's part; the package itself aligns at {angle}°",
                )
        return verdict.unresolved("red", verdict.fit, _mismatch_note(verdict))
    verdict.rotation = ccw_correction(transform.rotation_deg)
    verdict.method = "geometry"
    verdict.confidence = "high"
    if len(common) == 2:
        verdict.confidence = "medium"
        verdict.notes.append(
            "aligned on two shared pad names; remaining pads matched by position"
        )
    if verdict.name_rotation is not None and verdict.name_rotation != verdict.rotation:
        verdict.confidence = "medium"
        verdict.notes.append(
            f"KiCad footprint drawn non-standard; name says {verdict.name_rotation}°"
        )
    if verdict.pad_count_kicad != verdict.pad_count_jlc:
        verdict.notes.append(
            f"pad counts differ ({verdict.pad_count_kicad} vs {verdict.pad_count_jlc}) "
            f"but the {len(common)} shared names align"
        )
    verdict.status = (
        "yellow"
        if verdict.missing_central_pad or verdict.fit == "fits_tight"
        else "green"
    )
    return verdict


def kicad_reference_pad(pads: list[Pad], reference: str) -> tuple[Pad | None, bool]:
    """Return the KiCad pad carrying ``reference`` and whether pad 1 had to be assumed."""
    opposite = {"cathode": "anode", "positive": "negative"}[reference]
    named = {terminal_of(pad): pad for pad in pads if terminal_of(pad)}
    if reference in named:
        return named[reference], False
    if opposite in named and len(pads) == 2:
        return next(pad for pad in pads if pad is not named[opposite]), False
    for pad in pads:
        if pad.number == "1":
            return pad, True
    return None, True


def _side_of(pad: Pad, pads: list[Pad]) -> str | None:
    """Return ``left`` or ``right`` for one of two pads, or None when their axis is vertical."""
    other = next(p for p in pads if p is not pad)
    dx, dy = pad.x - other.x, pad.y - other.y
    if abs(dx) < abs(dy):
        return None
    return "left" if dx < 0 else "right"


def token_reference_side(package_name: str, reference: str) -> str | None:
    """Return ``left``, ``right``, ``none`` (bidirectional) or None (no token) from FD/RD/BI.

    Forward direction puts the anode or positive terminal on the left of the
    EasyEDA drawing, which is JLC's zero orientation.
    """
    polarity = [
        t for t in extract_orientation_tokens(package_name) if t in ("FD", "RD", "BI")
    ]
    if not polarity:
        return None
    token = polarity[-1]
    if token == "BI":
        return "none"
    positive_side = "left" if token == "FD" else "right"
    if reference == "cathode":
        return "right" if positive_side == "left" else "left"
    return positive_side


def label_reference_pad(
    jlc_pads: list[Pad], polarity: str | None, reference: str
) -> Pad | None:
    """Return the EasyEDA pad carrying ``reference`` from the symbol's pin-1 label, if known."""
    if polarity is None:
        return None
    pad1 = next((p for p in jlc_pads if p.number == "1"), None)
    if pad1 is None:
        return None
    other = next((p for p in jlc_pads if p is not pad1), None)
    if other is None:
        return None
    pin1_is_reference = (polarity == "K") == (reference == "cathode")
    return pad1 if pin1_is_reference else other


def _pin1_meaning(pads: list[Pad], kind: str) -> str | None:
    """Return 'A' or 'K' for what the KiCad side means by pad 1, by function or convention."""
    pad1 = next((p for p in pads if p.number == "1"), None)
    if pad1 is None:
        return None
    terminal = terminal_of(pad1)
    if terminal in ("cathode", "negative"):
        return "K"
    if terminal in ("anode", "positive"):
        return "A"
    return "K" if kind == "diode" else "A"


def _two_pad_fit(
    kicad: dict[str, Pad], jlc: dict[str, Pad], transform, verdict: Verdict
) -> bool:
    """Fill the fit for a two-pad alignment; return True when the part fits."""
    verdict.fit = _assess_fit(kicad, jlc, list(kicad.values()), [], transform, verdict)
    if verdict.fit in ("count", "pitch"):
        verdict.unresolved("red", "pitch", _mismatch_note(verdict))
        return False
    return True


def _resolve_polarized(
    kicad_sig: list[Pad],
    jlc_sig: list[Pad],
    verdict: Verdict,
    kind: str,
    package_name: str,
    symbol_pins: list[SymbolPin],
    polarity_source: str = "symbol",
) -> Verdict:
    """Align a polarized two-pad part by terminal meaning, never by pad number (spec 7.3)."""
    reference = "cathode" if kind == "diode" else "positive"
    kicad_ref, assumed = kicad_reference_pad(kicad_sig, reference)
    if kicad_ref is None:
        return verdict.unresolved("unknown", "no_data", "no pad 1 on the KiCad side")
    if assumed:
        verdict.notes.append(
            f"assumed KiCad pad 1 = {'K' if reference == 'cathode' else '+'}"
        )
    polarity = pin1_polarity(symbol_pins)
    side = token_reference_side(package_name, reference)
    if side == "none":
        verdict.notes.append("bidirectional part; aligned by axis")
        return _resolve_axis(kicad_sig, jlc_sig, verdict)
    token_pad = None
    if side is not None:
        token_pad = next((p for p in jlc_sig if _side_of(p, jlc_sig) == side), None)
        if token_pad is None:
            verdict.notes.append(
                "name token could not be applied: JLC pads are drawn vertical"
            )
    label_pad = label_reference_pad(jlc_sig, polarity, reference)
    if token_pad is not None and label_pad is not None and token_pad is not label_pad:
        if polarity_source != "symbol":
            verdict.notes.append(
                "seeded pin-1 polarity (per footprint) disagrees with the name token; token used"
            )
            label_pad = None
            polarity = None
        else:
            return verdict.unresolved(
                "red",
                "no_data",
                "EasyEDA data inconsistent: name token and symbol pin-1 label disagree",
            )
    jlc_ref = token_pad or label_pad
    if jlc_ref is None:
        return verdict.unresolved(
            "unknown", "no_data", "polarity unknown; check in JLC preview"
        )
    kicad_other = next(p for p in kicad_sig if p is not kicad_ref)
    jlc_other = next(p for p in jlc_sig if p is not jlc_ref)
    kicad = {"ref": kicad_ref, "other": kicad_other}
    jlc = {"ref": jlc_ref, "other": jlc_other}
    transform, _ = _align(kicad, jlc, verdict)
    if not _two_pad_fit(kicad, jlc, transform, verdict):
        return verdict
    verdict.rotation = ccw_correction(transform.rotation_deg)
    verdict.method = "polarity"
    verdict.confidence = "medium" if assumed else "high"
    kicad_pin1 = _pin1_meaning(kicad_sig, kind)
    if polarity is None or kicad_pin1 is None:
        verdict.polarity_light = "unknown"
        verdict.status = "green"
    elif polarity == kicad_pin1:
        verdict.polarity_light = "green"
        verdict.status = "green"
    else:
        verdict.polarity_light = "yellow"
        verdict.status = "yellow"
        verdict.notes.append(YELLOW_NOTE)
    if verdict.fit == "fits_tight":
        verdict.status = "yellow"
    return verdict


def _resolve_axis(
    kicad_sig: list[Pad], jlc_sig: list[Pad], verdict: Verdict
) -> Verdict:
    """Align a non-polar two-pad part by axis only; 180 degrees is irrelevant (spec 7.4)."""
    kicad = {"a": kicad_sig[0], "b": kicad_sig[1]}
    best = None
    for jlc in ({"a": jlc_sig[0], "b": jlc_sig[1]}, {"a": jlc_sig[1], "b": jlc_sig[0]}):
        transform, _ = _align(kicad, jlc, Verdict())
        if best is None or transform.residual < best[0].residual:
            best = (transform, jlc)
    transform, jlc = best
    _align(kicad, jlc, verdict)
    if not _two_pad_fit(kicad, jlc, transform, verdict):
        return verdict
    verdict.rotation = ccw_correction(transform.rotation_deg) % 180
    verdict.method = "axis"
    verdict.confidence = "high"
    verdict.status = "yellow" if verdict.fit == "fits_tight" else "green"
    return verdict


def resolve(
    kicad_pads: list[Pad],
    kicad_footprint_name: str,
    record_status: str,
    package_name: str,
    jlc_pads: list[Pad],
    symbol_pins: list[SymbolPin],
    polarity_source: str = "symbol",
) -> Verdict:
    """Return the verdict for one part (spec section 7).

    ``kicad_pads`` are in the footprint's own frame (bottom-side parts already
    un-mirrored by the caller); ``jlc_pads`` are millimetres in KiCad's frame.
    ``polarity_source`` is ``symbol`` when ``symbol_pins`` came from the part's own
    symbol and ``seed-puuid`` when they were seeded per footprint from the crawl.
    """
    verdict = Verdict()
    if record_status == "none":
        return verdict.unresolved("unknown", "no_data", CHECKERBOARD_NOTE)
    if record_status != "ok":
        return verdict.unresolved(
            "unknown", "no_data", "EasyEDA fetch failed; will retry"
        )
    kicad_named = named_pads(kicad_pads)
    jlc_named = named_pads(jlc_pads)
    verdict.pad_count_kicad = len(kicad_named)
    verdict.pad_count_jlc = len(jlc_named)
    kind = part_kind(package_name, kicad_footprint_name, kicad_pads, symbol_pins)
    parsed = parse_package_name(package_name, "Diodes" if kind == "diode" else None)
    if parsed.rotation_source == "naming_rule":
        verdict.name_rotation = parsed.rotation_correction
    if min(len(kicad_named), len(jlc_named)) < 2:
        return verdict.unresolved(
            "unknown", "no_data", "fewer than two named pads on one side"
        )
    if len(kicad_named) == 2 and len(jlc_named) == 2:
        marked = kind != "other" or token_reference_side(package_name, "positive") in (
            "left",
            "right",
        )
        if not marked:
            return _resolve_axis(kicad_named, jlc_named, verdict)
        if kind == "other":
            verdict.notes.append(
                "orientation token on a non-polar part: pin 1 kept where JLC draws it"
            )
        return _resolve_polarized(
            kicad_named,
            jlc_named,
            verdict,
            kind,
            package_name,
            symbol_pins,
            polarity_source,
        )
    return _resolve_multi_pin(kicad_pads, jlc_pads, verdict)
