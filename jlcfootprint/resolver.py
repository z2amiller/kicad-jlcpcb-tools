"""Per-part rotation, fit and polarity verdicts (spec section 7).

Pure and stdlib only.  The caller supplies KiCad pads in the footprint's own
frame, EasyEDA pads already converted to millimetres, and the symbol pins.
Multi-pin parts align by pad name (``fit``); polarized two-pad parts align by
terminal meaning (``polarity``) and never by pad number; non-polar two-pad
parts align by axis.  Fit is graded per JLC pad after the placement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math

from .easyeda_parse import SymbolPin, pin1_polarity
from .fit import FitReport, Placement, align, assess_fit, pair_by_name, shape_alignment
from .geometry import Pad, ccw_correction, crawl_to_cpl, named_pads
from .naming import parse_package_name
from .polarity import (
    CONVENTION,
    REFERENCE_TERMINAL,
    kicad_reference_pad,
    label_reference_pad,
    normalise_function,
    part_kind,
    pin1_meaning,
    side_of,
    terminal_of,
    token_reference_side,
)

__all__ = [
    "Verdict",
    "kicad_reference_pad",
    "label_reference_pad",
    "normalise_function",
    "pair_by_name",
    "part_kind",
    "resolve",
    "terminal_of",
    "token_reference_side",
]

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
    non_polar: bool = False  # True when 180 degrees apart is the same placement
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

    def take_placement(self, placement: Placement) -> None:
        """Copy the placement's metrics."""
        self.residual_mm = placement.residual
        self.angular_rms = placement.angular_rms
        self.matched_pads = placement.matched

    def take_fit(self, report: FitReport) -> None:
        """Copy the fit report's grade, notes and metrics."""
        self.fit = report.fit
        self.missing_central_pad = report.missing_central_pad
        self.overlap_min = report.overlap_min
        self.overlap_mean = report.overlap_mean
        self.notes.extend(report.notes)


def _mismatch_note(verdict: Verdict, fit: str) -> str:
    """Describe a red fit in the UI vocabulary, following the fit report's decision."""
    if fit == "count":
        return (
            f"does not fit: {verdict.pad_count_kicad} vs {verdict.pad_count_jlc} pads"
        )
    return f"does not fit: pitch (a JLC pad misses its KiCad pad; worst overlap {verdict.overlap_min:.0%})"


def _resolve_multi_pin(
    kicad_pads: list[Pad], jlc_pads: list[Pad], verdict: Verdict
) -> Verdict:
    """Align by pad name (spec section 7.2)."""
    kicad, jlc, jlc_rest = pair_by_name(kicad_pads, jlc_pads)
    if len(kicad) < 2:
        return verdict.unresolved(
            "unknown", "no_data", "fewer than two matching pad names"
        )
    placement = align(kicad, jlc)
    verdict.take_placement(placement)
    if placement.is_underdetermined:
        return verdict.unresolved("unknown", "no_data", "pad geometry is degenerate")
    if placement.is_mirrored:
        return verdict.unresolved(
            "red",
            "mirror",
            "pin order reversed: the KiCad footprint's pin numbering runs the opposite way to JLC's part",
        )
    report = assess_fit(
        kicad,
        jlc,
        named_pads(kicad_pads),
        jlc_rest,
        placement,
        verdict.pad_count_kicad,
        verdict.pad_count_jlc,
    )
    verdict.take_fit(report)
    if report.fit in ("count", "pitch"):
        if verdict.pad_count_kicad == verdict.pad_count_jlc:
            angle, landed = shape_alignment(
                named_pads(kicad_pads), named_pads(jlc_pads)
            )
            if landed == verdict.pad_count_jlc:
                return verdict.unresolved(
                    "red",
                    "numbering",
                    "pin numbering differs from JLC's part; the package itself aligns at "
                    f"{ccw_correction(angle)}°",
                )
        return verdict.unresolved(
            "red", report.fit, _mismatch_note(verdict, report.fit)
        )
    verdict.rotation = ccw_correction(placement.rotation_deg)
    verdict.method = "geometry"
    verdict.confidence = "high"
    if len(kicad) == 2:
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
            f"but the {len(kicad)} shared names align"
        )
    verdict.status = (
        "yellow"
        if verdict.missing_central_pad or verdict.fit == "fits_tight"
        else "green"
    )
    return verdict


def _two_pad_fit(
    kicad: dict[str, Pad], jlc: dict[str, Pad], placement: Placement, verdict: Verdict
) -> bool:
    """Fill the fit for a two-pad alignment; return True when the part fits."""
    report = assess_fit(kicad, jlc, list(kicad.values()), [], placement, 2, 2)
    verdict.take_fit(report)
    if report.fit in ("count", "pitch"):
        verdict.unresolved("red", "pitch", _mismatch_note(verdict, "pitch"))
        return False
    return True


def _resolve_polarized(
    kicad_named: list[Pad],
    jlc_named: list[Pad],
    verdict: Verdict,
    kind: str,
    package_name: str,
    symbol_pins: list[SymbolPin],
    polarity_source: str,
) -> Verdict:
    """Align a polarized two-pad part by terminal meaning, never by pad number (spec 7.3)."""
    if _coincident(kicad_named) or _coincident(jlc_named):
        return verdict.unresolved("unknown", "no_data", "pad geometry is degenerate")
    reference = REFERENCE_TERMINAL[kind]
    diode = kind == "diode"
    side = token_reference_side(package_name, reference)
    if side == "none":
        # A bidirectional TVS has no reference terminal; KiCad's D_TVS symbol names its
        # pins A1/A2, which would otherwise read as two anodes.
        verdict.notes.append("bidirectional part; aligned by axis")
        return _resolve_axis(kicad_named, jlc_named, verdict)
    kicad_ref, assumed, note = kicad_reference_pad(
        kicad_named, reference, diode, CONVENTION[kind]
    )
    if kicad_ref is None:
        return verdict.unresolved("unknown", "no_data", note)
    if note:
        verdict.notes.append(note)
    polarity = pin1_polarity(symbol_pins)
    token_pad = None
    if side is not None:
        token_pad = next((p for p in jlc_named if side_of(p, jlc_named) == side), None)
        if token_pad is None:
            verdict.notes.append(
                "name token could not be applied: JLC pads are drawn vertical"
            )
    label_pad = label_reference_pad(jlc_named, polarity, reference)
    seeded = polarity_source != "symbol"
    if token_pad is not None and label_pad is not None and token_pad is not label_pad:
        if seeded:
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
    jlc_ref = token_pad if token_pad is not None else label_pad
    if jlc_ref is None:
        return verdict.unresolved(
            "unknown", "no_data", "polarity unknown; check in JLC preview"
        )
    seed_decided = token_pad is None and seeded
    if seed_decided:
        verdict.notes.append(
            "reference terminal from the seeded per-footprint polarity"
        )
    kicad_other = next(p for p in kicad_named if p is not kicad_ref)
    jlc_other = next(p for p in jlc_named if p is not jlc_ref)
    kicad = {"ref": kicad_ref, "other": kicad_other}
    jlc = {"ref": jlc_ref, "other": jlc_other}
    placement = align(kicad, jlc)
    verdict.take_placement(placement)
    if placement.is_underdetermined:
        return verdict.unresolved("unknown", "no_data", "pad geometry is degenerate")
    if not _two_pad_fit(kicad, jlc, placement, verdict):
        return verdict
    verdict.rotation = ccw_correction(placement.rotation_deg)
    verdict.method = "polarity"
    verdict.confidence = "medium" if assumed or seed_decided else "high"
    if verdict.name_rotation is not None and verdict.name_rotation != verdict.rotation:
        verdict.confidence = "medium"
        verdict.notes.append(
            f"KiCad footprint drawn non-standard; name says {verdict.name_rotation}°"
        )
    kicad_pin1 = pin1_meaning(kicad_named, kind, diode)
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
    kicad_named: list[Pad], jlc_named: list[Pad], verdict: Verdict
) -> Verdict:
    """Align a non-polar two-pad part by axis only; 180 degrees is irrelevant (spec 7.4)."""
    if _coincident(kicad_named) or _coincident(jlc_named):
        return verdict.unresolved("unknown", "no_data", "pad geometry is degenerate")
    kicad = {"a": kicad_named[0], "b": kicad_named[1]}
    jlc = {"a": jlc_named[0], "b": jlc_named[1]}
    placement = align(kicad, jlc)
    verdict.take_placement(placement)
    if placement.is_underdetermined:
        return verdict.unresolved("unknown", "no_data", "pad geometry is degenerate")
    if not _two_pad_fit(kicad, jlc, placement, verdict):
        return verdict
    verdict.rotation = ccw_correction(placement.rotation_deg) % 180
    verdict.method = "axis"
    verdict.non_polar = True
    verdict.confidence = "high"
    verdict.status = "yellow" if verdict.fit == "fits_tight" else "green"
    return verdict


def _coincident(pads: list[Pad]) -> bool:
    """Return True when the pads share one position, so no axis can be drawn through them."""
    return len({(round(pad.x, 6), round(pad.y, 6)) for pad in pads}) < 2


def _finite(pads: list[Pad]) -> bool:
    """Return True when every pad coordinate, size and angle is a finite number."""
    return all(
        math.isfinite(value)
        for pad in pads
        for value in (pad.x, pad.y, pad.width, pad.height, pad.rotation)
    )


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
    if not _finite(kicad_pads) or not _finite(jlc_pads):
        return verdict.unresolved(
            "unknown", "no_data", "a pad has a non-numeric position or size"
        )
    kicad_named = named_pads(kicad_pads)
    jlc_named = named_pads(jlc_pads)
    verdict.pad_count_kicad = len(kicad_named)
    verdict.pad_count_jlc = len(jlc_named)
    kind = part_kind(package_name, kicad_footprint_name, kicad_pads, symbol_pins)
    parsed = parse_package_name(package_name, True if kind == "diode" else None)
    if parsed.rotation_source == "naming_rule":
        verdict.name_rotation = crawl_to_cpl(parsed.rotation_correction)
    if min(len(kicad_named), len(jlc_named)) < 2:
        return verdict.unresolved(
            "unknown", "no_data", "fewer than two named pads on one side"
        )
    marked = kind != "other" or token_reference_side(package_name, "positive") in (
        "left",
        "right",
    )
    if len(jlc_named) == 2 and marked:
        if len(kicad_named) != 2:
            # A two-terminal JLC part on a KiCad footprint with extra pads: align by
            # meaning on the two KiCad pads that share the part's pad names, never by number.
            shared = [
                pad
                for pad in kicad_named
                if pad.number in {p.number for p in jlc_named}
            ]
            if len(shared) != 2:
                return verdict.unresolved(
                    "unknown",
                    "no_data",
                    "two-terminal part on a footprint whose pads do not pair with it",
                )
            kicad_named = shared
        if kind == "other":
            verdict.non_polar = True
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
    if len(kicad_named) == 2 and len(jlc_named) == 2:
        return _resolve_axis(kicad_named, jlc_named, verdict)
    return _resolve_multi_pin(kicad_pads, jlc_pads, verdict)
