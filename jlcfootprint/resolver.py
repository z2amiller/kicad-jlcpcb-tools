"""Per-part rotation, fit and polarity verdicts (spec 7).

Pure and stdlib only.  The caller supplies KiCad pads in the footprint's own
frame, EasyEDA pads already converted to millimetres, and the symbol pins.
Multi-pin parts align by pad name (``fit``), or by pin function when the numbers
do not line up; polarized two-pad parts align by terminal meaning (``polarity``)
and never by pad number; non-polar two-pad parts align by axis.  Fit is graded
per JLC pad after the placement, and a fitting part's body is compared with the
KiCad courtyard (spec 16.6).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math

from .fit import (
    FitReport,
    Placement,
    align,
    assess_fit,
    courtyard_excess,
    package_origin,
    pair_by_function,
    pair_by_name,
    shape_alignment,
)
from .geometry import Pad, ccw_correction, crawl_to_cpl, named_pads
from .naming import parse_package_name
from .polarity import (
    CONVENTION,
    REFERENCE_TERMINAL,
    drawing_reference_pads,
    kicad_reference_pad,
    label_reference_pad,
    normalise_function,
    part_kind,
    pin1_meaning,
    side_of,
    terminal_of,
    token_is_weak,
    token_reference_side,
)
from .records import DrawingMarks, SymbolPin, pin1_polarity

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
DRAWING_NOTES = {
    "both": "polarity from the + marks of the symbol and the footprint",
    "footprint": "polarity from the footprint's + mark",
    "symbol": "polarity from the symbol's + mark",
}
# The body caveat: the JLC body may overhang the KiCad courtyard by this fraction of
# the courtyard's shorter side, never less than the floor (drawing noise on an 0201)
# nor more than the ceiling (a DIP-40 still flags a wide body) (spec 16.6).
BODY_EXCESS_FRACTION = 0.10
BODY_EXCESS_MIN_MM = 0.15
BODY_EXCESS_MAX_MM = 1.5

# The statuses whose row carries a derived rotation and a placement, so the CPL may
# use the rotation for its angle and the placement for its position (spec 8, 17.3).
# ``verdicts.py`` imports this name from here for its own row logic.
APPLIED_STATUSES = ("green", "yellow")


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
    polarity_source: str = ""  # token | label | seed | drawing | ""
    non_polar: bool = False  # True when 180 degrees apart is the same placement
    pad_count_kicad: int = 0
    pad_count_jlc: int = 0
    matched_pads: int = 0
    missing_central_pad: bool = False
    overlap_min: float = 0.0
    overlap_mean: float = 0.0
    angular_rms: float = 0.0
    residual_mm: float = 0.0
    body_excess_mm: float | None = None  # the caveat: how far the JLC body overhangs
    notes: list[str] = field(default_factory=list)
    placement: Placement | None = field(default=None, repr=False, compare=False)

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

    @property
    def origin(self) -> tuple[float, float] | None:
        """Return JLC's package origin in the footprint frame, or None (spec 17.2).

        Only a verdict the resolver settled as green or yellow carries one: a red,
        unknown or unfinished verdict has no placement the CPL may trust, and the
        stored row keeps NULL for it.
        """
        if self.placement is None or self.status not in APPLIED_STATUSES:
            return None
        return package_origin(self.placement)

    def take_placement(self, placement: Placement) -> None:
        """Copy the placement's metrics and keep the placement for the drawing."""
        self.placement = placement
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


def _finish_multi_pin(
    verdict: Verdict, kicad: dict[str, Pad], placement: Placement, by_name: bool = True
) -> Verdict:
    """Set the rotation and status of a multi-pin part whose pads fit.

    ``by_name`` says how the pads were paired: on their numbers (spec 7.2) or, when
    that fails, by the pin functions the schematic gave them (spec 16.6).  Only a
    by-name alignment can rest on "shared pad names", so the notes that say so are
    its own.
    """
    verdict.rotation = ccw_correction(placement.rotation_deg)
    verdict.method = "geometry"
    if verdict.confidence == "none":
        verdict.confidence = "high"
    if by_name and len(kicad) == 2:
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
        paired = "shared names" if by_name else "paired pads"
        verdict.notes.append(
            f"pad counts differ ({verdict.pad_count_kicad} vs {verdict.pad_count_jlc}) "
            f"but the {len(kicad)} {paired} align"
        )
    verdict.status = (
        "yellow"
        if verdict.missing_central_pad or verdict.fit == "fits_tight"
        else "green"
    )
    return verdict


def _resolve_by_function(
    kicad_pads: list[Pad],
    jlc_pads: list[Pad],
    verdict: Verdict,
    symbol_pins: list[SymbolPin],
) -> Verdict | None:
    """Align by pin function when the pad numbers failed; None when that fails too.

    KiCad's ``TO-252-3_TabPin2`` numbers the tab 2 where JLC's DPAK drawing numbers
    it 3: the numbers differ but the schematic's ``D`` and the symbol's ``D`` are the
    same physical pin, so the part fits once the pads pair by function (spec 16.6).
    """
    pairing = pair_by_function(kicad_pads, jlc_pads, symbol_pins)
    if pairing is None:
        return None
    placement = align(pairing.kicad, pairing.jlc)
    if placement.is_underdetermined or placement.is_mirrored:
        return None
    report = assess_fit(
        pairing.kicad,
        pairing.jlc,
        named_pads(kicad_pads),
        pairing.leftovers,
        placement,
        verdict.pad_count_kicad,
        verdict.pad_count_jlc,
    )
    if report.fit in ("count", "pitch"):
        return None
    verdict.take_placement(placement)
    verdict.take_fit(report)
    verdict.confidence = "medium"
    differences = ", ".join(
        f"JLC pin {jlc_number} ({function}) is pad {kicad_number} on the footprint"
        for function, kicad_number, jlc_number in pairing.differences
    )
    note = "pin numbers differ from JLC's part; paired by pin function"
    if differences:
        note += f": {differences}"
    if pairing.eliminated is not None:
        jlc_number, kicad_number = pairing.eliminated
        note += f"; JLC pin {jlc_number} and pad {kicad_number} paired by elimination"
    verdict.notes.append(note)
    return _finish_multi_pin(verdict, pairing.kicad, placement, by_name=False)


def _resolve_multi_pin(
    kicad_pads: list[Pad],
    jlc_pads: list[Pad],
    verdict: Verdict,
    symbol_pins: list[SymbolPin],
) -> Verdict:
    """Align by pad name (spec 7.2), or by pin function when the names fail."""
    kicad, jlc, jlc_rest = pair_by_name(kicad_pads, jlc_pads)
    if len(kicad) < 2:
        by_function = _resolve_by_function(kicad_pads, jlc_pads, verdict, symbol_pins)
        if by_function is not None:
            return by_function
        return verdict.unresolved(
            "unknown", "no_data", "fewer than two matching pad names"
        )
    placement = align(kicad, jlc)
    verdict.take_placement(placement)
    if placement.is_underdetermined:
        return verdict.unresolved("unknown", "no_data", "pad geometry is degenerate")
    if placement.is_mirrored:
        # A numbering that runs the other way is a by-number failure like any
        # other: the schematic's pin functions may still pair the pins (spec 16.6).
        by_function = _resolve_by_function(kicad_pads, jlc_pads, verdict, symbol_pins)
        if by_function is not None:
            return by_function
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
    if report.fit in ("count", "pitch"):
        by_function = _resolve_by_function(kicad_pads, jlc_pads, verdict, symbol_pins)
        if by_function is not None:
            return by_function
        verdict.take_fit(report)
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
    verdict.take_fit(report)
    return _finish_multi_pin(verdict, kicad, placement)


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


def _reference_vote(
    jlc_named: list[Pad],
    package_name: str,
    symbol_pins: list[SymbolPin],
    polarity_source: str,
    marks: DrawingMarks | None,
    reference: str,
    notes: list[str],
) -> tuple[Pad | None, str, list[str], str | None, bool]:
    """Vote for the EasyEDA pad carrying ``reference`` and what pin 1 means there.

    The EasyEDA side's reference terminal has up to four sources: the name's FD/RD
    token, the symbol's pin-1 label, and the ``+`` marks of the symbol and of the
    footprint (spec 16.6).  Each names a pad; the majority decides and a tie is
    inconsistent data.  Calibrated on 2026-09-17: the marks named the right pad on
    all 22 parts with a known truth, the token on all diodes and electrolytics but
    on a minority of the molded-chip tantalums, where the two marks outvote it.
    A seeded label is weaker: it yields to a token that disagrees with it.

    Returns ``(jlc_ref, source, outvoted, polarity, weak_token)``: the winning JLC
    pad and where it came from (``token``, ``seed``, ``label`` or ``drawing``); the
    names of the votes it outvoted; the pin-1 polarity the symbol's label or the
    seed implies (``None`` once outvoted or disagreeing); and whether an
    unchallenged token is from the one family the crawl found weak.  When nothing
    votes, or the vote ties, ``jlc_ref`` is ``None`` and ``source`` carries the
    verdict status to use instead -- the reason is already appended to ``notes``.
    """
    side = token_reference_side(package_name, reference)
    polarity = pin1_polarity(symbol_pins)
    token_pad = None
    if side is not None:
        token_pad = next((p for p in jlc_named if side_of(p, jlc_named) == side), None)
        if token_pad is None:
            notes.append("name token could not be applied: JLC pads are drawn vertical")
    label_pad = label_reference_pad(jlc_named, polarity, reference)
    seeded = polarity_source != "symbol"
    symbol_drawn, footprint_drawn = drawing_reference_pads(jlc_named, marks, reference)
    if (
        seeded
        and token_pad is not None
        and label_pad is not None
        and token_pad is not label_pad
    ):
        # The seed was read from one representative part per footprint; against
        # this part's own name it is set aside (it may still be that the marks
        # outvote the token below, so the note does not promise the token).
        notes.append(
            "seeded pin-1 polarity (per footprint) disagrees with the name token; not counted"
        )
        label_pad = None
        polarity = None
    votes: list[tuple[Pad, str]] = []
    if token_pad is not None:
        votes.append((token_pad, "name token"))
    if label_pad is not None:
        votes.append(
            (label_pad, "seeded pin-1 polarity" if seeded else "symbol pin-1 label")
        )
    if symbol_drawn is not None:
        votes.append((symbol_drawn, "symbol + mark"))
    if footprint_drawn is not None:
        votes.append((footprint_drawn, "footprint + mark"))
    if not votes:
        notes.append("polarity unknown; check in JLC preview")
        return None, "unknown", [], None, False
    sides: dict[int, list[str]] = {}
    pads_by_id: dict[int, Pad] = {}
    for pad, name in votes:
        sides.setdefault(id(pad), []).append(name)
        pads_by_id[id(pad)] = pad
    ranked = sorted(sides.values(), key=len, reverse=True)
    if len(ranked) > 1 and len(ranked[0]) == len(ranked[1]):
        notes.append(
            "EasyEDA data inconsistent: "
            + " and ".join(", ".join(names) for names in ranked)
            + " disagree"
        )
        return None, "red", [], None, False
    winners = ranked[0]
    jlc_ref = next(pad for pad, name in votes if name == winners[0])
    outvoted = [name for pad, name in votes if pad is not jlc_ref]
    if outvoted:
        notes.append(
            f"{', '.join(outvoted)} {'names' if len(outvoted) == 1 else 'name'} the other pad; "
            f"{', '.join(winners)} {'decides' if len(winners) == 1 else 'decide'}"
        )
        if label_pad is not None and label_pad is not jlc_ref:
            polarity = None
    if token_pad is jlc_ref:
        source = "token"
    elif label_pad is jlc_ref:
        source = "seed" if seeded else "label"
    else:
        source = "drawing"
        if not outvoted:
            drawn_by = [name.split(" ")[0] for name in winners]  # symbol, footprint
            notes.append(DRAWING_NOTES["both" if len(drawn_by) == 2 else drawn_by[0]])
    if source == "seed":
        notes.append("reference terminal from the seeded per-footprint polarity")
    # A token nothing checked, on the one family whose token the crawl found weak,
    # is resolved at medium confidence (spec 16.9).
    weak_token = source == "token" and len(votes) == 1 and token_is_weak(package_name)
    return jlc_ref, source, outvoted, polarity, weak_token


def _resolve_polarized(
    kicad_named: list[Pad],
    jlc_named: list[Pad],
    verdict: Verdict,
    kind: str,
    package_name: str,
    symbol_pins: list[SymbolPin],
    polarity_source: str,
    marks: DrawingMarks | None = None,
) -> Verdict:
    """Align a polarized two-pad part by terminal meaning, never by pad number (spec 7.3).

    The KiCad side's reference pad comes from its pin functions or, by convention,
    pad 1; :func:`_reference_vote` decides which EasyEDA pad is the reference and
    what its pin 1 means.  Once both reference pads are known the two pads align
    like any other two-pad part.
    """
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
    jlc_ref, source, outvoted, polarity, weak_token = _reference_vote(
        jlc_named,
        package_name,
        symbol_pins,
        polarity_source,
        marks,
        reference,
        verdict.notes,
    )
    if jlc_ref is None:
        # _reference_vote already recorded why; source carries the status to use.
        verdict.status = source
        verdict.fit = "no_data"
        verdict.rotation = None
        verdict.method = "none"
        return verdict
    verdict.polarity_source = source
    if kind != "other":
        # The vote settles which EasyEDA pad carries the reference terminal and with it
        # what JLC's pad 1 means, so the pin-1 light no longer waits for a symbol label:
        # the token and the marks tell it too, and an outvoted label is overruled.
        jlc_pad1 = next((p for p in jlc_named if p.number == "1"), None)
        if jlc_pad1 is not None:
            polarity = "K" if (jlc_ref is jlc_pad1) == (reference == "cathode") else "A"
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
    verdict.confidence = (
        "medium"
        if assumed or outvoted or weak_token or source in ("seed", "drawing")
        else "high"
    )
    if (
        source == "token"
        and verdict.name_rotation is not None
        and verdict.name_rotation != verdict.rotation
    ):
        # Only a token that was applied can accuse the footprint.
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


def body_threshold_mm(courtyard: tuple[float, float, float, float]) -> float:
    """Return how far the JLC body may overhang this courtyard before the caveat."""
    shorter = min(courtyard[2] - courtyard[0], courtyard[3] - courtyard[1])
    return min(
        max(BODY_EXCESS_FRACTION * shorter, BODY_EXCESS_MIN_MM), BODY_EXCESS_MAX_MM
    )


def _body_caveat(
    verdict: Verdict,
    courtyard: tuple[float, float, float, float],
    body: tuple[float, float, float, float],
) -> None:
    """Note a JLC body that overhangs the KiCad courtyard (spec 16.6).

    The status and the rotation stand: the pads fit, the part is only bigger than
    the footprint's author allowed for, which the preview shows.

    A courtyard with a zero-length side measures nothing (a footprint whose
    courtyard is a single line, or a box read from one degenerate graphic), so it
    raises no caveat instead of flagging the whole body as overhang.
    """
    if verdict.placement is None:
        return
    if courtyard[2] - courtyard[0] <= 0 or courtyard[3] - courtyard[1] <= 0:
        return
    excess = courtyard_excess(courtyard, body, verdict.placement)
    if excess <= body_threshold_mm(courtyard):
        return
    verdict.body_excess_mm = round(excess, 2)
    verdict.notes.append(
        f"fits, JLC body {excess:.1f} mm larger than the KiCad courtyard"
    )


def resolve(
    kicad_pads: list[Pad],
    kicad_footprint_name: str,
    record_status: str,
    package_name: str,
    jlc_pads: list[Pad],
    symbol_pins: list[SymbolPin],
    polarity_source: str = "symbol",
    marks: DrawingMarks | None = None,
    kicad_courtyard: tuple[float, float, float, float] | None = None,
) -> Verdict:
    """Return the verdict for one part (spec 7).

    ``kicad_pads`` are in the footprint's own frame (bottom-side parts already
    un-mirrored by the caller); ``jlc_pads`` are millimetres in KiCad's frame.
    ``polarity_source`` is ``symbol`` when ``symbol_pins`` came from the part's own
    symbol and ``seed-puuid`` when they were seeded per footprint from the crawl.
    ``marks`` are the drawings' ``+`` marks and the JLC body box
    (:func:`jlcfootprint.drawing.drawing_marks`) and ``kicad_courtyard`` the
    footprint's courtyard box, both optional; together they add the polarity
    source of last resort and the body-size caveat.
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
    # Counts compare distinct numbers: a DPAK's tab and its stub are one pad 2.
    verdict.pad_count_kicad = len({pad.number for pad in kicad_named})
    verdict.pad_count_jlc = len({pad.number for pad in jlc_named})
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
        _resolve_polarized(
            kicad_named,
            jlc_named,
            verdict,
            kind,
            package_name,
            symbol_pins,
            polarity_source,
            marks,
        )
    elif len(kicad_named) == 2 and len(jlc_named) == 2:
        _resolve_axis(kicad_named, jlc_named, verdict)
    else:
        _resolve_multi_pin(kicad_pads, jlc_pads, verdict, symbol_pins)
    if (
        verdict.rotation is not None
        and kicad_courtyard is not None
        and marks is not None
        and marks.body_box is not None
    ):
        _body_caveat(verdict, kicad_courtyard, marks.body_box)
    return verdict
