"""What the part list shows for one part: the JLC glyph, its order, and the hover text.

Pure and stdlib only (spec 16.3).  One function turns a CPL decision and the
fetch queue's state into the column's state name; the glyph, the colour and the
sort order follow from that name, so the model stores the state and the view maps
it.  A second function composes the cell's help text out of the stored verdict,
the worker state and the two package names, which is why both are testable with
no wx anywhere near them.
"""

from __future__ import annotations

import time

from .drawing import drawing_marks
from .geometry import easyeda_pads_to_mm, pad_box_centre
from .model import Decision, FetchState, PartDetail

# Closer than this to the pad-box centre reads as "on" it, which is what the two
# decimals the line prints can tell apart.
ORIGIN_EPSILON_MM = 0.005

# The column's states, worst first, which is also the column's ascending sort
# order: a misfit, a warning, no data, paused, queued, a derived green, an
# override, then the parts the check says nothing about (spec 16.3).
BLANK = ""
RED = "red"
YELLOW = "yellow"
CAVEAT = "caveat"
UNKNOWN = "unknown"
PAUSED = "paused"
PENDING = "pending"
GREEN = "green"
OVERRIDE = "override"

SORT_ORDER = (RED, YELLOW, CAVEAT, UNKNOWN, PAUSED, PENDING, GREEN, OVERRIDE, BLANK)
GLYPHS = {
    RED: "✗",
    YELLOW: "!",
    CAVEAT: "!",
    UNKNOWN: "?",
    PAUSED: "‖",
    PENDING: "◷",
    GREEN: "✓",
    OVERRIDE: "✓",
    BLANK: "",
}
# A caveated fit shares the amber "!" with a pin-1 warning but not its meaning, so
# it keeps its own state: the glyph is one character, the hover says which it is.
_RANKS = {state: index for index, state in enumerate(SORT_ORDER)}

FIT_TEXT = {
    "fits": "Fits",
    "fits_larger_pads": "Fits, KiCad pads larger",
    "fits_tight": "Fits, JLC pads at the edge of yours",
}
METHOD_TEXT = {
    "geometry": "pad geometry",
    "polarity": "terminal polarity",
    "axis": "the pad axis",
}
RAW_SENTENCE = "The CPL emits the raw angle."
YELLOW_SENTENCE = (
    "JLC's pin-1 marker will sit on the other terminal: a numbering difference, "
    "not a rotation error. Do not renumber the footprint."
)
RED_REASONS = {
    "count": "pad count",
    "pitch": "pitch",
    "mirror": "pin order",
    "numbering": "numbering",
    "no_data": "no data",
}
FETCH_TEXT = {
    "lookup": "Looking up the part at EasyEDA…",
    "footprint": "Fetching the footprint…",
    "symbol": "Fetching the symbol…",
}


def describe_seconds(seconds: float) -> str:
    """Render a queue estimate as seconds under a minute, else minutes.

    The controller re-exports it: the log line, the wait dialog and this module's
    help text quote one estimate in one wording.
    """
    if seconds < 60:
        return f"{int(round(seconds))} s"
    return f"{int(round(seconds / 60.0))} min"


def sort_rank(state: str) -> int:
    """Return the ascending sort position of one column state (spec 16.3)."""
    return _RANKS.get(state, _RANKS[BLANK])


def glyph(state: str) -> str:
    """Return the character the column draws for one state ("" for no state)."""
    return GLYPHS.get(state, "")


def jlc_state(decision: Decision | None, fetch: FetchState | None = None) -> str:
    """Return the JLC column's state for one part, which picks its glyph (spec 16.3).

    ``decision`` is :class:`jlcfootprint.controller.Decision` (None for a part the
    check has not seen) and ``fetch`` :class:`jlcfootprint.controller.FetchState`,
    which is what tells a queued part from one waiting out a backoff.  A part being
    fetched shows the clock even when it carries an override, because the override
    is what the Rotation column is already showing; the glyph reports the data.
    """
    if decision is None or not decision.lcsc:
        return BLANK
    state = "idle" if fetch is None else fetch.state
    if state in ("paused", "tripped"):
        return PAUSED
    if state in ("queued", "fetching") or decision.pending:
        return PENDING
    verdict = decision.verdict
    if verdict is not None and verdict.override_rotation is not None:
        return OVERRIDE
    if decision.status == "green":
        return CAVEAT if decision.body_excess is not None else GREEN
    if decision.status == "yellow":
        return YELLOW
    if decision.status == "red":
        return RED
    return UNKNOWN


def _degrees(rotation: int | None) -> str:
    """Render a correction for a sentence, or say there is none."""
    return "none" if rotation is None else f"{rotation}°"


def _fit_sentence(decision: Decision) -> str:
    """Return the sentence that opens a fitting part's help."""
    verdict = decision.verdict
    fit = FIT_TEXT.get(decision.fit or "", "Fits")
    method = METHOD_TEXT.get("" if verdict is None else (verdict.method or ""), "")
    confidence = "" if verdict is None else (verdict.confidence or "")
    if decision.polarity_light == "yellow" or not method:
        return f"{fit}; rotation {_degrees(decision.rotation)}."
    detail = f" derived from {method}"
    if confidence and confidence != "none":
        detail += f" ({confidence})"
    return f"{fit}; rotation {_degrees(decision.rotation)}{detail}."


def _packages_sentence(jlc_package: str, kicad_footprint: str) -> str:
    """Return the sentence naming both packages, or "" when neither is known."""
    if jlc_package and kicad_footprint:
        return f"JLC {jlc_package} on {kicad_footprint}."
    if jlc_package:
        return f"JLC {jlc_package}."
    return f"KiCad {kicad_footprint}." if kicad_footprint else ""


def _capitalised(note: str) -> str:
    """Return a stored note as a sentence."""
    text = note.strip()
    if not text:
        return ""
    text = text[0].upper() + text[1:]
    return text if text.endswith(".") else f"{text}."


def verdict_text(
    decision: Decision | None,
    fetch: FetchState | None = None,
    kicad_footprint: str = "",
    jlc_package: str = "",
) -> str:
    """Return one to three sentences of help for a JLC cell's hover (spec 16.3).

    Composed from the stored verdict on the decision, the fetch queue's state and
    the two package names; nothing here reads a database or touches wx.  The
    queue's state comes first, because a part still being fetched has nothing else
    to say; then an override, which is what the CPL emits; then the verdict.
    """
    if decision is None:
        return ""
    if not decision.lcsc:
        return f"No LCSC number assigned. {RAW_SENTENCE}"
    state = "idle" if fetch is None else fetch.state
    if state == "tripped":
        return "Paused after three failed requests; reopen the plugin to retry."
    if state == "paused":
        return (
            f"EasyEDA asked us to wait {int(round(fetch.seconds))} s; "
            f"{fetch.queued_parts} part(s) queued."
        )
    if state == "fetching":
        return FETCH_TEXT.get(fetch.kind, "Fetching the EasyEDA data…")
    if state == "queued":
        return (
            f"Queued for EasyEDA, {fetch.ahead} request(s) ahead, "
            f"about {describe_seconds(fetch.seconds)}."
        )
    verdict = decision.verdict
    if decision.pending:
        sentence = (
            "Waiting for EasyEDA data; the Rotation column shows what the CPL emits."
        )
        if verdict is not None and verdict.override_rotation is not None:
            # "Re-fetch data" marks the row pending but keeps the override (spec
            # 16.5); say so, or the user cannot tell it survived the re-fetch.
            sentence += f" Override {verdict.override_rotation}° set by you."
        return sentence
    if verdict is not None and verdict.override_rotation is not None:
        note = _capitalised(verdict.override_note or "")
        derived = f"Derived: {_degrees(verdict.rotation)} ({verdict.status})."
        return " ".join(
            part
            for part in (
                f"Override {verdict.override_rotation}° set by you.",
                note,
                derived,
            )
            if part
        )
    sentences = []
    if decision.status in ("green", "yellow"):
        sentences.append(_fit_sentence(decision))
        if decision.polarity_light == "yellow":
            sentences.append(YELLOW_SENTENCE)
        if decision.body_excess is not None:
            sentences.append(
                f"The JLC body is {decision.body_excess:g} mm larger than the KiCad "
                "courtyard on one side; check the preview."
            )
        sentences.append(_packages_sentence(jlc_package, kicad_footprint))
    elif decision.status == "red":
        # The resolver's own note usually opens with "does not fit: ..." and says
        # more than the grade does; only a note that does not is prefixed with it.
        note = _capitalised(decision.note)
        if not note.lower().startswith("does not fit"):
            sentences.append(
                f"Does not fit: {RED_REASONS.get(decision.fit or '', decision.fit or 'no data')}."
            )
        sentences.append(note)
        sentences.append(_packages_sentence(jlc_package, kicad_footprint))
        sentences.append(RAW_SENTENCE)
    else:
        sentences.append(_capitalised(decision.note) or "Not checked against JLC yet.")
        sentences.append(_packages_sentence(jlc_package, kicad_footprint))
        sentences.append(RAW_SENTENCE)
    return " ".join(sentence for sentence in sentences if sentence)


def glyph_state(check, reference: str) -> str:
    """Return the JLC column's state for one reference ("" when it has no part)."""
    part = check.parts.get(reference)
    if part is None:
        return ""
    return jlc_state(check.decision(part), check.fetch_state(part.lcsc))


def cell_help(check, reference: str) -> str:
    """Return the JLC cell's hover text for one reference (spec 16.3)."""
    part = check.parts.get(reference)
    if part is None:
        return ""
    decision = check.decision(part)
    return verdict_text(
        decision,
        check.fetch_state(part.lcsc),
        kicad_footprint=part.footprint_name,
        jlc_package=check.package_name(part.lcsc),
    )


# ---------------------------------------------------------------------------
# The detail dialog's text (spec 16.4)
# ---------------------------------------------------------------------------

OVERRIDE_ANGLES = (0, 90, 180, 270)


def part_detail(check, reference: str, reread: bool = False) -> PartDetail | None:
    """Return everything the detail dialog shows for one reference (spec 16.4).

    ``reread`` re-reads the board first, which the dialog does on open so an edit
    made since the last scan is what is drawn.  None when the reference is not on
    the board at all.
    """
    if reread:
        for part in check.read_board():
            check.parts[part.reference] = part
    part = check.parts.get(reference)
    if part is None:
        return None
    decision = check.decision(part)
    detail = PartDetail(
        reference=part.reference,
        lcsc=part.lcsc,
        kicad_footprint=part.footprint_name,
        kicad_pads=list(part.pads),
        courtyard=part.courtyard,
        is_bottom=part.is_bottom,
        placed_rotation=part.placed_rotation,
        stored=decision.verdict,
        decision=decision,
        fetch=check.fetch_state(part.lcsc),
    )
    if not part.lcsc:
        return detail
    cached = check.cache.part(part.lcsc)
    if cached is None:
        return detail
    record = cached.record
    detail.package_name = record.package_name
    detail.puuid = record.puuid
    detail.jlc_pads = easyeda_pads_to_mm(record.pads)
    detail.symbol_pins = list(record.symbol_pins)
    detail.marks = drawing_marks(
        record.symbol_shapes, record.footprint_shapes, record.footprint_origin
    )
    detail.source = cached.source
    detail.fetched_at = cached.fetched_at
    if record.status == "ok" and record.pads:
        # The placement the canvas draws is solved here, not read from the row.
        detail.verdict = check.resolve_part(part, cached)
    return detail


def dialog_title(detail: PartDetail) -> str:
    """Return the dialog's title: the reference and the LCSC, or just the reference."""
    return f"{detail.reference} · {detail.lcsc}" if detail.lcsc else detail.reference


def parse_override(text: str) -> int | None:
    """Return the angle an override field holds, or None when it is not one.

    Any integer is accepted and taken modulo 360, because a footprint may need a
    value the four buttons do not offer; anything else is rejected rather than
    guessed at.
    """
    cleaned = text.strip().rstrip("°").strip()
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]
    try:
        return int(cleaned) % 360
    except ValueError:
        return None


def _millimetres(value: float | None, digits: int = 2) -> str:
    """Render a millimetre value, or "unknown" when there is none."""
    return "unknown" if value is None else f"{value:.{digits}f} mm"


def _percent(value: float | None) -> str:
    """Render an overlap fraction as a percentage."""
    return "unknown" if value is None else f"{value:.0%}"


def _degrees_2dp(value: float | None) -> str:
    """Render an angle to two decimals, or "unknown" when a row carries none."""
    return "unknown" if value is None else f"{value:.2f}°"


def _when(stamp: int) -> str:
    """Render a cache timestamp in local time, or "unknown" for nothing."""
    if not stamp:
        return "unknown"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(stamp))


def _courtyard_size(courtyard: tuple | None) -> str:
    """Render a courtyard box as its width by its height, or say it has none."""
    if courtyard is None:
        return "none"
    width = courtyard[2] - courtyard[0]
    height = courtyard[3] - courtyard[1]
    return f"{width:.2f} x {height:.2f} mm"


def kicad_facts(detail: PartDetail) -> list:
    """Return the dialog's left column: what the board says about this footprint."""
    pads = [pad for pad in detail.kicad_pads if pad.number]
    numbers = sorted({pad.number for pad in pads})
    pin1 = detail.kicad_pin1
    functions = detail.pin_functions
    facts = [
        ("Footprint", detail.kicad_footprint or "unknown"),
        (
            "Pads",
            f"{len(numbers)} terminal(s), {len(pads)} pad(s)" if pads else "none",
        ),
        ("Pitch", _millimetres(detail.kicad_pitch)),
        (
            "Pin 1",
            "no pad 1"
            if pin1 is None
            else f"{pin1.x:+.2f}, {pin1.y:+.2f} mm in the footprint frame",
        ),
        (
            "Pin functions",
            ", ".join(f"{number} = {functions[number]}" for number in sorted(functions))
            or "none in the schematic",
        ),
        ("Raw angle", f"{detail.placed_rotation:g}°"),
        ("Side", "bottom" if detail.is_bottom else "top"),
        ("Courtyard", _courtyard_size(detail.courtyard)),
    ]
    return facts


def origin_text(detail: PartDetail, exact_origin: bool = False) -> str:
    """Return the JLC panel's origin line: where it is and whether the CPL uses it (17.5).

    The offset is the one the CPL applies, so it is read against the pad-box centre
    upstream emits today; a part with no placement has no origin to state.
    """
    origin = detail.origin
    if origin is None:
        return "none: no placement"
    used = "used in the CPL" if exact_origin else "not used: the setting is off"
    centre = pad_box_centre(detail.kicad_pads)
    if centre is None:
        return f"{origin[0]:.2f}, {origin[1]:.2f} mm in the footprint frame; {used}"
    dx, dy = origin[0] - centre[0], origin[1] - centre[1]
    parts = []
    if abs(dx) >= ORIGIN_EPSILON_MM:
        parts.append(f"{abs(dx):.2f} mm {'right' if dx > 0 else 'left'}")
    if abs(dy) >= ORIGIN_EPSILON_MM:
        # The footprint frame is KiCad's, Y down, so a positive dy is below.
        parts.append(f"{abs(dy):.2f} mm {'below' if dy > 0 else 'above'}")
    if not parts:
        return f"on the pad-box centre; {used}"
    # "0.30 mm right, 0.15 mm below the pad-box centre" reads as the spec writes it;
    # a horizontal word on its own needs the "of".
    joint = (
        " the pad-box centre"
        if abs(dy) >= ORIGIN_EPSILON_MM
        else " of the pad-box centre"
    )
    return f"{', '.join(parts)}{joint}; {used}"


def jlc_facts(detail: PartDetail, exact_origin: bool = False) -> list:
    """Return the dialog's right column: what EasyEDA's drawing and symbol say."""
    pads = [pad for pad in detail.jlc_pads if pad.number]
    numbers = sorted({pad.number for pad in pads})
    stored = detail.stored
    verdict = detail.verdict
    polarity_source = "" if verdict is None else verdict.polarity_source
    marks = detail.marks
    name_rotation = None if stored is None else stored.name_rotation
    facts = [
        ("Package", detail.package_name or "not fetched yet"),
        ("puuid", detail.puuid or "unknown"),
        (
            "Pads",
            f"{len(numbers)} terminal(s), {len(pads)} pad(s)" if pads else "none",
        ),
        ("Pitch", _millimetres(detail.jlc_pitch)),
        (
            "Name rotation",
            "no orientation token" if name_rotation is None else f"{name_rotation}°",
        ),
        (
            "Confidence",
            "unknown"
            if stored is None or not stored.confidence
            else f"{stored.confidence} ({stored.method or 'no method'})",
        ),
        (
            "Polarity",
            "non-polar"
            if verdict is not None and verdict.non_polar
            else (polarity_source or "unknown"),
        ),
        (
            "Marks",
            "none"
            if marks is None
            else ", ".join(
                part
                for part in (
                    f"symbol + on pin {marks.positive_pin}"
                    if marks.positive_pin
                    else "",
                    f"footprint + on pad {marks.positive_pad}"
                    if marks.positive_pad
                    else "",
                    "body box" if marks.body_box is not None else "",
                )
                if part
            )
            or "none",
        ),
        (
            "Symbol pins",
            ", ".join(
                f"{pin.number} = {pin.label or '?'}" for pin in detail.symbol_pins
            )
            or "none",
        ),
        ("Origin", origin_text(detail, exact_origin)),
        (
            "Fetched",
            f"{_when(detail.fetched_at)} ({detail.source})"
            if detail.source
            else "not fetched yet",
        ),
    ]
    return facts


def fit_numbers(detail: PartDetail) -> list:
    """Return the numbers printed under the canvas (spec 16.4)."""
    stored = detail.stored
    verdict = detail.verdict
    source = verdict if verdict is not None else stored
    kicad_count = getattr(source, "pad_count_kicad", None)
    jlc_count = getattr(source, "pad_count_jlc", None)
    return [
        (
            "Pads KiCad/JLC",
            "unknown" if kicad_count is None else f"{kicad_count} / {jlc_count}",
        ),
        ("Overlap min", _percent(getattr(source, "overlap_min", None))),
        ("Overlap mean", _percent(getattr(source, "overlap_mean", None))),
        ("Residual", _millimetres(getattr(source, "residual_mm", None), 3)),
        ("Angular rms", _degrees_2dp(getattr(source, "angular_rms", None))),
        (
            "Body excess",
            "none"
            if getattr(source, "body_excess_mm", None) is None
            else _millimetres(source.body_excess_mm),
        ),
    ]


def cpl_sentence(detail: PartDetail) -> str:
    """Return what the CPL will emit for this part, in one sentence (spec 8)."""
    decision = detail.decision
    if decision is None or not detail.lcsc:
        return f"The CPL emits the raw angle, {detail.placed_rotation:g}°."
    rotation = decision.rotation
    if rotation is None:
        return f"The CPL emits the raw angle, {detail.placed_rotation:g}°."
    emitted = (detail.placed_rotation + rotation) % 360
    return (
        f"The CPL emits {emitted:g}°: the raw {detail.placed_rotation:g}° "
        f"with {rotation:+d}° applied ({decision.source})."
    )


def banner(detail: PartDetail) -> tuple:
    """Return the dialog's banner: the state, the verdict text and the CPL sentence."""
    decision = detail.decision
    state = jlc_state(decision, detail.fetch)
    text = verdict_text(
        decision,
        detail.fetch,
        kicad_footprint=detail.kicad_footprint,
        jlc_package=detail.package_name,
    )
    return (state, text, cpl_sentence(detail))


def describe_board_estimate(parts: int, seconds: float) -> str:
    """Render a board-wide fetch as the confirmation quotes it: "66 parts, about 90 s"."""
    return f"{parts} part(s), about {describe_seconds(seconds)}"
