"""What the part list shows for one part: the JLC glyph, its order, and the hover text.

Pure and stdlib only (spec 16.3).  One function turns a CPL decision and the
fetch queue's state into the column's state name; the glyph, the colour and the
sort order follow from that name, so the model stores the state and the view maps
it.  A second function composes the cell's help text out of the stored verdict,
the worker state and the two package names, which is why both are testable with
no wx anywhere near them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - annotations only; no runtime import cycle
    from .controller import Decision, FetchState

# The column's states, worst first: this is also the ascending sort order of
# spec 16.3 (a misfit, a warning, no data, paused, queued, a derived green, an
# override, then the parts the check says nothing about).
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
    """Return the JLC column's state for one part (spec 16.3's table).

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
    """Return one to three sentences of help for a JLC cell (spec 16.3's list).

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
        return "Waiting for EasyEDA data; the Rotation column shows what the CPL emits."
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
