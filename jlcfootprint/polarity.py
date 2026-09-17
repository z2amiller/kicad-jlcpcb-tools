"""Terminal vocabulary for polarized two-pad parts (spec section 7.3).

Which pad is the cathode or the positive terminal, on the KiCad side from the
schematic's pin functions and on the EasyEDA side from the footprint name's
FD/RD token, the symbol's pin-1 label and the drawings' ``+`` marks.  Anode and positive terminal are the
same idea here, as are cathode and negative terminal: an LED whose symbol says
``+``/``-`` and a capacitor whose symbol says ``A``/``K`` must both resolve.
"""

from __future__ import annotations

from .drawing import DrawingMarks
from .easyeda_parse import PIN1_ANODE_LABELS, PIN1_CATHODE_LABELS, SymbolPin
from .geometry import Pad
from .naming import (
    CATHODE_PIN1_FAMILIES,
    POLARIZED_CAP_FAMILIES,
    extract_family,
    extract_orientation_tokens,
)

# Pin-function tokens, after normalisation, that name the terminals.  ``C`` is a
# collector on KiCad's transistor symbols and a cathode on some vendor symbols; it is
# accepted as a cathode only when the part is already known to be a diode.
_CATHODE_TOKENS = frozenset({"K", "CATHODE", "CAT"})
_ANODE_TOKENS = frozenset({"A", "ANODE", "AN"})
_POSITIVE_TOKENS = frozenset({"+", "POS", "POSITIVE"})
_NEGATIVE_TOKENS = frozenset({"-", "NEG", "NEGATIVE"})
_CAP_LABELS = frozenset({"+", "-", "POS", "NEG"})
_DIODE_LABELS = (PIN1_CATHODE_LABELS | PIN1_ANODE_LABELS) - _CAP_LABELS
# Pin functions that say "no connection" rather than naming a signal: they carry no
# terminal and do not disqualify the other pads' functions (kicad-x2ib).
_NO_FUNCTION_TEXTS = frozenset({"NC", "N/C", "N.C.", "~", "DNC", "NP", "NU"})

# The two terminal names, unified: the reference terminal of a diode is its cathode,
# of a capacitor its positive terminal, and each name's opposite.
REFERENCE_TERMINAL = {"diode": "cathode", "polar_cap": "positive", "other": "positive"}
# What pad 1 is assumed to be when the KiCad side says nothing, per kind, for the note.
CONVENTION = {"diode": "K", "polar_cap": "+", "other": "JLC's pin 1"}
OPPOSITE = {
    "cathode": "anode",
    "anode": "cathode",
    "positive": "negative",
    "negative": "positive",
}
SAME_MEANING = {
    "cathode": {"cathode", "negative"},
    "negative": {"cathode", "negative"},
    "anode": {"anode", "positive"},
    "positive": {"anode", "positive"},
}


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


def is_no_function(text: str) -> bool:
    """Return True for an empty pin function or one that says "not connected"."""
    raw = text.strip().upper()
    return (
        not raw
        or raw in _NO_FUNCTION_TEXTS
        or normalise_function(text)
        in (
            "",
            "NC",
            "DNC",
            "NP",
            "NU",
        )
    )


def terminal_of(pad: Pad, diode: bool = False) -> str:
    """Return ``cathode``, ``anode``, ``positive``, ``negative`` or ``""`` from the pin function.

    ``C`` counts as a cathode only when ``diode`` is True (see the token note above).
    """
    token = normalise_function(pad.pin_function)
    if token in _CATHODE_TOKENS or (diode and token == "C"):
        return "cathode"
    if token in _ANODE_TOKENS:
        return "anode"
    if token in _POSITIVE_TOKENS:
        return "positive"
    if token in _NEGATIVE_TOKENS:
        return "negative"
    return ""


def function_terminals(pads: list[Pad], diode: bool = False) -> set[str]:
    """Return the terminals the pads' functions name, or nothing when one names something else.

    A switch symbol's ``A``/``B`` and a connector's ``A1`` must not read as an anode: the
    functions count only when every function on the pads is a terminal name, where
    an empty or "not connected" function (``NC``, ``DNC``, ``~``) is no function at
    all rather than a disqualifying one.
    """
    terminals: set[str] = set()
    for pad in pads:
        if is_no_function(pad.pin_function):
            continue
        terminal = terminal_of(pad, diode)
        if not terminal:
            return set()
        terminals.add(terminal)
    return terminals


def part_kind(
    package_name: str,
    kicad_footprint_name: str,
    kicad_pads: list[Pad],
    symbol_pins: list[SymbolPin],
) -> str:
    """Return ``diode``, ``polar_cap`` or ``other`` (spec section 7.1).

    Evidence is weighed from the most to the least reliable: the KiCad footprint's name
    and its pads' functions, then EasyEDA's package family, then the symbol's pin
    labels.  LED symbols labelled ``+``/``-`` are common, so labels alone never outrank
    a diode family, and ``+``/``-`` pad functions name a capacitor only when nothing
    else says diode.
    """
    family = extract_family(package_name).upper()
    footprint = kicad_footprint_name.rsplit(":", 1)[-1].upper()
    terminals = function_terminals(kicad_pads)
    labels = {pin.label.strip().upper() for pin in symbol_pins}
    polarity_tokens = {
        t for t in extract_orientation_tokens(package_name) if t in ("FD", "RD")
    }
    if footprint.startswith(("D_", "LED_")) or terminals & {"cathode", "anode"}:
        return "diode"
    if footprint.startswith(("CP_", "C_ELEC", "TANTALUM")):
        return "polar_cap"
    if family in CATHODE_PIN1_FAMILIES:
        return "diode"
    if family in POLARIZED_CAP_FAMILIES or (
        family.startswith("CAP") and polarity_tokens
    ):
        return "polar_cap"
    if labels & _DIODE_LABELS:
        return "diode"
    if terminals & {"positive", "negative"} or labels & _CAP_LABELS:
        return "polar_cap"
    return "other"


def kicad_reference_pad(
    pads: list[Pad], reference: str, diode: bool = False, convention: str | None = None
) -> tuple[Pad | None, bool, str]:
    """Return (pad, assumed, note) for the KiCad pad carrying ``reference``.

    A pin function naming the reference terminal, or its opposite on the other of two
    pads, decides; anode and positive (cathode and negative) are interchangeable.  Two
    pads claiming the same terminal are contradictory: no pad, with a note.  Without any
    usable function, pad 1 is assumed and the note says what it is assumed to be
    (``convention``, by default K for a cathode reference and + otherwise).
    """
    wanted = SAME_MEANING[reference]
    opposite = SAME_MEANING[OPPOSITE[reference]]
    claims_reference = [pad for pad in pads if terminal_of(pad, diode) in wanted]
    claims_opposite = [pad for pad in pads if terminal_of(pad, diode) in opposite]
    if len(claims_reference) > 1 or len(claims_opposite) > 1:
        return None, False, "contradictory pin functions on the KiCad footprint"
    if claims_reference:
        return claims_reference[0], False, ""
    if claims_opposite and len(pads) == 2:
        return next(pad for pad in pads if pad is not claims_opposite[0]), False, ""
    if convention is None:
        convention = "K" if reference == "cathode" else "+"
    for pad in pads:
        if pad.number == "1":
            return pad, True, f"assumed KiCad pad 1 = {convention}"
    return None, True, "no pad 1 on the KiCad side"


def side_of(pad: Pad, pads: list[Pad]) -> str | None:
    """Return ``left`` or ``right`` for one of two pads, or None when their axis is not horizontal."""
    other = next(p for p in pads if p is not pad)
    dx, dy = pad.x - other.x, pad.y - other.y
    if abs(dx) <= abs(dy):
        return None
    return "left" if dx < 0 else "right"


def token_reference_side(package_name: str, reference: str) -> str | None:
    """Return ``left``, ``right``, ``none`` (bidirectional) or None (no token) from FD/RD/BI.

    Forward direction puts the part's marking band on the right of the EasyEDA
    drawing (JLC's zero orientation) and reverse direction on the left.  The band
    is the cathode of a diode and the negative end of a capacitor, so FD means
    cathode right and positive left.  That reading held on the crawl's 203 marked
    electrolytic and 39 of 45 marked molded-chip drawings (2026-09-17); the six
    others, C7171's among them, are decided by the drawings' ``+`` marks in the
    resolver, which outvote the token.
    """
    polarity = [
        t for t in extract_orientation_tokens(package_name) if t in ("FD", "RD", "BI")
    ]
    if not polarity:
        return None
    token = polarity[-1]
    if token == "BI":
        return "none"
    band_side = "right" if token == "FD" else "left"
    if reference in SAME_MEANING["negative"]:
        return band_side
    return "left" if band_side == "right" else "right"


def label_reference_pad(
    jlc_pads: list[Pad], polarity: str | None, reference: str
) -> Pad | None:
    """Return the EasyEDA pad carrying ``reference`` from the symbol's pin-1 label, if known.

    ``polarity`` is 'K' (pin 1 is the cathode or negative terminal) or 'A' (anode or
    positive).
    """
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


def drawing_reference_pads(
    jlc_pads: list[Pad], marks: DrawingMarks | None, reference: str
) -> tuple[Pad | None, Pad | None]:
    """Return the EasyEDA pad carrying ``reference`` per the symbol's and the footprint's ``+``.

    The symbol's ``+`` names a pin number and the footprint's ``+`` a pad number
    (:mod:`jlcfootprint.drawing`); both name the positive terminal, the anode of a
    diode.  A drawing without a mark gives None.  The two come back apart so that
    each casts its own vote in the resolver (spec 16.6): two marks naming
    different pads are one disagreement more, outvoted when the token and the
    label side with one of them and a tie otherwise.
    """
    if marks is None:
        return None, None
    by_number = {pad.number: pad for pad in jlc_pads}

    def carrying(number: str | None) -> Pad | None:
        positive = by_number.get(number) if number else None
        if positive is None:
            return None
        if reference in SAME_MEANING["positive"]:
            return positive
        return next((pad for pad in jlc_pads if pad is not positive), None)

    return carrying(marks.positive_pin), carrying(marks.positive_pad)


def pin1_meaning(pads: list[Pad], kind: str, diode: bool = False) -> str | None:
    """Return 'A' or 'K' for what the KiCad side means by pad 1.

    Pad 1's own function decides; failing that, the other pad's function implies it;
    failing both, the convention for the kind (pad 1 = K on diodes, + on capacitors).
    """
    pad1 = next((p for p in pads if p.number == "1"), None)
    if pad1 is None:
        return None
    terminal = terminal_of(pad1, diode)
    if not terminal and len(pads) == 2:
        other = terminal_of(next(p for p in pads if p is not pad1), diode)
        terminal = OPPOSITE.get(other, "")
    if terminal in ("cathode", "negative"):
        return "K"
    if terminal in ("anode", "positive"):
        return "A"
    return "K" if kind == "diode" else "A"
