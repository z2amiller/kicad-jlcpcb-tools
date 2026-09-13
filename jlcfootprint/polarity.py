"""Terminal vocabulary for polarized two-pad parts (spec section 7.3).

Which pad is the cathode or the positive terminal, on the KiCad side from the
schematic's pin functions and on the EasyEDA side from the footprint name's
FD/RD token and the symbol's pin-1 label.  Anode and positive terminal are the
same idea here, as are cathode and negative terminal: an LED whose symbol says
``+``/``-`` and a capacitor whose symbol says ``A``/``K`` must both resolve.
"""

from __future__ import annotations

from .easyeda_parse import PIN1_ANODE_LABELS, PIN1_CATHODE_LABELS, SymbolPin
from .geometry import Pad
from .naming import (
    CATHODE_PIN1_FAMILIES,
    POLARIZED_CAP_FAMILIES,
    extract_family,
    extract_orientation_tokens,
)

# Pin-function tokens, after normalisation, that name the two terminals.  ``C`` is a
# collector on KiCad's transistor symbols and a cathode on some vendor symbols; it is
# accepted as a cathode only when the part is already known to be a diode.
_CATHODE_TOKENS = frozenset({"K", "CATHODE", "CAT", "NEG"})
_ANODE_TOKENS = frozenset({"A", "ANODE", "AN", "POS"})
_CAP_LABELS = frozenset({"+", "-", "POS", "NEG"})
_DIODE_LABELS = (PIN1_CATHODE_LABELS | PIN1_ANODE_LABELS) - _CAP_LABELS

# The two terminal names, unified: the reference terminal of a diode is its cathode,
# of a capacitor its positive terminal, and each name's opposite.
REFERENCE_TERMINAL = {"diode": "cathode", "polar_cap": "positive", "other": "positive"}
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


def terminal_of(pad: Pad, diode: bool = False) -> str:
    """Return ``cathode``, ``anode``, ``positive``, ``negative`` or ``""`` from the pin function.

    ``C`` counts as a cathode only when ``diode`` is True (see the token note above).
    """
    token = normalise_function(pad.pin_function)
    if token in _CATHODE_TOKENS or (diode and token == "C"):
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
    """Return ``diode``, ``polar_cap`` or ``other`` (spec section 7.1).

    KiCad-side evidence wins: a footprint named ``D_``/``LED_`` or pads whose functions
    say cathode/anode make the part a diode whatever labels EasyEDA's symbol uses
    (LED symbols labelled ``+``/``-`` are common).  Then capacitor evidence, then
    EasyEDA's own diode evidence.
    """
    family = extract_family(package_name).upper()
    footprint = kicad_footprint_name.rsplit(":", 1)[-1].upper()
    terminals = {terminal_of(pad) for pad in kicad_pads}
    labels = {pin.label.strip().upper() for pin in symbol_pins}
    polarity_tokens = {
        t for t in extract_orientation_tokens(package_name) if t in ("FD", "RD")
    }
    if footprint.startswith(("D_", "LED_")) or terminals & {"cathode", "anode"}:
        return "diode"
    if (
        family in POLARIZED_CAP_FAMILIES
        or (family.startswith("CAP") and polarity_tokens)
        or footprint.startswith(("CP_", "C_ELEC", "TANTALUM"))
        or terminals & {"positive", "negative"}
        or labels & _CAP_LABELS
    ):
        return "polar_cap"
    if family in CATHODE_PIN1_FAMILIES or labels & _DIODE_LABELS:
        return "diode"
    return "other"


def kicad_reference_pad(
    pads: list[Pad], reference: str, diode: bool = False
) -> tuple[Pad | None, bool, str]:
    """Return (pad, assumed, note) for the KiCad pad carrying ``reference``.

    A pin function naming the reference terminal, or its opposite on the other of two
    pads, decides; anode and positive (cathode and negative) are interchangeable.  Two
    pads claiming the same terminal are contradictory: no pad, with a note.  Without any
    usable function, pad 1 is assumed and said so.
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
    for pad in pads:
        if pad.number == "1":
            return (
                pad,
                True,
                f"assumed KiCad pad 1 = {'K' if reference == 'cathode' else '+'}",
            )
    return None, True, "no pad 1 on the KiCad side"


def side_of(pad: Pad, pads: list[Pad]) -> str | None:
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


def pin1_meaning(pads: list[Pad], kind: str, diode: bool = False) -> str | None:
    """Return 'A' or 'K' for what the KiCad side means by pad 1, by function or convention."""
    pad1 = next((p for p in pads if p.number == "1"), None)
    if pad1 is None:
        return None
    terminal = terminal_of(pad1, diode)
    if terminal in ("cathode", "negative"):
        return "K"
    if terminal in ("anode", "positive"):
        return "A"
    return "K" if kind == "diode" else "A"
