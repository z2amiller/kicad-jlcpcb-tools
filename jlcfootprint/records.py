"""Small records the resolver core shares with the parsers.

``SymbolPin`` (a pin's number and label), the pin-1 label sets, the reading
``pin1_polarity`` takes from them, and ``DrawingMarks`` (what the drawings say
about polarity and the body box) are two fields, two frozensets and a loop over
them; the resolver core (``fit.py``, ``polarity.py``, ``resolver.py``) needs
them without needing the 656-line EasyEDA response parser or the 657-line
drawing reader that build them.  ``easyeda_parse.py`` and ``drawing.py`` import
``SymbolPin`` and ``DrawingMarks`` back from here, so every existing import of
those two names from those two modules keeps working.
"""

from __future__ import annotations

from dataclasses import dataclass

# Label sets shared with the crawler's extract_pin1_polarity.
PIN1_CATHODE_LABELS = frozenset({"K", "C", "CA", "CAT", "CATHODE", "K1", "NEG", "-"})
PIN1_ANODE_LABELS = frozenset({"A", "AN", "ANODE", "A1", "AK", "POS", "+"})


@dataclass
class SymbolPin:
    """One schematic-symbol pin: its number and its label text (may be empty)."""

    number: str
    label: str


@dataclass(frozen=True)
class DrawingMarks:
    """What the drawings say: the ``+`` pin, the ``+`` pad and the body box in millimetres."""

    positive_pin: str | None = None
    positive_pad: str | None = None
    body_box: tuple[float, float, float, float] | None = None


def pin1_polarity(pins: list[SymbolPin]) -> str | None:
    """Return 'K', 'A' or None for pin 1, using the crawler's label sets."""
    for pin in pins:
        if pin.number != "1":
            continue
        label = pin.label.upper()
        if label in PIN1_CATHODE_LABELS:
            return "K"
        if label in PIN1_ANODE_LABELS:
            return "A"
    return None
