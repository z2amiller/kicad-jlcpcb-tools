r"""Canonical handling of LCSC part numbers.

Part numbers reach the plugin from a footprint field, the clipboard, the parts
database and the mapping table, and not all of those are upper case and
unpadded. Every question about one -- is this a part number, what is it called,
is there one in this text -- is answered here, rather than being decided again
at each call site.

:class:`Lcsc` is the preferred form: a value that has been through it is a real
part number in canonical form, which a ``str`` never tells you. The plain
string helpers are the same answers rendered as strings, for boundaries that
must hand one to sqlite, wx or a CSV writer.

Absence is ``None``, never an empty :class:`Lcsc`, and there is no
``.valid()`` -- a part in hand is always valid. The empty string means "no
part" only at the edges, and :func:`format_lcsc` is where that conversion
happens.
"""

from dataclasses import dataclass
import re
from typing import Optional

# At least four digits: measured across the JLC assembly catalogue (708,966
# parts), where every number is a capital C and digits and the shortest is
# C1002. The floor also keeps capacitor reference designators, which share the
# shape, from reading as part numbers -- unlikely to collide above C1000, and
# mostly harmless when it does.
_PART_NUMBER = re.compile(r"^C\d{4,}$")
_PART_NUMBER_IN_TEXT = re.compile(r"C\d{4,}", re.IGNORECASE)


def normalize_lcsc(value):
    """Return an LCSC part number in canonical form for keying and comparison.

    The parts database filters its own FTS5 results with ``==``, so a value
    differing only in case or padding finds nothing and reports no error. The
    same value is also used as a cache key and grouped on when the BOM is
    written, where two spellings of one part silently become two parts.
    """
    if not value:
        return ""
    return str(value).strip().upper()


def is_lcsc_part(value):
    """Report whether a value names an LCSC part.

    The string-level form of :meth:`Lcsc.parse`, for boundaries that only need
    the answer and not the part.
    """
    return Lcsc.parse(value) is not None


def extract_lcsc(text):
    """Return the first LCSC part number in text, as a string, or "".

    The string-level form of :meth:`Lcsc.find_in`.
    """
    found = Lcsc.find_in(text)
    return str(found) if found else ""


@dataclass(frozen=True)
class Lcsc:
    """A JLCPCB/LCSC part number, known to be well formed and canonical.

    Construct one through :meth:`parse` or :meth:`find_in`, which answer
    ``None`` when the text does not name a part. The constructor itself
    rejects anything that is not a part number, so an ``Lcsc`` in hand needs
    no further checking -- which is the whole point of having the type. It is
    frozen, so it can be a dict key, and it renders as the bare number, so it
    can be formatted straight into a query, a CSV cell or a log line.

    Deliberately not ordered. Comparing the strings would put C10000 before
    C9999, and comparing the digits would invent a ranking that means nothing
    -- part numbers are identifiers, not quantities. Sort with an explicit key
    if a display ever needs one.

        >>> part = Lcsc.parse(" c12345 ")
        >>> str(part)
        'C12345'
        >>> f"ordering {part}"
        'ordering C12345'
    """

    value: str

    def __post_init__(self):
        """Reject anything that is not a canonical part number."""
        canonical = normalize_lcsc(self.value)
        if not _PART_NUMBER.match(canonical):
            raise ValueError(f"not an LCSC part number: {self.value!r}")
        # frozen dataclasses refuse plain assignment, even from __post_init__
        object.__setattr__(self, "value", canonical)

    @classmethod
    def parse(cls, value) -> Optional["Lcsc"]:
        """Return the part this value names, or None if it names none.

        Use this wherever a value is *claimed* to be a part number -- a
        schematic field, a database column -- and the claim has to be checked.
        """
        try:
            return cls(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def find_in(cls, text) -> Optional["Lcsc"]:
        """Return the first part number appearing anywhere in text.

        Use this for text a person pasted, where the number arrives with
        whatever surrounded it. It is lenient about the surroundings only: a
        copied reference designator is not a part number and does not become
        one by being pasted into the right box.
        """
        if not text:
            return None
        match = _PART_NUMBER_IN_TEXT.search(str(text))
        return cls(match.group(0)) if match else None

    def __str__(self) -> str:
        """Render as the bare canonical part number."""
        return self.value
