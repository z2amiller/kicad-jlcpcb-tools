"""Canonical handling of LCSC part numbers.

Part numbers reach the plugin from a footprint field, the clipboard, the parts
database and the mapping table, and only some of those sources are already
upper case and unpadded. Every question about a part number -- is this one,
what is one called, is there one in this text -- is answered here, so the
answers cannot drift apart.

This module is meant to be the only place that decides what a part number
looks like. Three separate spellings of that decision had drifted apart before
it existed: two anchored patterns in footprint_helpers and an unanchored,
case-insensitive one in mainwindow, which disagreed about whether "C12345 "
was a part number at all. Reach for is_lcsc_part or extract_lcsc rather than
writing a fourth.
"""

import re

# At least four digits. Measured against the full JLC assembly catalogue
# (708,966 parts, 2026-04-17 snapshot): no part number has fewer, the smallest
# is C1002, and every entry is a capital C followed only by digits. A second,
# independently built cache agrees. Requiring four digits costs nothing today
# and stops a capacitor reference designator -- C1 through C999, which is every
# designator a board of this size will ever have -- from reading as a part
# number. If LCSC ever issues a shorter one, this is the line to relax.
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

    The value is normalised first, so a number typed into a schematic field
    with a stray space or in lower case still reads as the part it names.
    """
    return bool(_PART_NUMBER.match(normalize_lcsc(value)))


def extract_lcsc(text):
    """Return the first LCSC part number appearing anywhere in text.

    Unlike :func:`is_lcsc_part` this is deliberately lenient, because it reads
    what a person pasted: a part number copied out of a web page arrives
    surrounded by whatever came with it. It is lenient about the surroundings
    only -- a copied reference designator like C12 is not a part number and
    does not become one by being pasted into the right box.
    """
    if not text:
        return ""
    match = _PART_NUMBER_IN_TEXT.search(str(text))
    return normalize_lcsc(match.group(0)) if match else ""
