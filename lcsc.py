"""Canonical handling of LCSC part numbers.

Part numbers reach the plugin from a footprint field, the clipboard, the parts
database and the mapping table, and only some of those sources are already
upper case and unpadded. Every question about a part number -- is this one,
what is one called, is there one in this text -- is answered here, so the
answers cannot drift apart.
"""

import re

_PART_NUMBER = re.compile(r"^C\d+$")
_PART_NUMBER_IN_TEXT = re.compile(r"C\d+", re.IGNORECASE)


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
    surrounded by whatever came with it.
    """
    if not text:
        return ""
    match = _PART_NUMBER_IN_TEXT.search(str(text))
    return normalize_lcsc(match.group(0)) if match else ""
