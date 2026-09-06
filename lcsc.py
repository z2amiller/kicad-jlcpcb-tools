r"""Canonical handling of LCSC part numbers.

Part numbers reach the plugin from a footprint field, the clipboard, the parts
database and the mapping table, and not all of those are upper case and
unpadded. Every question about one -- is this a part number, what is it called,
is there one in this text -- is answered here, rather than being decided again
at each call site.
"""

import re

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
