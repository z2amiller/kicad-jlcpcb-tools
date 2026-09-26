"""Canonical handling of LCSC part numbers."""

import re


def normalize_lcsc(value):
    """Return an LCSC part number in canonical form for keying and comparison.

    Part numbers reach the plugin from a footprint field, a pasted clipboard
    string, the parts database and saved part preferences. The current entry
    points canonicalise what they accept, but a project database written by an
    earlier release can still hold a number as it was typed then, and nothing
    rewrites stored rows. Corrections are keyed exactly, so every key and every
    lookup passes through here or a rule silently never fires.
    """
    if not value:
        return ""
    return str(value).strip().upper()


def is_lcsc_part(value):
    """Report whether a value names an LCSC part number.

    The value is normalised first, so a number typed into a schematic field
    with a stray space or in lower case still reads as the part it names.
    Testing raw text against a bare C-plus-digits pattern rejects exactly the
    values issue #773 is about, so the test belongs beside the normaliser and
    every caller gets the same answer.
    """
    return bool(re.fullmatch(r"C[0-9]+", normalize_lcsc(value)))


# A part number only counts when no ASCII letter or digit touches it. Product
# links put it after "_" or "/", and a lookalike such as "C0G" or the "C0603"
# inside "RC0603FR" must never be read as a part.
_STANDALONE_LCSC_PART = re.compile(r"(?<![A-Za-z0-9])[Cc][0-9]+(?![A-Za-z0-9])")


def parse_lcsc_entry(text):
    """Return the one LCSC part number in typed or pasted text, or "".

    The Enter LCSC prompt accepts a bare number and also a product link copied
    from lcsc.com or jlcpcb.com, so the number is searched for rather than
    matched against the whole text. Text naming two different numbers is
    ambiguous and yields nothing, so the prompt never guesses which was meant.
    """
    found = {match.upper() for match in _STANDALONE_LCSC_PART.findall(str(text or ""))}
    return found.pop() if len(found) == 1 else ""
