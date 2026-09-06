"""Canonical handling of LCSC part numbers."""


def normalize_lcsc(value):
    """Return an LCSC part number in canonical form for keying and comparison.

    LCSC numbers reach us from several places -- a footprint field, a pasted
    clipboard string, the parts database, a mapping -- and only some of them
    are already upper case and unpadded. sanitize_lcsc() in particular matches
    case-insensitively and returns the text as typed, so "c12345" survives into
    the store. Corrections are keyed exactly, so every key and every lookup has
    to pass through here or a rule silently never fires.
    """
    if not value:
        return ""
    return str(value).strip().upper()
