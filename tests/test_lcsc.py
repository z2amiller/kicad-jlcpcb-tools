"""Tests for the single definition of an LCSC part number."""

import importlib.util
from pathlib import Path
import re

import pytest

_ROOT = Path(__file__).parent.parent

_spec = importlib.util.spec_from_file_location("standalone_lcsc", _ROOT / "lcsc.py")
assert _spec is not None and _spec.loader is not None
_lcsc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lcsc)

normalize_lcsc = _lcsc.normalize_lcsc
is_lcsc_part = _lcsc.is_lcsc_part
extract_lcsc = _lcsc.extract_lcsc


class TestNormalize:
    """normalize_lcsc folds the forms a part number arrives in into one."""

    @pytest.mark.parametrize(
        "value", ["C12345", "c12345", " C12345 ", "\tc12345\n", "C12345 "]
    )
    def test_every_spelling_reaches_one_key(self, value):
        """Case and surrounding whitespace do not change the key."""
        assert normalize_lcsc(value) == "C12345"

    @pytest.mark.parametrize("value", ["", None, 0])
    def test_absent_values_become_empty(self, value):
        """A missing number normalizes to the empty string, not "NONE"."""
        assert normalize_lcsc(value) == ""

    def test_distinct_parts_stay_distinct(self):
        """Normalizing does not merge different part numbers."""
        assert normalize_lcsc("C1234") != normalize_lcsc("C12345")


class TestIsLcscPart:
    """is_lcsc_part answers whether a value names a part, strictly."""

    @pytest.mark.parametrize("value", ["C12345", "c12345", " C12345 ", "C1"])
    def test_a_part_number_is_recognised(self, value):
        """A part number is recognised however it was typed."""
        assert is_lcsc_part(value)

    @pytest.mark.parametrize(
        "value",
        ["", None, "C", "12345", "CC1", "C12a45", "foo C12345 bar", "C12345,C9"],
    )
    def test_anything_else_is_rejected(self, value):
        """A value that is not exactly one part number is not one."""
        assert not is_lcsc_part(value)


class TestExtractLcsc:
    """extract_lcsc is the lenient counterpart, for text a person pasted."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("C12345", "C12345"),
            ("c12345", "C12345"),
            ("  C12345  ", "C12345"),
            ("LCSC Part C12345 in stock", "C12345"),
            ("https://jlcpcb.com/partdetail/C12345", "C12345"),
            ("C12345, C99999", "C12345"),
        ],
    )
    def test_a_part_number_is_pulled_out_canonically(self, text, expected):
        """A number is found inside surrounding text and returned canonically."""
        assert extract_lcsc(text) == expected

    @pytest.mark.parametrize("text", ["", None, "no part here", "12345"])
    def test_text_without_a_part_number_yields_empty(self, text):
        """Text carrying no part number produces the empty string."""
        assert extract_lcsc(text) == ""

    def test_it_is_more_lenient_than_is_lcsc_part(self):
        """The two answer different questions and are not interchangeable.

        Pasted text should give up its part number; a schematic field claiming
        to *be* a part number should not be accepted when it is something else
        with a number buried in it.
        """
        text = "LCSC Part C12345 in stock"
        assert extract_lcsc(text) == "C12345"
        assert not is_lcsc_part(text)


class TestOneDefinition:
    """No module outside lcsc.py may decide what a part number looks like.

    Three separate definitions had drifted apart before this was consolidated:
    two anchored ones in footprint_helpers and an unanchored, case-insensitive
    one in mainwindow. The disagreement is what let a field of "C12345 " read
    as no part at all. A fourth would reintroduce the same class of bug, so
    this fails rather than waiting for someone to notice.
    """

    # A part number written as a pattern, in the spellings that actually turn
    # up: a raw string (C\\d+), a plain string where the backslash is doubled
    # (C\\\\d+), or an explicit class (C[0-9]+). This is a heuristic, not a
    # proof -- str.startswith("C") plus str.isdigit() would pass it -- but it
    # catches every form this codebase has used.
    DEFINITION = re.compile(r"C\\{1,2}d|C\[0-9\]")

    # footprint_helpers.py still spells the pattern out three times. Those go
    # away with the fix for issue #773, which is in flight separately; until it
    # lands this allows them while still refusing any new ones.
    ALLOWED = {"footprint_helpers.py"}

    PACKAGES = ("common", "dblib", "core", "bom_estimation", "enrichment")

    def test_the_part_number_pattern_appears_only_in_lcsc_py(self):
        """Only lcsc.py may spell out the part-number pattern."""
        paths = list(_ROOT.glob("*.py"))
        for package in self.PACKAGES:
            paths.extend((_ROOT / package).rglob("*.py"))

        offenders = []
        for path in sorted(paths):
            if path.name == "lcsc.py" or path.name in self.ALLOWED:
                continue
            source = path.read_text(encoding="utf-8")
            for number, line in enumerate(source.splitlines(), start=1):
                if self.DEFINITION.search(line):
                    rel = path.relative_to(_ROOT)
                    offenders.append(f"{rel}:{number}: {line.strip()}")
        assert offenders == [], (
            "these lines define an LCSC part number outside lcsc.py; call "
            "is_lcsc_part() or extract_lcsc() instead.\n"
            "If the line matches a capacitor reference designator rather than a "
            "part number -- the two look identical -- add the file to ALLOWED "
            "with a comment saying so.\n" + "\n".join(offenders)
        )

    def test_the_allowance_is_still_needed(self):
        """Delete an allowed file from the set once it stops defining one.

        An allowance nobody needs is worse than none: it silently re-permits
        the thing this guards against.
        """
        stale = [
            name
            for name in sorted(self.ALLOWED)
            if not (_ROOT / name).is_file()
            or not self.DEFINITION.search((_ROOT / name).read_text(encoding="utf-8"))
        ]
        assert stale == [], (
            f"these files no longer define a part number; remove them from "
            f"ALLOWED: {stale}"
        )
