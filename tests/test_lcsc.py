"""Tests for the single definition of an LCSC part number."""

import importlib.util
from pathlib import Path

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

    @pytest.mark.parametrize(
        "value", ["C12345", "c12345", " C12345 ", "C1002", "C9900101779"]
    )
    def test_a_part_number_is_recognised(self, value):
        """A part number is recognised however it was typed.

        C1002 is the smallest number in the JLC assembly catalogue and
        C9900101779 the longest, so the bounds are covered rather than assumed.
        """
        assert is_lcsc_part(value)

    @pytest.mark.parametrize("value", ["C1", "C12", "C123", "C999"])
    def test_a_reference_designator_is_not_a_part_number(self, value):
        """A capacitor designator shares the shape but never the length.

        No catalogue part has fewer than four digits, and no board has a
        thousand capacitors, so the two ranges do not overlap in practice.
        """
        assert not is_lcsc_part(value)

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

    @pytest.mark.parametrize("text", ["C12", "C1,C2,C3", "refdes C99 on the board"])
    def test_pasted_reference_designators_yield_nothing(self, text):
        """Copying a designator into the LCSC box does not assign a part.

        This is the mistake the length bound is really for: the strict check
        is guarded by a field-name match, but pasted text has no such guard.
        """
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
