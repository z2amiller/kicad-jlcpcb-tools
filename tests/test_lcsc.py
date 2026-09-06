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
Lcsc = _lcsc.Lcsc
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
        """A capacitor designator in the usual range is not a part number.

        No catalogue part has fewer than four digits, so designators below
        C1000 cannot be mistaken for one. The ranges do still meet above that:
        a large array, or a sub-assembly numbered from C1001, gets there.
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


# ---------------------------------------------------------------------------
# The value type
# ---------------------------------------------------------------------------


class TestLcscConstruction:
    """An Lcsc in hand is a real part number in canonical form."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("C12345", "C12345"),
            ("c12345", "C12345"),
            (" C12345 ", "C12345"),
            ("\tc12345\n", "C12345"),
            ("C1002", "C1002"),
        ],
    )
    def test_it_canonicalises_what_it_is_given(self, value, expected):
        """Every spelling of one number produces the same part."""
        assert str(Lcsc(value)) == expected

    @pytest.mark.parametrize(
        "value", ["", None, "C", "C12", "C999", "12345", "foo C12345 bar"]
    )
    def test_it_refuses_anything_that_is_not_one(self, value):
        """The constructor is the check, so holding one needs no further test."""
        with pytest.raises((ValueError, TypeError)):
            Lcsc(value)

    def test_parse_answers_none_instead_of_raising(self):
        """Values merely claimed to be part numbers go through parse."""
        assert Lcsc.parse("C12345") == Lcsc("C12345")
        assert Lcsc.parse("not a part") is None
        assert Lcsc.parse(None) is None

    def test_find_in_reads_pasted_text(self):
        """find_in is lenient about surroundings, strict about the number."""
        assert Lcsc.find_in("LCSC Part C12345 in stock") == Lcsc("C12345")
        assert Lcsc.find_in("designators C1, C2, C3") is None
        assert Lcsc.find_in("") is None


class TestLcscBehaviour:
    """It formats and compares as the thing it represents."""

    def test_it_renders_as_the_bare_number(self):
        """Rendering gives the canonical number, not a repr."""
        part = Lcsc.parse(" c12345 ")
        assert str(part) == "C12345"
        assert f"ordering {part}" == "ordering C12345"

    def test_spellings_compare_equal(self):
        """Two spellings of one number are one part."""
        assert Lcsc("c12345 ") == Lcsc("C12345")

    def test_different_parts_are_not_equal(self):
        """Distinct numbers stay distinct."""
        assert Lcsc("C12345") != Lcsc("C12346")

    def test_it_works_as_a_dict_key(self):
        """Being frozen makes it usable as a cache key, which is how it is used.

        The details cache in populate_footprint_list is keyed on it, so two
        spellings collapsing to one entry is the property that matters.
        """
        cache = {Lcsc("C12345"): "details"}
        assert cache[Lcsc(" c12345 ")] == "details"
        assert len({Lcsc("C12345"), Lcsc("c12345")}) == 1

    def test_it_is_immutable(self):
        """A part cannot be edited into a different part after validation."""
        part = Lcsc("C12345")
        with pytest.raises(Exception):
            part.value = "C99999"

    def test_it_sorts_by_number(self):
        """Ordering is defined, so part lists can be sorted without a key."""
        parts = [Lcsc("C12345"), Lcsc("C1002"), Lcsc("C99999")]
        assert [str(p) for p in sorted(parts)] == ["C1002", "C12345", "C99999"]


class TestHelpersAgreeWithTheType:
    """The string helpers are the type's answers, not a second implementation."""

    @pytest.mark.parametrize(
        "value", ["C12345", "c12345", " C12345 ", "C999", "C", "", None, "junk"]
    )
    def test_is_lcsc_part_matches_parse(self, value):
        """is_lcsc_part is true exactly when parse returns a part."""
        assert is_lcsc_part(value) == (Lcsc.parse(value) is not None)

    @pytest.mark.parametrize(
        "text", ["C12345", "buy C12345 now", "C1,C2", "", None, "no part"]
    )
    def test_extract_lcsc_matches_find_in(self, text):
        """extract_lcsc is find_in rendered as a string."""
        found = Lcsc.find_in(text)
        assert extract_lcsc(text) == (str(found) if found else "")
