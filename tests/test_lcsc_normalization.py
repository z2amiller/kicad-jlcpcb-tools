"""Tests for canonical LCSC part numbers and the lookups keyed on them."""

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
parse_lcsc_entry = _lcsc.parse_lcsc_entry


class TestNormalizeLcsc:
    """normalize_lcsc folds the forms a part number arrives in into one."""

    @pytest.mark.parametrize(
        "value", ["C12345", "c12345", " C12345 ", "\tc12345\n", "C12345 "]
    )
    def test_all_spellings_reach_one_key(self, value):
        """Case and surrounding whitespace do not change the key."""
        assert normalize_lcsc(value) == "C12345"

    @pytest.mark.parametrize("value", ["", None, 0])
    def test_absent_values_become_empty(self, value):
        """A missing part number normalizes to the empty string, not "NONE"."""
        assert normalize_lcsc(value) == ""

    def test_distinct_parts_stay_distinct(self):
        """Normalization does not merge different part numbers."""
        assert normalize_lcsc("C1234") != normalize_lcsc("C12345")


class TestIsLcscPart:
    """is_lcsc_part validates whether a normalized string is an LCSC part number."""

    @pytest.mark.parametrize(
        "value", ["C12345", "c12345", " C12345 ", "\tc12345\n", "C1"]
    )
    def test_valid_part_numbers_accepted(self, value):
        """Standard C-prefix followed by ASCII digits is recognized."""
        assert is_lcsc_part(value) is True

    @pytest.mark.parametrize(
        "value",
        [
            "",
            None,
            0,
            "C",
            "12345",
            "CC12345",
            "C１２３",  # Fullwidth decimal digits
            "C¹²³",  # Superscript digits
            "C 12345",  # Internal whitespace
        ],
    )
    def test_invalid_values_and_unicode_digits_rejected(self, value):
        """Non-part strings and non-ASCII digits are rejected."""
        assert is_lcsc_part(value) is False


class TestParseLcscEntry:
    """parse_lcsc_entry reads the one part number in typed or pasted text."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("C2925591", "C2925591"),
            (" c2925591 ", "C2925591"),
            ("https://www.lcsc.com/product-detail/C2925591.html", "C2925591"),
            (
                "https://www.lcsc.com/product-detail/"
                "Multilayer-Ceramic-Capacitors-MLCC-SMD-SMT_Samsung-Electro-Mechanics-"
                "CL10A106KP8NNNC_C19702.html",
                "C19702",
            ),
            (
                "https://www.lcsc.com/product-detail/"
                "Multilayer-Ceramic-Capacitors-MLCC-SMD-SMT_KEMET-C0603C104K5RACTU_C1590.html",
                "C1590",
            ),
            (
                "https://jlcpcb.com/partdetail/"
                "SamsungElectro_Mechanics-CL10A106KP8NNNC/C19702",
                "C19702",
            ),
            ("C19702 C19702", "C19702"),  # The same number twice is still one part
        ],
    )
    def test_one_number_is_read_from_a_code_or_product_link(self, text, expected):
        """A bare number or a product link yields the number it names."""
        assert parse_lcsc_entry(text) == expected

    @pytest.mark.parametrize(
        "text",
        [
            "",
            None,
            "   ",
            "C0G",  # A dielectric, not a part
            "RC0603FR-0710KL",  # A letter touches the C
            "RC0603",  # A letter touches the C, with nothing after the digits
            "GRM188R71C104",  # A digit touches the C
            "CL10A106KP8NNNC",  # The C is followed by a letter
            "C123 C456",  # Two different parts are ambiguous
            "C１２３",  # Fullwidth digits are not part numbers
        ],
    )
    def test_text_without_exactly_one_number_yields_nothing(self, text):
        """No number, a lookalike, or two different numbers give the empty string."""
        assert parse_lcsc_entry(text) == ""
