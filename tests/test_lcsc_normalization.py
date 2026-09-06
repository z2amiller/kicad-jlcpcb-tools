"""Tests for canonical LCSC part numbers and the lookups keyed on them."""

import importlib.util
from pathlib import Path
import sys
import types
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).parent.parent

_spec = importlib.util.spec_from_file_location("standalone_lcsc", _ROOT / "lcsc.py")
assert _spec is not None and _spec.loader is not None
_lcsc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lcsc)

normalize_lcsc = _lcsc.normalize_lcsc


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


# ---------------------------------------------------------------------------
# The lookups that are keyed on it
# ---------------------------------------------------------------------------


def _load(name, stub_footprint_helpers=False):
    """Load a plugin module with its KiCad and wx dependencies mocked out."""
    for mod in ["pcbnew", "wx", "wx.dataview", "requests"]:
        sys.modules.setdefault(mod, MagicMock())
    pkg = sys.modules.get("kicadplugin")
    if pkg is None:
        pkg = types.ModuleType("kicadplugin")
        pkg.__path__ = [str(_ROOT)]
        sys.modules["kicadplugin"] = pkg
    for dep in [
        "dblib",
        "events",
        "helpers",
        "partselector_columns",
        "search_escape",
        "unzip_parts",
    ]:
        sys.modules.setdefault(f"kicadplugin.{dep}", MagicMock())
    if stub_footprint_helpers:
        fh = types.ModuleType("kicadplugin.footprint_helpers")
        fh.get_is_dnp = lambda fp: False
        sys.modules.setdefault("kicadplugin.footprint_helpers", fh)
    spec = importlib.util.spec_from_file_location(
        f"kicadplugin.{name}", _ROOT / f"{name}.py"
    )
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "kicadplugin"
    sys.modules[f"kicadplugin.{name}"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def library(tmp_path):
    """Build a bare Library on a temporary corrections database."""
    lib = object.__new__(_load("library").Library)
    lib.logger = MagicMock()
    lib.correctionsdb_file = str(tmp_path / "corrections.db")
    lib.create_lcsc_correction_table()
    return lib


class TestTableStoresCanonicalKeys:
    """The lcsc_correction table only ever holds the canonical form."""

    def test_a_lowercase_key_is_stored_uppercase(self, library):
        """Saving c12345 produces a rule that C12345 will find."""
        library.insert_lcsc_correction_data("c12345", 90, (0.0, 0.0))
        assert library.get_all_lcsc_correction_data() == [("C12345", 90, (0.0, 0.0))]

    def test_a_padded_key_is_stored_trimmed(self, library):
        """Surrounding whitespace never reaches the table."""
        library.insert_lcsc_correction_data("  C12345 ", 90, (0.0, 0.0))
        assert library.get_all_lcsc_correction_data()[0][0] == "C12345"

    def test_lookup_by_any_spelling_finds_the_rule(self, library):
        """A rule can be read back using whichever form the caller holds."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        assert library.get_lcsc_correction_data("c12345") is not None
        assert library.get_lcsc_correction_data(" C12345 ") is not None

    def test_update_and_delete_accept_any_spelling(self, library):
        """Editing and removing work from a differently cased key."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.update_lcsc_correction_data("c12345", 180, (0.0, 0.0))
        assert library.get_all_lcsc_correction_data()[0][1] == 180
        library.delete_lcsc_correction_data(" c12345 ")
        assert library.get_all_lcsc_correction_data() == []


class TestCplLookupNormalizes:
    """The CPL resolves a correction whatever case the store holds."""

    def test_a_lowercase_store_value_still_matches(self):
        """A part pasted as c12345 gets the rule saved for C12345.

        sanitize_lcsc() matches case-insensitively and returns the text as
        typed, so a pasted lower-case number reaches the store unchanged.
        """
        fabrication = _load("fabrication", stub_footprint_helpers=True)
        fab = object.__new__(fabrication.Fabrication)
        fab.logger = MagicMock()
        fab.corrections = []
        fab.lcsc_corrections = {"C12345": (90, (0.0, 0.0))}
        assert fab._find_lcsc_correction("c12345") == (90, (0.0, 0.0))
        assert fab._find_lcsc_correction("  C12345 ") == (90, (0.0, 0.0))

    def test_an_unrelated_part_still_misses(self):
        """Normalization does not make unrelated part numbers match."""
        fabrication = _load("fabrication", stub_footprint_helpers=True)
        fab = object.__new__(fabrication.Fabrication)
        fab.logger = MagicMock()
        fab.corrections = []
        fab.lcsc_corrections = {"C12345": (90, (0.0, 0.0))}
        assert fab._find_lcsc_correction("c1234") is None
