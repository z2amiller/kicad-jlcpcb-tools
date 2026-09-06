"""Tests for the lcsc_correction table and its global/board-local switching."""

import importlib.util
from pathlib import Path
import sqlite3
import sys
import types
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).parent.parent

# Mock the KiCad and network modules library.py imports at module scope
for _mod in ["pcbnew", "wx", "wx.dataview", "requests"]:
    sys.modules.setdefault(_mod, MagicMock())

# library.py uses relative imports, so give it a fake parent package
_pkg = types.ModuleType("kicadplugin")
_pkg.__path__ = [str(_ROOT)]
sys.modules["kicadplugin"] = _pkg
for _name in [
    "dblib",
    "events",
    "helpers",
    "partselector_columns",
    "search_escape",
    "unzip_parts",
]:
    sys.modules[f"kicadplugin.{_name}"] = MagicMock()

_spec = importlib.util.spec_from_file_location(
    "kicadplugin.library", _ROOT / "library.py"
)
assert _spec is not None and _spec.loader is not None
_lib_mod = importlib.util.module_from_spec(_spec)
_lib_mod.__package__ = "kicadplugin"
sys.modules["kicadplugin.library"] = _lib_mod
_spec.loader.exec_module(_lib_mod)

Library = _lib_mod.Library


@pytest.fixture
def library(tmp_path):
    """Build a bare Library wired only to temporary corrections databases."""
    lib = object.__new__(Library)
    lib.logger = MagicMock()
    lib.globalcorrectionsdb_file = str(tmp_path / "corrections.db")
    lib.localcorrectionsdb_file = str(tmp_path / "project.db")
    lib.correctionsdb_file = lib.globalcorrectionsdb_file
    lib.create_correction_table()
    lib.create_lcsc_correction_table()
    return lib


def table_names(path):
    """Return the set of table names in the database at path."""
    with sqlite3.connect(path) as con:
        return {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


class TestLcscCorrectionCrud:
    """Insert, read, update and delete round-trip through the lcsc_correction table."""

    def test_insert_and_read_back(self, library):
        """An inserted correction reads back with its rotation and offset."""
        library.insert_lcsc_correction_data("C12345", 90, (0.5, -0.25))
        assert library.get_all_lcsc_correction_data() == [("C12345", 90, (0.5, -0.25))]

    def test_get_by_exact_lcsc(self, library):
        """Lookup is by exact part number."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        assert library.get_lcsc_correction_data("C12345")[0] == "C12345"

    def test_lookup_is_not_a_prefix_match(self, library):
        """C1234 does not match the entry stored for C12345."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        assert library.get_lcsc_correction_data("C1234") is None

    def test_update_changes_values_only(self, library):
        """Updating replaces rotation and offset for that part number."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.update_lcsc_correction_data("C12345", 180, (1.0, 2.0))
        assert library.get_all_lcsc_correction_data() == [("C12345", 180, (1.0, 2.0))]

    def test_delete_removes_the_entry(self, library):
        """Deleting leaves the table empty."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.delete_lcsc_correction_data("C12345")
        assert library.get_all_lcsc_correction_data() == []

    def test_results_are_ordered_by_part_number(self, library):
        """Entries come back sorted by LCSC number."""
        for lcsc in ("C300", "C100", "C200"):
            library.insert_lcsc_correction_data(lcsc, 0, (0.0, 0.0))
        assert [c[0] for c in library.get_all_lcsc_correction_data()] == [
            "C100",
            "C200",
            "C300",
        ]

    def test_part_number_is_unique(self, library):
        """A second insert for the same part number is rejected."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        with pytest.raises(sqlite3.IntegrityError):
            library.insert_lcsc_correction_data("C12345", 180, (0.0, 0.0))

    def test_negative_rotation_survives_the_round_trip(self, library):
        """Rotations are stored signed, matching the shipped corrections."""
        library.insert_lcsc_correction_data("C12345", -90, (0.0, 0.0))
        assert library.get_all_lcsc_correction_data()[0][1] == -90


# ---------------------------------------------------------------------------
# Databases that predate the lcsc_correction table
# ---------------------------------------------------------------------------


class TestMissingTable:
    """Reads tolerate a corrections database that has no lcsc_correction table."""

    def test_get_all_returns_empty(self, tmp_path):
        """An old corrections database reads as having no LCSC corrections."""
        lib = object.__new__(Library)
        lib.logger = MagicMock()
        lib.correctionsdb_file = str(tmp_path / "old.db")
        lib.create_correction_table()
        assert lib.get_all_lcsc_correction_data() == []

    def test_get_one_returns_none(self, tmp_path):
        """A single lookup against an old database returns None, not an error."""
        lib = object.__new__(Library)
        lib.logger = MagicMock()
        lib.correctionsdb_file = str(tmp_path / "old.db")
        lib.create_correction_table()
        assert lib.get_lcsc_correction_data("C12345") is None

    def test_create_is_idempotent(self, library):
        """Creating the table again keeps existing rows."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.create_lcsc_correction_table()
        assert library.get_all_lcsc_correction_data() == [("C12345", 90, (0.0, 0.0))]


# ---------------------------------------------------------------------------
# Switching between the global and board-local corrections database
# ---------------------------------------------------------------------------


class TestGlobalLocalSwitch:
    """LCSC corrections travel with the regex corrections across a switch."""

    def test_going_local_copies_lcsc_corrections(self, library):
        """Switching to board-local copies both tables into the project database."""
        library.insert_correction_data("^SOT-23", 180, (0.0, 0.0))
        library.insert_lcsc_correction_data("C12345", 90, (0.5, -0.5))

        library.switch_to_global_correction_database(False)

        assert library.correctionsdb_file == library.localcorrectionsdb_file
        assert library.get_all_correction_data() == [("^SOT-23", 180, (0.0, 0.0))]
        assert library.get_all_lcsc_correction_data() == [("C12345", 90, (0.5, -0.5))]

    def test_going_local_leaves_the_global_database_intact(self, library):
        """The global database keeps its rows after switching to board-local."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.switch_to_global_correction_database(False)

        library.correctionsdb_file = library.globalcorrectionsdb_file
        assert library.get_all_lcsc_correction_data() == [("C12345", 90, (0.0, 0.0))]

    def test_going_global_drops_the_local_lcsc_table(self, library):
        """Switching back to global removes both board-local tables.

        uses_global_correction_database() decides which database is in use by
        looking for the correction table, so a surviving lcsc_correction table
        would strand board-local overrides in a database nothing reads.
        """
        library.insert_correction_data("^SOT-23", 180, (0.0, 0.0))
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.switch_to_global_correction_database(False)

        library.switch_to_global_correction_database(True)

        assert library.correctionsdb_file == library.globalcorrectionsdb_file
        assert "correction" not in table_names(library.localcorrectionsdb_file)
        assert "lcsc_correction" not in table_names(library.localcorrectionsdb_file)

    def test_switching_to_the_current_database_is_a_no_op(self, library):
        """Asking for the database already in use changes nothing."""
        library.insert_lcsc_correction_data("C12345", 90, (0.0, 0.0))
        library.switch_to_global_correction_database(True)
        assert library.correctionsdb_file == library.globalcorrectionsdb_file
        assert library.get_all_lcsc_correction_data() == [("C12345", 90, (0.0, 0.0))]
