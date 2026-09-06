"""Tests for the correction manager's routing between its two rule tables."""

import importlib.util
from pathlib import Path
import sys
import types
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).parent.parent

# wx.Dialog must be a real class: CorrectionManagerDialog subclasses it, and a
# MagicMock base yields a MagicMock rather than a class we can instantiate.
for _mod in ["wx", "wx.dataview"]:
    sys.modules.setdefault(_mod, MagicMock())
if not isinstance(sys.modules["wx"].Dialog, type):
    sys.modules["wx"].Dialog = type("Dialog", (), {})

_pkg = types.ModuleType("kicadplugin")
_pkg.__path__ = [str(_ROOT)]
sys.modules["kicadplugin"] = _pkg
for _name in ["events", "helpers"]:
    sys.modules[f"kicadplugin.{_name}"] = MagicMock()

_spec = importlib.util.spec_from_file_location(
    "kicadplugin.corrections", _ROOT / "corrections.py"
)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_mod.__package__ = "kicadplugin"
sys.modules["kicadplugin.corrections"] = _mod
_spec.loader.exec_module(_mod)

CorrectionManagerDialog = _mod.CorrectionManagerDialog
KIND_FOOTPRINT = _mod.KIND_FOOTPRINT
KIND_LCSC = _mod.KIND_LCSC


class FakeField:
    """Stand-in for a wx.TextCtrl or wx.CheckBox."""

    def __init__(self, value):
        self._value = value

    def GetValue(self):
        """Return the field's current value."""
        return self._value

    def SetValue(self, value):
        """Set the field's value."""
        self._value = value


class FakeList:
    """Stand-in for the DataViewListCtrl, holding (key, rot, x, y, kind) rows."""

    def __init__(self, rows=()):
        self.rows = [list(row) for row in rows]

    def GetItemCount(self):
        """Return how many rules are listed."""
        return len(self.rows)

    def GetTextValue(self, row, col):
        """Return one cell of the list."""
        return self.rows[row][col]


class RecordingLibrary:
    """Library stand-in that records which table each call was routed to."""

    def __init__(self):
        self.calls = []

    def insert_correction_data(self, regex, rotation, offset):
        """Record a footprint-rule insert."""
        self.calls.append(("insert", KIND_FOOTPRINT, regex, rotation, offset))

    def update_correction_data(self, regex, rotation, offset):
        """Record a footprint-rule update."""
        self.calls.append(("update", KIND_FOOTPRINT, regex, rotation, offset))

    def delete_correction_data(self, regex):
        """Record a footprint-rule delete."""
        self.calls.append(("delete", KIND_FOOTPRINT, regex))

    def insert_lcsc_correction_data(self, lcsc, rotation, offset):
        """Record an LCSC-rule insert."""
        self.calls.append(("insert", KIND_LCSC, lcsc, rotation, offset))

    def update_lcsc_correction_data(self, lcsc, rotation, offset):
        """Record an LCSC-rule update."""
        self.calls.append(("update", KIND_LCSC, lcsc, rotation, offset))

    def delete_lcsc_correction_data(self, lcsc):
        """Record an LCSC-rule delete."""
        self.calls.append(("delete", KIND_LCSC, lcsc))


def make_dialog(key="", lcsc_mode=False, rows=(), selection=None):
    """Build a bare dialog wired to fakes, skipping all wx widget construction."""
    dialog = object.__new__(CorrectionManagerDialog)
    dialog.logger = MagicMock()
    dialog.library = RecordingLibrary()
    dialog.parent = MagicMock()
    dialog.parent.library = dialog.library
    dialog.regex = FakeField(key)
    dialog.rotation = FakeField("90")
    dialog.offset_x = FakeField("0.00")
    dialog.offset_y = FakeField("0.00")
    dialog.lcsc_mode = FakeField(lcsc_mode)
    dialog.corrections_list = FakeList(rows)
    dialog.selection_regex = selection[0] if selection else None
    dialog.selection_kind = selection[1] if selection else None
    dialog.populate_corrections_list = MagicMock()
    return dialog


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


class TestSaveRouting:
    """save_correction writes to the table matching the form's kind."""

    def test_footprint_rule_goes_to_the_regex_table(self):
        """With the box unticked the rule is a footprint pattern."""
        dialog = make_dialog(key="^SOT-23")
        dialog.save_correction()
        assert dialog.library.calls == [
            ("insert", KIND_FOOTPRINT, "^SOT-23", 90, (0.0, 0.0))
        ]

    def test_lcsc_rule_goes_to_the_lcsc_table(self):
        """With the box ticked the rule is a part number."""
        dialog = make_dialog(key="C12345", lcsc_mode=True)
        dialog.save_correction()
        assert dialog.library.calls == [("insert", KIND_LCSC, "C12345", 90, (0.0, 0.0))]

    def test_unchanged_selection_updates_in_place(self):
        """Re-saving the selected rule updates it rather than moving it."""
        dialog = make_dialog(
            key="C12345", lcsc_mode=True, selection=("C12345", KIND_LCSC)
        )
        dialog.save_correction()
        assert dialog.library.calls == [("update", KIND_LCSC, "C12345", 90, (0.0, 0.0))]


class TestLcscValidation:
    """An LCSC rule must be a part number, since it is matched exactly."""

    @pytest.mark.parametrize("key", ["^C12345$", "C12345|C999", "SOT-23", "", "C"])
    def test_patterns_are_rejected(self, key):
        """A value that is not a bare part number is refused, and nothing written."""
        dialog = make_dialog(key=key, lcsc_mode=True)
        dialog.save_correction()
        assert dialog.library.calls == []

    def test_a_part_number_is_accepted(self):
        """A bare part number saves normally."""
        dialog = make_dialog(key="C12345", lcsc_mode=True)
        dialog.save_correction()
        assert dialog.library.calls[0][2] == "C12345"

    def test_surrounding_whitespace_is_trimmed(self):
        """A pasted part number with stray spaces is stored trimmed."""
        dialog = make_dialog(key="  C12345 ", lcsc_mode=True)
        dialog.save_correction()
        assert dialog.library.calls[0][2] == "C12345"

    def test_footprint_rules_are_not_validated(self):
        """Patterns stay legal for footprint rules."""
        dialog = make_dialog(key="^SOT-23")
        dialog.save_correction()
        assert dialog.library.calls[0][1] == KIND_FOOTPRINT


class TestKindChangeIsNotARename:
    """Switching kind creates a rule; it must not move the selected one."""

    def test_switching_kind_leaves_the_selected_rule_alone(self):
        """Ticking the box and entering a part number keeps the pattern rule.

        Editing the key moves a rule, which is right for a text edit. A kind
        change is not an edit of the same rule, and one checkbox click must
        not silently delete an unrelated correction.
        """
        dialog = make_dialog(
            key="C12345",
            lcsc_mode=True,
            rows=[["^SOT-23", "180", "0.00", "0.00", KIND_FOOTPRINT]],
            selection=("^SOT-23", KIND_FOOTPRINT),
        )
        dialog.save_correction()
        assert ("delete", KIND_FOOTPRINT, "^SOT-23") not in dialog.library.calls
        assert ("insert", KIND_LCSC, "C12345", 90, (0.0, 0.0)) in dialog.library.calls

    def test_editing_the_key_within_one_kind_still_moves_the_rule(self):
        """Renaming a pattern deletes the old rule, as it always has."""
        dialog = make_dialog(
            key="^SOT-223",
            rows=[["^SOT-23", "180", "0.00", "0.00", KIND_FOOTPRINT]],
            selection=("^SOT-23", KIND_FOOTPRINT),
        )
        dialog.save_correction()
        assert ("delete", KIND_FOOTPRINT, "^SOT-23") in dialog.library.calls
        assert ("insert", KIND_FOOTPRINT, "^SOT-223", 90, (0.0, 0.0)) in (
            dialog.library.calls
        )


class TestKindAwareDuplicateScan:
    """Rules of different kinds are independent even when they read alike."""

    def test_a_matching_key_of_the_other_kind_is_not_a_duplicate(self):
        """A footprint rule spelled C12345 does not block an LCSC rule."""
        dialog = make_dialog(
            key="C12345",
            lcsc_mode=True,
            rows=[["C12345", "180", "0.00", "0.00", KIND_FOOTPRINT]],
        )
        dialog.save_correction()
        assert dialog.library.calls == [("insert", KIND_LCSC, "C12345", 90, (0.0, 0.0))]


class TestDeleteRouting:
    """delete_correction removes the rule from the table its row came from."""

    def _delete_row(self, dialog, row):
        """Delete the given row, standing in for the list's selection."""
        dialog.corrections_list.GetSelection = MagicMock(return_value=object())
        dialog.corrections_list.ItemToRow = MagicMock(return_value=row)
        dialog.delete_correction()

    def test_deleting_an_lcsc_row_hits_the_lcsc_table(self):
        """An LCSC row routes to the LCSC delete."""
        dialog = make_dialog(rows=[["C12345", "90", "0.00", "0.00", KIND_LCSC]])
        self._delete_row(dialog, 0)
        assert dialog.library.calls == [("delete", KIND_LCSC, "C12345")]

    def test_deleting_a_footprint_row_hits_the_regex_table(self):
        """A footprint row routes to the footprint delete."""
        dialog = make_dialog(rows=[["^SOT-23", "180", "0.00", "0.00", KIND_FOOTPRINT]])
        self._delete_row(dialog, 0)
        assert dialog.library.calls == [("delete", KIND_FOOTPRINT, "^SOT-23")]
