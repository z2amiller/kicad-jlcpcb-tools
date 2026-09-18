"""Tests for the JLC glyph column: the model's cells, its order, its style and the view column."""

from collections.abc import Iterator
import types
from typing import Any
from unittest.mock import MagicMock

import pytest

from jlcfootprint.presentation import SORT_ORDER

from . import test_window_layout as layout
from .stock_test_support import board_row, stock_modules


class CellAttr:
    """Keep the cell attributes a real renderer would read."""

    def __init__(self) -> None:
        self.background = None
        self.colour = None
        self.bold = False

    def SetBackgroundColour(self, colour: object) -> None:
        """Retain the background, which the JLC cell must never set."""
        self.background = colour

    def SetColour(self, colour: object) -> None:
        """Retain the foreground colour."""
        self.colour = colour

    def SetBold(self, bold: bool) -> None:
        """Retain the font weight."""
        self.bold = bold


@pytest.fixture
def models() -> Iterator[types.SimpleNamespace]:
    """Load the real data model and helpers under the shared wx doubles."""
    with stock_modules() as loaded:
        yield loaded


def _model(models: types.SimpleNamespace, *references: str) -> Any:
    """Return a part list model carrying one row per reference."""
    model = models.datamodel.PartListDataModel(1.0)
    for reference in references:
        model.AddEntry(board_row(reference, 100))
    return model


def test_the_cell_is_the_state_s_glyph_read_only_and_blank_without_a_state(models):
    """The column renders the glyph of the stored state and accepts no edit (spec 16.3)."""
    model = _model(models, "R1", "R2")
    column = model.columns["JLC_COL"]
    row = model.data[0]
    assert (column, model.GetColumnCount()) == (16, 17)
    assert model.GetColumnType(column) == "string"
    assert model.GetValue(row, column) == ""
    assert model.HasValue(row, column) is False
    model.set_jlc_state("R1", "green")
    assert model.GetValue(row, column) == "✓"
    assert model.HasValue(row, column) is True
    model.set_jlc_state("R1", "red")
    assert model.GetValue(row, column) == "✗"
    assert model.SetValue("✓", row, column) is False
    assert model.GetValue(row, column) == "✗"
    assert model.get_jlc_state(row) == "red"
    assert model.get_jlc_state(model.data[1]) == ""


def test_setting_a_state_repaints_the_row_only_when_it_changed(models):
    """A changed state notifies the row (never the cell: #834's Cocoa index caveat)."""
    model = _model(models, "R1")
    model.notifications.clear()
    model.set_jlc_state("R1", "pending")
    assert model.notifications == [("changed", (model.data[0],))]
    model.notifications.clear()
    model.set_jlc_state("R1", "pending")
    assert model.notifications == []
    model.set_jlc_state("R9", "green")  # a reference that is not on the board
    assert model.notifications == []
    assert model._jlc_states["R9"] == "green"


@pytest.mark.parametrize("luminance", [0.1, 0.9])
def test_each_state_has_a_bold_foreground_colour_for_this_theme(models, luminance):
    """Foreground and weight only, two stops per state, nothing for a blank cell."""
    models.wx.SystemSettings.GetColour = lambda _key: types.SimpleNamespace(
        GetLuminance=lambda: luminance
    )
    model = _model(models, "R1")
    row = model.data[0]
    column = model.columns["JLC_COL"]
    attr = CellAttr()
    assert model.GetAttr(row, column, attr) is False
    assert (attr.colour, attr.bold, attr.background) == (None, False, None)
    seen = {}
    for state in SORT_ORDER:
        if not state:
            continue
        model.set_jlc_state("R1", state)
        attr = CellAttr()
        assert model.GetAttr(row, column, attr) is True, state
        assert attr.bold is True and attr.background is None
        seen[state] = attr.colour
    assert seen["red"] != seen["green"] != seen["override"]
    assert seen["yellow"] == seen["caveat"]
    assert seen["pending"] == seen["paused"] == seen["unknown"]
    dark = luminance < 0.5
    for state, colour in seen.items():
        assert (sum(colour) > 380) is dark, (state, colour)


def test_the_column_sorts_worst_first_then_by_reference(models):
    """Spec 16.3's ascending order, ties by reference, reversed when descending."""
    states = {
        "R10": "green",
        "R2": "red",
        "R3": "unknown",
        "R4": "override",
        "R5": "pending",
        "R6": "yellow",
        "R7": "paused",
        "R8": "caveat",
        "R9": "",
        "R1": "red",
    }
    model = _model(models, *states)
    for reference, state in states.items():
        model.set_jlc_state(reference, state)
    column = model.columns["JLC_COL"]
    ascending = sorted(
        model.data,
        key=lambda row: _cmp_key(model, row, column, True),
    )
    assert [row[0] for row in ascending] == [
        "R1",
        "R2",
        "R6",
        "R8",
        "R3",
        "R7",
        "R5",
        "R10",
        "R4",
        "R9",
    ]
    descending = sorted(model.data, key=lambda row: _cmp_key(model, row, column, False))
    assert [row[0] for row in descending] == [row[0] for row in reversed(ascending)]
    # Other columns keep upstream's natural sort: R2 before R10, not after it.
    rows = {row[0]: row for row in model.data}
    reference_column = model.columns["REF_COL"]
    assert model.Compare(rows["R2"], rows["R10"], reference_column, True) < 0


class _Key:
    """Order rows through the model's own Compare, as the native view does."""

    def __init__(self, model, row, column, ascending):
        self.model, self.row = model, row
        self.column, self.ascending = column, ascending

    def __lt__(self, other) -> bool:
        """Return whether this row sorts before the other."""
        return self.model.Compare(self.row, other.row, self.column, self.ascending) < 0


def _cmp_key(model, row, column, ascending):
    """Return a sort key that defers to the model's Compare."""
    return _Key(model, row, column, ascending)


def test_a_new_lcsc_or_a_cleared_list_forgets_the_glyph(models):
    """The verdict belongs to the part number and the row: both changes drop it."""
    model = _model(models, "R1", "R2")
    model.set_jlc_state("R1", "green")
    model.set_jlc_state("R2", "red")
    model.set_lcsc("R1", "C999", "Basic", 100, "params")
    assert model.get_jlc_state(model.data[0]) == ""
    model.remove_lcsc_number(model.data[1])
    assert model.get_jlc_state(model.data[1]) == ""
    model.set_jlc_state("R1", "green")
    model.RemoveAll()
    assert model._jlc_states == {}


# ---------------------------------------------------------------------------
# The view column and the window's wiring
# ---------------------------------------------------------------------------


def test_the_view_column_sits_between_type_and_std_with_std_s_width(monkeypatch):
    """Header "JLC" between Type and Std, Std's 36 DIP, read-only, sortable, not a persisted width."""
    main = layout.mainwindow
    monkeypatch.setattr(main, "TypeCellTooltip", MagicMock())
    monkeypatch.setattr(main.wx, "ToolTip", str, raising=False)
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    window = layout._open_main(monkeypatch, {})
    control = window.footprint_list
    titles = [column.GetTitle() for column in control.GetColumns()]
    assert titles[titles.index("Type") + 1] == "JLC"
    assert titles[titles.index("JLC") + 1] == "Std"
    jlc = layout._column_by_title(control, "JLC")
    standard = layout._column_by_title(control, "Std")
    assert jlc.GetModelColumn() == main.PartListDataModel.columns["JLC_COL"]
    assert jlc.specified_width == standard.specified_width
    assert jlc.sortable is True
    assert "JLC_COL" not in main.FOOTPRINT_COLUMN_KEYS.values()
    assert window.jlc_column is jlc


def _enabled(settings: dict) -> bool:
    """Read the shipped setting the way the facade does (the harness stubs it off)."""
    return bool(settings.get("jlcfootprint", {}).get("enabled", True))


def _window(module, check):
    """Return a window with the real model and a footprint check double."""
    window = object.__new__(module.JLCPCBTools)
    window.settings = {"jlcfootprint": {"enabled": True}}
    window.jlc_footprint_check = check
    window.logger = MagicMock()
    return window


def test_the_window_fills_and_clears_the_glyph_through_the_check(models, monkeypatch):
    """A result repaints both cells of every reference; the setting off clears the glyph."""
    main = layout.mainwindow
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    model = _model(models, "R1", "R2")
    check = types.SimpleNamespace(
        generation=4,
        references_for=lambda lcsc: ["R1", "R2"],
        display_text=lambda reference: "180°",
        glyph_state=lambda reference: "caveat",
    )
    window = _window(main, check)
    window.partlist_data_model = model
    main.JLCPCBTools.on_jlc_footprint_result(
        window, types.SimpleNamespace(lcsc="C1", generation=4)
    )
    column = model.columns["JLC_COL"]
    assert [model.GetValue(row, column) for row in model.data] == ["!", "!"]
    assert [row[model.columns["ROT_COL"]] for row in model.data] == ["180°", "180°"]
    window.settings = {"jlcfootprint": {"enabled": False}}
    main.JLCPCBTools._apply_jlc_cell(window, "R1")
    assert model.GetValue(model.data[0], column) == ""
    assert model.get_jlc_state(model.data[1]) == "caveat"
