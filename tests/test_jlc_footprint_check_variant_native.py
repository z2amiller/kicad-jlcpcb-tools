"""The JLC column in upstream's native variant matrix (spec 18.5), headless in CI's lane.

The last test follows a JLC cell into the detail dialog and its override prompt,
which names the references and the variants an override reaches (spec 18.2).

The matrix is a real ``wx.grid``: these tests mount it (or the whole plugin window)
under Xvfb and check what wx actually lays out, paints and shows on hover.  The
window test runs the plugin's own check, through its variant read, against a
cache that already holds the board's part numbers (``checked_window``).
"""

from __future__ import annotations

import sys
from typing import Any

import pytest

from jlcfootprint.presentation import GLYPHS, MatrixCell

from .jlc_footprint_variant_native_support import checked_window
from .native_window_support import choose_output, focus, modal_handler, window_ui
from .native_wx_support import pump
from .variant_matrix_native_test_support import (
    MatrixHarness,
    click_native_cell,
    matrix,
    modules,
    native_marks,
)
from .variant_matrix_render_test_support import paint_header
from .variant_model_test_support import Snapshot, State

__all__ = ["matrix", "modules", "window_ui"]
pytestmark = native_marks

STOPS = {
    "red": ((255, 128, 128), (176, 0, 0)),
    "green": ((96, 200, 120), (0, 112, 48)),
    "override": ((124, 176, 255), (0, 82, 204)),
    "pending": ((176, 176, 176), (102, 102, 102)),
}


def _cell(state: str) -> MatrixCell:
    """Return a check cell in one state, with a recognisable hover."""
    return MatrixCell("C1", state, f"verdict text for {state}", "180°", 180, "derived")


# A long footprint name gives the Footprint column room to give way.
FOOTPRINT = "Resistor_SMD:R_0603_1608Metric_Pad0.98x0.95mm_HandSolder"


def _model(model_module: Any, states: tuple[str, ...] = ("green", "red", "")) -> Any:
    """Return a two-variant model whose components carry the given check states."""
    snapshot = Snapshot(
        tuple(
            State(f"component-{row}", f"R{row}", name, footprint=FOOTPRINT)
            for row in range(1, len(states) + 1)
            for name in ("", "A")
        ),
        ("", "A"),
    )
    return model_module.MatrixModel(
        snapshot,
        corrections={
            f"component-{row}": model_module.CorrectionState()
            for row in range(1, len(states) + 1)
        },
        correction_variant="A",
        jlc={
            f"component-{row}": _cell(state)
            for row, state in enumerate(states, start=1)
        },
    )


def _jlc_width(view: Any) -> int:
    """Return the width the JLC column takes: Std's 36 DIP, or its header's minimum."""
    return max(view._minimum_widths["jlc"], view.FromDIP(36))


def test_the_jlc_column_is_frozen_at_the_ordinary_list_s_std_width(
    matrix: Any, modules: Any
) -> None:
    """Six frozen shared columns; JLC is 36 DIP and keeps it while Footprint gives way."""
    view_module, model_module = modules

    def check(h: MatrixHarness) -> None:
        view, wx = h.view, h.wx
        jlc = view.model.column_for(None, "jlc")
        assert jlc == 5
        assert view.GetNumberFrozenCols() == 6
        assert view.GetColSize(jlc) == _jlc_width(view)
        footprint = view.model.column_for(None, "footprint")
        wide = view.GetColSize(footprint)
        slack = wide - view.GetColMinimalWidth(footprint)
        assert slack > 40
        frozen = sum(view.GetColSize(col) for col in range(6))
        h.frame.SetClientSize((frozen - slack // 2 + view._scrolling_reserve, 500))
        pump(wx)
        view._resize_viewport()
        pump(wx)
        assert view.GetNumberFrozenCols() == 6
        assert view.GetColSize(footprint) < wide
        assert view.GetColSize(jlc) == _jlc_width(view)

    matrix(check, _model(modules[1]))


@pytest.mark.parametrize("dark", [False, True], ids=["light", "dark"])
@pytest.mark.parametrize("state", sorted(STOPS))
def test_the_jlc_glyph_is_drawn_bold_in_its_state_s_colour(
    matrix: Any, modules: Any, state: str, dark: bool
) -> None:
    """The ordinary list's two colour stops, picked by the cell's background (spec 16.3)."""
    view_module, model_module = modules

    def check(h: MatrixHarness) -> None:
        view, wx = h.view, h.wx
        view.SetDefaultCellBackgroundColour(
            wx.Colour(*((0, 0, 0) if dark else (255, 255, 255)))
        )
        view.SetDefaultCellTextColour(
            wx.Colour(*((240, 240, 240) if dark else (0, 0, 0)))
        )
        col = view.model.column_for(None, "jlc")
        drawn = []
        bitmap = wx.Bitmap(80, 40)
        dc = wx.MemoryDC(bitmap)
        draw_text = dc.DrawText

        def record(text: str, x: int, y: int) -> None:
            drawn.append(
                (text, tuple(dc.GetTextForeground())[:3], dc.GetFont().GetWeight())
            )
            draw_text(text, x, y)

        dc.DrawText = record
        attr = view_module.gridlib.GridCellAttr()
        attr.SetFont(view.GetCellFont(0, col))
        try:
            view_module.MatrixCellRenderer().Draw(
                view, attr, dc, wx.Rect(4, 4, 36, 20), 0, col, False
            )
        finally:
            dc.SelectObject(wx.NullBitmap)
        stops = STOPS[state]
        assert drawn == [
            (GLYPHS[state], stops[0] if dark else stops[1], wx.FONTWEIGHT_BOLD)
        ]

    matrix(check, _model(modules[1], (state,)))


def test_a_blank_jlc_cell_draws_nothing_in_the_default_weight(
    matrix: Any, modules: Any
) -> None:
    """No state (no LCSC, or the check off): no glyph, no bold, no colour."""
    view_module, model_module = modules

    def check(h: MatrixHarness) -> None:
        view, wx = h.view, h.wx
        col = view.model.column_for(None, "jlc")
        weights = []
        bitmap = wx.Bitmap(80, 40)
        dc = wx.MemoryDC(bitmap)
        draw_text = dc.DrawText

        def record(text: str, x: int, y: int) -> None:
            weights.append((text, dc.GetFont().GetWeight()))
            draw_text(text, x, y)

        dc.DrawText = record
        attr = view_module.gridlib.GridCellAttr()
        attr.SetFont(view.GetCellFont(0, col))
        try:
            view_module.MatrixCellRenderer().Draw(
                view, attr, dc, wx.Rect(4, 4, 36, 20), 0, col, False
            )
        finally:
            dc.SelectObject(wx.NullBitmap)
        assert weights == [("", attr.GetFont().GetWeight())]

    matrix(check, _model(modules[1], ("",)))


def test_hovering_a_jlc_cell_shows_the_check_s_verdict(
    matrix: Any, modules: Any
) -> None:
    """The native tooltip is the model's cell details: the output variant and the help."""

    def check(h: MatrixHarness) -> None:
        col = h.view.model.column_for(None, "jlc")
        row = h.view.model.row_for_component("component-2")
        assert h.hover(row, col) == "R2 · Output: A · JLC\nverdict text for red"

    matrix(check, _model(modules[1]))


def test_the_jlc_header_is_painted_in_the_frozen_block(
    matrix: Any, modules: Any
) -> None:
    """The header reads "JLC", turned like its compact neighbours, inside column 5."""

    def check(h: MatrixHarness) -> None:
        view = h.view
        header = paint_header(view, h.wx, [5])
        (label,) = [text for text in header.text if text.text == "JLC"]
        assert label.rotation == 90
        left, right = view.GetColLeft(5), view.GetColRight(5)
        assert left <= label.bounds[0] and label.bounds[0] + label.bounds[2] <= right

    matrix(check, _model(modules[1]))


def test_a_variant_window_judges_its_output_variant_in_the_jlc_and_corr_columns(
    window_ui: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Open, switch the output, edit, change the board: the cells follow (spec 18.4, 18.5)."""
    checks = checked_window(window_ui, monkeypatch)

    def check(ui: Any) -> None:
        view = ui.controller.view

        def cells() -> tuple[str, str]:
            model = view.model
            row = model.row_for_component("component-1")
            return (
                model.get_display(row, model.column_for(None, "jlc")),
                model.get_display(row, model.column_for(None, "correction")),
            )

        assert ui.dialog.jlc_footprint_check is checks[0]
        assert view.model.jlc_available
        assert cells() == ("✓", "180°")
        model = view.model
        row = model.row_for_component("component-1")
        assert "Fits; rotation 180°" in model.cell_details(
            row, model.column_for(None, "jlc")
        )
        choose_output(ui, "B")
        assert checks[0].parts["R1"].lcsc == "C2"
        assert cells() == ("✗", "raw")
        target = focus(ui, "B", "lcsc")
        ui.controller._on_edit(target, "C1")
        assert cells() == ("✓", "180°")
        ui.board.parts[0].GetVariant("B").SetFieldValue("LCSC", "C2")
        ui.controller._on_timer(None)
        assert cells() == ("✗", "raw")
        assert ui.messages == []

    window_ui.run(check)


@pytest.mark.parametrize("editable", [True, False], ids=["editable", "held"])
def test_double_click_or_enter_on_a_jlc_cell_asks_for_its_details(
    matrix: Any, modules: Any, editable: bool
) -> None:
    """Both open the detail dialog, even while upstream holds edits (spec 18.5)."""

    def check(h: MatrixHarness) -> None:
        view, wx = h.view, h.wx
        col = view.model.column_for(None, "jlc")
        view.set_mutations_enabled(editable)
        click_native_cell(view, wx, 1, col, double=True)
        h.key(wx.WXK_RETURN)
        details = [
            (name, target.field, target.component_id)
            for name, target in h.actions
            if name == "jlc_details"
        ]
        component = view.model.rows[1].component_id
        assert details == [("jlc_details", "jlc", component)] * 2
        assert h.activations == []

    matrix(check, _model(modules[1]))


def _jlc_submenu(
    h: MatrixHarness, row: int, col: int, choose: str = ""
) -> dict[str, Any]:
    """Right-click a cell, return the JLC submenu's enablement by label, choose one.

    The menu lives only while it is shown, so the chosen entry is sent its command
    from inside the popup, as a user's click would be.
    """
    view, wx = h.view, h.wx
    items: dict[str, Any] = {"separators": 0}

    def popup(menu: Any) -> None:
        (entry,) = [
            item
            for item in menu.GetMenuItems()
            if item.GetItemLabelText() == "JLC footprint"
        ]
        submenu = entry.GetSubMenu()
        for item in submenu.GetMenuItems():
            if item.IsSeparator():
                items["separators"] += 1
                continue
            items[item.GetItemLabelText()] = item.IsEnabled()
            if item.GetItemLabelText() == choose:
                submenu.ProcessEvent(wx.CommandEvent(wx.wxEVT_MENU, item.GetId()))

    view.PopupMenu = popup
    click_native_cell(view, wx, row, col, right=True)
    return items


@pytest.mark.parametrize(
    "state,checked,enabled",
    [
        ("green", True, (True, True, True, True, True)),
        ("", True, (False, False, True, True, True)),
        ("green", False, (False,) * 5),
    ],
    ids=["numbered_row", "row_without_number", "check_off"],
)
def test_the_cell_menu_carries_the_jlc_submenu_enabled_as_on_the_list(
    matrix: Any, modules: Any, state: str, checked: bool, enabled: tuple
) -> None:
    """Five entries and a separator; Details and Re-fetch need a numbered row."""
    model_module = modules[1]
    model = _model(model_module, (state,))
    if not checked:
        model = model_module.MatrixModel(
            model.snapshot,
            corrections={"component-1": model_module.CorrectionState()},
            correction_variant="A",
        )
    elif not state:
        model._jlc["component-1"] = MatrixCell("", "", "", "raw", None, "")

    def check(h: MatrixHarness) -> None:
        view = h.view
        items = _jlc_submenu(
            h, 0, view.model.column_for("A", "lcsc"), choose="Re-check board"
        )
        labels = (
            "Details…",
            "Re-fetch data",
            "Re-check board",
            "Refresh board data",
            "Clear cache",
        )
        assert tuple(items[label] for label in labels) == enabled
        assert items["separators"] == 1
        if checked:
            name, target = h.actions[-1]
            assert (name, target.variant, target.field) == ("jlc_recheck", "A", "lcsc")

    matrix(check, model)


def test_a_variant_window_opens_the_detail_and_re_fetches_through_the_matrix(
    window_ui: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Double-click the JLC cell, then Re-fetch data: the check's own actions run."""
    checks = checked_window(window_ui, monkeypatch)

    def check(ui: Any) -> None:
        view, wx = ui.controller.view, ui.wx
        shown: list[str] = []
        presenter = ui.dialog.jlc_footprint_presenter
        monkeypatch.setattr(presenter, "show_jlc_footprint_detail", shown.append)
        col = view.model.column_for(None, "jlc")
        click_native_cell(view, wx, 0, col, double=True)
        assert shown == ["R1"]
        h = MatrixHarness(ui.dialog, wx, ui.view, ui.module, view=view)
        assert _jlc_submenu(h, 0, col, choose="Re-fetch data")["Re-fetch data"]
        pump(wx)
        assert checks[0].cache.status("C1") is None
        assert "C1" in checks[0].worker.pending_lookups()
        row = view.model.row_for_component("component-1")
        assert view.model.get_display(row, col) == "◷"
        assert ui.messages == []

    window_ui.run(check)


def test_the_override_prompt_names_the_references_and_the_variants(
    window_ui: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Double-click the JLC cell, then "Set override…": the prompt says what it reaches (spec 18.2)."""
    checked_window(window_ui, monkeypatch)

    def check(ui: Any) -> None:
        view, wx = ui.controller.view, ui.wx
        detail = sys.modules[
            ui.main.jlc_footprint_window.JlcFootprintDetailDialog.__module__
        ]
        said: list[str] = []

        def prompt(dialog: Any) -> None:
            # wx's Wrap breaks the label into lines; the words are what matter.
            said.append(
                " ".join(dialog.scope.GetLabel().split()) if dialog.scope else ""
            )

        def open_prompt(dialog: Any) -> None:
            with modal_handler(ui, detail.OverrideDialog, prompt):
                dialog.on_set_override(None)

        with modal_handler(ui, detail.JlcFootprintDetailDialog, open_prompt) as shown:
            click_native_cell(
                view, wx, 0, view.model.column_for(None, "jlc"), double=True
            )
        assert len(shown) == 1
        assert said == [
            "This override applies to R1, in every variant that orders C1 on this "
            "footprint."
        ]
        assert ui.messages == []

    window_ui.run(check)
