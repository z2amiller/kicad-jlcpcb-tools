"""The JLC column in upstream's native variant matrix (spec 18.5), headless in CI's lane.

The matrix is a real ``wx.grid``: these tests mount it (or the whole plugin window)
under Xvfb and check what wx actually lays out, paints and shows on hover.  The
window test runs the plugin's own check, through its variant read, against a
cache that already holds the board's part numbers (``checked_window``).
"""

from __future__ import annotations

from typing import Any

import pytest

from jlcfootprint.presentation import GLYPHS, MatrixCell

from .jlc_footprint_variant_native_support import checked_window
from .native_window_support import choose_output, focus, window_ui
from .native_wx_support import pump
from .variant_matrix_native_test_support import (
    MatrixHarness,
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
