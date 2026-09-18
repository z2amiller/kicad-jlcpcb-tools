"""Native JLC column checks; enable with KICAD_NATIVE_WX_TESTS=1 on a desktop (spec 16.7)."""

from __future__ import annotations

from collections.abc import Iterator
import os
from types import SimpleNamespace

import pytest

from jlcfootprint.presentation import GLYPHS, SORT_ORDER

from .wx_harness import load_siblings

pytestmark = pytest.mark.skipif(
    os.environ.get("KICAD_NATIVE_WX_TESTS") != "1",
    reason="Native wx tests require KICAD_NATIVE_WX_TESTS=1 and a desktop session",
)


@pytest.fixture
def native_list() -> Iterator[SimpleNamespace]:
    """Build a real DataViewCtrl over the real part list model, without KiCad."""
    wx = pytest.importorskip("wx")
    dv = pytest.importorskip("wx.dataview")
    app = wx.App.Get() or wx.App(False)
    with load_siblings("_jlc_column_native", ("datamodel", "helpers"), {}) as modules:
        frame = wx.Frame(None)
        control = dv.DataViewCtrl(frame, style=dv.DV_ROW_LINES | dv.DV_MULTIPLE)
        model = modules["datamodel"].PartListDataModel(1.0)
        control.AssociateModel(model)
        columns = model.columns
        width = frame.FromDIP(wx.Size(36, -1)).GetWidth()
        rotation = control.AppendTextColumn(
            "Rotation", columns["ROT_COL"], width=120, mode=dv.DATAVIEW_CELL_INERT
        )
        jlc = control.AppendTextColumn(
            "JLC",
            columns["JLC_COL"],
            width=width,
            mode=dv.DATAVIEW_CELL_INERT,
            align=wx.ALIGN_CENTER,
            flags=0,
        )
        jlc.SetSortable(True)
        row = ["R1", "10k", "R_0603", "C1", "Basic", "100", *(["0"] * 8)]
        model.AddEntry(row)
        frame.Show()
        try:
            yield SimpleNamespace(
                wx=wx,
                dv=dv,
                app=app,
                frame=frame,
                control=control,
                model=model,
                jlc=jlc,
                rotation=rotation,
                width=width,
            )
        finally:
            frame.Destroy()
            app.Yield()


def test_the_native_column_keeps_its_width_and_renders_every_glyph(native_list):
    """Std's 36 DIP survives the native layout and each state draws its own glyph."""
    native = native_list
    native.app.Yield()
    assert native.jlc.GetWidth() == native.width
    assert native.jlc.GetModelColumn() == native.model.columns["JLC_COL"]
    assert native.jlc.IsSortable()
    item = native.model.ObjectToItem(native.model.data[0])
    column = native.model.columns["JLC_COL"]
    for state in SORT_ORDER:
        native.model.set_jlc_state("R1", state)
        native.app.Yield()
        assert native.model.GetValue(item, column) == GLYPHS[state]
        assert native.model.HasValue(item, column) is bool(GLYPHS[state])
        attr = native.dv.DataViewItemAttr()
        styled = native.model.GetAttr(item, column, attr)
        assert styled is bool(state)
        if styled:
            assert attr.GetBold() is True
            assert attr.HasColour() is True
