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


@pytest.fixture
def native_detail() -> Iterator[SimpleNamespace]:
    """Build one part's detail from the recorded fixtures and the KiCad library."""
    from jlcfootprint.controller import PartDetail
    from jlcfootprint.drawing import drawing_marks
    from jlcfootprint.geometry import easyeda_pads_to_mm
    from jlcfootprint.resolver import resolve
    from jlcfootprint.verdicts import StoredVerdict

    from .jlcfootprint_support import library_footprint, pro_record

    record = pro_record("C7175")
    pads, courtyard = library_footprint(
        "Capacitor_Tantalum_SMD", "CP_EIA-3216-18_Kemet-A"
    )
    marks = drawing_marks(
        record.symbol_shapes, record.footprint_shapes, record.footprint_origin
    )
    verdict = resolve(
        pads,
        "Capacitor_Tantalum_SMD:CP_EIA-3216-18_Kemet-A",
        record.status,
        record.package_name,
        easyeda_pads_to_mm(record.pads),
        record.symbol_pins,
        marks=marks,
        kicad_courtyard=courtyard,
    )
    yield SimpleNamespace(
        detail=PartDetail(
            "C5",
            "C7175",
            kicad_footprint="Capacitor_Tantalum_SMD:CP_EIA-3216-18_Kemet-A",
            kicad_pads=pads,
            courtyard=courtyard,
            package_name=record.package_name,
            puuid=record.puuid,
            jlc_pads=easyeda_pads_to_mm(record.pads),
            symbol_pins=record.symbol_pins,
            marks=marks,
            source="live",
            fetched_at=1_757_000_000,
            verdict=verdict,
            stored=StoredVerdict(
                "C7175",
                "hash",
                status=verdict.status,
                fit=verdict.fit,
                rotation=verdict.rotation,
                method=verdict.method,
                confidence=verdict.confidence,
            ),
        )
    )


def test_the_native_dialog_is_shown_once_and_paints_its_canvas(native_detail):
    """Spec 16.7's native check: the real dialog opens, lays out and draws (once)."""
    wx = pytest.importorskip("wx")
    app = wx.App.Get() or wx.App(False)
    with load_siblings("_jlc_dialog_native", ("jlc_footprint_detail",), {}) as modules:
        module = modules["jlc_footprint_detail"]
        frame = wx.Frame(None)
        frame.SetPosition(wx.Point(-20000, -20000))
        dialog = module.JlcFootprintDetailDialog(frame, native_detail.detail)
        try:
            dialog.Show()
            app.Yield()
            assert dialog.GetSize().GetWidth() >= 960
            assert dialog.canvas.GetSize().GetWidth() >= 420
            bitmap = wx.Bitmap(*dialog.canvas.GetClientSize())
            dc = wx.MemoryDC(bitmap)
            drawn = module.draw_primitives(dc, dialog.canvas.build_overlay().primitives)
            dc.SelectObject(wx.NullBitmap)
            assert drawn > 10
            assert dialog.banner.GetLabel().startswith("Fits;")
        finally:
            dialog.Destroy()
            frame.Destroy()
            app.Yield()
