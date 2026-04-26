"""Minimal wx UI smoke tests for main dialog construction."""

# ruff: noqa: D103, I001

import importlib
import json
from pathlib import Path
import sys
import types

import pytest


pytestmark = pytest.mark.ui_smoke

ROOT = Path(__file__).resolve().parent.parent


class _FakeBoard:
    def GetFileName(self):
        return str(ROOT / "tests" / "fixtures" / "k9_smoke_ok" / "fx-Full125B.kicad_pcb")


class _FakePcbnew:
    def GetBoard(self):
        return _FakeBoard()


class _FakeProvider:
    def get_pcbnew(self):
        return _FakePcbnew()


class _FakeToolbarButton:
    def __init__(self):
        self.labels = []
        self.bitmaps = []

    def SetNormalBitmap(self, bitmap):
        self.bitmaps.append(bitmap)

    def SetLabel(self, label):
        self.labels.append(label)


class _FakeToggleEvent:
    def __init__(self, checked):
        self.checked = checked

    def IsChecked(self):
        return self.checked


class _FakeFootprintList:
    def __init__(self, selected_count):
        self.selected_count = selected_count

    def GetSelectedItemsCount(self):
        return self.selected_count


def _load_mainwindow_module():
    """Import mainwindow.py under a synthetic package for relative imports."""
    pkg = types.ModuleType("kicadplugin")
    pkg.__path__ = [str(ROOT)]
    sys.modules["kicadplugin"] = pkg
    return importlib.import_module("kicadplugin.mainwindow")


def test_wx_app_fixture_bootstraps(wx_app):
    wx = pytest.importorskip("wx")
    assert wx.GetApp() is wx_app


def test_main_dialog_constructs_with_fake_provider(wx_app, monkeypatch):
    _ = pytest.importorskip("pcbnew")
    mainwindow = _load_mainwindow_module()

    # Keep this as a UI construction smoke test only.
    monkeypatch.setattr(mainwindow.JLCPCBTools, "init_data", lambda self: None)

    dialog = mainwindow.JLCPCBTools(None, kicad_provider=_FakeProvider())
    try:
        assert dialog.footprint_list is not None
        assert dialog.partlist_data_model is not None
        assert dialog.footprint_list.GetColumnCount() >= 12
    finally:
        dialog.Destroy()


def test_main_dialog_materializes_local_settings_from_default(
    wx_app, monkeypatch, tmp_path
):
    _ = pytest.importorskip("pcbnew")
    mainwindow = _load_mainwindow_module()

    settings_path = tmp_path / "settings.json"
    default_path = tmp_path / "settings.default.json"
    expected = {
        "gerber": {"force_drc": True, "fill_zones": True},
        "general": {"select_alike_auto": False},
    }
    default_path.write_text(json.dumps(expected), encoding="utf-8")

    monkeypatch.setattr(mainwindow, "SETTINGS_PATH", str(settings_path))
    monkeypatch.setattr(mainwindow, "SETTINGS_DEFAULT_PATH", str(default_path))
    monkeypatch.setattr(mainwindow.JLCPCBTools, "init_data", lambda self: None)

    dialog = mainwindow.JLCPCBTools(None, kicad_provider=_FakeProvider())
    try:
        assert dialog.settings == expected
        assert settings_path.exists()
        assert json.loads(settings_path.read_text(encoding="utf-8")) == expected
    finally:
        dialog.Destroy()


def test_hide_bom_toggle_updates_label_and_refreshes(wx_app, monkeypatch):
    _ = pytest.importorskip("pcbnew")
    mainwindow = _load_mainwindow_module()

    dialog = mainwindow.JLCPCBTools.__new__(mainwindow.JLCPCBTools)
    dialog.hide_bom_parts = False
    dialog.scale_factor = 1.0
    dialog.hide_bom_button = _FakeToolbarButton()

    refresh_calls = []
    dialog.populate_footprint_list = lambda: refresh_calls.append("refresh")
    monkeypatch.setattr(mainwindow, "loadBitmapScaled", lambda name, scale: (name, scale))

    dialog.OnBomHide()
    assert dialog.hide_bom_parts is True
    assert dialog.hide_bom_button.labels[-1] == "Show excluded BOM"

    dialog.OnBomHide()
    assert dialog.hide_bom_parts is False
    assert dialog.hide_bom_button.labels[-1] == "Hide excluded BOM"
    assert refresh_calls == ["refresh", "refresh"]


def test_toggle_select_alike_persists_setting_and_triggers_selection(wx_app):
    _ = pytest.importorskip("pcbnew")
    mainwindow = _load_mainwindow_module()

    dialog = mainwindow.JLCPCBTools.__new__(mainwindow.JLCPCBTools)
    dialog.auto_select_alike = False
    dialog.settings = {"general": {}}
    dialog.footprint_list = _FakeFootprintList(selected_count=1)

    saved = []
    selected = []
    dialog.save_settings = lambda: saved.append(True)
    dialog.select_alike_parts = lambda: selected.append(True)

    dialog.toggle_select_alike(_FakeToggleEvent(True))

    assert dialog.auto_select_alike is True
    assert dialog.settings["general"]["select_alike_auto"] is True
    assert saved == [True]
    assert selected == [True]
