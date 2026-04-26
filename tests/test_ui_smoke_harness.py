"""Minimal wx UI smoke tests for main dialog construction."""

# ruff: noqa: D103, I001

import importlib
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
