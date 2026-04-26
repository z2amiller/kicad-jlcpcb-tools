"""Shared pytest fixtures for test suite."""

from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture(scope="session")
def wx_app():
    """Provide a session-wide wx App for UI smoke tests, or skip if unavailable."""
    wx = pytest.importorskip("wx")

    if sys.platform.startswith("linux") and not (
        os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")
    ):
        pytest.skip("No GUI display available for wx UI tests")

    app = wx.GetApp() or wx.App(False)
    if app is None:
        pytest.skip("Unable to initialize wx App")

    yield app
