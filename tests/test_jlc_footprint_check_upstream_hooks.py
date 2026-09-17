"""The JLC footprint check where it meets upstream's window.

Upstream's main window gained a variant matrix, a board-context guard and its own
refresh of the Correction cell (the check's Rotation cell); these tests pin how the
check sits beside each of them.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .wx_harness import load_mainwindow, wx_stubs

_PACKAGE = "jlc_footprint_check_upstream_hooks_tests"


def _enabled(settings):
    """Read the shipped setting the way the facade does (the harness stubs it off)."""
    return bool(settings.get("jlcfootprint", {}).get("enabled", True))


@pytest.fixture
def mainwindow():
    """Load mainwindow with a check factory that records every check it builds."""
    module = load_mainwindow(
        _PACKAGE,
        wx=wx_stubs(
            Frame=type("Frame", (), {}), NewIdRef=MagicMock(side_effect=object)
        ),
    )
    checks = []

    def create(_window, _pcbnew):
        checks.append(MagicMock())
        return checks[-1]

    module.create_footprint_check = create
    module.is_footprint_check_enabled = _enabled
    return module, checks


def _window(module, check=None, *, enabled=True):
    """Return a window carrying only what the check's hooks read."""
    window = object.__new__(module.JLCPCBTools)
    window.settings = {"jlcfootprint": {"enabled": enabled}}
    window.store = SimpleNamespace(dbfile="project.db")
    window.pcbnew = SimpleNamespace()
    window.logger = MagicMock()
    window.jlc_footprint_check = check
    return window


def test_a_board_with_variants_gets_no_check(mainwindow):
    """Upstream edits a board with KiCad variants in its matrix, never in this list."""
    module, checks = mainwindow
    window = _window(module)
    window._variant_controller = object()
    window._start_jlc_footprint_check()
    assert checks == []
    assert window.jlc_footprint_check is None
    del window._variant_controller
    window._start_jlc_footprint_check()
    assert window.jlc_footprint_check is checks[0]
