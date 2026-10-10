"""The matrix's "JLC footprint" actions and its detail dialog (spec 18.5).

The view dispatches each submenu entry, and a double-click or Enter on a JLC cell,
through upstream's ``action_<name>`` convention; the controller's five actions are
thin delegators that name the target's row and the selection; the presenter keeps
the parts whose output variant carries a number and calls the same facade
functions the ordinary list does.  Controller and presenter run for real here
under a fake wx; the native tests drive the view.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .jlc_footprint_wx_support import check_window, facade_symbols, load_window
from .test_fabrication_jlc_generation_paths import _load_variant_controller
from .variant_native_support import Snapshot, State

_PACKAGE = "jlc_footprint_check_variant_menu_tests"


@pytest.fixture
def controller_module():
    """Load the real ``variant/controller.py`` under a fake wx."""
    with _load_variant_controller(f"{_PACKAGE}_controller") as loaded:
        yield loaded["variant.controller"]


def _controller(controller_module, selected=("id-R2", "id-R1")):
    """Return a controller whose view selects ``selected`` and whose presenter records."""
    snapshot = Snapshot(
        tuple(
            State(f"id-{reference}", reference, name)
            for reference in ("R1", "R2", "R3")
            for name in ("", "A")
        ),
        ("", "A"),
    )
    controller = object.__new__(controller_module.VariantMainController)
    controller.closed = False
    controller.session = SimpleNamespace(snapshot=snapshot)
    controller.view = SimpleNamespace(
        selected_physical_component_ids=lambda: tuple(selected)
    )
    controller.dialog = SimpleNamespace(jlc_footprint_presenter=MagicMock())
    controller._error = MagicMock()
    return controller, controller.dialog.jlc_footprint_presenter


def _target(component_id):
    """Return a captured cell target on a component's JLC cell."""
    return SimpleNamespace(component_id=component_id, variant=None, field="jlc")


def test_details_name_the_target_s_row_first_then_the_selection(controller_module):
    """Double-click names its own row; the selection follows, each reference once."""
    controller, presenter = _controller(controller_module)
    controller.dispatch_action("jlc_details", _target("id-R1"))
    presenter.show_variant_jlc_detail.assert_called_once_with(["R1", "R2"])


def test_re_fetch_names_the_selected_rows(controller_module):
    """Re-fetch data acts on the selection (the right-clicked row is in it)."""
    controller, presenter = _controller(controller_module, selected=("id-R3",))
    controller.dispatch_action("jlc_refetch", _target("id-R3"))
    presenter.refetch_variant_references.assert_called_once_with(["R3"])


@pytest.mark.parametrize(
    "action,method",
    [
        ("jlc_recheck", "on_jlc_footprint_recheck"),
        ("jlc_refresh", "on_jlc_footprint_refresh"),
        ("jlc_clear_cache", "on_jlc_footprint_clear_cache"),
    ],
)
def test_the_board_wide_actions_are_the_ordinary_list_s(
    controller_module, action, method
):
    """Re-check, Refresh and Clear cache call the presenter's own handlers."""
    controller, presenter = _controller(controller_module)
    controller.dispatch_action(action, _target("id-R1"))
    getattr(presenter, method).assert_called_once_with()
    controller._error.assert_not_called()


def test_the_actions_are_no_ops_for_a_window_without_a_presenter(controller_module):
    """Upstream's controller under another dialog: nothing to call, nothing raised."""
    controller, _presenter = _controller(controller_module)
    del controller.dialog.jlc_footprint_presenter
    for action in ("jlc_details", "jlc_refetch", "jlc_recheck", "jlc_refresh"):
        controller.dispatch_action(action, _target("id-R1"))
    controller._error.assert_not_called()


@pytest.fixture
def window():
    """Return a variant window with a check whose R1 and R3 carry numbers, R2 none."""
    main = load_window(_PACKAGE, facade_symbols())
    parts = {
        "R1": SimpleNamespace(lcsc="C1"),
        "R2": SimpleNamespace(lcsc=""),
        "R3": SimpleNamespace(lcsc="C3"),
    }
    window = check_window(main, MagicMock(parts=parts))
    window._variant_controller = MagicMock()
    return main, window


def test_details_open_on_the_first_reference_that_carries_a_number(window, monkeypatch):
    """A row without a number is skipped, as the ordinary list's Details is."""
    _main, window = window
    presenter = window.jlc_footprint_presenter
    shown = []
    monkeypatch.setattr(presenter, "show_jlc_footprint_detail", shown.append)
    presenter.show_variant_jlc_detail(["R2", "R3", "R1"])
    presenter.show_variant_jlc_detail(["R2", "R9"])
    assert shown == ["R3"]


def test_re_fetch_acts_on_the_numbered_references_and_repaints_the_matrix(window):
    """The facade's re-fetch gets R1 and R3; the matrix repaints."""
    main, window = window
    refetch = main.jlc_footprint_window.refetch_jlc_footprint_references
    window.jlc_footprint_presenter.refetch_variant_references(["R1", "R2", "R3"])
    refetch.assert_called_once_with(window, ["R1", "R3"], window.jlc_footprint_check)
    window._variant_controller.repaint_jlc.assert_called_once_with()


def test_nothing_happens_with_the_check_off(window, monkeypatch):
    """The setting off: no dialog, no re-fetch."""
    main, window = window
    window.settings = {"jlcfootprint": {"enabled": False}}
    presenter = window.jlc_footprint_presenter
    shown = []
    monkeypatch.setattr(presenter, "show_jlc_footprint_detail", shown.append)
    presenter.show_variant_jlc_detail(["R1"])
    presenter.refetch_variant_references(["R1"])
    assert shown == []
    main.jlc_footprint_window.refetch_jlc_footprint_references.assert_not_called()
