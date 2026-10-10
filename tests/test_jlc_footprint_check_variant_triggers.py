"""What starts a scan on a board with KiCad design variants (spec 18.4).

Upstream's variant controller emits no events: an output switch goes through
``_on_output``, every edit through ``_apply``, a board change made in pcbnew is
noticed by the ``_on_timer`` poll or a ``refresh`` comparing ``source_token``.
The controller keeps one thin hook in each, calling the window's presenter, which
scans: a full scan for a switch or a board change, a reference scan for an edit.
The controller runs for real here (its module loaded under a fake wx, with a real
``VariantSession`` over upstream's stateful board double); the presenter runs for
real in the window tests below it.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .jlc_footprint_wx_support import check_window, facade_symbols, load_window
from .test_fabrication_jlc_generation_paths import _load_variant_controller
from .variant_data_support import _native_session
from .variant_native_support import Snapshot, State, native
from .wx_harness import module, temporary_modules

_PACKAGE = "jlc_footprint_check_variant_triggers_tests"


# ---------------------------------------------------------------------------
# Upstream's variant controller: one hook per trigger
# ---------------------------------------------------------------------------


@pytest.fixture
def controller_module():
    """Load the real ``variant/controller.py`` under a fake wx."""
    with _load_variant_controller(f"{_PACKAGE}_controller") as loaded:
        controller_module = loaded["variant.controller"]
        controller_module.wx.CallAfter = MagicMock()
        yield controller_module


def _controller(controller_module, tmp_path: Path):
    """Return a controller over a real session whose presenter and render record calls."""
    _session_module, session, _store, _adapter, board = _native_session(tmp_path)
    calls: list = []
    controller = object.__new__(controller_module.VariantMainController)
    controller.closed = False
    controller._refreshing = False
    controller._presentation = None
    controller._jlc_repaint_queued = False
    controller.session = session
    controller.render = lambda: calls.append(("render", session.output_variant))
    controller.recompute = lambda: calls.append(("recompute",))
    controller.start_enrichment = lambda **_kwargs: None
    controller.timer = SimpleNamespace(Start=lambda _ms: None, Stop=lambda: None)
    controller.dialog = SimpleNamespace(
        jlc_footprint_presenter=SimpleNamespace(
            rescan_variant_board=lambda: calls.append(
                ("rescan", session.output_variant)
            ),
            variant_board_edited=lambda before, after: calls.append(
                ("edited", before, after)
            ),
        ),
        pcbnew=SimpleNamespace(Refresh=lambda: None),
        _invalidate_catalog_details=lambda: None,
    )
    return controller, session, board, calls


def _choose(controller, session, name):
    """Dispatch upstream's output choice handler for one variant name."""
    names = [variant.name for variant in session.snapshot.variants]
    controller._on_output(SimpleNamespace(GetSelection=lambda: names.index(name)))


def test_an_output_switch_rescans_before_the_matrix_re_renders(
    controller_module, tmp_path
):
    """The scan reads the new output variant, then the matrix draws its cells."""
    controller, session, _board, calls = _controller(controller_module, tmp_path)
    _choose(controller, session, "B")
    assert calls == [("rescan", "B"), ("render", "B")]


def test_a_failed_output_switch_does_not_rescan(controller_module, tmp_path):
    """Upstream refuses the switch (generation running): the check keeps its output."""
    controller, session, _board, calls = _controller(controller_module, tmp_path)
    session.generating = True
    controller.output_choice = MagicMock()
    controller._error = MagicMock()
    _choose(controller, session, "B")
    assert calls == []
    controller._error.assert_called_once()


def _edit(session, variant, lcsc):
    """Return one native LCSC edit of the board double's only component."""
    target = session.snapshot.target("component-1", variant)
    return (native.VariantEdit(target, (("lcsc", lcsc),)),)


def test_an_edit_hands_the_snapshots_before_and_after_to_the_check(
    controller_module, tmp_path
):
    """The check re-reads what the edit changed before the matrix re-renders."""
    controller, session, _board, calls = _controller(controller_module, tmp_path)
    before = session.snapshot
    controller._apply(_edit(session, "A", "C999"))
    (edited, rendered) = calls
    assert edited[0] == "edited" and rendered[0] == "render"
    assert edited[1] is before
    assert edited[2] is session.snapshot
    assert edited[2].get("component-1", "A").lcsc == "C999"


def test_a_recovered_failed_edit_still_reports_what_changed(
    controller_module, tmp_path
):
    """Upstream re-reads after a rolled-back edit; the check sees that read too."""
    controller, session, board, calls = _controller(controller_module, tmp_path)
    before = session.snapshot
    board.parts[0].fail_field = "LCSC"
    with pytest.raises(native.NativeVariantError):
        controller._apply(_edit(session, "", "C999"))
    assert session.reliable
    assert [call[0] for call in calls] == ["edited", "render"]
    assert calls[0][1] is before


def test_the_board_poll_rescans_when_the_source_token_changes(
    controller_module, tmp_path
):
    """A pcbnew edit changes the token: one full scan, then the re-render."""
    controller, _session, board, calls = _controller(controller_module, tmp_path)
    controller._on_timer(None)
    assert calls == []
    board.parts[0].SetField("LCSC", "C555")
    controller._on_timer(None)
    assert calls == [("rescan", "A"), ("render", "A")]


def test_a_refresh_that_reads_a_new_token_rescans_too(controller_module, tmp_path):
    """Refresh advances the token the poll compares, so it scans for the poll."""
    controller, _session, board, calls = _controller(controller_module, tmp_path)
    controller.refresh()
    assert calls == [("render", "A")]
    board.parts[0].SetField("LCSC", "C555")
    calls.clear()
    controller.refresh()
    assert calls == [("rescan", "A"), ("render", "A")]
    calls.clear()
    controller._on_timer(None)
    assert calls == []


def test_check_results_repaint_once_per_burst(controller_module, tmp_path):
    """Results queue one re-render; generation holds it until the matrix is free."""
    controller, session, _board, calls = _controller(controller_module, tmp_path)
    call_after = controller_module.wx.CallAfter
    controller.repaint_jlc()
    controller.repaint_jlc()
    call_after.assert_called_once_with(controller._repaint_jlc_now)
    controller._repaint_jlc_now()
    assert calls == [("recompute",)]
    session.generating = True
    controller.repaint_jlc()
    controller._repaint_jlc_now()
    assert calls == [("recompute",)]
    controller.closed = True
    controller.repaint_jlc()
    assert call_after.call_count == 2


def test_a_window_without_a_presenter_keeps_upstream_s_behaviour(
    controller_module, tmp_path
):
    """The hooks are no-ops for a dialog that has no JLC footprint presenter."""
    controller, session, board, calls = _controller(controller_module, tmp_path)
    del controller.dialog.jlc_footprint_presenter
    _choose(controller, session, "B")
    controller._apply(_edit(session, "A", "C999"))
    board.parts[0].SetField("LCSC", "C555")
    controller._on_timer(None)
    assert [call[0] for call in calls] == ["render", "render", "render"]


# ---------------------------------------------------------------------------
# The window's presenter
# ---------------------------------------------------------------------------


@pytest.fixture
def mainwindow():
    """Load the main window with a check factory that records every check it builds."""
    checks = []

    def create(_window, _pcbnew):
        check = MagicMock()
        check.generation = 1
        check.references_for.return_value = ["R1"]
        checks.append(check)
        return check

    return load_window(_PACKAGE, facade_symbols(create_footprint_check=create)), checks


def _variant_window(main, check=None):
    """Return a window on a variant board: a store, a controller that records repaints."""
    window = check_window(main, check)
    window.store = SimpleNamespace(dbfile="project.db")
    window.pcbnew = SimpleNamespace()
    window.partlist_data_model = MagicMock()
    window._variant_controller = MagicMock()
    return window


def test_a_board_with_variants_gets_the_check(mainwindow):
    """The rebase's guard is gone: a variant board starts and scans its check (spec 18.7)."""
    main, checks = mainwindow
    window = _variant_window(main)
    window._start_jlc_footprint_check()
    (check,) = checks
    check.start.assert_called_once_with()
    check.scan_board.assert_called_once_with()
    assert window.jlc_footprint_check is check


def test_a_switch_or_a_board_change_is_a_full_scan(mainwindow):
    """``rescan_variant_board`` scans the whole board when the check runs, else nothing."""
    main, _checks = mainwindow
    check = MagicMock()
    window = _variant_window(main, check)
    window.jlc_footprint_presenter.rescan_variant_board()
    check.scan_board.assert_called_once_with()
    window.settings = {"jlcfootprint": {"enabled": False}}
    window.jlc_footprint_presenter.rescan_variant_board()
    check.scan_board.assert_called_once_with()


def _snapshot(numbers):
    """Return a real snapshot of R1 and R2 with {(reference, variant): lcsc}."""
    return Snapshot(
        tuple(
            State(f"id-{reference}", reference, name, lcsc=numbers[(reference, name)])
            for reference in ("R1", "R2")
            for name in ("", "A")
        ),
        ("", "A"),
    )


BEFORE = {("R1", ""): "C1", ("R1", "A"): "C1", ("R2", ""): "C2", ("R2", "A"): "C3"}


@pytest.mark.parametrize(
    "change,references",
    [
        ({("R1", "A"): "C9"}, ["R1"]),  # the output variant's number
        ({("R2", ""): "C8"}, ["R2"]),  # another variant: a new code to prefetch
        ({("R1", ""): "", ("R1", "A"): ""}, ["R1"]),  # cleared: "no LCSC"
        ({}, []),  # nothing the check reads changed
    ],
    ids=["output", "other_variant", "cleared", "unchanged"],
)
def test_an_edit_re_reads_the_references_whose_numbers_changed(
    mainwindow, change, references
):
    """Only references with a changed number in some variant are scanned again."""
    main, _checks = mainwindow
    check = MagicMock()
    window = _variant_window(main, check)
    window.jlc_footprint_presenter.variant_board_edited(
        _snapshot(BEFORE), _snapshot({**BEFORE, **change})
    )
    if references:
        check.enqueue_references.assert_called_once_with(references)
    else:
        check.enqueue_references.assert_not_called()


def test_a_failing_scan_is_logged_not_raised_into_upstream_s_handlers(mainwindow):
    """The check is advisory: a scan error must not mark the matrix unreliable."""
    main, _checks = mainwindow
    check = MagicMock()
    check.scan_board.side_effect = OSError("disk")
    check.enqueue_references.side_effect = RuntimeError("board replaced")
    window = _variant_window(main, check)
    window.jlc_footprint_presenter.rescan_variant_board()
    window.jlc_footprint_presenter.variant_board_edited(
        _snapshot(BEFORE), _snapshot({**BEFORE, ("R1", "A"): "C9"})
    )
    messages = [call.args[1] for call in window.logger.warning.call_args_list]
    assert [str(message) for message in messages] == ["disk", "board replaced"]


def test_a_result_repaints_the_matrix_not_the_hidden_part_list(mainwindow):
    """In variant mode a result event goes to the controller (spec 18.4)."""
    main, _checks = mainwindow
    check = MagicMock(generation=4)
    check.references_for.return_value = ["R1"]
    window = _variant_window(main, check)
    window.on_jlc_footprint_result(SimpleNamespace(lcsc="C1", generation=4))
    window._variant_controller.repaint_jlc.assert_called_once_with()
    window.partlist_data_model.set_rotation.assert_not_called()
    window.on_jlc_footprint_result(SimpleNamespace(lcsc="C1", generation=3))
    window._variant_controller.repaint_jlc.assert_called_once_with()


def test_the_repaint_paths_go_to_the_matrix_in_variant_mode(mainwindow):
    """Every full repaint and every per-reference repaint asks the controller instead."""
    main, _checks = mainwindow
    window = _variant_window(main, MagicMock())
    window._refresh_jlc_rotation_cells()
    window.jlc_footprint_presenter._repaint_jlc_references(["R1"])
    assert window._variant_controller.repaint_jlc.call_count == 2
    window.partlist_data_model.set_rotation.assert_not_called()
    window.partlist_data_model.get_all.assert_not_called()


def _init_store_window(main, steps, controller=None):
    """Return a window carrying what ``init_store``'s variant branch reads."""
    window = object.__new__(main.JLCPCBTools)
    window.project_path = "/project"
    window.fabrication = object()
    window._variant_mode = True
    window._variant_controller = controller
    window._get_current_board = lambda: SimpleNamespace(
        GetVariantNamesForUI=lambda: ["Default", "A"]
    )
    window.assembly_lookup = SimpleNamespace(invalidate=lambda: None)
    window._set_project_storage_error = lambda error: steps.append(("storage", error))
    window._migrate_legacy_assignments = lambda: None
    window._maybe_show_schematic_storage_notice = lambda _preexisted: None
    window._start_jlc_footprint_check = lambda: steps.append("start check")
    window._refresh_jlc_rotation_cells = lambda: steps.append("repaint")
    return window


def test_opening_a_variant_board_starts_the_check_after_the_matrix(mainwindow):
    """``init_store`` builds the matrix, then starts the check and repaints (spec 18.4)."""
    main, _checks = mainwindow
    steps: list = []

    class Controller:
        """Record the matrix being built and its enrichment starting."""

        def __init__(self, _window, _store):
            steps.append("matrix")

        def start_enrichment(self):
            """Record the supplier lookup."""
            steps.append("enrichment")

    variant = module(f"{_PACKAGE}.variant")
    variant.__path__ = []
    stubs = {
        f"{_PACKAGE}.variant": variant,
        f"{_PACKAGE}.variant.store": module(
            f"{_PACKAGE}.variant.store", VariantStore=lambda *_args: "store"
        ),
        f"{_PACKAGE}.variant.controller": module(
            f"{_PACKAGE}.variant.controller", VariantMainController=Controller
        ),
    }
    window = _init_store_window(main, steps)
    with temporary_modules(stubs):
        main.JLCPCBTools.init_store(window)
    assert steps == [
        ("storage", None),
        "matrix",
        "enrichment",
        "start check",
        "repaint",
    ]


def test_reopening_storage_on_a_variant_board_restarts_the_check(mainwindow):
    """The storage error stopped the check; the recovered branch starts it again."""
    main, _checks = mainwindow
    steps: list = []
    controller = SimpleNamespace(
        session=SimpleNamespace(_check_board=lambda: None, reliable=True),
        cache="store",
        refresh=lambda: steps.append("refresh"),
        _update_enabled=lambda: steps.append("enabled"),
    )
    window = _init_store_window(main, steps, controller)
    main.JLCPCBTools.init_store(window)
    assert steps == [
        "refresh",
        ("storage", None),
        "enabled",
        "start check",
        "repaint",
    ]
