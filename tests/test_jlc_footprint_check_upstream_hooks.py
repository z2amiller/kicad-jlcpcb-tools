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

    presenter = module.jlc_footprint_window
    presenter.create_footprint_check = create
    presenter.is_footprint_check_enabled = _enabled
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


def test_upstream_s_cell_refresh_shows_what_the_cpl_emits(mainwindow):
    """Upstream rewrites the cell on every assignment and removal: the check's text wins."""
    module, _ = mainwindow
    check = SimpleNamespace(display_text=lambda reference: "90°")
    for enabled, expected in ((True, "90°"), (False, "0°, 0.0/0.0 (fpt)")):
        window = _window(module, check, enabled=enabled)
        window.partlist_data_model = MagicMock()
        window.store = SimpleNamespace(
            get_part=lambda reference: {"reference": reference}
        )
        window.library = SimpleNamespace(
            read_correction_data=lambda: SimpleNamespace(corrections=())
        )
        window.update_correction_status = MagicMock()
        window.get_correction = MagicMock(return_value="0°, 0.0/0.0 (fpt)")
        module.JLCPCBTools.refresh_corrections(window, ["R1"])
        window.partlist_data_model.set_correction.assert_called_once_with(
            "R1", expected
        )


def _recording_window(module, monkeypatch, calls):
    """Return a window whose check, enrichment and cell refresh record their calls."""
    monkeypatch.setattr(module.wx, "PostEvent", MagicMock(), raising=False)
    check = SimpleNamespace(
        enqueue_references=lambda references: calls.append(("re-read", references))
    )
    window = _window(module, check)
    footprint = SimpleNamespace(
        GetFPID=lambda: SimpleNamespace(GetLibItemName=lambda: "R_0603"),
        GetValue=lambda: "10k",
    )
    window._get_current_board = lambda: SimpleNamespace(
        FindFootprintByReference=lambda reference: footprint
    )
    window._apply_board_change = MagicMock(return_value=True)
    window.partlist_data_model = MagicMock()
    window.refresh_corrections = lambda references: calls.append(
        ("refresh", list(references))
    )
    return window


def test_an_assignment_re_reads_the_parts_before_upstream_s_cell_refresh(
    mainwindow, monkeypatch
):
    """The check reads the new number first, so the refreshed cell shows its decision."""
    module, _ = mainwindow
    calls = []
    window = _recording_window(module, monkeypatch, calls)
    window.is_catalog_available = lambda: True
    window._catalog_get_part_details = lambda lcsc, strict=False: {"type": "Basic"}
    window.store = SimpleNamespace(get_part=lambda reference: None)
    window.assembly_lookup = SimpleNamespace(pending=set())
    window.start_assembly_enrichment = lambda references: calls.append(
        ("enrich", references)
    )

    assigned = module.JLCPCBTools._apply_lcsc_assignments(window, {"R1": "C25804"})

    assert assigned == ["R1"]
    assert calls == [("enrich", ["R1"]), ("re-read", ["R1"]), ("refresh", ["R1"])]


def test_removing_a_number_re_reads_the_part_before_the_cell_is_rewritten(
    mainwindow, monkeypatch
):
    """Removal re-reads the part, whose decision becomes "no LCSC", then rewrites the cell."""
    module, _ = mainwindow
    calls = []
    window = _recording_window(module, monkeypatch, calls)
    window.footprint_list = SimpleNamespace(GetSelections=lambda: ["item"])
    window.partlist_data_model.get_reference.return_value = "R1"

    module.JLCPCBTools.remove_lcsc_number(window)

    assert calls == [("re-read", ["R1"]), ("refresh", ["R1"])]


def test_a_board_replaced_before_the_first_scan_recovers_like_upstream(mainwindow):
    """The store refuses a replaced board mid-scan: the new check stops, storage recovers."""
    module, _ = mainwindow
    error = module.BoardContextChanged("replaced")
    check = MagicMock(scan_board=MagicMock(side_effect=error))
    module.jlc_footprint_window.create_footprint_check = MagicMock(return_value=check)
    window = _window(module)
    window._set_project_storage_error = MagicMock()

    window._start_jlc_footprint_check()

    check.stop.assert_called_once_with()
    window._set_project_storage_error.assert_called_once_with(error)
    assert window.jlc_footprint_check is None
