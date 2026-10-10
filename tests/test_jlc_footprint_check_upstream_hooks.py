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


class CloseEvent:
    """Keep the native veto flag, as upstream's close tests do."""

    def __init__(self) -> None:
        self.vetoed = False

    def CanVeto(self) -> bool:
        """Report an ordinary user close."""
        return True

    def Veto(self) -> None:
        """Remember that the window stays open."""
        self.vetoed = True


def _closing_window(module, steps, **state):
    """Return a window carrying what quit_dialog reads, recording its teardown steps."""
    window = _window(module, MagicMock(stop=lambda: steps.append("check")))
    window._closing = False
    window._saving_on_close = False
    window._layout_ready = False
    window._project_storage_unavailable = False
    window._variant_controller = None
    window.GetChildren = list
    window.Destroy = lambda: steps.append("destroy")
    window.assembly_lookup = SimpleNamespace(close=lambda: steps.append("lookup"))
    window._type_cell_tooltip = SimpleNamespace(stop=lambda: steps.append("tooltip"))

    def save(*, interactive: bool = True) -> bool:
        steps.append("save")
        return True

    window.export_to_schematic = MagicMock(side_effect=save)
    for name, value in state.items():
        setattr(window, name, value)
    return window


def test_a_completed_close_stops_the_check_after_upstream_s_save(mainwindow):
    """The check stops with the window's other workers, after the save on close."""
    module, _ = mainwindow
    steps = []
    window = _closing_window(module, steps)
    event = CloseEvent()

    module.JLCPCBTools.quit_dialog(window, event)

    assert not event.vetoed
    assert steps == ["save", "lookup", "tooltip", "check", "destroy"]
    assert window.jlc_footprint_check is None


@pytest.mark.parametrize("veto", ["generating", "save cancelled"])
def test_a_vetoed_close_keeps_the_check_of_the_open_window(mainwindow, veto):
    """Upstream's close vetoes come first: a window that stays open keeps its check."""
    module, _ = mainwindow
    steps = []
    window = _closing_window(module, steps, _generating=veto == "generating")
    if veto == "save cancelled":
        window.export_to_schematic.side_effect = lambda **_kwargs: False
    check = window.jlc_footprint_check
    event = CloseEvent()

    module.JLCPCBTools.quit_dialog(window, event)

    assert event.vetoed
    assert "check" not in steps
    assert window.jlc_footprint_check is check


def test_unavailable_project_storage_stops_the_check(mainwindow):
    """Upstream's storage-error path stops the check beside its own assembly lookup."""
    module, _ = mainwindow
    check = MagicMock()
    window = _window(module, check)
    for name in (
        "partlist_data_model",
        "assembly_lookup",
        "project_storage_status",
        "footprint_list",
        "right_toolbar",
        "upper_toolbar",
        "Layout",
    ):
        setattr(window, name, MagicMock())
    window.library = None

    module.JLCPCBTools._set_project_storage_error(window, OSError("gone"))

    check.stop.assert_called_once_with()
    assert window.jlc_footprint_check is None
    window.assembly_lookup.invalidate.assert_called_once_with()


@pytest.mark.parametrize("catalog", [True, False])
def test_the_check_starts_once_the_catalog_meets_the_listed_parts(mainwindow, catalog):
    """With a catalog the check starts after the list fills; the bare list waits for it."""
    module, _ = mainwindow
    calls = []
    window = _window(module)
    window.library = SimpleNamespace(state=module.LibraryState.INITIALIZED)
    window.is_catalog_available = lambda: catalog
    window._part_preferences_applied_on_open = True
    window.populate_footprint_list = lambda: calls.append("list")
    window.start_assembly_enrichment = lambda: calls.append("enrich")
    window.recompute_bom_estimate = lambda: calls.append("estimate")
    window._start_jlc_footprint_check = lambda: calls.append("check")
    window._refresh_jlc_rotation_cells = lambda: calls.append("cells")

    module.JLCPCBTools._initialize_catalog_parts(window)

    expected = ["list", "enrich", "estimate", "check", "cells"] if catalog else ["list"]
    assert calls == expected


def test_the_jlc_submenu_follows_upstream_s_last_correction_entry(
    mainwindow, monkeypatch
):
    """It closes the correction block, after "by LCSC", before the part preferences."""
    module, _ = mainwindow
    entries = []

    class Menu:
        def Append(self, item):
            entries.append(item.label)

        def Bind(self, *_args):
            pass

        def Destroy(self):
            pass

    monkeypatch.setattr(module.wx, "Menu", Menu, raising=False)
    monkeypatch.setattr(
        module.wx,
        "MenuItem",
        lambda _menu, _identifier, label: SimpleNamespace(label=label),
        raising=False,
    )
    window = _window(module)
    window.footprint_list = MagicMock()
    window._append_jlc_footprint_menu = lambda _menu: entries.append("JLC footprint")

    module.JLCPCBTools.OnRightDown(window)

    at = entries.index("JLC footprint")
    assert entries[at - 1 : at + 2] == [
        "Add Correction by LCSC",
        "JLC footprint",
        "Apply part preferences",
    ]


@pytest.mark.parametrize("enabled", [True, False])
def test_every_list_fill_shows_what_the_cpl_emits(mainwindow, monkeypatch, enabled):
    """Upstream refills the list on each window activation: the check's text and glyph."""
    module, _ = mainwindow
    monkeypatch.setattr(module.wx, "PostEvent", MagicMock(), raising=False)
    check = SimpleNamespace(
        display_text=lambda reference: {"R1": "90°", "R2": ""}[reference]
    )
    window = _window(module, check, enabled=enabled)
    footprint = SimpleNamespace(GetLayer=lambda: 0)
    window._get_current_board = lambda: SimpleNamespace(
        FindFootprintByReference=lambda _reference: footprint
    )
    window.store = SimpleNamespace(
        read_all=lambda: [
            {
                "reference": reference,
                "value": "10k",
                "footprint": "R_0603",
                "lcsc": "C25804",
                "exclude_from_bom": 0,
                "exclude_from_pos": 0,
            }
            for reference in ("R1", "R2")
        ]
    )
    window.library = SimpleNamespace(
        read_correction_data=lambda: SimpleNamespace(corrections=())
    )
    window.update_correction_status = MagicMock()
    window.partlist_data_model = MagicMock()
    window._catalog_get_part_details = lambda _lcsc: {}
    window.hide_bom_parts = window.hide_pos_parts = False
    window._get_enrichment_status_label = lambda _part: ""
    window.get_correction = lambda _part, _corrections: "0°, 0.0/0.0 (fpt)"
    painted = []
    window._apply_jlc_cell = painted.append

    module.JLCPCBTools._populate_footprint_rows(window)

    rows = [call.args[0] for call in window.partlist_data_model.AddEntry.call_args_list]
    expected = ["90°", "raw"] if enabled else ["0°, 0.0/0.0 (fpt)"] * 2
    assert [row[9] for row in rows] == expected
    assert painted == ["R1", "R2"]
