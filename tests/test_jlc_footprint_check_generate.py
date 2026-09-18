"""Generate-time behaviour of the JLC footprint check: the wait, the summary and the Rotation column."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .wx_harness import load, load_mainwindow, load_siblings, package_stubs, wx_stubs

_PACKAGE = "jlc_footprint_check_generate_tests"


# ---------------------------------------------------------------------------
# The facade's wait and summary
# ---------------------------------------------------------------------------


class FakeProgressDialog:
    """Records every update; answers ``keep_going`` from a script."""

    instances: list = []
    script: list = []

    def __init__(self, title, message, maximum=0, parent=None, style=0):
        self.title, self.message, self.maximum, self.parent = (
            title,
            message,
            maximum,
            parent,
        )
        self.updates = []
        self.destroyed = False
        self.answers = list(FakeProgressDialog.script)
        FakeProgressDialog.instances.append(self)

    def Update(self, value, message):
        """Record the update and answer whether to keep going."""
        self.updates.append((value, message))
        return (self.answers.pop(0) if self.answers else True), False

    def Destroy(self):
        """Note the dialog was destroyed."""
        self.destroyed = True


@pytest.fixture
def facade():
    """Load the real facade with fake dialogs."""
    package = f"{_PACKAGE}_facade"
    FakeProgressDialog.instances = []
    FakeProgressDialog.script = []
    stubs = wx_stubs(
        PostEvent=MagicMock(),
        ProgressDialog=FakeProgressDialog,
        MilliSleep=MagicMock(),
        Dialog=type("Dialog", (), {}),
    )
    stubs.update(package_stubs(package))
    stubs[f"{package}.events"] = load(package, "events", stubs)
    with load_siblings(package, ("jlc_footprint_check",), stubs) as loaded:
        yield loaded["jlc_footprint_check"], stubs["wx"]


def _check(pending_sequence, tripped=False):
    sequence = list(pending_sequence)
    check = SimpleNamespace(worker=SimpleNamespace(tripped=tripped))
    check.pending_references = lambda: list(sequence.pop(0)) if sequence else []
    check.queue_estimate = lambda: (4, 90.0)
    return check


def test_wait_returns_at_once_without_pending_parts(facade):
    """No pending part means no dialog."""
    module, _ = facade
    assert module.wait_for_pending_fetches(object(), _check([[]]))
    assert FakeProgressDialog.instances == []


def test_wait_polls_until_the_queue_drains(facade):
    """The dialog counts down the pending parts and is destroyed at the end."""
    module, wx = facade
    window = object()
    assert module.wait_for_pending_fetches(
        window, _check([["R1", "R2", "R3"], ["R3"], []])
    )
    (dialog,) = FakeProgressDialog.instances
    assert dialog.parent is window
    assert dialog.maximum == 3
    assert [value for value, _ in dialog.updates] == [0, 2]
    assert "3 of 3 part(s) left (R1, R2, R3)" in dialog.updates[0][1]
    assert "4 request(s) queued, about 2 min" in dialog.updates[0][1]
    assert "1 of 3 part(s) left (R3)" in dialog.updates[1][1]
    assert dialog.destroyed
    assert wx.MilliSleep.call_count == 2


def test_wait_stops_on_cancel_or_a_tripped_breaker(facade):
    """Cancel and a tripped breaker both give up, leaving the pending parts raw."""
    module, _ = facade
    FakeProgressDialog.script = [True, False]
    assert not module.wait_for_pending_fetches(
        object(), _check([["R1"], ["R1"], ["R1"]])
    )
    assert FakeProgressDialog.instances[-1].destroyed
    assert not module.wait_for_pending_fetches(object(), _check([["R1"]], tripped=True))
    assert FakeProgressDialog.instances[-1].updates == []


def test_summary_dialog_shows_the_formatted_text(facade, monkeypatch):
    """The summary text is built from the report rows and shown modally."""
    module, _ = facade
    shown = []

    class Dialog:
        def __init__(self, parent, text):
            shown.append((parent, text))

        def ShowModal(self):
            return 0

        def Destroy(self):
            shown.append("destroyed")

    monkeypatch.setattr(module, "GenerateSummaryDialog", Dialog)
    rows = [
        module.CplRotation(
            "Q1",
            "C1",
            "SOT-23",
            "v",
            0,
            180,
            180,
            "derived",
            "green",
            None,
            "fits",
            legacy_correction=0,
        )
    ]
    text = module.show_generate_summary("window", rows, True)
    assert shown[0][0] == "window"
    assert shown[0][1] == text
    assert shown[-1] == "destroyed"
    assert text.startswith("Does not fit (0)\n\nApplied rotations (1)")
    assert "* Q1" in text


# ---------------------------------------------------------------------------
# The main window's generate path and Rotation column
# ---------------------------------------------------------------------------


@pytest.fixture
def mainwindow():
    """Load mainwindow with the facade's generate-time functions faked."""
    facade = {
        "clear_cache": MagicMock(return_value=0),
        "create_footprint_check": MagicMock(),
        "is_footprint_check_enabled": lambda settings: settings.get(
            "jlcfootprint", {}
        ).get("enabled", True),
        "recheck_board": MagicMock(return_value=0),
        "refetch_references": MagicMock(return_value=0),
        "refresh_board_data": MagicMock(return_value=0),
        "wait_for_pending_fetches": MagicMock(return_value=True),
        "show_generate_summary": MagicMock(return_value="summary"),
    }
    module = load_mainwindow(
        _PACKAGE,
        wx=wx_stubs(
            Frame=type("Frame", (), {}),
            NewIdRef=MagicMock(side_effect=object),
            BeginBusyCursor=MagicMock(),
            EndBusyCursor=MagicMock(),
            IsBusy=MagicMock(return_value=True),
            MessageBox=MagicMock(),
            MessageDialog=MagicMock(),
        ),
        jlc_footprint_check=facade,
    )
    return module, facade


def _generate_window(module, check, corrections=()):
    fabrication = SimpleNamespace(
        get_part_consistency_warnings=MagicMock(return_value=""),
        fill_zones=MagicMock(return_value=[]),
        generate_geber=MagicMock(),
        generate_excellon=MagicMock(),
        zip_gerber_excellon=MagicMock(),
        prepare_cpl=MagicMock(return_value=()),
        write_cpl=MagicMock(),
        generate_bom=MagicMock(),
        rotation_report=["row"],
    )
    window = SimpleNamespace(
        _project_storage_unavailable=False,
        generate_button=MagicMock(),
        reset_gauge=MagicMock(),
        settings={"general": {}, "gerber": {}},
        fabrication=fabrication,
        logger=MagicMock(),
        run_drc_before_gerber_export=MagicMock(return_value=True),
        layer_selection=MagicMock(),
        count_order_number_placeholders=MagicMock(return_value=0),
        store=MagicMock(),
        build_generate_hook_env=MagicMock(return_value={}),
        run_generate_hook=MagicMock(return_value=True),
        report_generation_step=MagicMock(),
        update_correction_status=MagicMock(),
        jlc_footprint_check=check,
        library=SimpleNamespace(
            read_correction_data=MagicMock(
                return_value=SimpleNamespace(corrections=corrections)
            )
        ),
    )
    window.read_valid_corrections_for_generation = (
        lambda: window.library.read_correction_data().corrections
    )
    window.read_corrections_for_summary = (
        lambda: module.JLCPCBTools.read_corrections_for_summary(window)
    )
    window.layer_selection.GetSelection.return_value = 0
    window.layer_selection.GetString.return_value = "Auto"
    window.store.get_generation_count.return_value = 0
    window.store.increment_generation_count.return_value = 1
    steps = []

    def run_generation_step(description, function, *args):
        window._current_generation_step = description
        steps.append(description)
        return function(*args)

    window.run_generation_step = run_generation_step
    return window, steps


def test_generate_waits_collects_decisions_and_shows_the_summary(mainwindow):
    """With the check on, the CPL takes the decisions and the summary follows the CPL write."""
    module, facade = mainwindow
    check = SimpleNamespace(
        decisions=MagicMock(return_value={"R1": "decision"}), scan_board=MagicMock()
    )
    window, steps = _generate_window(module, check, corrections=("rule",))

    module.JLCPCBTools.generate_fabrication_data(window)

    check.scan_board.assert_called_once_with()
    assert steps.index("Checking the board for unchecked parts") < steps.index(
        "Waiting for JLC footprint data"
    )
    facade["wait_for_pending_fetches"].assert_called_once_with(window, check)
    window.fabrication.prepare_cpl.assert_called_once_with(
        ("rule",), {"R1": "decision"}
    )
    facade["show_generate_summary"].assert_called_once_with(window, ["row"], True)
    assert steps.index("Summarising JLC footprint rotations") > steps.index(
        "Generating BOM"
    )
    assert steps[-1] == "Summarising JLC footprint rotations"
    assert "Validating corrections" not in steps
    window.update_correction_status.assert_called_once()
    window.generate_button.Enable.assert_any_call(True)


def test_generate_continues_raw_after_a_cancelled_wait_and_without_rules(mainwindow):
    """A cancelled wait is reported; unresolved rules only remove the summary's comparison."""
    module, facade = mainwindow
    facade["wait_for_pending_fetches"].return_value = False
    check = SimpleNamespace(
        decisions=MagicMock(return_value={}), scan_board=MagicMock()
    )
    window, steps = _generate_window(module, check, corrections=None)

    module.JLCPCBTools.generate_fabrication_data(window)

    window.report_generation_step.assert_any_call(
        "JLC footprint data still pending: those parts keep their raw angle"
    )
    window.fabrication.prepare_cpl.assert_called_once_with(None, {})
    facade["show_generate_summary"].assert_called_once_with(window, ["row"], False)
    module.wx.MessageBox.assert_not_called()


def test_generate_takes_the_legacy_path_when_the_check_is_off(mainwindow):
    """With the setting off the correction rules are validated and applied as before."""
    module, facade = mainwindow
    window, steps = _generate_window(module, check=None, corrections=("rule",))
    window.settings["jlcfootprint"] = {"enabled": False}

    module.JLCPCBTools.generate_fabrication_data(window)

    assert "Validating corrections" in steps
    window.fabrication.prepare_cpl.assert_called_once_with(("rule",))
    facade["wait_for_pending_fetches"].assert_not_called()
    facade["show_generate_summary"].assert_not_called()


def _column_window(module, check, enabled=True):
    window = object.__new__(module.JLCPCBTools)
    window.settings = {"jlcfootprint": {"enabled": enabled}}
    window.jlc_footprint_check = check
    window.logger = MagicMock()
    window.get_correction = MagicMock(return_value="0°, 0.0/0.0")
    model = MagicMock()
    model.columns = {"REF_COL": 0}
    model.get_all.return_value = [["R1", "x"], ["R2", "y"]]
    window.partlist_data_model = model
    return window, model


def test_rotation_cells_show_the_decision_or_the_rule(mainwindow):
    """The Rotation column carries the check's text when it runs, else the rule as before."""
    module, _ = mainwindow
    check = SimpleNamespace(
        display_text=lambda reference: {"R1": "180°", "R2": ""}[reference],
        glyph_state=lambda reference: {"R1": "green", "R2": "unknown"}[reference],
        generation=3,
    )
    window, model = _column_window(module, check)
    assert (
        module.JLCPCBTools._rotation_cell_text(window, {"reference": "R1"}, ())
        == "180°"
    )
    assert (
        module.JLCPCBTools._rotation_cell_text(window, {"reference": "R2"}, ()) == "raw"
    )
    module.JLCPCBTools._refresh_jlc_rotation_cells(window)
    assert model.set_rotation.call_args_list == [(("R1", "180°"),), (("R2", "raw"),)]
    assert model.set_jlc_state.call_args_list == [
        (("R1", "green"),),
        (("R2", "unknown"),),
    ]

    window, model = _column_window(module, check, enabled=False)
    assert (
        module.JLCPCBTools._rotation_cell_text(window, {"reference": "R1"}, ())
        == "0°, 0.0/0.0"
    )
    assert (
        module.JLCPCBTools._rotation_cell_text(window, {"reference": "R1"}, None)
        == "Unresolved"
    )
    module.JLCPCBTools._refresh_jlc_rotation_cells(window)
    model.set_rotation.assert_not_called()
    model.set_jlc_state.assert_not_called()


def test_result_event_repaints_the_checked_references(mainwindow):
    """A current-generation result repaints its references; a stale one changes nothing."""
    module, _ = mainwindow
    check = SimpleNamespace(
        display_text=lambda reference: "90° !",
        glyph_state=lambda reference: "yellow",
        references_for=lambda lcsc: ["R1", "R2"],
        generation=3,
    )
    window, model = _column_window(module, check)
    module.JLCPCBTools.on_jlc_footprint_result(
        window, SimpleNamespace(lcsc="C1", generation=2)
    )
    model.set_rotation.assert_not_called()
    module.JLCPCBTools.on_jlc_footprint_result(
        window, SimpleNamespace(lcsc="C1", generation=3)
    )
    assert model.set_rotation.call_args_list == [(("R1", "90° !"),), (("R2", "90° !"),)]
    assert model.set_jlc_state.call_args_list == [
        (("R1", "yellow"),),
        (("R2", "yellow"),),
    ]
