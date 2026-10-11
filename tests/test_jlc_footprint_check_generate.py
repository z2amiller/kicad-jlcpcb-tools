"""Generate-time behaviour of the JLC footprint check: the wait, the summary and its prompt, the Rotation column."""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from jlcfootprint.report import format_summary, summarise
from jlcfootprint.resolver import SINGLE_PAD_NOTE

from .jlc_footprint_wx_support import (
    check_window,
    facade_symbols,
    load_facade,
    load_window,
)

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


class FakeMessageDialog:
    """Records the question and its button labels; answers from ``answer``."""

    instances: list = []
    answer = None

    def __init__(self, parent, message, caption, style):
        self.parent, self.message, self.caption, self.style = (
            parent,
            message,
            caption,
            style,
        )
        self.labels = None
        self.destroyed = False
        FakeMessageDialog.instances.append(self)

    def SetYesNoLabels(self, yes, no):
        """Record the two button labels."""
        self.labels = (yes, no)
        return True

    def ShowModal(self):
        """Return the scripted answer."""
        return FakeMessageDialog.answer

    def Destroy(self):
        """Note the dialog was destroyed."""
        self.destroyed = True


@pytest.fixture
def facade():
    """Load the real facade with fake dialogs."""
    FakeProgressDialog.instances = []
    FakeProgressDialog.script = []
    FakeMessageDialog.instances = []
    with load_facade(
        f"{_PACKAGE}_facade",
        ProgressDialog=FakeProgressDialog,
        MessageDialog=FakeMessageDialog,
        MilliSleep=MagicMock(),
    ) as loaded:
        FakeMessageDialog.answer = loaded.wx.ID_YES
        yield loaded.module, loaded.wx


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


def _row(module, reference, status, correction=None, **fields):
    """Return one CPL rotation row: derived when ``correction`` is set, else raw."""
    source = fields.pop("source", "derived" if correction is not None else "raw")
    return module.CplRotation(
        reference,
        fields.pop("lcsc", "C1"),
        "SOT-23",
        "v",
        0,
        0 if correction is None else correction,
        correction,
        source,
        status,
        None,
        fields.pop("fit", "fits" if status in ("green", "yellow") else "count"),
        **fields,
    )


def _records(caplog, level):
    """Return the facade's messages logged at exactly ``level``."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == level and record.name.endswith("jlc_footprint_check")
    ]


def test_a_clean_cpl_logs_the_summary_and_asks_nothing(facade, caplog):
    """Every part placed by the check: the summary goes to the log at info, no prompt."""
    module, _ = facade
    rows = [
        _row(module, "Q1", "green", 180, legacy_correction=0),
        _row(module, "R1", "no-lcsc", lcsc="", fit=None),
    ]
    with caplog.at_level(logging.INFO):
        assert module.confirm_cpl_rotations(object(), rows, True) is True
    assert FakeMessageDialog.instances == []
    assert _records(caplog, logging.WARNING) == []
    info = _records(caplog, logging.INFO)
    assert info[0] == "JLC footprint check: rotations in the CPL"
    assert info[1:] == [
        line for line in format_summary(summarise(rows, True)).splitlines() if line
    ]
    assert any(line.startswith("* Q1") for line in info)


def test_parts_emitted_raw_against_the_check_are_warned_about_and_asked_for(
    facade, caplog
):
    """Red, unknown, still fetching and part-number-changed parts prompt; overrides, one-pad and no-LCSC rows do not."""
    module, wx = facade
    rows = [
        _row(module, "Q1", "green", 180),
        _row(module, "U3", "red", lcsc="C3"),
        _row(module, "U4", "red", lcsc="C4", fit="no_data"),
        _row(module, "U5", "unknown", lcsc="C5", fit="no_data"),
        _row(module, "TP1", "unknown", lcsc="C6", fit="no_data", note=SINGLE_PAD_NOTE),
        _row(module, "Q7", "pending", lcsc="C7", fit=None, pending=True),
        _row(module, "D2", "lcsc-mismatch", lcsc="C9", fit=None),
        _row(module, "U9", "red", 90, source="override", lcsc="C10"),
        _row(module, "R1", "no-lcsc", lcsc="", fit=None),
    ]
    with caplog.at_level(logging.INFO):
        assert module.confirm_cpl_rotations(object(), rows, True) is True
    warned = _records(caplog, logging.WARNING)
    assert [line.split()[0] for line in warned[:-1]] == ["U3", "U4", "U5", "Q7", "D2"]
    assert any(
        line.lstrip().startswith("TP1") for line in _records(caplog, logging.INFO)
    )
    assert warned[-1] == (
        "JLC footprint check: 5 part(s) at KiCad's raw angle, user chose to continue export"
    )
    (dialog,) = FakeMessageDialog.instances
    assert dialog.caption == "JLC footprint check"
    assert dialog.message == (
        "5 part(s) would be placed at KiCad's raw angle because the JLC footprint "
        "check did not accept their rotation:\n\n"
        "  U3  C3  does not fit\n"
        "  U4  C4  EasyEDA data inconsistent\n"
        "  U5  C5  unknown\n"
        "  Q7  C7  still fetching\n"
        "  D2  C9  part number changed since it was checked\n\n"
        "Details are in the log. Continue?"
    )
    assert dialog.labels == ("Continue Anyway", "Cancel Export")
    assert dialog.style & wx.NO_DEFAULT and dialog.style & wx.ICON_WARNING
    assert dialog.destroyed


def test_cancel_at_the_prompt_returns_false(facade, caplog):
    """Cancel Export answers False, so the window stops before writing; the choice is logged."""
    module, wx = facade
    FakeMessageDialog.answer = wx.ID_NO
    with caplog.at_level(logging.INFO):
        assert (
            module.confirm_cpl_rotations(object(), [_row(module, "U3", "red")], True)
            is False
        )
    assert _records(caplog, logging.WARNING)[-1].endswith("user chose to stop export")
    assert FakeMessageDialog.instances[0].destroyed


def test_closing_the_prompt_without_an_answer_stops_too(facade):
    """Only Continue Anyway goes on: a dialog closed any other way stops generation."""
    module, wx = facade
    FakeMessageDialog.answer = wx.ID_CANCEL
    assert (
        module.confirm_cpl_rotations(object(), [_row(module, "U3", "red")], True)
        is False
    )


def test_the_prompt_names_ten_parts_and_counts_the_rest(facade):
    """A long list is cut at ten references; the count and the log cover every one."""
    module, _ = facade
    rows = [_row(module, f"U{index}", "red") for index in range(1, 13)]
    module.confirm_cpl_rotations(object(), rows, True)
    message = FakeMessageDialog.instances[0].message
    assert message.startswith("12 part(s) would be placed at KiCad's raw angle")
    listed = message.split("\n\n")[1].splitlines()
    assert listed[0] == "  U1  C1  does not fit"
    assert listed[9] == "  U10  C1  does not fit"
    assert listed[10:] == ["  and 2 more"]


# ---------------------------------------------------------------------------
# The main window's generate path and Rotation column
# ---------------------------------------------------------------------------


@pytest.fixture
def mainwindow():
    """Load mainwindow with the facade's generate-time functions faked."""
    facade = facade_symbols()
    module = load_window(
        _PACKAGE,
        facade,
        BeginBusyCursor=MagicMock(),
        EndBusyCursor=MagicMock(),
        IsBusy=MagicMock(return_value=True),
        MessageBox=MagicMock(),
        MessageDialog=MagicMock(),
    )
    return module, facade


def _generate_window(module, check, corrections=()):
    fabrication = SimpleNamespace(
        begin_ordinary_generation=MagicMock(return_value=SimpleNamespace(cpl_rows=())),
        end_ordinary_generation=MagicMock(),
        validate_generation=MagicMock(),
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
        _get_current_board=MagicMock(return_value=SimpleNamespace()),
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
    window.read_corrections_for_summary = lambda: (
        module.jlc_footprint_window.JlcFootprintPresenter(
            window
        ).read_corrections_for_summary()
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


def test_generate_waits_collects_decisions_and_asks_before_writing(mainwindow):
    """With the check on, the CPL takes the decisions and the summary comes before any write."""
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
    window.fabrication.begin_ordinary_generation.assert_called_once_with(
        ("rule",), decisions={"R1": "decision"}
    )
    facade["confirm_cpl_rotations"].assert_called_once_with(window, ["row"], True)
    summary = steps.index("Summarising JLC footprint rotations")
    assert steps[summary - 1] == "Preparing placement data"
    assert steps[summary + 1] == "Checking part consistency"
    assert steps[-1] == "Generating BOM"
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
    window.fabrication.begin_ordinary_generation.assert_called_once_with(
        (), decisions={}
    )
    facade["confirm_cpl_rotations"].assert_called_once_with(window, ["row"], False)
    module.wx.MessageBox.assert_not_called()


def test_generate_takes_the_legacy_path_when_the_check_is_off(mainwindow):
    """With the setting off the correction rules are validated and applied as before."""
    module, facade = mainwindow
    window, steps = _generate_window(module, check=None, corrections=("rule",))
    window.settings["jlcfootprint"] = {"enabled": False}

    module.JLCPCBTools.generate_fabrication_data(window)

    assert "Validating corrections" in steps
    window.fabrication.begin_ordinary_generation.assert_called_once_with(
        ("rule",), decisions=None
    )
    facade["wait_for_pending_fetches"].assert_not_called()
    facade["confirm_cpl_rotations"].assert_not_called()
    assert "Summarising JLC footprint rotations" not in steps
    window.fabrication.write_cpl.assert_called_once()


def test_generate_hands_the_decisions_to_upstream_s_variant_controller(mainwindow):
    """Upstream's variant path gets the decisions by keyword, as the ordinary path does."""
    module, facade = mainwindow
    check = SimpleNamespace(
        decisions=MagicMock(return_value={"R1": "decision"}), scan_board=MagicMock()
    )
    window, _steps = _generate_window(module, check, corrections=("rule",))
    controller = window._variant_controller = MagicMock()
    window.fabrication.generation_publication = MagicMock()

    module.JLCPCBTools.generate_fabrication_data(window)

    controller.begin_generation.assert_called_once_with(
        ("rule",), decisions={"R1": "decision"}
    )
    window.fabrication.begin_ordinary_generation.assert_not_called()
    controller.end_generation.assert_called_once_with()
    module.wx.MessageBox.assert_not_called()


@pytest.mark.parametrize("variant", [False, True], ids=["ordinary", "variant"])
def test_cancel_at_the_rotation_prompt_writes_no_file(mainwindow, variant):
    """Cancel Export stops both generation paths before anything is filled, saved or written."""
    module, facade = mainwindow
    facade["confirm_cpl_rotations"].return_value = False
    check = SimpleNamespace(
        decisions=MagicMock(return_value={"R1": "decision"}), scan_board=MagicMock()
    )
    window, steps = _generate_window(module, check, corrections=("rule",))
    if variant:
        controller = window._variant_controller = MagicMock()

    module.JLCPCBTools.generate_fabrication_data(window)

    facade["confirm_cpl_rotations"].assert_called_once_with(window, ["row"], True)
    assert steps[-1] == "Summarising JLC footprint rotations"
    window.report_generation_step.assert_any_call(
        "Export stopped by the JLC footprint check"
    )
    fabrication = window.fabrication
    for written in (
        fabrication.fill_zones,
        fabrication.generate_geber,
        fabrication.generate_excellon,
        fabrication.zip_gerber_excellon,
        fabrication.write_cpl,
        fabrication.generate_bom,
        window.run_drc_before_gerber_export,
        window.run_generate_hook,
        window.store.increment_generation_count,
    ):
        written.assert_not_called()
    if variant:
        controller.end_generation.assert_called_once_with()
    else:
        fabrication.end_ordinary_generation.assert_called_once_with()


def _column_window(module, check, enabled=True):
    window = check_window(module, check, enabled=enabled)
    window.get_correction = MagicMock(return_value="0°, 0.0/0.0")
    model = MagicMock()
    model.columns = {"REF_COL": 0}
    model.get_all.return_value = [["R1", "x"], ["R2", "y"]]
    window.partlist_data_model = model
    return window, model


def test_rotation_cells_show_the_decision_or_the_rule(mainwindow, monkeypatch):
    """The Rotation column carries the check's text when it runs, else the rule as before."""
    module, _ = mainwindow
    check = SimpleNamespace(
        display_text=lambda reference: {"R1": "180°", "R2": ""}[reference],
        generation=3,
    )
    monkeypatch.setattr(
        module.jlc_footprint_window,
        "glyph_state",
        lambda check, reference: {"R1": "green", "R2": "unknown"}[reference],
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


def test_result_event_repaints_the_checked_references(mainwindow, monkeypatch):
    """A current-generation result repaints its references; a stale one changes nothing."""
    module, _ = mainwindow
    check = SimpleNamespace(
        display_text=lambda reference: "90° !",
        references_for=lambda lcsc: ["R1", "R2"],
        generation=3,
    )
    monkeypatch.setattr(
        module.jlc_footprint_window, "glyph_state", lambda check, reference: "yellow"
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


def test_the_summary_reports_the_position_split_from_the_window_s_settings(
    facade, caplog
):
    """Spec 17.4: the window carries the setting, so no generate step has to pass it."""
    module, _ = facade
    rows = [
        module.CplRotation(
            "Q1", "C1", "SOT-23", "v", 0, 180, 180, "derived", "green", None, "fits"
        ),
        module.CplRotation("Q2", "C2", "SOT-23", "v", 0, 0, None, "raw", "pending"),
    ]
    rows[0].position_source = "origin"
    window = SimpleNamespace(settings={"jlcfootprint": {"exact_origin": True}})

    with caplog.at_level(logging.INFO):
        module.confirm_cpl_rotations(window, rows, True)

    assert _records(caplog, logging.INFO)[1] == (
        "Positions: 1 at JLC's package origin, 1 at the pad-box centre"
    )
    assert module.is_exact_origin_enabled(window.settings) is True
    assert module.is_exact_origin_enabled({}) is False


def test_a_window_carrying_no_settings_logs_no_position_split(facade, caplog):
    """A window object without the attribute reads the setting off, not on."""
    module, _ = facade
    rows = [
        module.CplRotation(
            "Q1", "C1", "SOT-23", "v", 0, 180, 180, "derived", "green", None, "fits"
        )
    ]
    rows[0].position_source = "origin"

    with caplog.at_level(logging.INFO):
        module.confirm_cpl_rotations(object(), rows, True)

    assert _records(caplog, logging.INFO)[1] == "Does not fit (0)"
