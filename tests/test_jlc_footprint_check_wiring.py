"""Tests for the plugin-side wiring of the JLC footprint check: facade, main window, settings."""

from contextlib import closing
import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .test_jlcfootprint_kicad_adapter import FakeFootprint, FakePad
from .wx_harness import load, load_mainwindow, load_siblings, package_stubs, wx_stubs

_PACKAGE = "jlc_footprint_check_wiring_tests"


# ---------------------------------------------------------------------------
# The facade
# ---------------------------------------------------------------------------


@pytest.fixture
def facade():
    """Load the real facade with a fake wx that records posted events."""
    package = f"{_PACKAGE}_facade"
    stubs = wx_stubs(PostEvent=MagicMock(), Dialog=type("Dialog", (), {}))
    stubs.update(package_stubs(package))
    stubs[f"{package}.events"] = load(package, "events", stubs)
    with load_siblings(package, ("jlc_footprint_check",), stubs) as loaded:
        yield loaded["jlc_footprint_check"], stubs["wx"], stubs[f"{package}.events"]


def _window(tmp_path):
    board = SimpleNamespace(
        GetFootprints=lambda: [
            FakeFootprint(
                "Q1",
                [FakePad("1", -1, 0.95), FakePad("2", -1, -0.95), FakePad("3", 1, 0)],
            )
        ]
    )
    pcbnew = SimpleNamespace(
        GetBoard=lambda: board, ToMM=lambda v: v, PAD_ATTRIB_NPTH=3, PAD_SHAPE_CUSTOM=6
    )
    with closing(sqlite3.connect(tmp_path / "project.db")) as con:
        con.execute("CREATE TABLE part_info (reference TEXT)")
    window = SimpleNamespace(
        library=SimpleNamespace(datadir=str(tmp_path)),
        store=SimpleNamespace(dbfile=str(tmp_path / "project.db")),
        settings={},
    )
    return window, pcbnew


def test_enabled_setting_defaults_on(facade):
    """The fork ships the switch on; a stored False turns it off."""
    module, _, _ = facade
    assert module.is_footprint_check_enabled({})
    assert not module.is_footprint_check_enabled({"jlcfootprint": {"enabled": False}})


def test_create_footprint_check_uses_the_library_directory_and_the_project_db(
    facade, tmp_path
):
    """The cache sits beside corrections.db, verdicts go to project.db, and results post events."""
    module, wx, events = facade
    window, pcbnew = _window(tmp_path)
    check = module.create_footprint_check(window, pcbnew)
    assert check.cache.path == str(tmp_path / "jlcfootprint-cache.db")
    assert check.verdicts.db_path == str(tmp_path / "project.db")
    assert (tmp_path / "jlcfootprint-cache.db").exists()
    with closing(sqlite3.connect(tmp_path / "project.db")) as con:
        tables = {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert {"part_info", "footprint_verdict"} <= tables
    parts = check.read_board()
    assert [(p.reference, p.lcsc, len(p.pads)) for p in parts] == [("Q1", "C2132", 3)]
    check.post("C2132", 7)
    target, event = wx.PostEvent.call_args.args
    assert target is window
    assert isinstance(event, events.JlcFootprintResultEvent)
    assert (event.lcsc, event.generation) == ("C2132", 7)
    check.message("breaker")
    target, event = wx.PostEvent.call_args.args
    assert isinstance(event, events.MessageEvent)
    assert (event.title, event.style, event.text) == (
        "JLC footprint check",
        "warning",
        "breaker",
    )
    assert not check.worker.is_running()


def test_import_seed_merges_and_rescans(facade, tmp_path):
    """A seed file is merged into the running check's cache, which then rescans the board."""
    module, _, _ = facade
    window, pcbnew = _window(tmp_path)
    seed = module.Cache(str(tmp_path / "seed.db"))
    with closing(seed.connect()) as con, con:
        con.execute(
            "INSERT INTO lcsc_map (lcsc, puuid, status, source, fetched_at) VALUES ('C5', 'p5', 'ok', 'seed', 1)"
        )
    result = module.import_seed(window, seed.path)
    assert (result.parts, result.packages) == (1, 0)
    assert module.Cache(module.cache_path(str(tmp_path))).status("C5") == "ok"
    check = module.create_footprint_check(window, pcbnew)
    check.scan_board = MagicMock()
    window.jlc_footprint_check = check
    module.import_seed(window, seed.path)
    check.scan_board.assert_called_once_with()


# ---------------------------------------------------------------------------
# The main window
# ---------------------------------------------------------------------------


@pytest.fixture
def mainwindow():
    """Load mainwindow with a fake check factory that records what the window asks of it."""
    checks = []

    def create(window, pcbnew):
        check = MagicMock()
        check.generation = 1
        check.references_for.return_value = ["R1", "R2"]
        checks.append((window, pcbnew, check))
        return check

    module = load_mainwindow(
        _PACKAGE,
        wx=wx_stubs(Frame=type("Frame", (), {}), NewIdRef=lambda: 1),
        jlc_footprint_check={
            "create_footprint_check": create,
            "is_footprint_check_enabled": lambda settings: settings.get(
                "jlcfootprint", {}
            ).get("enabled", True),
            "show_generate_summary": lambda *_args, **_kwargs: "",
            "wait_for_pending_fetches": lambda *_args, **_kwargs: True,
        },
    )
    return module, checks


def _make_window(module):
    window = object.__new__(module.JLCPCBTools)
    window.settings = {}
    window.store = SimpleNamespace(dbfile="project.db")
    window.pcbnew = SimpleNamespace()
    window.logger = MagicMock()
    window.jlc_footprint_check = None
    window.save_settings = MagicMock()
    window.partlist_data_model = MagicMock()
    window.populate_footprint_list = MagicMock()
    return window


def test_window_starts_scans_stops_and_enqueues(mainwindow):
    """Starting creates the check for this window, starts it and scans; stopping joins it."""
    module, checks = mainwindow
    window = _make_window(module)
    window._start_jlc_footprint_check()
    ((owner, pcbnew, check),) = checks
    assert owner is window and pcbnew is window.pcbnew
    check.start.assert_called_once_with()
    check.scan_board.assert_called_once_with()
    assert window.jlc_footprint_check is check
    window._enqueue_jlc_footprint_check(["R1"])
    check.enqueue_references.assert_called_once_with(["R1"])
    window._stop_jlc_footprint_check()
    check.stop.assert_called_once_with()
    assert window.jlc_footprint_check is None
    window._enqueue_jlc_footprint_check(["R1"])
    check.enqueue_references.assert_called_once()


def test_window_skips_the_check_when_disabled_or_without_storage(mainwindow):
    """The setting off, or no project storage, means no check and no thread."""
    module, checks = mainwindow
    window = _make_window(module)
    window.settings = {"jlcfootprint": {"enabled": False}}
    window._start_jlc_footprint_check()
    assert checks == []
    window.settings = {}
    window.store = None
    window._start_jlc_footprint_check()
    assert checks == []
    assert window.jlc_footprint_check is None


def test_restarting_replaces_a_running_check(mainwindow):
    """A second start stops the first check before creating the next one."""
    module, checks = mainwindow
    window = _make_window(module)
    window._start_jlc_footprint_check()
    window._start_jlc_footprint_check()
    first, second = (check for _, _, check in checks)
    first.stop.assert_called_once_with()
    second.stop.assert_not_called()
    assert window.jlc_footprint_check is second


def test_result_events_from_a_superseded_board_load_are_dropped(mainwindow):
    """Only events carrying the check's current generation are logged."""
    module, checks = mainwindow
    window = _make_window(module)
    window._start_jlc_footprint_check()
    window.on_jlc_footprint_result(SimpleNamespace(lcsc="C1", generation=0))
    window.logger.debug.assert_not_called()
    window.on_jlc_footprint_result(SimpleNamespace(lcsc="C1", generation=1))
    window.logger.debug.assert_called_once()
    assert "R1, R2" in window.logger.debug.call_args.args[2]
    window.jlc_footprint_check = None
    window.on_jlc_footprint_result(SimpleNamespace(lcsc="C1", generation=1))
    window.logger.debug.assert_called_once()


def test_toggling_the_setting_starts_or_stops_the_check(mainwindow):
    """The settings event restarts the check when it turns on and stops it when it turns off."""
    module, checks = mainwindow
    window = _make_window(module)
    window.update_settings(
        SimpleNamespace(section="jlcfootprint", setting="enabled", value=True)
    )
    assert window.settings == {"jlcfootprint": {"enabled": True}}
    assert len(checks) == 1
    window.save_settings.assert_called_once_with()
    window.update_settings(
        SimpleNamespace(section="jlcfootprint", setting="enabled", value=False)
    )
    checks[0][2].stop.assert_called_once_with()
    assert window.jlc_footprint_check is None
    assert len(checks) == 1


def test_default_settings_ship_the_switch():
    """The shipped defaults carry the setting (test_settings_defaults checks every read)."""
    from .test_settings_defaults import shipped_defaults

    assert shipped_defaults()["jlcfootprint"] == {"enabled": True}


def test_pcm_keeps_the_cache_on_update():
    """The cache is user data like corrections.db and must survive a PCM update."""
    from .wx_harness import ROOT

    # The template holds unquoted placeholders the release script fills in, so it is
    # not JSON yet; the keep list is checked as text between its key and the next one.
    template = (ROOT / "PCM" / "metadata.template.json").read_text()
    keep_list = template.index('"keep_on_update"')
    versions = template.index('"versions"')
    cache = template.index("jlcpcb/jlcfootprint-cache")
    assert keep_list < cache < versions
