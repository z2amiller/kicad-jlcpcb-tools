"""Tests for the plugin-side wiring of the JLC footprint check: facade, main window, settings."""

from contextlib import closing
import dataclasses
import logging
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
                fields={"LCSC": "C1"},
            ),
            FakeFootprint(
                "R1", [FakePad("1", -1, 0), FakePad("2", 1, 0)], fields={"LCSC": "C77"}
            ),
        ]
    )
    pcbnew = SimpleNamespace(
        GetBoard=lambda: board, ToMM=lambda v: v, PAD_ATTRIB_NPTH=3, PAD_SHAPE_CUSTOM=6
    )
    with closing(sqlite3.connect(tmp_path / "project.db")) as con:
        con.execute("CREATE TABLE part_info (reference TEXT)")
    stored = {"Q1": {"lcsc": "C2132"}}
    window = SimpleNamespace(
        library=SimpleNamespace(datadir=str(tmp_path)),
        store=SimpleNamespace(dbfile=str(tmp_path / "project.db"), get_part=stored.get),
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
    # The project database's LCSC wins over the footprint field; the field is the fallback.
    assert [(p.reference, p.lcsc, len(p.pads)) for p in parts] == [
        ("Q1", "C2132", 3),
        ("R1", "C77", 2),
    ]
    assert check.worker.buckets is module.shared_buckets()
    assert (
        module.create_footprint_check(window, pcbnew).worker.buckets
        is check.worker.buckets
    )
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
            "clear_cache": lambda *_args, **_kwargs: 0,
            "create_footprint_check": create,
            "is_footprint_check_enabled": lambda settings: settings.get(
                "jlcfootprint", {}
            ).get("enabled", True),
            "recheck_board": lambda *_args, **_kwargs: 0,
            "refetch_references": lambda *_args, **_kwargs: 0,
            "refresh_board_data": lambda *_args, **_kwargs: 0,
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


# ---------------------------------------------------------------------------
# The four actions of spec 16.5, against a real cache and verdict store
# ---------------------------------------------------------------------------


@pytest.fixture
def actions(facade, tmp_path):
    """Return the facade, a real check over a temporary cache and project.db, and answers."""
    module, wx, _events = facade
    window, pcbnew = _window(tmp_path)
    check = module.create_footprint_check(window, pcbnew)
    window.jlc_footprint_check = check
    answers = {"yes": True, "asked": []}

    class Dialog:
        """Record the question and answer it the way the test asked."""

        def __init__(self, _parent, question, _title, _style):
            answers["asked"].append(question)

        def ShowModal(self):
            """Return the id the test chose."""
            return wx.ID_YES if answers["yes"] else wx.ID_NO

        def Destroy(self):
            """Accept the destroy."""

    wx.MessageDialog = Dialog
    wx.ID_YES = 5103
    wx.ID_NO = 5104
    return SimpleNamespace(
        module=module, window=window, check=check, answers=answers, tmp_path=tmp_path
    )


def _cache_a_part(actions, lcsc="C2132"):
    """Store one recorded part in the check's cache so it needs no network."""
    from .jlcfootprint_support import recorded

    record = recorded(lcsc)
    record.lcsc = lcsc
    actions.check.cache.store(record, now=1_757_000_000)
    return record


def test_rechecking_the_board_resolves_cached_parts_without_the_network(
    actions, caplog
):
    """Spec 16.5: the resolver runs again from the cache, no request, one log line."""
    _cache_a_part(actions)
    parts = actions.check.read_board()
    assert actions.check.verdicts.all() == []
    with caplog.at_level(logging.INFO):
        checked = actions.module.recheck_board(actions.window)
    assert checked == 1  # Q1 is cached; R1's C77 is not
    assert "re-checked 1 part(s)" in caplog.text
    (stored,) = actions.check.verdicts.all()
    assert (stored.lcsc, stored.footprint_hash) == ("C2132", parts[0].footprint_hash)
    assert actions.check.worker.counts() == {
        "lookups": 0,
        "footprints": 0,
        "symbols": 0,
    }
    assert actions.answers["asked"] == []  # no confirmation for a local re-check


def test_refetching_selected_parts_forgets_their_rows_and_queues_them(actions, caplog):
    """Spec 16.5: forget, mark pending (override kept), enqueue; one log line."""
    _cache_a_part(actions)
    actions.check.scan_board()
    stored = actions.check.set_override("Q1", 90, "by hand")
    assert stored.override_rotation == 90
    with caplog.at_level(logging.INFO):
        enqueued = actions.module.refetch_references(actions.window, ["Q1"])
    assert enqueued == 1
    assert "re-fetching 1 part(s): Q1" in caplog.text
    assert actions.check.cache.status("C2132") is None
    row = actions.check.verdicts.get("C2132", actions.check.parts["Q1"].footprint_hash)
    assert (row.status, row.override_rotation) == ("pending", 90)
    assert "C2132" in actions.check.worker.pending_lookups()


def test_refreshing_the_board_confirms_with_the_estimate_and_keeps_overrides(
    actions, caplog
):
    """Spec 16.5: the confirmation quotes the parts and the time; No changes nothing."""
    _cache_a_part(actions)
    actions.check.scan_board()
    actions.check.set_override("Q1", 180, "keep me")
    actions.answers["yes"] = False
    assert actions.module.refresh_board_data(actions.window) == 0
    assert "part(s), about" in actions.answers["asked"][0]
    assert "2 part(s)" in actions.answers["asked"][0]  # C2132 and C77
    assert "overrides are kept" in actions.answers["asked"][0]
    assert actions.check.cache.status("C2132") == "ok"
    actions.answers["yes"] = True
    with caplog.at_level(logging.INFO):
        assert actions.module.refresh_board_data(actions.window) == 2
    assert "refreshing 2 part(s)" in caplog.text
    assert actions.check.cache.status("C2132") is None
    row = actions.check.verdicts.get("C2132", actions.check.parts["Q1"].footprint_hash)
    assert (row.status, row.override_rotation) == ("pending", 180)


def test_clearing_the_cache_confirms_and_deletes_every_row(actions, caplog):
    """Spec 16.5: the whole cache goes (schema kept), the board is rescanned."""
    _cache_a_part(actions)
    assert actions.check.cache.counts() == {"parts": 1, "packages": 1}
    actions.answers["yes"] = False
    assert actions.module.clear_cache(actions.window) == 0
    assert (
        "Delete the whole EasyEDA cache (1 part(s), 1 footprint(s))?"
        in (actions.answers["asked"][0])
    )
    assert "seed file" in actions.answers["asked"][0]
    assert actions.check.cache.counts() == {"parts": 1, "packages": 1}
    actions.answers["yes"] = True
    with caplog.at_level(logging.INFO):
        assert actions.module.clear_cache(actions.window) == 1
    assert "cleared the cache (1 part(s), 1 footprint(s))" in caplog.text
    assert actions.check.cache.counts() == {"parts": 0, "packages": 0}
    # The schema survives, so the next scan can write again.
    actions.check.cache.store_lookup_miss("C77", now=1)
    assert actions.check.cache.status("C77") == "none"


def test_a_board_with_no_part_numbers_is_not_worth_confirming(actions, caplog):
    """Refresh says so in the log instead of asking about nothing."""
    actions.window.store.get_part = lambda _reference: {}
    board = actions.check.read_board
    actions.check.read_board = lambda: [
        dataclasses.replace(part, lcsc="") for part in board()
    ]
    with caplog.at_level(logging.INFO):
        assert actions.module.refresh_board_data(actions.window) == 0
    assert "no part on the board carries an LCSC number" in caplog.text
    assert actions.answers["asked"] == []


def test_the_actions_do_nothing_without_a_running_check(actions):
    """Every action is a no-op when the check is not running (the setting off, no storage)."""
    module = actions.module
    empty = SimpleNamespace(jlc_footprint_check=None)
    assert module.recheck_board(empty) == 0
    assert module.refresh_board_data(empty) == 0
    assert module.clear_cache(empty) == 0
    assert module.refetch_references(empty, ["Q1"]) == 0
    assert actions.answers["asked"] == []


def test_the_window_menu_handlers_call_the_actions_and_repaint(mainwindow):
    """The four menu entries act through the facade and repaint what changed."""
    module, checks = mainwindow
    window = _make_window(module)
    window._start_jlc_footprint_check()
    ((_owner, _pcbnew, check),) = checks
    check.glyph_state.return_value = "pending"
    check.display_text.return_value = "raw"
    model = window.partlist_data_model
    model.columns = {"REF_COL": 0}
    model.get_all.return_value = [["Q1"], ["R1"]]
    items = [SimpleNamespace(name="Q1"), SimpleNamespace(name="R1")]
    model.get_reference.side_effect = lambda item: item.name
    model.get_lcsc.side_effect = lambda item: "C1" if item.name == "Q1" else ""
    window.footprint_list = MagicMock()
    window.footprint_list.GetSelections.return_value = items
    window.on_jlc_footprint_refetch()
    assert model.set_jlc_state.call_args_list == [(("Q1", "pending"),)]
    model.reset_mock()
    window.on_jlc_footprint_recheck()
    assert model.set_jlc_state.call_args_list == [
        (("Q1", "pending"),),
        (("R1", "pending"),),
    ]
    model.reset_mock()
    window.on_jlc_footprint_clear_cache()
    assert model.set_jlc_state.call_count == 2
    window.jlc_footprint_check = None
    model.reset_mock()
    for handler in (
        window.on_jlc_footprint_refetch,
        window.on_jlc_footprint_recheck,
        window.on_jlc_footprint_refresh,
        window.on_jlc_footprint_clear_cache,
    ):
        handler()
    model.set_jlc_state.assert_not_called()
