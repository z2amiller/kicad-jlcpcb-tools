"""Tests for the JLC footprint check's actions: the facade's four actions and the window's menu handlers."""

import dataclasses
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .test_jlc_footprint_check_wiring import (
    _make_window,
    _window,
    facade as wiring_facade,
    mainwindow as wiring_mainwindow,
)

facade = wiring_facade
mainwindow = wiring_mainwindow


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
    parts, _seconds = actions.check.board_estimate()
    assert parts == 2  # the confirmation's own count matches the board: C2132 and C77
    assert (
        "This board's parts are fetched again: 2 part(s), about 6 s. Rotation "
        "overrides are kept." in actions.answers["asked"][0]
    )
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


def test_the_window_menu_handlers_call_the_actions_and_repaint(mainwindow, monkeypatch):
    """The four menu entries act through the facade and repaint what changed."""
    module, checks = mainwindow
    window = _make_window(module)
    window._start_jlc_footprint_check()
    ((_owner, _pcbnew, check),) = checks
    monkeypatch.setattr(
        module.jlc_footprint_window, "glyph_state", lambda check, reference: "pending"
    )
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
    # Refresh and Clear cache only repaint when the facade reports a Yes: both are
    # confirmations, and a stubbed 0 stands in for "No" without a real dialog.
    model.reset_mock()
    module.jlc_footprint_window.refresh_jlc_footprint_board_data = (
        lambda *_args, **_kwargs: 2
    )
    window.on_jlc_footprint_refresh()
    assert model.set_jlc_state.call_args_list == [
        (("Q1", "pending"),),
        (("R1", "pending"),),
    ]
    model.reset_mock()
    module.jlc_footprint_window.refresh_jlc_footprint_board_data = (
        lambda *_args, **_kwargs: 0
    )
    window.on_jlc_footprint_refresh()
    model.set_jlc_state.assert_not_called()

    model.reset_mock()
    module.jlc_footprint_window.clear_jlc_footprint_cache = lambda *_args, **_kwargs: 0
    window.on_jlc_footprint_clear_cache()
    model.set_jlc_state.assert_not_called()
    model.reset_mock()
    module.jlc_footprint_window.clear_jlc_footprint_cache = lambda *_args, **_kwargs: 3
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
