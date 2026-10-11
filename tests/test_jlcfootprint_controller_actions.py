"""Tests for the footprint check controller's actions: the package name, overrides, re-fetch, recheck, refresh, clear."""

import threading

import pytest

from jlcfootprint.kicad_adapter import BoardPart
from jlcfootprint.verdicts import PENDING
from tools.jlcfootprint.kicad_library import footprints_available

from .jlcfootprint_controller_support import controller_setup
from .jlcfootprint_fetch_support import sot23
from .jlcfootprint_support import recorded

pytestmark = pytest.mark.skipif(
    not footprints_available(), reason="KiCad library footprints unavailable"
)


@pytest.fixture
def setup(tmp_path):
    """Return a controller over a fresh cache and verdict store with a fake client."""
    return controller_setup(tmp_path)


# ---------------------------------------------------------------------------
# The package name memo (spec 16.3)
# ---------------------------------------------------------------------------


def test_the_package_name_is_read_from_the_cache_once_per_part(setup):
    """The hover names the JLC package, which the verdict row does not carry."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    assert check.package_name("") == ""
    assert check.package_name("C2132") == recorded("C2132").package_name
    check.cache.forget("C2132")
    # Memoised: a forgotten row does not change the name the hover already shows.
    assert check.package_name("C2132") == recorded("C2132").package_name
    # A part with no footprint yet is asked again, so the name appears when it lands.
    assert check.package_name("C0000") == ""
    assert "C0000" not in check._package_names


def test_the_package_name_memo_reads_the_cache_only_once_per_part(setup):
    """A second call for the same LCSC must not touch the cache again."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    calls: list = []
    original_part = check.cache.part
    check.cache.part = lambda lcsc: calls.append(lcsc) or original_part(lcsc)
    assert check.package_name("C2132") == recorded("C2132").package_name
    assert check.package_name("C2132") == recorded("C2132").package_name
    assert calls == ["C2132"]


def test_a_name_that_lands_after_an_empty_read_is_picked_up_next_time(setup):
    """A part with no footprint yet has no name to remember; the next call rereads it."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()  # Q1 (C2132) is only queued: nothing is cached yet.
    assert check.package_name("C2132") == ""
    assert "C2132" not in check._package_names
    check.worker.run_pending()  # The footprint has landed now.
    assert check.package_name("C2132") == recorded("C2132").package_name
    assert check._package_names["C2132"] == recorded("C2132").package_name


def test_the_three_actions_forget_the_memoised_package_name(setup):
    """Re-fetch, Refresh board and Clear cache must each drop the memoised name too.

    Priming the memo first is what test_clearing_the_cache_empties_it_but_keeps_the
    _schema missed: with nothing memoised beforehand, dropping the memo's own
    ``.pop``/``.clear()`` call has nothing to undo and the test cannot see it.
    """
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()

    assert check.package_name("C2132") == recorded("C2132").package_name
    check.refetch(["Q1"])
    assert check.package_name("C2132") == ""  # not the stale memoised name

    check.worker.run_pending()
    assert check.package_name("C2132") == recorded("C2132").package_name
    check.refresh_board()
    assert check.package_name("C2132") == ""

    check.worker.run_pending()
    assert check.package_name("C2132") == recorded("C2132").package_name
    check.clear_cache()
    assert check.package_name("C2132") == ""


# ---------------------------------------------------------------------------
# The override, the actions and the board estimate (spec 16.5)
# ---------------------------------------------------------------------------


def test_an_override_on_a_part_with_no_verdict_row_yet_creates_one(setup):
    """A part whose data never arrived can still be overridden (the row is minted pending)."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    part = board["parts"][0]
    check.verdicts.delete("C2132", part.footprint_hash)
    stored = check.set_override("Q1", 90, "by hand")
    assert (stored.status, stored.override_rotation) == (PENDING, 90)
    assert check.display_text("Q1") == "90° set"


def test_set_override_runs_under_the_controllers_lock(setup):
    """The read-mark-write-read sequence is atomic with the worker's result handlers.

    Without the lock, a `save` landing between the first read and `mark_pending`
    would be overwritten back to pending until the next result lands.
    """
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    real_set_override = check.verdicts.set_override
    lock_held = []

    def spy(*args, **kwargs):
        def probe():
            got = check.lock.acquire(blocking=False)
            lock_held.append(got)
            if got:
                check.lock.release()

        thread = threading.Thread(target=probe)
        thread.start()
        thread.join(5.0)
        return real_set_override(*args, **kwargs)

    check.verdicts.set_override = spy
    check.set_override("Q1", 270, "note")
    assert lock_held == [False]  # another thread could not acquire it meanwhile


def test_two_placements_of_one_part_share_the_verdict_and_the_repaint(setup):
    """An override set on one reference repaints every reference on that row (spec 5.3)."""
    check, board, _events, _messages, _client = setup
    board["parts"] = [*board["parts"], sot23("Q2"), sot23("Q3", lcsc="C2286")]
    check.scan_board()
    assert check.references_sharing_verdict("Q1") == ["Q1", "Q2"]
    assert check.references_sharing_verdict("Q3") == ["Q3"]
    assert check.references_sharing_verdict("R1") == ["R1"]
    assert check.references_sharing_verdict("nope") == []


def test_the_board_estimate_counts_distinct_parts_and_their_documents(setup):
    """The confirmation's numbers: one lookup per 200 codes and two documents each."""
    check, board, _events, _messages, _client = setup
    parts, seconds = check.board_estimate()
    assert parts == 2  # C2132 and C2286; R1 has no LCSC
    assert seconds > 0
    board["parts"] = [*board["parts"], sot23("Q2")]  # the same part number again
    assert check.board_estimate()[0] == 2
    board["parts"] = [BoardPart("R1", "", "R", False, 0.0, [], "")]
    assert check.board_estimate() == (0, 0.0)


def test_the_board_estimate_is_the_number_the_queue_itself_quotes(setup):
    """The confirmation and the generate-time wait dialog quote one estimate, not two."""
    check, _board, _events, _messages, _client = setup
    parts, seconds = check.board_estimate()
    summary = check.refresh_board()
    # The same codes, now really queued: the number the question promised is the
    # number the queue reports, rather than twice its documents.
    assert (parts, seconds) == (summary.enqueued, summary.estimate_s)


def test_rechecking_the_board_resolves_from_the_cache_without_any_request(setup):
    """Spec 16.5: the resolver runs again on cached data, no lookup, no document."""
    check, board, _events, _messages, client = setup
    check.scan_board()
    check.worker.run_pending()
    calls = (len(client.lookups), len(client.documents))
    check.verdicts.delete("C2132", board["parts"][0].footprint_hash)
    assert check.recheck_board() == 2
    assert (len(client.lookups), len(client.documents)) == calls
    assert check.verdicts.get("C2132", board["parts"][0].footprint_hash) is not None
    # A part the cache does not know is skipped rather than fetched.
    check.cache.forget("C2132")
    assert check.recheck_board() == 1
    assert (len(client.lookups), len(client.documents)) == calls


def test_rechecking_skips_a_part_whose_cache_row_is_incomplete(setup):
    """A row with uuids but no documents is not "no data": it waits, it is not judged."""
    check, board, _events, _messages, _client = setup
    part = board["parts"][0]
    check.cache.store_lookup("C2132", "sym-C2132", "puuid-not-fetched", 1)
    check.verdicts.mark_pending("C2132", part.footprint_hash, part.footprint_name, 1)
    assert check.cache.needs("C2132", 2) == {"footprint", "symbol"}
    assert check.recheck_board() == 0
    assert check.verdicts.get("C2132", part.footprint_hash).status == PENDING


def test_refreshing_the_board_forgets_every_row_and_rescans(setup):
    """Spec 16.5: every LCSC on the board is fetched again, overrides kept."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    check.set_override("Q1", 90, "keep me")
    summary = check.refresh_board()
    assert summary.enqueued == 2
    assert check.cache.status("C2132") is None and check.cache.status("C2286") is None
    row = check.verdicts.get("C2132", board["parts"][0].footprint_hash)
    assert (row.status, row.override_rotation) == (PENDING, 90)


def test_clearing_the_cache_empties_it_but_keeps_the_schema(setup):
    """Spec 16.5: every cache row goes, the board is rescanned and refilled."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    assert check.cache.counts()["parts"] >= 2
    summary = check.clear_cache()
    assert check.cache.counts() == {"parts": 0, "packages": 0}
    assert summary.enqueued == 2
    assert check.package_name("C2132") == ""
    check.cache.store_lookup_miss("C99", now=1)
    assert check.cache.status("C99") == "none"
