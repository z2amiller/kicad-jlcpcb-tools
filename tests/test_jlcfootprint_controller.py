"""Tests for the footprint check controller: scanning, the lookup and document flow, deciding."""

import logging
import threading

import pytest

from jlcfootprint.cache import Cache
from jlcfootprint.controller import Decision, FootprintCheck
from jlcfootprint.geometry import pad_hash
from jlcfootprint.kicad_adapter import BoardPart
from jlcfootprint.verdicts import PENDING, VerdictStore
from jlcfootprint.worker import FOOTPRINT, SYMBOL, FetchWorker
from scripts.kicad_library import footprints_available

from .jlcfootprint_support import (
    alias,
    controller_setup,
    led,
    recorded,
    sod323,
    sot23,
    tantalum,
)

pytestmark = pytest.mark.skipif(
    not footprints_available(), reason="KiCad library footprints unavailable"
)


@pytest.fixture
def setup(tmp_path):
    """Return a controller over a fresh cache and verdict store with a fake client."""
    return controller_setup(tmp_path)


def test_scan_queues_unknown_parts_and_the_worker_resolves_them(setup, caplog):
    """Nothing cached: one lookup, then footprints and symbols, then the verdicts and one event each."""
    check, board, events, messages, client = setup
    summary = check.scan_board()
    assert (
        summary.scanned,
        summary.without_lcsc,
        summary.enqueued,
        summary.pending,
    ) == (3, 1, 2, 2)
    assert summary.queued == {"lookups": 2, "footprints": 0, "symbols": 0}
    # Two codes: one chunk plus up to two documents each.
    assert "queued: 2 lookup(s), 0 footprint(s), 0 symbol(s), about 6 s" in str(summary)
    assert check.generation == 1
    assert check.pending_references() == ["D1", "Q1"]
    assert check.queue_estimate() == (2, 6.0)
    assert (
        check.verdicts.get("C2132", board["parts"][0].footprint_hash).status == PENDING
    )
    # Spec 16.3: a pending part's Rotation cell says what the CPL emits, not "…".
    assert check.display_text("Q1") == "raw"
    with caplog.at_level(logging.INFO, logger="jlcfootprint.controller"):
        assert check.worker.run_pending() == 5
    assert "lookup answered: 2 of 2 part(s) known to EasyEDA" in caplog.text
    assert client.lookups == [["C2132", "C2286"]]
    q1_puuid, d1_puuid = recorded("C2132").puuid, recorded("C2286").puuid
    assert client.documents == [
        (FOOTPRINT, q1_puuid),
        (FOOTPRINT, d1_puuid),
        (SYMBOL, "sym-C2132"),
        (SYMBOL, "sym-C2286"),
    ]
    assert events == [("C2132", 1), ("C2286", 1)]
    assert check.pending_references() == []
    assert check.queue_estimate() == (0, 0.0)
    q1 = check.verdicts.get("C2132", board["parts"][0].footprint_hash)
    assert (q1.status, q1.rotation, q1.method) == ("green", 180, "geometry")
    d1 = check.verdicts.get("C2286", board["parts"][1].footprint_hash)
    assert (d1.status, d1.rotation, d1.method, d1.polarity_light) == (
        "yellow",
        0,
        "polarity",
        "yellow",
    )
    part = check.cache.part("C2132")
    assert (part.source, part.polarity_source, part.record.symbol_uuid) == (
        "live",
        "symbol",
        "sym-C2132",
    )
    assert check.cache.needs("C2132") == set()
    assert check.display_text("Q1") == "180°"
    assert messages == []


def test_scan_resolves_from_the_cache_without_fetching(setup):
    """A cached part is resolved on the spot and never queued."""
    check, board, events, _, client = setup
    check.cache.store(recorded("C2132"), now=1)
    summary = check.scan_board()
    assert (summary.resolved_from_cache, summary.enqueued) == (1, 1)
    assert check.verdicts.get("C2132", board["parts"][0].footprint_hash).rotation == 180
    assert client.lookups == []
    assert check.pending_references() == ["D1"]


def test_resolved_parts_are_left_alone_on_the_next_scan(setup):
    """A stored verdict is not recomputed; a new generation still starts."""
    check, board, events, _, client = setup
    check.scan_board()
    check.worker.run_pending()
    summary = check.scan_board()
    assert (
        summary.already_resolved,
        summary.enqueued,
        summary.resolved_from_cache,
    ) == (2, 0, 0)
    assert check.generation == 2
    assert client.lookups == [["C2132", "C2286"]]


def test_assignment_rescans_only_the_given_references(setup):
    """Assigning an LCSC resolves that part (from the cache here) without a new generation."""
    check, board, events, _, client = setup
    check.scan_board()
    check.worker.run_pending()
    board["parts"][2] = sot23("R1", is_bottom=True)
    summary = check.enqueue_references(["R1"])
    # The same part on the same pad geometry shares Q1's verdict row, bottom or not.
    assert (summary.scanned, summary.already_resolved, summary.enqueued) == (1, 1, 0)
    assert check.generation == 1
    assert check.decision(board["parts"][2]).rotation == 180
    board["parts"].append(led("D2", lcsc="C2132"))
    summary = check.enqueue_references(["D2"])
    assert (summary.scanned, summary.resolved_from_cache, summary.enqueued) == (1, 1, 0)
    assert check.decision(board["parts"][3]).status != PENDING
    assert check.references_for("C2132") == ["D2", "Q1", "R1"]


def test_one_lookup_resolves_every_part_sharing_the_lcsc(setup):
    """Two footprints with one LCSC and different pad geometry get two verdict rows from one fetch."""
    check, board, events, _, client = setup
    other = sot23("Q2")
    other.pads = other.pads[:2]
    other.footprint_hash = pad_hash(other.pads)
    board["parts"].append(other)
    check.scan_board()
    check.worker.run_pending()
    assert client.lookups == [["C2132", "C2286"]]
    assert (
        check.verdicts.get("C2132", board["parts"][0].footprint_hash).status == "green"
    )
    assert check.verdicts.get("C2132", other.footprint_hash).status != PENDING
    assert events == [("C2132", 1), ("C2286", 1)]


def test_a_shared_footprint_is_fetched_once(setup):
    """Two parts with one footprint uuid cost one footprint document and a symbol each."""
    check, board, events, _, client = setup
    client.records["C1"] = alias("C1")
    client.records["C2"] = alias("C2")
    board["parts"] = [sot23("Q1", lcsc="C1"), sot23("Q2", lcsc="C2")]
    check.scan_board()
    check.worker.run_pending()
    puuid = recorded("C2132").puuid
    assert client.documents == [
        (FOOTPRINT, puuid),
        (SYMBOL, "sym-C1"),
        (SYMBOL, "sym-C2"),
    ]
    assert {check.decision(part).rotation for part in board["parts"]} == {180}
    assert events == [("C1", 1), ("C2", 1)]


def test_transient_failures_trip_the_breaker_and_leave_parts_pending(setup):
    """Three symbol failures in a row trip the breaker; those parts retry next session, the rest wait."""
    check, board, events, messages, client = setup
    for i in range(1, 5):
        client.records[f"C{i}"] = alias(f"C{i}")
        client.transient_symbols.add(f"sym-C{i}")
    board["parts"] = [sot23(f"Q{i}", lcsc=f"C{i}") for i in range(1, 5)]
    check.scan_board()
    check.worker.run_pending()
    assert client.lookups == [["C1", "C2", "C3", "C4"]]
    assert [kind for kind, _ in client.documents] == [FOOTPRINT, SYMBOL, SYMBOL, SYMBOL]
    assert check.worker.tripped
    assert len(messages) == 1
    assert check.pending_references() == ["Q4"]
    assert check.cache.status("C1") == "error"
    assert (
        check.verdicts.get("C1", board["parts"][0].footprint_hash).status == "unknown"
    )
    assert events == [("C1", 1), ("C2", 1), ("C3", 1)]


def test_a_failed_lookup_marks_its_parts_for_retry(setup, caplog):
    """A refused lookup leaves error rows and unknown verdicts that the next scan asks for again."""
    check, board, events, _, client = setup
    client.fail_lookups = True
    board["parts"] = [sot23("Q9", lcsc="C999")]
    check.scan_board()
    with caplog.at_level(logging.WARNING, logger="jlcfootprint.controller"):
        check.worker.run_pending()
    assert "lookup of 1 part(s) failed: HTTP 403 after retries" in caplog.text
    assert check.cache.status("C999") == "error"
    assert (
        check.verdicts.get("C999", board["parts"][0].footprint_hash).status == "unknown"
    )
    assert events == [("C999", 1)]
    assert check.pending_references() == []
    client.fail_lookups = False
    client.records["C999"] = alias("C999")
    summary = check.scan_board()
    assert (summary.already_resolved, summary.enqueued) == (0, 1)
    check.worker.run_pending()
    assert (
        check.verdicts.get("C999", board["parts"][0].footprint_hash).status == "green"
    )
    summary = check.scan_board()
    assert (summary.already_resolved, summary.enqueued) == (1, 0)


def test_lookup_misses_and_missing_documents(setup):
    """A miss is the checkerboard case; a silent symbol leaves the token or the geometry to decide."""
    check, board, events, _, client = setup
    client.records["C16133"] = recorded("C16133")
    client.records["C5"] = alias("C5", "C2286")
    client.silent_symbols.update({"sym-C16133", "sym-C5", "sym-C2132"})
    board["parts"] = [
        sot23("Q1"),
        tantalum("C6"),
        led("D5", lcsc="C5"),
        led("D6", lcsc="C404"),
    ]
    check.scan_board()
    check.worker.run_pending()
    assert check.cache.status("C404") == "none"
    d6 = check.verdicts.get("C404", board["parts"][3].footprint_hash)
    assert d6.status == "unknown" and "no JLC footprint data" in d6.notes
    # No token in the name and no symbol: the footprint's own + mark decides (spec 16.6).
    c6 = check.verdicts.get("C16133", board["parts"][1].footprint_hash)
    assert (c6.status, c6.rotation) == ("green", 0)
    assert "polarity from the footprint's + mark" in c6.notes
    # The RD token orients the LED on its own and says JLC's pad 1 is the anode,
    # where KiCad's pad 1 is the cathode: the pin-1 marker warning.
    d5 = check.verdicts.get("C5", board["parts"][2].footprint_hash)
    assert (d5.status, d5.rotation, d5.polarity_light) == ("yellow", 0, "yellow")
    q1 = check.verdicts.get("C2132", board["parts"][0].footprint_hash)
    assert (q1.status, q1.rotation) == ("green", 180)
    assert all(check.cache.needs(code) == set() for code in ("C2132", "C16133", "C5"))
    assert sorted(events) == [("C16133", 1), ("C2132", 1), ("C404", 1), ("C5", 1)]
    assert check.pending_references() == []


def test_a_final_document_error_is_retried_next_session_not_stored(setup):
    """HTTP 401 on a footprint or a symbol leaves an error row: never none, never an empty pin list."""
    check, board, events, _, client = setup
    client.records["C1"] = alias("C1")
    client.records["C2"] = alias("C2", "C2286")
    client.error_footprints.add(recorded("C2132").puuid)
    client.error_symbols.add("sym-C2")
    board["parts"] = [sot23("Q1", lcsc="C1"), led("D2", lcsc="C2")]
    check.scan_board()
    check.worker.run_pending()
    assert (check.cache.status("C1"), check.cache.status("C2")) == ("error", "error")
    assert check.cache.needs("C1") == check.cache.needs("C2") == {"lookup"}
    for part in board["parts"]:
        verdict = check.verdicts.get(part.lcsc, part.footprint_hash)
        assert (verdict.status, "will retry" in verdict.notes) == ("unknown", True)
    # A final failure is not the breaker's business.
    assert (check.worker.consecutive_failures, check.worker.tripped) == (0, False)
    assert check.pending_references() == []
    assert sorted(events) == [("C1", 1), ("C2", 1)]
    client.error_footprints.clear()
    client.error_symbols.clear()
    assert check.scan_board().enqueued == 2
    check.worker.run_pending()
    assert check.verdicts.get("C1", board["parts"][0].footprint_hash).status == "green"
    assert check.verdicts.get("C2", board["parts"][1].footprint_hash).status == "yellow"
    assert check.waiting == {}


def test_a_scan_right_after_a_result_never_parks_the_part(setup):
    """A scan landing between a lookup's answer and the next job re-requests the part and it resolves."""
    check, board, events, _, client = setup
    client.fail_lookups = True
    board["parts"] = [sot23("Q9", lcsc="C999")]
    handle = check._on_lookup

    def on_lookup(codes, lookup):
        handle(codes, lookup)
        check.worker.on_lookup = (
            handle  # once: only the failed lookup is followed by a scan
        )
        # Generate's scan arriving right after the failed lookup was handled.
        client.fail_lookups = False
        client.records["C999"] = alias("C999")
        check.scan_board()

    check.worker.on_lookup = on_lookup
    check.scan_board()
    assert (
        check.worker.run_pending() == 4
    )  # the failed lookup, its retry, two documents
    assert client.lookups == [["C999"], ["C999"]]
    assert check.waiting == {}
    assert check.pending_references() == []
    assert (
        check.verdicts.get("C999", board["parts"][0].footprint_hash).status == "green"
    )
    assert events == [("C999", 1), ("C999", 2)]


def test_the_scan_step_and_the_handlers_share_one_lock(setup):
    """A pending query from another thread waits while the lock is held."""
    check, *_ = setup
    check.scan_board()
    seen = []
    with check.lock:
        thread = threading.Thread(target=lambda: seen.append(check.pending_lcscs()))
        thread.start()
        thread.join(0.2)
        assert thread.is_alive() and seen == []
    thread.join(5.0)
    assert seen == [{"C2132", "C2286"}]


def test_decisions_follow_the_cpl_precedence(setup):
    """Override beats a derived value, which beats the raw angle; pending and no-LCSC parts are raw."""
    check, board, events, _, client = setup
    check.scan_board()
    check.worker.run_pending()
    q1 = board["parts"][0]
    check.verdicts.set_override("C2132", q1.footprint_hash, 90, "seen in the preview")
    board["parts"].append(sot23("Q9", lcsc="C999"))
    check.enqueue_references(["Q9"])
    decisions = check.decisions()
    assert decisions["Q1"].source == "override"
    assert (
        decisions["Q1"].rotation,
        decisions["Q1"].note,
        decisions["Q1"].display,
    ) == (90, "seen in the preview", "90° set")
    assert (
        decisions["D1"].source,
        decisions["D1"].rotation,
        decisions["D1"].status,
    ) == ("derived", 0, "yellow")
    assert decisions["D1"].display == "0° !"
    assert decisions["R1"] == Decision(
        "R1", "", status="no-lcsc", note="no LCSC number"
    )
    assert decisions["R1"].display == "raw"
    assert (
        decisions["Q9"].pending,
        decisions["Q9"].rotation,
        decisions["Q9"].display,
    ) == (True, None, "raw")
    assert check.display_text("nope") == ""


def test_decisions_reread_the_board(setup):
    """Generate time reads the live board so a footprint changed since the scan is judged afresh."""
    check, board, events, _, client = setup
    check.scan_board()
    check.worker.run_pending()
    changed = sot23("Q1")
    changed.pads = changed.pads[:2]
    changed.footprint_hash = pad_hash(changed.pads)
    board["parts"][0] = changed
    decisions = check.decisions()
    assert decisions["Q1"].status == "no-verdict"
    assert decisions["Q1"].rotation is None
    assert decisions["Q1"].display == "raw"
    stale = check.decisions(reread=False)
    assert stale["Q1"].status == "no-verdict"


def test_client_shares_the_worker_pacing_and_stop(tmp_path):
    """The real client is wired to the worker's buckets and stop event."""
    check = FootprintCheck(
        Cache(str(tmp_path / "c.db")),
        VerdictStore(str(tmp_path / "p.db")),
        read_board=list,
        post=lambda *_: None,
    )
    assert isinstance(check.worker, FetchWorker)
    assert check.client.acquire == check.worker.acquire
    assert check.client.wait == check.worker.wait_for
    check.start()
    check.stop()
    assert not check.worker.is_running()


def test_swapped_pin_functions_get_their_own_verdicts(setup):
    """Two footprints with the same pads but K/A swapped are keyed apart and rotate 180 apart."""
    check, board, events, _, client = setup
    d4 = sod323("D4", {"1": "K", "2": "A"})
    d5 = sod323("D5", {"1": "A", "2": "K"})
    assert d4.footprint_hash != d5.footprint_hash
    board["parts"] = [d4, d5]
    check.scan_board()
    check.worker.run_pending()
    assert client.lookups == [["C7502694"]]
    assert len(client.documents) == 2
    first = check.verdicts.get("C7502694", d4.footprint_hash)
    second = check.verdicts.get("C7502694", d5.footprint_hash)
    # One of them has JLC's pin-1 marker on the other terminal (yellow), never a wrong angle.
    assert {first.status, second.status} <= {"green", "yellow"}
    assert (second.rotation - first.rotation) % 360 == 180
    decisions = check.decisions()
    assert (decisions["D5"].rotation - decisions["D4"].rotation) % 360 == 180


def test_pending_is_per_part_and_in_flight_parts_are_not_marked_again(setup):
    """A resolved part is not pending for its LCSC's other geometry; a queued LCSC resolves new parts too."""
    check, board, events, _, client = setup
    q1 = board["parts"][0]
    check.scan_board()
    other = sot23("Q2")
    other.pads = other.pads[:2]
    other.footprint_hash = pad_hash(other.pads)
    board["parts"].append(other)
    summary = check.enqueue_references(["Q2"])
    assert (summary.enqueued, summary.pending) == (0, 2)
    assert check.verdicts.get("C2132", other.footprint_hash) is None
    assert check.decision(other).pending
    check.verdicts.save(
        "C2132",
        q1.footprint_hash,
        q1.footprint_name,
        "p",
        check.resolve_part(q1, _cached(check, "C2132")),
        5,
    )
    assert not check.decision(q1).pending
    assert check.decision(other).pending
    check.worker.run_pending()
    assert check.verdicts.get("C2132", other.footprint_hash).status != PENDING
    assert not check.decision(other).pending


def _cached(check, lcsc):
    """Store the recorded part in the cache and return it as the controller reads it."""
    check.cache.store(recorded(lcsc), now=1)
    return check.cache.part(lcsc)


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
    """Spec 16.5: every cache row goes, the board is rescanned, a seed can refill it."""
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
