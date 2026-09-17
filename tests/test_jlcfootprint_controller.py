"""Tests for the footprint check controller: scanning, fetching, storing, deciding."""

import pytest

from jlcfootprint.cache import Cache
from jlcfootprint.controller import Decision, FootprintCheck
from jlcfootprint.easyeda_client import Fetched
from jlcfootprint.easyeda_parse import ComponentRecord
from jlcfootprint.geometry import pad_hash
from jlcfootprint.kicad_adapter import BoardPart
from jlcfootprint.verdicts import PENDING, VerdictStore
from jlcfootprint.worker import FetchWorker

from .jlcfootprint_support import (
    footprints_available,
    library_pads,
    recorded,
    with_functions,
)
from .test_jlcfootprint_worker import Clock

pytestmark = pytest.mark.skipif(
    not footprints_available(), reason="KiCad library footprints unavailable"
)


class FakeClient:
    """Answers from recorded responses; counts the fetches."""

    def __init__(self, records=None):
        self.records = records or {}
        self.fetched = []

    def fetch_component(self, lcsc):
        """Return the recorded part or an error."""
        self.fetched.append(lcsc)
        record = self.records.get(lcsc)
        if record is None:
            return Fetched(
                ComponentRecord(lcsc=lcsc, status="error", error="HTTP 403"),
                transient=True,
            )
        return Fetched(record)


def sot23(reference, lcsc="C2132", is_bottom=False):
    """Return a SOT-23 board part from KiCad's library."""
    pads = with_functions(
        library_pads("Package_TO_SOT_SMD", "SOT-23"), {"1": "B", "2": "E", "3": "C"}
    )
    return BoardPart(
        reference,
        lcsc,
        "Package_TO_SOT_SMD:SOT-23",
        is_bottom,
        0.0,
        pads,
        pad_hash(pads),
    )


def led(reference, lcsc="C2286"):
    """Return an 0603 LED board part with K on pad 1."""
    pads = with_functions(
        library_pads("LED_SMD", "LED_0603_1608Metric"), {"1": "K", "2": "A"}
    )
    return BoardPart(
        reference, lcsc, "LED_SMD:LED_0603_1608Metric", False, 0.0, pads, pad_hash(pads)
    )


@pytest.fixture
def setup(tmp_path):
    """Return a controller over a fresh cache and verdict store with a fake client."""
    board = {
        "parts": [sot23("Q1"), led("D1"), BoardPart("R1", "", "R", False, 0.0, [], "")]
    }
    events = []
    messages = []
    cache = Cache(str(tmp_path / "cache.db"))
    verdicts = VerdictStore(str(tmp_path / "project.db"))
    client = FakeClient({"C2132": recorded("C2132"), "C2286": recorded("C2286")})
    clock = Clock()
    check = FootprintCheck(
        cache,
        verdicts,
        read_board=lambda: list(board["parts"]),
        post=lambda lcsc, generation: events.append((lcsc, generation)),
        client=client,
        worker=None,
        message=messages.append,
        now=clock,
    )
    check.worker.bucket.wait = clock.wait
    return check, board, events, messages, client


def test_scan_queues_unknown_parts_and_the_worker_resolves_them(setup):
    """Nothing cached: parts go pending, the fetch stores the record and the verdicts, one event each."""
    check, board, events, messages, client = setup
    summary = check.scan_board()
    assert (
        summary.scanned,
        summary.without_lcsc,
        summary.enqueued,
        summary.pending,
    ) == (3, 1, 2, 2)
    assert check.generation == 1
    assert check.pending_references() == ["D1", "Q1"]
    assert (
        check.verdicts.get("C2132", board["parts"][0].footprint_hash).status == PENDING
    )
    assert check.display_text("Q1") == "…"
    assert check.worker.run_pending() == 2
    assert client.fetched == ["C2132", "C2286"]
    assert events == [("C2132", 1), ("C2286", 1)]
    assert check.pending_references() == []
    q1 = check.verdicts.get("C2132", board["parts"][0].footprint_hash)
    assert (q1.status, q1.rotation, q1.method) == ("green", 180, "geometry")
    d1 = check.verdicts.get("C2286", board["parts"][1].footprint_hash)
    assert (d1.status, d1.rotation, d1.method, d1.polarity_light) == (
        "yellow",
        0,
        "polarity",
        "yellow",
    )
    assert check.cache.part("C2132").source == "live"
    assert check.display_text("Q1") == "180°"
    assert messages == []


def test_scan_resolves_from_the_cache_without_fetching(setup):
    """A cached part is resolved on the spot and never queued."""
    check, board, events, _, client = setup
    check.cache.store(recorded("C2132"), now=1)
    summary = check.scan_board()
    assert (summary.resolved_from_cache, summary.enqueued) == (1, 1)
    assert check.verdicts.get("C2132", board["parts"][0].footprint_hash).rotation == 180
    assert client.fetched == []
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
    assert client.fetched == ["C2132", "C2286"]


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


def test_one_fetch_resolves_every_part_sharing_the_lcsc(setup):
    """Two footprints with one LCSC and different pad geometry get two verdict rows from one fetch."""
    check, board, events, _, client = setup
    other = sot23("Q2")
    other.pads = other.pads[:2]
    other.footprint_hash = pad_hash(other.pads)
    board["parts"].append(other)
    check.scan_board()
    check.worker.run_pending()
    assert client.fetched == ["C2132", "C2286"]
    assert (
        check.verdicts.get("C2132", board["parts"][0].footprint_hash).status == "green"
    )
    assert check.verdicts.get("C2132", other.footprint_hash).status != PENDING
    assert events == [("C2132", 1), ("C2286", 1)]


def test_transient_failures_trip_the_breaker_and_leave_parts_pending(setup):
    """Unknown parts fail transiently; after three the message is forwarded and the rest wait."""
    check, board, events, messages, client = setup
    board["parts"] = [sot23(f"Q{i}", lcsc=f"C{i}") for i in range(1, 6)]
    check.scan_board()
    check.worker.run_pending()
    assert client.fetched == ["C1", "C2", "C3"]
    assert check.worker.tripped
    assert len(messages) == 1
    assert check.pending_references() == ["Q4", "Q5"]
    assert check.cache.status("C1") == "error"
    assert (
        check.verdicts.get("C1", board["parts"][0].footprint_hash).status == "unknown"
    )
    assert events == [("C1", 1), ("C2", 1), ("C3", 1)]


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
    ) == (True, None, "…")
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
    """The real client is wired to the worker's token bucket and stop event."""
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
