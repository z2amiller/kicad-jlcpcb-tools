"""Tests for the footprint check controller: scanning, the lookup and document flow, deciding."""

import copy
import logging
import threading

import pytest

from jlcfootprint.cache import Cache
from jlcfootprint.controller import Decision, FootprintCheck
from jlcfootprint.easyeda_client import Document, Lookup
from jlcfootprint.easyeda_parse import (
    DeviceHit,
    DevicesResult,
    FootprintRecord,
    SymbolRecord,
)
from jlcfootprint.geometry import pad_hash
from jlcfootprint.kicad_adapter import BoardPart, verdict_key
from jlcfootprint.verdicts import PENDING, VerdictStore
from jlcfootprint.worker import FOOTPRINT, SYMBOL, FetchWorker

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
    """Answer the three Pro calls from recorded classic responses; count every call.

    A part's symbol uuid is ``sym-<lcsc>``; its footprint uuid is the classic
    record's, so two parts sharing a footprint share one document.
    """

    def __init__(self, records=None):
        self.records = records or {}
        self.lookups = []
        self.documents = []
        self.fail_lookups = False
        self.transient_symbols = set()
        self.silent_symbols = set()
        # Documents that answer a final error (HTTP 401, an unreadable body).
        self.error_footprints = set()
        self.error_symbols = set()

    def search_by_codes(self, codes):
        """Return hits for the recorded parts and misses for the rest."""
        codes = list(codes)
        self.lookups.append(codes)
        if self.fail_lookups:
            return Lookup(
                codes, DevicesResult(error="HTTP 403 after retries"), transient=True
            )
        devices = DevicesResult()
        for code in codes:
            record = self.records.get(code)
            if record is not None:
                devices.hits[code] = DeviceHit(
                    code, f"sym-{code}", record.puuid, record.package_name
                )
        devices.missing = [code for code in codes if code not in devices.hits]
        return Lookup(codes, devices)

    def fetch_footprint(self, puuid):
        """Return the recorded footprint with this uuid, or none."""
        self.documents.append((FOOTPRINT, puuid))
        if puuid in self.error_footprints:
            return Document(
                FOOTPRINT, puuid, FootprintRecord(puuid=puuid, error="HTTP 401")
            )
        for record in self.records.values():
            if record.puuid == puuid:
                return Document(
                    FOOTPRINT,
                    puuid,
                    FootprintRecord(
                        puuid=puuid,
                        status="ok",
                        package_name=record.package_name,
                        pads=record.pads,
                        footprint_shapes=record.footprint_shapes,
                    ),
                )
        return Document(FOOTPRINT, puuid, FootprintRecord(puuid=puuid, status="none"))

    def fetch_symbol(self, uuid):
        """Return the recorded symbol pins for ``sym-<lcsc>``; a failure or none when told to."""
        self.documents.append((SYMBOL, uuid))
        lcsc = uuid[len("sym-") :]
        if uuid in self.transient_symbols:
            return Document(
                SYMBOL,
                uuid,
                SymbolRecord(uuid=uuid, error="HTTP 500 after retries"),
                transient=True,
            )
        if uuid in self.error_symbols:
            return Document(SYMBOL, uuid, SymbolRecord(uuid=uuid, error="HTTP 401"))
        record = self.records.get(lcsc)
        if record is None or uuid in self.silent_symbols:
            return Document(SYMBOL, uuid, SymbolRecord(uuid=uuid, status="none"))
        return Document(
            SYMBOL,
            uuid,
            SymbolRecord(
                uuid=uuid,
                status="ok",
                pins=record.symbol_pins,
                shapes=record.symbol_shapes,
            ),
        )


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


def sod323(reference, functions, lcsc="C7502694"):
    """Return a SOD-323 diode part with the given pin functions."""
    pads = with_functions(library_pads("Diode_SMD", "D_SOD-323"), functions)
    return BoardPart(
        reference, lcsc, "Diode_SMD:D_SOD-323", False, 0.0, pads, verdict_key(pads)
    )


def tantalum(reference, lcsc="C16133"):
    """Return a case-B tantalum part whose JLC name carries no orientation token."""
    pads = with_functions(
        library_pads("Capacitor_Tantalum_SMD", "CP_EIA-3528-21_Kemet-B"),
        {"1": "+", "2": "-"},
    )
    return BoardPart(
        reference,
        lcsc,
        "Capacitor_Tantalum_SMD:CP_EIA-3528-21_Kemet-B",
        False,
        0.0,
        pads,
        verdict_key(pads),
    )


def alias(lcsc, source="C2132"):
    """Return the recorded part under another LCSC (same footprint and symbol)."""
    record = copy.deepcopy(recorded(source))
    record.lcsc = lcsc
    return record


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
    client = FakeClient(
        {
            "C2132": recorded("C2132"),
            "C2286": recorded("C2286"),
            "C7502694": recorded("C7502694"),
        }
    )
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
    check.worker.wait = clock.wait
    check.worker.clock = clock
    return check, board, events, messages, client


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
    assert check.display_text("Q1") == "…"
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
    # No token in the name and no symbol: never a guess.
    c6 = check.verdicts.get("C16133", board["parts"][1].footprint_hash)
    assert c6.status == "unknown" and "polarity unknown" in c6.notes
    # The RD token orients the LED on its own; only the pin-1 light is unknown.
    d5 = check.verdicts.get("C5", board["parts"][2].footprint_hash)
    assert (d5.status, d5.rotation, d5.polarity_light) == ("green", 0, "unknown")
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
