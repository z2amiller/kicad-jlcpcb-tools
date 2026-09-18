"""Tests for the footprint check controller: scanning, the lookup and document flow, deciding."""

import copy
import logging
import threading

import pytest

from jlcfootprint.cache import Cache
from jlcfootprint.controller import Decision, FetchState, FootprintCheck
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
# The column, the hover and the queue's state (spec 16.3)
# ---------------------------------------------------------------------------


def test_the_queue_state_of_one_part_while_it_is_scanned_fetched_and_resolved(setup):
    """A part reads queued, then fetching its own document, then idle once resolved."""
    check, board, _events, _messages, _client = setup
    assert check.fetch_state("C2132") == FetchState()
    assert check.glyph_state("Q1") == ""
    check.scan_board()
    queued = check.fetch_state("C2132")
    assert (queued.state, queued.ahead, queued.queued_parts) == ("queued", 2, 2)
    assert check.glyph_state("Q1") == "pending"
    assert "Queued for EasyEDA, 2 request(s) ahead" in check.cell_help("Q1")
    # The lookup in flight is this part's, so the cell says what is being fetched.
    check.worker.in_flight = ("lookup", ["C2132", "C2286"])
    assert check.fetch_state("C2132").state == "fetching"
    assert check.cell_help("Q1") == "Looking up the part at EasyEDA…"
    check.worker.in_flight = None
    check.worker.run_pending()
    assert check.fetch_state("C2132") == FetchState(queued_parts=0)
    assert check.glyph_state("Q1") == "green"
    assert check.cell_help("Q1") == (
        "Fits; rotation 180° derived from pad geometry (high). "
        f"JLC {recorded('C2132').package_name} on Package_TO_SOT_SMD:SOT-23."
    )
    assert check.glyph_state("D1") == "yellow"
    assert "pin-1 marker will sit on the other terminal" in check.cell_help("D1")
    assert check.glyph_state("R1") == ""
    assert (
        check.cell_help("R1") == "No LCSC number assigned. The CPL emits the raw angle."
    )
    assert check.cell_help("nope") == ""


def test_a_document_in_flight_and_a_backoff_and_the_breaker(setup):
    """Fetching a document this part waits on, a backoff and the breaker each have their own state."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    # The state after a lookup has landed: the part waits on its footprint document.
    puuid = recorded("C2132").puuid
    check.waiting[(FOOTPRINT, puuid)] = {"C2132"}
    check.worker.in_flight = (FOOTPRINT, puuid)
    fetching = check.fetch_state("C2132")
    assert (fetching.state, fetching.kind) == ("fetching", FOOTPRINT)
    assert check.cell_help("Q1") == "Fetching the footprint…"
    check.worker.backoff_until = check.worker.clock() + 45.0
    paused = check.fetch_state("C2132")
    assert (paused.state, round(paused.seconds)) == ("paused", 45)
    assert check.glyph_state("Q1") == "paused"
    assert check.cell_help("Q1") == "EasyEDA asked us to wait 45 s; 1 part(s) queued."
    check.worker.backoff_until = None
    check.worker.tripped = True
    assert check.fetch_state("C2132").state == "tripped"
    assert check.glyph_state("Q1") == "paused"
    assert check.cell_help("Q1") == (
        "Paused after three failed requests; reopen the plugin to retry."
    )


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


# ---------------------------------------------------------------------------
# The detail view, the override and the re-fetch (spec 16.4, 16.5)
# ---------------------------------------------------------------------------


def test_the_detail_of_a_resolved_part_carries_both_pad_sets_and_a_fresh_placement(
    setup,
):
    """Spec 16.4: the dialog's inputs are the resolver's own, re-run, not the stored row."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    detail = check.detail("Q1")
    assert (detail.reference, detail.lcsc) == ("Q1", "C2132")
    assert detail.kicad_footprint == "Package_TO_SOT_SMD:SOT-23"
    assert len(detail.kicad_pads) == 3 and len(detail.jlc_pads) == 3
    assert detail.package_name == recorded("C2132").package_name
    assert detail.puuid == recorded("C2132").puuid
    assert detail.source == "live" and detail.fetched_at > 0
    assert detail.pin_functions == {"1": "B", "2": "E", "3": "C"}
    assert detail.kicad_pitch == pytest.approx(
        1.9, abs=0.05
    )  # the SOT-23's nearest pads
    assert detail.jlc_pitch is not None
    assert detail.kicad_pin1 is not None and detail.jlc_pin1 is not None
    # The verdict is re-resolved here and carries the placement the canvas needs.
    assert detail.verdict is not None and detail.placement is not None
    assert (detail.verdict.status, detail.verdict.rotation) == ("green", 180)
    assert detail.stored is not None and detail.stored.rotation == 180
    assert detail.emitted_rotation == 180
    assert detail.fetch.state == "idle"
    assert detail.decision is not None and detail.decision.status == "green"
    assert check.detail("nope") is None


def test_the_detail_of_a_part_with_no_data_still_describes_the_footprint(setup):
    """A pending part, and a part with no LCSC, draw their KiCad pads and nothing else."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    pending = check.detail("Q1")
    assert pending.jlc_pads == [] and pending.verdict is None
    assert pending.package_name == "" and pending.source == ""
    assert len(pending.kicad_pads) == 3
    assert pending.fetch.state == "queued"
    assert pending.stored is not None and pending.stored.status == PENDING
    bare = check.detail("R1")
    assert (bare.lcsc, bare.jlc_pads, bare.stored) == ("", [], None)
    assert bare.kicad_pads == []


def test_the_detail_rereads_the_board_when_asked(setup):
    """The dialog opens on what the board says now, not on the last scan."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    board["parts"] = [*board["parts"], sot23("Q2")]
    assert check.detail("Q2") is None
    assert check.detail("Q2", reread=True) is not None


def test_an_override_is_written_kept_and_cleared(setup):
    """Spec 16.5: the override lives on the verdict row, survives a re-resolve, and clears."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    part = board["parts"][0]
    stored = check.set_override("Q1", 270, "JLC's preview needed it")
    assert (stored.override_rotation, stored.override_note) == (
        270,
        "JLC's preview needed it",
    )
    assert check.display_text("Q1") == "270° set"
    assert check.decision(part).source == "override"
    assert check.glyph_state("Q1") == "override"
    assert "Override 270° set by you." in check.cell_help("Q1")
    # A re-resolve and a re-scan keep it (save and mark_pending both do).
    check.verdicts.mark_pending("C2132", part.footprint_hash, part.footprint_name, 9)
    assert check.verdicts.get("C2132", part.footprint_hash).override_rotation == 270
    check.scan_board()
    check.worker.run_pending()
    assert check.verdicts.get("C2132", part.footprint_hash).override_rotation == 270
    cleared = check.set_override("Q1", None)
    assert (cleared.override_rotation, cleared.override_note) == (None, None)
    assert check.display_text("Q1") == "180°"
    assert check.set_override("R1", 90) is None  # no LCSC, no row
    assert check.set_override("nope", 90) is None


def test_an_override_on_a_part_with_no_verdict_row_yet_creates_one(setup):
    """A part whose data never arrived can still be overridden (the row is minted pending)."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    part = board["parts"][0]
    check.verdicts.delete("C2132", part.footprint_hash)
    stored = check.set_override("Q1", 90, "by hand")
    assert (stored.status, stored.override_rotation) == (PENDING, 90)
    assert check.display_text("Q1") == "90° set"


def test_two_placements_of_one_part_share_the_verdict_and_the_repaint(setup):
    """An override set on one reference repaints every reference on that row (spec 5.3)."""
    check, board, _events, _messages, _client = setup
    board["parts"] = [*board["parts"], sot23("Q2"), sot23("Q3", lcsc="C2286")]
    check.scan_board()
    assert check.references_sharing_verdict("Q1") == ["Q1", "Q2"]
    assert check.references_sharing_verdict("Q3") == ["Q3"]
    assert check.references_sharing_verdict("R1") == ["R1"]
    assert check.references_sharing_verdict("nope") == []


def test_refetching_forgets_the_cached_rows_and_queues_the_parts_again(setup, caplog):
    """Spec 16.5: the cache row goes, the verdict is pending again, the override stays."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    part = board["parts"][0]
    check.set_override("Q1", 270, "keep me")
    assert check.cache.status("C2132") == "ok"
    with caplog.at_level(logging.INFO, logger="jlcfootprint.controller"):
        summary = check.refetch(["Q1"])
    assert "re-fetching 1 part(s): C2132" in caplog.text
    assert check.cache.status("C2132") is None
    assert summary.enqueued == 1
    stored = check.verdicts.get("C2132", part.footprint_hash)
    assert (stored.status, stored.override_rotation) == (PENDING, 270)
    assert check.glyph_state("Q1") == "pending"
    assert check.package_name("C2132") == ""
    # The queued request answers and the part resolves again, override intact.
    check.worker.run_pending()
    assert check.cache.status("C2132") == "ok"
    assert check.verdicts.get("C2132", part.footprint_hash).override_rotation == 270
    assert check.package_name("C2132") == recorded("C2132").package_name
    assert check.refetch(["nope"]).scanned == 0


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


def test_rechecking_the_board_resolves_from_the_cache_without_any_request(
    setup, caplog
):
    """Spec 16.5: the resolver runs again on cached data, no lookup, no document."""
    check, board, _events, _messages, client = setup
    check.scan_board()
    check.worker.run_pending()
    calls = (len(client.lookups), len(client.documents))
    check.verdicts.delete("C2132", board["parts"][0].footprint_hash)
    with caplog.at_level(logging.INFO, logger="jlcfootprint.controller"):
        assert check.recheck_board() == 2
    assert "re-checked 2 part(s)" in caplog.text
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


def test_refreshing_the_board_forgets_every_row_and_rescans(setup, caplog):
    """Spec 16.5: every LCSC on the board is fetched again, overrides kept."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    check.set_override("Q1", 90, "keep me")
    with caplog.at_level(logging.INFO, logger="jlcfootprint.controller"):
        summary = check.refresh_board()
    assert "refreshing 2 part(s) from EasyEDA" in caplog.text
    assert summary.enqueued == 2
    assert check.cache.status("C2132") is None and check.cache.status("C2286") is None
    row = check.verdicts.get("C2132", board["parts"][0].footprint_hash)
    assert (row.status, row.override_rotation) == (PENDING, 90)


def test_clearing_the_cache_empties_it_but_keeps_the_schema(setup, caplog):
    """Spec 16.5: every cache row goes, the board is rescanned, a seed can refill it."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    assert check.cache.counts()["parts"] >= 2
    with caplog.at_level(logging.INFO, logger="jlcfootprint.controller"):
        summary = check.clear_cache()
    assert "cleared the cache" in caplog.text
    assert check.cache.counts() == {"parts": 0, "packages": 0}
    assert summary.enqueued == 2
    assert check.package_name("C2132") == ""
    check.cache.store_lookup_miss("C99", now=1)
    assert check.cache.status("C99") == "none"
