"""One plugin session's footprint check: scan, fetch, resolve, store (spec sections 8, 10, 15).

The controller owns the cache, the verdict store, the client and the worker for one
open board.  Scanning reads the board on the caller's thread, resolves what the
cache already holds and queues the rest: a batch lookup for parts the cache does
not know, then the footprint and symbol documents their rows still lack.  Results
arrive on the worker thread: each piece goes into the cache, and once a part's row
is complete every board part with that LCSC is resolved, its verdict stored, and
one event posted to the main thread with the session generation so a stale board
reload can drop it.  One lock keeps each scan step and each result handler whole
with respect to the other, so a scan never sees a half-handled result and never
queues a part behind a request that has just answered.  The CPL path asks
:meth:`FootprintCheck.decisions` for one rotation decision per reference.  Stdlib
only; wx and pcbnew stay in the caller.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
import logging
import threading
import time
from typing import Any

from .cache import Cache, CachedPart
from .drawing import DrawingMarks, drawing_marks
from .easyeda_client import EasyEdaClient
from .easyeda_parse import ComponentRecord
from .geometry import Pad, easyeda_pads_to_mm, named_pads, pad_pitch
from .kicad_adapter import BoardPart
from .presentation import describe_seconds, jlc_state, verdict_text
from .resolver import Verdict, resolve
from .verdicts import PENDING, StoredVerdict, VerdictStore
from .worker import FOOTPRINT, LOOKUP, SYMBOL, Buckets, FetchWorker

logger = logging.getLogger(__name__)


@dataclass
class ScanSummary:
    """What one board scan found (the log window's line)."""

    scanned: int = 0
    without_lcsc: int = 0
    already_resolved: int = 0
    resolved_from_cache: int = 0
    enqueued: int = 0
    pending: int = 0
    queued: dict = field(default_factory=dict)  # lookups, footprints, symbols
    estimate_s: float = 0.0

    def __str__(self) -> str:
        """Render the summary for the log window."""
        text = (
            f"scanned {self.scanned} footprints, {self.without_lcsc} without LCSC, "
            f"{self.already_resolved} already resolved, {self.resolved_from_cache} resolved "
            f"from the cache, {self.enqueued} enqueued, {self.pending} pending"
        )
        if self.pending:
            text += (
                f"; queued: {self.queued.get('lookups', 0)} lookup(s), "
                f"{self.queued.get('footprints', 0)} footprint(s), "
                f"{self.queued.get('symbols', 0)} symbol(s), about {describe_seconds(self.estimate_s)}"
            )
        return text


@dataclass
class FetchState:
    """Where one part stands in the fetch queue (spec 16.3's ◷ and ‖ rows).

    ``state`` is ``idle`` (nothing outstanding), ``queued``, ``fetching`` (this
    part's own request is in flight, ``kind`` says which document), ``paused`` (a
    backoff is running, ``seconds`` is what is left of it) or ``tripped`` (the
    session's breaker).  ``ahead`` counts the requests queued before this part's and
    ``seconds`` how long they take; ``queued_parts`` is how many parts wait in all.
    """

    state: str = "idle"
    kind: str = ""
    ahead: int = 0
    seconds: float = 0.0
    queued_parts: int = 0


@dataclass
class Decision:
    """What the CPL emits for one reference (spec section 8)."""

    reference: str
    lcsc: str
    rotation: int | None = None  # the correction applied; None emits the raw angle
    source: str = "raw"  # 'override' | 'derived' | 'raw'
    status: str = "unknown"  # verdict status, 'pending', or 'no-lcsc' / 'no-verdict'
    polarity_light: str | None = None
    fit: str | None = None
    note: str = ""
    pending: bool = False
    body_excess: float | None = None  # the body-size caveat in mm (spec 16.6 item 4)
    verdict: StoredVerdict | None = field(default=None, repr=False)

    @property
    def display(self) -> str:
        """Return the Rotation column text: what the CPL emits, never the queue's state.

        Spec 16.3 retired the pending ellipsis: a part still being fetched emits its
        raw angle, so the cell says "raw" and the JLC column's clock says why.
        """
        if self.verdict is not None:
            return self.verdict.display_text
        return "raw"


@dataclass
class PartDetail:
    """Everything the detail dialog shows for one part (spec 16.4).

    The two pad sets and the placement are the resolver's own inputs and output,
    re-run here rather than read back from the verdict row, so the canvas draws what
    the check decided and not a rounded copy of it.  ``verdict`` is None when the
    cache has nothing to resolve against (a part still being fetched, a part EasyEDA
    does not know), and then only the KiCad pads and whatever raw JLC pads the cache
    holds can be drawn.
    """

    reference: str
    lcsc: str
    kicad_footprint: str = ""
    kicad_pads: list[Pad] = field(default_factory=list)
    courtyard: tuple | None = None
    is_bottom: bool = False
    placed_rotation: float = 0.0
    package_name: str = ""
    puuid: str = ""
    jlc_pads: list[Pad] = field(default_factory=list)
    symbol_pins: list = field(default_factory=list)
    marks: DrawingMarks | None = None
    source: str = ""  # 'live' | 'seed' | '' when nothing is cached
    fetched_at: int = 0
    verdict: Verdict | None = None  # re-resolved, carries the placement
    stored: StoredVerdict | None = None
    decision: Decision | None = None
    fetch: FetchState = field(default_factory=FetchState)

    @property
    def pin_functions(self) -> dict:
        """Return the pin function per KiCad pad number, where the schematic gave one."""
        return {
            pad.number: pad.pin_function
            for pad in named_pads(self.kicad_pads)
            if pad.pin_function
        }

    @property
    def kicad_pitch(self) -> float | None:
        """Return the KiCad footprint's nearest-terminal pitch in millimetres."""
        return pad_pitch(self.kicad_pads)

    @property
    def jlc_pitch(self) -> float | None:
        """Return the JLC drawing's nearest-terminal pitch in millimetres."""
        return pad_pitch(self.jlc_pads)

    @property
    def kicad_pin1(self) -> Pad | None:
        """Return the KiCad pad numbered 1, if the footprint has one."""
        return next(
            (pad for pad in named_pads(self.kicad_pads) if pad.number == "1"), None
        )

    @property
    def jlc_pin1(self) -> Pad | None:
        """Return the JLC pad numbered 1, if the drawing has one."""
        return next(
            (pad for pad in named_pads(self.jlc_pads) if pad.number == "1"), None
        )

    @property
    def placement(self):
        """Return the placement the resolver solved, or None when there is none."""
        return None if self.verdict is None else self.verdict.placement

    @property
    def emitted_rotation(self) -> int | None:
        """Return the correction the CPL applies for this part, None for the raw angle."""
        return None if self.stored is None else self.stored.emitted_rotation


class FootprintCheck:
    """Lifecycle and bookkeeping for the footprint check of one open board."""

    def __init__(
        self,
        cache: Cache,
        verdicts: VerdictStore,
        read_board: Callable[[], list[BoardPart]],
        post: Callable[[str, int], None],
        client: EasyEdaClient | None = None,
        worker: FetchWorker | None = None,
        message: Callable[[str], None] | None = None,
        now: Callable[[], float] = time.time,
        buckets: Buckets | None = None,
    ) -> None:
        self.cache = cache
        self.verdicts = verdicts
        self.read_board = read_board
        self.post = post
        self.message = message
        self.now = now
        self.generation = 0
        self.parts: dict[str, BoardPart] = {}
        # LCSC -> EasyEDA package name, read from the cache on demand (see package_name).
        self._package_names: dict[str, str] = {}
        # Which parts wait on which request: ('lookup', lcsc) or (kind, uuid) -> LCSCs.
        # Read and written under the lock by the scan (main thread) and the handlers.
        self.waiting: dict[tuple[str, str], set[str]] = {}
        self.lock = threading.RLock()
        # Shared buckets keep the request rate across restarts of the check in one session.
        self.worker = worker or FetchWorker(
            self._lookup,
            self._fetch_document,
            self._on_lookup,
            self._on_document,
            on_tripped=self._on_tripped,
            buckets=buckets,
        )
        self.client = client or EasyEdaClient()
        # Every request and every retry spends the worker's tokens.
        self.client.acquire = self.worker.acquire
        self.client.wait = self.worker.wait_for

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background fetch thread."""
        self.worker.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop and join the background thread; a backoff in progress is interrupted."""
        self.worker.stop(timeout)

    # ------------------------------------------------------------------
    # Scanning
    # ------------------------------------------------------------------

    def scan_board(self, references: Iterable[str] | None = None) -> ScanSummary:
        """Read the board and resolve or queue every part with an LCSC.

        ``references`` limits the work to those parts (after an assignment); a full
        scan starts a new generation so events from an earlier board state are dropped.
        """
        if references is None:
            self.generation += 1
            self.parts = {}
        wanted = None if references is None else set(references)
        summary = ScanSummary()
        for part in self.read_board():
            if wanted is not None and part.reference not in wanted:
                continue
            self.parts[part.reference] = part
            summary.scanned += 1
            if not part.lcsc:
                summary.without_lcsc += 1
                continue
            # One part at a time, atomically with the worker's result handlers, so a
            # result landing mid-scan is seen whole: still pending, or complete.
            with self.lock:
                self._scan_part(part, summary)
        summary.pending = len(self.pending_lcscs())
        summary.queued = self.worker.counts()
        summary.estimate_s = self.worker.estimate_seconds()
        logger.info("jlcfootprint: %s", summary)
        return summary

    def enqueue_references(self, references: Iterable[str]) -> ScanSummary:
        """Re-read the given references after an LCSC assignment and resolve or queue them."""
        return self.scan_board(references)

    def _scan_part(self, part: BoardPart, summary: ScanSummary) -> None:
        """Resolve one part from the cache or queue what it needs (the caller holds the lock)."""
        stored = self.verdicts.get(part.lcsc, part.footprint_hash)
        needed = self.cache.needs(part.lcsc, self.now())
        # A verdict is final only while its cache row is: an error row is fetched
        # again next session and a ``none`` row after thirty days (spec 5.1).
        if stored is not None and stored.status != PENDING and not needed:
            summary.already_resolved += 1
            return
        if not needed:
            cached = self.cache.part(part.lcsc)
            if cached is not None:
                self._resolve_and_store(part, cached)
                summary.resolved_from_cache += 1
                return
            needed = {"lookup"}
        if part.lcsc in self.pending_lcscs():
            # Queued or in flight already: that request resolves this part too, and
            # marking it pending here could overwrite a verdict saved meanwhile.
            return
        self.verdicts.mark_pending(
            part.lcsc, part.footprint_hash, part.footprint_name, self.now()
        )
        self._request(part.lcsc, needed)
        summary.enqueued += 1

    def _request(self, lcsc: str, needed: set[str]) -> None:
        """Queue what a part still needs and remember that the part waits on it (lock held)."""
        if "lookup" in needed:
            self.worker.enqueue_lookups([lcsc])
            self.waiting.setdefault((LOOKUP, lcsc), set()).add(lcsc)
            return
        cached = self.cache.part(lcsc)
        if cached is None:
            self.worker.enqueue_lookups([lcsc])
            self.waiting.setdefault((LOOKUP, lcsc), set()).add(lcsc)
            return
        documents = []
        if "footprint" in needed and cached.record.puuid:
            documents.append((FOOTPRINT, cached.record.puuid))
        if "symbol" in needed and cached.record.symbol_uuid:
            documents.append((SYMBOL, cached.record.symbol_uuid))
        for item in documents:
            self.waiting.setdefault(item, set()).add(lcsc)
        self.worker.enqueue_documents(documents)

    # ------------------------------------------------------------------
    # Resolving
    # ------------------------------------------------------------------

    def resolve_part(self, part: BoardPart, cached: CachedPart) -> Verdict:
        """Run the resolver for one board part against its cached EasyEDA data."""
        record: ComponentRecord = cached.record
        return resolve(
            part.pads,
            part.footprint_name,
            record.status,
            record.package_name,
            easyeda_pads_to_mm(record.pads),
            record.symbol_pins,
            cached.polarity_source,
            marks=drawing_marks(
                record.symbol_shapes, record.footprint_shapes, record.footprint_origin
            ),
            kicad_courtyard=part.courtyard,
        )

    def _resolve_and_store(self, part: BoardPart, cached: CachedPart) -> StoredVerdict:
        verdict = self.resolve_part(part, cached)
        stored = self.verdicts.save(
            part.lcsc,
            part.footprint_hash,
            part.footprint_name,
            cached.record.puuid,
            verdict,
            self.now(),
        )
        # A clean green verdict at zero says nothing the user must act on (kicad-bpzt).
        quiet = stored.status == "green" and not stored.rotation and not stored.notes
        logger.log(
            logging.DEBUG if quiet else logging.INFO,
            "jlcfootprint: %s %s (%s): %s, fit %s, rotation %s, %s%s",
            part.reference,
            part.lcsc,
            cached.record.package_name or "no package",
            stored.status,
            stored.fit,
            "none" if stored.rotation is None else f"{stored.rotation}°",
            stored.method,
            f"; {stored.notes}" if stored.notes else "",
        )
        return stored

    def _finish(self, lcsc: str) -> None:
        """Resolve every board part with this LCSC from the cache and post the event (lock held)."""
        cached = self.cache.part(lcsc)
        if cached is not None:
            for part in list(self.parts.values()):
                if part.lcsc == lcsc:
                    self._resolve_and_store(part, cached)
        self.post(lcsc, self.generation)

    def _fail(self, lcsc: str, error: str) -> None:
        """Record a failed fetch as an error row so the next session tries again (lock held)."""
        self.cache.store(
            ComponentRecord(lcsc=lcsc, status="error", error=error or "fetch failed"),
            self.now(),
        )
        self._finish(lcsc)

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _lookup(self, codes: list[str]) -> Any:
        logger.info("jlcfootprint: looking up %d part(s)", len(codes))
        return self.client.search_by_codes(codes)

    def _fetch_document(self, kind: str, uuid: str) -> Any:
        counts = self.worker.counts()
        logger.info(
            "jlcfootprint: fetching %s %s (%d document(s) queued)",
            kind,
            uuid,
            counts["footprints"] + counts["symbols"],
        )
        if kind == FOOTPRINT:
            return self.client.fetch_footprint(uuid)
        return self.client.fetch_symbol(uuid)

    def _on_lookup(self, codes: list[str], lookup: Any) -> None:
        """Worker thread: store each hit's uuids and queue its documents; misses become none."""
        devices = getattr(lookup, "devices", None)
        error = str(getattr(lookup, "error", "") or "")
        with self.lock:
            if devices is None or error:
                logger.warning(
                    "jlcfootprint: lookup of %d part(s) failed: %s; retried next session",
                    len(codes),
                    error or "lookup failed",
                )
                for code in codes:
                    self.waiting.pop((LOOKUP, code), None)
                    self._fail(code, error or "lookup failed")
                return
            logger.info(
                "jlcfootprint: lookup answered: %d of %d part(s) known to EasyEDA",
                sum(1 for code in codes if code in devices.hits),
                len(codes),
            )
            for code in codes:
                self.waiting.pop((LOOKUP, code), None)
                hit = devices.hits.get(code)
                if hit is None:
                    self.cache.store_lookup_miss(code, self.now())
                    self._finish(code)
                    continue
                self.cache.store_lookup(code, hit.symbol_uuid, hit.puuid, self.now())
                needed = self.cache.needs(code, self.now())
                if needed:
                    self._request(code, needed)
                else:
                    self._finish(code)

    def _on_document(self, kind: str, uuid: str, document: Any) -> None:
        """Worker thread: store the document, then resolve every part whose row is complete."""
        with self.lock:
            lcscs = sorted(self.waiting.pop((kind, uuid), set()))
            record = getattr(document, "record", None)
            transient = bool(getattr(document, "transient", False))
            if record is None or transient or record.status == "error":
                # Transient or final, a failed document is never stored as an answer:
                # the part gets an error row and the next session asks again.
                error = str(
                    getattr(record, "error", "") or getattr(document, "error", "")
                )
                logger.warning(
                    "jlcfootprint: %s %s failed for %s: %s",
                    kind,
                    uuid,
                    ", ".join(lcscs) or "no part",
                    error or "fetch failed",
                )
                for lcsc in lcscs:
                    self._fail(lcsc, error)
                return
            if kind == FOOTPRINT:
                if record.status == "ok":
                    self.cache.store_footprint(record, self.now())
                else:
                    # No footprint means nothing to align: the checkerboard case.
                    for lcsc in lcscs:
                        self.cache.store_lookup_miss(lcsc, self.now())
            else:
                for lcsc in lcscs:
                    self.cache.store_symbol(lcsc, record, self.now())
            for lcsc in lcscs:
                if not self.cache.needs(lcsc, self.now()):
                    self._finish(lcsc)

    def _on_tripped(self, message: str) -> None:
        if self.message is not None:
            self.message(message)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def pending_lcscs(self) -> set[str]:
        """Return the LCSCs with a request queued or in flight."""
        with self.lock:
            pending: set[str] = set()
            for lcscs in self.waiting.values():
                pending.update(lcscs)
            return pending

    def pending_references(self) -> list[str]:
        """Return the references whose EasyEDA data is still queued or in flight."""
        pending = self.pending_lcscs()
        return sorted(
            reference for reference, part in self.parts.items() if part.lcsc in pending
        )

    def queue_estimate(self) -> tuple[int, float]:
        """Return the outstanding requests (queued and in flight) and roughly how long they take.

        A backoff the request in flight is sleeping through counts toward the time.
        """
        counts = self.worker.counts()
        active, backoff = self.worker.active()
        return sum(counts.values()) + active, self.worker.estimate_seconds() + backoff

    def references_for(self, lcsc: str) -> list[str]:
        """Return the references currently carrying this LCSC."""
        return sorted(
            reference for reference, part in self.parts.items() if part.lcsc == lcsc
        )

    def decision(self, part: BoardPart) -> Decision:
        """Return the CPL decision for one board part (spec section 8)."""
        if not part.lcsc:
            return Decision(part.reference, "", status="no-lcsc", note="no LCSC number")
        stored = self.verdicts.get(part.lcsc, part.footprint_hash)
        # Pending is per part: a resolved part is not pending because another pad
        # geometry of the same LCSC is still being fetched.
        pending = part.lcsc in self.pending_lcscs() and (
            stored is None or stored.status == PENDING
        )
        if stored is None:
            return Decision(
                part.reference,
                part.lcsc,
                status="no-verdict",
                pending=pending,
                note="not checked yet",
            )
        return Decision(
            part.reference,
            part.lcsc,
            rotation=stored.emitted_rotation,
            source=stored.source,
            status=stored.status,
            polarity_light=stored.polarity_light,
            fit=stored.fit,
            note=stored.override_note or stored.notes or "",
            pending=pending or stored.status == PENDING,
            body_excess=stored.body_excess_mm,
            verdict=stored,
        )

    def decisions(self, reread: bool = True) -> dict[str, Decision]:
        """Return one decision per reference, re-reading the board first by default."""
        if reread:
            for part in self.read_board():
                self.parts[part.reference] = part
        return {
            reference: self.decision(part) for reference, part in self.parts.items()
        }

    def display_text(self, reference: str) -> str:
        """Return the Rotation column text for one reference."""
        part = self.parts.get(reference)
        if part is None:
            return ""
        return self.decision(part).display

    def fetch_state(self, lcsc: str) -> FetchState:
        """Return where this part's EasyEDA data stands in the queue (spec 16.3).

        The breaker is the session's, so it answers for every part; a backoff stalls
        the whole queue behind the request that is sleeping, so a part waiting on
        anything reads as paused while it runs.
        """
        if not lcsc:
            return FetchState()
        with self.lock:
            keys = [key for key, lcscs in self.waiting.items() if lcsc in lcscs]
        queued_parts = len(self.pending_lcscs())
        if self.worker.tripped:
            return FetchState("tripped", queued_parts=queued_parts)
        if not keys:
            return FetchState(queued_parts=queued_parts)
        requests, seconds = self.queue_estimate()
        in_flight = self.worker.in_flight
        _active, backoff = self.worker.active()
        if backoff > 0:
            return FetchState(
                "paused", seconds=backoff, queued_parts=queued_parts, ahead=requests
            )
        if in_flight is not None:
            kind, payload = in_flight
            mine = lcsc in payload if kind == LOOKUP else (kind, str(payload)) in keys
            if mine:
                return FetchState(
                    "fetching", kind=kind, queued_parts=queued_parts, ahead=requests
                )
        return FetchState(
            "queued", ahead=requests, seconds=seconds, queued_parts=queued_parts
        )

    def package_name(self, lcsc: str) -> str:
        """Return the EasyEDA package name for one part, read from the cache once.

        The hover and the dialog name the JLC package, which the verdict row does not
        carry; a board's worth of names is a handful of rows, so they are memoised for
        the session rather than read per repaint.
        """
        if not lcsc:
            return ""
        name = self._package_names.get(lcsc, "")
        if not name:
            cached = self.cache.part(lcsc)
            record = None if cached is None else cached.record
            name = "" if record is None else record.package_name
            if name:
                # A part whose footprint has not landed yet has no name to remember:
                # asking again after it lands is what fills the hover and the dialog.
                self._package_names[lcsc] = name
        return name

    def glyph_state(self, reference: str) -> str:
        """Return the JLC column's state for one reference ("" when it has no part)."""
        part = self.parts.get(reference)
        if part is None:
            return ""
        return jlc_state(self.decision(part), self.fetch_state(part.lcsc))

    def detail(self, reference: str, reread: bool = False) -> PartDetail | None:
        """Return everything the detail dialog shows for one reference (spec 16.4).

        ``reread`` re-reads the board first, which the dialog does on open so an edit
        made since the last scan is what is drawn.  None when the reference is not on
        the board at all.
        """
        if reread:
            for part in self.read_board():
                self.parts[part.reference] = part
        part = self.parts.get(reference)
        if part is None:
            return None
        decision = self.decision(part)
        detail = PartDetail(
            reference=part.reference,
            lcsc=part.lcsc,
            kicad_footprint=part.footprint_name,
            kicad_pads=list(part.pads),
            courtyard=part.courtyard,
            is_bottom=part.is_bottom,
            placed_rotation=part.placed_rotation,
            stored=decision.verdict,
            decision=decision,
            fetch=self.fetch_state(part.lcsc),
        )
        if not part.lcsc:
            return detail
        cached = self.cache.part(part.lcsc)
        if cached is None:
            return detail
        record = cached.record
        detail.package_name = record.package_name
        detail.puuid = record.puuid
        detail.jlc_pads = easyeda_pads_to_mm(record.pads)
        detail.symbol_pins = list(record.symbol_pins)
        detail.marks = drawing_marks(
            record.symbol_shapes, record.footprint_shapes, record.footprint_origin
        )
        detail.source = cached.source
        detail.fetched_at = cached.fetched_at
        if record.status == "ok" and record.pads:
            # The placement the canvas draws is solved here, not read from the row.
            detail.verdict = self.resolve_part(part, cached)
        return detail

    def set_override(
        self, reference: str, rotation: int | None, note: str = ""
    ) -> StoredVerdict | None:
        """Set or clear the user's rotation for one reference and return the stored row.

        The override lives on the verdict row of (LCSC, pad hash) (spec 16.5), so it
        survives every re-resolve and every ``mark_pending`` and disappears only with
        this method or with a footprint edit, which changes the key.
        """
        part = self.parts.get(reference)
        if part is None or not part.lcsc:
            return None
        stored = self.verdicts.get(part.lcsc, part.footprint_hash)
        if stored is None:
            # A part with no row yet (never fetched) can still carry a decision.
            self.verdicts.mark_pending(
                part.lcsc, part.footprint_hash, part.footprint_name, self.now()
            )
        self.verdicts.set_override(
            part.lcsc,
            part.footprint_hash,
            rotation,
            note if rotation is not None else "",
        )
        updated = self.verdicts.get(part.lcsc, part.footprint_hash)
        logger.info(
            "jlcfootprint: %s %s override %s%s",
            reference,
            part.lcsc,
            "cleared" if rotation is None else f"set to {rotation}°",
            f" ({note})" if rotation is not None and note else "",
        )
        return updated

    def refetch(self, references: Iterable[str]) -> ScanSummary:
        """Forget these parts' cached EasyEDA data and queue them again (spec 16.5).

        The cache row goes, the verdict is marked pending by the rescan (which keeps
        the override, spec 5.3) and the rows show the clock until the answers land.
        """
        wanted = [reference for reference in references if reference in self.parts]
        lcscs = sorted(
            {
                self.parts[reference].lcsc
                for reference in wanted
                if self.parts[reference].lcsc
            }
        )
        for lcsc in lcscs:
            self.cache.forget(lcsc)
            self._package_names.pop(lcsc, None)
        logger.info(
            "jlcfootprint: re-fetching %d part(s): %s",
            len(lcscs),
            ", ".join(lcscs) or "none",
        )
        return self.scan_board(wanted)

    def references_sharing_verdict(self, reference: str) -> list[str]:
        """Return every reference whose verdict row is the one this reference uses.

        Two placements of one part on one footprint share a row, so an override set on
        either repaints both (spec 5.3 keys a verdict on the LCSC and the pad hash).
        """
        part = self.parts.get(reference)
        if part is None or not part.lcsc:
            return [] if part is None else [reference]
        return sorted(
            other.reference
            for other in self.parts.values()
            if other.lcsc == part.lcsc and other.footprint_hash == part.footprint_hash
        )

    def cell_help(self, reference: str) -> str:
        """Return the JLC cell's hover text for one reference (spec 16.3)."""
        part = self.parts.get(reference)
        if part is None:
            return ""
        decision = self.decision(part)
        return verdict_text(
            decision,
            self.fetch_state(part.lcsc),
            kicad_footprint=part.footprint_name,
            jlc_package=self.package_name(part.lcsc),
        )
