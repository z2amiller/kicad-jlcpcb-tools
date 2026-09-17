"""One plugin session's footprint check: scan, fetch, resolve, store (spec sections 8, 10, 15).

The controller owns the cache, the verdict store, the client and the worker for one
open board.  Scanning reads the board on the caller's thread, resolves what the
cache already holds and queues the rest: a batch lookup for parts the cache does
not know, then the footprint and symbol documents their rows still lack.  Results
arrive on the worker thread: each piece goes into the cache, and once a part's row
is complete every board part with that LCSC is resolved, its verdict stored, and
one event posted to the main thread with the session generation so a stale board
reload can drop it.  The CPL path asks :meth:`FootprintCheck.decisions` for one
rotation decision per reference.  Stdlib only; wx and pcbnew stay in the caller.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
import logging
import time
from typing import Any

from .cache import Cache, CachedPart
from .easyeda_client import EasyEdaClient
from .easyeda_parse import ComponentRecord
from .geometry import easyeda_pads_to_mm
from .kicad_adapter import BoardPart
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


def describe_seconds(seconds: float) -> str:
    """Render an estimate as seconds under a minute, else minutes."""
    if seconds < 60:
        return f"{int(round(seconds))} s"
    return f"{int(round(seconds / 60.0))} min"


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
    verdict: StoredVerdict | None = field(default=None, repr=False)

    @property
    def display(self) -> str:
        """Return the Rotation column text."""
        if self.pending:
            return "…"
        if self.verdict is not None:
            return self.verdict.display_text
        return "raw"


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
        # Which parts wait on which request: ('lookup', lcsc) or (kind, uuid) -> LCSCs.
        self.waiting: dict[tuple[str, str], set[str]] = {}
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
            stored = self.verdicts.get(part.lcsc, part.footprint_hash)
            needed = self.cache.needs(part.lcsc, self.now())
            # A verdict is final only while its cache row is: an error row is fetched
            # again next session and a ``none`` row after thirty days (spec 5.1).
            if stored is not None and stored.status != PENDING and not needed:
                summary.already_resolved += 1
                continue
            if not needed:
                cached = self.cache.part(part.lcsc)
                if cached is not None:
                    self._resolve_and_store(part, cached)
                    summary.resolved_from_cache += 1
                    continue
                needed = {"lookup"}
            if part.lcsc in self.pending_lcscs():
                # Queued or in flight already: that request resolves this part too, and
                # marking it pending here could overwrite a verdict saved meanwhile.
                continue
            self.verdicts.mark_pending(
                part.lcsc, part.footprint_hash, part.footprint_name, self.now()
            )
            self._request(part.lcsc, needed)
            summary.enqueued += 1
        summary.pending = len(self.pending_lcscs())
        summary.queued = self.worker.counts()
        summary.estimate_s = self.worker.estimate_seconds()
        logger.info("jlcfootprint: %s", summary)
        return summary

    def enqueue_references(self, references: Iterable[str]) -> ScanSummary:
        """Re-read the given references after an LCSC assignment and resolve or queue them."""
        return self.scan_board(references)

    def _request(self, lcsc: str, needed: set[str]) -> None:
        """Queue what a part still needs and remember that the part waits on it."""
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
        """Worker thread: resolve every board part with this LCSC from the cache, post the event."""
        cached = self.cache.part(lcsc)
        if cached is not None:
            for part in list(self.parts.values()):
                if part.lcsc == lcsc:
                    self._resolve_and_store(part, cached)
        self.post(lcsc, self.generation)

    def _fail(self, lcsc: str, error: str) -> None:
        """Worker thread: record a transient failure so the next session tries again."""
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
        if devices is None or error:
            for code in codes:
                self.waiting.pop((LOOKUP, code), None)
                self._fail(code, error or "lookup failed")
            return
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
        lcscs = sorted(self.waiting.pop((kind, uuid), set()))
        record = getattr(document, "record", None)
        if record is None or getattr(document, "transient", False):
            error = str(getattr(record, "error", "") or getattr(document, "error", ""))
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
        """Return the queued request count and roughly how long they take."""
        counts = self.worker.counts()
        return sum(counts.values()), self.worker.estimate_seconds()

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
