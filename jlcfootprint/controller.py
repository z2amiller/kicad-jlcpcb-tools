"""One plugin session's footprint check: scan, fetch, resolve, store (spec sections 8 and 10).

The controller owns the cache, the verdict store, the client and the worker for one
open board.  Scanning reads the board on the caller's thread, resolves what the
cache already holds and queues the rest.  Fetch results arrive on the worker
thread: the record goes into the cache, every board part with that LCSC is
resolved and its verdict stored, and one event is posted to the main thread with
the session generation so a stale board reload can drop it.  The CPL path asks
:meth:`FootprintCheck.decisions` for one rotation decision per reference.
Stdlib only; wx and pcbnew stay in the caller.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
import logging
import time
from typing import Any

from .cache import Cache, CachedPart
from .easyeda_client import EasyEdaClient, Fetched
from .easyeda_parse import ComponentRecord
from .geometry import easyeda_pads_to_mm
from .kicad_adapter import BoardPart
from .resolver import Verdict, resolve
from .verdicts import PENDING, StoredVerdict, VerdictStore
from .worker import FetchWorker, TokenBucket

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

    def __str__(self) -> str:
        """Render the summary for the log window."""
        return (
            f"scanned {self.scanned} footprints, {self.without_lcsc} without LCSC, "
            f"{self.already_resolved} already resolved, {self.resolved_from_cache} resolved "
            f"from the cache, {self.enqueued} enqueued, {self.pending} pending"
        )


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
        bucket: TokenBucket | None = None,
    ) -> None:
        self.cache = cache
        self.verdicts = verdicts
        self.read_board = read_board
        self.post = post
        self.message = message
        self.now = now
        self.generation = 0
        self.parts: dict[str, BoardPart] = {}
        # A shared bucket keeps the request rate across restarts of the check in one session.
        self.worker = worker or FetchWorker(
            self._fetch, self._on_fetched, on_tripped=self._on_tripped, bucket=bucket
        )
        self.client = client or EasyEdaClient()
        # The per-uuid fallback and every retry spend the worker's tokens too.
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
        to_fetch: list[str] = []
        for part in self.read_board():
            if wanted is not None and part.reference not in wanted:
                continue
            self.parts[part.reference] = part
            summary.scanned += 1
            if not part.lcsc:
                summary.without_lcsc += 1
                continue
            stored = self.verdicts.get(part.lcsc, part.footprint_hash)
            refetch = self.cache.needs_fetch(part.lcsc, self.now())
            # A verdict is final only while its cache row is: an error row is fetched
            # again next session and a ``none`` row after thirty days (spec 5.1).
            if stored is not None and stored.status != PENDING and not refetch:
                summary.already_resolved += 1
                continue
            cached = None if refetch else self.cache.part(part.lcsc)
            if cached is not None:
                self._resolve_and_store(part, cached)
                summary.resolved_from_cache += 1
                continue
            if part.lcsc in self.worker.pending():
                # Queued or in flight already: that fetch resolves this part too, and
                # marking it pending here could overwrite a verdict saved meanwhile.
                continue
            self.verdicts.mark_pending(
                part.lcsc, part.footprint_hash, part.footprint_name, self.now()
            )
            to_fetch.append(part.lcsc)
        summary.enqueued = self.worker.enqueue(to_fetch)
        summary.pending = len(self.worker.pending())
        logger.info("jlcfootprint: %s", summary)
        return summary

    def enqueue_references(self, references: Iterable[str]) -> ScanSummary:
        """Re-read the given references after an LCSC assignment and resolve or queue them."""
        return self.scan_board(references)

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
        logger.info(
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

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _fetch(self, lcsc: str) -> Fetched:
        logger.info("jlcfootprint: fetching %s", lcsc)
        return self.client.fetch_component(lcsc)

    def _on_fetched(self, lcsc: str, fetched: Any) -> None:
        """Worker thread: cache the record, resolve every part with this LCSC, post the event."""
        record = getattr(fetched, "record", None)
        if record is None:
            record = ComponentRecord(
                lcsc=lcsc,
                status="error",
                error=str(getattr(fetched, "error", "fetch failed")),
            )
        self.cache.store(record, self.now())
        cached = self.cache.part(lcsc)
        if cached is not None:
            for part in list(self.parts.values()):
                if part.lcsc == lcsc:
                    self._resolve_and_store(part, cached)
        self.post(lcsc, self.generation)

    def _on_tripped(self, message: str) -> None:
        if self.message is not None:
            self.message(message)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def pending_references(self) -> list[str]:
        """Return the references whose EasyEDA data is still queued or in flight."""
        pending = self.worker.pending()
        return sorted(
            reference for reference, part in self.parts.items() if part.lcsc in pending
        )

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
        pending = part.lcsc in self.worker.pending() and (
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
