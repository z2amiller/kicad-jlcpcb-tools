"""Helpers for assembly enrichment state and status labeling."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, MutableSet
from threading import Thread
import time
from typing import Protocol

try:
    from .bom_estimator import fetch_assembly_processes
except ImportError:  # pragma: no cover - test import fallback
    from bom_estimator import fetch_assembly_processes

STATUS_PENDING = "Pending"
STATUS_DONE = "Done"
STATUS_QUEUED = "Queued"
STATUS_NO_DATA = "No data"


class _Logger(Protocol):
    """Minimal logger protocol for enrichment worker error reporting."""

    def exception(self, msg: str, *args) -> None:
        """Log an exception with formatting args."""


class _ThreadLike(Protocol):
    """Minimal thread interface needed by start_assembly_enrichment."""

    def start(self) -> None:
        """Start background execution."""


def get_enrichment_status_label(
    part: Mapping[str, object], pending_lcsc_codes: Collection[str]
) -> str:
    """Build UI status text for per-part assembly enrichment state."""
    lcsc = str(part.get("lcsc") or "")
    if not lcsc:
        return ""
    if lcsc in pending_lcsc_codes:
        return STATUS_PENDING
    if str(part.get("assembly_process") or "") or part.get("component_product_type") is not None:
        return STATUS_DONE
    return STATUS_QUEUED


def get_enrichment_result_status(metadata: Mapping[str, object]) -> str:
    """Build status text for an enrichment result payload."""
    assembly_process = metadata.get("assembly_process", "")
    component_product_type = metadata.get("component_product_type")
    if assembly_process or component_product_type is not None:
        return STATUS_DONE
    return STATUS_NO_DATA


def run_assembly_enrichment_worker(
    targets: Mapping[str, Iterable[str]],
    post_progress: Callable[[str, list[str], Mapping[str, object]], None],
    *,
    fetch_metadata: Callable[[Iterable[str]], Mapping[str, Mapping[str, object]]] = fetch_assembly_processes,
    request_interval_seconds: float = 1.0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    logger: _Logger | None = None,
) -> None:
    """Fetch assembly metadata for targets and report each result via callback."""
    next_allowed_request = 0.0

    for lcsc, refs in targets.items():
        try:
            delay_seconds = max(0.0, next_allowed_request - monotonic())
            if delay_seconds > 0:
                sleep(delay_seconds)

            metadata = dict(fetch_metadata([lcsc]).get(lcsc, {}))
            next_allowed_request = monotonic() + request_interval_seconds
        except Exception:  # pylint: disable=broad-exception-caught
            if logger is not None:
                logger.exception("Assembly enrichment worker failed for %s", lcsc)
            metadata = {}
            next_allowed_request = monotonic() + request_interval_seconds

        post_progress(lcsc, list(refs), metadata)


def start_assembly_enrichment(
    references,
    *,
    get_targets: Callable[[object], Mapping[str, list[str]]],
    pending_lcsc_codes: MutableSet[str],
    set_pending_status: Callable[[str, str], None],
    post_progress: Callable[[str, list[str], Mapping[str, object]], None],
    logger: _Logger | None = None,
    worker: Callable[..., None] = run_assembly_enrichment_worker,
    thread_factory: Callable[..., _ThreadLike] = Thread,
) -> bool:
    """Queue enrichment work and spawn a background worker thread.

    Returns True when work was started, False when there was nothing to do.
    """
    targets = dict(get_targets(references))
    targets = {
        lcsc: refs for lcsc, refs in targets.items() if lcsc not in pending_lcsc_codes
    }
    if not targets:
        return False

    for lcsc in targets:
        pending_lcsc_codes.add(lcsc)
    for refs in targets.values():
        for reference in refs:
            set_pending_status(reference, STATUS_PENDING)

    thread_factory(
        target=worker,
        args=(targets, post_progress),
        kwargs={"logger": logger},
        daemon=True,
    ).start()
    return True
