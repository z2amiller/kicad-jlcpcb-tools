"""EasyEDA client: one part per call with retries and backoff (spec sections 4 and 10).

The per-LCSC endpoint answers with the symbol and, nearly always, the footprint.
When the footprint is missing the footprint is fetched by its uuid: the classic
host first, since a classic response names a classic uuid, then the EasyEDA Pro
host, whose uuids are a separate space (the crawl's seed rows use those).
403/429/5xx back off with ``Retry-After`` when present, else 60, 120 and 240 s;
network errors and malformed bodies are transient failures the cache retries
next session.  Nothing here raises past :meth:`EasyEdaClient.fetch_component`.
Pacing is the caller's (``acquire`` is called before every request), so the
worker's token bucket counts the per-uuid call like any other.  ``requests`` is
imported on first use; the rest is stdlib.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import time
from typing import Any

from .easyeda_parse import (
    ComponentRecord,
    FootprintRecord,
    parse_component_response,
    parse_puuid_response,
)

COMPONENT_URL = "https://easyeda.com/api/products/{lcsc}/components"
FOOTPRINT_URLS = (
    "https://easyeda.com/api/components/{puuid}",
    "https://pro.easyeda.com/api/components/{puuid}",
)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}
RETRYABLE_STATUSES = frozenset({403, 429, 500, 502, 503, 504})
BACKOFF_S = (60.0, 120.0, 240.0)
MAX_RETRY_AFTER_S = 600.0
TIMEOUT_S = 20.0


class FetchError(Exception):
    """A request that yielded no usable body; ``transient`` says whether a retry may help."""

    def __init__(self, message: str, transient: bool) -> None:
        super().__init__(message)
        self.transient = transient


@dataclass
class Fetched:
    """One part's fetch outcome: the record (status ``error`` on failure) and the failure kind."""

    record: ComponentRecord
    transient: bool = False
    requests: int = 0


def _default_get(url: str, headers: dict, timeout: float) -> Any:
    """Perform the HTTP GET with requests, imported here so the package loads without it."""
    import requests  # noqa: PLC0415

    return requests.get(url, headers=headers, timeout=timeout)


def _default_wait(seconds: float) -> bool:
    """Sleep; never interrupted."""
    time.sleep(seconds)
    return False


def retry_after_seconds(headers: Any) -> float | None:
    """Return a numeric ``Retry-After`` header in seconds, capped, or None."""
    try:
        value = headers.get("Retry-After") if headers is not None else None
    except AttributeError:
        return None
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    if seconds <= 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_S)


class EasyEdaClient:
    """Fetch and parse EasyEDA responses; test doubles replace ``get``, ``wait`` and ``acquire``."""

    def __init__(
        self,
        get: Callable[..., Any] | None = None,
        wait: Callable[[float], bool] | None = None,
        acquire: Callable[[], bool] | None = None,
        backoff: tuple[float, ...] = BACKOFF_S,
        timeout: float = TIMEOUT_S,
    ) -> None:
        self.get = get or _default_get
        # wait(seconds) returns True when the caller wants to stop instead of waiting on.
        self.wait = wait or _default_wait
        # acquire() blocks for the pacing token; False means stop without requesting.
        self.acquire = acquire or (lambda: True)
        self.backoff = tuple(backoff)
        self.timeout = timeout
        self.requests = 0

    def _get_json(self, url: str) -> Any:
        """Return the decoded body, retrying rate limits and server errors with backoff."""
        for delay in (*self.backoff, None):
            if not self.acquire():
                raise FetchError("stopped before the request", transient=True)
            self.requests += 1
            try:
                response = self.get(url, headers=HEADERS, timeout=self.timeout)
            except (
                Exception
            ) as error:  # requests' own hierarchy; SSL and timeouts included
                raise FetchError(f"network error: {error}", transient=True) from error
            status = int(getattr(response, "status_code", 0))
            if status in RETRYABLE_STATUSES:
                if delay is None:
                    raise FetchError(f"HTTP {status} after retries", transient=True)
                pause = retry_after_seconds(getattr(response, "headers", None)) or delay
                if self.wait(pause):
                    raise FetchError(
                        f"HTTP {status}; stopped while backing off", transient=True
                    )
                continue
            if status != 200:
                raise FetchError(f"HTTP {status}", transient=status >= 500)
            try:
                return response.json()
            except ValueError as error:
                raise FetchError(f"invalid JSON: {error}", transient=True) from error
        raise AssertionError("unreachable: the last attempt returns or raises")

    def fetch_footprint(self, puuid: str) -> FootprintRecord:
        """Fetch one footprint by uuid, trying each host until one has it; never raises."""
        last = FootprintRecord(puuid=puuid, error="no footprint host answered")
        for url in FOOTPRINT_URLS:
            try:
                body = self._get_json(url.format(puuid=puuid))
            except FetchError as error:
                last = FootprintRecord(puuid=puuid, error=str(error))
                if "stopped" in str(error):
                    return last
                continue
            record = parse_puuid_response(body, puuid)
            if record.status == "ok":
                return record
            last = record
        return last

    def fetch_component(self, lcsc: str) -> Fetched:
        """Fetch one part; fall back to the per-uuid endpoint when the footprint is missing."""
        before = self.requests
        try:
            body = self._get_json(COMPONENT_URL.format(lcsc=lcsc))
        except FetchError as error:
            record = ComponentRecord(lcsc=lcsc, status="error", error=str(error))
            return Fetched(
                record, transient=error.transient, requests=self.requests - before
            )
        record = parse_component_response(body, lcsc)
        if record.status == "ok" and not record.pads and record.puuid:
            footprint = self.fetch_footprint(record.puuid)
            if footprint.status == "ok":
                record.package_name = footprint.package_name or record.package_name
                record.pads = footprint.pads
                record.footprint_shapes = footprint.footprint_shapes
                record.footprint_source = "puuid-endpoint"
                record.skipped_shapes += footprint.skipped_shapes
                record.error = ""
            else:
                record.error = (
                    "response without a footprint and the per-uuid endpoint gave "
                    f"{footprint.status}: {footprint.error or 'no detail'}"
                )
        return Fetched(record, transient=False, requests=self.requests - before)
