"""EasyEDA Pro client: the batch device lookup and per-uuid documents (spec section 15).

Three calls, all on ``pro.easyeda.com``.  ``search_by_codes`` posts up to 200 LCSC
codes and gets each known part's symbol uuid, footprint uuid and package name;
``fetch_footprint`` and ``fetch_symbol`` get one document by its uuid.  403/429/5xx
back off with ``Retry-After`` when present, else 60, 120 and 240 s; network errors
and malformed bodies are transient failures the cache retries next session; an
HTTP 404 is ``none`` (nothing there).  Nothing here raises past the three calls.
Pacing is the caller's: ``acquire`` is called before every request, retries
included, so the worker's buckets count each one; ``wait(seconds)`` returns True to
abandon a backoff when the plugin closes.  ``requests`` is imported on first use;
the rest is stdlib.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
import logging
import time
from typing import Any, Union

from .easyeda_parse import (
    DevicesResult,
    FootprintRecord,
    SymbolRecord,
    parse_devices_response,
    parse_puuid_response,
    parse_symbol_response,
)

logger = logging.getLogger(__name__)

DEVICES_URL = "https://pro.easyeda.com/api/devices/searchByCodes"
DOCUMENT_URL = "https://pro.easyeda.com/api/components/{uuid}"
# The LCSC library's path on the Pro host; the batch lookup searches inside it.
LCSC_COMPANY_PATH = "0819f05c4eef4c71ace90d822a990e87"
BATCH_SIZE = 200
FOOTPRINT = "footprint"
SYMBOL = "symbol"
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

    def __init__(self, message: str, transient: bool, status: int = 0) -> None:
        super().__init__(message)
        self.transient = transient
        self.status = status


@dataclass
class Lookup:
    """One batch call's outcome: the devices found and the codes not found, or a failure."""

    codes: list[str]
    devices: DevicesResult = field(default_factory=DevicesResult)
    transient: bool = False  # True when the whole chunk should be asked again
    requests: int = 0

    @property
    def failed(self) -> bool:
        """Return True when the answer classified nothing."""
        return bool(self.devices.error)

    @property
    def error(self) -> str:
        """Return the failure text, empty on success."""
        return self.devices.error


@dataclass
class Document:
    """One per-uuid call's outcome: a footprint or a symbol record, and the failure kind."""

    kind: str  # 'footprint' | 'symbol'
    uuid: str
    record: Union[FootprintRecord, SymbolRecord]
    transient: bool = False
    requests: int = 0


def _default_get(url: str, headers: dict, timeout: float) -> Any:
    """Perform the HTTP GET with requests, imported here so the package loads without it."""
    import requests  # noqa: PLC0415

    return requests.get(url, headers=headers, timeout=timeout)


def _default_post(url: str, headers: dict, json_body: Any, timeout: float) -> Any:
    """Perform the HTTP POST with a JSON body through requests."""
    import requests  # noqa: PLC0415

    return requests.post(url, headers=headers, json=json_body, timeout=timeout)


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
    """Fetch and parse Pro-host answers; test doubles replace ``get``, ``post``, ``wait``, ``acquire``."""

    def __init__(
        self,
        get: Callable[..., Any] | None = None,
        post: Callable[..., Any] | None = None,
        wait: Callable[[float], bool] | None = None,
        acquire: Callable[[], bool] | None = None,
        backoff: tuple[float, ...] = BACKOFF_S,
        timeout: float = TIMEOUT_S,
    ) -> None:
        self.get = get or _default_get
        self.post = post or _default_post
        # wait(seconds) returns True when the caller wants to stop instead of waiting on.
        self.wait = wait or _default_wait
        # acquire() blocks for the pacing token; False means stop without requesting.
        self.acquire = acquire or (lambda: True)
        self.backoff = tuple(backoff)
        self.timeout = timeout
        self.requests = 0

    def _request(self, url: str, json_body: Any = None) -> Any:
        """Return the decoded body, retrying rate limits and server errors with backoff.

        A POST when ``json_body`` is given, else a GET.
        """
        for delay in (*self.backoff, None):
            if not self.acquire():
                raise FetchError("stopped before the request", transient=True)
            self.requests += 1
            try:
                if json_body is None:
                    response = self.get(url, headers=HEADERS, timeout=self.timeout)
                else:
                    response = self.post(
                        url, headers=HEADERS, json_body=json_body, timeout=self.timeout
                    )
            except (
                Exception
            ) as error:  # requests' own hierarchy; SSL and timeouts included
                raise FetchError(f"network error: {error}", transient=True) from error
            status = int(getattr(response, "status_code", 0))
            if status in RETRYABLE_STATUSES:
                if delay is None:
                    raise FetchError(
                        f"HTTP {status} after retries", transient=True, status=status
                    )
                pause = retry_after_seconds(getattr(response, "headers", None)) or delay
                logger.info(
                    "jlcfootprint: HTTP %d from EasyEDA; waiting %.0f s before trying again",
                    status,
                    pause,
                )
                if self.wait(pause):
                    raise FetchError(
                        f"HTTP {status}; stopped while backing off",
                        transient=True,
                        status=status,
                    )
                continue
            if status != 200:
                raise FetchError(
                    f"HTTP {status}", transient=status >= 500, status=status
                )
            try:
                return response.json()
            except ValueError as error:
                raise FetchError(f"invalid JSON: {error}", transient=True) from error
        raise AssertionError("unreachable: the last attempt returns or raises")

    def search_by_codes(self, codes: list[str]) -> Lookup:
        """Look up one chunk of LCSC codes; never raises.

        A failed request or an unreadable answer fails the whole chunk transiently,
        so the caller asks again; a well-formed answer classifies every code as a hit
        or a miss.
        """
        codes = list(codes)
        if len(codes) > BATCH_SIZE:
            raise ValueError(f"at most {BATCH_SIZE} codes per lookup, got {len(codes)}")
        before = self.requests
        try:
            body = self._request(
                DEVICES_URL, {"codes": codes, "path": LCSC_COMPANY_PATH}
            )
        except FetchError as error:
            return Lookup(
                codes,
                DevicesResult(error=str(error)),
                transient=error.transient,
                requests=self.requests - before,
            )
        devices = parse_devices_response(body, codes)
        return Lookup(
            codes,
            devices,
            transient=bool(devices.error),
            requests=self.requests - before,
        )

    def fetch_footprint(self, puuid: str) -> Document:
        """Fetch one footprint by uuid; never raises."""
        before = self.requests
        try:
            body = self._request(DOCUMENT_URL.format(uuid=puuid))
        except FetchError as error:
            record = FootprintRecord(puuid=puuid, error=str(error))
            if error.status == 404:
                record.status = "none"
            return Document(
                FOOTPRINT,
                puuid,
                record,
                transient=error.transient,
                requests=self.requests - before,
            )
        return Document(
            FOOTPRINT,
            puuid,
            parse_puuid_response(body, puuid),
            requests=self.requests - before,
        )

    def fetch_symbol(self, uuid: str) -> Document:
        """Fetch one symbol by uuid; never raises."""
        before = self.requests
        try:
            body = self._request(DOCUMENT_URL.format(uuid=uuid))
        except FetchError as error:
            record = SymbolRecord(uuid=uuid, error=str(error))
            if error.status == 404:
                record.status = "none"
            return Document(
                SYMBOL,
                uuid,
                record,
                transient=error.transient,
                requests=self.requests - before,
            )
        return Document(
            SYMBOL,
            uuid,
            parse_symbol_response(body, uuid),
            requests=self.requests - before,
        )
