"""Background fetch worker: one daemon thread, a token bucket and a breaker (spec section 10).

Parts are queued by LCSC code, de-duplicated, and fetched one at a time.  A token
bucket allows a burst of ten requests, then one every ten seconds with a little
jitter (EasyEDA answers 403 to faster clients; section 2).  Three consecutive
transient failures trip the session's breaker: the remaining parts stay queued
as pending and one message is reported.  Each part is processed in isolation, so
nothing raises out of the thread.  Stdlib only.
"""

from __future__ import annotations

import collections
from collections.abc import Callable, Iterable
import logging
import random
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

BURST = 10
INTERVAL_S = 10.0
JITTER_S = 2.0
MAX_CONSECUTIVE_FAILURES = 3
BREAKER_MESSAGE = (
    "EasyEDA refused {failures} requests in a row; the remaining {pending} part(s) stay "
    "pending until the plugin is reopened."
)


class TokenBucket:
    """Ten tokens to start, one more every interval plus jitter; ``take`` waits for one."""

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        wait: Callable[[float], bool] | None = None,
        jitter: Callable[[], float] = random.random,
        burst: int = BURST,
        interval: float = INTERVAL_S,
    ) -> None:
        self.clock = clock
        # wait(seconds) returns True when the caller should stop instead of waiting.
        self.wait = wait or (lambda seconds: (time.sleep(seconds), False)[1])
        self.jitter = jitter
        self.burst = burst
        self.interval = interval
        self.tokens = float(burst)
        self.next_refill = clock() + self._spacing()
        self.lock = threading.Lock()

    def _spacing(self) -> float:
        return self.interval + self.jitter() * JITTER_S

    def _refill(self) -> None:
        now = self.clock()
        while now >= self.next_refill and self.tokens < self.burst:
            self.tokens += 1
            self.next_refill += self._spacing()
        if self.tokens >= self.burst:
            self.next_refill = max(self.next_refill, now + self._spacing())

    def take(self, wait: Callable[[float], bool] | None = None) -> bool:
        """Block until a token is available and spend it; False when asked to stop.

        ``wait`` is the caller's stop-aware sleep, so a bucket shared by successive
        workers waits on the worker that is asking, not on the one that created it.
        """
        wait = wait or self.wait
        while True:
            with self.lock:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
                pause = max(0.0, self.next_refill - self.clock())
            if wait(pause):
                return False


class FetchWorker:
    """Drain queued LCSC codes through ``fetch`` on a daemon thread, reporting each result."""

    def __init__(
        self,
        fetch: Callable[[str], Any],
        on_done: Callable[[str, Any], None],
        on_tripped: Callable[[str], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
        bucket: TokenBucket | None = None,
    ) -> None:
        self.fetch = fetch
        self.on_done = on_done
        self.on_tripped = on_tripped
        self.stop_event = threading.Event()
        self.bucket = bucket or TokenBucket(
            clock=clock, wait=self.stop_event.wait, jitter=jitter
        )
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.queue: collections.deque = collections.deque()
        self.queued: set[str] = set()
        self.in_flight: str | None = None
        self.consecutive_failures = 0
        self.tripped = False
        self.thread = threading.Thread(
            target=self._run, name="jlcfootprint-fetch", daemon=True
        )

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------

    def enqueue(self, codes: Iterable[str]) -> int:
        """Queue codes not already queued or in flight; return how many were added."""
        added = 0
        with self.lock:
            for code in codes:
                if code and code not in self.queued and code != self.in_flight:
                    self.queue.append(code)
                    self.queued.add(code)
                    added += 1
        if added:
            self.wake.set()
        return added

    def pending(self) -> set[str]:
        """Return the codes still queued or in flight."""
        with self.lock:
            pending = set(self.queued)
            if self.in_flight is not None:
                pending.add(self.in_flight)
            return pending

    def wait_for(self, seconds: float) -> bool:
        """Wait up to ``seconds`` for work; True when the worker was asked to stop."""
        return self.stop_event.wait(seconds)

    def acquire(self) -> bool:
        """Spend a request token, waiting for one; False when stopping."""
        return self.bucket.take(self.stop_event.wait)

    # ------------------------------------------------------------------
    # Thread
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the daemon thread once."""
        if not self.thread.is_alive():
            self.thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Ask the thread to stop (interrupting any backoff) and join it."""
        self.stop_event.set()
        self.wake.set()
        if self.thread.is_alive():
            self.thread.join(timeout)

    def is_running(self) -> bool:
        """Return True while the thread is alive."""
        return self.thread.is_alive()

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self.wake.wait()
            self.wake.clear()
            self.run_pending()

    def run_pending(self) -> int:
        """Process the queue on the calling thread until it is empty, tripped or stopped."""
        done = 0
        while not self.stop_event.is_set() and not self.tripped:
            with self.lock:
                if not self.queue:
                    return done
                code = self.queue.popleft()
                self.queued.discard(code)
                self.in_flight = code
            try:
                self._process(code)
            finally:
                with self.lock:
                    self.in_flight = None
            done += 1
        return done

    def _process(self, code: str) -> None:
        """Fetch one part and report it; a raised exception counts as a transient failure."""
        try:
            fetched = self.fetch(code)
        except Exception as error:
            logger.exception("jlcfootprint: fetching %s raised", code)
            fetched = _Failure(code, str(error))
        transient = bool(getattr(fetched, "transient", False))
        if transient:
            self.consecutive_failures += 1
        else:
            self.consecutive_failures = 0
        try:
            self.on_done(code, fetched)
        except Exception:
            logger.exception("jlcfootprint: result handler raised for %s", code)
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES and not self.tripped:
            self.tripped = True
            with self.lock:
                pending = len(self.queued)
            message = BREAKER_MESSAGE.format(
                failures=self.consecutive_failures, pending=pending
            )
            logger.warning("jlcfootprint: %s", message)
            if self.on_tripped is not None:
                try:
                    self.on_tripped(message)
                except Exception:
                    logger.exception("jlcfootprint: breaker handler raised")


class _Failure:
    """A fetch result standing in for a fetch function that raised."""

    transient = True

    def __init__(self, code: str, error: str) -> None:
        self.code = code
        self.error = error
        self.record = None
