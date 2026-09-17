"""Tests for the fetch worker: token bucket, queue, breaker, isolation, stopping."""

import threading
from types import SimpleNamespace

from jlcfootprint.worker import (
    BURST,
    INTERVAL_S,
    MAX_CONSECUTIVE_FAILURES,
    FetchWorker,
    TokenBucket,
)


class Clock:
    """A clock that only advances when something waits on it."""

    def __init__(self):
        self.now = 1000.0
        self.waits = []

    def __call__(self):
        """Return the current fake time."""
        return self.now

    def wait(self, seconds):
        """Advance by the pause and report that nobody asked to stop."""
        self.waits.append(seconds)
        self.now += seconds
        return False


def test_token_bucket_allows_a_burst_then_one_per_interval():
    """Ten tokens are free; the eleventh waits for the next refill; refills accrue while idle."""
    clock = Clock()
    bucket = TokenBucket(clock=clock, wait=clock.wait, jitter=lambda: 0.0)
    for _ in range(BURST):
        assert bucket.take()
    assert clock.waits == []
    assert bucket.take()
    assert clock.waits == [INTERVAL_S]
    assert bucket.take()
    assert clock.waits == [INTERVAL_S, INTERVAL_S]
    clock.now += 3 * INTERVAL_S
    for _ in range(3):
        assert bucket.take()
    assert clock.waits == [INTERVAL_S, INTERVAL_S]
    assert bucket.take()
    assert clock.waits == [INTERVAL_S, INTERVAL_S, INTERVAL_S]


def test_token_bucket_jitter_and_stop():
    """Jitter stretches the spacing; a stop request during the wait gives no token."""
    clock = Clock()
    bucket = TokenBucket(clock=clock, wait=clock.wait, jitter=lambda: 0.5, burst=1)
    assert bucket.take()
    assert bucket.take()
    assert clock.waits == [INTERVAL_S + 1.0]
    stopping = TokenBucket(
        clock=clock, wait=lambda seconds: True, jitter=lambda: 0.0, burst=1
    )
    assert stopping.take()
    assert not stopping.take()


def _worker(fetch, on_done=None, on_tripped=None):
    clock = Clock()
    done = []
    worker = FetchWorker(
        fetch,
        on_done or (lambda code, fetched: done.append((code, fetched))),
        on_tripped=on_tripped,
        clock=clock,
        jitter=lambda: 0.0,
    )
    worker.bucket.wait = clock.wait
    return worker, done, clock


def test_queue_deduplicates_and_reports_each_result():
    """Codes are fetched once each in order; results reach the callback; pending empties."""
    worker, done, _ = _worker(
        lambda code: SimpleNamespace(record=code, transient=False)
    )
    assert worker.enqueue(["C1", "C2", "C1", "", "C3"]) == 3
    assert worker.enqueue(["C2"]) == 0
    assert worker.pending() == {"C1", "C2", "C3"}
    assert worker.run_pending() == 3
    assert [code for code, _ in done] == ["C1", "C2", "C3"]
    assert worker.pending() == set()
    assert not worker.tripped


def test_breaker_trips_after_three_transient_failures():
    """The breaker leaves later parts pending and reports once; a success resets the count."""
    outcomes = {"C1": True, "C2": False, "C3": True, "C4": True, "C5": True, "C6": True}
    messages = []
    worker, done, _ = _worker(
        lambda code: SimpleNamespace(record=code, transient=outcomes[code]),
        on_tripped=messages.append,
    )
    worker.enqueue(["C1", "C2", "C3", "C4", "C5", "C6"])
    worker.run_pending()
    assert [code for code, _ in done] == ["C1", "C2", "C3", "C4", "C5"]
    assert worker.tripped
    assert worker.pending() == {"C6"}
    assert len(messages) == 1
    assert "3 requests in a row" in messages[0]
    assert "1 part(s)" in messages[0]
    assert worker.consecutive_failures == MAX_CONSECUTIVE_FAILURES
    assert worker.enqueue(["C7"]) == 1
    assert worker.run_pending() == 0


def test_exceptions_are_isolated():
    """A fetch that raises counts as a transient failure; a callback that raises is swallowed."""
    calls = []

    def fetch(code):
        calls.append(code)
        if code == "C1":
            raise RuntimeError("boom")
        return SimpleNamespace(record=code, transient=False)

    def on_done(code, fetched):
        if code == "C2":
            raise RuntimeError("handler")

    worker, _, _ = _worker(fetch, on_done=on_done)
    worker.enqueue(["C1", "C2", "C3"])
    assert worker.run_pending() == 3
    assert calls == ["C1", "C2", "C3"]
    assert worker.consecutive_failures == 0


def test_thread_processes_and_stops():
    """The daemon thread drains the queue and joins on stop; stop interrupts a wait."""
    processed = threading.Event()
    worker = FetchWorker(
        lambda code: SimpleNamespace(record=code, transient=False),
        lambda code, fetched: processed.set(),
        jitter=lambda: 0.0,
    )
    worker.start()
    worker.enqueue(["C1"])
    assert processed.wait(5.0)
    assert not worker.wait_for(0.01)
    worker.stop()
    assert not worker.is_running()
    assert worker.wait_for(0.0)
    assert not worker.acquire() or worker.bucket.tokens >= 0


def test_a_shared_bucket_waits_on_the_asking_worker():
    """Two workers share one budget; each waits on its own stop event."""
    clock = Clock()
    bucket = TokenBucket(clock=clock, jitter=lambda: 0.0)
    first = FetchWorker(lambda code: None, lambda code, fetched: None, bucket=bucket)
    second = FetchWorker(lambda code: None, lambda code, fetched: None, bucket=bucket)
    assert first.bucket is second.bucket
    for _ in range(BURST):
        assert bucket.take()
    first.stop()
    second.stop()
    assert not second.acquire()
    assert not first.acquire()
