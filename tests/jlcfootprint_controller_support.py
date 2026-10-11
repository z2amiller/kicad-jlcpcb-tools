"""The footprint check controller over a fresh cache and verdict store, for the tests that drive it."""

from __future__ import annotations

from jlcfootprint.cache import Cache
from jlcfootprint.controller import FootprintCheck
from jlcfootprint.kicad_adapter import BoardPart
from jlcfootprint.verdicts import VerdictStore
from jlcfootprint.worker import Buckets

from .jlcfootprint_fetch_support import Clock, FakeClient, led, sot23
from .jlcfootprint_support import recorded


def controller_setup(tmp_path):
    """Return a controller over a fresh cache and verdict store with a fake client.

    Each test file that drives the controller wraps this in its own pytest fixture,
    so pytest reports the fixture against that file.
    """
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
    # Buckets built on the fake clock so try_take/seconds_until_token refill against
    # clock.now instead of the real wall clock; otherwise run_pending's token wait
    # busy-loops through real seconds while only the worker's own clock is fake.
    buckets = Buckets.default(clock=clock, jitter=lambda: 0.0)
    check = FootprintCheck(
        cache,
        verdicts,
        read_board=lambda: list(board["parts"]),
        post=lambda lcsc, generation: events.append((lcsc, generation)),
        client=client,
        worker=None,
        message=messages.append,
        now=clock,
        buckets=buckets,
    )
    check.worker.wait = clock.wait
    check.worker.clock = clock
    return check, board, events, messages, client
