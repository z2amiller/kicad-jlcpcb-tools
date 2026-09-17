"""Tests for the EasyEDA client with the network faked: fallback, backoff, failure kinds."""

import copy
import json
from pathlib import Path

import pytest

from jlcfootprint.easyeda_client import (
    BACKOFF_S,
    COMPONENT_URL,
    FOOTPRINT_URLS,
    EasyEdaClient,
    retry_after_seconds,
)

FIXTURES = Path(__file__).parent / "fixtures" / "jlcfootprint"
UUID = "b3b82869fa924bae820e3a6cfb44d689"


class Response:
    """The three things the client reads from a response."""

    def __init__(self, status_code=200, body=None, headers=None):
        self.status_code = status_code
        self.body = body
        self.headers = headers or {}

    def json(self):
        """Return the body, or fail like requests does on a non-JSON page."""
        if self.body is None:
            raise ValueError("not JSON")
        return self.body


class Network:
    """Serve queued responses and record every URL, wait and token taken."""

    def __init__(self, *responses):
        self.queue = list(responses)
        self.urls = []
        self.waits = []
        self.tokens = 0
        self.stop_on_wait = False
        self.allow_tokens = True

    def get(self, url, headers=None, timeout=None):
        """Pop the next queued response, raising when it is an exception."""
        self.urls.append(url)
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def wait(self, seconds):
        """Record the pause; a stop request interrupts it."""
        self.waits.append(seconds)
        return self.stop_on_wait

    def acquire(self):
        """Count the token; refuse when the worker is stopping."""
        self.tokens += 1
        return self.allow_tokens

    def client(self):
        """Build a client bound to this fake network."""
        return EasyEdaClient(get=self.get, wait=self.wait, acquire=self.acquire)


def component_body(lcsc="C2132"):
    """Return a real recorded per-LCSC body."""
    return json.loads((FIXTURES / "easyeda" / f"{lcsc}.json").read_text())


def uuid_body():
    """Return the real recorded classic-host per-uuid body for C2132's footprint."""
    return json.loads((FIXTURES / "easyeda_uuid" / f"uuid_{UUID}.json").read_text())


def without_package(body):
    """Return the body as EasyEDA sends it for the rare part with no footprint block."""
    stripped = copy.deepcopy(body)
    del stripped["result"]["packageDetail"]
    return stripped


def test_component_with_footprint_needs_one_request():
    """A complete response is parsed and costs one token."""
    network = Network(Response(body=component_body()))
    fetched = network.client().fetch_component("C2132")
    assert fetched.record.status == "ok"
    assert fetched.record.puuid == UUID
    assert len(fetched.record.pads) == 3
    assert fetched.record.footprint_source == "component"
    assert not fetched.transient
    assert fetched.requests == 1
    assert network.tokens == 1
    assert network.urls == [COMPONENT_URL.format(lcsc="C2132")]


def test_missing_footprint_falls_back_to_the_classic_uuid_endpoint():
    """No packageDetail: the classic host answers for the classic uuid; the Pro host is not asked."""
    network = Network(
        Response(body=without_package(component_body())), Response(body=uuid_body())
    )
    fetched = network.client().fetch_component("C2132")
    record = fetched.record
    assert record.status == "ok"
    assert record.footprint_source == "puuid-endpoint"
    assert record.package_name == "SOT-23-3_L2.9-W1.6-P1.90-LS2.8-BR"
    assert [p["number"] for p in record.pads] == ["1", "2", "3"]
    assert record.error == ""
    assert fetched.requests == 2
    assert network.urls[1] == FOOTPRINT_URLS[0].format(puuid=UUID)


def test_fallback_tries_the_pro_host_after_the_classic_host_says_not_found():
    """A uuid the classic host lacks is asked from the Pro host; both failing leaves a note."""
    not_found = {"success": False, "code": 404, "message": "Component not found"}
    network = Network(
        Response(body=without_package(component_body())),
        Response(body=not_found),
        Response(body=uuid_body()),
    )
    fetched = network.client().fetch_component("C2132")
    assert fetched.record.status == "ok"
    assert len(fetched.record.pads) == 3
    assert network.urls[1:] == [url.format(puuid=UUID) for url in FOOTPRINT_URLS]

    network = Network(
        Response(body=without_package(component_body())),
        Response(body=not_found),
        Response(body=not_found),
    )
    fetched = network.client().fetch_component("C2132")
    assert fetched.record.status == "ok"
    assert fetched.record.pads == []
    assert "per-uuid endpoint gave none" in fetched.record.error
    assert not fetched.transient


def test_rate_limit_backs_off_with_retry_after_then_the_ladder():
    """403 honours Retry-After, otherwise waits 60, 120, 240 s, then gives up transiently."""
    network = Network(
        Response(403, headers={"Retry-After": "30"}),
        Response(403),
        Response(body=component_body()),
    )
    fetched = network.client().fetch_component("C2132")
    assert fetched.record.status == "ok"
    assert network.waits == [30.0, BACKOFF_S[1]]
    assert fetched.requests == 3

    network = Network(Response(403), Response(429), Response(503), Response(403))
    fetched = network.client().fetch_component("C2132")
    assert fetched.record.status == "error"
    assert fetched.transient
    assert "after retries" in fetched.record.error
    assert network.waits == list(BACKOFF_S)


def test_retry_after_parsing():
    """Only a positive number of seconds counts, capped to keep a session responsive."""
    assert retry_after_seconds({"Retry-After": "12"}) == 12.0
    assert retry_after_seconds({"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}) is None
    assert retry_after_seconds({"Retry-After": "0"}) is None
    assert retry_after_seconds({}) is None
    assert retry_after_seconds(None) is None
    assert retry_after_seconds({"Retry-After": "99999"}) == 600.0


@pytest.mark.parametrize(
    ("item", "transient", "fragment"),
    [
        (ConnectionError("reset"), True, "network error"),
        (Response(200, body=None), True, "invalid JSON"),
        (Response(404), False, "HTTP 404"),
        (Response(418), False, "HTTP 418"),
    ],
)
def test_failure_kinds(item, transient, fragment):
    """Network and body problems retry next session; other HTTP statuses are final."""
    fetched = Network(item).client().fetch_component("C2132")
    assert fetched.record.status == "error"
    assert fetched.transient is transient
    assert fragment in fetched.record.error


def test_stopping_interrupts_a_backoff_and_a_token_wait():
    """Both interruption paths return a transient error without touching the network again."""
    network = Network(Response(403), Response(body=component_body()))
    network.stop_on_wait = True
    fetched = network.client().fetch_component("C2132")
    assert fetched.record.status == "error"
    assert fetched.transient
    assert "stopped" in fetched.record.error
    assert len(network.urls) == 1

    network = Network(Response(body=component_body()))
    network.allow_tokens = False
    fetched = network.client().fetch_component("C2132")
    assert "stopped before the request" in fetched.record.error
    assert network.urls == []
