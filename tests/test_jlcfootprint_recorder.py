"""Tests for scripts/fetch_easyeda_fixture.py with the network faked: pacing, backoff, what gets kept."""

import importlib.util
import json
from pathlib import Path

import pytest

requests = pytest.importorskip("requests")

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"


def load_script():
    """Import the recorder script as a module without running its CLI."""
    spec = importlib.util.spec_from_file_location(
        "fetch_easyeda_fixture", ROOT / "scripts" / "fetch_easyeda_fixture.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    """The three things the recorder reads from a response."""

    def __init__(self, status_code=200, body=None, text="<html>challenge</html>"):
        self.status_code = status_code
        self.body = body
        self.text = text

    def raise_for_status(self):
        """Mimic requests: an HTTPError for any 4xx or 5xx."""
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        """Return the body, or fail like requests does on a non-JSON page."""
        if self.body is None:
            raise ValueError("not JSON")
        return self.body


@pytest.fixture
def fake_network(monkeypatch):
    """Serve queued responses, record every request URL and every sleep."""
    recorder = load_script()
    log = {"urls": [], "sleeps": [], "queue": []}

    def get(url, headers=None, timeout=None):
        log["urls"].append(url)
        return log["queue"].pop(0)

    monkeypatch.setattr(recorder.requests, "get", get)
    monkeypatch.setattr(recorder.time, "sleep", log["sleeps"].append)
    return recorder, log


def good_body():
    """Return a real recorded response, one the parser reads as ``ok``."""
    return json.loads((FIXTURES / "C2132.json").read_text(encoding="utf-8"))


def test_requests_are_paced_and_only_usable_bodies_are_kept(tmp_path, fake_network):
    """Interval sleeps sit between requests, never after the last; ok and none are written."""
    recorder, log = fake_network
    log["queue"] = [
        FakeResponse(body=good_body()),
        FakeResponse(body={"success": False, "code": 404, "message": "not found"}),
    ]
    assert (
        recorder.main(["C2132", "C99", "--out", str(tmp_path), "--interval", "10"]) == 0
    )
    assert len(log["urls"]) == 2 and log["urls"][0].endswith("/C2132/components")
    assert log["sleeps"] == [10.0]
    assert json.loads((tmp_path / "C2132.json").read_text())["success"] is True
    assert (tmp_path / "C99.json").exists()
    assert not list(tmp_path.glob("*.part"))


def test_failures_are_paced_too_and_three_in_a_row_stop_the_run(tmp_path, fake_network):
    """Non-retryable failures never burst, an error body is not kept, and the breaker trips."""
    recorder, log = fake_network
    log["queue"] = [
        FakeResponse(body=None),
        FakeResponse(body={"success": False, "code": 500, "message": "server"}),
        FakeResponse(status_code=404),
        FakeResponse(body=good_body()),
    ]
    rc = recorder.main(
        ["C1", "C2", "C3", "C4", "--out", str(tmp_path), "--interval", "10"]
    )
    assert rc == 1
    assert len(log["urls"]) == 3, "the fourth part must not be attempted"
    assert log["sleeps"] == [10.0, 10.0]
    assert not list(tmp_path.iterdir()), "no failed body is written"


def test_rate_limit_backs_off_then_gives_up_that_part(tmp_path, fake_network, capsys):
    """403 backs off 60, 120, 240 s (plus the interval) and the part is reported failed."""
    recorder, log = fake_network
    log["queue"] = [FakeResponse(status_code=403) for _ in range(4)] + [
        FakeResponse(body=good_body())
    ]
    rc = recorder.main(["C1", "C2132", "--out", str(tmp_path), "--interval", "10"])
    assert rc == 1
    assert log["sleeps"] == [60.0, 10.0, 120.0, 10.0, 240.0, 10.0, 10.0]
    assert not (tmp_path / "C1.json").exists()
    assert (tmp_path / "C2132.json").exists()
    assert "C1: FAILED" in capsys.readouterr().out


def test_existing_files_and_bad_codes_cost_no_request(tmp_path, fake_network):
    """A recorded part is skipped without a request; a malformed code stops before any."""
    recorder, log = fake_network
    (tmp_path / "C2132.json").write_text("{}", encoding="utf-8")
    assert recorder.main(["C2132", "--out", str(tmp_path)]) == 0
    assert log["urls"] == []
    with pytest.raises(SystemExit):
        recorder.main(["c2132", "--out", str(tmp_path)])
    assert log["urls"] == []
