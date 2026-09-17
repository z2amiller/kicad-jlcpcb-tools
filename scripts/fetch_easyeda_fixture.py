"""Record EasyEDA responses as test fixtures (dev-time only; needs requests).

Usage:
    python3 scripts/fetch_easyeda_fixture.py C2132 C1978115 [...] [--out DIR] [--interval 10]
    python3 scripts/fetch_easyeda_fixture.py --puuid b3b82869fa924bae820e3a6cfb44d689 [--out DIR]

Per-LCSC responses land in ``tests/fixtures/jlcfootprint/easyeda/<lcsc>.json``;
per-uuid footprint responses (the client's fallback when a per-LCSC response
carries no ``packageDetail``) land in ``tests/fixtures/jlcfootprint/easyeda_uuid/
uuid_<uuid>.json`` from the classic host, or ``pro_<uuid>.json`` with ``--pro`` from
the EasyEDA Pro host (the two hosts have separate uuid spaces; a classic per-LCSC
response names a classic uuid).  Existing files are skipped, so re-running costs nothing.
Requests are spaced ``interval`` seconds apart whatever happened to the previous
one (EasyEDA returned 403 after about 18 requests at 1.5 s; 10 s has been safe).
A 403/429/5xx backs off 60, 120 and 240 s before that item is given up, and three
items failing in a row stop the run: a server that keeps refusing is left alone.
A response is kept only when the plugin's parser reads it as ``ok`` (data for
the item) or ``none`` (EasyEDA has nothing for it); anything else is a failure
and no file is written.  Exits 1 when any item failed or was not attempted.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
import time
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.easyeda_parse import (  # noqa: E402
    parse_component_response,
    parse_puuid_response,
)

URL = "https://easyeda.com/api/products/{lcsc}/components"
PUUID_URLS = {
    "classic": "https://easyeda.com/api/components/{puuid}",
    "pro": "https://pro.easyeda.com/api/components/{puuid}",
}
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}
RETRYABLE = frozenset({403, 429, 500, 502, 503, 504})
BACKOFF_S = (60.0, 120.0, 240.0)
MAX_CONSECUTIVE_FAILURES = 3
LCSC_RE = re.compile(r"^C\d+$")
PUUID_RE = re.compile(r"^[0-9a-f]{32}$")
FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint"
DEFAULT_OUT = FIXTURES / "easyeda"
DEFAULT_PUUID_OUT = FIXTURES / "easyeda_uuid"


class Pacer:
    """Sleep ``interval`` seconds before every request but the first, whatever the last one did."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self.requests = 0

    def __call__(self) -> None:
        """Wait out the interval, then count the request about to be made."""
        if self.requests:
            time.sleep(self.interval)
        self.requests += 1


def fetch(url: str, pace: Pacer, label: str) -> Any:
    """Return the decoded response body for one URL, backing off on rate limits.

    Raises ``requests.RequestException`` (the last retryable status included) or
    ``ValueError`` (a body that is not JSON).  The parser classifies whatever comes back.
    """
    for delay in (*BACKOFF_S, None):
        pace()
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code in RETRYABLE and delay is not None:
            print(f"{label}: HTTP {response.status_code}, sleeping {delay:.0f}s")
            time.sleep(delay)
            continue
        response.raise_for_status()
        return response.json()
    raise AssertionError("unreachable: the last attempt returns or raises")


def write_fixture(path: Path, body: Any) -> None:
    """Write the body compactly through a partial file, so a crash leaves no half fixture."""
    partial = path.with_name(path.name + ".part")
    partial.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    partial.replace(path)


def record(lcsc: str, out: Path, pace: Pacer) -> str:
    """Fetch one part, keep the body when the parser accepts it, and return its status."""
    body = fetch(URL.format(lcsc=lcsc), pace, lcsc)
    parsed = parse_component_response(body, lcsc)
    if parsed.status not in ("ok", "none"):
        raise ValueError(
            f"parser says {parsed.status!r}: {parsed.error or 'no detail'}"
        )
    write_fixture(out / f"{lcsc}.json", body)
    return parsed.status


def record_puuid(puuid: str, out: Path, pace: Pacer, host: str = "classic") -> str:
    """Fetch one footprint by uuid, keep the body when the parser accepts it, return its status."""
    body = fetch(PUUID_URLS[host].format(puuid=puuid), pace, puuid)
    parsed = parse_puuid_response(body, puuid)
    if parsed.status not in ("ok", "none"):
        raise ValueError(
            f"parser says {parsed.status!r}: {parsed.error or 'no detail'}"
        )
    write_fixture(out / puuid_fixture_name(puuid, host), body)
    return parsed.status


def puuid_fixture_name(puuid: str, host: str) -> str:
    """Return the fixture file name for one footprint uuid on one host."""
    return f"{'pro' if host == 'pro' else 'uuid'}_{puuid}.json"


def main(argv: list[str] | None = None) -> int:
    """Record each requested item unless its fixture already exists."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("lcsc", nargs="*", help="LCSC part numbers, e.g. C2132")
    parser.add_argument(
        "--puuid",
        nargs="*",
        default=[],
        help="EasyEDA footprint uuids to record from the per-uuid endpoint",
    )
    parser.add_argument(
        "--pro",
        action="store_true",
        help="record --puuid footprints from the EasyEDA Pro host instead of the classic one",
    )
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args(argv)
    if not args.lcsc and not args.puuid:
        parser.error("nothing to record: give LCSC codes or --puuid uuids")
    malformed = [code for code in args.lcsc if not LCSC_RE.match(code)]
    if malformed:
        parser.error(
            f"not LCSC codes (expected C followed by digits): {' '.join(malformed)}"
        )
    malformed = [uuid for uuid in args.puuid if not PUUID_RE.match(uuid)]
    if malformed:
        parser.error(f"not footprint uuids (32 hex digits): {' '.join(malformed)}")
    items: list[tuple[str, Path, Any]] = [
        (code, (args.out or DEFAULT_OUT) / f"{code}.json", record) for code in args.lcsc
    ]
    host = "pro" if args.pro else "classic"
    items += [
        (
            uuid,
            (args.out or DEFAULT_PUUID_OUT) / puuid_fixture_name(uuid, host),
            lambda item, out, pace: record_puuid(item, out, pace, host),
        )
        for uuid in args.puuid
    ]
    for _, path, _ in items:
        path.parent.mkdir(parents=True, exist_ok=True)
    pace = Pacer(args.interval)
    failed: list[str] = []
    not_attempted: list[str] = []
    consecutive = 0
    for index, (item, path, recorder) in enumerate(items):
        if path.exists():
            print(f"{item}: exists")
            continue
        try:
            print(f"{item}: {recorder(item, path.parent, pace)}")
            consecutive = 0
        except (requests.RequestException, ValueError) as error:
            print(f"{item}: FAILED ({error})")
            failed.append(item)
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                not_attempted = [
                    later
                    for later, later_path, _ in items[index + 1 :]
                    if not later_path.exists()
                ]
                print(
                    f"{consecutive} items failed in a row; stopping so EasyEDA is not hammered"
                )
                break
    if failed:
        print(f"failed: {' '.join(failed)} (re-run later; existing files are kept)")
    if not_attempted:
        print(f"not attempted: {' '.join(not_attempted)}")
    return 1 if failed or not_attempted else 0


if __name__ == "__main__":
    sys.exit(main())
