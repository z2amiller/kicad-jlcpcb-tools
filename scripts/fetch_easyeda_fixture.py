"""Record EasyEDA per-LCSC responses as test fixtures (dev-time only; needs requests).

Usage:
    python3 scripts/fetch_easyeda_fixture.py C2132 C1978115 [...] [--out DIR] [--interval 10]

Existing files are skipped, so re-running costs nothing.  Requests are spaced
``interval`` seconds apart whatever happened to the previous one (EasyEDA
returned 403 after about 18 requests at 1.5 s; 10 s has been safe).  A
403/429/5xx backs off 60, 120 and 240 s before that part is given up, and three
parts failing in a row stop the run: a server that keeps refusing is left alone.
A response is kept only when the plugin's parser reads it as ``ok`` (data for
the part) or ``none`` (EasyEDA has nothing for it); anything else is a failure
and no file is written.  Exits 1 when any part failed or was not attempted.
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

from jlcfootprint.easyeda_parse import parse_component_response  # noqa: E402

URL = "https://easyeda.com/api/products/{lcsc}/components"
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
DEFAULT_OUT = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "jlcfootprint"
    / "easyeda"
)


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


def fetch(lcsc: str, pace: Pacer) -> Any:
    """Return the decoded response body for one part, backing off on rate limits.

    Raises ``requests.RequestException`` (the last retryable status included) or
    ``ValueError`` (a body that is not JSON).  The parser classifies whatever comes back.
    """
    for delay in (*BACKOFF_S, None):
        pace()
        response = requests.get(URL.format(lcsc=lcsc), headers=HEADERS, timeout=20)
        if response.status_code in RETRYABLE and delay is not None:
            print(f"{lcsc}: HTTP {response.status_code}, sleeping {delay:.0f}s")
            time.sleep(delay)
            continue
        response.raise_for_status()
        return response.json()
    raise AssertionError("unreachable: the last attempt returns or raises")


def record(lcsc: str, out: Path, pace: Pacer) -> str:
    """Fetch one part, keep the body when the parser accepts it, and return its status."""
    body = fetch(lcsc, pace)
    parsed = parse_component_response(body, lcsc)
    if parsed.status not in ("ok", "none"):
        raise ValueError(
            f"parser says {parsed.status!r}: {parsed.error or 'no detail'}"
        )
    path = out / f"{lcsc}.json"
    partial = path.with_name(f"{lcsc}.json.part")
    partial.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    partial.replace(path)
    return parsed.status


def main(argv: list[str] | None = None) -> int:
    """Record each requested part unless its fixture already exists."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("lcsc", nargs="+", help="LCSC part numbers, e.g. C2132")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args(argv)
    malformed = [code for code in args.lcsc if not LCSC_RE.match(code)]
    if malformed:
        parser.error(
            f"not LCSC codes (expected C followed by digits): {' '.join(malformed)}"
        )
    args.out.mkdir(parents=True, exist_ok=True)
    pace = Pacer(args.interval)
    failed: list[str] = []
    not_attempted: list[str] = []
    consecutive = 0
    for index, lcsc in enumerate(args.lcsc):
        if (args.out / f"{lcsc}.json").exists():
            print(f"{lcsc}: exists")
            continue
        try:
            print(f"{lcsc}: {record(lcsc, args.out, pace)}")
            consecutive = 0
        except (requests.RequestException, ValueError) as error:
            print(f"{lcsc}: FAILED ({error})")
            failed.append(lcsc)
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                not_attempted = [
                    code
                    for code in args.lcsc[index + 1 :]
                    if not (args.out / f"{code}.json").exists()
                ]
                print(
                    f"{consecutive} parts failed in a row; stopping so EasyEDA is not hammered"
                )
                break
    if failed:
        print(f"failed: {' '.join(failed)} (re-run later; existing files are kept)")
    if not_attempted:
        print(f"not attempted: {' '.join(not_attempted)}")
    return 1 if failed or not_attempted else 0


if __name__ == "__main__":
    sys.exit(main())
