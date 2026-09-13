"""Record EasyEDA per-LCSC responses as test fixtures (dev-time only; needs requests).

Usage:
    python3 scripts/fetch_easyeda_fixture.py C2132 C1978115 [...] [--out DIR] [--interval 10]

Existing files are skipped, so re-running costs nothing.  One request per
``interval`` seconds (EasyEDA returned 403 after about 18 requests at 1.5 s;
10 s has been safe); 403/429/5xx back off 60, 120, 240 s, then that part is
given up and the rest continue.  Exits 1 when any part failed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

import requests

URL = "https://easyeda.com/api/products/{lcsc}/components"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}
RETRYABLE = frozenset({403, 429, 500, 502, 503, 504})
DEFAULT_OUT = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "jlcfootprint"
    / "easyeda"
)


def fetch(lcsc: str, retries: int = 3) -> dict:
    """Return the decoded response body for one part, backing off on rate limits."""
    delay = 60.0
    for attempt in range(retries + 1):
        response = requests.get(URL.format(lcsc=lcsc), headers=HEADERS, timeout=20)
        if response.status_code in RETRYABLE and attempt < retries:
            print(f"{lcsc}: HTTP {response.status_code}, sleeping {delay:.0f}s")
            time.sleep(delay)
            delay *= 2
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"{lcsc}: gave up after {retries} retries")


def main(argv: list[str] | None = None) -> int:
    """Record each requested part unless its fixture already exists."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lcsc", nargs="+", help="LCSC part numbers, e.g. C2132")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    failed: list[str] = []
    for lcsc in args.lcsc:
        path = args.out / f"{lcsc}.json"
        if path.exists():
            print(f"{lcsc}: exists")
            continue
        try:
            body = fetch(lcsc)
        except (requests.RequestException, RuntimeError, ValueError) as error:
            print(f"{lcsc}: FAILED ({error})")
            failed.append(lcsc)
            continue
        path.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
        print(f"{lcsc}: {'ok' if body.get('result') else 'none'}")
        time.sleep(args.interval)
    if failed:
        print(f"failed: {' '.join(failed)} (re-run later; existing files are kept)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
