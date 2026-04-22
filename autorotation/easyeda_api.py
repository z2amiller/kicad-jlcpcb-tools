"""EasyEDA component API client and response parser.

Fetches pad geometry from EasyEDA's public component API and parses it into a
simple ``FootprintResult`` dataclass. The parser is split from the fetch so that
tests can exercise it against on-disk fixtures without any network access.

EasyEDA internal units are 10 mil per unit; one unit is therefore
``10 * 0.0254 = 0.254`` mm. The ``packageDetail.dataStr.head`` object carries
the canvas-local origin for the footprint; we subtract it from pad centers
before scaling so returned coordinates are footprint-local millimetres.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# 10 mil per EasyEDA internal unit -> millimetres.
_UNIT_MM = 10 * 0.0254  # 0.254 mm / unit

_EASYEDA_URL = "https://easyeda.com/api/products/{code}/components"

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class FootprintResult:
    """Result of fetching/parsing an EasyEDA footprint.

    On success, ``pads`` is a list of dicts with keys ``number`` (str),
    ``x`` (float, mm), and ``y`` (float, mm). Coordinates are footprint-local
    (head origin subtracted) in millimetres.
    """

    success: bool = False
    lcsc_code: str = ""
    package: str = ""
    title: str = ""
    pads: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


def _failure(error: str, lcsc_code: str = "") -> FootprintResult:
    return FootprintResult(success=False, lcsc_code=lcsc_code, error=error)


def parse_easyeda_response(json_dict: Any, lcsc_code: str = "") -> FootprintResult:
    """Parse a decoded EasyEDA component API JSON body into a FootprintResult.

    Never raises on malformed input -- returns a failure FootprintResult
    with a descriptive ``error`` instead.
    """
    if not isinstance(json_dict, dict):
        return _failure("response is not a JSON object", lcsc_code)

    if not json_dict:
        return _failure("empty response", lcsc_code)

    if json_dict.get("success") is False:
        msg = json_dict.get("message") or json_dict.get("result") or "api returned success=false"
        return _failure(f"api error: {msg}", lcsc_code)

    result = json_dict.get("result")
    if not isinstance(result, dict):
        return _failure("missing 'result' object", lcsc_code)

    package_detail = result.get("packageDetail")
    if not isinstance(package_detail, dict):
        return _failure("missing 'result.packageDetail'", lcsc_code)

    data_str = package_detail.get("dataStr")
    if not isinstance(data_str, dict):
        return _failure("missing 'packageDetail.dataStr'", lcsc_code)

    head = data_str.get("head")
    if not isinstance(head, dict):
        return _failure("missing 'dataStr.head'", lcsc_code)

    try:
        origin_x = float(head.get("x", 0.0))
        origin_y = float(head.get("y", 0.0))
    except (TypeError, ValueError):
        return _failure("head origin x/y not numeric", lcsc_code)

    c_para = head.get("c_para") if isinstance(head.get("c_para"), dict) else {}
    package_name = c_para.get("package", "") if isinstance(c_para, dict) else ""

    title = package_detail.get("title", "") or ""

    shape = data_str.get("shape")
    if not isinstance(shape, list):
        return _failure("missing 'dataStr.shape' array", lcsc_code)

    pads: List[Dict[str, Any]] = []
    for entry in shape:
        if not isinstance(entry, str) or not entry.startswith("PAD~"):
            continue
        parts = entry.split("~")
        # Need at least through field index 8 (number).
        if len(parts) < 9:
            continue
        try:
            cx = float(parts[2])
            cy = float(parts[3])
        except (TypeError, ValueError):
            continue
        number = parts[8]
        x_mm = (cx - origin_x) * _UNIT_MM
        y_mm = (cy - origin_y) * _UNIT_MM
        pads.append({"number": str(number), "x": x_mm, "y": y_mm})

    if not pads:
        return _failure("no PAD entries found in shape", lcsc_code)

    return FootprintResult(
        success=True,
        lcsc_code=lcsc_code,
        package=str(package_name),
        title=str(title),
        pads=pads,
        error=None,
    )


def fetch_easyeda_pads(lcsc_code: str, timeout: float = 10.0) -> FootprintResult:
    """Fetch and parse an EasyEDA footprint by LCSC part code (e.g. ``"C2132"``).

    Never raises. Network / decode / parse failures are reported through
    ``FootprintResult(success=False, error=...)``.
    """
    if not lcsc_code or not isinstance(lcsc_code, str):
        return _failure("lcsc_code must be a non-empty string", str(lcsc_code or ""))

    url = _EASYEDA_URL.format(code=lcsc_code)
    req = urllib.request.Request(url, headers=_DEFAULT_HEADERS)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        return _failure(f"HTTP {e.code}: {e.reason}", lcsc_code)
    except urllib.error.URLError as e:
        return _failure(f"URL error: {e.reason}", lcsc_code)
    except (TimeoutError, OSError) as e:
        return _failure(f"network error: {e}", lcsc_code)

    try:
        data = json.loads(raw)
    except (ValueError, TypeError) as e:
        return _failure(f"invalid JSON: {e}", lcsc_code)

    return parse_easyeda_response(data, lcsc_code=lcsc_code)
