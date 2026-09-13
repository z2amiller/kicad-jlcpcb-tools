"""Parse recorded EasyEDA component responses (no network; the client is M1).

The per-LCSC response carries two drawings.  ``result.dataStr`` is the schematic
symbol, per part, whose ``P~`` shapes carry pin numbers and optional labels such
as ``A`` and ``K``.  ``result.packageDetail.dataStr`` is the footprint, per
puuid, whose ``PAD~`` shapes carry the pad geometry.  Spec section 2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

# Label sets shared with the crawler's extract_pin1_polarity.
PIN1_CATHODE_LABELS = frozenset({"K", "C", "CA", "CAT", "CATHODE", "K1", "NEG", "-"})
PIN1_ANODE_LABELS = frozenset({"A", "AN", "ANODE", "A1", "AK", "POS", "+"})


@dataclass
class SymbolPin:
    """One schematic-symbol pin: its number and its label text (may be empty)."""

    number: str
    label: str


@dataclass
class ComponentRecord:
    """Everything the resolver needs from one per-LCSC EasyEDA response."""

    lcsc: str
    status: str  # 'ok' | 'none' | 'error'
    error: str = ""
    symbol_uuid: str = ""
    symbol_pins: list[SymbolPin] = field(default_factory=list)
    symbol_shapes: list[str] = field(default_factory=list)
    puuid: str = ""
    package_name: str = ""
    pads: list[dict] = field(
        default_factory=list
    )  # raw canvas units, origin subtracted
    footprint_shapes: list[str] = field(default_factory=list)
    footprint_source: str = ""  # 'component' | 'puuid-endpoint' | ''


def parse_symbol_pins(shapes: list[Any]) -> list[SymbolPin]:
    """Return the symbol's pins from its ``P~`` shape strings.

    Sections are separated by ``^^``.  Section 0 is
    ``P~show~locked~pin_number~x~y~rotation~gId~id``; section 3 is
    ``show~x~y~rotation~pin_name~alignment~~~color``.
    """
    pins: list[SymbolPin] = []
    for shape in shapes:
        if not isinstance(shape, str) or not shape.startswith("P~"):
            continue
        sections = shape.split("^^")
        header = sections[0].split("~")
        if len(header) < 4:
            continue
        label = ""
        if len(sections) >= 4:
            name_parts = sections[3].split("~")
            if len(name_parts) >= 5:
                label = name_parts[4].strip()
        pins.append(SymbolPin(number=header[3].strip(), label=label))
    return pins


def pin1_polarity(pins: list[SymbolPin]) -> str | None:
    """Return 'K', 'A' or None for pin 1, using the crawler's label sets."""
    for pin in pins:
        if pin.number != "1":
            continue
        label = pin.label.upper()
        if label in PIN1_CATHODE_LABELS:
            return "K"
        if label in PIN1_ANODE_LABELS:
            return "A"
    return None


def parse_footprint_pads(
    shapes: list[Any], origin_x: float, origin_y: float
) -> list[dict]:
    """Return raw pad dicts from ``PAD~`` shapes, origin subtracted, still in canvas units."""
    pads: list[dict] = []
    for shape in shapes:
        if not isinstance(shape, str) or not shape.startswith("PAD~"):
            continue
        parts = shape.split("~")
        if len(parts) < 9:
            continue
        try:
            x, y, w, h = (float(parts[i]) for i in (2, 3, 4, 5))
        except ValueError:
            continue
        rotation = 0.0
        if len(parts) > 11 and parts[11]:
            try:
                rotation = float(parts[11])
            except ValueError:
                rotation = 0.0
        pads.append(
            {
                "number": parts[8].strip(),
                "x": x - origin_x,
                "y": y - origin_y,
                "w": w,
                "h": h,
                "rotation": rotation,
                "shape": parts[1],
            }
        )
    return pads


def _shape_list(container: dict) -> list[str]:
    """Return the string entries of a ``shape`` array."""
    return [s for s in (container.get("shape") or []) if isinstance(s, str)]


def parse_component_response(body: Any, lcsc: str) -> ComponentRecord:
    """Classify and parse one per-LCSC response body.

    ``success: false`` or a null ``result`` means EasyEDA has no component for
    the part (the checkerboard case): status ``none``.  Anything malformed is
    ``error``.  A record without a puuid is also ``error``.
    """
    if not isinstance(body, dict):
        return ComponentRecord(
            lcsc=lcsc, status="error", error="response is not a JSON object"
        )
    if body.get("success") is False or body.get("result") in (None, {}, []):
        return ComponentRecord(lcsc=lcsc, status="none")
    result = body.get("result")
    if not isinstance(result, dict):
        return ComponentRecord(
            lcsc=lcsc, status="error", error="result is not an object"
        )

    data_str = result.get("dataStr") or {}
    if isinstance(data_str, str):
        try:
            data_str = json.loads(data_str)
        except ValueError:
            data_str = {}
    head = data_str.get("head") or {}
    c_para = head.get("c_para") or {}

    record = ComponentRecord(
        lcsc=lcsc,
        status="ok",
        symbol_uuid=str(head.get("uuid") or result.get("uuid") or ""),
        symbol_shapes=_shape_list(data_str),
        puuid=str(head.get("puuid") or ""),
    )
    record.symbol_pins = parse_symbol_pins(record.symbol_shapes)

    package = result.get("packageDetail") or {}
    package_data = package.get("dataStr")
    if isinstance(package_data, dict):
        package_head = package_data.get("head") or {}
        record.package_name = str(
            package.get("title")
            or (package_head.get("c_para") or {}).get("package")
            or c_para.get("package")
            or ""
        )
        record.puuid = record.puuid or str(package.get("uuid") or "")
        record.footprint_shapes = _shape_list(package_data)
        try:
            origin_x = float(package_head.get("x", 0.0) or 0.0)
            origin_y = float(package_head.get("y", 0.0) or 0.0)
        except (TypeError, ValueError):
            origin_x = origin_y = 0.0
        record.pads = parse_footprint_pads(record.footprint_shapes, origin_x, origin_y)
        record.footprint_source = "component"
    else:
        record.package_name = str(c_para.get("package") or "")

    if not record.puuid:
        record.status = "error"
        record.error = "no puuid in response"
    return record
