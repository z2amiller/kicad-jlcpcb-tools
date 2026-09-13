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
    skipped_shapes: int = 0  # PAD~/P~ shapes that could not be read


def parse_symbol_pins(shapes: list[Any]) -> tuple[list[SymbolPin], int]:
    """Return the symbol's pins from its ``P~`` shape strings, plus the count of unreadable ones.

    Sections are separated by ``^^``.  Section 0 is
    ``P~show~locked~spice_number~x~y~rotation~gId~id``; section 3 is
    ``show~x~y~rotation~pin_name~alignment~~~color`` and section 4 is the displayed
    pin number in the same layout.  The displayed number is what the library draws
    next to the pin and what matches the footprint's pad numbers (the SPICE field
    disagrees on some symbols), so it is preferred when present.
    """
    pins: list[SymbolPin] = []
    skipped = 0
    for shape in shapes:
        if not isinstance(shape, str) or not shape.startswith("P~"):
            continue
        sections = shape.split("^^")
        header = sections[0].split("~")
        if len(header) < 4:
            skipped += 1
            continue
        number = header[3].strip()
        label = ""
        if len(sections) >= 4:
            name_parts = sections[3].split("~")
            if len(name_parts) >= 5:
                label = name_parts[4].strip()
        if len(sections) >= 5:
            number_parts = sections[4].split("~")
            if len(number_parts) >= 5 and number_parts[4].strip():
                number = number_parts[4].strip()
        pins.append(SymbolPin(number=_normal_number(number), label=label))
    return pins, skipped


def _normal_number(number: str) -> str:
    """Strip leading zeros from purely numeric pin numbers so ``01`` and ``1`` agree."""
    return (number.lstrip("0") or "0") if number.isdigit() else number


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
) -> tuple[list[dict], int]:
    """Return raw pad dicts from ``PAD~`` shapes (origin subtracted, canvas units) and the skipped count.

    Fields: 1 shape, 2 x, 3 y, 4 width, 5 height, 6 layer, 8 number, 9 hole radius,
    11 rotation.
    """
    pads: list[dict] = []
    skipped = 0
    for shape in shapes:
        if not isinstance(shape, str) or not shape.startswith("PAD~"):
            continue
        parts = shape.split("~")
        if len(parts) < 9:
            skipped += 1
            continue
        try:
            x, y, w, h = (float(parts[i]) for i in (2, 3, 4, 5))
        except ValueError:
            skipped += 1
            continue
        rotation = 0.0
        if len(parts) > 11 and parts[11]:
            try:
                rotation = float(parts[11])
            except ValueError:
                skipped += 1
                continue
        hole = 0.0
        if len(parts) > 9 and parts[9]:
            try:
                hole = float(parts[9])
            except ValueError:
                hole = 0.0
        pads.append(
            {
                "number": _normal_number(parts[8].strip()),
                "x": x - origin_x,
                "y": y - origin_y,
                "w": w,
                "h": h,
                "rotation": rotation,
                "shape": parts[1],
                "layer": parts[6],
                "hole": hole,
            }
        )
    return pads, skipped


# 3D-model nodes are artwork, not geometry; a graphics-based polarity reader needs
# the drawing commands only, and the nodes are half the bytes of a footprint.
_DROPPED_SHAPE_PREFIXES = ("SVGNODE~",)

# ``success: false`` bodies that mean "no such component" rather than a server problem.
_NOT_FOUND_CODES = frozenset({None, 0, 404, "404"})


def _dict(value: Any) -> dict:
    """Return ``value`` when it is a dict, else an empty dict (malformed input never raises)."""
    return value if isinstance(value, dict) else {}


def _decode_data_str(value: Any) -> dict:
    """Return a drawing's ``dataStr`` as a dict, decoding a JSON string when EasyEDA sends one."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return _dict(value)


def _shape_list(container: dict) -> list[str]:
    """Return the string entries of a ``shape`` array, without 3D-model nodes."""
    shapes = container.get("shape") if isinstance(container.get("shape"), list) else []
    return [
        s
        for s in shapes
        if isinstance(s, str) and not s.startswith(_DROPPED_SHAPE_PREFIXES)
    ]


def parse_component_response(body: Any, lcsc: str) -> ComponentRecord:
    """Classify and parse one per-LCSC response body.

    ``success: true`` with a null or empty ``result`` means EasyEDA has no component
    for the part (the checkerboard case): status ``none``.  ``success: false`` is
    ``none`` only when it carries no code or a not-found code; any other code is an
    ``error`` with the code and message kept, so the cache retries it rather than
    parking it for a month.  Anything malformed is ``error`` and never raises.  A
    record without a puuid is also ``error``.
    """
    if not isinstance(body, dict):
        return ComponentRecord(
            lcsc=lcsc, status="error", error="response is not a JSON object"
        )
    if body.get("success") is False:
        code = body.get("code")
        detail = f"code {code}: {body.get('message', '')}".strip(": ")
        if code in _NOT_FOUND_CODES:
            return ComponentRecord(lcsc=lcsc, status="none", error=detail)
        return ComponentRecord(lcsc=lcsc, status="error", error=detail)
    result = body.get("result")
    if result in (None, {}, [], ""):
        return ComponentRecord(lcsc=lcsc, status="none")
    if not isinstance(result, dict):
        return ComponentRecord(
            lcsc=lcsc, status="error", error="result is not an object"
        )

    data_str = _decode_data_str(result.get("dataStr"))
    head = _dict(data_str.get("head"))
    c_para = _dict(head.get("c_para"))

    record = ComponentRecord(
        lcsc=lcsc,
        status="ok",
        symbol_uuid=str(head.get("uuid") or result.get("uuid") or ""),
        symbol_shapes=_shape_list(data_str),
        puuid=str(head.get("puuid") or ""),
    )
    record.symbol_pins, skipped_pins = parse_symbol_pins(record.symbol_shapes)
    record.skipped_shapes += skipped_pins

    package = _dict(result.get("packageDetail"))
    package_data = _decode_data_str(package.get("dataStr"))
    if package_data:
        package_head = _dict(package_data.get("head"))
        record.package_name = str(
            package.get("title")
            or _dict(package_head.get("c_para")).get("package")
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
            record.error = (
                "footprint origin unreadable; pads left in canvas coordinates"
            )
        record.pads, skipped_pads = parse_footprint_pads(
            record.footprint_shapes, origin_x, origin_y
        )
        record.skipped_shapes += skipped_pads
        record.footprint_source = "component"
        if not record.pads:
            record.error = record.error or "packageDetail without readable pads"
    else:
        record.package_name = str(c_para.get("package") or "")

    if not record.puuid:
        record.status = "error"
        record.error = "no puuid in response"
    return record
