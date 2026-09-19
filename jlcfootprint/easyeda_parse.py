"""Parse EasyEDA responses (no network here; the client fetches them).

The classic per-LCSC response carries two drawings.  ``result.dataStr`` is the
schematic symbol, per part, whose ``P~`` shapes carry pin numbers and optional
labels such as ``A`` and ``K``.  ``result.packageDetail.dataStr`` is the
footprint, per puuid, whose ``PAD~`` shapes carry the pad geometry.  Spec
section 2.  The EasyEDA Pro host answers the batch device lookup with one
record per known part (symbol uuid, footprint uuid, package name) and the
per-uuid endpoint with one document, a footprint or a symbol, in the Pro text
form (newline-delimited JSON arrays).  Spec section 15.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from .records import PIN1_ANODE_LABELS, PIN1_CATHODE_LABELS, SymbolPin

# PIN1_CATHODE_LABELS, PIN1_ANODE_LABELS and SymbolPin live in .records, shared
# with the resolver core; re-exported here so this module's own names are unchanged.


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
    # The classic drawing's head x/y, subtracted from its pads; (0, 0) for Pro text,
    # whose coordinates are already about the footprint origin.
    footprint_origin: tuple[float, float] = (0.0, 0.0)


def classic_pin_records(shapes: list[Any]) -> list[tuple[str, str, float, float]]:
    """Return ``(number, label, x, y)`` per classic ``P~`` pin shape, unreadable ones skipped.

    Sections are separated by ``^^``.  Section 0 is
    ``P~show~locked~spice_number~x~y~rotation~gId~id``; section 3 is
    ``show~x~y~rotation~pin_name~alignment~~~color`` and section 4 is the displayed
    pin number in the same layout.  The displayed number is what the library draws
    next to the pin and what matches the footprint's pad numbers (the SPICE field
    disagrees on some symbols), so it is preferred when present.  A pin whose
    position is unreadable keeps the number and label with a position of 0, 0.
    """
    pins: list[tuple[str, str, float, float]] = []
    for shape in shapes:
        if not isinstance(shape, str) or not shape.startswith("P~"):
            continue
        sections = shape.split("^^")
        header = sections[0].split("~")
        if len(header) < 4:
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
        x = y = 0.0
        if len(header) >= 6:
            try:
                x, y = float(header[4]), float(header[5])
            except ValueError:
                x = y = 0.0
        pins.append((_normal_number(number), label, x, y))
    return pins


def parse_symbol_pins(shapes: list[Any]) -> tuple[list[SymbolPin], int]:
    """Return the symbol's pins from its ``P~`` shape strings, plus the count of unreadable ones."""
    readable = classic_pin_records(shapes)
    total = sum(1 for s in shapes if isinstance(s, str) and s.startswith("P~"))
    return (
        [SymbolPin(number=number, label=label) for number, label, _x, _y in readable],
        total - len(readable),
    )


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
        record.footprint_origin = (origin_x, origin_y)
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


# ---------------------------------------------------------------------------
# EasyEDA Pro footprint text: the per-uuid endpoint and the crawl's stored copies
# ---------------------------------------------------------------------------

# Pro footprint text is in mils with Y up; the classic canvas unit is 10 mil with Y
# down.  Pro pads are converted to classic canvas units when parsed, so every consumer
# sees one raw pad form.  Checked on C2132, C46749, C6186, C19213, C7950 and C88744
# against the classic responses for the same footprints (2026-09-16).
PRO_MILS_PER_CANVAS_UNIT = 10.0


@dataclass
class FootprintRecord:
    """One footprint on its own: what the per-uuid endpoint or a seed row yields.

    ``pads`` use the same raw form as ``ComponentRecord.pads``: classic canvas units,
    Y down, origin subtracted.
    """

    puuid: str
    status: str = "error"  # 'ok' | 'none' | 'error'
    error: str = ""
    package_name: str = ""
    pads: list[dict] = field(default_factory=list)
    footprint_shapes: list[str] = field(default_factory=list)
    footprint_source: str = "puuid-endpoint"
    skipped_shapes: int = 0
    footprint_origin: tuple[float, float] = (
        0.0,
        0.0,
    )  # classic head x/y; (0, 0) for Pro


def pro_shape_lines(text: str) -> list[str]:
    """Split Pro footprint text into its non-empty record lines."""
    return [line.strip() for line in text.split("\n") if line.strip()]


def _poly_extent(points: Any) -> tuple[float, float]:
    """Return the width and height of a Pro ``POLY`` outline's bounding box in canvas units.

    The outline lists coordinates in mils with letter tokens (``L``, ``A``) between
    segments; the numbers are read in x/y pairs and the letters skipped.
    """
    numbers = [float(value) for value in points if not isinstance(value, str)]
    if len(numbers) < 4:
        raise ValueError("polygon pad without an outline")
    xs = numbers[0::2]
    ys = numbers[1::2]
    return (
        (max(xs) - min(xs)) / PRO_MILS_PER_CANVAS_UNIT,
        (max(ys) - min(ys)) / PRO_MILS_PER_CANVAS_UNIT,
    )


def parse_pro_pads(lines: list[str]) -> tuple[list[dict], int]:
    """Return raw pads from Pro ``["PAD", ...]`` records, plus the count of unreadable ones.

    A record is ``["PAD", id, net, "", layer, number, x, y, rotation, hole, [shape,
    width, height, ...], ...]`` in mils with Y up.  A ``POLY`` shape carries its
    outline as absolute points instead of a size; its bounding box stands in for
    the size (a merged connector pad, for instance) with no rotation of its own.
    The result is in classic canvas units with Y down, the form
    :func:`jlcfootprint.geometry.easyeda_pads_to_mm` converts; the hole is kept as
    a radius like the classic ``PAD~`` field, and the pad number has its leading
    zeros stripped like a classic one's.
    """
    pads: list[dict] = []
    skipped = 0
    for line in lines:
        if not isinstance(line, str) or not line.startswith('["PAD"'):
            continue
        try:
            parts = json.loads(line)
            number = _normal_number(str(parts[5]).strip())
            x = float(parts[6]) / PRO_MILS_PER_CANVAS_UNIT
            y = -float(parts[7]) / PRO_MILS_PER_CANVAS_UNIT
            rotation = float(parts[8] or 0.0)
            layer = str(parts[4])
            hole = parts[9]
            shape = parts[10]
            shape_name = str(shape[0])
            if shape_name.upper() == "POLY":
                width, height = _poly_extent(shape[1])
                rotation = 0.0
            else:
                width = float(shape[1]) / PRO_MILS_PER_CANVAS_UNIT
                height = float(shape[2]) / PRO_MILS_PER_CANVAS_UNIT
        except (ValueError, TypeError, IndexError, KeyError):
            skipped += 1
            continue
        hole_radius = 0.0
        if isinstance(hole, list) and len(hole) > 1:
            try:
                hole_radius = float(hole[1]) / PRO_MILS_PER_CANVAS_UNIT / 2.0
            except (ValueError, TypeError):
                hole_radius = 0.0
        pads.append(
            {
                "number": number,
                "x": x,
                "y": y,
                "w": width,
                "h": height,
                "rotation": rotation,
                "shape": shape_name,
                "layer": layer,
                "hole": hole_radius,
            }
        )
    return pads, skipped


def parse_puuid_response(body: Any, puuid: str) -> FootprintRecord:
    """Classify and parse one per-uuid response (``pro.easyeda.com/api/components/{puuid}``).

    The endpoint answers with ``result.dataStr`` as Pro text (newline-delimited JSON
    arrays) or, for older footprints, as the classic dict with ``head`` and ``shape``.
    Status rules follow :func:`parse_component_response`; malformed input never raises.
    """
    record = FootprintRecord(puuid=puuid)
    if not isinstance(body, dict):
        record.error = "response is not a JSON object"
        return record
    if body.get("success") is False:
        code = body.get("code")
        record.error = f"code {code}: {body.get('message', '')}".strip(": ")
        record.status = "none" if code in _NOT_FOUND_CODES else "error"
        return record
    result = body.get("result")
    if result in (None, {}, [], ""):
        record.status = "none"
        return record
    if not isinstance(result, dict):
        record.error = "result is not an object"
        return record
    data_str = result.get("dataStr")
    record.package_name = str(result.get("display_title") or result.get("title") or "")
    if isinstance(data_str, str) and data_str.lstrip().startswith("["):
        record.footprint_shapes = pro_shape_lines(data_str)
        record.pads, record.skipped_shapes = parse_pro_pads(record.footprint_shapes)
    else:
        package_data = _decode_data_str(data_str)
        head = _dict(package_data.get("head"))
        record.package_name = record.package_name or str(
            _dict(head.get("c_para")).get("package") or ""
        )
        record.footprint_shapes = _shape_list(package_data)
        try:
            origin_x = float(head.get("x", 0.0) or 0.0)
            origin_y = float(head.get("y", 0.0) or 0.0)
        except (TypeError, ValueError):
            origin_x = origin_y = 0.0
            record.error = (
                "footprint origin unreadable; pads left in canvas coordinates"
            )
        record.pads, record.skipped_shapes = parse_footprint_pads(
            record.footprint_shapes, origin_x, origin_y
        )
        record.footprint_origin = (origin_x, origin_y)
    if not record.pads:
        record.error = record.error or "no readable pads in the footprint"
        return record
    record.status = "ok"
    return record


# ----------------------------------------------------------------------
# EasyEDA Pro: the batch device lookup and the per-uuid symbol document
# ----------------------------------------------------------------------

DOCTYPE_SYMBOL = "SYMBOL"


@dataclass
class DeviceHit:
    """One part the batch lookup knows: its symbol uuid, footprint uuid and package name."""

    lcsc: str
    symbol_uuid: str
    puuid: str
    package_name: str


@dataclass
class DevicesResult:
    """What one ``searchByCodes`` answer says about the codes it was asked for.

    ``error`` is non-empty when the answer could not be read; nothing is a hit or a
    miss then and the caller retries the chunk.
    """

    hits: dict[str, DeviceHit] = field(default_factory=dict)
    missing: list[str] = field(default_factory=list)
    error: str = ""


def parse_devices_response(body: Any, codes: list[str]) -> DevicesResult:
    """Read one batch answer; codes absent from a well-formed answer are misses.

    ``result`` is a list of device records, or a dict whose ``lists`` holds them.  A
    record names its part in ``product_code``, its footprint in ``footprint.uuid``
    (``attributes.Footprint`` says the same) and its symbol in ``attributes.Symbol``;
    ``footprint.display_title`` is the suffix-bearing package name.  A record with no
    footprint uuid is a miss: there is nothing to align.  Never raises.
    """
    result = DevicesResult()
    if not isinstance(body, dict):
        result.error = "response is not a JSON object"
        return result
    if body.get("success") is False:
        code = body.get("code")
        result.error = f"code {code}: {body.get('message', '')}".strip(": ")
        return result
    raw = body.get("result")
    if raw is None:
        raw = []
    items = raw.get("lists", []) if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        result.error = "result is not a list of devices"
        return result
    for item in items:
        if not isinstance(item, dict):
            continue
        lcsc = str(item.get("product_code") or "").strip()
        footprint = _dict(item.get("footprint"))
        attributes = _dict(item.get("attributes"))
        puuid = str(footprint.get("uuid") or attributes.get("Footprint") or "").strip()
        if not lcsc or not puuid:
            continue
        result.hits[lcsc] = DeviceHit(
            lcsc=lcsc,
            symbol_uuid=str(attributes.get("Symbol") or "").strip(),
            puuid=puuid,
            package_name=str(
                footprint.get("display_title") or footprint.get("title") or ""
            ).strip(),
        )
    result.missing = [code for code in codes if code not in result.hits]
    return result


@dataclass
class SymbolRecord:
    """One schematic symbol on its own: what the per-uuid endpoint yields for a symbol uuid."""

    uuid: str
    status: str = "error"  # 'ok' | 'none' | 'error'
    error: str = ""
    title: str = ""
    pins: list[SymbolPin] = field(default_factory=list)
    shapes: list[str] = field(default_factory=list)  # the Pro record lines
    skipped_pins: int = 0  # PIN records without a displayed number


def pro_doctype(lines: list[str]) -> str:
    """Return the document kind named by the first ``["DOCTYPE", kind, ...]`` record, or ''."""
    for line in lines:
        if not isinstance(line, str) or not line.startswith('["DOCTYPE"'):
            continue
        try:
            parts = json.loads(line)
        except ValueError:
            return ""
        return str(parts[1]).upper() if len(parts) > 1 else ""
    return ""


def pro_pin_records(lines: list[Any]) -> list[tuple[str, str, float, float]]:
    """Return ``(number, label, x, y)`` per Pro ``PIN`` record, in record order.

    A pin is a ``["PIN", id, ?, ?, x, y, ...]`` record.  Its displayed name and
    number are the attribute records ``["ATTR", attrId, pinId, "NAME", text, ...]``
    and ``["ATTR", attrId, pinId, "NUMBER", text, ...]``; the number is what matches
    the footprint's pad numbers, leading zeros stripped by :func:`_normal_number` so a
    Pro ``01`` and a classic ``1`` are one pad.  A pin without a number is returned
    with ``""``; :func:`parse_pro_pins` and :func:`jlcfootprint.drawing.symbol_pins`
    drop those, since a pin with no number names no pad.
    """
    order: list[str] = []
    positions: dict[str, tuple[float, float]] = {}
    names: dict[str, str] = {}
    numbers: dict[str, str] = {}
    for line in lines:
        if isinstance(line, list):
            parts: Any = line
        elif isinstance(line, str) and line.startswith(('["PIN"', '["ATTR"')):
            try:
                parts = json.loads(line)
            except ValueError:
                continue
        else:
            continue
        if not isinstance(parts, list) or len(parts) < 2:
            continue
        if parts[0] == "PIN":
            pin_id = str(parts[1])
            order.append(pin_id)
            try:
                positions[pin_id] = (float(parts[4]), float(parts[5]))
            except (IndexError, TypeError, ValueError):
                positions[pin_id] = (0.0, 0.0)
        elif parts[0] == "ATTR" and len(parts) >= 5 and parts[3] in ("NAME", "NUMBER"):
            value = "" if parts[4] is None else str(parts[4]).strip()
            if parts[3] == "NUMBER":
                names_or_numbers, value = numbers, _normal_number(value)
            else:
                names_or_numbers = names
            names_or_numbers[str(parts[2])] = value
    return [
        (numbers.get(pin_id, ""), names.get(pin_id, ""), *positions[pin_id])
        for pin_id in order
    ]


def parse_pro_pins(lines: list[str]) -> tuple[list[SymbolPin], int]:
    """Return the pins of Pro symbol text, plus the count of pins without a number.

    Pins come out in record order; see :func:`pro_pin_records` for the records.  A
    pin the symbol leaves unnumbered is skipped on purpose and counted instead: it
    cannot be matched to a pad, and the numbered pins of the symbol are unaffected.
    """
    pins: list[SymbolPin] = []
    skipped = 0
    for number, label, _x, _y in pro_pin_records(lines):
        if not number:
            skipped += 1
            continue
        pins.append(SymbolPin(number=number, label=label))
    return pins, skipped


def parse_symbol_response(body: Any, uuid: str) -> SymbolRecord:
    """Classify and parse one per-uuid answer for a symbol uuid; never raises.

    Status rules follow :func:`parse_puuid_response`.  A body whose document is a
    footprint, is not in the Pro text form or has no readable pin reads as ``error``:
    the uuid names no usable symbol.
    """
    record = SymbolRecord(uuid=uuid)
    if not isinstance(body, dict):
        record.error = "response is not a JSON object"
        return record
    if body.get("success") is False:
        code = body.get("code")
        record.error = f"code {code}: {body.get('message', '')}".strip(": ")
        record.status = "none" if code in _NOT_FOUND_CODES else "error"
        return record
    result = body.get("result")
    if result in (None, {}, [], ""):
        record.status = "none"
        return record
    if not isinstance(result, dict):
        record.error = "result is not an object"
        return record
    record.title = str(result.get("display_title") or result.get("title") or "")
    data_str = result.get("dataStr")
    if not isinstance(data_str, str) or not data_str.lstrip().startswith("["):
        record.error = "symbol is not in the Pro text form"
        return record
    record.shapes = pro_shape_lines(data_str)
    doctype = pro_doctype(record.shapes)
    if doctype and doctype != DOCTYPE_SYMBOL:
        record.error = f"uuid names a {doctype.lower()}, not a symbol"
        return record
    record.pins, record.skipped_pins = parse_pro_pins(record.shapes)
    if not record.pins:
        record.error = "no readable pins in the symbol"
        return record
    record.status = "ok"
    return record
