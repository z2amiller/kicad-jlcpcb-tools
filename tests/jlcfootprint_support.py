"""Shared helpers for the jlcfootprint tests: KiCad library footprints and recorded responses."""

from __future__ import annotations

import json
import os
from pathlib import Path

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb_text
from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    DeviceHit,
    parse_component_response,
    parse_devices_response,
    parse_puuid_response,
    parse_symbol_response,
)
from jlcfootprint.geometry import Pad

Box = tuple[float, float, float, float]

FIXTURES = Path(__file__).parent / "fixtures" / "jlcfootprint" / "easyeda"
KICAD_SNAPSHOTS = Path(__file__).parent / "fixtures" / "jlcfootprint" / "kicad"
# Set JLCFOOTPRINT_NO_KICAD=1 to hide the installed libraries and prove the snapshots
# alone carry the library-backed tests, as they must in CI.
KICAD_FOOTPRINTS = (
    Path("/nonexistent/kicad/footprints")
    if os.environ.get("JLCFOOTPRINT_NO_KICAD")
    else Path("/Applications/KiCad/KiCad.app/Contents/SharedSupport/footprints")
)


def snapshot_path(library: str, name: str) -> Path:
    """Return where the recorded copy of one library footprint's pads lives."""
    return KICAD_SNAPSHOTS / f"{library}__{name}.json"


def footprints_available() -> bool:
    """Return True when library footprints can be read: recorded snapshots or an installed KiCad."""
    return KICAD_SNAPSHOTS.is_dir() or KICAD_FOOTPRINTS.is_dir()


def installed_library_footprint(
    library: str, name: str
) -> tuple[list[Pad], Box | None]:
    """Read one footprint's pads and courtyard box from the installed KiCad libraries."""
    text = (KICAD_FOOTPRINTS / f"{library}.pretty" / f"{name}.kicad_mod").read_text(
        encoding="utf-8"
    )
    (footprint,) = parse_kicad_pcb_text(f"(kicad_pcb {text})")
    return footprint_pads(footprint), footprint.courtyard


def installed_library_pads(library: str, name: str) -> list[Pad]:
    """Read one footprint's pads from the KiCad libraries installed on this machine."""
    return installed_library_footprint(library, name)[0]


def library_footprint(library: str, name: str) -> tuple[list[Pad], Box | None]:
    """Return a KiCad library footprint's pads and courtyard box, footprint frame, unplaced.

    The recorded snapshot under ``tests/fixtures/jlcfootprint/kicad`` is used when it
    exists, so the tests run without KiCad; otherwise the installed library is read.
    Record a snapshot with ``python3 scripts/snapshot_kicad_footprints.py Library:Name``.
    A snapshot recorded before courtyards were kept has no box (None).
    """
    snapshot = snapshot_path(library, name)
    if snapshot.exists():
        data = json.loads(snapshot.read_text(encoding="utf-8"))
        courtyard = data.get("courtyard")
        return (
            [Pad(**pad) for pad in data["pads"]],
            None if courtyard is None else tuple(courtyard),
        )
    if KICAD_FOOTPRINTS.is_dir():
        return installed_library_footprint(library, name)
    raise FileNotFoundError(
        f"no snapshot {snapshot.name} and no KiCad footprint libraries at {KICAD_FOOTPRINTS}; "
        f"run scripts/snapshot_kicad_footprints.py {library}:{name} on a machine with KiCad"
    )


def library_pads(library: str, name: str) -> list[Pad]:
    """Return a KiCad library footprint's pads in the footprint frame (see ``library_footprint``)."""
    return library_footprint(library, name)[0]


def with_functions(pads: list[Pad], functions: dict[str, str]) -> list[Pad]:
    """Return the pads with pin functions assigned by pad number, as a schematic would."""
    return [pad._replace(pin_function=functions.get(pad.number, "")) for pad in pads]


def recorded(lcsc: str) -> ComponentRecord:
    """Parse the recorded EasyEDA response for one part."""
    return parse_component_response(
        json.loads((FIXTURES / f"{lcsc}.json").read_text()), lcsc
    )


PRO_FIXTURES = Path(__file__).parent / "fixtures" / "jlcfootprint" / "easyeda_pro"


def recorded_devices(name: str) -> tuple[list[str], dict]:
    """Return the codes asked and the body answered by one recorded batch lookup."""
    data = json.loads(
        (PRO_FIXTURES / f"devices_{name}.json").read_text(encoding="utf-8")
    )
    return list(data["codes"]), data["body"]


def recorded_document(kind: str, uuid: str) -> dict:
    """Return one recorded Pro-host document body (``footprint`` or ``symbol``)."""
    return json.loads(
        (PRO_FIXTURES / f"{kind}_{uuid}.json").read_text(encoding="utf-8")
    )


def pro_hit(lcsc: str) -> DeviceHit:
    """Return the recorded batch answer's hit for one part (any ``devices_*.json``)."""
    for path in sorted(PRO_FIXTURES.glob("devices_*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        result = parse_devices_response(data["body"], list(data["codes"]))
        if lcsc in result.hits:
            return result.hits[lcsc]
    raise FileNotFoundError(f"{lcsc} is in no recorded batch answer")


def pro_record(lcsc: str) -> ComponentRecord:
    """Assemble one part from the Pro recordings the way the plugin's cache holds it.

    The batch hit names the uuids, ``footprint_<uuid>.json`` gives the pads and the
    drawing, ``symbol_<uuid>.json`` the pins and the symbol drawing.
    """
    hit = pro_hit(lcsc)
    footprint = parse_puuid_response(
        recorded_document("footprint", hit.puuid), hit.puuid
    )
    record = ComponentRecord(
        lcsc=lcsc,
        status=footprint.status,
        symbol_uuid=hit.symbol_uuid,
        puuid=hit.puuid,
        package_name=footprint.package_name or hit.package_name,
        pads=footprint.pads,
        footprint_shapes=footprint.footprint_shapes,
        footprint_source="puuid-endpoint",
        footprint_origin=footprint.footprint_origin,
    )
    if hit.symbol_uuid:
        symbol = parse_symbol_response(
            recorded_document("symbol", hit.symbol_uuid), hit.symbol_uuid
        )
        if symbol.status == "ok":
            record.symbol_pins = symbol.pins
            record.symbol_shapes = symbol.shapes
    return record
