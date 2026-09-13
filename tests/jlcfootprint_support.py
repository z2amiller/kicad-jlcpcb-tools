"""Shared helpers for the jlcfootprint tests: KiCad library footprints and recorded responses."""

from __future__ import annotations

import json
import os
from pathlib import Path

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb_text
from jlcfootprint.easyeda_parse import ComponentRecord, parse_component_response
from jlcfootprint.geometry import Pad

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


def installed_library_pads(library: str, name: str) -> list[Pad]:
    """Read one footprint's pads from the KiCad libraries installed on this machine."""
    text = (KICAD_FOOTPRINTS / f"{library}.pretty" / f"{name}.kicad_mod").read_text(
        encoding="utf-8"
    )
    (footprint,) = parse_kicad_pcb_text(f"(kicad_pcb {text})")
    return footprint_pads(footprint)


def library_pads(library: str, name: str) -> list[Pad]:
    """Return a KiCad library footprint's pads in the footprint frame (unplaced).

    The recorded snapshot under ``tests/fixtures/jlcfootprint/kicad`` is used when it
    exists, so the tests run without KiCad; otherwise the installed library is read.
    Record a snapshot with ``python3 scripts/snapshot_kicad_footprints.py Library:Name``.
    """
    snapshot = snapshot_path(library, name)
    if snapshot.exists():
        data = json.loads(snapshot.read_text(encoding="utf-8"))
        return [Pad(**pad) for pad in data["pads"]]
    if KICAD_FOOTPRINTS.is_dir():
        return installed_library_pads(library, name)
    raise FileNotFoundError(
        f"no snapshot {snapshot.name} and no KiCad footprint libraries at {KICAD_FOOTPRINTS}; "
        f"run scripts/snapshot_kicad_footprints.py {library}:{name} on a machine with KiCad"
    )


def with_functions(pads: list[Pad], functions: dict[str, str]) -> list[Pad]:
    """Return the pads with pin functions assigned by pad number, as a schematic would."""
    return [pad._replace(pin_function=functions.get(pad.number, "")) for pad in pads]


def recorded(lcsc: str) -> ComponentRecord:
    """Parse the recorded EasyEDA response for one part."""
    return parse_component_response(
        json.loads((FIXTURES / f"{lcsc}.json").read_text()), lcsc
    )
