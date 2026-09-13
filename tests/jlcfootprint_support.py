"""Shared helpers for the jlcfootprint tests: KiCad library footprints and recorded responses."""

from __future__ import annotations

import json
from pathlib import Path

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb_text
from jlcfootprint.easyeda_parse import ComponentRecord, parse_component_response
from jlcfootprint.geometry import Pad

FIXTURES = Path(__file__).parent / "fixtures" / "jlcfootprint" / "easyeda"
KICAD_FOOTPRINTS = Path(
    "/Applications/KiCad/KiCad.app/Contents/SharedSupport/footprints"
)


def kicad_available() -> bool:
    """Return True when KiCad's footprint libraries are installed on this machine."""
    return KICAD_FOOTPRINTS.is_dir()


def library_pads(library: str, name: str) -> list[Pad]:
    """Return a KiCad library footprint's pads in the footprint frame (unplaced)."""
    text = (KICAD_FOOTPRINTS / f"{library}.pretty" / f"{name}.kicad_mod").read_text(
        encoding="utf-8"
    )
    (footprint,) = parse_kicad_pcb_text(f"(kicad_pcb {text})")
    return footprint_pads(footprint)


def with_functions(pads: list[Pad], functions: dict[str, str]) -> list[Pad]:
    """Return the pads with pin functions assigned by pad number, as a schematic would."""
    return [pad._replace(pin_function=functions.get(pad.number, "")) for pad in pads]


def recorded(lcsc: str) -> ComponentRecord:
    """Parse the recorded EasyEDA response for one part."""
    return parse_component_response(
        json.loads((FIXTURES / f"{lcsc}.json").read_text()), lcsc
    )
