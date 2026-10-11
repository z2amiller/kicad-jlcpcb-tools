"""Shared helpers for the jlcfootprint tests: recorded EasyEDA responses and the script loader.

Everything here reads files: EasyEDA's recorded answers under
``tests/fixtures/jlcfootprint/`` and the development tools under
``tools/jlcfootprint/``.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    DeviceHit,
    assemble_record,
    parse_component_response,
    parse_devices_response,
    parse_puuid_response,
    parse_symbol_response,
)

ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / "fixtures" / "jlcfootprint" / "easyeda"


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
    """Assemble one part from its recorded Pro answers, as a live fetch assembles it.

    The batch hit names the uuids, ``footprint_<uuid>.json`` gives the pads and the
    drawing, ``symbol_<uuid>.json`` the pins and the symbol drawing.
    """
    hit = pro_hit(lcsc)
    footprint = parse_puuid_response(
        recorded_document("footprint", hit.puuid), hit.puuid
    )
    symbol = None
    if hit.symbol_uuid:
        symbol = parse_symbol_response(
            recorded_document("symbol", hit.symbol_uuid), hit.symbol_uuid
        )
    return assemble_record(lcsc, hit, footprint, symbol)


def load_script(name: str) -> ModuleType:
    """Import one ``tools/jlcfootprint/`` module as a module, without running its CLI."""
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "tools" / "jlcfootprint" / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded
