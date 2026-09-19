"""Shared helpers for the jlcfootprint tests: KiCad library footprints and recorded responses."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb_text
from jlcfootprint.cache import Cache
from jlcfootprint.controller import FootprintCheck
from jlcfootprint.easyeda_client import Document, Lookup
from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    DeviceHit,
    DevicesResult,
    FootprintRecord,
    SymbolRecord,
    parse_component_response,
    parse_devices_response,
    parse_puuid_response,
    parse_symbol_response,
)
from jlcfootprint.geometry import Pad, pad_hash
from jlcfootprint.kicad_adapter import BoardPart, verdict_key
from jlcfootprint.verdicts import VerdictStore
from jlcfootprint.worker import FOOTPRINT, SYMBOL, Buckets

from .test_jlcfootprint_worker import Clock

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


# ---------------------------------------------------------------------------
# The controller tests' board parts, fake client and setup fixture builder
# ---------------------------------------------------------------------------


def sot23(reference: str, lcsc: str = "C2132", is_bottom: bool = False) -> BoardPart:
    """Return a SOT-23 board part from KiCad's library."""
    pads = with_functions(
        library_pads("Package_TO_SOT_SMD", "SOT-23"), {"1": "B", "2": "E", "3": "C"}
    )
    return BoardPart(
        reference,
        lcsc,
        "Package_TO_SOT_SMD:SOT-23",
        is_bottom,
        0.0,
        pads,
        pad_hash(pads),
    )


def led(reference: str, lcsc: str = "C2286") -> BoardPart:
    """Return an 0603 LED board part with K on pad 1."""
    pads = with_functions(
        library_pads("LED_SMD", "LED_0603_1608Metric"), {"1": "K", "2": "A"}
    )
    return BoardPart(
        reference, lcsc, "LED_SMD:LED_0603_1608Metric", False, 0.0, pads, pad_hash(pads)
    )


def sod323(reference: str, functions: dict, lcsc: str = "C7502694") -> BoardPart:
    """Return a SOD-323 diode part with the given pin functions."""
    pads = with_functions(library_pads("Diode_SMD", "D_SOD-323"), functions)
    return BoardPart(
        reference, lcsc, "Diode_SMD:D_SOD-323", False, 0.0, pads, verdict_key(pads)
    )


def tantalum(reference: str, lcsc: str = "C16133") -> BoardPart:
    """Return a case-B tantalum part whose JLC name carries no orientation token."""
    pads = with_functions(
        library_pads("Capacitor_Tantalum_SMD", "CP_EIA-3528-21_Kemet-B"),
        {"1": "+", "2": "-"},
    )
    return BoardPart(
        reference,
        lcsc,
        "Capacitor_Tantalum_SMD:CP_EIA-3528-21_Kemet-B",
        False,
        0.0,
        pads,
        verdict_key(pads),
    )


def alias(lcsc: str, source: str = "C2132") -> ComponentRecord:
    """Return the recorded part under another LCSC (same footprint and symbol)."""
    record = copy.deepcopy(recorded(source))
    record.lcsc = lcsc
    return record


class FakeClient:
    """Answer the three Pro calls from recorded classic responses; count every call.

    A part's symbol uuid is ``sym-<lcsc>``; its footprint uuid is the classic
    record's, so two parts sharing a footprint share one document.
    """

    def __init__(self, records=None):
        self.records = records or {}
        self.lookups = []
        self.documents = []
        self.fail_lookups = False
        self.transient_symbols = set()
        self.silent_symbols = set()
        # Documents that answer a final error (HTTP 401, an unreadable body).
        self.error_footprints = set()
        self.error_symbols = set()

    def search_by_codes(self, codes):
        """Return hits for the recorded parts and misses for the rest."""
        codes = list(codes)
        self.lookups.append(codes)
        if self.fail_lookups:
            return Lookup(
                codes, DevicesResult(error="HTTP 403 after retries"), transient=True
            )
        devices = DevicesResult()
        for code in codes:
            record = self.records.get(code)
            if record is not None:
                devices.hits[code] = DeviceHit(
                    code, f"sym-{code}", record.puuid, record.package_name
                )
        devices.missing = [code for code in codes if code not in devices.hits]
        return Lookup(codes, devices)

    def fetch_footprint(self, puuid):
        """Return the recorded footprint with this uuid, or none."""
        self.documents.append((FOOTPRINT, puuid))
        if puuid in self.error_footprints:
            return Document(
                FOOTPRINT, puuid, FootprintRecord(puuid=puuid, error="HTTP 401")
            )
        for record in self.records.values():
            if record.puuid == puuid:
                return Document(
                    FOOTPRINT,
                    puuid,
                    FootprintRecord(
                        puuid=puuid,
                        status="ok",
                        package_name=record.package_name,
                        pads=record.pads,
                        footprint_shapes=record.footprint_shapes,
                    ),
                )
        return Document(FOOTPRINT, puuid, FootprintRecord(puuid=puuid, status="none"))

    def fetch_symbol(self, uuid):
        """Return the recorded symbol pins for ``sym-<lcsc>``; a failure or none when told to."""
        self.documents.append((SYMBOL, uuid))
        lcsc = uuid[len("sym-") :]
        if uuid in self.transient_symbols:
            return Document(
                SYMBOL,
                uuid,
                SymbolRecord(uuid=uuid, error="HTTP 500 after retries"),
                transient=True,
            )
        if uuid in self.error_symbols:
            return Document(SYMBOL, uuid, SymbolRecord(uuid=uuid, error="HTTP 401"))
        record = self.records.get(lcsc)
        if record is None or uuid in self.silent_symbols:
            return Document(SYMBOL, uuid, SymbolRecord(uuid=uuid, status="none"))
        return Document(
            SYMBOL,
            uuid,
            SymbolRecord(
                uuid=uuid,
                status="ok",
                pins=record.symbol_pins,
                shapes=record.symbol_shapes,
            ),
        )


def controller_setup(tmp_path):
    """Return a controller over a fresh cache and verdict store with a fake client.

    Shared by ``test_jlcfootprint_controller.py`` and
    ``test_jlcfootprint_presentation_glue.py``, each of which wraps this in its own
    ``setup`` pytest fixture.
    """
    board = {
        "parts": [sot23("Q1"), led("D1"), BoardPart("R1", "", "R", False, 0.0, [], "")]
    }
    events = []
    messages = []
    cache = Cache(str(tmp_path / "cache.db"))
    verdicts = VerdictStore(str(tmp_path / "project.db"))
    client = FakeClient(
        {
            "C2132": recorded("C2132"),
            "C2286": recorded("C2286"),
            "C7502694": recorded("C7502694"),
        }
    )
    clock = Clock()
    # Buckets built on the fake clock so try_take/seconds_until_token refill against
    # clock.now instead of the real wall clock; otherwise run_pending's token wait
    # busy-loops through real seconds while only the worker's own clock is fake.
    buckets = Buckets.default(clock=clock, jitter=lambda: 0.0)
    check = FootprintCheck(
        cache,
        verdicts,
        read_board=lambda: list(board["parts"]),
        post=lambda lcsc, generation: events.append((lcsc, generation)),
        client=client,
        worker=None,
        message=messages.append,
        now=clock,
        buckets=buckets,
    )
    check.worker.wait = clock.wait
    check.worker.clock = clock
    return check, board, events, messages, client
