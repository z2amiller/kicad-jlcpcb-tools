"""Shared helpers for the jlcfootprint tests: KiCad library footprints and recorded responses."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from jlcfootprint.cache import Cache
from jlcfootprint.controller import FootprintCheck
from jlcfootprint.easyeda_client import Document, Lookup
from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    DeviceHit,
    DevicesResult,
    FootprintRecord,
    SymbolRecord,
    assemble_record,
    parse_component_response,
    parse_devices_response,
    parse_puuid_response,
    parse_symbol_response,
)
from jlcfootprint.geometry import pad_hash
from jlcfootprint.kicad_adapter import BoardPart, verdict_key
from jlcfootprint.verdicts import VerdictStore
from jlcfootprint.worker import FOOTPRINT, SYMBOL, Buckets
from scripts.kicad_library import library_pads, with_functions

from .test_jlcfootprint_worker import Clock

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
    """Assemble one part from the Pro recordings the way the plugin's cache holds it.

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
