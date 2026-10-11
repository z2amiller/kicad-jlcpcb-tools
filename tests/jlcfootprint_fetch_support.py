"""Test doubles for the fetch path and for the code that runs on it; each says what it stands in for."""

from __future__ import annotations

import copy

from jlcfootprint.easyeda_client import Document, Lookup
from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    DeviceHit,
    DevicesResult,
    FootprintRecord,
    SymbolRecord,
)
from jlcfootprint.geometry import pad_hash
from jlcfootprint.kicad_adapter import BoardPart, verdict_key
from jlcfootprint.worker import FOOTPRINT, SYMBOL
from tools.jlcfootprint.kicad_library import library_pads, with_functions

from .jlcfootprint_support import recorded


class Clock:
    """A clock that only advances when something waits on it."""

    def __init__(self):
        self.now = 1000.0
        self.waits = []

    def __call__(self):
        """Return the current fake time."""
        return self.now

    def wait(self, seconds):
        """Advance by the pause and report that nobody asked to stop."""
        self.waits.append(seconds)
        self.now += seconds
        return False


def sot23(reference: str, lcsc: str = "C2132", is_bottom: bool = False) -> BoardPart:
    """Return a SOT-23 board part from KiCad's library, with the pin functions a schematic gives it."""
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
