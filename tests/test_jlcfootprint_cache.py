"""Tests for the global EasyEDA cache: round trips, retry rules, the layout version."""

from contextlib import closing
from pathlib import Path
import sqlite3

import pytest

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb
from jlcfootprint.cache import NONE_RETRY_S, SCHEMA_VERSION, Cache
from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    FootprintRecord,
    SymbolPin,
    SymbolRecord,
)
from jlcfootprint.resolving import resolve_record

from .jlcfootprint_support import recorded

CORNER_CASE = (
    Path(__file__).parent
    / "fixtures"
    / "jlcfootprint"
    / "boards"
    / "corner_case"
    / "corner_case.kicad_pcb"
)


def _rounded(pads):
    return [
        (
            p["number"],
            round(p["x"], 4),
            round(p["y"], 4),
            round(p["w"], 4),
            round(p["h"], 4),
        )
        for p in pads
    ]


@pytest.fixture
def cache(tmp_path):
    """Return a fresh cache file."""
    return Cache(str(tmp_path / "jlcfootprint-cache.db"))


def test_schema_and_version(cache):
    """Both tables exist and the version is recorded once."""
    with closing(sqlite3.connect(cache.path)) as con:
        tables = {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        version = con.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()[0]
    assert {"meta", "lcsc_map", "package"} <= tables
    assert version == SCHEMA_VERSION
    Cache(cache.path)  # reopening keeps the version and never fails
    assert cache.counts() == {"parts": 0, "packages": 0}


def test_live_record_round_trip(cache):
    """A fetched part comes back with its symbol, its footprint and both drawings."""
    record = recorded("C2132")
    cache.store(record, now=1000)
    part = cache.part("C2132")
    assert part is not None
    assert part.fetched_at == 1000
    got = part.record
    assert got.status == "ok"
    assert got.puuid == record.puuid
    assert got.package_name == record.package_name
    assert got.symbol_uuid == record.symbol_uuid
    assert got.symbol_pins == record.symbol_pins
    assert _rounded(got.pads) == _rounded(record.pads)
    assert got.symbol_shapes == record.symbol_shapes
    assert got.footprint_shapes == record.footprint_shapes
    assert got.footprint_source == "component"
    assert got.footprint_origin == record.footprint_origin != (0.0, 0.0)
    assert cache.has_package(record.puuid)
    assert cache.counts() == {"parts": 1, "packages": 1}


def test_a_cached_classic_part_resolves_as_its_record_does(cache):
    """The drawing's origin comes back with it, so the body outline is read where it is drawn.

    Q2 on the corner-case board is C2132, a SOT-23 whose classic drawing has its
    origin at (4000.0005, 3000) canvas units: read against (0, 0) instead, its body
    would sit a metre away and overhang the courtyard by that much.
    """
    record = recorded("C2132")
    q2 = next(fp for fp in parse_kicad_pcb(str(CORNER_CASE)) if fp.reference == "Q2")
    pads = footprint_pads(q2)
    direct = resolve_record(pads, q2.footprint_name, record, q2.courtyard)
    cache.store(record, now=1)
    cached = resolve_record(
        pads, q2.footprint_name, cache.part("C2132").record, q2.courtyard
    )
    assert direct.body_excess_mm is None and cached.body_excess_mm is None
    assert (cached.status, cached.rotation, cached.fit, cached.origin) == (
        direct.status,
        direct.rotation,
        direct.fit,
        direct.origin,
    )
    assert cached.note_text == direct.note_text


def test_needs_rules(cache):
    """Absent and error rows fetch; ok rows never; none rows after thirty days."""
    assert cache.needs("C1")
    cache.store(recorded("C2132"), now=1000)
    assert not cache.needs("C2132", now=10**9)
    cache.store(ComponentRecord(lcsc="C2", status="error", error="HTTP 403"), now=1000)
    assert cache.needs("C2", now=1001)
    cache.store(ComponentRecord(lcsc="C3", status="none"), now=1000)
    assert not cache.needs("C3", now=1000 + NONE_RETRY_S - 1)
    assert cache.needs("C3", now=1000 + NONE_RETRY_S)
    assert cache.status("C3") == "none"
    assert cache.status("C4") is None


def test_error_and_none_rows_carry_no_footprint(cache):
    """A part EasyEDA has nothing for stores its status and nothing else."""
    cache.store(ComponentRecord(lcsc="C3", status="none"), now=5)
    part = cache.part("C3")
    assert part.record.status == "none"
    assert part.record.pads == []
    assert part.record.symbol_pins == []


def test_store_footprint_from_the_uuid_endpoint(cache):
    """A footprint fetched by uuid gets its own package row; an empty one is ignored."""
    record = recorded("C2132")
    cache.store(ComponentRecord(lcsc="C2132", status="ok", puuid=record.puuid), now=1)
    assert cache.part("C2132").record.pads == []
    cache.store_footprint(FootprintRecord(puuid=record.puuid, status="error"), now=2)
    assert not cache.has_package(record.puuid)
    cache.store_footprint(
        FootprintRecord(
            puuid=record.puuid,
            status="ok",
            package_name=record.package_name,
            pads=record.pads,
            footprint_shapes=record.footprint_shapes,
        ),
        now=2,
    )
    part = cache.part("C2132")
    assert _rounded(part.record.pads) == _rounded(record.pads)
    assert part.record.footprint_source == "puuid-endpoint"
    assert part.record.footprint_origin == (0.0, 0.0)
    cache.store_footprint(
        FootprintRecord(
            puuid=record.puuid,
            status="ok",
            package_name=record.package_name,
            pads=record.pads,
            footprint_shapes=record.footprint_shapes,
            footprint_origin=record.footprint_origin,
        ),
        now=3,
    )
    assert cache.part("C2132").record.footprint_origin == record.footprint_origin


def test_forget_and_clear(cache):
    """Forgetting a part keeps its footprint; clearing drops everything."""
    cache.store(recorded("C2132"), now=1)
    cache.forget("C2132")
    assert cache.part("C2132") is None
    assert cache.counts() == {"parts": 0, "packages": 1}
    cache.store(recorded("C2132"), now=1)
    cache.clear()
    assert cache.counts() == {"parts": 0, "packages": 0}


def test_needs_follows_a_live_row_through_its_pieces(cache):
    """No row asks for a lookup; the uuids ask for their documents; a complete row asks for nothing."""
    assert cache.needs("C1") == {"lookup"}
    cache.store_lookup("C1", "sym-1", "fp-1", now=10)
    assert cache.needs("C1") == {"footprint", "symbol"}
    assert cache.needs("C1")
    part = cache.part("C1")
    assert (part.record.status, part.record.puuid, part.record.symbol_uuid) == (
        "ok",
        "fp-1",
        "sym-1",
    )
    assert part.record.symbol_pins == []
    record = recorded("C2132")
    cache.store_footprint(
        FootprintRecord(
            puuid="fp-1",
            status="ok",
            package_name=record.package_name,
            pads=record.pads,
        ),
        now=11,
    )
    assert cache.needs("C1") == {"symbol"}
    symbol = SymbolRecord(
        uuid="sym-1",
        status="ok",
        pins=[SymbolPin("1", "K"), SymbolPin("2", "A")],
        shapes=['["DOCTYPE","SYMBOL","1.1"]'],
    )
    assert cache.store_symbol("C1", symbol, now=12)
    assert cache.needs("C1") == set()
    part = cache.part("C1")
    assert part.record.symbol_pins == symbol.pins
    assert part.record.symbol_shapes == symbol.shapes
    assert part.record.package_name == record.package_name
    assert part.fetched_at == 12
    assert not cache.store_symbol("C9", symbol, now=12)
    # A part whose lookup named no symbol is complete once its footprint is in.
    cache.store_lookup("C2", "", "fp-1", now=10)
    assert cache.needs("C2") == set()
    assert cache.part("C2").record.symbol_pins == []
    # A shared footprint already cached is not asked for again.
    cache.store_lookup("C3", "sym-3", "fp-1", now=10)
    assert cache.needs("C3") == {"symbol"}


def test_lookup_misses_and_empty_symbols(cache):
    """A miss is a none row on the 30-day clock; a symbol that answered nothing leaves no pins."""
    cache.store_lookup_miss("C5", now=100)
    assert cache.status("C5") == "none"
    assert cache.needs("C5", now=100) == set()
    assert cache.needs("C5", now=100 + NONE_RETRY_S) == {"lookup"}
    cache.store_lookup("C6", "sym-6", "fp-6", now=1)
    assert cache.store_symbol("C6", SymbolRecord(uuid="sym-6", status="none"), now=2)
    assert cache.needs("C6") == {"footprint"}
    part = cache.part("C6")
    assert part.record.symbol_pins == []
    assert part.record.symbol_uuid == "sym-6"


@pytest.mark.parametrize("version", ["1", "99"], ids=["older", "newer"])
def test_a_file_from_another_layout_is_dropped_and_refilled(tmp_path, version):
    """The cache is only fetched data, so a file of another layout starts over instead of failing on its columns."""
    path = str(tmp_path / "jlcfootprint-cache.db")
    with closing(sqlite3.connect(path)) as con, con:
        con.executescript(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);"
            f"INSERT INTO meta VALUES ('schema_version', '{version}');"
            "CREATE TABLE lcsc_map (lcsc TEXT PRIMARY KEY, status TEXT NOT NULL,"
            " source TEXT NOT NULL, fetched_at INTEGER NOT NULL);"
            "INSERT INTO lcsc_map VALUES ('C9', 'ok', 'live', 1);"
            "CREATE TABLE package (puuid TEXT PRIMARY KEY, package_name TEXT NOT NULL,"
            " pads_json TEXT NOT NULL, source TEXT NOT NULL, fetched_at INTEGER NOT NULL);"
            "INSERT INTO package VALUES ('fp-9', 'R0603', '[]', 'live', 1);"
        )
    cache = Cache(path)
    assert cache.counts() == {"parts": 0, "packages": 0}
    cache.store(recorded("C2132"), now=1)
    assert cache.part("C2132").record.status == "ok"
    with closing(sqlite3.connect(path)) as con:
        assert con.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone() == (SCHEMA_VERSION,)
    # Reopened at its own layout, the file keeps what it holds.
    assert Cache(path).counts()["parts"] == 1
