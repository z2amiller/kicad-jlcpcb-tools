"""Tests for the global EasyEDA cache: round trips, retry rules, stored pad formats, seeding."""

from contextlib import closing
import json
import sqlite3

import pytest

from jlcfootprint.cache import (
    NONE_RETRY_S,
    PADS_CLASSIC_SHAPES,
    PADS_PRO,
    SCHEMA_VERSION,
    Cache,
    pads_from_stored,
)
from jlcfootprint.easyeda_parse import (
    ComponentRecord,
    FootprintRecord,
    SymbolPin,
    SymbolRecord,
)

from .jlcfootprint_support import FIXTURES, recorded
from .test_jlcfootprint_pro_format import PRO_SOT23


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
    assert part.source == "live"
    assert part.fetched_at == 1000
    assert part.polarity_source == "symbol"
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
    assert cache.has_package(record.puuid)
    assert cache.counts() == {"parts": 1, "packages": 1}


def test_needs_fetch_rules(cache):
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
    assert part.polarity_source == "none"


def test_seeded_pin1_polarity_becomes_a_one_pin_symbol(cache):
    """A seed row with only the crawl's pin-1 polarity resolves at seed strength."""
    with closing(cache.connect()) as con, con:
        con.execute(
            "INSERT INTO lcsc_map (lcsc, puuid, status, pin1_polarity, polarity_source, source, fetched_at)"
            " VALUES ('C9', 'p9', 'ok', 'A', 'seed-puuid', 'seed', 1)"
        )
        con.execute(
            "INSERT INTO package (puuid, package_name, pads_json, pads_format, source, fetched_at)"
            " VALUES ('p9', 'LED0603-RD', ?, ?, 'seed', 1)",
            (json.dumps(PRO_SOT23), PADS_PRO),
        )
    part = cache.part("C9")
    assert part.source == "seed"
    assert part.polarity_source == "seed-puuid"
    assert part.record.symbol_pins == [SymbolPin(number="1", label="A")]
    assert part.record.package_name == "LED0603-RD"
    assert _rounded(part.record.pads) == _rounded(recorded("C2132").pads)
    assert not cache.needs("C9", now=10**9)


def test_stored_pad_formats_read_back_as_raw_pads():
    """Pro record lines and classic shape strings both yield the classic raw pad form."""
    record = recorded("C2132")
    body = json.loads((FIXTURES / "C2132.json").read_text())
    package = body["result"]["packageDetail"]["dataStr"]
    stored = {
        "x": package["head"]["x"],
        "y": package["head"]["y"],
        "shapes": package["shape"],
    }
    assert _rounded(
        pads_from_stored(json.dumps(stored), PADS_CLASSIC_SHAPES)
    ) == _rounded(record.pads)
    assert _rounded(pads_from_stored(json.dumps(PRO_SOT23), PADS_PRO)) == _rounded(
        record.pads
    )
    assert pads_from_stored(json.dumps(record.pads), "classic") == record.pads


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


def test_forget_and_clear(cache):
    """Forgetting a part keeps its footprint; clearing drops everything."""
    cache.store(recorded("C2132"), now=1)
    cache.forget("C2132")
    assert cache.part("C2132") is None
    assert cache.counts() == {"parts": 0, "packages": 1}
    cache.store(recorded("C2132"), now=1)
    cache.clear()
    assert cache.counts() == {"parts": 0, "packages": 0}


def _seed(path, parts, packages):
    """Write a seed file in the cache's own schema."""
    seed = Cache(str(path))
    with closing(seed.connect()) as con, con:
        con.executemany(
            "INSERT INTO lcsc_map (lcsc, puuid, status, pin1_polarity, polarity_source, source, fetched_at)"
            " VALUES (?, ?, 'ok', ?, 'seed-puuid', 'seed', 7)",
            parts,
        )
        con.executemany(
            "INSERT INTO package (puuid, package_name, pads_json, pads_format, source, fetched_at)"
            " VALUES (?, ?, ?, 'pro', 'seed', 7)",
            packages,
        )
    return seed


def test_import_seed_fills_gaps_and_never_touches_live_rows(cache, tmp_path):
    """Seed rows are added or refreshed; rows the plugin fetched itself stay as they are."""
    live = recorded("C2132")
    cache.store(live, now=1000)
    seed = _seed(
        tmp_path / "seed.db",
        [("C2132", "pro-uuid", "K"), ("C9", "p9", "A")],
        [
            ("pro-uuid", "SOT-23-3_L2.9-W1.6-P1.90-LS2.8-BR", json.dumps(PRO_SOT23)),
            (live.puuid, "IGNORED", "[]"),
            ("p9", "LED0603-RD", json.dumps(PRO_SOT23)),
        ],
    )
    result = cache.import_seed(seed.path)
    assert (result.parts, result.packages) == (1, 2)
    assert (result.kept_live_parts, result.kept_live_packages) == (1, 1)
    kept = cache.part("C2132")
    assert kept.source == "live"
    assert kept.record.puuid == live.puuid
    assert kept.record.package_name == live.package_name
    added = cache.part("C9")
    assert added.source == "seed"
    assert added.record.symbol_pins == [SymbolPin(number="1", label="A")]
    assert cache.counts() == {"parts": 2, "packages": 3}

    newer = _seed(
        tmp_path / "seed2.db",
        [("C9", "p9", "K")],
        [("p9", "LED0603-FD", json.dumps(PRO_SOT23))],
    )
    cache.import_seed(newer.path)
    refreshed = cache.part("C9")
    assert refreshed.record.symbol_pins == [SymbolPin(number="1", label="K")]
    assert refreshed.record.package_name == "LED0603-FD"


def test_import_seed_rejects_another_schema_version(cache, tmp_path):
    """A seed written for a different cache layout is refused before any row moves."""
    seed = _seed(tmp_path / "seed.db", [("C9", "p9", "A")], [("p9", "X", "[]")])
    with closing(seed.connect()) as con, con:
        con.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    with pytest.raises(ValueError, match="schema version"):
        cache.import_seed(seed.path)
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
    assert part.record.symbol_pins == [] and part.polarity_source == "none"
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
    assert part.polarity_source == "symbol"
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
    assert part.record.symbol_pins == [] and part.polarity_source == "none"
    assert part.record.symbol_uuid == "sym-6"


def test_seed_rows_never_need_a_symbol(cache):
    """A seed row with a symbol uuid but no pins is complete: its meaning came with the seed or not at all."""
    with closing(cache.connect()) as con, con:
        con.execute(
            "INSERT INTO lcsc_map (lcsc, puuid, status, symbol_uuid, source, fetched_at)"
            " VALUES ('C9', 'p9', 'ok', 'sym-9', 'seed', 1)"
        )
        con.execute(
            "INSERT INTO package (puuid, package_name, pads_json, pads_format, source, fetched_at)"
            " VALUES ('p9', 'LED0603-RD', ?, ?, 'seed', 1)",
            (json.dumps(PRO_SOT23), PADS_PRO),
        )
    assert cache.needs("C9") == set()
    assert not cache.needs("C9")


def test_stored_origin_reads_the_classic_head_and_is_zero_otherwise():
    """A classic_shapes row keeps its drawing's head x/y; Pro and classic pad rows have none."""
    from jlcfootprint.cache import stored_origin

    assert stored_origin('{"x": 4000, "y": 3000, "shapes": []}', "classic_shapes") == (
        4000.0,
        3000.0,
    )
    assert stored_origin("[]", "pro") == (0.0, 0.0)
    assert stored_origin("[]", "classic") == (0.0, 0.0)
    assert stored_origin('{"x": "bad"}', "classic_shapes") == (0.0, 0.0)
