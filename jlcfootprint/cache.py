"""Global EasyEDA cache: one sqlite file beside ``corrections.db`` (spec section 5.1).

Symbol data is keyed per LCSC because pin meaning is a per-part fact; footprint
data is keyed per footprint uuid because footprints are shared.  Pads are stored
as EasyEDA sent them and converted when read, so a change to the frame
convention never invalidates the cache.  Rows never expire on their own: ``none``
rows are retried after 30 days, ``error`` rows next session, ``ok`` rows only on
an explicit refresh.  A live row is filled in steps (spec section 15): the batch
lookup writes the uuids, the footprint and the symbol documents follow, and
``needs`` says which piece is still missing.  Stdlib only; safe to use from the
worker thread and the main thread at once (each call opens its own connection).
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
import sqlite3
import time
from typing import Any
import zlib

from .easyeda_parse import (
    ComponentRecord,
    FootprintRecord,
    SymbolPin,
    SymbolRecord,
    parse_footprint_pads,
    parse_pro_pads,
    pin1_polarity,
)
from .naming import parse_package_name

FILENAME = "jlcfootprint-cache.db"
SCHEMA_VERSION = "1"
NONE_RETRY_S = 30 * 24 * 3600
CONNECT_TIMEOUT_S = 5.0

# ``pads_json`` holds the pads as their source produced them; ``pads_format`` says how
# to read them: ``classic`` is the list of raw pad dicts a per-LCSC response gives,
# ``pro`` the ``["PAD", ...]`` record lines of EasyEDA Pro footprint text (the per-uuid
# endpoint and the crawl), ``classic_shapes`` the ``PAD~`` strings of a classic
# footprint drawing with its origin (``{"x": .., "y": .., "shapes": [..]}``).
PADS_CLASSIC = "classic"
PADS_PRO = "pro"
PADS_CLASSIC_SHAPES = "classic_shapes"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS lcsc_map (
  lcsc             TEXT PRIMARY KEY,
  puuid            TEXT,
  status           TEXT NOT NULL,
  error            TEXT,
  symbol_uuid      TEXT,
  symbol_pins_json TEXT,
  pin1_polarity    TEXT,
  polarity_source  TEXT,
  symbol_blob      BLOB,
  source           TEXT NOT NULL,
  fetched_at       INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS package (
  puuid            TEXT PRIMARY KEY,
  package_name     TEXT NOT NULL,
  family           TEXT,
  name_rotation    INTEGER,
  name_source      TEXT,
  name_confidence  TEXT,
  pads_json        TEXT NOT NULL,
  pads_format      TEXT NOT NULL DEFAULT 'classic',
  footprint_blob   BLOB,
  footprint_source TEXT,
  source           TEXT NOT NULL,
  fetched_at       INTEGER NOT NULL
);
"""

_LCSC_COLUMNS = (
    "lcsc",
    "puuid",
    "status",
    "error",
    "symbol_uuid",
    "symbol_pins_json",
    "pin1_polarity",
    "polarity_source",
    "symbol_blob",
    "source",
    "fetched_at",
)
_PACKAGE_COLUMNS = (
    "puuid",
    "package_name",
    "family",
    "name_rotation",
    "name_source",
    "name_confidence",
    "pads_json",
    "pads_format",
    "footprint_blob",
    "footprint_source",
    "source",
    "fetched_at",
)


@dataclass
class CachedPart:
    """One part as the cache knows it, ready for the resolver."""

    record: ComponentRecord
    polarity_source: str  # 'symbol' | 'seed-puuid' | 'none'
    source: str  # 'live' | 'seed'
    fetched_at: int


@dataclass
class SeedImportResult:
    """What a seed import did."""

    parts: int = 0
    packages: int = 0
    kept_live_parts: int = 0
    kept_live_packages: int = 0


def compress(shapes: list[str]) -> bytes | None:
    """Return the drawing's shape strings as a zlib-compressed JSON blob, or None when empty."""
    if not shapes:
        return None
    return zlib.compress(json.dumps(shapes, separators=(",", ":")).encode("utf-8"))


def decompress(blob: bytes | None) -> list[str]:
    """Return the shape strings stored by :func:`compress` (an empty list for None)."""
    if not blob:
        return []
    return json.loads(zlib.decompress(blob).decode("utf-8"))


def pads_from_stored(pads_json: str, pads_format: str) -> list[dict]:
    """Return raw pads (classic canvas units, Y down) from a stored ``pads_json``."""
    stored = json.loads(pads_json)
    if pads_format == PADS_PRO:
        pads, _ = parse_pro_pads(stored)
        return pads
    if pads_format == PADS_CLASSIC_SHAPES:
        pads, _ = parse_footprint_pads(
            stored.get("shapes") or [],
            float(stored.get("x") or 0.0),
            float(stored.get("y") or 0.0),
        )
        return pads
    return list(stored)


def stored_origin(pads_json: str, pads_format: str) -> tuple[float, float]:
    """Return the classic drawing's head x/y from a stored ``pads_json``; (0, 0) otherwise."""
    if pads_format != PADS_CLASSIC_SHAPES:
        return (0.0, 0.0)
    stored = json.loads(pads_json)
    try:
        return (float(stored.get("x") or 0.0), float(stored.get("y") or 0.0))
    except (AttributeError, TypeError, ValueError):
        return (0.0, 0.0)


def _upsert(con: sqlite3.Connection, table: str, columns: tuple, row: dict) -> None:
    """Insert or replace one row by primary key."""
    placeholders = ",".join("?" for _ in columns)
    con.execute(
        f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
        tuple(row.get(column) for column in columns),
    )


class Cache:
    """The global cache file; every method opens and closes its own connection."""

    def __init__(self, path: str) -> None:
        self.path = str(path)
        with closing(self.connect()) as con, con:
            con.executescript(SCHEMA)
            con.execute(
                "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )

    def connect(self) -> sqlite3.Connection:
        """Open a connection with row access by name."""
        con = sqlite3.connect(self.path, timeout=CONNECT_TIMEOUT_S)
        con.row_factory = sqlite3.Row
        return con

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def status(self, lcsc: str) -> str | None:
        """Return the stored status for a part, or None when it has no row."""
        with closing(self.connect()) as con:
            row = con.execute(
                "SELECT status FROM lcsc_map WHERE lcsc = ?", (lcsc,)
            ).fetchone()
        return None if row is None else str(row["status"])

    def needs(self, lcsc: str, now: float | None = None) -> set[str]:
        """Return what the part still needs from the network (spec section 15.4).

        ``{'lookup'}`` when there is no row, an ``error`` row or a ``none`` row older
        than thirty days; otherwise the subset of ``{'footprint', 'symbol'}`` that is
        missing: a ``puuid`` without a package row, or a live row whose symbol uuid
        is known but whose symbol has not been fetched.  Seed rows never ask for a
        symbol: their pin meaning came with the seed or is absent for good.
        """
        with closing(self.connect()) as con:
            row = con.execute(
                "SELECT status, fetched_at, puuid, symbol_uuid, symbol_pins_json,"
                " pin1_polarity, source FROM lcsc_map WHERE lcsc = ?",
                (lcsc,),
            ).fetchone()
            if row is None or row["status"] == "error":
                return {"lookup"}
            if row["status"] == "none":
                current = time.time() if now is None else now
                if current - float(row["fetched_at"]) >= NONE_RETRY_S:
                    return {"lookup"}
                return set()
            needed: set[str] = set()
            if row["puuid"] and not self._has_package(con, str(row["puuid"])):
                needed.add("footprint")
            if (
                row["source"] == "live"
                and row["symbol_uuid"]
                and row["symbol_pins_json"] is None
                and row["pin1_polarity"] is None
            ):
                needed.add("symbol")
        return needed

    def needs_fetch(self, lcsc: str, now: float | None = None) -> bool:
        """Return True when the part must hit the network for anything."""
        return bool(self.needs(lcsc, now))

    @staticmethod
    def _has_package(con: sqlite3.Connection, puuid: str) -> bool:
        return (
            con.execute("SELECT 1 FROM package WHERE puuid = ?", (puuid,)).fetchone()
            is not None
        )

    def part(self, lcsc: str) -> CachedPart | None:
        """Return the part's record with its footprint joined in, or None without a row."""
        with closing(self.connect()) as con:
            row = con.execute(
                "SELECT * FROM lcsc_map WHERE lcsc = ?", (lcsc,)
            ).fetchone()
            if row is None:
                return None
            package = None
            if row["puuid"]:
                package = con.execute(
                    "SELECT * FROM package WHERE puuid = ?", (row["puuid"],)
                ).fetchone()
        record = ComponentRecord(
            lcsc=lcsc,
            status=str(row["status"]),
            error=str(row["error"] or ""),
            symbol_uuid=str(row["symbol_uuid"] or ""),
            symbol_shapes=decompress(row["symbol_blob"]),
            puuid=str(row["puuid"] or ""),
        )
        polarity_source = "none"
        pins = json.loads(row["symbol_pins_json"]) if row["symbol_pins_json"] else []
        if pins:
            record.symbol_pins = [
                SymbolPin(number=str(pin["number"]), label=str(pin.get("label", "")))
                for pin in pins
            ]
            polarity_source = str(row["polarity_source"] or "symbol")
        elif row["pin1_polarity"]:
            # The crawl read pin 1's meaning from one representative part per footprint;
            # a one-pin symbol carries that label to the resolver at seed strength.
            record.symbol_pins = [
                SymbolPin(number="1", label=str(row["pin1_polarity"]))
            ]
            polarity_source = str(row["polarity_source"] or "seed-puuid")
        if package is not None:
            record.package_name = str(package["package_name"])
            record.pads = pads_from_stored(package["pads_json"], package["pads_format"])
            record.footprint_shapes = decompress(package["footprint_blob"])
            record.footprint_source = str(package["footprint_source"] or "")
            record.footprint_origin = stored_origin(
                package["pads_json"], package["pads_format"]
            )
        return CachedPart(
            record=record,
            polarity_source=polarity_source,
            source=str(row["source"]),
            fetched_at=int(row["fetched_at"]),
        )

    def has_package(self, puuid: str) -> bool:
        """Return True when a footprint with this uuid is stored."""
        with closing(self.connect()) as con:
            return (
                con.execute(
                    "SELECT 1 FROM package WHERE puuid = ?", (puuid,)
                ).fetchone()
                is not None
            )

    def counts(self) -> dict[str, int]:
        """Return the row counts of both tables."""
        with closing(self.connect()) as con:
            parts = con.execute("SELECT COUNT(*) FROM lcsc_map").fetchone()[0]
            packages = con.execute("SELECT COUNT(*) FROM package").fetchone()[0]
        return {"parts": int(parts), "packages": int(packages)}

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def store(
        self, record: ComponentRecord, now: float | None = None, source: str = "live"
    ) -> None:
        """Store one fetched part: its symbol row and, when it carries pads, its footprint row."""
        fetched_at = int(time.time() if now is None else now)
        polarity = pin1_polarity(record.symbol_pins) if record.symbol_pins else None
        row = {
            "lcsc": record.lcsc,
            "puuid": record.puuid or None,
            "status": record.status,
            "error": record.error or None,
            "symbol_uuid": record.symbol_uuid or None,
            "symbol_pins_json": (
                json.dumps(
                    [
                        {"number": pin.number, "label": pin.label}
                        for pin in record.symbol_pins
                    ],
                    separators=(",", ":"),
                )
                if record.symbol_pins
                else None
            ),
            "pin1_polarity": polarity,
            "polarity_source": "symbol" if record.symbol_pins else None,
            "symbol_blob": compress(record.symbol_shapes),
            "source": source,
            "fetched_at": fetched_at,
        }
        with closing(self.connect()) as con, con:
            _upsert(con, "lcsc_map", _LCSC_COLUMNS, row)
            if record.puuid and record.pads:
                _upsert(
                    con,
                    "package",
                    _PACKAGE_COLUMNS,
                    self._package_row(
                        record.puuid,
                        record.package_name,
                        record.pads,
                        record.footprint_shapes,
                        record.footprint_source or "component",
                        source,
                        fetched_at,
                    ),
                )

    def store_lookup(
        self,
        lcsc: str,
        symbol_uuid: str,
        puuid: str,
        now: float | None = None,
    ) -> None:
        """Store what the batch lookup knows about a part: its uuids, no drawings yet.

        A part with no symbol uuid gets an empty pin list, so ``needs`` never asks
        for a symbol that does not exist.
        """
        fetched_at = int(time.time() if now is None else now)
        row = {
            "lcsc": lcsc,
            "puuid": puuid or None,
            "status": "ok",
            "error": None,
            "symbol_uuid": symbol_uuid or None,
            "symbol_pins_json": None if symbol_uuid else "[]",
            "pin1_polarity": None,
            "polarity_source": None,
            "symbol_blob": None,
            "source": "live",
            "fetched_at": fetched_at,
        }
        with closing(self.connect()) as con, con:
            _upsert(con, "lcsc_map", _LCSC_COLUMNS, row)

    def store_lookup_miss(self, lcsc: str, now: float | None = None) -> None:
        """Store that EasyEDA has nothing for a part: a ``none`` row retried after 30 days."""
        self.store(ComponentRecord(lcsc=lcsc, status="none"), now)

    def store_symbol(
        self, lcsc: str, symbol: SymbolRecord, now: float | None = None
    ) -> bool:
        """Store a part's fetched symbol on its row; return False when the part has no row.

        A symbol that answered ``none`` or could not be read leaves an empty pin
        list: the part resolves by geometry and a polarized one reads as unknown.
        """
        pins = symbol.pins if symbol.status == "ok" else []
        fetched_at = int(time.time() if now is None else now)
        with closing(self.connect()) as con, con:
            cursor = con.execute(
                "UPDATE lcsc_map SET symbol_uuid = ?, symbol_pins_json = ?,"
                " pin1_polarity = ?, polarity_source = ?, symbol_blob = ?,"
                " fetched_at = ? WHERE lcsc = ?",
                (
                    symbol.uuid or None,
                    json.dumps(
                        [{"number": pin.number, "label": pin.label} for pin in pins],
                        separators=(",", ":"),
                    ),
                    pin1_polarity(pins) if pins else None,
                    "symbol" if pins else None,
                    compress(symbol.shapes) if pins else None,
                    fetched_at,
                    lcsc,
                ),
            )
            return cursor.rowcount > 0

    def store_footprint(
        self, footprint: FootprintRecord, now: float | None = None, source: str = "live"
    ) -> None:
        """Store one footprint fetched by uuid (spec section 15.2)."""
        if not footprint.pads:
            return
        fetched_at = int(time.time() if now is None else now)
        with closing(self.connect()) as con, con:
            _upsert(
                con,
                "package",
                _PACKAGE_COLUMNS,
                self._package_row(
                    footprint.puuid,
                    footprint.package_name,
                    footprint.pads,
                    footprint.footprint_shapes,
                    footprint.footprint_source,
                    source,
                    fetched_at,
                ),
            )

    @staticmethod
    def _package_row(
        puuid: str,
        package_name: str,
        pads: list[dict],
        shapes: list[str],
        footprint_source: str,
        source: str,
        fetched_at: int,
    ) -> dict[str, Any]:
        """Build a package row; the name-derived rotation is the naming parser's, crawl sense."""
        parsed = parse_package_name(package_name)
        return {
            "puuid": puuid,
            "package_name": package_name,
            "family": parsed.family or None,
            "name_rotation": (
                parsed.rotation_correction
                if parsed.rotation_source == "naming_rule"
                else None
            ),
            "name_source": parsed.rotation_source,
            "name_confidence": parsed.parser_confidence,
            "pads_json": json.dumps(pads, separators=(",", ":")),
            "pads_format": PADS_CLASSIC,
            "footprint_blob": compress(shapes),
            "footprint_source": footprint_source,
            "source": source,
            "fetched_at": fetched_at,
        }

    def forget(self, lcsc: str) -> None:
        """Drop a part's row so the next scan fetches it again (its footprint row stays)."""
        with closing(self.connect()) as con, con:
            con.execute("DELETE FROM lcsc_map WHERE lcsc = ?", (lcsc,))

    def clear(self) -> None:
        """Drop every cached part and footprint."""
        with closing(self.connect()) as con, con:
            con.execute("DELETE FROM lcsc_map")
            con.execute("DELETE FROM package")

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def import_seed(self, seed_path: str) -> SeedImportResult:
        """Merge a seed file (spec section 5.2) into the cache without touching live rows.

        Seed rows replace older seed rows and fill gaps; a row the plugin fetched itself
        is never overwritten.  The seed must carry this cache's schema version.
        """
        result = SeedImportResult()
        with closing(self.connect()) as con, con:
            con.execute("ATTACH DATABASE ? AS seed", (str(seed_path),))
            try:
                version = con.execute(
                    "SELECT value FROM seed.meta WHERE key = 'schema_version'"
                ).fetchone()
                if version is None or str(version[0]) != SCHEMA_VERSION:
                    raise ValueError(
                        f"seed schema version {None if version is None else version[0]!r}"
                        f" does not match the cache's {SCHEMA_VERSION!r}"
                    )
                result.kept_live_parts = con.execute(
                    "SELECT COUNT(*) FROM seed.lcsc_map s JOIN lcsc_map m ON m.lcsc = s.lcsc"
                    " WHERE m.source = 'live'"
                ).fetchone()[0]
                result.kept_live_packages = con.execute(
                    "SELECT COUNT(*) FROM seed.package s JOIN package p ON p.puuid = s.puuid"
                    " WHERE p.source = 'live'"
                ).fetchone()[0]
                columns = ",".join(_LCSC_COLUMNS)
                updates = ",".join(
                    f"{column} = excluded.{column}" for column in _LCSC_COLUMNS[1:]
                )
                con.execute(
                    f"INSERT INTO lcsc_map ({columns}) SELECT {columns} FROM seed.lcsc_map"
                    f" WHERE 1 ON CONFLICT(lcsc) DO UPDATE SET {updates}"
                    " WHERE lcsc_map.source != 'live'"
                )
                result.parts = (
                    con.execute("SELECT COUNT(*) FROM seed.lcsc_map").fetchone()[0]
                    - result.kept_live_parts
                )
                columns = ",".join(_PACKAGE_COLUMNS)
                updates = ",".join(
                    f"{column} = excluded.{column}" for column in _PACKAGE_COLUMNS[1:]
                )
                con.execute(
                    f"INSERT INTO package ({columns}) SELECT {columns} FROM seed.package"
                    f" WHERE 1 ON CONFLICT(puuid) DO UPDATE SET {updates}"
                    " WHERE package.source != 'live'"
                )
                result.packages = (
                    con.execute("SELECT COUNT(*) FROM seed.package").fetchone()[0]
                    - result.kept_live_packages
                )
            finally:
                con.commit()
                con.execute("DETACH DATABASE seed")
        return result
