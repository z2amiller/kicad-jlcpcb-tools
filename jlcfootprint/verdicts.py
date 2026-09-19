"""Per-project verdict store: table ``footprint_verdict`` in ``project.db`` (spec section 5.3).

A verdict is keyed on the part and the KiCad pad geometry, ``(lcsc, footprint_hash)``,
so editing a footprint orphans its verdict and its override together, by design.
Stdlib only; every method opens its own connection, so the worker thread may write
while the main thread reads.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, fields
import sqlite3
import time

from .resolver import APPLIED_STATUSES, Verdict

CONNECT_TIMEOUT_S = 5.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS footprint_verdict (
  lcsc              TEXT NOT NULL,
  footprint_hash    TEXT NOT NULL,
  footprint_name    TEXT,
  puuid             TEXT,
  status            TEXT NOT NULL,
  fit               TEXT,
  rotation          INTEGER,
  method            TEXT,
  confidence        TEXT,
  name_rotation     INTEGER,
  polarity_light    TEXT,
  pad_count_kicad   INTEGER,
  pad_count_jlc     INTEGER,
  overlap_min       REAL,
  overlap_mean      REAL,
  angular_rms       REAL,
  residual_mm       REAL,
  body_excess_mm    REAL,
  origin_dx_mm      REAL,
  origin_dy_mm      REAL,
  override_rotation INTEGER,
  override_note     TEXT,
  notes             TEXT,
  resolved_at       INTEGER,
  PRIMARY KEY (lcsc, footprint_hash)
);
"""

PENDING = "pending"


@dataclass
class StoredVerdict:
    """One row of ``footprint_verdict``."""

    lcsc: str
    footprint_hash: str
    footprint_name: str | None = None
    puuid: str | None = None
    status: str = PENDING
    fit: str | None = None
    rotation: int | None = None
    method: str | None = None
    confidence: str | None = None
    name_rotation: int | None = None
    polarity_light: str | None = None
    pad_count_kicad: int | None = None
    pad_count_jlc: int | None = None
    overlap_min: float | None = None
    overlap_mean: float | None = None
    angular_rms: float | None = None
    residual_mm: float | None = None
    body_excess_mm: float | None = None  # spec 16.6 item 4: the body-size caveat
    # JLC's package origin in the KiCad footprint frame (spec 17.3), NULL without one.
    origin_dx_mm: float | None = None
    origin_dy_mm: float | None = None
    override_rotation: int | None = None
    override_note: str | None = None
    notes: str | None = None
    resolved_at: int | None = None

    @property
    def source(self) -> str:
        """Return where the emitted rotation comes from: ``override``, ``derived`` or ``raw``."""
        if self.override_rotation is not None:
            return "override"
        if self.rotation is not None and self.status in APPLIED_STATUSES:
            return "derived"
        return "raw"

    @property
    def emitted_rotation(self) -> int | None:
        """Return the correction the CPL applies (spec section 8), None for the raw angle."""
        if self.override_rotation is not None:
            return self.override_rotation
        if self.source == "derived":
            return self.rotation
        return None

    @property
    def origin(self) -> tuple[float, float] | None:
        """Return the package origin the CPL may place this part at, or None (spec 17.3).

        An override changes the rotation only, so an overridden green or yellow row
        keeps its origin; a row marked pending keeps the two columns but reports no
        origin until the re-resolve rewrites them, which is what keeps a part being
        re-fetched on upstream's pad-box centre.
        """
        if self.status not in APPLIED_STATUSES:
            return None
        if self.origin_dx_mm is None or self.origin_dy_mm is None:
            return None
        return (self.origin_dx_mm, self.origin_dy_mm)

    @property
    def display_text(self) -> str:
        """Return the Rotation column text: the angle, "set", "!" or "raw" (spec 16.3).

        A pending row reads "raw", which is what the CPL emits for it; the ellipsis
        the first design showed while fetching is retired, and the JLC column's clock
        carries that state instead.
        """
        if self.override_rotation is not None:
            return f"{self.override_rotation}° set"
        if self.source == "derived":
            return (
                f"{self.rotation}° !"
                if self.status == "yellow"
                else f"{self.rotation}°"
            )
        return "raw"


_COLUMNS = tuple(field.name for field in fields(StoredVerdict))
# Columns added after the first release, created on an existing table at open.
_ADDED_COLUMNS = (
    ("body_excess_mm", "REAL"),
    ("origin_dx_mm", "REAL"),
    ("origin_dy_mm", "REAL"),
)


def _table_columns(con: sqlite3.Connection) -> set:
    """Return the column names ``footprint_verdict`` currently has."""
    return {row["name"] for row in con.execute("PRAGMA table_info(footprint_verdict)")}


def add_missing_columns(con: sqlite3.Connection, present: set) -> None:
    """Add the columns an older ``footprint_verdict`` lacks, tolerating a racing writer.

    Two KiCad processes can open one project's database at the same moment, both
    read the same old column list and both try the same ``ALTER TABLE``; the loser
    must not fail the plugin on "duplicate column name".  A failure that leaves the
    column missing is a real one and is re-raised.
    """
    for column, kind in _ADDED_COLUMNS:
        if column in present:
            continue
        try:
            con.execute(f"ALTER TABLE footprint_verdict ADD COLUMN {column} {kind}")
        except sqlite3.OperationalError:
            if column not in _table_columns(con):
                raise


class VerdictStore:
    """The ``footprint_verdict`` table in one project's ``project.db``."""

    def __init__(self, db_path: str) -> None:
        self.db_path = str(db_path)
        with closing(self.connect()) as con, con:
            con.executescript(SCHEMA)
            add_missing_columns(con, _table_columns(con))

    def connect(self) -> sqlite3.Connection:
        """Open a connection with row access by name."""
        con = sqlite3.connect(self.db_path, timeout=CONNECT_TIMEOUT_S)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _from_row(row: sqlite3.Row) -> StoredVerdict:
        return StoredVerdict(**{column: row[column] for column in _COLUMNS})

    def get(self, lcsc: str, footprint_hash: str) -> StoredVerdict | None:
        """Return the stored verdict for one part on one pad geometry, or None."""
        with closing(self.connect()) as con:
            row = con.execute(
                "SELECT * FROM footprint_verdict WHERE lcsc = ? AND footprint_hash = ?",
                (lcsc, footprint_hash),
            ).fetchone()
        return None if row is None else self._from_row(row)

    def all(self) -> list[StoredVerdict]:
        """Return every stored verdict."""
        with closing(self.connect()) as con:
            rows = con.execute(
                "SELECT * FROM footprint_verdict ORDER BY lcsc, footprint_hash"
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def mark_pending(
        self,
        lcsc: str,
        footprint_hash: str,
        footprint_name: str,
        now: float | None = None,
    ) -> None:
        """Record that a part is waiting for its EasyEDA data; an override is kept."""
        with closing(self.connect()) as con, con:
            con.execute(
                "INSERT INTO footprint_verdict (lcsc, footprint_hash, footprint_name, status,"
                " resolved_at) VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(lcsc, footprint_hash) DO UPDATE SET status = excluded.status,"
                " footprint_name = excluded.footprint_name, rotation = NULL, method = NULL,"
                " fit = NULL, notes = NULL, resolved_at = excluded.resolved_at",
                (
                    lcsc,
                    footprint_hash,
                    footprint_name,
                    PENDING,
                    int(time.time() if now is None else now),
                ),
            )

    def save(
        self,
        lcsc: str,
        footprint_hash: str,
        footprint_name: str,
        puuid: str,
        verdict: Verdict,
        now: float | None = None,
    ) -> StoredVerdict:
        """Store a resolver verdict, keeping any override already on the row.

        The origin columns follow the verdict's own rule (spec 17.3): a green or
        yellow verdict writes JLC's package origin, everything else writes NULL, so
        a part that stops fitting loses the origin the CPL would have used.
        """
        origin = verdict.origin
        stored = StoredVerdict(
            lcsc=lcsc,
            footprint_hash=footprint_hash,
            footprint_name=footprint_name,
            puuid=puuid or None,
            status=verdict.status,
            fit=verdict.fit,
            rotation=verdict.rotation,
            method=verdict.method,
            confidence=verdict.confidence,
            name_rotation=verdict.name_rotation,
            polarity_light=verdict.polarity_light,
            pad_count_kicad=verdict.pad_count_kicad,
            pad_count_jlc=verdict.pad_count_jlc,
            overlap_min=verdict.overlap_min,
            overlap_mean=verdict.overlap_mean,
            angular_rms=verdict.angular_rms,
            residual_mm=verdict.residual_mm,
            body_excess_mm=verdict.body_excess_mm,
            origin_dx_mm=origin[0] if origin is not None else None,
            origin_dy_mm=origin[1] if origin is not None else None,
            notes=verdict.note_text or None,
            resolved_at=int(time.time() if now is None else now),
        )
        kept = ("override_rotation", "override_note")
        columns = [column for column in _COLUMNS if column not in kept]
        updates = ",".join(f"{column} = excluded.{column}" for column in columns[2:])
        with closing(self.connect()) as con, con:
            con.execute(
                f"INSERT INTO footprint_verdict ({','.join(columns)})"
                f" VALUES ({','.join('?' for _ in columns)})"
                f" ON CONFLICT(lcsc, footprint_hash) DO UPDATE SET {updates}",
                tuple(getattr(stored, column) for column in columns),
            )
            row = con.execute(
                "SELECT * FROM footprint_verdict WHERE lcsc = ? AND footprint_hash = ?",
                (lcsc, footprint_hash),
            ).fetchone()
        return self._from_row(row)

    def set_override(
        self, lcsc: str, footprint_hash: str, rotation: int | None, note: str = ""
    ) -> None:
        """Set (or clear, with None) the user's rotation on an existing verdict row."""
        with closing(self.connect()) as con, con:
            con.execute(
                "UPDATE footprint_verdict SET override_rotation = ?, override_note = ?"
                " WHERE lcsc = ? AND footprint_hash = ?",
                (rotation, note or None, lcsc, footprint_hash),
            )

    def delete(self, lcsc: str, footprint_hash: str) -> None:
        """Drop one verdict row (a refresh re-resolves the part from scratch)."""
        with closing(self.connect()) as con, con:
            con.execute(
                "DELETE FROM footprint_verdict WHERE lcsc = ? AND footprint_hash = ?",
                (lcsc, footprint_hash),
            )
