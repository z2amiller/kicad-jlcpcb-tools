"""Per-project verdict store: table ``footprint_verdict`` in ``project.db``.

A verdict is keyed on the part and the footprint, ``(lcsc, footprint_hash)``, where
the hash is ``kicad_adapter.verdict_key``: the KiCad pad geometry plus the pin
functions.  Editing a footprint's pads or swapping its pin functions orphans its
verdict and its override together, by design.  Stdlib only; every method opens
its own connection, so the worker thread may write while the main thread reads.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, fields
import sqlite3
import time
from typing import Optional

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
    footprint_name: Optional[str] = None
    puuid: Optional[str] = None
    status: str = PENDING
    fit: Optional[str] = None
    rotation: Optional[int] = None
    method: Optional[str] = None
    confidence: Optional[str] = None
    name_rotation: Optional[int] = None
    polarity_light: Optional[str] = None
    pad_count_kicad: Optional[int] = None
    pad_count_jlc: Optional[int] = None
    overlap_min: Optional[float] = None
    overlap_mean: Optional[float] = None
    angular_rms: Optional[float] = None
    residual_mm: Optional[float] = None
    # How far the JLC body overhangs the KiCad courtyard, in mm.
    body_excess_mm: Optional[float] = None
    # JLC's package origin in the KiCad footprint frame, NULL without one.
    origin_dx_mm: Optional[float] = None
    origin_dy_mm: Optional[float] = None
    override_rotation: Optional[int] = None
    override_note: Optional[str] = None
    notes: Optional[str] = None
    resolved_at: Optional[int] = None

    @property
    def source(self) -> str:
        """Return where the emitted rotation comes from: ``override``, ``derived`` or ``raw``."""
        if self.override_rotation is not None:
            return "override"
        if self.rotation is not None and self.status in APPLIED_STATUSES:
            return "derived"
        return "raw"

    @property
    def emitted_rotation(self) -> Optional[int]:
        """Return the correction the CPL applies: the override, else a green or yellow rotation.

        None leaves the part at its raw angle.
        """
        if self.override_rotation is not None:
            return self.override_rotation
        if self.source == "derived":
            return self.rotation
        return None

    @property
    def origin(self) -> Optional[tuple[float, float]]:
        """Return the package origin the CPL may place this part at, or None.

        Only a green or yellow row has one.  An override changes the rotation only,
        so an overridden green or yellow row keeps its origin.  A row marked pending
        keeps the two columns but reports no origin until it is resolved again and
        they are rewritten, so a part waiting for its data goes out at the CPL's
        own pad-box centre, not at an origin it may no longer have.
        """
        if self.status not in APPLIED_STATUSES:
            return None
        if self.origin_dx_mm is None or self.origin_dy_mm is None:
            return None
        return (self.origin_dx_mm, self.origin_dy_mm)

    @property
    def display_text(self) -> str:
        """Return the emitted rotation as text, or "raw" when the CPL keeps the raw angle.

        A derived green rotation reads "180°", a yellow one "90° !" and an override
        "270° set".  A pending row reads "raw", which is what the CPL emits for it
        until it is resolved.
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
# Columns an existing table may lack, added to it when the store opens.
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

    def get(self, lcsc: str, footprint_hash: str) -> Optional[StoredVerdict]:
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
        now: Optional[float] = None,
    ) -> None:
        """Record that a part is waiting for its EasyEDA data.

        A new row is created pending.  An existing row turns pending with its
        rotation, method, fit and notes cleared; its override, its origin columns
        and its other measurements stay until the next ``save``.
        """
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
        now: Optional[float] = None,
    ) -> StoredVerdict:
        """Store a resolver verdict, keeping any override already on the row.

        The origin columns follow the verdict's own rule: a green or yellow verdict
        writes JLC's package origin, everything else writes NULL, so a part that
        stops fitting loses the origin the CPL would have used.
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
        self, lcsc: str, footprint_hash: str, rotation: Optional[int], note: str = ""
    ) -> None:
        """Set (or clear, with None) the user's rotation on an existing verdict row."""
        with closing(self.connect()) as con, con:
            con.execute(
                "UPDATE footprint_verdict SET override_rotation = ?, override_note = ?"
                " WHERE lcsc = ? AND footprint_hash = ?",
                (rotation, note or None, lcsc, footprint_hash),
            )

    def delete(self, lcsc: str, footprint_hash: str) -> None:
        """Drop one verdict row outright, its override included.

        ``mark_pending`` keeps the row and its override; this removes both, so the
        part's next verdict starts with no override.
        """
        with closing(self.connect()) as con, con:
            con.execute(
                "DELETE FROM footprint_verdict WHERE lcsc = ? AND footprint_hash = ?",
                (lcsc, footprint_hash),
            )
