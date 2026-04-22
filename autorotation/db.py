"""DB module for the autorotation feature.

Manages a SQLite cache of LCSC component rotation/offset corrections.
The lookup key is (lcsc_code, kicad_footprint_hash).

The `lcsc_rotation` table coexists in the plugin's existing
`<project>/jlcpcb/project.db` file alongside `part_info` (managed by
store.Store) — one DB file per project, two tables. Use
`db_path_for_project(project_path)` to get the same path that Store uses.

Python 3.9 compatible; stdlib only.
"""

import contextlib
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple


def db_path_for_project(project_path) -> str:
    """Path to the shared jlcpcb/project.db — matches store.Store.dbfile."""
    return os.path.join(str(project_path), "jlcpcb", "project.db")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class RotationRecord:
    lcsc: str
    footprint_hash: str
    rotation_deg: Optional[int]
    offset_x_mm: Optional[float]
    offset_y_mm: Optional[float]
    residual_mm: Optional[float]
    resolved_at: Optional[str]          # ISO 8601 UTC
    manual_resolution_needed: bool
    manual_override_active: bool
    error_message: Optional[str]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS lcsc_rotation (
    lcsc                     TEXT    NOT NULL,
    footprint_hash           TEXT    NOT NULL,
    rotation_deg             INTEGER,
    offset_x_mm              REAL,
    offset_y_mm              REAL,
    residual_mm              REAL,
    resolved_at              TEXT,
    manual_resolution_needed INTEGER DEFAULT 0,
    manual_override_active   INTEGER DEFAULT 0,
    error_message            TEXT,
    PRIMARY KEY (lcsc, footprint_hash)
);
"""

# Columns that may need to be added to a pre-existing table when the schema
# evolves.  Each entry is (column_name, column_definition).
_MIGRATION_COLUMNS: List[Tuple[str, str]] = [
    # Future migrations go here, e.g.:
    # ("new_column", "REAL DEFAULT 0"),
]


def _now_utc() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _row_to_record(row: tuple) -> RotationRecord:
    """Convert a raw SQLite row to a RotationRecord."""
    (
        lcsc,
        footprint_hash,
        rotation_deg,
        offset_x_mm,
        offset_y_mm,
        residual_mm,
        resolved_at,
        manual_resolution_needed,
        manual_override_active,
        error_message,
    ) = row
    return RotationRecord(
        lcsc=lcsc,
        footprint_hash=footprint_hash,
        rotation_deg=rotation_deg,
        offset_x_mm=offset_x_mm,
        offset_y_mm=offset_y_mm,
        residual_mm=residual_mm,
        resolved_at=resolved_at,
        manual_resolution_needed=bool(manual_resolution_needed),
        manual_override_active=bool(manual_override_active),
        error_message=error_message,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_schema(db_path) -> None:
    """Create the table if missing; add any new columns for forward migrations.

    Idempotent: safe to call on a brand-new file or on a file that already
    has the table with data.  Does NOT drop or rename existing columns.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with contextlib.closing(sqlite3.connect(str(db_path))) as con, con:
        con.execute(_CREATE_TABLE)

        # Forward-migration: add any columns that don't exist yet.
        existing = {
            row[1]
            for row in con.execute("PRAGMA table_info(lcsc_rotation)")
        }
        for col_name, col_def in _MIGRATION_COLUMNS:
            if col_name not in existing:
                con.execute(
                    f"ALTER TABLE lcsc_rotation ADD COLUMN {col_name} {col_def}"
                )


def get(db_path, lcsc: str, footprint_hash: str) -> Optional[RotationRecord]:
    """Return the RotationRecord for (lcsc, footprint_hash), or None."""
    with contextlib.closing(sqlite3.connect(str(db_path))) as con, con:
        cur = con.execute(
            """
            SELECT lcsc, footprint_hash, rotation_deg, offset_x_mm, offset_y_mm,
                   residual_mm, resolved_at, manual_resolution_needed,
                   manual_override_active, error_message
            FROM lcsc_rotation
            WHERE lcsc = ? AND footprint_hash = ?
            """,
            (lcsc, footprint_hash),
        )
        row = cur.fetchone()
    return _row_to_record(row) if row is not None else None


def upsert(db_path, record: RotationRecord) -> None:
    """INSERT OR REPLACE the record.

    If record.resolved_at is None, it is set to the current UTC timestamp
    before writing.
    """
    resolved_at = record.resolved_at or _now_utc()

    with contextlib.closing(sqlite3.connect(str(db_path))) as con, con:
        con.execute(
            """
            INSERT OR REPLACE INTO lcsc_rotation (
                lcsc, footprint_hash, rotation_deg, offset_x_mm, offset_y_mm,
                residual_mm, resolved_at, manual_resolution_needed,
                manual_override_active, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.lcsc,
                record.footprint_hash,
                record.rotation_deg,
                record.offset_x_mm,
                record.offset_y_mm,
                record.residual_mm,
                resolved_at,
                int(record.manual_resolution_needed),
                int(record.manual_override_active),
                record.error_message,
            ),
        )


def mark_needs_manual(
    db_path, lcsc: str, footprint_hash: str, error: str
) -> None:
    """Record an auto-resolution failure.

    If a row already exists its rotation/offset data is preserved; only
    manual_resolution_needed and error_message are updated.  If no row
    exists, a new one is created with rotation_deg=NULL and the flag set.
    """
    with contextlib.closing(sqlite3.connect(str(db_path))) as con, con:
        con.execute(
            """
            INSERT INTO lcsc_rotation (
                lcsc, footprint_hash, manual_resolution_needed, error_message
            ) VALUES (?, ?, 1, ?)
            ON CONFLICT(lcsc, footprint_hash) DO UPDATE SET
                manual_resolution_needed = 1,
                error_message = excluded.error_message
            """,
            (lcsc, footprint_hash, error),
        )


def set_manual_override(
    db_path,
    lcsc: str,
    footprint_hash: str,
    rot: int,
    ox: float,
    oy: float,
) -> None:
    """Record a user-edited transform and mark manual_override_active=1."""
    resolved_at = _now_utc()

    with contextlib.closing(sqlite3.connect(str(db_path))) as con, con:
        con.execute(
            """
            INSERT INTO lcsc_rotation (
                lcsc, footprint_hash, rotation_deg, offset_x_mm, offset_y_mm,
                resolved_at, manual_override_active
            ) VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(lcsc, footprint_hash) DO UPDATE SET
                rotation_deg           = excluded.rotation_deg,
                offset_x_mm            = excluded.offset_x_mm,
                offset_y_mm            = excluded.offset_y_mm,
                resolved_at            = excluded.resolved_at,
                manual_override_active = 1
            """,
            (lcsc, footprint_hash, rot, ox, oy, resolved_at),
        )


def list_needs_resolution(
    db_path, known_lcsc_codes: List[Tuple[str, str]]
) -> List[Tuple[str, str]]:
    """Return the subset of (lcsc, footprint_hash) pairs that need resolution.

    A pair "needs resolution" only when NO row exists for it yet.  Pairs
    that already have:
      - manual_override_active=1, OR
      - manual_resolution_needed=0 AND rotation_deg IS NOT NULL  (auto-OK), OR
      - manual_resolution_needed=1                               (auto-failed)
    are all considered "done" and are excluded from the result.

    Empty input → empty output.
    """
    if not known_lcsc_codes:
        return []

    with contextlib.closing(sqlite3.connect(str(db_path))) as con, con:
        # Fetch every row that matches any of the supplied LCSC codes so we
        # can do the filtering in Python without dynamic SQL placeholders.
        lcsc_set = {lc for lc, _ in known_lcsc_codes}
        placeholders = ",".join("?" * len(lcsc_set))
        cur = con.execute(
            f"""
            SELECT lcsc, footprint_hash,
                   rotation_deg, manual_resolution_needed, manual_override_active
            FROM lcsc_rotation
            WHERE lcsc IN ({placeholders})
            """,
            tuple(lcsc_set),
        )
        rows = cur.fetchall()

    # Build a set of (lcsc, hash) pairs that are already resolved in any way.
    resolved: set = set()
    for lcsc, fh, rotation_deg, manual_needed, override_active in rows:
        key = (lcsc, fh)
        if override_active:
            resolved.add(key)
        elif not manual_needed and rotation_deg is not None:
            resolved.add(key)
        elif manual_needed:
            resolved.add(key)

    return [pair for pair in known_lcsc_codes if pair not in resolved]
