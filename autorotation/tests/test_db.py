"""Comprehensive tests for autorotation/db.py.

Run with:
    python3 -m pytest autorotation/tests/test_db.py -v
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from autorotation.db import (
    RotationRecord,
    ensure_schema,
    get,
    list_needs_resolution,
    mark_needs_manual,
    set_manual_override,
    upsert,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path):
    """A fresh, temporary DB path (file does not exist yet)."""
    return tmp_path / "autorotation_test.db"


@pytest.fixture()
def seeded_db(db_path):
    """DB with schema already created."""
    ensure_schema(db_path)
    return db_path


def _make_record(lcsc="C12345", fhash="deadbeef", rotation_deg=90,
                 offset_x_mm=0.5, offset_y_mm=-0.25, residual_mm=0.01,
                 resolved_at=None, manual_resolution_needed=False,
                 manual_override_active=False, error_message=None):
    return RotationRecord(
        lcsc=lcsc,
        footprint_hash=fhash,
        rotation_deg=rotation_deg,
        offset_x_mm=offset_x_mm,
        offset_y_mm=offset_y_mm,
        residual_mm=residual_mm,
        resolved_at=resolved_at,
        manual_resolution_needed=manual_resolution_needed,
        manual_override_active=manual_override_active,
        error_message=error_message,
    )


# ---------------------------------------------------------------------------
# 1. ensure_schema on a fresh path creates the file and table
# ---------------------------------------------------------------------------

def test_ensure_schema_creates_file_and_table(db_path):
    assert not db_path.exists()
    ensure_schema(db_path)
    assert db_path.exists()

    with sqlite3.connect(str(db_path)) as con:
        cur = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='lcsc_rotation'"
        )
        assert cur.fetchone() is not None, "lcsc_rotation table not found"


# ---------------------------------------------------------------------------
# 2. ensure_schema on an existing populated DB is a no-op
# ---------------------------------------------------------------------------

def test_ensure_schema_does_not_clobber_existing_data(seeded_db):
    rec = _make_record()
    upsert(seeded_db, rec)

    ensure_schema(seeded_db)  # call again on populated DB

    fetched = get(seeded_db, rec.lcsc, rec.footprint_hash)
    assert fetched is not None
    assert fetched.rotation_deg == rec.rotation_deg


# ---------------------------------------------------------------------------
# 3. ensure_schema is safe to call N times in a row
# ---------------------------------------------------------------------------

def test_ensure_schema_idempotent(db_path):
    for _ in range(5):
        ensure_schema(db_path)

    with sqlite3.connect(str(db_path)) as con:
        cur = con.execute("PRAGMA table_info(lcsc_rotation)")
        columns = [row[1] for row in cur.fetchall()]
    assert "lcsc" in columns
    assert "footprint_hash" in columns


# ---------------------------------------------------------------------------
# 4. get on a missing row returns None
# ---------------------------------------------------------------------------

def test_get_missing_returns_none(seeded_db):
    result = get(seeded_db, "CNOTEXIST", "badhash")
    assert result is None


# ---------------------------------------------------------------------------
# 5. upsert + get round-trip
# ---------------------------------------------------------------------------

def test_upsert_get_roundtrip(seeded_db):
    rec = _make_record(resolved_at="2024-01-01T00:00:00+00:00")
    upsert(seeded_db, rec)

    fetched = get(seeded_db, rec.lcsc, rec.footprint_hash)
    assert fetched is not None
    assert fetched.lcsc == rec.lcsc
    assert fetched.footprint_hash == rec.footprint_hash
    assert fetched.rotation_deg == rec.rotation_deg
    assert fetched.offset_x_mm == pytest.approx(rec.offset_x_mm)
    assert fetched.offset_y_mm == pytest.approx(rec.offset_y_mm)
    assert fetched.residual_mm == pytest.approx(rec.residual_mm)
    assert fetched.resolved_at == rec.resolved_at
    assert fetched.manual_resolution_needed == rec.manual_resolution_needed
    assert fetched.manual_override_active == rec.manual_override_active
    assert fetched.error_message == rec.error_message


# ---------------------------------------------------------------------------
# 6. upsert with same PK replaces the existing row
# ---------------------------------------------------------------------------

def test_upsert_replaces_existing(seeded_db):
    rec1 = _make_record(rotation_deg=90)
    upsert(seeded_db, rec1)

    rec2 = _make_record(rotation_deg=180)
    upsert(seeded_db, rec2)

    fetched = get(seeded_db, rec1.lcsc, rec1.footprint_hash)
    assert fetched.rotation_deg == 180


# ---------------------------------------------------------------------------
# 7. upsert auto-fills resolved_at when None
# ---------------------------------------------------------------------------

def test_upsert_autofills_resolved_at(seeded_db):
    rec = _make_record(resolved_at=None)
    upsert(seeded_db, rec)

    fetched = get(seeded_db, rec.lcsc, rec.footprint_hash)
    assert fetched.resolved_at is not None
    assert len(fetched.resolved_at) > 0


# ---------------------------------------------------------------------------
# 8. mark_needs_manual creates a row with flag set + error message
# ---------------------------------------------------------------------------

def test_mark_needs_manual_creates_row(seeded_db):
    mark_needs_manual(seeded_db, "C99999", "aabbccdd", "Solver failed")

    fetched = get(seeded_db, "C99999", "aabbccdd")
    assert fetched is not None
    assert fetched.manual_resolution_needed is True
    assert fetched.error_message == "Solver failed"


# ---------------------------------------------------------------------------
# 9. mark_needs_manual on an existing row preserves rotation data, sets flag
# ---------------------------------------------------------------------------

def test_mark_needs_manual_preserves_rotation(seeded_db):
    rec = _make_record(rotation_deg=45, resolved_at="2024-06-01T12:00:00+00:00",
                       manual_resolution_needed=False)
    upsert(seeded_db, rec)

    mark_needs_manual(seeded_db, rec.lcsc, rec.footprint_hash, "Retry limit hit")

    fetched = get(seeded_db, rec.lcsc, rec.footprint_hash)
    assert fetched.rotation_deg == 45         # preserved
    assert fetched.manual_resolution_needed is True
    assert fetched.error_message == "Retry limit hit"


# ---------------------------------------------------------------------------
# 10. set_manual_override sets override flag and rotation values
# ---------------------------------------------------------------------------

def test_set_manual_override(seeded_db):
    set_manual_override(seeded_db, "C11111", "cafebabe", rot=270, ox=1.0, oy=-0.5)

    fetched = get(seeded_db, "C11111", "cafebabe")
    assert fetched is not None
    assert fetched.manual_override_active is True
    assert fetched.rotation_deg == 270
    assert fetched.offset_x_mm == pytest.approx(1.0)
    assert fetched.offset_y_mm == pytest.approx(-0.5)
    assert fetched.resolved_at is not None


def test_set_manual_override_updates_existing(seeded_db):
    rec = _make_record(rotation_deg=90)
    upsert(seeded_db, rec)

    set_manual_override(seeded_db, rec.lcsc, rec.footprint_hash, rot=180, ox=0.0, oy=0.0)

    fetched = get(seeded_db, rec.lcsc, rec.footprint_hash)
    assert fetched.rotation_deg == 180
    assert fetched.manual_override_active is True


# ---------------------------------------------------------------------------
# 11. list_needs_resolution filtering
# ---------------------------------------------------------------------------

def test_list_needs_resolution_basic(seeded_db):
    # Pair A: auto-resolved (rotation_deg set, manual_resolution_needed=0)
    rec_a = _make_record(lcsc="CA", fhash="ha", rotation_deg=90,
                         manual_resolution_needed=False, manual_override_active=False)
    upsert(seeded_db, rec_a)

    # Pair B: manual override active
    set_manual_override(seeded_db, "CB", "hb", rot=0, ox=0.0, oy=0.0)

    # Pair C: no row at all

    pairs = [("CA", "ha"), ("CB", "hb"), ("CC", "hc")]
    result = list_needs_resolution(seeded_db, pairs)
    assert result == [("CC", "hc")]


def test_list_needs_resolution_manual_needed_not_returned(seeded_db):
    # Pair D: auto-resolution failed — should NOT be retried automatically
    mark_needs_manual(seeded_db, "CD", "hd", "timeout")

    pairs = [("CD", "hd")]
    result = list_needs_resolution(seeded_db, pairs)
    assert result == []


def test_list_needs_resolution_empty_input(seeded_db):
    result = list_needs_resolution(seeded_db, [])
    assert result == []


def test_list_needs_resolution_all_unresolved(seeded_db):
    pairs = [("CX", "hx"), ("CY", "hy")]
    result = list_needs_resolution(seeded_db, pairs)
    assert set(result) == {("CX", "hx"), ("CY", "hy")}


def test_list_needs_resolution_mixed(seeded_db):
    """Thorough test: A auto-ok, B override, C nothing, D manual-needed → only C."""
    upsert(seeded_db, _make_record(lcsc="M_A", fhash="fa", rotation_deg=45))
    set_manual_override(seeded_db, "M_B", "fb", rot=90, ox=0.0, oy=0.0)
    mark_needs_manual(seeded_db, "M_D", "fd", "bad data")

    pairs = [("M_A", "fa"), ("M_B", "fb"), ("M_C", "fc"), ("M_D", "fd")]
    result = list_needs_resolution(seeded_db, pairs)
    assert result == [("M_C", "fc")]


# ---------------------------------------------------------------------------
# 12. Concurrent write safety — two sequential upserts from separate connections
# ---------------------------------------------------------------------------

def test_concurrent_upserts_no_deadlock(seeded_db):
    """Two separate connections upsert the same PK sequentially; no deadlock."""
    rec1 = _make_record(rotation_deg=10)
    rec2 = _make_record(rotation_deg=20)

    import contextlib

    with contextlib.closing(sqlite3.connect(str(seeded_db))) as con1:
        con1.execute(
            """
            INSERT OR REPLACE INTO lcsc_rotation (
                lcsc, footprint_hash, rotation_deg, offset_x_mm, offset_y_mm,
                residual_mm, resolved_at, manual_resolution_needed,
                manual_override_active, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (rec1.lcsc, rec1.footprint_hash, rec1.rotation_deg,
             rec1.offset_x_mm, rec1.offset_y_mm, rec1.residual_mm,
             "2024-01-01T00:00:00+00:00",
             int(rec1.manual_resolution_needed), int(rec1.manual_override_active),
             rec1.error_message),
        )
        con1.commit()

    with contextlib.closing(sqlite3.connect(str(seeded_db))) as con2:
        con2.execute(
            """
            INSERT OR REPLACE INTO lcsc_rotation (
                lcsc, footprint_hash, rotation_deg, offset_x_mm, offset_y_mm,
                residual_mm, resolved_at, manual_resolution_needed,
                manual_override_active, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (rec2.lcsc, rec2.footprint_hash, rec2.rotation_deg,
             rec2.offset_x_mm, rec2.offset_y_mm, rec2.residual_mm,
             "2024-01-02T00:00:00+00:00",
             int(rec2.manual_resolution_needed), int(rec2.manual_override_active),
             rec2.error_message),
        )
        con2.commit()

    fetched = get(seeded_db, rec2.lcsc, rec2.footprint_hash)
    assert fetched.rotation_deg == 20
