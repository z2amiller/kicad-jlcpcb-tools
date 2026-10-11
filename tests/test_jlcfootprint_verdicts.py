"""Tests for the per-project verdict store: round trips, overrides, emitted rotation, display, origin."""

from contextlib import closing
import sqlite3

import pytest

from jlcfootprint.fit import package_origin
from jlcfootprint.resolver import Verdict
from jlcfootprint.verdicts import (
    PENDING,
    SCHEMA,
    StoredVerdict,
    VerdictStore,
    add_missing_columns,
)

from .test_jlcfootprint_origin import placement

SPEC_COLUMNS = [
    "lcsc",
    "footprint_hash",
    "footprint_name",
    "puuid",
    "status",
    "fit",
    "rotation",
    "method",
    "confidence",
    "name_rotation",
    "polarity_light",
    "pad_count_kicad",
    "pad_count_jlc",
    "overlap_min",
    "overlap_mean",
    "angular_rms",
    "residual_mm",
    "body_excess_mm",
    "origin_dx_mm",
    "origin_dy_mm",
    "override_rotation",
    "override_note",
    "notes",
    "resolved_at",
]


@pytest.fixture
def store(tmp_path):
    """Return a verdict store in a fresh project database."""
    return VerdictStore(str(tmp_path / "project.db"))


def _verdict(**overrides) -> Verdict:
    verdict = Verdict(
        status="green",
        fit="fits",
        rotation=180,
        method="geometry",
        confidence="high",
        name_rotation=180,
        pad_count_kicad=3,
        pad_count_jlc=3,
        matched_pads=3,
        overlap_min=0.9,
        overlap_mean=0.95,
        angular_rms=0.1,
        residual_mm=0.01,
        notes=["hello"],
    )
    for key, value in overrides.items():
        setattr(verdict, key, value)
    return verdict


def test_schema_matches_the_spec(store):
    """The table has exactly these columns, in this order, keyed on (lcsc, footprint_hash)."""
    with closing(sqlite3.connect(store.db_path)) as con:
        columns = [
            row[1] for row in con.execute("PRAGMA table_info(footprint_verdict)")
        ]
        keys = [
            row[1]
            for row in con.execute("PRAGMA table_info(footprint_verdict)")
            if row[5]
        ]
    assert columns == SPEC_COLUMNS
    assert keys == ["lcsc", "footprint_hash"]
    VerdictStore(store.db_path)  # opening it again changes nothing


def test_an_older_table_gains_the_caveat_column(tmp_path):
    """A table without the body caveat's column gains it in place, rows kept."""
    path = tmp_path / "project.db"
    with closing(sqlite3.connect(path)) as con, con:
        con.executescript(SCHEMA.replace("  body_excess_mm    REAL,\n", ""))
        con.execute(
            "INSERT INTO footprint_verdict (lcsc, footprint_hash, status, rotation,"
            " override_rotation) VALUES ('C1', 'h', 'green', 90, 180)"
        )
    store = VerdictStore(str(path))
    stored = store.get("C1", "h")
    assert stored is not None
    assert (stored.rotation, stored.override_rotation, stored.body_excess_mm) == (
        90,
        180,
        None,
    )
    with closing(sqlite3.connect(path)) as con:
        columns = [
            row[1] for row in con.execute("PRAGMA table_info(footprint_verdict)")
        ]
    assert columns[:-1] == SPEC_COLUMNS[:17] + SPEC_COLUMNS[18:]
    assert columns[-1] == "body_excess_mm"
    saved = store.save("C1", "h", "F", "p", _verdict(body_excess_mm=0.9))
    assert (saved.body_excess_mm, saved.override_rotation) == (0.9, 180)


def test_save_and_get_round_trip(store):
    """Every verdict field lands in its column and comes back."""
    stored = store.save("C2132", "abc", "Package:SOT-23", "puuid", _verdict(), now=42)
    assert store.get("C2132", "abc") == stored
    assert stored.footprint_name == "Package:SOT-23"
    assert stored.puuid == "puuid"
    assert (stored.status, stored.fit, stored.rotation, stored.method) == (
        "green",
        "fits",
        180,
        "geometry",
    )
    assert (stored.confidence, stored.name_rotation, stored.polarity_light) == (
        "high",
        180,
        None,
    )
    assert (stored.pad_count_kicad, stored.pad_count_jlc) == (3, 3)
    assert (
        stored.overlap_min,
        stored.overlap_mean,
        stored.angular_rms,
        stored.residual_mm,
    ) == (0.9, 0.95, 0.1, 0.01)
    assert stored.notes == "hello"
    assert stored.resolved_at == 42
    assert store.get("C2132", "other") is None
    assert [v.footprint_hash for v in store.all()] == ["abc"]


def test_override_survives_resolving_and_pending(store):
    """A user's rotation stays on the row through re-resolution and a pending mark."""
    store.save("C1", "h", "fp", "p", _verdict(), now=1)
    store.set_override("C1", "h", 90, "checked in the preview")
    stored = store.save("C1", "h", "fp", "p", _verdict(rotation=270), now=2)
    assert (stored.rotation, stored.override_rotation, stored.override_note) == (
        270,
        90,
        "checked in the preview",
    )
    store.mark_pending("C1", "h", "fp", now=3)
    pending = store.get("C1", "h")
    assert pending.status == PENDING
    assert pending.rotation is None
    assert pending.override_rotation == 90
    store.set_override("C1", "h", None)
    assert store.get("C1", "h").override_rotation is None
    store.delete("C1", "h")
    assert store.get("C1", "h") is None


def test_mark_pending_creates_a_row(store):
    """A part waiting for its data has a pending row with its footprint name."""
    store.mark_pending("C1", "h", "fp", now=3)
    stored = store.get("C1", "h")
    assert (stored.status, stored.footprint_name, stored.resolved_at) == (
        PENDING,
        "fp",
        3,
    )
    assert stored.source == "raw"
    assert stored.emitted_rotation is None
    assert stored.display_text == "raw"  # what the CPL emits while the part waits


@pytest.mark.parametrize(
    ("status", "rotation", "override", "source", "emitted", "text"),
    [
        ("green", 180, None, "derived", 180, "180°"),
        ("yellow", 90, None, "derived", 90, "90° !"),
        ("red", 90, None, "raw", None, "raw"),
        ("unknown", None, None, "raw", None, "raw"),
        ("red", None, 270, "override", 270, "270° set"),
        ("green", 0, 180, "override", 180, "180° set"),
    ],
)
def test_emitted_rotation_precedence_and_display(
    status, rotation, override, source, emitted, text
):
    """Override first, then a green or yellow derived value, else the raw angle."""
    stored = StoredVerdict(
        "C1", "h", status=status, rotation=rotation, override_rotation=override
    )
    assert stored.source == source
    assert stored.emitted_rotation == emitted
    assert stored.display_text == text


def test_a_second_process_adding_the_same_column_is_not_an_error(store):
    """Two KiCad processes add the same column to one project.db at once; the loser must not raise."""
    with closing(store.connect()) as con, con:
        # The column is already there: this is the state the loser of the race sees
        # after the winner's ALTER, holding the column list it read beforehand.
        add_missing_columns(con, set())
        assert "body_excess_mm" in {
            row["name"] for row in con.execute("PRAGMA table_info(footprint_verdict)")
        }


def test_a_migration_failure_that_leaves_the_column_missing_still_raises(tmp_path):
    """Only "duplicate column" is tolerated: a real failure is not silently swallowed."""
    with closing(sqlite3.connect(str(tmp_path / "other.db"))) as con:
        con.row_factory = sqlite3.Row
        with pytest.raises(sqlite3.OperationalError):
            add_missing_columns(con, set())


# ---------------------------------------------------------------------------
# JLC's package origin in the stored row
# ---------------------------------------------------------------------------


def green(**overrides) -> Verdict:
    """Return a green verdict carrying a placement, with fields overridden."""
    verdict = Verdict(
        status="green", fit="fits", rotation=90, placement=placement(0, 0.4, -0.2)
    )
    for name, value in overrides.items():
        setattr(verdict, name, value)
    return verdict


def test_saving_a_fitting_verdict_writes_the_origin_and_a_refusal_writes_null(store):
    """Green and yellow rows carry the origin; red, unknown and pending rows do not."""
    fitting = store.save("C1", "h", "F", "p", green())
    assert (fitting.origin_dx_mm, fitting.origin_dy_mm) == pytest.approx(
        package_origin(placement(0, 0.4, -0.2))
    )
    assert fitting.origin == (fitting.origin_dx_mm, fitting.origin_dy_mm)

    refused = store.save("C1", "h", "F", "p", green(status="red", rotation=None))
    assert (refused.origin_dx_mm, refused.origin_dy_mm, refused.origin) == (
        None,
        None,
        None,
    )


def test_an_older_table_gains_the_two_origin_columns(tmp_path):
    """A table without the two origin columns gains them in place and keeps its rows."""
    path = tmp_path / "project.db"
    older = SCHEMA.replace("  origin_dx_mm      REAL,\n", "").replace(
        "  origin_dy_mm      REAL,\n", ""
    )
    with closing(sqlite3.connect(path)) as con, con:
        con.executescript(older)
        con.execute(
            "INSERT INTO footprint_verdict (lcsc, footprint_hash, status, rotation,"
            " override_rotation) VALUES ('C1', 'h', 'green', 90, 180)"
        )

    store = VerdictStore(str(path))

    kept = store.get("C1", "h")
    assert (kept.rotation, kept.override_rotation, kept.origin) == (90, 180, None)
    saved = store.save("C1", "h", "F", "p", green())
    assert saved.origin is not None
    assert saved.override_rotation == 180


def test_marking_a_row_pending_keeps_the_columns_but_reports_no_origin(store):
    """The columns survive a re-fetch, and the pending status keeps them out of the CPL."""
    store.save("C1", "h", "F", "p", green())

    store.mark_pending("C1", "h", "F")

    pending = store.get("C1", "h")
    assert (pending.origin_dx_mm, pending.origin_dy_mm) == pytest.approx(
        package_origin(placement(0, 0.4, -0.2))
    )
    assert pending.origin is None


def test_an_override_keeps_the_origin_because_it_only_changes_the_rotation(store):
    """Setting and clearing an override leaves the two columns exactly as they were."""
    saved = store.save("C1", "h", "F", "p", green())

    store.set_override("C1", "h", 270, "by hand")
    overridden = store.get("C1", "h")
    store.set_override("C1", "h", None)

    assert overridden.origin == saved.origin
    assert overridden.emitted_rotation == 270
    assert store.get("C1", "h").origin == saved.origin
