"""The CPL path with the footprint check's decisions (spec section 8) beside the legacy path."""

from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.correction_test_support import make_library, seed_raw
from tests.test_fabrication_correction_recovery import Point, make_fabrication, read_cpl
from tests.wx_harness import load_correction_modules, module


@pytest.fixture
def modules() -> Iterator[SimpleNamespace]:
    """Keep real storage and placement modules registered with one set of doubles."""
    package = "fabrication_jlc_rotation_tests"
    pcbnew = MagicMock()
    pcbnew.FromMM = lambda value: value
    pcbnew.ToMM = lambda value: value
    pcbnew.wxPoint = Point
    pcbnew.VECTOR2I = Point
    with load_correction_modules(
        package=package,
        pcbnew=pcbnew,
        names=("fabrication",),
        replacements={
            f"{package}.footprint_helpers": module(
                f"{package}.footprint_helpers", get_is_dnp=lambda _footprint: False
            )
        },
    ) as loaded:
        yield loaded


@pytest.fixture
def library(modules, tmp_path):
    """Create actual SQLite correction storage away from user databases."""
    return make_library(modules.library, tmp_path)


def decision(
    rotation,
    source="derived",
    status="green",
    light=None,
    fit="fits",
    note="",
    pending=False,
):
    """Return what the controller's decision carries for one reference."""
    return SimpleNamespace(
        rotation=rotation,
        source=source,
        status=status,
        polarity_light=light,
        fit=fit,
        note=note,
        pending=pending,
    )


def _rule(library, rotation=90, offset=(1.0, 2.0)):
    """Persist one correction rule matching both test footprints by value."""
    seed_raw(library, [("Device", rotation, offset[0], offset[1])])
    return library.read_correction_data().corrections


def test_decisions_set_the_rotation_and_leave_positions_alone(
    modules, library, tmp_path
):
    """Derived and override values are applied after the bottom mirror; the rule's offset is not."""
    fabrication = make_fabrication(modules, library, tmp_path)
    corrections = _rule(library)
    decisions = {
        "U1": decision(180),
        "U2": decision(270, source="override", status="red", note="checked"),
    }
    rows = fabrication.prepare_cpl(corrections, decisions)
    # U1: top, raw 0 + 180.  U2: bottom, placed 90 mirrors to 90, + 270 = 0.  Positions
    # are the bounding-box centres minus the aux origin, with no correction offset.
    assert [(row[0], row[3], row[4], row[5], row[6]) for row in rows] == [
        ("U1", "9.000000", "-18.000000", 180.0, "top"),
        ("U2", "29.000000", "-38.000000", 0.0, "bottom"),
    ]
    fabrication.write_cpl(rows)
    assert [
        (line["Designator"], line["Rotation"]) for line in read_cpl(fabrication)
    ] == [("U1", "180.0"), ("U2", "0.0")]
    report = {row.reference: row for row in fabrication.rotation_report}
    assert (
        report["U1"].raw,
        report["U1"].emitted,
        report["U1"].correction,
        report["U1"].source,
    ) == (0.0, 180.0, 180, "derived")
    assert (
        report["U2"].raw,
        report["U2"].emitted,
        report["U2"].correction,
        report["U2"].source,
    ) == (90.0, 0.0, 270, "override")
    assert report["U2"].note == "checked"
    assert report["U1"].legacy_correction == report["U2"].legacy_correction == 90
    assert fabrication.corrections == corrections


def test_parts_without_a_usable_decision_keep_the_raw_angle(modules, library, tmp_path):
    """Pending, unresolved and unlisted parts emit the mirrored raw angle and appear in the report."""
    fabrication = make_fabrication(modules, library, tmp_path)
    decisions = {"U1": decision(None, source="raw", status="pending", pending=True)}
    rows = fabrication.prepare_cpl(None, decisions)
    assert [(row[0], row[5]) for row in rows] == [("U1", 0.0), ("U2", 90.0)]
    report = {row.reference: row for row in fabrication.rotation_report}
    assert (report["U1"].source, report["U1"].pending, report["U1"].status) == (
        "raw",
        True,
        "pending",
    )
    assert (
        report["U2"].source,
        report["U2"].status,
        report["U2"].legacy_correction,
    ) == ("raw", "no-verdict", None)
    assert fabrication.corrections == ()


def test_missing_rules_do_not_stop_generation_with_decisions(
    modules, library, tmp_path
):
    """Unresolved correction storage is fatal only on the legacy path."""
    fabrication = make_fabrication(modules, library, tmp_path)
    fabrication.parent.library = SimpleNamespace(
        read_correction_data=lambda: SimpleNamespace(
            corrections=None, scope="global", db_path="x"
        )
    )
    with pytest.raises(ValueError, match="unresolved"):
        fabrication.prepare_cpl()
    rows = fabrication.prepare_cpl(None, {"U1": decision(90)})
    assert [(row[0], row[5]) for row in rows] == [("U1", 90.0), ("U2", 90.0)]
    assert [row.legacy_correction for row in fabrication.rotation_report] == [
        None,
        None,
    ]


def test_legacy_path_is_unchanged_without_decisions(modules, library, tmp_path):
    """Without decisions the rule's rotation and offset apply exactly as before."""
    fabrication = make_fabrication(modules, library, tmp_path)
    corrections = _rule(library)
    legacy = fabrication.prepare_cpl(corrections)
    assert legacy == fabrication.prepare_cpl(corrections, None)
    assert [(row[0], row[3], row[4], row[5]) for row in legacy] == [
        ("U1", "10.000000", "-20.000000", 90.0),
        ("U2", "27.000000", "-37.000000", 180.0),
    ]
    assert fabrication.rotation_report == []
    with pytest.raises(TypeError):
        fabrication.prepare_cpl(["not", "a", "tuple"])
