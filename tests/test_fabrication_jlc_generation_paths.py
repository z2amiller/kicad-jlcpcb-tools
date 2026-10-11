"""The JLC footprint check's decisions on both of the plugin's generation paths.

Every CPL row is built in ``Fabrication._cpl_row``, which the ordinary
generation (``begin_ordinary_generation``, through ``prepare_cpl``) and the variant
generation (``begin_generation``, reached through the variant controller) both
call, so the check's decisions enter there for both.  The variant tests pin the
path itself: each step must hand ``decisions`` on, and without them the rows stay
upstream's.  What the check reads on a variant board is
``test_jlc_footprint_check_variants.py``'s (spec 18.2), and a variant generation
with the check's own decisions, end to end, is
``test_jlc_footprint_check_variant_generation.py``'s (spec 18.4).
"""

from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from jlcfootprint.report import raw_angle_reason
from tests.fabrication_test_support import (
    Point,
    make_footprint,
    modules as fabrication_modules,
)
from tests.jlc_footprint_wx_support import cpl_decision
from tests.variant_model_test_support import Snapshot, State
from tests.wx_harness import ROOT, load_siblings, module, wx_stubs

modules = fabrication_modules


@pytest.fixture
def ordinary(modules: SimpleNamespace, tmp_path: Path) -> SimpleNamespace:
    """Build the ordinary exporter over two parts that share one LCSC rule."""
    parts = [
        {
            "reference": reference,
            "value": "10k",
            "footprint": "Package:Device",
            "lcsc": "C111",
            "exclude_from_bom": 0,
            "exclude_from_pos": 0,
            "is_dnp": False,
            "stock": 20,
        }
        for reference in ("R1", "R2")
    ]
    footprints = [
        make_footprint(part["reference"], 0, 0, Point(10, 20)) for part in parts
    ]
    board = SimpleNamespace(
        GetFileName=lambda: str(tmp_path / "board.kicad_pcb"),
        Footprints=lambda: footprints,
        GetDesignSettings=lambda: SimpleNamespace(GetAuxOrigin=lambda: Point(1, 2)),
    )
    store = SimpleNamespace(
        read_all=lambda: [dict(part) for part in parts],
        get_part=lambda reference: next(
            (dict(part) for part in parts if part["reference"] == reference), None
        ),
        read_bom_parts=lambda captured=None: [
            {**part, "refs": part["reference"]} for part in captured or parts
        ],
    )
    parent = SimpleNamespace(settings={}, store=store)
    return SimpleNamespace(
        exporter=modules.fabrication.Fabrication(parent, board),
        corrections=(modules.data.LcscCorrection("C111", 90, (0, 0)),),
        parts=parts,
    )


def test_the_ordinary_generation_hands_the_decisions_to_its_rows(ordinary):
    """``begin_ordinary_generation`` passes them on: the check's angle, the rule only noted."""
    exporter = ordinary.exporter
    decisions = {
        "R1": cpl_decision(180, lcsc="C111"),
        "R2": cpl_decision(None, source="raw", status="red", lcsc="C111"),
    }

    output = exporter.begin_ordinary_generation(
        ordinary.corrections, decisions=decisions
    )
    try:
        rows = [(row[0], row[5]) for row in output.cpl_rows]
        report = {row.reference: row for row in exporter.rotation_report}
    finally:
        exporter.end_ordinary_generation()

    assert rows == [("R1", 180.0), ("R2", 0.0)]
    assert (report["R1"].source, report["R1"].legacy_correction) == ("derived", 90)
    assert (report["R2"].source, report["R2"].status) == ("raw", "red")


def test_the_ordinary_generation_asks_about_a_refused_part_but_not_an_override(
    ordinary,
):
    """A red part emitted raw is named for the prompt; a red part under an override is not."""
    exporter = ordinary.exporter
    decisions = {
        "R1": cpl_decision(270, source="override", status="red", lcsc="C111"),
        "R2": cpl_decision(None, source="raw", status="red", lcsc="C111"),
    }

    exporter.begin_ordinary_generation(ordinary.corrections, decisions=decisions)
    try:
        report = {row.reference: row for row in exporter.rotation_report}
    finally:
        exporter.end_ordinary_generation()

    assert raw_angle_reason(report["R1"]) is None
    assert raw_angle_reason(report["R2"]) == "does not fit"


@pytest.mark.parametrize(("flag", "value"), [("exclude_from_pos", 1), ("is_dnp", True)])
def test_a_refused_part_the_ordinary_cpl_leaves_out_is_never_asked_about(
    ordinary, flag, value
):
    """An excluded or DNP part gets no CPL row, so it has no report row to name."""
    ordinary.parts[1][flag] = value
    exporter = ordinary.exporter
    refused = {"source": "raw", "status": "red", "lcsc": "C111"}
    decisions = {
        "R1": cpl_decision(None, **refused),
        "R2": cpl_decision(None, **refused),
    }

    exporter.begin_ordinary_generation(ordinary.corrections, decisions=decisions)
    try:
        report = list(exporter.rotation_report)
    finally:
        exporter.end_ordinary_generation()

    assert [row.reference for row in report if raw_angle_reason(row)] == ["R1"]


def test_the_ordinary_generation_without_decisions_is_upstream_s(ordinary):
    """With the check off the C111 rule turns both parts and no report row is made."""
    output = ordinary.exporter.begin_ordinary_generation(ordinary.corrections)
    ordinary.exporter.end_ordinary_generation()

    assert [(row[0], row[5]) for row in output.cpl_rows] == [("R1", 90), ("R2", 90)]
    assert ordinary.exporter.rotation_report == []


@pytest.fixture
def variant(modules: SimpleNamespace, tmp_path: Path) -> SimpleNamespace:
    """Build the variant exporter over one footprint, R1 at (10, 20)."""
    footprints = [make_footprint("R1", 0, 0, Point(10, 20))]
    board = SimpleNamespace(
        GetFileName=lambda: str(tmp_path / "board.kicad_pcb"),
        Footprints=lambda: footprints,
        GetDesignSettings=lambda: SimpleNamespace(GetAuxOrigin=lambda: Point(1, 2)),
    )
    parent = SimpleNamespace(settings={}, store=MagicMock(), library=MagicMock())
    return SimpleNamespace(
        exporter=modules.fabrication.Fabrication(parent, board),
        parent=parent,
        modules=modules,
    )


def _variant_rows(
    variant: SimpleNamespace,
    decisions: Any,
    *,
    lcsc: str = "C999",
    corrections: tuple = (),
    **changes: Any,
) -> tuple[Any, dict[str, Any]]:
    """Run the variant generation of output "A" and return its rows and report."""
    state = State(
        "R1",
        "R1",
        "A",
        value="Variant device",
        lcsc=lcsc,
        footprint="Package:Device",
        **changes,
    )
    snapshot = Snapshot(
        tuple(replace(state, variant_name=name) for name in ("", "A", "B"))
    )
    exporter = variant.exporter
    exporter.begin_generation(
        snapshot, "A", corrections, lambda: None, decisions=decisions
    )
    try:
        rows = exporter.output_snapshot.cpl_rows
        report = {row.reference: row for row in exporter.rotation_report}
    finally:
        exporter.abort_generation()
    return rows, report


def test_a_variant_row_takes_the_decision_for_its_own_part(variant):
    """The decision for the part the variant orders sets the angle and its report row."""
    rows, report = _variant_rows(variant, {"R1": cpl_decision(90, lcsc="C999")})

    assert [(row[0], row[5]) for row in rows] == [("R1", 90.0)]
    assert (report["R1"].lcsc, report["R1"].source, report["R1"].status) == (
        "C999",
        "derived",
        "green",
    )


def test_a_variant_ordering_another_part_keeps_the_raw_angle(variant):
    """The LCSC-mismatch guard holds on the variant path: raw angle, noted."""
    rows, report = _variant_rows(variant, {"R1": cpl_decision(90, lcsc="C123")})

    assert [(row[0], row[5]) for row in rows] == [("R1", 0.0)]
    assert (report["R1"].status, report["R1"].source) == ("lcsc-mismatch", "raw")
    assert "the check saw C123 but the project has C999" in report["R1"].note


@pytest.mark.parametrize("source", ["override", "derived"])
def test_a_variant_ordering_another_part_is_asked_about_even_under_an_override(
    variant, source
):
    """The old part's override or rotation is not this part's: emitted raw, named for the prompt."""
    rows, report = _variant_rows(
        variant, {"R1": cpl_decision(90, source=source, lcsc="C123")}
    )

    assert [(row[0], row[5]) for row in rows] == [("R1", 0.0)]
    assert raw_angle_reason(report["R1"]) == "part number changed since it was checked"
    _rows, own = _variant_rows(
        variant,
        {"R1": cpl_decision(90, source="override", status="red", lcsc="C999")},
    )
    assert raw_angle_reason(own["R1"]) is None


@pytest.mark.parametrize(
    "left_out", [{"pos": False}, {"pop": False}], ids=["pos", "pop"]
)
def test_a_refused_part_the_variant_cpl_leaves_out_is_never_asked_about(
    variant, left_out
):
    """A variant part outside the CPL gets no report row, so it has none to name."""
    refused = cpl_decision(None, source="raw", status="red", lcsc="C999")
    _rows, placed = _variant_rows(variant, {"R1": refused})
    rows, report = _variant_rows(variant, {"R1": refused}, **left_out)

    assert raw_angle_reason(placed["R1"]) == "does not fit"
    assert (rows, report) == ((), {})


def test_without_decisions_the_variant_rows_are_upstream_s(variant):
    """No decisions: the variant's shared rule turns the part and no report is made."""
    rule = (variant.modules.data.Correction("^Variant device$", 180, (0, 0)),)

    rows, report = _variant_rows(variant, None, corrections=rule)
    checked, _ = _variant_rows(
        variant, {"R1": cpl_decision(90, lcsc="C999")}, corrections=rule
    )

    assert [(row[0], row[5]) for row in rows] == [("R1", 180.0)]
    assert report == {}
    assert [(row[0], row[5]) for row in checked] == [("R1", 90.0)]


def test_the_exact_origin_setting_reaches_the_variant_path(variant):
    """``jlcfootprint.exact_origin`` moves a checked variant row to JLC's origin."""
    decision = {"R1": cpl_decision(0, origin=(0.5, 0.25), lcsc="C999")}
    variant.parent.settings = {"jlcfootprint": {"exact_origin": True}}
    rows, report = _variant_rows(variant, decision)
    variant.parent.settings = {}
    plain, _ = _variant_rows(variant, decision)

    assert report["R1"].position_source == "origin"
    assert rows[0][3:5] != plain[0][3:5]


def test_each_variant_generation_starts_a_fresh_report(variant):
    """A second generation in one session reports its own rows, not both runs'."""
    decision = {"R1": cpl_decision(90, lcsc="C999")}
    _variant_rows(variant, decision)
    _variant_rows(variant, decision)

    assert [row.reference for row in variant.exporter.rotation_report] == ["R1"]


def _load_variant_controller(package: str) -> Any:
    """Load the real ``variant/controller.py`` with every import it names stubbed.

    The controller needs real wx and the matrix view to import; its
    ``begin_generation`` needs neither, so each name the module imports from the
    plugin becomes a MagicMock and only the method under test runs for real.
    """
    source = (ROOT / "variant" / "controller.py").read_text(encoding="utf-8")
    stubs = wx_stubs()
    parent = module(f"{package}.variant")
    parent.__path__ = [str(ROOT / "variant")]
    stubs[f"{package}.variant"] = parent
    for node in ast.parse(source).body:
        if isinstance(node, ast.ImportFrom) and node.level:
            base = [package] if node.level == 2 else [package, "variant"]
            parts = [*base, *node.module.split(".")]
            # Stub the packages in between too, so no plugin __init__ runs.
            for depth in range(len(base) + 1, len(parts)):
                between = ".".join(parts[:depth])
                if between not in stubs:
                    stubs[between] = module(between)
                    stubs[between].__path__ = []
            name = ".".join(parts)
            stubs[name] = module(
                name, **{alias.name: MagicMock() for alias in node.names}
            )
    return load_siblings(package, ("variant.controller",), stubs)


def test_the_variant_controller_hands_the_decisions_to_the_exporter():
    """``VariantMainController.begin_generation`` forwards the keyword unchanged."""
    session = MagicMock()
    session.begin_generation.return_value = ("snapshot", "A")
    controller = SimpleNamespace(
        session=session, _publish_output=MagicMock(), dialog=MagicMock()
    )
    decisions = {"R1": cpl_decision(90)}

    with _load_variant_controller("jlc_variant_controller_tests") as loaded:
        loaded["variant.controller"].VariantMainController.begin_generation(
            controller, ("rule",), decisions=decisions
        )

    controller.dialog.fabrication.begin_generation.assert_called_once_with(
        "snapshot", "A", ("rule",), session.validate_generation, decisions=decisions
    )
