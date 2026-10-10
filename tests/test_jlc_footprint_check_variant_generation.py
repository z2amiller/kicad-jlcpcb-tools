"""A variant board's generation, end to end with the check on (spec 18.4).

The check reads the output variant through upstream's session, the window hands
its decisions to upstream's variant ``begin_generation``, every CPL row takes the
decision for the part that variant orders (its rotation and, with the setting on,
JLC's package origin), and the rotation summary that follows is built from the
same rows.  The facade and the exporter run for real; only the board and the
window are doubles.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from jlcfootprint.report import format_summary, summarise

from .fabrication_test_support import Point, make_footprint, modules
from .jlc_footprint_wx_support import facade_symbols, load_facade, load_window
from .jlcfootprint_support import recorded
from .test_jlc_footprint_check_generate import _generate_window
from .test_jlc_footprint_check_variants import _footprint
from .test_jlcfootprint_kicad_adapter import FakePad
from .variant_native_support import Snapshot, State

__all__ = ["modules"]
_PACKAGE = "jlc_footprint_check_variant_generation_tests"

# R1's part per variant: Default and A order C2132 (green at 180 on these SOT-23
# pads, recorded), B orders C9 (unknown to the cache: still pending).
NUMBERS = {"": "C2132", "A": "C2132", "B": "C9"}


@pytest.fixture
def facade():
    """Load the real facade with a fake wx."""
    with load_facade(f"{_PACKAGE}_facade") as loaded:
        yield loaded.module


def _snapshot():
    """Return R1 in Default, A and B with the numbers above."""
    return Snapshot(
        tuple(
            State("id-R1", "R1", name, lcsc=lcsc, footprint="Package:Device")
            for name, lcsc in NUMBERS.items()
        ),
        tuple(NUMBERS),
    )


def _check(facade, tmp_path, session):
    """Build the window's check over SOT-23 pads, the recorded C2132 cached."""
    footprint = _footprint("R1", "id-R1")
    footprint.pads = [
        FakePad("1", -0.9375, -0.95, 1.475, 0.6),
        FakePad("2", -0.9375, 0.95, 1.475, 0.6),
        FakePad("3", 0.9375, 0.0, 1.475, 0.6),
    ]
    board = SimpleNamespace(GetFootprints=lambda: [footprint])
    pcbnew = SimpleNamespace(
        GetBoard=lambda: board, ToMM=lambda v: v, PAD_ATTRIB_NPTH=3, PAD_SHAPE_CUSTOM=6
    )
    window = SimpleNamespace(
        library=SimpleNamespace(datadir=str(tmp_path)),
        store=SimpleNamespace(dbfile=str(tmp_path / "project.db")),
        settings={},
        _variant_controller=SimpleNamespace(session=session),
    )
    check = facade.create_footprint_check(window, pcbnew)
    check.cache.store(recorded("C2132"), now=1)
    return check


def _generate(modules, tmp_path, snapshot, output, decisions, settings=None):
    """Run upstream's variant generation of one output; return its rows and report."""
    board = SimpleNamespace(
        GetFileName=lambda: str(tmp_path / "board.kicad_pcb"),
        Footprints=lambda: [make_footprint("R1", 0, 0, Point(10, 20))],
        GetDesignSettings=lambda: SimpleNamespace(GetAuxOrigin=lambda: Point(1, 2)),
    )
    parent = SimpleNamespace(
        settings=settings or {}, store=MagicMock(), library=MagicMock()
    )
    exporter = modules.fabrication.Fabrication(parent, board)
    exporter.begin_generation(snapshot, output, (), lambda: None, decisions=decisions)
    try:
        return exporter.output_snapshot.cpl_rows, list(exporter.rotation_report)
    finally:
        exporter.abort_generation()


def test_each_output_variant_generates_with_the_check_s_decisions_for_its_part(
    facade, modules, tmp_path
):
    """A orders C2132: turned 180°.  B orders C9, still pending: the raw angle."""
    snapshot = _snapshot()
    session = SimpleNamespace(snapshot=snapshot, output_variant="A")
    check = _check(facade, tmp_path, session)
    check.scan_board()
    rows, report = _generate(modules, tmp_path, snapshot, "A", check.decisions())
    assert [(row[0], row[5]) for row in rows] == [("R1", 180.0)]
    (row,) = report
    assert (row.lcsc, row.source, row.status, row.correction) == (
        "C2132",
        "derived",
        "green",
        180,
    )
    text = format_summary(summarise(report, True, False))
    assert "Applied rotations (1)" in text and "R1" in text

    session.output_variant = "B"
    check.scan_board()
    rows, report = _generate(modules, tmp_path, snapshot, "B", check.decisions())
    assert [(row[0], row[5]) for row in rows] == [("R1", 0.0)]
    (row,) = report
    assert (row.lcsc, row.source, row.pending) == ("C9", "raw", True)
    assert "Unresolved, raw angle emitted (1)" in format_summary(summarise(report))


def test_the_package_origin_reaches_a_variant_cpl_from_the_check(
    facade, modules, tmp_path
):
    """With the setting on, the variant row sits at JLC's package origin (spec 17.4)."""
    snapshot = _snapshot()
    session = SimpleNamespace(snapshot=snapshot, output_variant="A")
    check = _check(facade, tmp_path, session)
    check.scan_board()
    decisions = check.decisions()
    assert decisions["R1"].origin is not None
    placed, report = _generate(
        modules,
        tmp_path,
        snapshot,
        "A",
        decisions,
        {"jlcfootprint": {"exact_origin": True}},
    )
    plain, _report = _generate(modules, tmp_path, snapshot, "A", decisions)
    assert report[0].position_source == "origin"
    assert placed[0][3:5] != plain[0][3:5]


def test_a_variant_generation_shows_the_rotation_summary():
    """The summary dialog follows a variant generation too (spec 18.4)."""
    facade = facade_symbols()
    module = load_window(
        _PACKAGE,
        facade,
        BeginBusyCursor=MagicMock(),
        EndBusyCursor=MagicMock(),
        IsBusy=MagicMock(return_value=True),
        MessageBox=MagicMock(),
        MessageDialog=MagicMock(),
    )
    check = SimpleNamespace(
        decisions=MagicMock(return_value={"R1": "decision"}), scan_board=MagicMock()
    )
    window, _steps = _generate_window(module, check, corrections=("rule",))
    window._variant_controller = MagicMock()
    window.fabrication.generation_publication = MagicMock()

    module.JLCPCBTools.generate_fabrication_data(window)

    check.scan_board.assert_called_once_with()
    facade["show_generate_summary"].assert_called_once_with(
        window, window.fabrication.rotation_report, True
    )
