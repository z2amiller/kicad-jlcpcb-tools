"""The CPL path with ``jlcfootprint.exact_origin`` on: JLC's package origin (spec 17.4)."""

from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.correction_test_support import make_library
from tests.test_fabrication_correction_recovery import Point, read_cpl
from tests.wx_harness import load_correction_modules, module

# The origin every part in these tests carries, in its own footprint frame.
ORIGIN = (0.5, 0.25)
# Where each footprint sits and what the board subtracts, so the base position is (9, 18).
PLACED_AT = Point(10, 20)
AUX_ORIGIN = Point(1, 2)


class Box:
    """A pcbnew bounding box with just the two calls ``get_position`` makes."""

    def __init__(self, left: float, top: float, right: float, bottom: float) -> None:
        self.left, self.top, self.right, self.bottom = left, top, right, bottom

    def Merge(self, other: "Box") -> None:
        """Grow this box to hold another."""
        self.left = min(self.left, other.left)
        self.top = min(self.top, other.top)
        self.right = max(self.right, other.right)
        self.bottom = max(self.bottom, other.bottom)

    def GetCenter(self) -> Point:
        """Return the box's centre."""
        return Point((self.left + self.right) / 2, (self.top + self.bottom) / 2)


def pad(centre: Point, half: float = 0.5) -> SimpleNamespace:
    """Return a pad double whose bounding box is a square around ``centre``."""
    return SimpleNamespace(
        GetBoundingBox=lambda: Box(
            centre.x - half, centre.y - half, centre.x + half, centre.y + half
        )
    )


def footprint(
    reference: str,
    layer: int,
    rotation: float,
    position: Point = PLACED_AT,
    pads: list | None = None,
) -> SimpleNamespace:
    """Return the board data the placement code reads for one part."""
    return SimpleNamespace(
        GetReference=lambda: reference,
        GetValue=lambda: "Device",
        GetLayer=lambda: layer,
        GetOrientation=lambda: SimpleNamespace(AsDegrees=lambda: float(rotation)),
        GetFPID=lambda: SimpleNamespace(GetLibItemName=lambda: "Package:Device"),
        Pads=lambda: list(pads or []),
        GetPosition=lambda: position,
    )


def decision(
    origin: tuple | None = ORIGIN,
    rotation: int | None = 0,
    source: str = "derived",
    status: str = "green",
    lcsc: str = "C123",
) -> SimpleNamespace:
    """Return what the controller's decision carries for one reference."""
    return SimpleNamespace(
        rotation=rotation,
        source=source,
        status=status,
        polarity_light=None,
        fit="fits",
        note="",
        pending=False,
        lcsc=lcsc,
        body_excess=None,
        origin=origin,
    )


@pytest.fixture
def modules() -> Iterator[SimpleNamespace]:
    """Keep real storage and placement modules registered with one set of doubles."""
    package = "fabrication_jlc_origin_tests"
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


def fabrication(modules, library, tmp_path, footprints, exact_origin=True):
    """Return a real generator over ``footprints`` with the setting as asked."""
    board = SimpleNamespace(
        GetFileName=lambda: str(tmp_path / "board.kicad_pcb"),
        GetDesignSettings=MagicMock(
            return_value=SimpleNamespace(GetAuxOrigin=lambda: AUX_ORIGIN)
        ),
        Footprints=MagicMock(return_value=list(footprints)),
    )
    parts = {
        str(fp.GetReference()): {
            "reference": str(fp.GetReference()),
            "value": "Device",
            "footprint": "Package:Device",
            "exclude_from_pos": 0,
            "lcsc": "C123",
        }
        for fp in footprints
    }
    parent = SimpleNamespace(
        library=library,
        settings={"jlcfootprint": {"exact_origin": exact_origin}},
        store=SimpleNamespace(get_part=parts.get),
    )
    return modules.fabrication.Fabrication(parent, board)


def positions(rows) -> dict:
    """Return ``{reference: (Mid X, Mid Y)}`` from prepared CPL rows."""
    return {row[0]: (row[3], row[4]) for row in rows}


# The turned and mirrored origin at every angle, worked by hand from reposition's
# transform: the footprint frame is un-mirrored, so the bottom negates dy first.
EXPECTED = {
    ("top", 0): ("9.500000", "-18.250000"),
    ("top", 90): ("9.250000", "-17.500000"),
    ("top", 180): ("8.500000", "-17.750000"),
    ("top", 270): ("8.750000", "-18.500000"),
    ("bottom", 0): ("9.500000", "-17.750000"),
    ("bottom", 90): ("8.750000", "-17.500000"),
    ("bottom", 180): ("8.500000", "-18.250000"),
    ("bottom", 270): ("9.250000", "-18.500000"),
}


@pytest.mark.parametrize("angle", [0, 90, 180, 270])
@pytest.mark.parametrize("side", ["top", "bottom"])
def test_the_origin_is_turned_by_the_placement_and_mirrored_on_the_bottom(
    modules, library, tmp_path, side, angle
):
    """Every snapped angle on both sides moves the position the way reposition does."""
    layer = 0 if side == "top" else 31
    generator = fabrication(
        modules, library, tmp_path, [footprint("U1", layer, angle)]
    )

    rows = generator.prepare_cpl(None, {"U1": decision()})

    assert positions(rows)["U1"] == EXPECTED[(side, angle)]
    assert rows[0][6] == side
    assert generator.rotation_report[0].position_source == "origin"


def test_the_setting_off_leaves_every_position_on_the_pad_box_centre(
    modules, library, tmp_path
):
    """With the setting off the CPL is exactly M3's, and every row says pad-box."""
    parts = [footprint("U1", 0, 0), footprint("U2", 31, 90)]
    generator = fabrication(modules, library, tmp_path, parts, exact_origin=False)

    rows = generator.prepare_cpl(None, {"U1": decision(), "U2": decision()})

    assert positions(rows) == {
        "U1": ("9.000000", "-18.000000"),
        "U2": ("9.000000", "-18.000000"),
    }
    assert {row.position_source for row in generator.rotation_report} == {"pad-box"}


def test_a_part_without_an_origin_keeps_upstream_s_pad_box_centre(
    modules, library, tmp_path
):
    """Red, unknown and pending parts carry no origin, so the setting cannot move them."""
    parts = [footprint("U1", 0, 0), footprint("U2", 0, 0)]
    generator = fabrication(modules, library, tmp_path, parts)

    rows = generator.prepare_cpl(
        None,
        {
            "U1": decision(origin=None, rotation=None, source="raw", status="red"),
            "U2": decision(),
        },
    )

    assert positions(rows)["U1"] == ("9.000000", "-18.000000")
    assert positions(rows)["U2"] == ("9.500000", "-18.250000")
    report = {row.reference: row for row in generator.rotation_report}
    assert report["U1"].position_source == "pad-box"
    assert report["U2"].position_source == "origin"


def test_a_part_the_check_judged_under_another_lcsc_keeps_the_pad_box_centre(
    modules, library, tmp_path
):
    """An LCSC mismatch is not a part the check knows, so neither value may be used."""
    generator = fabrication(modules, library, tmp_path, [footprint("U1", 0, 0)])

    rows = generator.prepare_cpl(None, {"U1": decision(rotation=180, lcsc="C999")})

    assert positions(rows)["U1"] == ("9.000000", "-18.000000")
    report = generator.rotation_report[0]
    assert (report.position_source, report.status, report.emitted) == (
        "pad-box",
        "lcsc-mismatch",
        0.0,
    )
    assert "the check saw C999 but the project has C123" in report.note


def test_a_part_whose_origin_is_its_pad_box_centre_emits_a_byte_equal_cpl(
    modules, library, tmp_path
):
    """Spec 17.4: rounding is upstream's, so such a board's file does not change."""
    # The pad box is the origin's own place on the board, so both paths agree.
    centred = [
        footprint(
            "U1",
            0,
            0,
            pads=[pad(Point(10.5, 20.25)), pad(Point(10.5, 20.25))],
        )
    ]
    decisions = {"U1": decision()}

    off = fabrication(modules, library, tmp_path, centred, exact_origin=False)
    off.write_cpl(off.prepare_cpl(None, decisions))
    without = Path(off.get_cpl_csv_path()).read_bytes()
    on = fabrication(modules, library, tmp_path, centred)
    on.write_cpl(on.prepare_cpl(None, decisions))

    assert Path(on.get_cpl_csv_path()).read_bytes() == without
    assert [line["Mid X"] for line in read_cpl(on)] == ["9.500000"]
    assert on.rotation_report[0].position_source == "origin"
    assert off.rotation_report[0].position_source == "pad-box"


def test_the_legacy_path_never_reads_the_setting(modules, library, tmp_path):
    """Without decisions there are no verdicts, so the correction rules run as before."""
    generator = fabrication(modules, library, tmp_path, [footprint("U1", 0, 0)])

    rows = generator.prepare_cpl(())

    assert positions(rows)["U1"] == ("9.000000", "-18.000000")
    assert generator.rotation_report == []
