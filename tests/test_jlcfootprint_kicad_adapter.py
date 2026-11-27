"""Tests for the live board reader over kicad_backend records: frames, filters, fields, courtyards."""

from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from jlcfootprint.geometry import Pad, pad_hash, rotate
from jlcfootprint.kicad_adapter import (
    BoardPart,
    board_part,
    board_parts,
    footprint_courtyard,
    footprint_pads,
    is_soldered,
    lcsc_value,
)
from jlcfootprint.verdicts import verdict_key

from .wx_harness import load_siblings

MM = 1_000_000
ANCHOR = (10 * MM, 20 * MM)


@pytest.fixture
def backend() -> Iterator[SimpleNamespace]:
    """Load the backend's records and its in-memory board under a scoped name."""
    with load_siblings(
        "_jlcfootprint_adapter_tests",
        ("kicad_backend", "kicad_backend.records", "kicad_backend.fake"),
        {},
    ) as modules:
        yield SimpleNamespace(
            records=modules["kicad_backend.records"], fake=modules["kicad_backend.fake"]
        )


def placed(local, orientation=0.0, bottom=False, anchor=ANCHOR):
    """Return where KiCad has a footprint-frame point (mm) on the board (nm).

    A bottom-side footprint is mirrored across its X axis first, then the
    footprint's angle turns it and its anchor moves it, as KiCad places it.
    """
    x, y = local
    if bottom:
        y = -y
    bx, by = rotate(x * MM, y * MM, -orientation)
    return round(anchor[0] + bx), round(anchor[1] + by)


def sot23(fake, orientation=0.0, bottom=False, shape="roundrect"):
    """Return a SOT-23's three pads where KiCad has them on the board."""
    return [
        fake.pad(
            number=number,
            position_nm=placed((x, y), orientation, bottom),
            size_nm=(900_000, 800_000),
            orientation_deg=orientation,
            shape=shape,
            pin_function=function,
        )
        for number, x, y, function in (
            ("1", -1.0, 0.95, "B"),
            ("2", -1.0, -0.95, "E"),
            ("3", 1.0, 0.0, "C"),
        )
    ]


def footprint(fake, reference, pads, *, orientation=0.0, side="top", **kw):
    """Return a footprint record anchored at (10, 20) mm with an LCSC field."""
    kw.setdefault("fields", {"LCSC": "C2132"})
    return fake.component(
        reference,
        value="value",
        footprint="Lib:Name",
        side=side,
        position_nm=ANCHOR,
        orientation_deg=orientation,
        pads=pads,
        **kw,
    )


def rounded(pads):
    """Return the pads' numbers, positions and angles rounded past float noise."""
    return [
        (p.number, round(p.x, 9), round(p.y, 9), round(p.rotation, 9) % 360)
        for p in pads
    ]


def test_top_part_reads_pads_in_the_footprint_frame(backend):
    """Positions come back in the footprint frame; sizes, functions and shapes as KiCad has them."""
    fake = backend.fake
    part = board_part(footprint(fake, "Q1", sot23(fake, 90.0), orientation=90.0))
    assert isinstance(part, BoardPart)
    assert (
        part.reference,
        part.lcsc,
        part.footprint_name,
        part.is_bottom,
        part.placed_rotation,
        part.value,
    ) == ("Q1", "C2132", "Lib:Name", False, 90.0, "value")
    assert rounded(part.pads) == [
        ("1", -1.0, 0.95, 0.0),
        ("2", -1.0, -0.95, 0.0),
        ("3", 1.0, 0.0, 0.0),
    ]
    first = part.pads[0]
    assert (first.width, first.height, first.pin_function, first.shape) == (
        0.9,
        0.8,
        "B",
        "roundrect",
    )
    assert part.footprint_hash == verdict_key(part.pads)
    assert part.footprint_hash != pad_hash(part.pads)


def test_placement_is_removed_by_the_footprints_angle(backend):
    """Board offsets are turned back by the footprint's angle, worked by hand.

    A footprint at (10, 20) turned 90 degrees on KiCad's Y-down canvas puts local
    (-1, 0.95) at board (10.95, 21).
    """
    fake = backend.fake
    pads = [
        fake.pad(number=number, position_nm=(round(x * MM), round(y * MM)))
        for number, x, y in (("1", 10.95, 21.0), ("2", 9.05, 21.0), ("3", 10.0, 19.0))
    ]
    part = board_part(footprint(fake, "Q1", pads, orientation=90.0))
    assert [(n, x, y) for n, x, y, _ in rounded(part.pads)] == [
        ("1", -1.0, 0.95),
        ("2", -1.0, -0.95),
        ("3", 1.0, 0.0),
    ]


def test_bottom_part_is_unmirrored_and_shares_the_hash(backend):
    """KiCad stores a flipped footprint's pads mirrored; the reader un-mirrors like the validator."""
    fake = backend.fake
    top = board_part(footprint(fake, "Q1", sot23(fake)))
    bottom = board_part(footprint(fake, "Q2", sot23(fake, bottom=True), side="bottom"))
    assert bottom.is_bottom
    assert bottom.pads == top.pads
    assert bottom.footprint_hash == top.footprint_hash
    turned = board_part(
        footprint(
            fake,
            "Q3",
            sot23(fake, 270.0, bottom=True),
            orientation=270.0,
            side="bottom",
        )
    )
    assert rounded(turned.pads) == rounded(top.pads)


def test_a_turned_part_keys_as_it_does_unturned(backend):
    """A pad on the key's 0.001 mm rounding boundary keys the same at every quarter turn.

    KiCad's SOT-23 puts pads 1 and 2 at x = -0.9375 mm; turned back from 180
    degrees in floating point, one lands a hair either side of the boundary.
    """
    fake = backend.fake
    pads = (("1", -0.9375, 0.95), ("2", -0.9375, -0.95), ("3", 0.9375, 0.0))

    def sot23_at(orientation):
        return board_part(
            footprint(
                fake,
                "Q1",
                [
                    fake.pad(
                        number=number,
                        position_nm=placed((x, y), orientation),
                        orientation_deg=orientation,
                    )
                    for number, x, y in pads
                ],
                orientation=orientation,
            )
        )

    unturned = sot23_at(0.0)
    for orientation in (90.0, 180.0, 270.0, -90.0):
        turned = sot23_at(orientation)
        assert turned.pads == unturned.pads
        assert turned.footprint_hash == unturned.footprint_hash


def test_pads_that_are_not_solder_joints_are_dropped(backend):
    """NPTH holes and paste-only apertures never reach the resolver; a custom shape is named."""
    fake = backend.fake
    pads = [
        *sot23(fake),
        fake.pad("npth", position_nm=ANCHOR, on_copper=False),
        fake.pad("npth", position_nm=ANCHOR),
        fake.pad(position_nm=placed((2.0, 0.0)), on_copper=False),
        fake.pad(number="4", position_nm=placed((3.0, 0.0)), shape="custom"),
    ]
    part = board_part(footprint(fake, "Q1", pads))
    assert [p.number for p in part.pads] == ["1", "2", "3", "4"]
    assert part.pads[3].shape == "custom"
    assert is_soldered(fake.pad("smd"))
    assert is_soldered(fake.pad("pth"))
    assert not is_soldered(fake.pad("npth"))
    assert not is_soldered(fake.pad("smd", on_copper=False))


def test_a_zero_padded_number_reads_as_the_board_file_does(backend):
    """Pad 01 is pad 1, as boardfile and the EasyEDA readers read it."""
    fake = backend.fake
    part = board_part(
        footprint(fake, "J1", [fake.pad(number="01"), fake.pad(number="A1")])
    )
    assert [pad.number for pad in part.pads] == ["1", "A1"]


def test_units_and_angles(backend):
    """Nanometres become millimetres; a pad's angle is its own less the footprint's."""
    fake = backend.fake
    pad = fake.pad(
        number="1",
        position_nm=(1 * MM, 2 * MM),
        size_nm=(500_000, 250_000),
        orientation_deg=90.0,
    )
    part = board_part(
        fake.component("R1", position_nm=(0, 0), pads=[pad], fields={"LCSC": "C1"})
    )
    assert part.pads == [Pad("1", 1.0, 2.0, 0.5, 0.25, 90.0, "", "rect")]
    turned = fake.pad(orientation_deg=135.0)
    part = board_part(fake.component("R2", orientation_deg=45.0, pads=[turned]))
    assert (part.placed_rotation, part.pads[0].rotation) == (45.0, 90.0)
    part = board_part(fake.component("R3", orientation_deg=-90.0, pads=[turned]))
    assert (part.placed_rotation, part.pads[0].rotation) == (270.0, 225.0)


def test_lcsc_value_reads_the_footprints_own_fields(backend):
    """A part-number field holding C and digits counts; anything else is no number."""
    fake = backend.fake
    assert lcsc_value(fake.component("R1", fields={"JLCPCB Part": " C123 "})) == "C123"
    assert lcsc_value(fake.component("R1", fields={"LCSC": "not a code"})) == ""
    assert lcsc_value(fake.component("R1")) == ""


@pytest.mark.parametrize(
    ("fields", "lcsc"),
    [
        ({"JLCPCB Rotation": "C123"}, ""),
        ({"Supplier LCSC": "C123"}, ""),
        ({"LCSC": "C123", "JLCPCB Part": "C456"}, ""),
        ({"LCSC": "C123", "JLCPCB Part": " c123 "}, "C123"),
        ({"LCSC Part #": " c123 "}, "C123"),
    ],
    ids=["other-jlc-field", "other-lcsc-field", "two-numbers", "same-number", "case"],
)
def test_lcsc_value_reads_the_field_as_the_board_file_does(backend, fields, lcsc):
    """A live footprint's fields give the number its board file gives (boardfile.lcsc_assignment)."""
    assert lcsc_value(backend.fake.component("R1", fields=fields)) == lcsc


def test_the_value_is_kicads_own_field(backend):
    """A second field named Value does not replace the footprint's value."""
    fake = backend.fake
    part = board_part(fake.component("R1", value="10k", fields=[("Value", "other")]))
    assert part.value == "10k"


def test_board_parts_reads_the_components_it_is_given_in_order(backend):
    """Every component passed in is read, in order; choosing them is the caller's business."""
    fake = backend.fake
    components = [
        footprint(fake, "R1", [fake.pad(number="1"), fake.pad(number="2")]),
        footprint(fake, "", [fake.pad(number="1")]),
        footprint(fake, "C1", [], fields={}),
    ]
    parts = board_parts(iter(components))
    assert [(p.reference, p.lcsc, len(p.pads)) for p in parts] == [
        ("R1", "C2132", 2),
        ("", "C2132", 1),
        ("C1", "", 0),
    ]
    assert board_parts(components[:1], lcsc_of=lambda _component: "C9")[0].lcsc == "C9"
    assert footprint_pads(components[0])[0].number == "1"


def test_the_verdict_key_includes_the_pin_functions(backend):
    """Swapped K/A functions give different keys; no function gives the pad hash itself."""
    fake = backend.fake

    def diode(reference, first, second):
        return board_part(
            footprint(
                fake,
                reference,
                [
                    fake.pad(
                        number="1", position_nm=placed((-1, 0)), pin_function=first
                    ),
                    fake.pad(
                        number="2", position_nm=placed((1, 0)), pin_function=second
                    ),
                ],
            )
        )

    cathode_first, anode_first, plain = (
        diode("D1", "K", "A"),
        diode("D2", "A", "K"),
        diode("D3", "", ""),
    )
    assert cathode_first.footprint_hash != anode_first.footprint_hash
    assert plain.footprint_hash == pad_hash(plain.pads)
    assert cathode_first.footprint_hash != plain.footprint_hash


def courtyard_line(records, box_mm, stroke_mm=0.05):
    """Return a courtyard shape from its board box in mm, stroke included."""
    return records.CourtyardShape(
        tuple(round(v * MM) for v in box_mm), round(stroke_mm * MM)
    )


def test_footprint_courtyard_shrinks_the_stroke_and_returns_to_the_footprint_frame(
    backend,
):
    """Two courtyard lines at 10, 20 with a 0.05 stroke give the centreline box about the anchor."""
    records = backend.records
    shapes = [
        courtyard_line(records, (8.975, 17.975, 12.025, 18.025)),  # y = 18
        courtyard_line(records, (8.975, 17.975, 9.025, 22.025)),  # x = 9
    ]
    component = footprint(backend.fake, "U1", [], courtyard=shapes)
    assert footprint_courtyard(component) == pytest.approx((-1.0, -2.0, 2.0, 2.0))
    assert board_part(component).courtyard == pytest.approx((-1.0, -2.0, 2.0, 2.0))


def test_footprint_courtyard_unrotates_and_mirrors(backend):
    """A part placed at 90 gives its footprint-frame box; a bottom part is mirrored back."""
    fake, records = backend.fake, backend.records
    turned = footprint(
        fake,
        "U1",
        [],
        orientation=90.0,
        courtyard=[courtyard_line(records, (7.975, 18.975, 12.025, 21.025))],
    )
    assert footprint_courtyard(turned) == pytest.approx((-1.0, -2.0, 1.0, 2.0))
    bottom = footprint(
        fake,
        "U2",
        [],
        side="bottom",
        courtyard=[courtyard_line(records, (8.975, 18.975, 11.025, 23.025))],
    )
    assert footprint_courtyard(bottom) == pytest.approx((-1.0, -3.0, 1.0, 1.0))
    assert footprint_courtyard(footprint(fake, "U3", [])) is None
