"""What every backend reports about the open board and its footprints."""

from collections.abc import Callable, Sequence
from types import SimpleNamespace

import pytest

from .support import Board, PadSpec, Part

MakeBoard = Callable[[Sequence[Part]], Board]


def _fields(record: object) -> list[tuple[str, str]]:
    """Return a record's Default fields as (name, text), in order."""
    return [
        (field.name, field.text)
        for field in record.default.fields.values()  # type: ignore[attr-defined]
    ]


def test_a_snapshot_names_its_board(board: MakeBoard) -> None:
    """The board's file, and a token that stays the same while it is open."""
    backend = board([Part("R1")]).backend
    info = backend.snapshot().info

    assert info.file_name.endswith("board.kicad_pcb")
    assert info.board_token
    assert backend.snapshot().info == info


def test_default_fields_and_flags_read_back(board: MakeBoard) -> None:
    """Fields keep KiCad's order; the three flags are independent.

    Each KiCad gives a footprint its own built-in fields, and KiCad 7 has no
    DNP flag, so a part marked DNP reads as fitted there.
    """
    parts = [
        Part("R1", fields=(("LCSC", "C25804"), ("MPN", "RC0603"))),
        Part("R2", dnp=True),
        Part("R3", exclude_from_bom=True),
        Part("R4", exclude_from_pos=True),
    ]
    made = board(parts)
    snapshot = made.backend.snapshot()

    assert [variant.name for variant in snapshot.variants] == [""]
    first = snapshot.components[0]
    assert _fields(first) == [
        ("Reference", "R1"),
        ("Value", "10k"),
        *((name, "") for name in made.builtin_fields[2:]),
        ("LCSC", "C25804"),
        ("MPN", "RC0603"),
    ]
    assert [
        (
            record.default.dnp,
            record.default.exclude_from_bom,
            record.default.exclude_from_pos,
        )
        for record in snapshot.components
    ] == [
        (False, False, False),
        (made.holds_dnp, False, False),
        (False, True, False),
        (False, False, True),
    ]
    assert first.footprint_lib_id == "Lib:R_0603"
    assert first.schematic_path == ""


def test_side_and_pads_read_back(board: MakeBoard) -> None:
    """The side, and each pad's kind and whether it has a hole."""
    pads = (PadSpec("1", "smd"), PadSpec("2", "pth"), PadSpec("", "npth"))
    snapshot = board(
        [Part("J1", pads=pads), Part("U1", side="bottom")]
    ).backend.snapshot()
    connector, chip = snapshot.components

    assert (connector.side, chip.side) == ("top", "bottom")
    assert [(pad.kind, pad.has_hole) for pad in connector.pads] == [
        ("smd", False),
        ("pth", True),
        ("npth", True),
    ]
    assert chip.pads == ()


def test_placement_pads_and_courtyard_read_back_on_the_board(board: MakeBoard) -> None:
    """Where KiCad has a turned footprint, its pads and its courtyard on the board.

    J1's anchor is at (10, 20) mm and it is turned 90 degrees, which on KiCad's
    Y-down canvas takes the footprint's +X to the board's -Y: pad 1 at (1, 0)
    lands at (10, 19), pad 2 at (0, 0.5) at (10.5, 20).
    """
    pads = (
        PadSpec("1", "smd", at=(1.0, 0.0), size=(1.0, 0.5), pin_function="A"),
        PadSpec("2", "pth", at=(0.0, 0.5), size=(1.2, 1.2), angle=30.0),
    )
    part = Part(
        "J1", pads=pads, at=(10.0, 20.0), angle=90.0, courtyard=(-2.0, -1.0, 2.0, 1.0)
    )
    (connector,) = board([part]).backend.snapshot().components
    mm = 1_000_000

    assert connector.position_nm == (10 * mm, 20 * mm)
    assert connector.orientation_deg == 90.0
    assert [
        (
            pad.number,
            pad.position_nm,
            pad.size_nm,
            round(pad.orientation_deg, 9),
            pad.shape,
            pad.pin_function,
            pad.on_copper,
        )
        for pad in connector.pads
    ] == [
        ("1", (10 * mm, 19 * mm), (mm, mm // 2), 90.0, "rect", "A", True),
        ("2", (10_500_000, 20 * mm), (1_200_000, 1_200_000), 120.0, "rect", "", True),
    ]
    # The rectangle's 4 x 2 mm turns to 2 x 4 mm about (10, 20); the box
    # holds half the 0.05 mm stroke on every side.
    assert [(shape.bbox_nm, shape.stroke_nm) for shape in connector.courtyard] == [
        ((8_975_000, 17_975_000, 11_025_000, 22_025_000), 50_000)
    ]


def test_references_may_repeat(board: MakeBoard) -> None:
    """Footprints sharing a reference stay distinct, in board order, by UUID."""
    snapshot = board(
        [Part("R1", value="10k"), Part("R1", value="22k")]
    ).backend.snapshot()

    first, second = snapshot.by_reference("R1")
    assert (
        first.default.fields["Value"].text,
        second.default.fields["Value"].text,
    ) == ("10k", "22k")
    assert first.uuid != second.uuid
    assert snapshot.get(second.uuid) is second
    assert snapshot.components == (first, second)


def test_a_repeated_field_name_reads_as_kicad_reopens_the_board(
    board: MakeBoard,
) -> None:
    """The later field's text, in the first one's place; the earlier one is kept.

    KiCad's footprint dialog accepts a second field with a name the
    footprint already has, and keeps both until the board is reopened.
    KiCad 7 keeps fields in a property map, where the second text simply
    replaces the first.
    """
    made = board(
        [
            Part("R1", fields=(("LCSC", "C1"), ("MPN", "X"), ("LCSC", "C2"))),
            Part("R2", fields=(("LCSC", "C2"), ("MPN", "X"))),
        ]
    )
    snapshot = made.backend.snapshot()

    repeated, plain = snapshot.components
    kept = made.keeps_repeated_fields
    assert _fields(repeated)[len(made.builtin_fields) :] == [
        ("LCSC", "C2"),
        ("MPN", "X"),
    ]
    assert _fields(repeated)[1:] == _fields(plain)[1:]
    assert [(field.name, field.text) for field in repeated.default.replaced_fields] == (
        [("LCSC", "C1")] if kept else []
    )
    assert repeated.default.duplicate_names == (("LCSC",) if kept else ())
    assert plain.default.duplicate_names == ()


def test_footprints_sharing_a_uuid_are_read_apart(board: MakeBoard) -> None:
    """Both keep the UUID they share; their keys tell them apart in board order.

    KiCad gives every footprint its own UUID, but a board file can hold two
    with one, and KiCad keeps both when it opens the file, in its own order.
    """
    snapshot = board(
        [Part("R1", value="10k"), Part("C1"), Part("R2", value="22k", uuid_of="R1")]
    ).backend.snapshot()

    first, second = (c for c in snapshot.components if c.reference in {"R1", "R2"})
    assert first.uuid == second.uuid
    assert (first.key, second.key) == (first.uuid, f"{first.uuid}#2")
    assert snapshot.get(first.key) is first
    assert snapshot.get(second.key) is second
    assert {
        record.reference: record.default.fields["Value"].text
        for record in (first, second)
    } == {"R1": "10k", "R2": "22k"}


def test_a_closed_board_raises_board_changed(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Once the editor has no board, a read returns none."""
    made = board([Part("R1")])
    made.close()

    with pytest.raises(backend_modules.api.BoardChanged):
        made.backend.snapshot()
