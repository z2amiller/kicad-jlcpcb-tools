"""What every backend reports about the open board and its footprints."""

from collections.abc import Callable, Sequence
from types import SimpleNamespace

import pytest

from .support import Board, PadSpec, Part

MakeBoard = Callable[[Sequence[Part]], Board]


def _fields(record: object) -> list[tuple[str, str, bool]]:
    """Return a record's Default fields as (name, text, visible), in order."""
    return [
        (field.name, field.text, field.visible)
        for field in record.default.fields.values()  # type: ignore[attr-defined]
    ]


def test_a_snapshot_names_its_board(board: MakeBoard) -> None:
    """The board's file, and a token that stays the same while it is open.

    ``info`` names the board as a snapshot does, without reading it.
    """
    backend = board([Part("R1")]).backend
    info = backend.snapshot().info

    assert info.file_name.endswith("board.kicad_pcb")
    assert info.board_token
    assert backend.snapshot().info == info
    assert backend.info() == info


def test_default_fields_and_flags_read_back(board: MakeBoard) -> None:
    """Fields keep KiCad's order and visibility; the three flags are independent.

    Each KiCad gives a footprint its own built-in fields. KiCad 7 shows no
    field but Reference and Value, and has no DNP flag, so a part marked DNP
    reads as fitted there.
    """
    parts = [
        Part("R1", fields=(("LCSC", "C25804"), ("MPN", "RC0603")), hidden=("LCSC",)),
        Part("R2", dnp=True),
        Part("R3", exclude_from_bom=True),
        Part("R4", exclude_from_pos=True),
    ]
    made = board(parts)
    snapshot = made.backend.snapshot()

    assert [variant.name for variant in snapshot.variants] == [""]
    first = snapshot.components[0]
    assert _fields(first) == [
        ("Reference", "R1", True),
        ("Value", "10k", True),
        *((name, "", False) for name in made.builtin_fields[2:]),
        ("LCSC", "C25804", False),
        ("MPN", "RC0603", made.shows_fields),
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
    kept, shown = made.keeps_repeated_fields, made.shows_fields
    assert _fields(repeated)[len(made.builtin_fields) :] == [
        ("LCSC", "C2", shown),
        ("MPN", "X", shown),
    ]
    assert _fields(repeated)[1:] == _fields(plain)[1:]
    assert [(field.name, field.text) for field in repeated.default.replaced_fields] == (
        [("LCSC", "C1")] if kept else []
    )
    assert repeated.default.duplicate_names == (("LCSC",) if kept else ())
    assert plain.default.duplicate_names == ()
    assert repeated.default.revision != plain.default.revision


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


def test_revisions_follow_the_state(board: MakeBoard) -> None:
    """Equal Default states share a revision; another text or visibility changes it."""
    made = board(
        [
            Part("R1", fields=(("LCSC", "C1"),)),
            Part("R1", fields=(("LCSC", "C1"),)),
            Part("R1", fields=(("LCSC", "C2"),)),
            Part("R1", fields=(("LCSC", "C1"),), hidden=("LCSC",)),
        ]
    )
    snapshot = made.backend.snapshot()

    first, same, other, hidden = (
        record.default.revision for record in snapshot.components
    )
    assert first == same
    assert other not in {first, hidden}
    # A KiCad that never shows a field cannot hide one either.
    assert (hidden != first) is made.shows_fields


def test_a_closed_board_raises_board_changed(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Once the editor has no board, neither read returns one."""
    made = board([Part("R1")])
    made.close()

    with pytest.raises(backend_modules.api.BoardChanged):
        made.backend.info()
    with pytest.raises(backend_modules.api.BoardChanged):
        made.backend.snapshot()
