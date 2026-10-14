"""The SWIG reader finds live footprints by their snapshot keys."""

from .swig_doubles import Board, Footprint
from .wx_harness import load_siblings


def test_footprints_are_found_by_uuid_only() -> None:
    """Unknown UUIDs are left out; a repeated reference still names one footprint."""
    first, second = Footprint("R1", uuid="a"), Footprint("R1", uuid="b")
    board = Board([first, second, Footprint("C1", uuid="c")])
    with load_siblings(
        "_swig_by_key_tests", ("kicad_backend.swig.read",), {}
    ) as modules:
        live = modules["kicad_backend.swig.read"].footprints_by_key

        assert live(board, ["b", "missing"]) == {"b": second}
        assert live(board, iter(["a", "a"])) == {"a": first}
        assert live(board, []) == {}


def test_footprints_sharing_a_uuid_are_found_by_their_keys() -> None:
    """The second footprint with a UUID, in board order, is found by "#2"."""
    first, other, second = (
        Footprint("R1", uuid="a"),
        Footprint("C1", uuid="c"),
        Footprint("R2", uuid="a"),
    )
    board = Board([first, other, second])
    with load_siblings(
        "_swig_by_key_shared", ("kicad_backend.swig.read",), {}
    ) as modules:
        live = modules["kicad_backend.swig.read"].footprints_by_key

        assert live(board, ["a", "a#2"]) == {"a": first, "a#2": second}
        assert live(board, ["a#2"]) == {"a#2": second}
