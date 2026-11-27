"""Describe a board once and build it in each backend under test.

A contract test describes footprints with ``Part``; a builder returns a
``Board``: a backend holding those footprints, a way to close the board as
its editor would, and a way to make one footprint's writes fail.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any


@dataclass(frozen=True)
class PadSpec:
    """A pad by number and kind."""

    number: str
    kind: str = "smd"


@dataclass(frozen=True)
class Part:
    """One footprint as a test describes it."""

    reference: str
    value: str = "10k"
    footprint: str = "Lib:R_0603"
    fields: tuple[tuple[str, str], ...] = ()
    hidden: tuple[str, ...] = ()
    dnp: bool = False
    exclude_from_bom: bool = False
    exclude_from_pos: bool = False
    side: str = "top"
    pads: tuple[PadSpec, ...] = ()
    uuid_of: str = ""  # share the UUID of the first part with this reference


def uuid_owner(parts: Sequence[Part], index: int) -> int:
    """Return the index of the part whose UUID this part has."""
    reference = parts[index].uuid_of
    if not reference:
        return index
    return next(i for i, part in enumerate(parts) if part.reference == reference)


@dataclass
class Board:
    """A backend holding the described parts, and how to close its board.

    ``fail_writes(reference, restore)`` makes writes to that footprint change
    it and then fail, as a KiCad setter can; with ``restore=False`` putting
    it back fails too.

    The rest says what the backend's KiCad holds: the fields every footprint
    has, in order, whether a footprint has a DNP flag, whether it keeps two
    fields with one name, whether a field other than Reference and Value
    can be shown, and whether the fields after the built-in ones are sorted
    by name. The SWIG builder takes them from its KiCad's dialect; the
    defaults, KiCad 9's and 10's, are the fake's.
    """

    backend: Any
    close: Callable[[], None]
    fail_writes: Callable[..., None]
    builtin_fields: tuple[str, ...] = ("Reference", "Value", "Datasheet", "Description")
    holds_dnp: bool = True
    keeps_repeated_fields: bool = True
    shows_fields: bool = True
    sorts_fields: bool = False


def fake_board(modules: SimpleNamespace, parts: Sequence[Part], _request: Any) -> Board:
    """Hold the parts in memory."""
    fake = modules.fake
    backend = fake.FakeBackend(
        fake.component(
            part.reference,
            uuid=f"uuid-{uuid_owner(parts, index)}",
            value=part.value,
            footprint=part.footprint,
            fields=part.fields,
            hidden=part.hidden,
            dnp=part.dnp,
            exclude_from_bom=part.exclude_from_bom,
            exclude_from_pos=part.exclude_from_pos,
            side=part.side,
            pads=[fake.pad(pad.kind) for pad in part.pads],
        )
        for index, part in enumerate(parts)
    )

    def close() -> None:
        backend.board = None

    def fail_writes(reference: str, restore: bool = True) -> None:
        (key,) = (
            c.key for c in backend.snapshot().components if c.reference == reference
        )
        backend.fail_writes.add(key)
        if not restore:
            backend.fail_restores.add(key)

    return Board(backend, close, fail_writes)
