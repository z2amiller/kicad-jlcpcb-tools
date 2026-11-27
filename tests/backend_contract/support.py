"""Describe a board once and build it in each backend under test.

A contract test describes footprints with ``Part``; a builder returns a
``Board``: a backend holding those footprints and a way to close the board
as its editor would.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import math
from types import SimpleNamespace
from typing import Any

MM = 1_000_000  # KiCad's nanometres in a millimetre


@dataclass(frozen=True)
class PadSpec:
    """A pad by number and kind, and where it sits in its footprint (mm, degrees)."""

    number: str
    kind: str = "smd"
    at: tuple[float, float] = (0.0, 0.0)
    size: tuple[float, float] = (1.0, 1.0)
    angle: float = 0.0
    pin_function: str = ""


@dataclass(frozen=True)
class Part:
    """One footprint as a test describes it."""

    reference: str
    value: str = "10k"
    footprint: str = "Lib:R_0603"
    fields: tuple[tuple[str, str], ...] = ()
    dnp: bool = False
    exclude_from_bom: bool = False
    exclude_from_pos: bool = False
    side: str = "top"
    pads: tuple[PadSpec, ...] = ()
    uuid_of: str = ""  # share the UUID of the first part with this reference
    at: tuple[float, float] = (0.0, 0.0)  # the anchor on the board, mm
    angle: float = 0.0  # degrees
    courtyard: tuple[float, ...] = ()  # x0, y0, x1, y1 of a rectangle, footprint frame
    courtyard_stroke: float = 0.05


def place(part: Part, point: tuple[float, float]) -> tuple[int, int]:
    """Return where KiCad puts a top-side footprint-frame point (mm) on the board (nm).

    A footprint's angle turns it counter-clockwise on KiCad's Y-down canvas,
    then its anchor moves it.
    """
    theta = math.radians(part.angle)
    x, y = point
    turned_x = x * math.cos(theta) + y * math.sin(theta)
    turned_y = y * math.cos(theta) - x * math.sin(theta)
    return round((part.at[0] + turned_x) * MM), round((part.at[1] + turned_y) * MM)


def courtyard_box(part: Part) -> tuple[int, int, int, int]:
    """Return a top-side part's courtyard rectangle on the board, stroke included (nm).

    Two opposite corners span the box while the part is turned by quarter turns,
    the only turns these tests make.
    """
    x0, y0, x1, y1 = part.courtyard
    corners = [place(part, corner) for corner in ((x0, y0), (x1, y1))]
    half = round(part.courtyard_stroke * MM / 2)
    xs, ys = [x for x, _ in corners], [y for _, y in corners]
    return min(xs) - half, min(ys) - half, max(xs) + half, max(ys) + half


def uuid_owner(parts: Sequence[Part], index: int) -> int:
    """Return the index of the part whose UUID this part has."""
    reference = parts[index].uuid_of
    if not reference:
        return index
    return next(i for i, part in enumerate(parts) if part.reference == reference)


@dataclass
class Board:
    """A backend holding the described parts, and how to close its board.

    The rest says what the backend's KiCad holds: the fields every footprint
    has, in order, whether a footprint has a DNP flag, and whether it keeps
    two fields with one name. The SWIG builder takes them from its KiCad's
    dialect; the defaults, KiCad 9's and 10's, are the fake's.
    """

    backend: Any
    close: Callable[[], None]
    builtin_fields: tuple[str, ...] = ("Reference", "Value", "Datasheet", "Description")
    holds_dnp: bool = True
    keeps_repeated_fields: bool = True


def fake_board(modules: SimpleNamespace, parts: Sequence[Part], _request: Any) -> Board:
    """Hold the parts in memory, placed as KiCad places a top-side footprint."""
    fake = modules.fake
    backend = fake.FakeBackend(
        fake.component(
            part.reference,
            uuid=f"uuid-{uuid_owner(parts, index)}",
            value=part.value,
            footprint=part.footprint,
            fields=part.fields,
            dnp=part.dnp,
            exclude_from_bom=part.exclude_from_bom,
            exclude_from_pos=part.exclude_from_pos,
            side=part.side,
            position_nm=place(part, (0.0, 0.0)),
            orientation_deg=part.angle,
            pads=[
                fake.pad(
                    pad.kind,
                    number=pad.number,
                    position_nm=place(part, pad.at),
                    size_nm=(round(pad.size[0] * MM), round(pad.size[1] * MM)),
                    orientation_deg=pad.angle + part.angle,
                    pin_function=pad.pin_function,
                )
                for pad in part.pads
            ],
            courtyard=[
                modules.records.CourtyardShape(
                    courtyard_box(part), round(part.courtyard_stroke * MM)
                )
            ]
            if part.courtyard
            else [],
        )
        for index, part in enumerate(parts)
    )

    def close() -> None:
        backend.board = None

    return Board(backend, close)
