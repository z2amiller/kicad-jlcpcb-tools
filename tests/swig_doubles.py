"""Stateful stand-ins for the KiCad SWIG objects the board backend reads.

They answer every call ``kicad_backend.swig`` makes, so tests that still
edit footprints through SWIG-shaped objects can read them through the real
SWIG backend.
"""

from collections.abc import Iterable, Mapping
from types import SimpleNamespace
from typing import Any, Optional
from uuid import NAMESPACE_URL, uuid5

# PAD_ATTRIB values: PTH=0, SMD=1, CONN=2, NPTH=3.
PAD_ATTRIBUTES = {"pth": 0, "smd": 1, "conn": 2, "npth": 3}
# PAD_SHAPE values.
PAD_SHAPES = {
    "circle": 0,
    "rect": 1,
    "oval": 2,
    "trapezoid": 3,
    "roundrect": 4,
    "chamfered_rect": 5,
    "custom": 6,
}
# Two layer IDs standing for the courtyards, with KiCad's own names for them.
LAYER_NAMES = {29: "B.Courtyard", 31: "F.Courtyard", 0: "F.Cu", 2: "B.Cu"}
F_COURTYARD = 31


def point(x: int, y: int) -> SimpleNamespace:
    """Return a VECTOR2I."""
    return SimpleNamespace(x=x, y=y)


class Field:
    """A footprint field with text and visibility."""

    def __init__(self, name: str, text: str, visible: bool = True) -> None:
        self.name, self.text, self.visible = name, text, visible

    def GetName(self) -> str:
        """Return the field name."""
        return self.name

    def GetText(self) -> str:
        """Return the field text."""
        return self.text

    def IsVisible(self) -> bool:
        """Return whether the field is shown."""
        return self.visible

    def SetVisible(self, visible: bool) -> None:
        """Show or hide the field."""
        self.visible = visible


class Pad:
    """A pad by number and kind, where it is on the board, its size, shape and pin."""

    def __init__(
        self,
        number: str = "1",
        kind: str = "smd",
        *,
        position: tuple[int, int] = (0, 0),
        size: tuple[int, int] = (1_000_000, 1_000_000),
        orientation: float = 0.0,
        shape: str = "rect",
        pin_function: str = "",
        on_copper: bool = True,
    ) -> None:
        self.number, self.kind = number, kind
        self.position, self.size, self.orientation = position, size, orientation
        self.shape, self.pin_function, self.on_copper = shape, pin_function, on_copper

    def GetAttribute(self) -> int:
        """Return KiCad's PAD_ATTRIB value."""
        return PAD_ATTRIBUTES[self.kind]

    def HasHole(self) -> bool:
        """Return whether the pad is drilled."""
        return self.kind in {"pth", "npth"}

    def GetNumber(self) -> str:
        """Return the pad number."""
        return self.number

    def GetPosition(self) -> SimpleNamespace:
        """Return the pad's centre on the board."""
        return point(*self.position)

    def GetSize(self) -> SimpleNamespace:
        """Return the pad's width and height."""
        return point(*self.size)

    def GetOrientation(self) -> SimpleNamespace:
        """Return the pad's angle on the board."""
        return SimpleNamespace(AsDegrees=lambda: self.orientation)

    def GetShape(self) -> int:
        """Return KiCad's PAD_SHAPE value."""
        return PAD_SHAPES[self.shape]

    def GetPinFunction(self) -> str:
        """Return the schematic pin's name."""
        return self.pin_function

    def IsOnCopperLayer(self) -> bool:
        """Return whether the pad has copper."""
        return self.on_copper


class Text:
    """A text on a layer: a graphic item with no stroke width."""

    def __init__(self, layer: int = F_COURTYARD) -> None:
        self.layer = layer

    def GetLayer(self) -> int:
        """Return the layer ID."""
        return self.layer

    def GetClass(self) -> str:
        """Return KiCad's class name for a footprint text."""
        return "PCB_TEXT"


class Shape:
    """A drawn shape on a layer: its box on the board, stroke included, and stroke width."""

    def __init__(
        self,
        box: tuple[int, int, int, int],
        *,
        width: int = 50_000,
        layer: int = F_COURTYARD,
        kicad_class: str = "PCB_SHAPE",
    ) -> None:
        self.box, self.width, self.layer = box, width, layer
        self.kicad_class = kicad_class

    def GetLayer(self) -> int:
        """Return the layer ID."""
        return self.layer

    def GetClass(self) -> str:
        """Return KiCad's class name for a drawn shape (KiCad 7's is MGRAPHIC)."""
        return self.kicad_class

    def GetWidth(self) -> int:
        """Return the stroke width."""
        return self.width

    def GetBoundingBox(self) -> SimpleNamespace:
        """Return the box, stroke included."""
        left, top, right, bottom = self.box
        return SimpleNamespace(
            GetLeft=lambda: left,
            GetTop=lambda: top,
            GetRight=lambda: right,
            GetBottom=lambda: bottom,
        )


class Footprint:
    """A footprint with live fields, attribute bits, placement and pads."""

    def __init__(
        self,
        reference: str = "R1",
        *,
        value: str = "10k",
        footprint: str = "Lib:R_0603",
        fields: Optional[Mapping[str, str]] = None,  # noqa: UP045
        attributes: int = 0,
        dnp: bool = False,
        flipped: bool = False,
        position: tuple[int, int] = (0, 0),
        orientation: float = 0.0,
        pads: Iterable[Pad] = (),
        shapes: Iterable[Shape] = (),
        uuid: Optional[str] = None,  # noqa: UP045
        path: str = "",
    ) -> None:
        self.reference, self.value, self.footprint = reference, value, footprint
        self.fields = {name: Field(name, text) for name, text in (fields or {}).items()}
        self.attributes, self.dnp, self.flipped = attributes, dnp, flipped
        self.position, self.orientation = position, orientation
        self.pads = list(pads)
        self.shapes = list(shapes)
        self.uuid = uuid or str(uuid5(NAMESPACE_URL, f"swig-double:{reference}"))
        self.path = path
        self.modified = False
        self.descriptions = {"Datasheet": "", "Description": ""}
        self.m_Uuid = SimpleNamespace(AsString=lambda: self.uuid)

    def GetReference(self) -> str:
        """Return the reference."""
        return self.reference

    def GetValue(self) -> str:
        """Return the Value field's text."""
        return self.value

    def GetFPID(self) -> SimpleNamespace:
        """Return the library identifier."""
        return SimpleNamespace(GetLibItemName=lambda: self.footprint.split(":", 1)[-1])

    def GetFPIDAsString(self) -> str:
        """Return the library identifier as "Lib:Name"."""
        return self.footprint

    def GetFields(self) -> list[Field]:
        """Return Reference, Value, Datasheet, Description, then the user fields."""
        return [
            Field("Reference", self.reference),
            Field("Value", self.value),
            *(Field(name, text, False) for name, text in self.descriptions.items()),
            *self.fields.values(),
        ]

    def SetField(self, name: str, text: str) -> None:
        """Change a field, adding a user field when the name is new."""
        if name == "Reference":
            self.reference = text
        elif name == "Value":
            self.value = text
        elif name in self.descriptions:
            self.descriptions[name] = text
        else:
            self.fields.setdefault(name, Field(name, "")).text = text

    def RemoveField(self, name: str) -> None:
        """Remove a user field."""
        self.fields.pop(name, None)

    def GetAttributes(self) -> int:
        """Return the attribute bits."""
        return self.attributes

    def SetAttributes(self, attributes: int) -> None:
        """Replace the attribute bits."""
        self.attributes = attributes

    def IsDNP(self) -> bool:
        """Return the DNP flag."""
        return self.dnp

    def IsFlipped(self) -> bool:
        """Return whether the footprint is on the bottom."""
        return self.flipped

    def GetLayer(self) -> int:
        """Return F.Cu or B.Cu."""
        return 2 if self.flipped else 0

    def GetPosition(self) -> SimpleNamespace:
        """Return the anchor position."""
        return point(*self.position)

    def GetOrientation(self) -> SimpleNamespace:
        """Return the rotation."""
        return SimpleNamespace(AsDegrees=lambda: self.orientation)

    def GetPath(self) -> SimpleNamespace:
        """Return the symbol path."""
        return SimpleNamespace(AsString=lambda: self.path)

    def Pads(self) -> list[Pad]:
        """Return the pads."""
        return list(self.pads)

    def GraphicalItems(self) -> list[Shape]:
        """Return the drawn shapes."""
        return list(self.shapes)

    def SetModified(self) -> None:
        """Record an edit."""
        self.modified = True


class Board:
    """A saved board holding footprints in order."""

    def __init__(
        self,
        footprints: Iterable[Footprint] = (),
        *,
        filename: str = "/project/board.kicad_pcb",
        variants: Optional[Iterable[str]] = None,  # noqa: UP045
    ) -> None:
        self.footprints = list(footprints)
        self.filename = filename
        self.uuid = "board-uuid"
        self.m_Uuid = SimpleNamespace(AsString=lambda: self.uuid)
        if variants is not None:
            names = list(variants)
            self.GetVariantNamesForUI = lambda: names

    def GetFileName(self) -> str:
        """Return the board file."""
        return self.filename

    @staticmethod
    def GetStandardLayerName(layer: int) -> str:
        """Return KiCad's own name for a layer."""
        return LAYER_NAMES.get(layer, f"User.{layer}")

    def GetFootprints(self) -> list[Footprint]:
        """Return the footprints in board order."""
        return list(self.footprints)

    def FindFootprintByReference(self, reference: str) -> Optional[Footprint]:  # noqa: UP045
        """Return the first footprint with this reference."""
        return next((fp for fp in self.footprints if fp.reference == reference), None)


def editor(board: Any, version: str = "10.0.4") -> SimpleNamespace:
    """Return a pcbnew module of this KiCad version whose editor has ``board`` open."""
    return SimpleNamespace(
        GetBoard=lambda: board,
        GetBuildVersion=lambda: version,
        GetMajorMinorVersion=lambda: ".".join(version.split(".")[:2]),
    )
