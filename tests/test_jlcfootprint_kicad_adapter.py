"""Tests for the pcbnew adapter with duck-typed footprints: frames, filters, hashes."""

from types import SimpleNamespace

from jlcfootprint.geometry import Pad, pad_hash
from jlcfootprint.kicad_adapter import (
    BoardPart,
    board_part,
    board_parts,
    counts_as_pad,
    footprint_pads,
    lcsc_value,
    verdict_key,
)


class Angle:
    """An EDA_ANGLE stand-in."""

    def __init__(self, degrees):
        self.degrees = degrees

    def AsDegrees(self):
        """Return the angle in degrees."""
        return self.degrees


class FakePad:
    """A pcbnew PAD with the calls the adapter makes, in millimetres."""

    def __init__(
        self,
        number,
        x,
        y,
        w=1.0,
        h=0.5,
        rotation=0.0,
        function="",
        attribute=1,
        shape=4,
        copper=True,
        relative=True,
        board=(0.0, 0.0),
    ):
        self.number, self.x, self.y, self.w, self.h = number, x, y, w, h
        self.rotation, self.function, self.attribute, self.shape, self.copper = (
            rotation,
            function,
            attribute,
            shape,
            copper,
        )
        self.board = board
        if not relative:
            # An older pcbnew without the footprint-relative getters.
            self.GetFPRelativePosition = None
            self.GetFPRelativeOrientation = None

    def GetNumber(self):
        """Return the pad number."""
        return self.number

    def GetPinFunction(self):
        """Return the schematic pin name."""
        return self.function

    def GetSize(self):
        """Return the pad size."""
        return SimpleNamespace(x=self.w, y=self.h)

    def GetShape(self):
        """Return the pcbnew shape enumeration value."""
        return self.shape

    def GetAttribute(self):
        """Return the pcbnew attribute enumeration value."""
        return self.attribute

    def IsOnCopperLayer(self):
        """Return whether the pad has copper."""
        return self.copper

    def GetFPRelativePosition(self):
        """Return the position in the footprint frame (KiCad 8 and later)."""
        return SimpleNamespace(x=self.x, y=self.y)

    def GetFPRelativeOrientation(self):
        """Return the angle relative to the footprint (KiCad 8 and later)."""
        return Angle(self.rotation)

    def GetPosition(self):
        """Return the board position (for the fallback path)."""
        return SimpleNamespace(x=self.board[0], y=self.board[1])

    def GetOrientation(self):
        """Return the absolute angle (for the fallback path)."""
        return Angle(self.rotation + 90.0)


class FakeFootprint:
    """A pcbnew FOOTPRINT with the calls the adapter makes."""

    def __init__(
        self,
        reference,
        pads,
        layer=0,
        orientation=0.0,
        name="Lib:Name",
        fields=None,
        position=(10.0, 20.0),
    ):
        self.reference, self.pads, self.layer, self.orientation, self.name = (
            reference,
            pads,
            layer,
            orientation,
            name,
        )
        self.fields = {"LCSC": "C2132"} if fields is None else fields
        self.position = position

    def GetReference(self):
        """Return the reference."""
        return self.reference

    def GetValue(self):
        """Return the value."""
        return "value"

    def GetFPID(self):
        """Return the library id."""
        return SimpleNamespace(GetLibItemName=lambda: self.name)

    def GetLayer(self):
        """Return 0 for the front copper layer."""
        return self.layer

    def GetOrientation(self):
        """Return the placement angle."""
        return Angle(self.orientation)

    def GetPosition(self):
        """Return the placement position."""
        return SimpleNamespace(x=self.position[0], y=self.position[1])

    def Pads(self):
        """Return the pads."""
        return self.pads

    def GetFields(self):
        """Return the fields."""
        return [
            SimpleNamespace(GetName=lambda n=n: n, GetText=lambda t=t: t)
            for n, t in self.fields.items()
        ]


def _sot23(**kw):
    return [
        FakePad("1", -1.0, 0.95, 0.9, 0.8, function="B", **kw),
        FakePad("2", -1.0, -0.95, 0.9, 0.8, function="E", **kw),
        FakePad("3", 1.0, 0.0, 0.9, 0.8, function="C", **kw),
    ]


def test_top_part_reads_pads_in_the_footprint_frame():
    """Positions, sizes, functions and shape names come straight from pcbnew."""
    part = board_part(
        FakeFootprint("Q1", _sot23(), orientation=90.0), to_mm=lambda v: v
    )
    assert isinstance(part, BoardPart)
    assert (
        part.reference,
        part.lcsc,
        part.footprint_name,
        part.is_bottom,
        part.placed_rotation,
    ) == ("Q1", "C2132", "Lib:Name", False, 90.0)
    assert part.pads[0] == Pad("1", -1.0, 0.95, 0.9, 0.8, 0.0, "B", "roundrect")
    assert [p.number for p in part.pads] == ["1", "2", "3"]
    assert part.footprint_hash == verdict_key(part.pads)
    assert part.footprint_hash != pad_hash(part.pads)
    assert part.value == "value"


def test_bottom_part_is_unmirrored_and_shares_the_hash():
    """Pcbnew stores a flipped footprint's pads mirrored; the adapter un-mirrors like the validator."""
    top = board_part(FakeFootprint("Q1", _sot23()), to_mm=lambda v: v)
    bottom = board_part(
        FakeFootprint("Q2", mirror_pads(_sot23()), layer=2), to_mm=lambda v: v
    )
    assert bottom.is_bottom
    assert bottom.pads == top.pads
    assert bottom.footprint_hash == top.footprint_hash


def mirror_pads(pads):
    """Return the pads as pcbnew would store them for a flipped footprint (y negated)."""
    for pad in pads:
        pad.y = -pad.y
    return pads


def test_pads_that_are_not_solder_joints_are_dropped():
    """NPTH holes and paste-only copper never reach the resolver; a custom shape is named."""
    pads = _sot23() + [
        FakePad("", 0.0, 0.0, attribute=3, copper=False),
        FakePad("", 2.0, 0.0, copper=False),
        FakePad("4", 3.0, 0.0, shape=6),
    ]
    part = board_part(FakeFootprint("Q1", pads), to_mm=lambda v: v)
    assert [p.number for p in part.pads] == ["1", "2", "3", "4"]
    assert part.pads[3].shape == "custom"
    counted = board_part(
        FakeFootprint("Q1", pads),
        to_mm=lambda v: v,
        counts=lambda pad: pad.number != "4",
    )
    assert [p.number for p in counted.pads] == ["1", "2", "3"]
    assert counts_as_pad(FakePad("1", 0, 0, attribute=1))
    assert not counts_as_pad(FakePad("1", 0, 0, attribute=3))
    assert not counts_as_pad(FakePad("1", 0, 0, copper=False))
    assert not counts_as_pad(SimpleNamespace(IsNPTH=lambda: True))


def test_older_pcbnew_falls_back_to_unrotating_board_positions():
    """Without the relative getters, the board offset is un-rotated by the footprint's angle."""
    # Footprint at (10, 20) rotated 90 degrees CCW on KiCad's Y-down canvas: local
    # (-1, 0.95) lands at board (10 + 0.95, 20 + 1).
    pads = [
        FakePad("1", 0, 0, 0.9, 0.8, function="B", relative=False, board=(10.95, 21.0)),
        FakePad("2", 0, 0, 0.9, 0.8, function="E", relative=False, board=(9.05, 21.0)),
        FakePad("3", 0, 0, 0.9, 0.8, function="C", relative=False, board=(10.0, 19.0)),
    ]
    part = board_part(FakeFootprint("Q1", pads, orientation=90.0), to_mm=lambda v: v)
    got = [(p.number, round(p.x, 6), round(p.y, 6), p.rotation) for p in part.pads]
    assert got == [
        ("1", -1.0, 0.95, 0.0),
        ("2", -1.0, -0.95, 0.0),
        ("3", 1.0, 0.0, 0.0),
    ]


def test_unit_conversion_and_old_angle_format():
    """Sizes and positions pass through ``to_mm``; a numeric angle is tenths of a degree."""
    pad = FakePad("1", 1000, 2000, 500, 250)
    pad.GetFPRelativeOrientation = lambda: 900
    footprint = FakeFootprint("R1", [pad])
    footprint.GetOrientation = lambda: 450
    part = board_part(footprint, to_mm=lambda v: v / 1000)
    assert part.pads[0] == Pad("1", 1.0, 2.0, 0.5, 0.25, 90.0, "", "roundrect")
    assert part.placed_rotation == 45.0


def test_lcsc_value_reads_fields_and_legacy_properties():
    """Any lcsc/jlc field holding C<digits> counts, on new and old pcbnew alike."""
    assert (
        lcsc_value(FakeFootprint("R1", [], fields={"JLCPCB Part": " C123 "})) == "C123"
    )
    assert lcsc_value(FakeFootprint("R1", [], fields={"LCSC": "not a code"})) == ""
    legacy = SimpleNamespace(GetProperties=lambda: {"lcsc": "C9"})
    assert lcsc_value(legacy) == "C9"


def test_board_parts_reads_every_footprint_with_a_reference():
    """The board's footprints are read in order; a footprint without a reference is skipped."""
    board = SimpleNamespace(
        GetFootprints=lambda: [
            FakeFootprint("R1", [FakePad("1", -1, 0), FakePad("2", 1, 0)]),
            FakeFootprint("", [FakePad("1", 0, 0)]),
            FakeFootprint("C1", [], fields={}),
        ]
    )
    parts = board_parts(
        board,
        pcbnew=SimpleNamespace(PAD_ATTRIB_NPTH=3, PAD_SHAPE_CUSTOM=6, ToMM=lambda v: v),
    )
    assert [(p.reference, p.lcsc, len(p.pads)) for p in parts] == [
        ("R1", "C2132", 2),
        ("C1", "", 0),
    ]
    assert (
        footprint_pads(FakeFootprint("R1", [FakePad("1", 0, 0)]), to_mm=lambda v: v)[
            0
        ].number
        == "1"
    )


def test_verdict_key_includes_the_pin_functions():
    """Swapped K/A functions give different keys; no function gives the pad hash itself."""
    cathode_first = [
        FakePad("1", -1, 0, function="K"),
        FakePad("2", 1, 0, function="A"),
    ]
    anode_first = [FakePad("1", -1, 0, function="A"), FakePad("2", 1, 0, function="K")]
    plain = [FakePad("1", -1, 0), FakePad("2", 1, 0)]
    keyed = [
        board_part(FakeFootprint("D1", pads), to_mm=lambda v: v)
        for pads in (cathode_first, anode_first, plain)
    ]
    assert keyed[0].footprint_hash != keyed[1].footprint_hash
    assert keyed[2].footprint_hash == pad_hash(keyed[2].pads)
    assert keyed[0].footprint_hash != keyed[2].footprint_hash
    spelled = [
        FakePad("1", -1, 0, function="k_1"),
        FakePad("2", 1, 0, function="anode"),
    ]
    assert (
        verdict_key(board_part(FakeFootprint("D2", spelled), to_mm=lambda v: v).pads)
        != keyed[0].footprint_hash
    )
    assert verdict_key(
        [Pad("1", -1, 0, 1, 0.5, 0, "K"), Pad("2", 1, 0, 1, 0.5, 0, "A")]
    ) == verdict_key([Pad("2", 1, 0, 1, 0.5, 0, "a"), Pad("1", -1, 0, 1, 0.5, 0, "k")])
