"""Tests for jlcfootprint.boardfile: footprints, pads, pin functions and LCSC fields."""

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb_text
from jlcfootprint.geometry import Pad

BOARD = """
(kicad_pcb (version 20240108) (generator "pcbnew")
  (footprint "Diode_SMD:D_SMF" (layer "F.Cu") (at 10 20 90)
    (property "Reference" "D1" (at 0 0 0) (layer "F.SilkS"))
    (property "Value" "1N4148W" (at 0 0 0) (layer "F.Fab"))
    (property "LCSC" "C81598" (at 0 0 0) (layer "F.Fab"))
    (pad "1" smd roundrect (at -1.45 0 90) (size 1.3 1.4) (layers "F.Cu" "F.Paste" "F.Mask") (pinfunction "K"))
    (pad "2" smd roundrect (at 1.45 0 90) (size 1.3 1.4) (layers "F.Cu" "F.Paste" "F.Mask") (pinfunction "A"))
  )
  (footprint "Package_TO_SOT_SMD:SOT-23" (layer "B.Cu") (at 30 20)
    (property "Reference" "Q1" (at 0 0 0) (layer "B.SilkS"))
    (property "Value" "2SA812" (at 0 0 0) (layer "B.Fab"))
    (property "jlcpcb part" "C2132" (at 0 0 0) (layer "B.Fab"))
    (pad "1" smd roundrect (at -0.9375 0.95) (size 1.475 0.6) (layers "B.Cu" "B.Paste" "B.Mask"))
    (pad "2" smd roundrect (at -0.9375 -0.95) (size 1.475 0.6) (layers "B.Cu" "B.Paste" "B.Mask"))
    (pad "3" smd roundrect (at 0.9375 0) (size 1.475 0.6) (layers "B.Cu" "B.Paste" "B.Mask"))
    (pad "" np_thru_hole circle (at 0 3) (size 1 1) (drill 1) (layers "*.Cu"))
  )
  (footprint "Resistor_SMD:R_0603_1608Metric" (layer "F.Cu") (at 50 20)
    (property "Reference" "R9" (at 0 0 0) (layer "F.SilkS"))
    (property "Value" "10k" (at 0 0 0) (layer "F.Fab"))
    (property "LCSC" "" (at 0 0 0) (layer "F.Fab"))
    (pad "1" smd roundrect (at -0.7875 0) (size 0.875 0.95) (layers "F.Cu"))
    (pad "2" smd roundrect (at 0.7875 0) (size 0.875 0.95) (layers "F.Cu"))
  )
)
"""


def test_footprints_positions_layers_and_lcsc_fields():
    """Reference, placement, side and the LCSC field (any lcsc/jlc-named property) are read."""
    d1, q1, r9 = parse_kicad_pcb_text(BOARD)
    assert (d1.reference, d1.footprint_name, d1.lcsc) == (
        "D1",
        "Diode_SMD:D_SMF",
        "C81598",
    )
    assert (d1.placed_x, d1.placed_y, d1.placed_rotation, d1.is_bottom) == (
        10.0,
        20.0,
        90.0,
        False,
    )
    assert (q1.reference, q1.lcsc, q1.is_bottom, q1.placed_rotation) == (
        "Q1",
        "C2132",
        True,
        0.0,
    )
    assert (r9.reference, r9.lcsc) == ("R9", None)


KICAD7_TEXT = """(kicad_pcb (version 20221018) (generator pcbnew)
  (footprint "Resistor_SMD:R_0603_1608Metric" (layer "F.Cu") (at 5 5)
    (property "LCSC" "C25804")
    (fp_text reference "R7" (at 0 -1.4) (layer "F.SilkS"))
    (fp_text value "10k" (at 0 1.4) (layer "F.Fab"))
    (fp_text user "${REFERENCE}" (at 0 0) (layer "F.Fab"))
    (pad "1" smd roundrect (at -0.8 0) (size 0.8 0.95) (layers "F.Cu" "F.Paste" "F.Mask"))
  )
)"""


def test_kicad_7_reference_and_value_come_from_fp_text():
    """KiCad 7 and earlier keep the reference and the value in fp_text items, not properties."""
    (r7,) = parse_kicad_pcb_text(KICAD7_TEXT)
    assert (r7.reference, r7.value, r7.lcsc) == ("R7", "10k", "C25804")


def test_pads_carry_pin_functions_and_relative_rotation():
    """Pad angles in the file are absolute; the footprint frame removes the placed rotation."""
    d1, q1, _ = parse_kicad_pcb_text(BOARD)
    pads = footprint_pads(d1)
    assert pads == [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "K", "roundrect"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "A", "roundrect"),
    ]
    q1_pads = footprint_pads(q1)
    assert [p.number for p in q1_pads] == ["1", "2", "3"]
    assert q1_pads[0].pin_function == ""
    assert q1_pads[0].rotation == 0.0


SOLDERED_TEXT = """(kicad_pcb (footprint "Lib:Part" (layer "F.Cu") (at 0 0)
  (property "Reference" "U9")
  (pad "1" smd rect (at -1 0) (size 0.6 0.6) (layers "F.Cu" "F.Paste" "F.Mask"))
  (pad "2" smd rect (at 1 0) (size 0.6 0.6) (layers "F.Cu" "F.Paste" "F.Mask"))
  (pad "" smd rect (at 0 2) (size 3 1) (layers "F.Cu" "F.Mask"))
  (pad "3" np_thru_hole circle (at 0 -4) (size 1 1) (drill 1) (layers "*.Cu" "*.Mask"))
  (pad "4" smd rect (at 0 0.5) (size 0.4 0.4) (layers "F.Paste"))
  (pad "" thru_hole circle (at 3 0) (size 1.6 1.6) (drill 1) (layers "*.Cu" "*.Mask"))
))"""


def test_the_soldered_pads_are_the_copper_pads_that_are_not_holes():
    """An unnumbered copper pad is kept; a numbered NPTH hole and a paste-only aperture are not.

    These are the pads the plugin keeps when it reads the board through pcbnew, so
    both readings give the same set of pads; the unnumbered pads carry ``""``.
    """
    (footprint,) = parse_kicad_pcb_text(SOLDERED_TEXT)
    assert [(p.number, p.x, p.y) for p in footprint_pads(footprint)] == [
        ("1", -1.0, 0.0),
        ("2", 1.0, 0.0),
        ("", 0.0, 2.0),
        ("", 3.0, 0.0),
    ]


COURTYARD_TEXT = """(kicad_pcb (footprint "Lib:Part" (layer "F.Cu") (at 10 20 90)
  (property "Reference" "C9")
  (fp_line (start -1 -2) (end 1 -2) (layer "F.CrtYd"))
  (fp_rect (start -1.5 -0.5) (end 1.5 0.5) (layer "F.CrtYd"))
  (fp_circle (center 0 1) (end 0.5 1) (layer "F.CrtYd"))
  (fp_poly (pts (xy 0 2.5) (xy 0.2 2.6)) (layer "F.CrtYd"))
  (fp_arc (start -1 0) (mid -1.2 0.5) (end -1 1) (layer "F.CrtYd"))
  (fp_line (start -9 -9) (end 9 9) (layer "F.SilkS"))
  (pad "1" smd rect (at -1 0) (size 1 1) (layers "F.Cu"))
))"""


def test_courtyard_box_covers_lines_rectangles_circles_polygons_and_arcs():
    """Every courtyard graphic's points bound the box; silkscreen is ignored."""
    from jlcfootprint.boardfile import parse_kicad_pcb_text

    (footprint,) = parse_kicad_pcb_text(COURTYARD_TEXT)
    assert footprint.courtyard == (-1.5, -2.0, 1.5, 2.6)
    bare = COURTYARD_TEXT.split("\n")
    (plain,) = parse_kicad_pcb_text(
        "\n".join(line for line in bare if "CrtYd" not in line)
    )
    assert plain.courtyard is None
