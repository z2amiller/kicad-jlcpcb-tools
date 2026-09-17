"""Tests for the M3 resolver batch (spec 16.6): distinct pad numbers, pairing by function, the drawings' + marks, the body caveat."""

import pytest

from jlcfootprint.drawing import DrawingMarks, drawing_marks
from jlcfootprint.easyeda_parse import SymbolPin
from jlcfootprint.geometry import Pad, easyeda_pads_to_mm
from jlcfootprint.polarity import function_terminals, is_no_function, part_kind
from jlcfootprint.resolver import YELLOW_NOTE, body_threshold_mm, resolve
from tests.jlcfootprint_support import library_footprint, pro_record, with_functions

KICAD_SMF = [
    Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "K"),
    Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "A"),
]
JLC_SMF = [Pad("1", -1.45, 0.0, 1.3, 1.4), Pad("2", 1.45, 0.0, 1.3, 1.4)]
D1_PINS = [SymbolPin("1", "K"), SymbolPin("2", "A")]
NUMBERED = [SymbolPin("1", "1"), SymbolPin("2", "2")]


def resolve_library(library, footprint, lcsc, functions=None, marks=True):
    """Resolve a KiCad library footprint against a Pro-recorded part, drawings included."""
    record = pro_record(lcsc)
    pads, courtyard = library_footprint(library, footprint)
    if functions:
        pads = with_functions(pads, functions)
    return resolve(
        pads,
        f"{library}:{footprint}",
        record.status,
        record.package_name,
        easyeda_pads_to_mm(record.pads),
        record.symbol_pins,
        marks=drawing_marks(record.symbol_shapes, record.footprint_shapes)
        if marks
        else None,
        kicad_courtyard=courtyard,
    )


# --- item 1: distinct pad numbers and pairing by function ------------------


def test_dpak_with_a_stub_counts_three_pads_and_fits():
    """TO-252-3_TabPin2's tab and stub are one pad 2: C20717 (tab = pin 2 on both sides) is green at 0 with no count note."""
    verdict = resolve_library("Package_TO_SOT_SMD", "TO-252-3_TabPin2", "C20717")
    assert (verdict.status, verdict.rotation, verdict.method) == (
        "green",
        0,
        "geometry",
    )
    assert (verdict.pad_count_kicad, verdict.pad_count_jlc) == (3, 3)
    assert "pad counts differ" not in verdict.note_text
    assert verdict.confidence == "high"


def test_dpak_whose_jlc_drawing_numbers_the_tab_3_pairs_by_function():
    """C42441713's drawing calls the tab 3 and the S lead 2; the schematic's G/D/S pair the pins and the part fits at 270."""
    verdict = resolve_library(
        "Package_TO_SOT_SMD",
        "TO-252-3_TabPin2",
        "C42441713",
        {"1": "G", "2": "D", "3": "S"},
    )
    assert (verdict.status, verdict.rotation, verdict.confidence) == (
        "green",
        270,
        "medium",
    )
    assert "paired by pin function" in verdict.note_text
    assert "JLC pin 3 (D) is pad 2 on the footprint" in verdict.note_text
    assert "JLC pin 2 (S) is pad 3 on the footprint" in verdict.note_text
    assert "by elimination" not in verdict.note_text
    assert verdict.name_rotation == 270


def test_one_unmatched_function_pairs_by_elimination():
    """IN on the gate pad matches no symbol name; D and S pair and the gate follows by elimination."""
    verdict = resolve_library(
        "Package_TO_SOT_SMD",
        "TO-252-3_TabPin2",
        "C42441713",
        {"1": "IN", "2": "D", "3": "S"},
    )
    assert (verdict.status, verdict.rotation) == ("green", 270)
    assert "JLC pin 1 and pad 1 paired by elimination" in verdict.note_text


def test_without_pin_functions_the_numbering_difference_stays_red():
    """No functions, no pairing: the numbering finding stands, with the angle the package would take."""
    for footprint in ("TO-252-3_TabPin2", "TO-252-2"):
        verdict = resolve_library("Package_TO_SOT_SMD", footprint, "C42441713")
        assert (verdict.status, verdict.fit, verdict.rotation) == (
            "red",
            "numbering",
            None,
        )
        assert "the package itself aligns at 270°" in verdict.note_text


# --- item 2: the drawings' + marks ---------------------------------------


def test_tantalums_without_token_or_label_are_read_from_their_marks():
    """CASE-A and CASE-C parts carry + marks in both drawings; pad 1 is the left pad on one and the right pad on the other."""
    left = resolve_library("Capacitor_Tantalum_SMD", "CP_EIA-3216-18_Kemet-A", "C7175")
    assert (left.status, left.rotation, left.polarity_source, left.confidence) == (
        "green",
        0,
        "drawing",
        "medium",
    )
    assert "polarity from the + marks of the symbol and the footprint" in left.notes
    right = resolve_library("Capacitor_Tantalum_SMD", "CP_EIA-6032-20_AVX-F", "C140418")
    assert (right.rotation, right.polarity_source) == (180, "drawing")
    blind = resolve_library(
        "Capacitor_Tantalum_SMD", "CP_EIA-3216-18_Kemet-A", "C7175", marks=False
    )
    assert (blind.status, blind.rotation) == ("unknown", None)
    assert "polarity unknown" in blind.note_text


def test_reversed_numbering_tantalum_token_and_marks_agree():
    """C7192's -R-RD token, read as every capacitor's (band = negative), agrees with its marks: 180 from the token."""
    verdict = resolve_library("Capacitor_Tantalum_SMD", "CP_EIA-3528-15_AVX-H", "C7192")
    assert (verdict.rotation, verdict.polarity_source) == (180, "token")
    assert (
        verdict.confidence == "medium"
    )  # KiCad pad 1 = + is assumed, as on every CP_ footprint without functions
    assert "names the other pad" not in verdict.note_text
    wired = resolve_library(
        "Capacitor_Tantalum_SMD", "CP_EIA-3528-15_AVX-H", "C7192", {"1": "+", "2": "-"}
    )
    assert (wired.rotation, wired.confidence) == (180, "high")


def test_a_single_mark_against_the_token_is_a_tie_and_therefore_inconsistent():
    """A diode whose footprint + sits at the cathode end contradicts its RD token one to one: no guess."""
    marks = DrawingMarks(positive_pad="1")
    verdict = resolve(
        JLC_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "SMF_L2.8-W1.8-LS3.7-RD",
        JLC_SMF,
        NUMBERED,
        marks=marks,
    )
    assert (verdict.status, verdict.rotation) == ("red", None)
    assert verdict.note_text.endswith(
        "EasyEDA data inconsistent: name token and footprint + mark disagree"
    )


def test_token_and_label_outvote_a_footprint_mark():
    """Two sources against one: the token's pad stands, the outvoted mark is noted and confidence drops."""
    marks = DrawingMarks(positive_pad="1")
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "SMF_L2.8-W1.8-LS3.7-RD",
        JLC_SMF,
        D1_PINS,
        marks=marks,
    )
    assert (verdict.status, verdict.rotation, verdict.polarity_source) == (
        "green",
        0,
        "token",
    )
    assert verdict.confidence == "medium"
    assert (
        "footprint + mark names the other pad; name token, symbol pin-1 label decide"
        in verdict.notes
    )
    assert verdict.polarity_light == "green"


def test_both_marks_outvote_a_live_label_and_the_light_follows_the_vote():
    """Two marks against a label: the marks decide, and pin 1's meaning comes from the vote, not the outvoted label."""
    marks = DrawingMarks(positive_pin="1", positive_pad="1")
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "DIO-SMD_L2.8-W1.8",
        JLC_SMF,
        D1_PINS,
        marks=marks,
    )
    assert (verdict.rotation, verdict.polarity_source) == (180, "drawing")
    # JLC's pad 1 is the anode by the vote and KiCad's pad 1 is the cathode.
    assert (verdict.polarity_light, verdict.status) == ("yellow", "yellow")
    assert YELLOW_NOTE in verdict.notes
    assert (
        "symbol pin-1 label names the other pad; symbol + mark, footprint + mark decide"
        in verdict.notes
    )


def test_the_light_is_known_whenever_the_vote_settles_pad_1():
    """Without any label the token or the marks still say what JLC's pad 1 is: green when it matches KiCad's pad 1, yellow when not."""
    token_only = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, NUMBERED
    )
    assert (token_only.polarity_light, token_only.status) == ("green", "green")
    reversed_token = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-FD", JLC_SMF, NUMBERED
    )
    assert (reversed_token.rotation, reversed_token.polarity_light) == (180, "yellow")
    marks_only = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "DIO-SMD_L2.8-W1.8",
        JLC_SMF,
        NUMBERED,
        marks=DrawingMarks(positive_pad="2"),
    )
    assert (marks_only.rotation, marks_only.polarity_light) == (0, "green")


def test_token_label_and_symbol_mark_outvote_a_footprint_mark():
    """Marks that name different pads are two votes, not a veto: three sources against the footprint's mark decide."""
    marks = DrawingMarks(positive_pin="2", positive_pad="1")
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "SMF_L2.8-W1.8-LS3.7-RD",
        JLC_SMF,
        D1_PINS,
        marks=marks,
    )
    assert (verdict.status, verdict.rotation, verdict.confidence) == (
        "green",
        0,
        "medium",
    )
    assert (
        "footprint + mark names the other pad; name token, symbol pin-1 label, symbol + mark decide"
        in verdict.notes
    )


def test_marks_that_contradict_each_other_are_inconsistent():
    """The symbol's + beside pin 1 and the footprint's + beside pad 2 cannot both be right."""
    marks = DrawingMarks(positive_pin="1", positive_pad="2")
    verdict = resolve(
        JLC_SMF,
        "Capacitor_SMD:CP_Elec_4x5.4",
        "ok",
        "CASE-A_3216",
        JLC_SMF,
        NUMBERED,
        marks=marks,
    )
    assert (verdict.status, verdict.rotation) == ("red", None)
    assert verdict.note_text.endswith(
        "EasyEDA data inconsistent: symbol + mark and footprint + mark disagree"
    )


def test_marks_outvote_a_seeded_label():
    """A seeded per-footprint polarity is one vote; this part's own two marks win."""
    marks = DrawingMarks(positive_pin="1", positive_pad="1")
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "DIO-SMD_L2.8-W1.8",
        JLC_SMF,
        D1_PINS,
        "seed-puuid",
        marks=marks,
    )
    assert (verdict.rotation, verdict.polarity_source, verdict.confidence) == (
        180,
        "drawing",
        "medium",
    )
    assert "seeded pin-1 polarity names the other pad" in verdict.note_text


def test_a_lone_footprint_mark_decides_when_nothing_else_speaks():
    """A seeded row has no symbol; the footprint's own + mark is enough, at medium confidence."""
    marks = DrawingMarks(positive_pad="2")
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "DIO-SMD_L2.8-W1.8",
        JLC_SMF,
        NUMBERED,
        marks=marks,
    )
    assert (verdict.rotation, verdict.polarity_source, verdict.confidence) == (
        0,
        "drawing",
        "medium",
    )
    assert "polarity from the footprint's + mark" in verdict.notes


def test_reversed_numbering_with_pin_functions_pairs_by_function():
    """A JLC SOT-23 numbered the other way round is a mirror by number; the schematic's B/E/C pair the pins and the part fits as drawn."""
    kicad = [
        Pad("1", -0.95, 1.0, 0.6, 1.0, 0.0, "B"),
        Pad("2", 0.95, 1.0, 0.6, 1.0, 0.0, "E"),
        Pad("3", 0.0, -1.0, 0.6, 1.0, 0.0, "C"),
    ]
    jlc = [
        Pad("2", -0.95, 1.0, 0.6, 1.0),
        Pad("1", 0.95, 1.0, 0.6, 1.0),
        Pad("3", 0.0, -1.0, 0.6, 1.0),
    ]
    pins = [SymbolPin("1", "E"), SymbolPin("2", "B"), SymbolPin("3", "C")]
    paired = resolve(
        kicad, "Package_TO_SOT_SMD:SOT-23", "ok", "SOT-23-3_L2.9-W1.6", jlc, pins
    )
    assert (paired.status, paired.rotation, paired.confidence) == ("green", 0, "medium")
    assert "JLC pin 2 (B) is pad 1 on the footprint" in paired.note_text
    blind = resolve(
        [pad._replace(pin_function="") for pad in kicad],
        "Package_TO_SOT_SMD:SOT-23",
        "ok",
        "SOT-23-3_L2.9-W1.6",
        jlc,
        pins,
    )
    assert (blind.status, blind.fit, blind.rotation) == ("red", "mirror", None)


def test_pads_that_share_no_name_pair_by_function():
    """A JLC drawing naming its pads E, B and C shares no name with KiCad's 1, 2, 3; the schematic's functions pair them and the part fits as drawn."""
    kicad = [
        Pad("1", -0.95, 1.0, 0.6, 1.0, 0.0, "B"),
        Pad("2", 0.95, 1.0, 0.6, 1.0, 0.0, "E"),
        Pad("3", 0.0, -1.0, 0.6, 1.0, 0.0, "C"),
    ]
    jlc = [
        Pad("B", -0.95, 1.0, 0.6, 1.0),
        Pad("E", 0.95, 1.0, 0.6, 1.0),
        Pad("C", 0.0, -1.0, 0.6, 1.0),
    ]
    pins = [SymbolPin("B", "B"), SymbolPin("E", "E"), SymbolPin("C", "C")]
    paired = resolve(
        kicad, "Package_TO_SOT_SMD:SOT-23", "ok", "SOT-23-3_L2.9-W1.6", jlc, pins
    )
    assert (paired.status, paired.rotation, paired.confidence) == ("green", 0, "medium")
    assert "JLC pin B (B) is pad 1 on the footprint" in paired.note_text
    blind = resolve(
        [pad._replace(pin_function="") for pad in kicad],
        "Package_TO_SOT_SMD:SOT-23",
        "ok",
        "SOT-23-3_L2.9-W1.6",
        jlc,
        pins,
    )
    assert (blind.status, blind.fit) == ("unknown", "no_data")
    assert "fewer than two matching pad names" in blind.note_text


# --- item 3: kicad-x2ib ---------------------------------------------------


def test_no_connection_functions_carry_no_terminal_and_disqualify_nothing():
    """NC-like functions are skipped, so K and A beside an NC pad still read as a diode."""
    assert is_no_function("NC") and is_no_function("n/c") and is_no_function("~")
    assert is_no_function("DNC") and is_no_function("NP") and is_no_function("")
    assert not is_no_function("K") and not is_no_function("VCC")
    pads = [
        Pad("1", -1.0, 0.0, 1.0, 1.0, 0.0, "K"),
        Pad("2", 1.0, 0.0, 1.0, 1.0, 0.0, "A"),
        Pad("3", 0.0, 1.5, 1.0, 0.5, 0.0, "NC"),
    ]
    assert function_terminals(pads) == {"cathode", "anode"}
    assert part_kind("MPN-3PAD", "Custom:Three", pads, []) == "diode"
    switch = [Pad("1", -1, 0, 1, 1, 0, "A"), Pad("2", 1, 0, 1, 1, 0, "B")]
    assert function_terminals(switch) == set()
    c_and_a = [Pad("1", -1, 0, 1, 1, 0, "C"), Pad("2", 1, 0, 1, 1, 0, "A")]
    assert part_kind("MPN-2PAD", "Custom:Two", c_and_a, []) == "other"


# --- item 4: the body caveat ----------------------------------------------


def test_body_larger_than_the_courtyard_is_a_caveat_not_a_failure():
    """C88744's 6.6 mm body on CP_Elec_4x5.8 fits its pads but overhangs the courtyard by 0.9 mm each side."""
    small = resolve_library("Capacitor_SMD", "CP_Elec_4x5.8", "C88744")
    assert (small.status, small.rotation, small.fit) == ("green", 0, "fits")
    assert small.body_excess_mm == pytest.approx(0.9, abs=0.01)
    assert "fits, JLC body 0.9 mm larger than the KiCad courtyard" in small.notes
    big = resolve_library("Capacitor_SMD", "CP_Elec_6.3x5.9", "C88744")
    assert big.body_excess_mm is None and "larger than" not in big.note_text
    dpak = resolve_library(
        "Package_TO_SOT_SMD",
        "TO-252-3_TabPin2",
        "C42441713",
        {"1": "G", "2": "D", "3": "S"},
    )
    assert dpak.body_excess_mm is None  # the courtyard is turned with the pads


def test_the_caveat_needs_a_courtyard_and_a_body_and_a_fit():
    """No courtyard, no body box or no rotation gives no caveat."""
    record = pro_record("C88744")
    pads, courtyard = library_footprint("Capacitor_SMD", "CP_Elec_4x5.8")
    jlc = easyeda_pads_to_mm(record.pads)
    body = DrawingMarks(
        positive_pin="1", positive_pad="1", body_box=(-3.3, -3.3, 3.3, 3.3)
    )
    assert (
        resolve(
            pads,
            "Capacitor_SMD:CP_Elec_4x5.8",
            "ok",
            record.package_name,
            jlc,
            record.symbol_pins,
            marks=body,
        ).body_excess_mm
        is None
    )
    without_body = DrawingMarks(positive_pin="1", positive_pad="1")
    assert (
        resolve(
            pads,
            "Capacitor_SMD:CP_Elec_4x5.8",
            "ok",
            record.package_name,
            jlc,
            record.symbol_pins,
            marks=without_body,
            kicad_courtyard=courtyard,
        ).body_excess_mm
        is None
    )
    wide = [Pad("1", -6.0, 0.0, 3.5, 1.6), Pad("2", 6.0, 0.0, 3.5, 1.6)]
    red = resolve(
        pads,
        "Capacitor_SMD:CP_Elec_4x5.8",
        "ok",
        record.package_name,
        wide,
        record.symbol_pins,
        marks=body,
        kicad_courtyard=courtyard,
    )
    assert red.status == "red" and red.body_excess_mm is None


def test_body_threshold_is_relative_with_clamps():
    """Ten percent of the courtyard's shorter side, never below 0.15 mm nor above 1.5 mm."""
    assert body_threshold_mm((-0.7, -0.35, 0.7, 0.35)) == 0.15
    assert body_threshold_mm((-3.35, -2.4, 3.35, 2.4)) == pytest.approx(0.48)
    assert body_threshold_mm((-1.05, -1.53, 16.29, 49.78)) == 1.5
