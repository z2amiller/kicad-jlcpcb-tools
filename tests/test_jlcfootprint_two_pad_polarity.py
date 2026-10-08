"""Tests for spec 19: two-pad polarity read from numbered signs, families and marks, and the pad shapes."""

import pytest

from jlcfootprint.drawing import DrawingMarks
from jlcfootprint.easyeda_parse import (
    DeviceHit,
    SymbolPin,
    assemble_record,
    parse_puuid_response,
    parse_symbol_response,
)
from jlcfootprint.geometry import Pad, pad_hash
from jlcfootprint.kicad_adapter import verdict_key
from jlcfootprint.polarity import (
    is_no_function,
    normalise_function,
    part_kind,
    terminal_of,
)
from jlcfootprint.resolver import YELLOW_NOTE, resolve
from jlcfootprint.resolving import resolve_record

from .jlcfootprint_support import recorded_document

# BT1 on Andy's ocn-LCD board: a LianXin BS-CR2032-8 coin-cell holder (C7498149) on
# LowPower:BatteryHolder_LianXin_BS-CR2032-8_1x2032, copied from the board file with
# the pin functions KiCad stores for Device:Battery_Cell (``+`` = 1, ``-`` = 2).
BT1_FOOTPRINT = "LowPower:BatteryHolder_LianXin_BS-CR2032-8_1x2032"
BT1_PADS = [
    Pad("1", -12.0, 0.0, 4.1, 3.6, 0.0, "+_1", "rect"),
    Pad("2", 11.05, 0.0, 5.8, 2.6, 0.0, "-_2", "rect"),
]
BT1_COURTYARD = (-14.3, -10.5, 14.2, 10.5)
# The Pro host's uuids for C7498149, as the plugin's batch lookup returned them.
BT1_HIT = DeviceHit(
    "C7498149",
    "b13f7e1c7c93480e8dc6c46a56edb748",
    "4286f85526b2436ca287dbf50f305a73",
    "BAT-SMD_BS-CR2032-8",
)
# A two-pad part with nothing on either side naming a terminal: plain pads, a symbol
# numbering its pins 1 and 2, a footprint and a package that say nothing.
PLAIN_KICAD = [Pad("1", -2.0, 0.0, 1.5, 1.5), Pad("2", 2.0, 0.0, 1.5, 1.5)]
PLAIN_JLC = [Pad("1", -2.0, 0.0, 1.5, 1.5), Pad("2", 2.0, 0.0, 1.5, 1.5)]
NUMBERED_PINS = [SymbolPin("1", "1"), SymbolPin("2", "2")]


def bt1_record():
    """Assemble C7498149 from its recorded Pro footprint and symbol, as the cache holds it."""
    footprint = parse_puuid_response(
        recorded_document("footprint", BT1_HIT.puuid), BT1_HIT.puuid
    )
    symbol = parse_symbol_response(
        recorded_document("symbol", BT1_HIT.symbol_uuid), BT1_HIT.symbol_uuid
    )
    return assemble_record(BT1_HIT.lcsc, BT1_HIT, footprint, symbol)


def test_a_sign_with_kicads_pin_number_suffix_reads_as_the_sign():
    """``+_1`` and ``-_2`` read as ``+`` and ``-`` the way ``K_1`` reads as ``K`` (spec 19.3)."""
    assert normalise_function("+_1") == "+"
    assert normalise_function("-_2") == "-"
    assert normalise_function(" +_12 ") == "+"
    assert not is_no_function("+_1")
    assert terminal_of(Pad("1", 0, 0, 1, 1, 0, "+_1")) == "positive"
    assert terminal_of(Pad("2", 1, 0, 1, 1, 0, "-_2")) == "negative"


def test_a_supply_pin_named_v_plus_still_names_no_terminal():
    """Only a whole numbered sign is a sign: ``V+_8``, ``+5V_1`` and ``+_`` name no terminal."""
    assert normalise_function("V+_8") == "V"
    assert terminal_of(Pad("8", 0, 0, 1, 1, 0, "V+_8")) == ""
    assert terminal_of(Pad("4", 0, 0, 1, 1, 0, "V-_4")) == ""
    assert terminal_of(Pad("1", 0, 0, 1, 1, 0, "+5V_1")) == ""
    assert terminal_of(Pad("1", 0, 0, 1, 1, 0, "+_")) == ""


def test_swapped_numbered_signs_are_stored_under_different_keys():
    """The key reads ``+_1`` as ``+``: swapped signs on the same pads need opposite rotations.

    This is the K/A protection of spec 5.3 (corner-case D4/D5 share one LCSC on one
    footprint with swapped functions) for numbered signs: a battery with ``+`` on
    pad 1 and one with ``+`` on pad 2 must not share a stored verdict, and neither
    may share the key of the bare pads.
    """
    plus_first = [
        Pad("1", -1, 0, 1, 0.5, 0, "+_1"),
        Pad("2", 1, 0, 1, 0.5, 0, "-_2"),
    ]
    minus_first = [
        Pad("1", -1, 0, 1, 0.5, 0, "-_1"),
        Pad("2", 1, 0, 1, 0.5, 0, "+_2"),
    ]
    assert verdict_key(plus_first) != verdict_key(minus_first)
    assert verdict_key(plus_first) != pad_hash(plus_first)


def test_bt1_coin_cell_holder_resolves_yellow_at_180_from_the_footprint_mark():
    """The false green of spec 19.1: BT1 is yellow at 180 degrees, its polarity from JLC's ``+``."""
    verdict = resolve_record(
        BT1_PADS, BT1_FOOTPRINT, bt1_record(), "symbol", BT1_COURTYARD
    )
    assert (verdict.status, verdict.rotation, verdict.method) == (
        "yellow",
        180,
        "polarity",
    )
    assert verdict.confidence == "medium"
    assert verdict.fit == "fits"
    assert round(verdict.overlap_min, 3) == 0.947
    assert verdict.polarity_source == "drawing"
    assert verdict.polarity_light == "yellow"
    assert verdict.notes == ["polarity from the footprint's + mark", YELLOW_NOTE]


@pytest.mark.parametrize(
    "footprint",
    [
        "Battery:BatteryHolder_Keystone_3034_1x20mm",
        "Battery:Battery_CR1225",
        "LowPower:BatteryHolder_LianXin_BS-CR2032-8_1x2032",
        "Buzzer_Beeper:Buzzer_12x9.5RM7.6",
        "Buzzer_Beeper:MagneticBuzzer_CUI_CMT-8504-100-SMT",
    ],
)
def test_battery_and_buzzer_footprints_are_polarized_on_the_positive_terminal(
    footprint,
):
    """KiCad's Battery..., Buzzer_... and MagneticBuzzer_... footprints are ``polar`` (spec 19.3)."""
    assert part_kind("X", footprint, PLAIN_KICAD, NUMBERED_PINS) == "polar"


@pytest.mark.parametrize(
    "package",
    [
        "BAT-SMD_CR1220-2ZX",
        "BAT-TH_BS-2-1",
        "BATT-TH_MY-2032-02",
        "BATTERY-SMD_18650-1S-L77.1-W20.7-1",
        "BUZ-SMD_L5.0-W5.5-P4.60",
        "BUZ-TH_BD12.0-P7.60-D0.6-FD",
        "BUZZ-SMD_FUET-3020",
        "BUZZER-SMD_KMTG1740D",
        "BEEP-TH_BD12.0-P7.60-D0.6",
        "MIC-SMD_BD4.0-4013_SMDA",
        "MIC-TH_BD6.0-P2.00",
    ],
)
def test_battery_buzzer_and_microphone_families_are_polarized(package):
    """Every crawl spelling of the three families classifies as ``polar`` (spec 19.3)."""
    assert part_kind(package, "Custom:Part", PLAIN_KICAD, NUMBERED_PINS) == "polar"


@pytest.mark.parametrize(
    "package", ["MICROSMP_L2.2-W1.3-LS2.5-RD", "MICRO-MELF_L2.0-W1.2-RD"]
)
def test_microsmp_and_micro_melf_are_not_microphones(package):
    """``MIC-`` keeps its hyphen: the diode packages MICROSMP and MICRO-MELF never match."""
    assert part_kind(package, "Custom:Part", PLAIN_KICAD, NUMBERED_PINS) == "other"


def test_the_families_rank_after_the_capacitor_names_and_before_the_symbol_labels():
    """A CP_ footprint stays a capacitor; a battery whose symbol says A/K is not a diode."""
    assert (
        part_kind(
            "MIC-TH_BD6.0-P2.00",
            "Capacitor_THT:CP_Radial_D5.0mm_P2.00mm",
            PLAIN_KICAD,
            [],
        )
        == "polar_cap"
    )
    labelled = [SymbolPin("1", "A"), SymbolPin("2", "K")]
    assert part_kind("BAT-TH_BS-2-1", "Custom:Part", PLAIN_KICAD, labelled) == "polar"


@pytest.mark.parametrize(
    "marks",
    [DrawingMarks(positive_pad="2"), DrawingMarks(positive_pin="2")],
    ids=["footprint_mark", "symbol_mark"],
)
def test_a_drawn_plus_sends_an_unclassified_two_pad_part_to_the_meaning_path(marks):
    """JLC's ``+`` on pin 2 turns the part 180 degrees with KiCad pad 1 assumed ``+`` (spec 19.2, decision 1)."""
    assert (
        part_kind("CONN-TH_2P", "Custom:Part", PLAIN_KICAD, NUMBERED_PINS, marks)
        == "polar"
    )
    verdict = resolve(
        PLAIN_KICAD,
        "Custom:Part",
        "ok",
        "CONN-TH_2P",
        PLAIN_JLC,
        NUMBERED_PINS,
        marks=marks,
    )
    assert (verdict.status, verdict.rotation, verdict.method) == (
        "yellow",
        180,
        "polarity",
    )
    assert verdict.confidence == "medium"
    assert verdict.polarity_source == "drawing"
    assert verdict.notes[0] == "assumed KiCad pad 1 = +"
    assert not verdict.non_polar


def test_without_a_drawn_plus_the_same_part_keeps_the_axis_rule():
    """No mark, no token, no family: the part is non-polar and 180 degrees stays irrelevant."""
    assert part_kind("CONN-TH_2P", "Custom:Part", PLAIN_KICAD, NUMBERED_PINS) == "other"
    verdict = resolve(
        PLAIN_KICAD,
        "Custom:Part",
        "ok",
        "CONN-TH_2P",
        PLAIN_JLC,
        NUMBERED_PINS,
        marks=DrawingMarks(),
    )
    assert (verdict.status, verdict.rotation, verdict.method) == ("green", 0, "axis")
    assert verdict.non_polar


def test_a_polarized_family_part_with_no_polarity_source_is_unknown():
    """A battery JLC marks nowhere gets "polarity unknown" and the raw angle, never a green (decision 4)."""
    verdict = resolve(
        PLAIN_KICAD,
        "Custom:Part",
        "ok",
        "BAT-TH_BS-2-1",
        PLAIN_JLC,
        NUMBERED_PINS,
        marks=DrawingMarks(),
    )
    assert (verdict.status, verdict.rotation, verdict.method) == (
        "unknown",
        None,
        "none",
    )
    assert "polarity unknown; check in JLC preview" in verdict.notes
