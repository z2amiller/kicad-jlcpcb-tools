"""Tests for spec 19: two-pad polarity read from numbered signs, families and marks, and the pad shapes."""

from jlcfootprint.easyeda_parse import (
    DeviceHit,
    assemble_record,
    parse_puuid_response,
    parse_symbol_response,
)
from jlcfootprint.geometry import Pad, pad_hash
from jlcfootprint.kicad_adapter import verdict_key
from jlcfootprint.polarity import is_no_function, normalise_function, terminal_of
from jlcfootprint.resolver import YELLOW_NOTE
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
