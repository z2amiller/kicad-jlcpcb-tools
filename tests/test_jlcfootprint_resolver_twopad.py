"""Tests for the two-pad paths: alignment by terminal meaning, and by axis."""

from jlcfootprint.easyeda_parse import SymbolPin
from jlcfootprint.geometry import Pad
from jlcfootprint.resolver import (
    kicad_reference_pad,
    label_reference_pad,
    resolve,
    token_reference_side,
)

# KiCad Diode_SMD:D_SMF with the schematic's K on pad 1 and A on pad 2.
KICAD_SMF = [
    Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "K"),
    Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "A"),
]
# EasyEDA SMF drawn with the same geometry (JLC zero orientation).
JLC_SMF = [Pad("1", -1.45, 0.0, 1.3, 1.4), Pad("2", 1.45, 0.0, 1.3, 1.4)]
D1_PINS = [SymbolPin("1", "K"), SymbolPin("2", "A")]
D2_PINS = [SymbolPin("1", "A"), SymbolPin("2", "K")]


def test_token_reference_side():
    """FD puts anode/positive on the left; RD the reverse; BI is none; no token is None."""
    assert token_reference_side("SMF_L2.8-W1.8-LS3.7-FD-1", "cathode") == "right"
    assert token_reference_side("SMF_L2.8-W1.8-LS3.7-RD", "cathode") == "left"
    assert token_reference_side("CAP-SMD_BD6.3-L6.6-W6.6-FD", "positive") == "left"
    assert token_reference_side("CAP-SMD_BD6.3-L6.6-W6.6-RD", "positive") == "right"
    assert token_reference_side("SMB_L4.3-W3.6-LS5.4-BI", "cathode") == "none"
    assert token_reference_side("DIO-SMD_L2.8-W1.8", "cathode") is None


def test_kicad_reference_pad_by_function_or_convention():
    """The named terminal wins; the other named terminal implies it; pad 1 is the fallback."""
    assert kicad_reference_pad(KICAD_SMF, "cathode") == (KICAD_SMF[0], False, "")
    only_anode = [Pad("1", -1, 0, 1, 1, 0, "A"), Pad("2", 1, 0, 1, 1)]
    assert kicad_reference_pad(only_anode, "cathode") == (only_anode[1], False, "")
    pad, assumed, note = kicad_reference_pad(JLC_SMF, "cathode")
    assert (pad, assumed, note) == (JLC_SMF[0], True, "assumed KiCad pad 1 = K")
    pad, assumed, note = kicad_reference_pad(
        [Pad("A", 0, 0, 1, 1), Pad("B", 1, 0, 1, 1)], "positive"
    )
    assert (pad, assumed) == (None, True)
    assert "no pad 1" in note


def test_kicad_reference_pad_reads_positive_and_negative_as_anode_and_cathode():
    """An LED symbol labelled +/- names the anode and cathode; both vocabularies match."""
    led = [Pad("1", -1, 0, 1, 1, 0, "-"), Pad("2", 1, 0, 1, 1, 0, "+")]
    assert kicad_reference_pad(led, "cathode", diode=True) == (led[0], False, "")
    cap = [Pad("1", -1, 0, 1, 1, 0, "K"), Pad("2", 1, 0, 1, 1, 0, "A")]
    assert kicad_reference_pad(cap, "positive") == (cap[1], False, "")


def test_contradictory_pin_functions_give_no_reference_pad():
    """Two pads both claiming the cathode cannot be trusted; the caller reports unknown."""
    both_k = [Pad("1", -1, 0, 1, 1, 0, "K"), Pad("2", 1, 0, 1, 1, 0, "K")]
    pad, assumed, note = kicad_reference_pad(both_k, "cathode", diode=True)
    assert pad is None
    assert "contradictory" in note
    verdict = resolve(
        both_k, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert (verdict.status, verdict.rotation) == ("unknown", None)
    assert "contradictory" in verdict.note_text


def test_c_is_a_cathode_only_on_a_diode():
    """``C`` is a collector on a transistor symbol; it names the cathode only for diodes."""
    pads = [Pad("1", -1, 0, 1, 1, 0, "C"), Pad("2", 1, 0, 1, 1, 0, "E")]
    assert kicad_reference_pad(pads, "cathode") == (
        pads[0],
        True,
        "assumed KiCad pad 1 = K",
    )
    assert kicad_reference_pad(pads, "cathode", diode=True) == (pads[0], False, "")


def test_led_with_plus_minus_functions_is_a_diode_read_by_meaning():
    """C84256-style: LED0805-RD, KiCad pads labelled - (pad 1) and + (pad 2).

    The footprint name says diode, so ``-`` is the cathode on pad 1: the same terminal
    JLC's pin 1 = K names, so the rotation is 0.  Reading the part as a capacitor and
    ignoring the functions gave 180 before.
    """
    led = [
        Pad("1", -1.05, 0.0, 1.0, 1.2, 0.0, "-"),
        Pad("2", 1.05, 0.0, 1.0, 1.2, 0.0, "+"),
    ]
    jlc = [Pad("1", -1.05, 0.0, 1.0, 1.2), Pad("2", 1.05, 0.0, 1.0, 1.2)]
    verdict = resolve(
        led, "LED_SMD:LED_0805_2012Metric", "ok", "LED0805-RD", jlc, D1_PINS
    )
    assert (verdict.rotation, verdict.status, verdict.polarity_light) == (
        0,
        "green",
        "green",
    )
    assert verdict.method == "polarity"


def test_led_symbol_labelled_plus_minus_on_the_easyeda_side_is_still_a_diode():
    """EasyEDA symbols for LEDs often say +/-; the KiCad footprint name keeps it a diode."""
    led = [
        Pad("1", -1.05, 0.0, 1.0, 1.2, 0.0, "K"),
        Pad("2", 1.05, 0.0, 1.0, 1.2, 0.0, "A"),
    ]
    jlc = [Pad("1", -1.05, 0.0, 1.0, 1.2), Pad("2", 1.05, 0.0, 1.0, 1.2)]
    plus_minus = [SymbolPin("1", "-"), SymbolPin("2", "+")]
    verdict = resolve(
        led, "LED_SMD:LED_0805_2012Metric", "ok", "LED0805-RD", jlc, plus_minus
    )
    assert (verdict.rotation, verdict.polarity_light) == (0, "green")


def test_two_terminal_part_on_a_three_pad_kicad_footprint_aligns_by_meaning():
    """A KiCad diode footprint with an extra pad still pairs its 1 and 2 by terminal."""
    kicad = [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "K"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "A"),
        Pad("3", 0.0, 1.5, 1.0, 0.5),
    ]
    verdict = resolve(
        kicad, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert (verdict.rotation, verdict.method, verdict.status) == (
        0,
        "polarity",
        "green",
    )
    no_pair = [
        Pad("A", -1.45, 0.0, 1.3, 1.4, 0.0, "K"),
        Pad("B", 1.45, 0.0, 1.3, 1.4, 0.0, "A"),
        Pad("C", 0.0, 1.5, 1.0, 0.5),
    ]
    verdict = resolve(
        no_pair, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert (verdict.status, verdict.rotation) == ("unknown", None)


def test_degenerate_two_pad_geometry_is_unknown():
    """Two pads at the same point cannot define an axis: unknown, not a rotation."""
    stacked = [
        Pad("1", 0.0, 0.0, 1.0, 1.0, 0.0, "K"),
        Pad("2", 0.0, 0.0, 1.0, 1.0, 0.0, "A"),
    ]
    verdict = resolve(
        stacked, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert (verdict.status, verdict.rotation) == ("unknown", None)
    assert "degenerate" in verdict.note_text
    same = resolve(
        stacked, "Resistor_SMD:R_0603_1608Metric", "ok", "R0603", JLC_SMF, []
    )
    assert (same.status, same.rotation) == ("unknown", None)


def test_label_reference_pad():
    """Pin 1 = K means the cathode is pad 1; pin 1 = A means it is the other pad."""
    assert label_reference_pad(JLC_SMF, "K", "cathode") is JLC_SMF[0]
    assert label_reference_pad(JLC_SMF, "A", "cathode") is JLC_SMF[1]
    assert label_reference_pad(JLC_SMF, "A", "positive") is JLC_SMF[0]
    assert label_reference_pad(JLC_SMF, "K", "positive") is JLC_SMF[1]
    assert label_reference_pad(JLC_SMF, None, "cathode") is None


def test_reddit_d1_rd_with_pin1_cathode_is_0_and_green():
    """C1978115: reverse direction, EasyEDA pin 1 = K, same meaning as KiCad's pad 1."""
    verdict = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert verdict.rotation == 0
    assert (verdict.status, verdict.polarity_light) == ("green", "green")
    assert (verdict.method, verdict.confidence) == ("polarity", "high")
    assert verdict.name_rotation == 0
    assert verdict.fit == "fits"


def test_reddit_d2_fd_with_pin1_anode_is_180_and_yellow():
    """C1856655: forward direction, EasyEDA pin 1 = A; a pad-number match would give 0."""
    verdict = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-FD-1", JLC_SMF, D2_PINS
    )
    assert verdict.rotation == 180
    assert (verdict.status, verdict.polarity_light) == ("yellow", "yellow")
    assert "do not renumber" in verdict.note_text
    assert verdict.name_rotation == 180


def test_kicad_footprint_with_anode_on_pad_1_flips_the_answer():
    """The KiCad side is read by meaning too: anode on pad 1 turns D1's 0 into 180."""
    wired_backwards = [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "A"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "K"),
    ]
    verdict = resolve(
        wired_backwards,
        "Diode_SMD:D_SMF",
        "ok",
        "SMF_L2.8-W1.8-LS3.7-RD",
        JLC_SMF,
        D1_PINS,
    )
    assert verdict.rotation == 180
    assert verdict.polarity_light == "yellow"


def test_unnamed_pins_fall_back_to_pad_1_cathode_with_medium_confidence():
    """Without pin functions the KiCad convention pad 1 = K is assumed and said so."""
    verdict = resolve(
        JLC_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-FD-1", JLC_SMF, D2_PINS
    )
    assert verdict.rotation == 180
    assert verdict.confidence == "medium"
    assert any("assumed KiCad pad 1 = K" in note for note in verdict.notes)


def test_polarized_cap_fd_is_0_by_convention_and_rd_is_180():
    """Electrolytics use pad 1 = + on both sides, so FD is 0 and RD is 180."""
    kicad = [Pad("1", -2.7, 0, 3.5, 1.6), Pad("2", 2.7, 0, 3.5, 1.6)]
    jlc = [Pad("1", -2.7, 0, 3.5, 1.6), Pad("2", 2.7, 0, 3.5, 1.6)]
    cap_pins = [SymbolPin("1", "+"), SymbolPin("2", "-")]
    fd = resolve(
        kicad,
        "Capacitor_SMD:CP_Elec_6.3x7.7",
        "ok",
        "CAP-SMD_BD6.3-L6.6-W6.6-FD",
        jlc,
        cap_pins,
    )
    assert (fd.rotation, fd.status, fd.polarity_light, fd.name_rotation) == (
        0,
        "green",
        "green",
        0,
    )
    rd = resolve(
        kicad,
        "Capacitor_SMD:CP_Elec_6.3x7.7",
        "ok",
        "CAP-SMD_BD6.3-L6.6-W6.6-RD",
        jlc,
        [],
    )
    assert (rd.rotation, rd.polarity_light, rd.name_rotation) == (180, "unknown", 180)


def test_inconsistent_token_and_label_is_red():
    """FD says cathode right, the symbol says pin 1 (left) is K: data conflict, no guess."""
    verdict = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-FD", JLC_SMF, D1_PINS
    )
    assert verdict.status == "red"
    assert verdict.rotation is None
    assert "inconsistent" in verdict.note_text


def test_no_polarity_source_is_unknown_never_guessed():
    """No token and no labels: a polarized part is left for the JLC preview."""
    numbered = [SymbolPin("1", "1"), SymbolPin("2", "2")]
    verdict = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "DIO-SMD_L2.8-W1.8", JLC_SMF, numbered
    )
    assert verdict.status == "unknown"
    assert verdict.rotation is None
    assert "polarity unknown" in verdict.note_text


def test_vertical_jlc_drawing_uses_the_label_when_the_token_cannot_apply():
    """A left/right token cannot be read off a vertical drawing; the label still can."""
    vertical = [Pad("1", 0.0, -1.45, 1.4, 1.3), Pad("2", 0.0, 1.45, 1.4, 1.3)]
    verdict = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-FD", vertical, D2_PINS
    )
    assert verdict.rotation == 90  # cathode (pad 2) at the bottom of JLC's drawing
    assert any("vertical" in note for note in verdict.notes)
    cathode_on_top = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", vertical, D1_PINS
    )
    assert cathode_on_top.rotation == 270


def test_bidirectional_token_aligns_by_axis():
    """A -BI TVS has no preferred orientation and resolves like a resistor."""
    verdict = resolve(
        KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-BI", JLC_SMF, []
    )
    assert (verdict.rotation, verdict.method, verdict.status) == (0, "axis", "green")
    assert any("bidirectional" in note for note in verdict.notes)


def test_axis_alignment_for_a_resistor_drawn_vertical():
    """Non-polar parts only need the axis: a vertical JLC drawing is 90, the same drawing 0."""
    kicad = [Pad("1", -0.7875, 0, 0.875, 0.95), Pad("2", 0.7875, 0, 0.875, 0.95)]
    vertical = [
        Pad("1", 0, -0.7875, 0.875, 0.95, 90.0),
        Pad("2", 0, 0.7875, 0.875, 0.95, 90.0),
    ]
    verdict = resolve(
        kicad, "Resistor_SMD:R_0603_1608Metric", "ok", "R0603", vertical, []
    )
    assert (verdict.rotation, verdict.method, verdict.status, verdict.fit) == (
        90,
        "axis",
        "green",
        "fits",
    )
    same = resolve(kicad, "Resistor_SMD:R_0603_1608Metric", "ok", "R0603", kicad, [])
    assert same.rotation == 0


def test_0402_pads_under_an_0603_part_are_tight():
    """JLC's pad centres fall outside the inner 80 percent of the KiCad pads: yellow, tight."""
    kicad_0402 = [Pad("1", -0.51, 0, 0.54, 0.64), Pad("2", 0.51, 0, 0.54, 0.64)]
    jlc_0603 = [Pad("1", -0.7875, 0, 0.875, 0.95), Pad("2", 0.7875, 0, 0.875, 0.95)]
    verdict = resolve(
        kicad_0402, "Resistor_SMD:R_0402_1005Metric", "ok", "R0603", jlc_0603, []
    )
    assert (verdict.status, verdict.fit) == ("yellow", "fits_tight")
    assert verdict.rotation == 0


def test_led_family_with_plus_minus_labels_and_no_kicad_evidence_is_a_diode():
    """An LED package whose symbol says +/- on a custom-named footprint without functions.

    Reading it as a capacitor assumed KiCad pad 1 = + and gave 180 on a part at 0; the
    package family outranks the symbol labels.
    """
    plain = [Pad("1", -1.05, 0.0, 1.0, 1.2), Pad("2", 1.05, 0.0, 1.0, 1.2)]
    jlc = [Pad("1", -1.05, 0.0, 1.0, 1.2), Pad("2", 1.05, 0.0, 1.0, 1.2)]
    plus_minus = [SymbolPin("1", "-"), SymbolPin("2", "+")]
    verdict = resolve(plain, "Custom:LED0805-RD", "ok", "LED0805-RD", jlc, plus_minus)
    assert (verdict.rotation, verdict.status, verdict.polarity_light) == (
        0,
        "green",
        "green",
    )
    assert "assumed KiCad pad 1 = K" in verdict.note_text


def test_pin1_light_reads_the_other_pads_function():
    """Pad 1 unnamed and pad 2 = K: pad 1 is the anode, so JLC's K-on-pin-1 lands on it (yellow)."""
    half = [Pad("1", -1.45, 0.0, 1.3, 1.4), Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "K")]
    verdict = resolve(
        half, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert (verdict.rotation, verdict.polarity_light, verdict.status) == (
        180,
        "yellow",
        "yellow",
    )


def test_switch_a_b_functions_do_not_make_a_diode():
    """KiCad's SW_SPST names its pins A and B; the part stays non-polar and resolves by axis."""
    switch = [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "A"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "B"),
    ]
    numbered = [SymbolPin("1", "1"), SymbolPin("2", "2")]
    verdict = resolve(
        switch, "Switch:SW_SPST", "ok", "SW-SMD_2P-L3.0-W2.0", JLC_SMF, numbered
    )
    assert (verdict.method, verdict.rotation, verdict.status) == ("axis", 0, "green")


def test_pos_neg_functions_name_a_capacitor():
    """POS/NEG are capacitor words on both sides: polar_cap, FD is 0 and the name agrees."""
    cap = [Pad("1", -2.7, 0, 3.5, 1.6, 0, "POS"), Pad("2", 2.7, 0, 3.5, 1.6, 0, "NEG")]
    jlc = [Pad("1", -2.7, 0, 3.5, 1.6), Pad("2", 2.7, 0, 3.5, 1.6)]
    verdict = resolve(cap, "Custom:Cap", "ok", "CAP-SMD_BD6.3-L6.6-W6.6-FD", jlc, [])
    assert (verdict.rotation, verdict.name_rotation, verdict.confidence) == (
        0,
        0,
        "high",
    )


def test_non_finite_pad_rotation_is_unknown():
    """A pad angle of infinity or NaN short-circuits like a bad coordinate would."""
    for bad in (float("inf"), float("nan")):
        kicad = [KICAD_SMF[0]._replace(rotation=bad), KICAD_SMF[1]]
        verdict = resolve(
            kicad, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
        )
        assert (verdict.status, verdict.rotation) == ("unknown", None)
        jlc = [JLC_SMF[0]._replace(rotation=bad), JLC_SMF[1]]
        verdict = resolve(
            KICAD_SMF, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", jlc, D1_PINS
        )
        assert (verdict.status, verdict.rotation) == ("unknown", None)


def test_three_pad_footprint_pitch_failure_is_reported_as_pitch():
    """The note follows the fit decision: a two-terminal part missing its pads is pitch, not count."""
    kicad = [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "K"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "A"),
        Pad("3", 0.0, 1.5, 1.0, 0.5),
    ]
    wide = [Pad("1", -2.6, 0.0, 1.3, 1.4), Pad("2", 2.6, 0.0, 1.3, 1.4)]
    verdict = resolve(
        kicad, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", wide, D1_PINS
    )
    assert (verdict.status, verdict.fit) == ("red", "pitch")
    assert "does not fit: pitch" in verdict.note_text


def test_seeded_label_deciding_alone_is_medium_confidence():
    """No token and a seeded per-footprint polarity: the seed decides, and says so, at medium."""
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "DIO-SMD_L2.8-W1.8",
        JLC_SMF,
        D2_PINS,
        "seed-puuid",
    )
    assert (verdict.rotation, verdict.confidence) == (180, "medium")
    assert "seeded per-footprint polarity" in verdict.note_text


def test_polarized_result_contradicting_the_name_is_medium_with_a_note():
    """Anode on KiCad pad 1 gives 180 where the name says 0: right, but flagged."""
    wired_backwards = [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "A"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "K"),
    ]
    verdict = resolve(
        wired_backwards,
        "Diode_SMD:D_SMF",
        "ok",
        "SMF_L2.8-W1.8-LS3.7-RD",
        JLC_SMF,
        D1_PINS,
    )
    assert (verdict.rotation, verdict.confidence) == (180, "medium")
    assert "name says 0°" in verdict.note_text


def test_coincident_jlc_pads_are_degenerate_not_vertical():
    """Two JLC pads at one point: no axis, no token side, and the note says degenerate."""
    coincident = [Pad("1", 0.0, 0.0, 1.3, 1.4), Pad("2", 0.0, 0.0, 1.3, 1.4)]
    verdict = resolve(
        KICAD_SMF,
        "Diode_SMD:D_SMF",
        "ok",
        "SMF_L2.8-W1.8-LS3.7-RD",
        coincident,
        D1_PINS,
    )
    assert (verdict.status, verdict.rotation) == ("unknown", None)
    assert verdict.note_text == "pad geometry is degenerate"


def test_bidirectional_tvs_with_d_tvs_pin_names_aligns_by_axis():
    """KiCad's D_TVS symbol names both pins A1/A2; a -BI part must not read them as two anodes."""
    tvs = [
        Pad("1", -1.45, 0.0, 1.3, 1.4, 0.0, "A1"),
        Pad("2", 1.45, 0.0, 1.3, 1.4, 0.0, "A2"),
    ]
    verdict = resolve(
        tvs, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-BI", JLC_SMF, D1_PINS
    )
    assert (verdict.method, verdict.rotation, verdict.status) == ("axis", 0, "green")
    assert "bidirectional" in verdict.note_text
    polarized = resolve(
        tvs, "Diode_SMD:D_SMF", "ok", "SMF_L2.8-W1.8-LS3.7-RD", JLC_SMF, D1_PINS
    )
    assert polarized.status == "unknown"
    assert "contradictory" in polarized.note_text
