"""Tests for the JLC column's state, order, glyphs and hover text (spec 16.3), with no wx."""

import pytest

from jlcfootprint.controller import Decision, FetchState
from jlcfootprint.presentation import (
    GLYPHS,
    SORT_ORDER,
    banner,
    describe_seconds,
    dialog_title,
    fit_numbers,
    glyph,
    jlc_facts,
    jlc_state,
    kicad_facts,
    parse_override,
    sort_rank,
    verdict_text,
)
from jlcfootprint.verdicts import StoredVerdict


def decision(reference="C1", lcsc="C2132", **fields):
    """Return a CPL decision with a stored verdict carrying the given fields."""
    verdict_fields = {
        key: fields.pop(key)
        for key in list(fields)
        if key
        in {
            "status",
            "rotation",
            "method",
            "confidence",
            "fit",
            "polarity_light",
            "override_rotation",
            "override_note",
            "notes",
            "body_excess_mm",
            "footprint_name",
        }
    }
    stored = StoredVerdict("C2132", "hash", **verdict_fields)
    return Decision(
        reference,
        lcsc,
        rotation=stored.emitted_rotation,
        source=stored.source,
        status=stored.status,
        polarity_light=stored.polarity_light,
        fit=stored.fit,
        note=stored.override_note or stored.notes or "",
        body_excess=stored.body_excess_mm,
        verdict=stored,
        **fields,
    )


def test_every_state_of_the_spec_table_has_its_glyph_and_its_place_in_the_order():
    """Spec 16.3's glyph table and its ascending order: ✗, !, ?, ‖, ◷, green ✓, blue ✓, blank."""
    assert [glyph(state) for state in SORT_ORDER] == [
        "✗",
        "!",
        "!",
        "?",
        "‖",
        "◷",
        "✓",
        "✓",
        "",
    ]
    assert set(GLYPHS) == set(SORT_ORDER)
    assert sorted(SORT_ORDER, key=sort_rank) == list(SORT_ORDER)
    assert sort_rank("something else") == sort_rank("")


@pytest.mark.parametrize(
    ("fields", "fetch", "state"),
    [
        ({"status": "green", "rotation": 180}, None, "green"),
        (
            {"status": "green", "rotation": 0, "body_excess_mm": 0.9},
            None,
            "caveat",
        ),
        (
            {"status": "yellow", "rotation": 0, "polarity_light": "yellow"},
            None,
            "yellow",
        ),
        ({"status": "red", "fit": "pitch"}, None, "red"),
        ({"status": "unknown"}, None, "unknown"),
        ({"status": "red", "override_rotation": 270}, None, "override"),
        ({"status": "pending"}, None, "pending"),
        ({"status": "green", "rotation": 0}, FetchState("queued", ahead=3), "pending"),
        (
            {"status": "green", "rotation": 0},
            FetchState("fetching", kind="symbol"),
            "pending",
        ),
        (
            {"status": "green", "rotation": 0},
            FetchState("paused", seconds=60.0),
            "paused",
        ),
        ({"status": "green", "rotation": 0}, FetchState("tripped"), "paused"),
        (
            {"status": "green", "rotation": 0, "override_rotation": 90},
            FetchState("queued"),
            "pending",
        ),
    ],
)
def test_the_state_of_each_row_of_the_table(fields, fetch, state):
    """Each row of spec 16.3's table, including a re-fetch of an overridden part."""
    pending = fields.get("status") == "pending"
    assert jlc_state(decision(pending=pending, **fields), fetch) == state


def test_a_part_without_an_lcsc_or_without_a_verdict_row():
    """No LCSC is blank; a part the check has not seen has no state at all."""
    assert jlc_state(Decision("R1", "", status="no-lcsc")) == ""
    assert jlc_state(None) == ""
    assert jlc_state(Decision("R2", "C1", status="no-verdict")) == "unknown"


def test_the_green_help_names_the_rotation_its_source_and_both_packages():
    """Spec 16.3's green example: fit, rotation, method, confidence, then the two names."""
    text = verdict_text(
        decision(
            status="green",
            rotation=180,
            method="geometry",
            confidence="high",
            fit="fits",
        ),
        FetchState(),
        kicad_footprint="CP_EIA-2012-15_AVX-P",
        jlc_package="CAP-SMD_L2.0-W1.4-FD",
    )
    assert text == (
        "Fits; rotation 180° derived from pad geometry (high). "
        "JLC CAP-SMD_L2.0-W1.4-FD on CP_EIA-2012-15_AVX-P."
    )


def test_the_yellow_help_explains_the_pin_1_marker_without_blaming_the_footprint():
    """Spec 16.3's yellow example, with no method claim: the numbering differs, not the angle."""
    text = verdict_text(
        decision(
            status="yellow",
            rotation=0,
            method="polarity",
            confidence="high",
            fit="fits",
            polarity_light="yellow",
        )
    )
    assert text == (
        "Fits; rotation 0°. JLC's pin-1 marker will sit on the other terminal: "
        "a numbering difference, not a rotation error. Do not renumber the footprint."
    )


def test_the_caveated_help_quotes_the_body_excess_and_sends_the_user_to_the_preview():
    """The body caveat is a sentence of its own, after the fit and before the names."""
    text = verdict_text(
        decision(
            status="green",
            rotation=0,
            method="polarity",
            confidence="medium",
            fit="fits",
            body_excess_mm=0.9,
        ),
        jlc_package="CAP-SMD_BD4.0-L4.3-W4.3-FD",
    )
    assert "Fits; rotation 0° derived from terminal polarity (medium)." in text
    assert (
        "The JLC body is 0.9 mm larger than the KiCad courtyard on one side; "
        "check the preview." in text
    )


def test_the_red_help_names_the_reason_the_note_and_the_raw_angle():
    """Spec 16.3's red example: the reason, the note, the names and what the CPL does."""
    text = verdict_text(
        decision(
            status="red",
            fit="pitch",
            notes="does not fit: pitch (a JLC pad misses its KiCad pad; worst overlap 0%)",
        ),
        kicad_footprint="C_Rect_L7.2mm_W3.0mm_P5.00mm",
        jlc_package="CAP-TH_L18.0-W7.5-P15.00-D0.8",
    )
    assert text == (
        "Does not fit: pitch (a JLC pad misses its KiCad pad; worst overlap 0%). "
        "JLC CAP-TH_L18.0-W7.5-P15.00-D0.8 on C_Rect_L7.2mm_W3.0mm_P5.00mm. "
        "The CPL emits the raw angle."
    )
    # A note that does not open with the grade (the numbering finding) gets it prefixed.
    numbering = verdict_text(
        decision(
            status="red",
            fit="numbering",
            notes="pin numbering differs from JLC's part; the package itself aligns at 270°",
        )
    )
    assert numbering.startswith(
        "Does not fit: numbering. Pin numbering differs from JLC's part;"
    )
    assert verdict_text(decision(status="red", fit="count")).startswith(
        "Does not fit: pad count."
    )


def test_the_unknown_help_is_the_note_and_the_raw_angle():
    """An unresolved part quotes its own note; a part with no note says it is unchecked."""
    text = verdict_text(
        decision(
            status="unknown",
            fit="no_data",
            notes="polarity unknown; check in JLC preview",
        )
    )
    assert text == (
        "Polarity unknown; check in JLC preview. The CPL emits the raw angle."
    )
    assert verdict_text(Decision("R1", "C9", status="no-verdict")) == (
        "Not checked against JLC yet. The CPL emits the raw angle."
    )
    assert verdict_text(Decision("R1", "", status="no-lcsc")) == (
        "No LCSC number assigned. The CPL emits the raw angle."
    )
    assert verdict_text(None) == ""


def test_the_override_help_says_who_set_it_and_what_was_derived():
    """Spec 16.3's override example: the value, the user's note, the derived value."""
    text = verdict_text(
        decision(
            status="green",
            rotation=0,
            override_rotation=180,
            override_note="JLC's preview needed it",
        )
    )
    assert text == (
        "Override 180° set by you. JLC's preview needed it. Derived: 0° (green)."
    )
    bare = verdict_text(decision(status="red", override_rotation=90))
    assert bare == "Override 90° set by you. Derived: none (red)."


def test_the_queue_states_speak_for_themselves():
    """Queued, fetching, paused and tripped replace the verdict text while they last."""
    green = decision(status="green", rotation=0, method="geometry", confidence="high")
    assert verdict_text(green, FetchState("queued", ahead=12, seconds=20.0)) == (
        "Queued for EasyEDA, 12 request(s) ahead, about 20 s."
    )
    assert verdict_text(green, FetchState("queued", ahead=60, seconds=150.0)).endswith(
        "about 2 min."
    )
    assert verdict_text(green, FetchState("fetching", kind="footprint")) == (
        "Fetching the footprint…"
    )
    assert verdict_text(green, FetchState("fetching", kind="symbol")) == (
        "Fetching the symbol…"
    )
    assert verdict_text(green, FetchState("fetching", kind="lookup")) == (
        "Looking up the part at EasyEDA…"
    )
    assert verdict_text(green, FetchState("paused", seconds=59.6, queued_parts=12)) == (
        "EasyEDA asked us to wait 60 s; 12 part(s) queued."
    )
    assert verdict_text(green, FetchState("tripped", queued_parts=12)) == (
        "Paused after three failed requests; reopen the plugin to retry."
    )
    waiting = Decision("Q9", "C9", status="pending", pending=True)
    assert verdict_text(waiting, FetchState()) == (
        "Waiting for EasyEDA data; the Rotation column shows what the CPL emits."
    )


def test_describe_seconds_is_the_controllers_wording():
    """Seconds under a minute, else whole minutes."""
    assert (describe_seconds(0.4), describe_seconds(59.4), describe_seconds(90.0)) == (
        "0 s",
        "59 s",
        "2 min",
    )


# ---------------------------------------------------------------------------
# The detail dialog's text (spec 16.4)
# ---------------------------------------------------------------------------


def part_detail(**fields):
    """Return a part detail with an 0805 footprint and a resolved verdict."""
    from jlcfootprint.controller import PartDetail
    from jlcfootprint.drawing import DrawingMarks
    from jlcfootprint.easyeda_parse import SymbolPin
    from jlcfootprint.geometry import Pad
    from jlcfootprint.resolver import resolve

    kicad = [
        Pad("1", -1.0, 0.0, 1.2, 1.4, 0.0, "+"),
        Pad("2", 1.0, 0.0, 1.2, 1.4, 0.0, "-"),
    ]
    jlc = [Pad("1", -1.0, 0.0, 1.3, 1.5), Pad("2", 1.0, 0.0, 1.3, 1.5)]
    marks = DrawingMarks(
        positive_pin="1", positive_pad="1", body_box=(-1, -0.7, 1, 0.7)
    )
    fields.setdefault(
        "verdict",
        resolve(
            kicad,
            "Capacitor_SMD:C_0805_2012Metric",
            "ok",
            "CAP-SMD_L2.0-W1.3-FD",
            jlc,
            [SymbolPin("1", "1"), SymbolPin("2", "2")],
            marks=marks,
        ),
    )
    fields.setdefault("kicad_pads", kicad)
    fields.setdefault("jlc_pads", jlc)
    fields.setdefault("marks", marks)
    fields.setdefault("symbol_pins", [SymbolPin("1", "1"), SymbolPin("2", "2")])
    fields.setdefault("kicad_footprint", "Capacitor_SMD:C_0805_2012Metric")
    fields.setdefault("package_name", "CAP-SMD_L2.0-W1.3-FD")
    fields.setdefault("puuid", "b3b82869fa924bae820e3a6cfb44d689")
    fields.setdefault("source", "live")
    fields.setdefault("fetched_at", 1_757_000_000)
    fields.setdefault("courtyard", (-1.7, -0.95, 1.7, 0.95))
    return PartDetail("C1", fields.pop("lcsc", "C7192"), **fields)


def test_the_dialog_title_is_the_reference_and_the_lcsc():
    """Spec 16.4's title, and just the reference for a part with no LCSC."""
    assert dialog_title(part_detail()) == "C1 · C7192"
    assert dialog_title(part_detail(lcsc="")) == "C1"


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("180", 180),
        (" 180° ", 180),
        ("+90", 90),
        ("-90", 270),
        ("360", 0),
        ("450", 90),
        ("", None),
        ("ninety", None),
        ("90.5", None),
    ],
)
def test_an_override_field_takes_whole_degrees_only(text, value):
    """Any integer is accepted modulo 360; anything else is refused, not guessed."""
    assert parse_override(text) == value


def test_the_kicad_column_describes_the_footprint_as_the_board_has_it():
    """Spec 16.4's left column, including the pin functions and the courtyard."""
    facts = dict(kicad_facts(part_detail(placed_rotation=90.0, is_bottom=True)))
    assert facts["Footprint"] == "Capacitor_SMD:C_0805_2012Metric"
    assert facts["Pads"] == "2 terminal(s), 2 pad(s)"
    assert facts["Pitch"] == "2.00 mm"
    assert facts["Pin 1"] == "-1.00, +0.00 mm in the footprint frame"
    assert facts["Pin functions"] == "1 = +, 2 = -"
    assert facts["Raw angle"] == "90°"
    assert facts["Side"] == "bottom"
    assert facts["Courtyard"] == "3.40 x 1.90 mm"
    bare = dict(kicad_facts(part_detail(kicad_pads=[], courtyard=None)))
    assert (bare["Pads"], bare["Pitch"], bare["Pin 1"], bare["Courtyard"]) == (
        "none",
        "unknown",
        "no pad 1",
        "none",
    )
    assert bare["Pin functions"] == "none in the schematic"


def test_the_jlc_column_describes_the_drawing_and_the_symbol():
    """Spec 16.4's right column: package, puuid, pads, pitch, token, polarity, marks, when."""
    detail = part_detail()
    detail.stored = StoredVerdict(
        "C7192",
        "hash",
        status="green",
        rotation=0,
        method="polarity",
        confidence="high",
        name_rotation=0,
    )
    facts = dict(jlc_facts(detail))
    assert facts["Package"] == "CAP-SMD_L2.0-W1.3-FD"
    assert facts["puuid"] == "b3b82869fa924bae820e3a6cfb44d689"
    assert facts["Pads"] == "2 terminal(s), 2 pad(s)"
    assert facts["Pitch"] == "2.00 mm"
    assert facts["Name rotation"] == "0°"
    assert facts["Confidence"] == "high (polarity)"
    assert facts["Polarity"] == "token"
    assert facts["Marks"] == "symbol + on pin 1, footprint + on pad 1, body box"
    assert facts["Symbol pins"] == "1 = 1, 2 = 2"
    assert facts["Fetched"].endswith("(live)")
    empty = dict(
        jlc_facts(
            part_detail(
                package_name="",
                puuid="",
                jlc_pads=[],
                marks=None,
                symbol_pins=[],
                source="",
                fetched_at=0,
                verdict=None,
            )
        )
    )
    assert empty["Package"] == "not fetched yet"
    assert empty["Fetched"] == "not fetched yet"
    assert empty["Marks"] == "none"
    assert empty["Name rotation"] == "no orientation token"


def test_the_fit_numbers_under_the_canvas():
    """Spec 16.4: pads, overlap min and mean, residual, angular rms, and the caveat."""
    numbers = dict(fit_numbers(part_detail()))
    assert numbers["Pads KiCad/JLC"] == "2 / 2"
    assert numbers["Overlap min"].endswith("%")
    assert numbers["Residual"].endswith("mm")
    assert numbers["Angular rms"].endswith("°")
    assert numbers["Body excess"] == "none"
    caveated = part_detail()
    caveated.verdict.body_excess_mm = 0.9
    assert dict(fit_numbers(caveated))["Body excess"] == "0.90 mm"
    nothing = dict(fit_numbers(part_detail(verdict=None)))
    assert nothing["Pads KiCad/JLC"] == "unknown"


def test_the_banner_says_the_state_the_verdict_and_what_the_cpl_emits():
    """Spec 16.4's banner: the tint's state, the hover's text and the CPL sentence."""
    detail = part_detail(placed_rotation=90.0)
    detail.stored = StoredVerdict(
        "C7192",
        "hash",
        status="green",
        rotation=180,
        method="polarity",
        confidence="high",
    )
    detail.decision = Decision(
        "C1",
        "C7192",
        rotation=180,
        source="derived",
        status="green",
        fit="fits",
        verdict=detail.stored,
    )
    state, text, cpl = banner(detail)
    assert state == "green"
    assert text.startswith("Fits; rotation 180° derived from terminal polarity (high).")
    assert cpl == "The CPL emits 270°: the raw 90° with +180° applied (derived)."
    raw = part_detail(placed_rotation=90.0)
    raw.decision = Decision("C1", "C7192", status="unknown", note="polarity unknown")
    assert banner(raw)[2] == "The CPL emits the raw angle, 90°."
    assert banner(part_detail(lcsc="", placed_rotation=0.0))[2] == (
        "The CPL emits the raw angle, 0°."
    )
