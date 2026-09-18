"""Tests for the JLC column's state, order, glyphs and hover text (spec 16.3), with no wx."""

import pytest

from jlcfootprint.controller import Decision, FetchState
from jlcfootprint.presentation import (
    GLYPHS,
    SORT_ORDER,
    describe_seconds,
    glyph,
    jlc_state,
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
