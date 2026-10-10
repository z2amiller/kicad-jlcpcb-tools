"""What the variant matrix's JLC and Corr. cells say for one part (spec 18.5).

The JLC cell is the ordinary list's for the part the check judges: the same state,
glyph and hover.  The Corr. cell shows what the CPL emits in the Rotation column's
wording, and its hover names the source and what the correction rule would have
done.  Pure: a real check over a fake client, and the decision doubles of 16.3.
"""

from types import SimpleNamespace

import pytest

from jlcfootprint.model import Decision
from jlcfootprint.presentation import (
    BLANK,
    cell_help,
    glyph_state,
    matrix_cell,
    rotation_help,
)
from jlcfootprint.verdicts import StoredVerdict
from scripts.kicad_library import footprints_available

from .jlcfootprint_support import controller_setup

RULE = SimpleNamespace(
    rotation=90, offset_x=0.0, offset_y=0.0, source="fpt", status="complete"
)


def _decision(**fields):
    """Return a decision for R1 carrying C1 unless told otherwise."""
    values = {"reference": "R1", "lcsc": "C1"}
    values.update(fields)
    return Decision(**values)


@pytest.mark.skipif(
    not footprints_available(), reason="KiCad library footprints unavailable"
)
def test_a_matrix_cell_is_the_ordinary_list_s_state_hover_and_rotation(tmp_path):
    """Pending, then resolved: the cell follows ``glyph_state`` and ``cell_help``."""
    check, _board, _events, _messages, _client = controller_setup(tmp_path)
    check.scan_board()
    for reference in ("Q1", "D1", "R1"):
        cell = matrix_cell(check, reference, RULE)
        assert cell.state == glyph_state(check, reference)
        assert cell.help == cell_help(check, reference)
        assert cell.rotation_text == "raw"
    check.worker.run_pending()
    q1 = matrix_cell(check, "Q1", RULE)
    assert (q1.lcsc, q1.state, q1.glyph, q1.rotation) == ("C2132", "green", "✓", 180)
    assert (q1.rotation_text, q1.help) == ("180°", cell_help(check, "Q1"))
    d1 = matrix_cell(check, "D1", RULE)
    assert (d1.state, d1.rotation_text) == ("yellow", "0° !")
    r1 = matrix_cell(check, "R1", RULE)
    assert (r1.lcsc, r1.state, r1.glyph, r1.rotation) == ("", BLANK, "", None)
    assert r1.rank > q1.rank


def test_a_reference_the_check_has_not_read_is_blank_and_raw():
    """A footprint added since the last scan: no glyph, the raw angle."""
    check = SimpleNamespace(parts={})
    cell = matrix_cell(check, "R9", RULE)
    assert (cell.state, cell.help, cell.rotation_text) == (
        BLANK,
        "Not checked yet.",
        "raw",
    )
    assert cell.rotation_help.startswith("Raw angle: not checked yet.")


@pytest.mark.parametrize(
    "decision,sentence",
    [
        (
            _decision(rotation=180, source="derived", status="green"),
            "Derived 180° by the JLC footprint check (green).",
        ),
        (
            _decision(rotation=90, source="override", note="tab up"),
            "Override 90° set by you. Tab up.",
        ),
        (_decision(rotation=270, source="override"), "Override 270° set by you."),
        (_decision(lcsc=""), "Raw angle: no LCSC number assigned."),
        (_decision(pending=True), "Raw angle: waiting for EasyEDA data."),
        (
            _decision(status="red", fit="pitch"),
            "Raw angle: the JLC footprint does not fit (pitch).",
        ),
        (
            _decision(status="unknown", note="polarity unknown"),
            "Raw angle: polarity unknown.",
        ),
        (_decision(status="no-verdict"), "Raw angle: no rotation derived."),
    ],
    ids=[
        "derived",
        "override_with_note",
        "override",
        "no_lcsc",
        "pending",
        "red",
        "unknown",
        "nothing_derived",
    ],
)
def test_the_corr_hover_names_where_the_emitted_rotation_comes_from(decision, sentence):
    """Derived, an override, or the raw angle with its reason (spec 18.5)."""
    assert rotation_help(decision, RULE).startswith(f"{sentence} ")


@pytest.mark.parametrize(
    "rule,sentence",
    [
        (RULE, "Without the check the footprint rule would apply 90°."),
        (
            SimpleNamespace(
                rotation=180,
                offset_x=0.5,
                offset_y=-0.25,
                source="lcsc",
                status="complete",
            ),
            "Without the check the LCSC rule would apply 180° and move it 0.5/-0.25 mm.",
        ),
        (
            SimpleNamespace(
                rotation=0, offset_x=0.0, offset_y=0.0, source="", status="complete"
            ),
            "Without the check no correction rule applies.",
        ),
        (
            SimpleNamespace(
                rotation=0, offset_x=0.0, offset_y=0.0, source="", status="error"
            ),
            "The correction rules are unavailable.",
        ),
        (None, "The correction rules are unavailable."),
    ],
    ids=["rule", "rule_with_offset", "no_rule", "rules_error", "no_correction"],
)
def test_the_corr_hover_ends_with_what_the_rule_would_have_done(rule, sentence):
    """The rule the CPL does not apply while the check runs is quoted, offsets too."""
    decision = _decision(rotation=180, source="derived", status="green")
    assert rotation_help(decision, rule).endswith(f" {sentence}")


def test_a_pending_override_still_names_the_override():
    """Re-fetch keeps the override, and the CPL keeps emitting it (spec 16.5)."""
    stored = StoredVerdict("C1", "hash", status="pending", override_rotation=90)
    decision = _decision(
        rotation=90, source="override", pending=True, status="pending", verdict=stored
    )
    assert decision.display == "90° set"
    assert rotation_help(decision, RULE).startswith("Override 90° set by you.")
