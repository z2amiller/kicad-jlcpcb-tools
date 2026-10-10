"""The variant matrix's JLC column and the Corr. column with the check on (spec 18.5).

Pure model tests on upstream's real snapshot records: the sixth frozen shared
column, its glyph per state, its sort and hover, and what Corr. shows, sorts and
explains while the check runs; with the check off both are what they were.
"""

from __future__ import annotations

import pytest

from jlcfootprint.presentation import GLYPHS, SORT_ORDER, MatrixCell
from tests.variant_model_test_support import Snapshot, State, matrix as m


def _snapshot(references=("R1", "R2", "R10")):
    """Return three components in Default and A, references natural-ordered."""
    return Snapshot(
        tuple(
            State(f"id-{reference}", reference, name)
            for reference in references
            for name in ("", "A")
        ),
        ("", "A"),
    )


def _cell(state="green", rotation=180, text=None, lcsc="C1", **changes):
    """Return one check cell with readable defaults."""
    values = {
        "lcsc": lcsc,
        "state": state,
        "help": f"help for {state}",
        "rotation_text": text if text is not None else f"{rotation}°",
        "rotation": rotation,
        "rotation_help": f"rotation help {rotation}",
    }
    values.update(changes)
    return MatrixCell(**values)


def _model(cells=None, *, corrections=None, output="A", references=("R1", "R2", "R10")):
    """Return a matrix model with the check's cells keyed by component id, or off."""
    return m.MatrixModel(
        _snapshot(references),
        corrections=corrections
        if corrections is not None
        else {
            f"id-{reference}": m.CorrectionState(90, 0.5, 0.0, "fpt", 90.0)
            for reference in ("R1", "R2", "R10")
        },
        correction_variant=output,
        jlc=cells,
    )


def _display(model, key):
    """Return {reference: display} for one shared column."""
    column = model.column_for(None, key)
    return {
        row.reference: model.get_display(index, column)
        for index, row in enumerate(model.rows)
    }


def _order(model, key, descending=False):
    """Sort by one shared column and return the references in display order."""
    model.sort_by(model.column_for(None, key), descending)
    return [row.reference for row in model.rows]


def test_the_jlc_column_is_the_sixth_frozen_shared_column():
    """Key ``jlc``, header "JLC", read-only, after Corr.; nothing else moves."""
    model = _model()
    assert m.SHARED_COLUMN_COUNT == 6
    jlc = model.columns[5]
    assert (jlc.key, jlc.label, jlc.variant, jlc.editable) == (
        "jlc",
        "JLC",
        None,
        False,
    )
    assert [column.key for column in model.columns[:5]] == [
        "ref",
        "footprint",
        "side",
        "pcb_angle",
        "correction",
    ]
    assert model.columns[6].variant == ""


@pytest.mark.parametrize("state", SORT_ORDER)
def test_the_jlc_cell_draws_its_state_s_glyph(state):
    """Each of the ordinary list's nine states keeps its glyph (spec 16.3)."""
    model = _model({"id-R1": _cell(state)})
    row = model.row_for_component("id-R1")
    column = model.column_for(None, "jlc")
    assert model.get_display(row, column) == GLYPHS[state]
    assert model.cell_style(row, column).symbol == ""
    assert model.jlc_cell("id-R1").state == state


def test_the_jlc_column_is_blank_while_the_check_is_off():
    """No cells: a blank column, a hover that says why, and upstream's Corr."""
    model = _model(None)
    assert not model.jlc_available
    assert set(_display(model, "jlc").values()) == {""}
    assert set(_display(model, "correction").values()) == {"90°, 0.5/0"}
    row = model.row_for_component("id-R1")
    assert model.cell_details(row, model.column_for(None, "jlc")) == (
        "R1 · Output: A · JLC\nThe JLC footprint check is off."
    )
    details = model.cell_details(row, model.column_for(None, "correction"))
    assert "Rule source: fpt; final CPL angle: 90.0°" in details


def test_the_jlc_column_sorts_worst_first_with_the_reference_breaking_ties():
    """Ascending: red before green, equal states by reference; descending reverses."""
    model = _model(
        {
            "id-R1": _cell("green"),
            "id-R2": _cell("red"),
            "id-R10": _cell("green"),
        },
        references=("R10", "R2", "R1"),  # the board's own order is not the natural one
    )
    assert _order(model, "jlc") == ["R2", "R1", "R10"]
    assert _order(model, "jlc", descending=True) == ["R10", "R1", "R2"]


def test_the_jlc_hover_is_the_check_s_help_for_the_output_variant():
    """The matrix's own hover (F2 and Cell details too) carries the cell's help."""
    model = _model({"id-R1": _cell("yellow", help="Fits; rotation 180°.")})
    row = model.row_for_component("id-R1")
    column = model.column_for(None, "jlc")
    expected = "R1 · Output: A · JLC\nFits; rotation 180°."
    assert model.cell_details(row, column) == expected
    assert model.cell_tooltip(row, column) == expected


def test_corr_shows_what_the_cpl_emits_while_the_check_runs():
    """The Rotation wording replaces upstream's rule text, the hover names the source."""
    model = _model(
        {
            "id-R1": _cell("green", 180),
            "id-R2": _cell("yellow", 90, text="90° !"),
            "id-R10": _cell(
                "red", None, text="raw", rotation_help="Raw angle: misfit."
            ),
        }
    )
    assert _display(model, "correction") == {"R1": "180°", "R2": "90° !", "R10": "raw"}
    row = model.row_for_component("id-R10")
    assert model.cell_details(row, model.column_for(None, "correction")) == (
        "R10 · Output: A · Corr.\nRaw angle: misfit."
    )


def test_corr_sorts_by_the_emitted_angle_with_the_raw_angle_last():
    """Angles ascending, raw after them, ties by reference; descending reverses."""
    model = _model(
        {
            "id-R1": _cell(rotation=270),
            "id-R2": _cell(rotation=None, text="raw"),
            "id-R10": _cell(rotation=0),
        }
    )
    assert _order(model, "correction") == ["R10", "R1", "R2"]
    assert _order(model, "correction", descending=True) == ["R2", "R1", "R10"]


def test_corr_stays_readable_when_upstream_s_rules_are_unavailable():
    """With the check on, Corr. never shows "Unavailable": the CPL emits the check's angle."""
    model = _model({"id-R1": _cell(rotation=180)}, corrections={})
    display = _display(model, "correction")
    assert display["R1"] == "180°"
    assert display["R2"] == "raw"  # no cell for it: the raw angle
    row = model.row_for_component("id-R1")
    assert model.cell_style(row, model.column_for(None, "correction")).symbol == ""


def test_copying_a_jlc_cell_copies_its_glyph():
    """Copy cell value takes the displayed glyph, like the other read-only cells."""
    model = _model({"id-R1": _cell("red")})
    row = model.row_for_component("id-R1")
    payload = model.copy_cell(row, model.column_for(None, "jlc"))
    assert payload.plain_text == "✗"
