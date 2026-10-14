"""Part preferences through the session: remember, save, apply and fill on opening."""

import sqlite3
from types import SimpleNamespace

import pytest

from .app_support import app, lcsc

__all__ = ["app"]

LISTED = {
    "C25804": {"type": "Basic", "stock": 27, "description": "10k 0603"},
    "C21190": {"type": "Basic", "stock": 9, "description": "1k 0603"},
}
TEN_K = ("R_0603", "10k")


def _resistor(app: SimpleNamespace, reference: str, **kwargs: object) -> object:
    """Return a 10k 0603 resistor footprint."""
    kwargs.setdefault("value", "10k")
    return app.fake.component(reference, footprint="Lib:R_0603", **kwargs)


def test_saving_remembers_each_selected_part_as_the_board_holds_it(
    app: SimpleNamespace,
) -> None:
    """Rows without a number or a footprint and value are left out."""
    session = app.session(
        _resistor(app, "R1", fields={"LCSC": "C25804"}),
        _resistor(app, "R2", value=""),
        catalog=app.Catalog(LISTED),
    )

    outcome = session.save_preferences(["R1", "R2", "R9"])

    assert outcome.ok
    assert session.catalog.saved == [[("R_0603", "10k", "C25804")]]
    assert [message.text for message in outcome.messages] == [
        "Saved 1 part preference(s)."
    ]


def test_saving_leaves_a_row_without_a_number_to_the_catalog(
    app: SimpleNamespace,
) -> None:
    """The session passes the row on; the catalog decides what a number is."""
    session = app.session(_resistor(app, "R1"), catalog=app.Catalog(LISTED))

    outcome = session.save_preferences(["R1"])

    assert outcome.ok
    assert session.catalog.saved == [[("R_0603", "10k", "")]]


def test_a_preference_store_that_cannot_be_written_is_reported(
    app: SimpleNamespace,
) -> None:
    """The notice names the error; nothing is said to be saved."""
    session = app.session(
        _resistor(app, "R1", fields={"LCSC": "C25804"}), catalog=app.Catalog(LISTED)
    )
    session.catalog.save_failure = sqlite3.OperationalError("readonly database")

    outcome = session.save_preferences(["R1"])

    assert not outcome.ok
    assert outcome.messages == ()
    assert session.notices.texts == [
        "Unable to save part preferences: readonly database"
    ]


def test_applying_assigns_the_remembered_part_and_says_how_many(
    app: SimpleNamespace,
) -> None:
    """Rows with nothing remembered are left as they are."""
    session = app.session(
        _resistor(app, "R1"),
        _resistor(app, "R2", value="22k"),
        catalog=app.Catalog(LISTED, {TEN_K: "C25804"}),
    )

    outcome = session.apply_preferences(["R1", "R2"])

    assert outcome.references == ["R1"]
    assert (lcsc(session, "R1"), lcsc(session, "R2")) == ("C25804", "")
    assert [message.text for message in outcome.messages] == [
        "Applied part preferences to 1 assignment(s)."
    ]
    assert session.catalog.saved == []


def test_applying_reports_preferences_that_cannot_be_read(
    app: SimpleNamespace,
) -> None:
    """Nothing is assigned when the preferences cannot be read."""
    session = app.session(_resistor(app, "R1"), catalog=app.Catalog(LISTED))
    session.catalog.failure = sqlite3.OperationalError("database is locked")

    outcome = session.apply_preferences(["R1"])

    assert not outcome.ok and session.backend.applied == []
    assert session.notices.texts == [
        "Unable to apply part preferences: database is locked"
    ]


def test_opening_fills_only_truly_empty_placed_bom_parts(app: SimpleNamespace) -> None:
    """Excluded, unplaced, held-back and occupied parts keep what they have."""
    session = app.session(
        _resistor(app, "R1"),
        _resistor(app, "R2", exclude_from_bom=True),
        _resistor(app, "R3", exclude_from_pos=True),
        _resistor(app, "R4", dnp=True),
        _resistor(app, "R5", uuid="held-back"),
        _resistor(app, "R6", fields={"JLCPCB Part #": " "}),
        _resistor(app, "R7", fields={"LCSC": "C1"}),
        catalog=app.Catalog(LISTED, {TEN_K: "C25804"}),
    )

    outcome = session.fill_empty_assignments(skip={"held-back"})

    assert outcome.references == ["R1"]
    assert [lcsc(session, f"R{n}") for n in range(1, 8)] == [
        "C25804",
        "",
        "",
        "",
        "",
        "",
        "C1",
    ]
    assert [message.text for message in outcome.messages] == [
        "Skipped part preference for R6: field 'JLCPCB Part #' already contains ' '.",
        "Filled 1 empty LCSC assignment(s) from part preferences. To disable, "
        "clear 'Parts preferences fill in empty LCSC assignments' in Settings > "
        "Part preferences.",
    ]
    assert session.catalog.saved == []


@pytest.mark.parametrize("problem", ["board", "preferences"])
def test_opening_reports_what_stopped_the_fill(
    app: SimpleNamespace, problem: str
) -> None:
    """A board error blocks editing; unreadable preferences are only reported."""
    first, second = _resistor(app, "R1"), _resistor(app, "R2")
    session = app.session(first, second, catalog=app.Catalog(LISTED, {TEN_K: "C25804"}))
    if problem == "board":
        session.backend.board = None
    else:
        session.catalog.failure = sqlite3.OperationalError("database is locked")

    outcome = session.fill_empty_assignments()

    assert not outcome.ok and session.backend.applied == []
    (notice,) = session.notices
    assert notice.blocking is (problem == "board")
    if problem == "preferences":
        assert notice.text == (
            "Unable to fill assignments from part preferences: database is locked"
        )


@pytest.mark.parametrize("operation", ["save", "apply", "fill"])
def test_nothing_is_read_without_project_storage(
    app: SimpleNamespace, operation: str
) -> None:
    """Without the store the operations do nothing at all."""
    session = app.session(_resistor(app, "R1"), store=False)

    outcome = {
        "save": lambda: session.save_preferences(["R1"]),
        "apply": lambda: session.apply_preferences(["R1"]),
        "fill": session.fill_empty_assignments,
    }[operation]()

    assert not outcome.ok
    assert session.backend.reads == 0
