"""Assigning, clearing and excluding parts, through the session over a fake board."""

import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

from .app_support import app, lcsc

__all__ = ["app"]

LISTED = {"C25804": {"type": "Basic", "stock": 27, "description": "10k 0603"}}


def _fields(app: SimpleNamespace, component: Any, number: str) -> dict[str, Any]:
    """Return the set fields and visibility an assignment edit asks for."""
    edit = app.assignments.assignment_edit(component, number)
    return {"set": dict(edit.set_fields), "visibility": dict(edit.field_visibility)}


class TestTheAssignmentEdit:
    """Shared code decides which fields hold the number; the backend only writes."""

    def test_every_alias_gets_the_number(self, app: SimpleNamespace) -> None:
        """A stale alias never comes back once another is edited."""
        component = app.fake.component(
            "R1",
            fields={
                "LCSC Part #": "C12345 ",
                "LCSC": "C99999",
                "JLCPCB Rotation": "90",
            },
        )
        assert _fields(app, component, "C77777") == {
            "set": {"LCSC Part #": "C77777", "LCSC": "C77777"},
            "visibility": {},
        }

    def test_blank_and_invalid_aliases_keep_their_role(
        self, app: SimpleNamespace
    ) -> None:
        """Clearing leaves aliases in place; C-shaped text in other fields is not one."""
        component = app.fake.component(
            "R1",
            fields={
                "LCSC Part Number": "invalid",
                "JLCPCB Part #": "",
                "JLCPCB Customer ID": "C100",
                "JLCPCB Rotation": "C200",
            },
        )
        assert _fields(app, component, "")["set"] == {
            "LCSC Part Number": "",
            "JLCPCB Part #": "",
        }

    def test_a_footprint_without_one_gains_a_hidden_lcsc_field(
        self, app: SimpleNamespace
    ) -> None:
        """Clearing one also writes an empty hidden field, as assigning would."""
        component = app.fake.component("R1", fields={"Manufacturer": "Acme"})
        for number in ("C12345", ""):
            assert _fields(app, component, number) == {
                "set": {"LCSC": number},
                "visibility": {"LCSC": False},
            }

    def test_the_number_is_written_canonical_and_checked(
        self, app: SimpleNamespace
    ) -> None:
        """Trimmed and upper case; text that names no part is refused."""
        component = app.fake.component("R1")
        edit = app.assignments.assignment_edit(component, " c99999 ")
        assert edit.set_fields == (("LCSC", "C99999"),)
        assert edit.based_on == component.default.revision
        with pytest.raises(ValueError, match="C followed by digits"):
            app.assignments.assignment_edit(component, "invalid")


def test_an_assignment_returns_its_rows_and_remembers_its_parts(
    app: SimpleNamespace,
) -> None:
    """Each row carries the catalog's facts and the store's row after the edit."""
    session = app.session(
        app.fake.component("R1", value="10k", footprint="Lib:R_0603"),
        app.fake.component("R2", value="10k", footprint="Lib:R_0603"),
        catalog=app.Catalog(LISTED),
    )

    outcome = session.assign({"R1": " c25804 ", "R2": "C25804"}, remember=True)

    assert outcome.ok and outcome.references == ["R1", "R2"]
    update = outcome.updates[0]
    assert (update.lcsc, update.part_type, update.stock, update.params) == (
        "C25804",
        "Basic",
        27,
        "10k 0603",
    )
    assert update.part["lcsc"] == "C25804"
    assert lcsc(session, "R1") == lcsc(session, "R2") == "C25804"
    assert session.catalog.saved == [[("R_0603", "10k", "C25804")] * 2]
    assert [message.text for message in outcome.messages] == [
        "Saved 1 part preference(s). To disable automatic remembering, clear "
        "'Remember my part preferences' in Settings > Part preferences."
    ]
    assert session.catalog.reads == ["C25804"]
    assert session.backend.applied == ["apply LCSC assignments"]


def test_an_assignment_remembers_nothing_when_the_setting_is_off(
    app: SimpleNamespace,
) -> None:
    """Remembering follows the user's setting."""
    session = app.session(
        app.fake.component("R1", value="10k"),
        catalog=app.Catalog(LISTED),
        settings={"part_preferences": {"remember_lcsc_assignments": False}},
    )

    outcome = session.assign({"R1": "C25804"}, remember=True)

    assert outcome.ok and session.catalog.saved == [] and outcome.messages == ()


def test_numbers_that_name_no_part_and_references_gone_are_skipped(
    app: SimpleNamespace,
) -> None:
    """One bad entry is no reason to abandon the rest of the action."""
    session = app.session(app.fake.component("R1"), catalog=app.Catalog(LISTED))

    outcome = session.assign({"R1": "C25804", "R2": "C1", "R3": "not a part"})

    assert outcome.references == ["R1"]
    assert [(m.level, m.text) for m in outcome.messages] == [
        ("warning", "Skipped R3: 'not a part' does not name an LCSC part.")
    ]
    assert not session.assign({"R2": "C1"}).ok


def test_a_repeated_reference_assigns_the_first_footprint_in_board_order(
    app: SimpleNamespace,
) -> None:
    """KiCad's own lookup by reference finds the first."""
    first = app.fake.component("R1", uuid="first")
    second = app.fake.component("R1", uuid="second")
    session = app.session(first, second, catalog=app.Catalog(LISTED))

    session.assign({"R1": "C25804"})

    board = session.backend.snapshot()
    assert board.get("first").default.fields["LCSC"].text == "C25804"
    assert "LCSC" not in board.get("second").default.fields


@pytest.mark.parametrize("missing", ["store", "catalog"])
def test_nothing_is_read_or_written_without_the_store_or_catalog(
    app: SimpleNamespace, missing: str
) -> None:
    """Only a typed number may be assigned without a catalog.

    Without project storage, clearing and the exclusion toggles do nothing
    either.
    """
    session = app.session(app.fake.component("R1"), store=missing != "store")
    session.catalog.ready = missing != "catalog"

    assert not session.assign({"R1": "C25804"}).ok
    if missing == "store":
        assert not session.clear(["R1"]).ok
        assert not session.toggle(["R1"], bom=True, pos=True).ok
    assert session.backend.reads == 0
    if missing == "catalog":
        assert session.assign({"R1": "C25804"}, require_catalog=False).ok


def test_a_catalog_that_cannot_be_read_stops_the_assignment(
    app: SimpleNamespace,
) -> None:
    """Nothing is written; the notice names the action and the error."""
    session = app.session(app.fake.component("R1"))
    session.catalog.failure = sqlite3.OperationalError("database is locked")

    outcome = session.assign({"R1": "C25804"})

    assert not outcome.ok and session.backend.applied == []
    assert session.notices.texts == [
        "Unable to apply LCSC assignments: database is locked"
    ]
    assert not session.notices[0].blocking


def test_a_closed_board_stops_each_action_before_it_edits(
    app: SimpleNamespace,
) -> None:
    """Assign, clear and toggle each read the board first; a closed board blocks."""
    session = app.session(app.fake.component("R1"), catalog=app.Catalog(LISTED))
    session.backend.board = None

    outcomes = [
        session.assign({"R1": "C25804"}),
        session.clear(["R1"]),
        session.toggle(["R1"], bom=True, pos=False),
    ]

    assert not any(outcome.ok for outcome in outcomes)
    assert [notice.blocking for notice in session.notices] == [True] * 3
    assert all(
        isinstance(notice.error, app.api.BoardChanged) for notice in session.notices
    )


def test_a_failed_write_assigns_and_remembers_nothing(app: SimpleNamespace) -> None:
    """Every footprint is put back, and no preference is saved for the batch."""
    session = app.session(
        app.fake.component("R1", fields={"LCSC": "C1"}),
        app.fake.component("R2", fields={"LCSC": "C2"}),
        catalog=app.Catalog(LISTED),
    )
    session.backend.fail_writes.add(session.read_board().by_reference("R2")[0].uuid)

    outcome = session.assign({"R1": "C25804", "R2": "C25804"}, remember=True)

    assert not outcome.ok and outcome.updates == ()
    assert (lcsc(session, "R1"), lcsc(session, "R2")) == ("C1", "C2")
    assert session.catalog.saved == []
    assert session.notices.texts == [
        "Unable to apply LCSC assignments: Writing R2 failed"
    ]


@pytest.mark.parametrize("listed", [True, False])
def test_the_part_selector_assigns_with_the_type_and_stock_it_showed(
    app: SimpleNamespace, listed: bool
) -> None:
    """A listed part comes back for the view's cache; an unlisted one does not."""
    session = app.session(
        app.fake.component("R1", value="10k"),
        catalog=app.Catalog(LISTED if listed else {}),
    )

    outcome = session.assign_selected(["R1"], "C25804", "Extended", 3)

    assert (outcome.updates[0].part_type, outcome.updates[0].stock) == ("Extended", 3)
    if listed:
        assert outcome.details == {
            "type": "Extended",
            "stock": 3,
            "description": "10k 0603",
        }
    else:
        assert outcome.details is None
    assert session.catalog.saved == [[("Footprint", "10k", "C25804")]]


def test_the_part_selector_reports_a_catalog_it_cannot_read(
    app: SimpleNamespace,
) -> None:
    """The selected part's facts are read first; nothing is written without them."""
    session = app.session(app.fake.component("R1"))
    session.catalog.failure = OSError("disk unavailable")

    assert not session.assign_selected(["R1"], "C25804", "Basic", 1).ok
    assert session.notices.texts == [
        "Unable to read the selected part: disk unavailable"
    ]
    assert session.backend.reads == 0


@pytest.mark.parametrize(
    "catalog, expected",
    [
        ("listed", ({"type": "Basic", "stock": 27, "description": "10k 0603"}, False)),
        ("unlisted", ({}, True)),
        ("unavailable", ({}, False)),
        ("updating", ({}, False)),
        ("broken", None),
    ],
)
def test_a_typed_number_is_looked_up_before_it_is_assigned(
    app: SimpleNamespace, catalog: str, expected: Any
) -> None:
    """Only a number the catalog does not list is asked about."""
    session = app.session(catalog=app.Catalog(LISTED))
    number = "C1" if catalog == "unlisted" else "C25804"
    session.catalog.ready = catalog != "unavailable"
    session.catalog.busy = catalog == "updating"
    if catalog == "broken":
        session.catalog.failure = sqlite3.DatabaseError("malformed")

    typed = session.check_typed_part(number)

    if expected is None:
        assert typed is None
        assert session.notices.texts == [
            "Unable to look up C25804 in the parts catalog: malformed"
        ]
    else:
        assert (typed.details, typed.unlisted) == expected


def test_clearing_empties_every_alias_of_the_footprints_found(
    app: SimpleNamespace,
) -> None:
    """References the board no longer has are left out of the result."""
    session = app.session(
        app.fake.component("R1", fields={"LCSC Part #": "C1", "LCSC": "C1"}),
        app.fake.component("R2", fields={"LCSC": "C2"}),
    )

    outcome = session.clear(["R1", "R9"])

    assert outcome.ok and outcome.references == ["R1"]
    fields = session.backend.snapshot().by_reference("R1")[0].default.fields
    assert (fields["LCSC Part #"].text, fields["LCSC"].text) == ("", "")
    assert lcsc(session, "R2") == "C2"
    assert session.backend.applied == ["clear LCSC assignments"]
    assert not session.clear(["R9"]).ok


def test_every_field_of_a_repeated_name_is_written_and_each_row_says_so(
    app: SimpleNamespace,
) -> None:
    """Assigning and clearing write each field of the name; one warning per row.

    Only assignment fields are written, so a repeated MPN goes unmentioned.
    """
    repeated = (("LCSC", "C1"), ("MPN", "X"), ("LCSC", "C2"), ("MPN", "Y"))
    session = app.session(
        app.fake.component("R1", fields=repeated),
        app.fake.component("R2", fields={"LCSC": "C3"}),
        catalog=app.Catalog(LISTED),
    )

    def copies(reference: str) -> list[str]:
        state = session.backend.snapshot().by_reference(reference)[0].default
        return [
            record.text
            for record in (*state.replaced_fields, *state.fields.values())
            if record.name == "LCSC"
        ]

    assigned = session.assign({"R1": "C25804", "R2": "C25804"})

    assert [(u.reference, u.repeated) for u in assigned.updates] == [
        ("R1", ("LCSC",)),
        ("R2", ()),
    ]
    assert copies("R1") == ["C25804", "C25804"]
    assert session.backend.snapshot().components[0].default.duplicate_names == (
        "LCSC",
        "MPN",
    )
    assert [(m.level, m.text) for m in assigned.messages] == [
        (
            "warning",
            'Footprint R1 has more than one field named "LCSC"; each now holds C25804.',
        )
    ]

    cleared = session.clear(["R1", "R2"])

    assert [(u.reference, u.repeated) for u in cleared.updates] == [
        ("R1", ("LCSC",)),
        ("R2", ()),
    ]
    assert copies("R1") == ["", ""]
    assert [m.text for m in cleared.messages] == [
        'Footprint R1 has more than one field named "LCSC"; each is now empty.'
    ]


def test_a_repeated_name_that_already_holds_the_number_is_reported_truly(
    app: SimpleNamespace,
) -> None:
    """Nothing is written, and the warning still says only what the fields hold."""
    session = app.session(
        app.fake.component("R1", fields=(("LCSC", "C25804"), ("LCSC", "C25804"))),
        catalog=app.Catalog(LISTED),
    )

    outcome = session.assign({"R1": "C25804"})

    assert outcome.ok and session.backend.applied == []
    assert [m.text for m in outcome.messages] == [
        'Footprint R1 has more than one field named "LCSC"; each now holds C25804.'
    ]


@pytest.mark.parametrize("action", ["clear", "toggle"])
def test_two_selected_rows_with_one_reference_are_refused_as_before(
    app: SimpleNamespace, action: str
) -> None:
    """Both rows find the first footprint with the reference, as KiCad's lookup does.

    One batch cannot edit a footprint twice, so the action is refused whole
    and nothing is written, with the message the window has always logged.
    """
    session = app.session(
        app.fake.component("R1", uuid="first", fields={"LCSC": "C1"}),
        app.fake.component("R1", uuid="second", fields={"LCSC": "C2"}),
    )

    if action == "clear":
        outcome = session.clear(["R1", "R1"])
    else:
        outcome = session.toggle(["R1", "R1"], bom=True, pos=False)

    label = {"clear": "clear LCSC assignments", "toggle": "change assembly exclusions"}
    assert not outcome.ok and session.backend.applied == []
    assert session.notices.texts == [
        f"Unable to {label[action]}: Combine changes for each footprint into one edit"
    ]


@pytest.mark.parametrize(
    "bom, pos, expected",
    [
        (True, False, (False, False)),
        (False, True, (True, True)),
        (True, True, (False, True)),
    ],
)
def test_a_toggle_flips_each_footprints_own_flags(
    app: SimpleNamespace, bom: bool, pos: bool, expected: tuple[bool, bool]
) -> None:
    """Each footprint's flags flip from what the board read says, not the list."""
    session = app.session(
        app.fake.component("R1", exclude_from_bom=True),
        app.fake.component("R2", exclude_from_pos=False),
    )

    outcome = session.toggle(["R1", "R9", "R2"], bom=bom, pos=pos)

    assert outcome.references == ["R1", "R2"]
    first = session.backend.snapshot().by_reference("R1")[0].default
    assert (first.exclude_from_bom, first.exclude_from_pos) == expected
    assert [
        (u.reference, u.exclude_from_bom, u.exclude_from_pos) for u in outcome.updates
    ][0] == ("R1", *expected)
    assert session.backend.applied == ["change assembly exclusions"]
