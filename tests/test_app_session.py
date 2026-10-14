"""The session reads and writes the board, and reports what goes wrong once."""

from dataclasses import replace
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

from .app_support import app

__all__ = ["app"]


def _edit(app: SimpleNamespace, component: Any, **operations: Any) -> Any:
    """Edit a footprint's own fields and flags from the state read."""
    return app.api.ComponentEdit(
        component.uuid, component.default.revision, **operations
    )


def test_the_session_holds_the_board_it_last_read(app: SimpleNamespace) -> None:
    """A lookup answers from the held snapshot; a read replaces it."""
    session = app.session(app.fake.component("R1"))

    first = session.held_board()
    assert session.backend.reads == 1
    assert session.held_board() is first
    assert session.backend.reads == 1
    second = session.read_board()
    assert session.board is second is not first


def test_another_board_is_refused_and_the_held_snapshot_kept(
    app: SimpleNamespace,
) -> None:
    """The board token must stay the one the session was opened for."""
    session = app.session(app.fake.component("R1"))
    held = session.read_board()
    session.backend.board = app.fake.board_info(token="another-board")

    with pytest.raises(app.api.BoardChanged, match="Reopen JLCPCB Tools"):
        session.read_board()
    with pytest.raises(app.api.BoardChanged):
        session.held_board()
    assert session.board is held
    session.backend.board = None
    with pytest.raises(app.api.BoardChanged, match="no longer has an open board"):
        session.check_board()


def test_a_guard_checks_the_board_in_the_sessions_place(
    app: SimpleNamespace,
) -> None:
    """The window's check runs instead of the backend's description of the board."""
    session = app.session(app.fake.component("R1"))
    checks = []
    session._guard = lambda: checks.append("checked")
    session.backend.info = lambda: pytest.fail("the guard was not used")

    session.check_board()
    assert checks == ["checked"]


def test_a_batch_that_changes_nothing_is_not_written(app: SimpleNamespace) -> None:
    """KiCad marks the board modified for every write it wraps, so none is made."""
    session = app.session(app.fake.component("R1", fields={"LCSC": "C1"}))
    (r1,) = session.read_board().components

    assert session.apply("assign", [_edit(app, r1, set_fields=(("LCSC", "C1"),))])
    assert session.backend.applied == []
    assert session.apply("assign", [_edit(app, r1, set_fields=(("LCSC", "C2"),))])
    assert session.backend.applied == ["assign"]
    assert session.board.get(r1.uuid).default.fields["LCSC"].text == "C2"
    assert session.notices == []


def test_no_edit_is_written_while_generating(app: SimpleNamespace) -> None:
    """Generation reads the board while it runs; edits wait for it."""
    session = app.session(app.fake.component("R1"))
    (r1,) = session.read_board().components
    session.generating = True

    assert not session.apply(
        "apply LCSC assignments", [_edit(app, r1, flags=(("exclude_from_bom", True),))]
    )
    assert session.backend.applied == []
    assert session.notices.texts == [
        "Unable to apply LCSC assignments: Finish generation before editing part assignments."
    ]
    assert not session.notices[0].blocking


@pytest.mark.parametrize("problem", ["rejected", "stale", "recheck"])
def test_a_refused_batch_is_reported_and_editing_goes_on(
    app: SimpleNamespace, problem: str
) -> None:
    """Nothing is written; the board stays usable."""
    session = app.session(app.fake.component("R1"), app.fake.component("R2"))
    r1, r2 = session.read_board().components
    edits = [_edit(app, r1, flags=(("exclude_from_bom", True),))]
    recheck = None
    if problem == "rejected":
        edits.append(_edit(app, r1, flags=(("exclude_from_bom", False),)))
    elif problem == "stale":
        edits.append(
            replace(_edit(app, r2, flags=(("exclude_from_bom", True),)), based_on="old")
        )
    else:

        def recheck() -> None:
            raise RuntimeError("Legacy recovery sources changed; retry initialization.")

    assert not session.apply("recover legacy assignments", edits, recheck=recheck)

    assert session.backend.applied == []
    (notice,) = session.notices
    assert notice.text.startswith("Unable to recover legacy assignments: ")
    assert not notice.blocking
    assert session.reliable
    session.read_board()


def test_a_batch_for_a_board_the_session_did_not_open_writes_nothing(
    app: SimpleNamespace,
) -> None:
    """The session checks the board before it writes, not only when it reads."""
    session = app.session(app.fake.component("R1"))
    (r1,) = session.read_board().components
    session.backend.board = app.fake.board_info(token="another-board")

    assert not session.apply(
        "assign", [_edit(app, r1, flags=(("exclude_from_bom", True),))]
    )

    assert session.backend.applied == []
    (notice,) = session.notices
    assert notice.blocking and not session.reliable


def test_a_session_opened_without_a_token_keeps_the_first_board_it_sees(
    app: SimpleNamespace,
) -> None:
    """With neither a token nor a guard, the first check names the board."""
    session = app.session(app.fake.component("R1"))
    session.board_token = None

    session.read_board()
    assert session.board_token == session.backend.info().board_token
    session.backend.board = app.fake.board_info(token="another-board")
    with pytest.raises(app.api.BoardChanged, match="Reopen JLCPCB Tools"):
        session.read_board()


def test_a_recheck_runs_only_when_a_write_is_needed(app: SimpleNamespace) -> None:
    """A batch that changes nothing has nothing to recheck."""
    session = app.session(app.fake.component("R1", fields={"LCSC": "C1"}))
    (r1,) = session.read_board().components
    calls = []

    session.apply(
        "assign",
        [_edit(app, r1, set_fields=(("LCSC", "C1"),))],
        recheck=lambda: calls.append(1),
    )
    session.apply(
        "assign",
        [_edit(app, r1, set_fields=(("LCSC", "C2"),))],
        recheck=lambda: calls.append(2),
    )
    assert calls == [2]


@pytest.mark.parametrize("problem", ["recovery", "closed", "replaced"])
def test_a_write_that_leaves_the_board_unknown_stops_every_later_read(
    app: SimpleNamespace, problem: str
) -> None:
    """After a write could not be undone or the board went, editing stops."""
    session = app.session(app.fake.component("R1"), app.fake.component("R2"))
    r1, r2 = session.read_board().components
    if problem == "recovery":
        session.backend.fail_writes.add(r2.uuid)
        session.backend.fail_restores.add(r2.uuid)
    elif problem == "closed":
        session.backend.board = None
    else:
        apply = session.backend.apply

        def apply_then_replace(label: str, edits: Any) -> Any:
            snapshot = apply(label, edits)
            return replace(snapshot, info=replace(snapshot.info, board_token="other"))

        session.backend.apply = apply_then_replace

    assert not session.apply(
        "assign",
        [
            _edit(app, r1, flags=(("exclude_from_bom", True),)),
            _edit(app, r2, flags=(("exclude_from_bom", True),)),
        ],
    )

    (notice,) = session.notices
    assert notice.blocking and isinstance(
        notice.error, (app.api.BoardChanged, app.api.EditRecoveryFailed)
    )
    assert not session.reliable
    session.backend.board = app.fake.board_info()
    with pytest.raises(app.api.BoardChanged, match="could not be restored"):
        session.read_board()


def test_the_session_remembers_parts_for_a_view(app: SimpleNamespace) -> None:
    """A view remembers preferences through the session, as it calls the rest."""
    session = app.session()

    outcome = session.remember([("R_0603", "10k", " c25804 ")], automatic=True)

    assert session.catalog.saved == [[("R_0603", "10k", " c25804 ")]]
    assert [m.text for m in outcome.messages] == [
        "Saved 1 part preference(s). To disable automatic remembering, clear "
        "'Remember my part preferences' in Settings > Part preferences."
    ]


def test_errors_are_reported_by_kind(app: SimpleNamespace) -> None:
    """A board error blocks editing; a catalog error is shown with its wording.

    Neither marks the session unreliable: only a write can leave the board
    in a state the session cannot vouch for.
    """
    session = app.session()
    board_error = app.api.BoardChanged("The PCB editor no longer has an open board")
    catalog_error = sqlite3.OperationalError("database is locked")

    session.report(board_error)
    session.report(
        catalog_error, "Unable to apply LCSC assignments: database is locked"
    )

    assert [(n.text, n.error, n.blocking) for n in session.notices] == [
        ("The PCB editor no longer has an open board", board_error, True),
        ("Unable to apply LCSC assignments: database is locked", catalog_error, False),
    ]
    assert session.reliable
