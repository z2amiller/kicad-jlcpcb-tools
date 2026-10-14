"""Part preferences: the part remembered for each footprint and value.

Saving remembers the selected rows' parts; applying fills the selected rows
from what is remembered; opening the plugin fills empty assignments once.
"""

from collections.abc import Collection, Iterable, Sequence
from dataclasses import replace
import sqlite3
from typing import TYPE_CHECKING

from ..board_parts import find_component, occupied_assignment
from ..kicad_backend import BoardChanged
from .assignments import Outcome
from .notify import Message

if TYPE_CHECKING:
    from .session import PluginSession


class Preferences:
    """The part preference operations of one session."""

    def __init__(self, session: "PluginSession") -> None:
        self.session = session

    def remember(
        self, preferences: Iterable[tuple[str, str, str]], *, automatic: bool = False
    ) -> Outcome:
        """Remember one action's parts by footprint and value, all or none.

        A preference needs both a footprint and a value, and is kept only
        for a number that names a part. ``automatic`` says an assignment
        remembered it, which the user can turn off in Settings. The outcome
        holds the line for the log.
        """
        complete = [
            preference
            for preference in preferences
            if all(isinstance(text, str) and text.strip() for text in preference[:2])
        ]
        if not complete:
            return Outcome()
        try:
            saved = self.session.catalog.save_part_preferences(complete)
        except sqlite3.Error as error:
            self.session.report(error, f"Unable to save part preferences: {error}")
            return Outcome(False)
        if not saved:
            return Outcome()
        text = f"Saved {saved} part preference(s)."
        if automatic:
            text += (
                " To disable automatic remembering, clear 'Remember my part preferences'"
                " in Settings > Part preferences."
            )
        return Outcome(messages=(Message.info(text),))

    def save(self, references: Sequence[str]) -> Outcome:
        """Remember the parts these references are assigned, as the board holds them."""
        session = self.session
        if session.store is None:
            return Outcome(False)
        try:
            session.read_board()
            parts = [session.store.get_part(reference) for reference in references]
        except BoardChanged as error:
            session.report(error)
            return Outcome(False)
        preferences = [
            (part["footprint"], part["value"], part["lcsc"]) for part in parts if part
        ]
        return self.remember(preferences)

    def apply(self, references: Sequence[str]) -> Outcome:
        """Assign these references the parts remembered for their footprint and value."""
        session = self.session
        if session.store is None:
            return Outcome(False)
        assignments = {}
        try:
            session.read_board()
            for reference in references:
                part = session.store.get_part(reference)
                if part is None:
                    continue
                footprint, value = part["footprint"], part["value"]
                if footprint and value:
                    preference = session.catalog.part_preference(footprint, value)
                    if preference:
                        assignments[reference] = preference
        except BoardChanged as error:
            session.report(error)
            return Outcome(False)
        except sqlite3.Error as error:
            session.report(error, f"Unable to apply part preferences: {error}")
            return Outcome(False)
        outcome = session.assignments.assign(assignments)
        if not outcome.updates:
            return outcome
        return replace(
            outcome,
            messages=(
                *outcome.messages,
                Message.info(
                    f"Applied part preferences to {len(outcome.updates)} assignment(s)."
                ),
            ),
        )

    def fill_empty(self, *, skip: Collection[str] = ()) -> Outcome:
        """Fill every truly empty assignment from part preferences, once on opening.

        A part is filled only when it has a footprint and a value, is placed
        and in the BOM, and no assignment field holds any text. ``skip``
        names footprints, by UUID, that legacy recovery still holds back.
        """
        session = self.session
        if session.store is None:
            return Outcome(False)
        messages = []
        remembered = {}
        assignments = {}
        try:
            board = session.read_board()
            for part in session.store.rows(board):
                if part["lcsc"] or not part["footprint"] or not part["value"]:
                    continue
                footprint = find_component(board, part["reference"])
                if (
                    footprint is None
                    or footprint.uuid in skip
                    or footprint.default.exclude_from_bom
                    or footprint.default.exclude_from_pos
                    or footprint.default.dnp
                ):
                    continue
                key = (part["footprint"], part["value"])
                if key not in remembered:
                    remembered[key] = session.catalog.part_preference(*key)
                if lcsc := remembered[key]:
                    if occupied := occupied_assignment(footprint):
                        name, text = occupied
                        messages.append(
                            Message.info(
                                f"Skipped part preference for {part['reference']}: "
                                f"field {name!r} already contains {text!r}."
                            )
                        )
                    else:
                        assignments[part["reference"]] = lcsc
            outcome = session.assignments.assign(assignments)
        except BoardChanged as error:
            session.report(error)
            return Outcome(False, messages=tuple(messages))
        except sqlite3.Error as error:
            session.report(
                error, f"Unable to fill assignments from part preferences: {error}"
            )
            return Outcome(False, messages=tuple(messages))
        messages.extend(outcome.messages)
        if outcome.updates:
            messages.append(
                Message.info(
                    f"Filled {len(outcome.updates)} empty LCSC assignment(s) from part "
                    "preferences. To disable, clear 'Parts preferences fill in empty "
                    "LCSC assignments' in Settings > Part preferences."
                )
            )
        return replace(outcome, messages=tuple(messages))
