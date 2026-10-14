"""Assigning, clearing and excluding parts on the board.

Each operation reads the board when it starts, writes one batch, and
returns an ``Outcome``: the rows it changed with the facts to show for
them, and the lines for the user's log. A failed outcome changed nothing on
the board; a notice has said why.
"""

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
import sqlite3
from typing import TYPE_CHECKING, Any, Optional

from ..board_parts import find_component, find_components, library_item_name, value_text
from ..derive_params import params_for_part
from ..kicad_backend import BoardChanged, ComponentEdit, ComponentRecord, default_edit
from ..lcsc import Lcsc, is_lcsc_part, normalize_lcsc
from ..part_assignments import is_assignment_alias
from .notify import Message

if TYPE_CHECKING:
    from .session import PluginSession


@dataclass(frozen=True)
class PartUpdate:
    """One parts-list row an operation changed, with its facts afterwards.

    An assignment gives the number and the catalog's type, stock and
    parameters, with ``part``, the row as the store now reads it with its
    supplier facts. An exclusion change gives the two flags as the board now
    holds them. ``repeated`` names the assignment fields the footprint holds
    more than once, in its order; every field of those names now holds the
    row's number, or is empty after a clear.
    """

    reference: str
    lcsc: str = ""
    part_type: str = ""
    stock: Any = ""
    params: str = ""
    part: Optional[Mapping[str, Any]] = None  # noqa: UP045
    exclude_from_bom: Optional[bool] = None  # noqa: UP045
    exclude_from_pos: Optional[bool] = None  # noqa: UP045
    repeated: tuple[str, ...] = ()


@dataclass(frozen=True)
class Outcome:
    """What an operation did, for the view to show.

    ``ok`` is false when the operation changed nothing. ``details`` holds
    catalog facts the part selector supplied, for the view's catalog cache.
    """

    ok: bool = True
    updates: tuple[PartUpdate, ...] = ()
    messages: tuple[Message, ...] = ()
    details: Optional[Mapping[str, Any]] = None  # noqa: UP045

    @property
    def references(self) -> list[str]:
        """Return the references of the changed rows, in order."""
        return [update.reference for update in self.updates]


@dataclass(frozen=True)
class TypedPart:
    """What the catalog says about a typed LCSC number.

    ``unlisted`` means the catalog does not list it, so the user is asked
    before it is assigned with empty ``details``.
    """

    details: dict[str, Any]
    unlisted: bool = False


def repeated_assignment_fields(component: ComponentRecord) -> tuple[str, ...]:
    """Return the assignment fields the footprint holds more than once."""
    return tuple(
        name for name in component.default.duplicate_names if is_assignment_alias(name)
    )


def _repeated_note(update: PartUpdate, holds: str) -> Message:
    """Say what every field of a repeated name now holds.

    It is true whether the operation wrote them or they already held it.
    """
    named = " and more than one named ".join(f'"{name}"' for name in update.repeated)
    return Message.warning(
        f"Footprint {update.reference} has more than one field named {named}; {holds}."
    )


def assignment_edit(component: ComponentRecord, lcsc: str) -> ComponentEdit:
    """Write ``lcsc``, or "" to clear, into the footprint's assignment fields.

    Every field that holds an assignment gets the number, so a stale alias
    never comes back once another is edited; so does every field of a name
    the footprint repeats. A footprint with none gets a hidden LCSC field.
    """
    lcsc = normalize_lcsc(lcsc)
    if lcsc and not is_lcsc_part(lcsc):
        raise ValueError("LCSC must be empty or a C followed by digits")
    state = component.default
    names = [name for name in state.fields if is_assignment_alias(name)]
    return default_edit(
        component,
        set_fields=tuple((name, lcsc) for name in names or ["LCSC"]),
        field_visibility=() if names else (("LCSC", False),),
    )


class Assignments:
    """The assignment operations of one session."""

    def __init__(self, session: "PluginSession") -> None:
        self.session = session

    def assign(
        self,
        assignments: Mapping[str, str],
        *,
        details: Optional[Mapping[str, Mapping[str, Any]]] = None,  # noqa: UP045
        remember: bool = False,
        require_catalog: bool = True,
    ) -> Outcome:
        """Assign LCSC numbers by reference, as one batch.

        Numbers arrive as text from the part selector, the clipboard, the
        Enter LCSC prompt and saved preferences, so they are parsed once,
        here; one that names no part is skipped, as is a reference the board
        no longer has. ``details`` gives catalog facts by number; the rest are
        read from the catalog. With ``remember`` and the setting on, each part
        is remembered for its footprint and value.
        """
        session = self.session
        if session.store is None or (
            require_catalog and not session.catalog.available()
        ):
            return Outcome(False)
        try:
            board = session.read_board()
        except BoardChanged as error:
            session.report(error)
            return Outcome(False)
        messages = []
        parts = {}
        for reference, number in assignments.items():
            if (part := Lcsc.parse(number)) is None:
                messages.append(
                    Message.warning(
                        f"Skipped {reference}: {number!r} does not name an LCSC part."
                    )
                )
            else:
                parts[reference] = part
        footprints = find_components(board, parts)
        if not footprints:
            return Outcome(False, messages=tuple(messages))
        # The caller's details are keyed by whatever text it holds, so they
        # are parsed too rather than compared against a canonical key.
        supplied = {
            part: row
            for number, row in (details or {}).items()
            if (part := Lcsc.parse(number)) is not None
        }
        catalog = {}
        try:
            for part in dict.fromkeys(parts[reference] for reference in footprints):
                row = supplied.get(part)
                if row is None:
                    row = session.catalog.part_details(part)
                catalog[part] = (row, params_for_part(row))
        except (sqlite3.Error, OSError) as error:
            session.report(error, f"Unable to apply LCSC assignments: {error}")
            return Outcome(False, messages=tuple(messages))
        edits = [
            assignment_edit(footprint, str(parts[reference]))
            for reference, footprint in footprints.items()
        ]
        if not session.apply("apply LCSC assignments", edits):
            return Outcome(False, messages=tuple(messages))
        updates = []
        preferences = []
        for reference, footprint in footprints.items():
            try:
                current = session.store.get_part(reference)
            except BoardChanged as error:
                session.report(error)
                return Outcome(False, messages=tuple(messages))
            lcsc = str(parts[reference])
            row, params = catalog[parts[reference]]
            stock = row.get("stock")
            update = PartUpdate(
                reference,
                lcsc,
                row.get("type", ""),
                stock if stock is not None else "",
                params,
                current,
                repeated=repeated_assignment_fields(footprint),
            )
            updates.append(update)
            if update.repeated:
                messages.append(_repeated_note(update, f"each now holds {lcsc}"))
            preferences.append(
                (
                    library_item_name(footprint.footprint_lib_id),
                    value_text(footprint),
                    lcsc,
                )
            )
        remembered = Outcome()
        if remember and session.settings.get("part_preferences", {}).get(
            "remember_lcsc_assignments", True
        ):
            remembered = session.preferences.remember(preferences, automatic=True)
        return Outcome(True, tuple(updates), (*messages, *remembered.messages))

    def assign_selected(
        self, references: Sequence[str], lcsc: str, part_type: str, stock: object
    ) -> Outcome:
        """Assign the part chosen in the part selector, and remember it.

        The selector's type and stock replace the catalog's. A part the
        catalog lists comes back in ``details``, so the view can cache what
        the selector showed.
        """
        session = self.session
        part = Lcsc.parse(lcsc)
        try:
            details = session.catalog.part_details(part) if part is not None else {}
        except (sqlite3.Error, OSError) as error:
            session.report(error, f"Unable to read the selected part: {error}")
            return Outcome(False)
        # An unlisted number must stay a cached miss, or it reads as listed.
        listed = bool(details)
        details.update(type=part_type, stock=stock)
        outcome = self.assign(
            dict.fromkeys(references, lcsc), details={lcsc: details}, remember=True
        )
        if outcome.ok and listed and part is not None:
            outcome = replace(outcome, details=deepcopy(details))
        return outcome

    def check_typed_part(self, lcsc: str) -> Optional[TypedPart]:  # noqa: UP045
        """Look a typed number up in the catalog before it is assigned.

        The catalog lists only JLC's assembly library, yet JLC also assembles
        LCSC-only parts it buys in through pre-order or global sourcing, and a
        catalog can be older than the part, so a number it does not list is
        asked about. With no catalog, or while Update library rewrites it,
        there is nothing to check. None means the lookup failed; a notice says
        why.
        """
        catalog = self.session.catalog
        if not catalog.available() or catalog.updating():
            return TypedPart({})
        part = Lcsc.parse(lcsc)
        try:
            details = catalog.part_details(part) if part is not None else {}
        except (sqlite3.Error, OSError) as error:
            self.session.report(
                error, f"Unable to look up {lcsc} in the parts catalog: {error}"
            )
            return None
        return TypedPart(details) if details else TypedPart({}, unlisted=True)

    def clear(self, references: Sequence[str]) -> Outcome:
        """Clear the LCSC numbers of the footprints these references find."""
        session = self.session
        if session.store is None:
            return Outcome(False)
        try:
            board = session.read_board()
        except BoardChanged as error:
            session.report(error)
            return Outcome(False)
        targets = [
            (reference, footprint)
            for reference in references
            if (footprint := find_component(board, reference)) is not None
        ]
        if not targets or not session.apply(
            "clear LCSC assignments",
            [assignment_edit(footprint, "") for _reference, footprint in targets],
        ):
            return Outcome(False)
        updates = tuple(
            PartUpdate(reference, repeated=repeated_assignment_fields(footprint))
            for reference, footprint in targets
        )
        return Outcome(
            True,
            updates,
            tuple(
                _repeated_note(update, "each is now empty")
                for update in updates
                if update.repeated
            ),
        )

    def toggle(self, references: Sequence[str], *, bom: bool, pos: bool) -> Outcome:
        """Flip the BOM exclusion, the position exclusion, or both, by reference."""
        session = self.session
        if session.store is None:
            return Outcome(False)
        try:
            board = session.read_board()
        except BoardChanged as error:
            session.report(error)
            return Outcome(False)
        updates = []
        edits = []
        for reference in references:
            footprint = find_component(board, reference)
            if footprint is None:
                continue
            state = footprint.default
            flags = {}
            if bom:
                flags["exclude_from_bom"] = not state.exclude_from_bom
            if pos:
                flags["exclude_from_pos"] = not state.exclude_from_pos
            edits.append(default_edit(footprint, flags=tuple(flags.items())))
            updates.append(
                PartUpdate(
                    reference,
                    exclude_from_bom=flags.get(
                        "exclude_from_bom", state.exclude_from_bom
                    ),
                    exclude_from_pos=flags.get(
                        "exclude_from_pos", state.exclude_from_pos
                    ),
                )
            )
        if not edits or not session.apply("change assembly exclusions", edits):
            return Outcome(False)
        return Outcome(True, tuple(updates))
