"""Write component edits to the board open in KiCad, as one recoverable batch.

Every target is read and checked before anything is written: a footprint
that changed since its edit was made, or an edit the shared rules refuse,
stops the whole batch. Each write is read back, and if any write fails,
every footprint the batch touched is put back as it was. How fields are
written and put back differs between KiCad majors, so the dialect does it.

KiCad's footprint dialog can give a footprint two fields with one name. It
saves both and keeps one, the later, when the board is reopened. A field
an edit sets is written into every field of that name and none is
deleted, so the edit holds whichever one KiCad keeps; each is read back
and put back. The state read from the footprint records every one, so
the checks compare them all.
"""

from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from .. import (
    EDIT_RECOVERY_FAILED,
    BackendError,
    EditRecoveryFailed,
    EditRejected,
    StaleTarget,
)
from ..edits import check_batch, copies, edited_state, same_state
from ..records import ComponentEdit, ComponentState
from .dialects import Dialect
from .read import _EXCLUDE_FROM_BOM, _EXCLUDE_FROM_POS, footprints_by_key, read_state

# The attribute bit of each exclusion an edit can set, as the reader reads them.
_FLAG_BITS = {
    "exclude_from_pos": _EXCLUDE_FROM_POS,
    "exclude_from_bom": _EXCLUDE_FROM_BOM,
}


@dataclass(frozen=True)
class _Captured:
    """A footprint's values, kept to write over and to put back exactly."""

    state: ComponentState  # every field, those of a repeated name included
    attributes: int


def _capture(footprint: Any, dialect: Dialect) -> _Captured:
    """Read the footprint's state and its raw attribute bits."""
    return _Captured(read_state(footprint, dialect), int(footprint.GetAttributes()))


def _attributes(before: _Captured, edit: ComponentEdit) -> int:
    """Return the attribute bits with the edit's exclusions set or cleared."""
    attributes = before.attributes
    for name, value in edit.flags:
        if name in _FLAG_BITS:
            bit = _FLAG_BITS[name]
            attributes = attributes | bit if value else attributes & ~bit
    return attributes


def _write(
    footprint: Any, dialect: Dialect, before: _Captured, edit: ComponentEdit
) -> None:
    """Perform one edit's field and flag operations."""
    dialect.write_fields(footprint, edit)
    attributes = _attributes(before, edit)
    if attributes != before.attributes:
        footprint.SetAttributes(attributes)


def _matches(
    footprint: Any, dialect: Dialect, state: ComponentState, attributes: int
) -> bool:
    """Check the footprint holds this state and these raw attribute bits.

    Every field is compared, each field of a repeated name in turn.
    """
    if not same_state(read_state(footprint, dialect), state):
        return False
    return int(footprint.GetAttributes()) == attributes


def _restore(footprint: Any, dialect: Dialect, before: _Captured) -> bool:
    """Attempt every independent restoration, then check the whole state."""
    with suppress(Exception):
        footprint.SetAttributes(before.attributes)
    with suppress(Exception):
        dialect.restore_fields(footprint, copies(before.state))
    try:
        return _matches(footprint, dialect, before.state, before.attributes)
    except Exception:
        return False


def _prepare(
    board: Any, dialect: Dialect, edits: Sequence[ComponentEdit]
) -> list[tuple[Any, _Captured, ComponentEdit, ComponentState]]:
    """Check the whole batch against the board; keep the edits that change it.

    Each is kept with the footprint's values before it and the state it
    leads to.
    """
    batch = check_batch(edits)
    footprints = footprints_by_key(board, (edit.key for edit in batch))
    prepared = []
    try:
        for edit in batch:
            footprint = footprints.get(edit.key)
            if footprint is None:
                raise StaleTarget(
                    "A footprint to edit is no longer on the board; nothing was written."
                )
            before = _capture(footprint, dialect)
            if before.state.revision != edit.based_on:
                raise StaleTarget(
                    f"Footprint {footprint.GetReference()} changed since it was read; "
                    "nothing was written."
                )
            dialect.check_writable(edit)
            state = edited_state(
                before.state, edit, new_fields_shown=dialect.shows_fields
            )
            if not same_state(state, before.state):
                prepared.append((footprint, before, edit, state))
    except BackendError:
        raise
    except Exception as error:
        raise EditRejected(str(error)) from error
    return prepared


def edits_needed(board: Any, dialect: Dialect, edits: Sequence[ComponentEdit]) -> bool:
    """Check a batch against the board without writing; return whether it changes it."""
    return bool(_prepare(board, dialect, edits))


def apply_edits(board: Any, dialect: Dialect, edits: Sequence[ComponentEdit]) -> None:
    """Write a checked batch; on any failure put every touched footprint back.

    The caller owns KiCad's undo step. A failed write that is undone raises
    EditRejected with KiCad's message; one that cannot be undone raises
    EditRecoveryFailed, and the board must be inspected and reopened. Every
    attribute bit is read back, not only the ones the edit sets.
    """
    prepared = _prepare(board, dialect, edits)
    applied: list[tuple[Any, _Captured]] = []
    try:
        for footprint, before, edit, after in prepared:
            applied.append((footprint, before))
            _write(footprint, dialect, before, edit)
            if not _matches(footprint, dialect, after, _attributes(before, edit)):
                raise RuntimeError(
                    "Native board setter did not preserve the requested part state"
                )
        for footprint, _before in applied:
            footprint.SetModified()
    except Exception as error:
        restored = [
            _restore(footprint, dialect, before)
            for footprint, before in reversed(applied)
        ]
        if not all(restored):
            raise EditRecoveryFailed(EDIT_RECOVERY_FAILED) from error
        if isinstance(error, BackendError):
            raise
        raise EditRejected(str(error)) from error
