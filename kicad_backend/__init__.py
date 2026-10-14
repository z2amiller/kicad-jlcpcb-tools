"""Reach the open KiCad board through one interface, whichever way KiCad offers.

Shared code reads the board through ``BoardBackend`` and sees only the
immutable records in ``records``; ``fake`` holds a board in memory for
tests. The ``swig`` package implements the interface over KiCad's SWIG
bindings. Other code still works on SWIG objects, among them fabrication,
DRC, the variant adapter and the board edits, until later changes move it
behind the interface.
"""

from collections.abc import Sequence
from typing import Protocol

from .records import (
    BoardInfo,
    BoardSnapshot,
    ComponentEdit,
    ComponentRecord,
    ComponentState,
    FieldRecord,
    PadRecord,
    VariantDefinition,
    component_state,
    default_edit,
    footprint_keys,
)


class BackendError(RuntimeError):
    """A board operation could not be completed."""


class BoardChanged(BackendError):
    """The board was closed, or another board is open in its place."""


class StaleTarget(BackendError):
    """A footprint changed since the state an edit quotes; nothing was written."""


class EditRejected(BackendError):
    """An edit batch was not applied, and the board holds what it held before.

    Either the batch asks for something no backend may write, or KiCad
    refused or did not keep a write and every footprint the batch had
    touched was restored.
    """


class EditRecoveryFailed(BackendError):
    """A failed write could not be undone; the backend refuses further edits.

    The board may hold part of the batch. Only reopening the board, after
    inspecting it, clears this.
    """


class Unsupported(BackendError):
    """The running KiCad cannot do what was asked: its major has no dialect."""


# What a backend says when a failed write could not be undone, and again for
# every edit it is asked for after that.
EDIT_RECOVERY_FAILED = (
    "Board edit recovery failed; reopen the plugin and inspect the board before editing"
)


class BoardBackend(Protocol):
    """The board operations shared code may use."""

    def info(self) -> BoardInfo:
        """Identify the open board without reading its footprints.

        Raise BoardChanged when no board is open.
        """
        ...

    def snapshot(self) -> BoardSnapshot:
        """Read every footprint of the open board.

        Raise BoardChanged when no board is open.
        """
        ...

    def edits_needed(self, edits: Sequence[ComponentEdit]) -> bool:
        """Check a batch without writing; return whether it changes the board.

        Raise EditRejected for a batch no backend may write and StaleTarget
        when a footprint changed since the state its edit quotes. KiCad marks
        the board modified for every action it wraps, even one that changes
        nothing, so callers skip ``apply`` when this returns False.
        """
        ...

    def apply(self, label: str, edits: Sequence[ComponentEdit]) -> BoardSnapshot:
        """Write a batch as one undo step, check it, and read the board again.

        ``label`` says what the batch does, in the words the user chose the
        action by. The whole batch is checked before anything is written; a
        write that fails is undone. Raise EditRejected or StaleTarget with
        nothing written, and EditRecoveryFailed when a failed write could not
        be undone.
        """
        ...


__all__ = [
    "EDIT_RECOVERY_FAILED",
    "BackendError",
    "BoardBackend",
    "BoardChanged",
    "BoardInfo",
    "BoardSnapshot",
    "ComponentEdit",
    "ComponentRecord",
    "ComponentState",
    "EditRecoveryFailed",
    "EditRejected",
    "FieldRecord",
    "PadRecord",
    "StaleTarget",
    "Unsupported",
    "VariantDefinition",
    "component_state",
    "default_edit",
    "footprint_keys",
]
