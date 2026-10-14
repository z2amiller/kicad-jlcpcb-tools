"""The board backend over KiCad's SWIG bindings, for KiCad 7 to 10.

It reaches KiCad through the ``pcbnew`` module its caller passes in, and
the records it returns hold no SWIG object.
"""

from collections.abc import Callable, Sequence
import logging
from typing import Any, Optional

from .. import EDIT_RECOVERY_FAILED, BoardChanged, EditRecoveryFailed
from ..records import BoardInfo, BoardSnapshot, ComponentEdit
from .dialects import select
from .read import read_info, read_snapshot
from .write import apply_edits, edits_needed

_LOGGER = logging.getLogger(__name__)

Commit = Callable[[Callable[[], None]], None]


class SwigBackend:
    """Read and edit the board open in KiCad's PCB editor.

    It speaks the dialect of the KiCad major ``pcbnew`` belongs to, chosen
    when it is made; a major with no dialect raises Unsupported.

    ``commit`` runs a write inside KiCad's action wrapper, which gives it
    one undo step and marks the board modified; the action plugin supplies
    it. Without one, writes run directly, as in standalone mode.
    """

    def __init__(self, pcbnew: Any, commit: Optional[Commit] = None) -> None:  # noqa: UP045
        self._pcbnew = pcbnew
        self._dialect = select(pcbnew)
        self._commit = commit
        self._unreliable = False

    def _board(self) -> Any:
        """Return the editor's board, or raise BoardChanged when it has none."""
        board = self._pcbnew.GetBoard()
        if board is None:
            raise BoardChanged("The PCB editor no longer has an open board")
        return board

    def info(self) -> BoardInfo:
        """Identify the open board without reading its footprints."""
        return read_info(self._board())

    def snapshot(self) -> BoardSnapshot:
        """Read every footprint of the open board in its Default state.

        Raise BoardChanged when no board is open.
        """
        return read_snapshot(self._board(), self._dialect)

    def _check_reliable(self) -> None:
        """Refuse every edit once a failed write could not be undone."""
        if self._unreliable:
            raise EditRecoveryFailed(EDIT_RECOVERY_FAILED)

    def edits_needed(self, edits: Sequence[ComponentEdit]) -> bool:
        """Check a batch against the board without writing anything."""
        self._check_reliable()
        return edits_needed(self._board(), self._dialect, edits)

    def apply(self, label: str, edits: Sequence[ComponentEdit]) -> BoardSnapshot:
        """Write a batch inside KiCad's action wrapper, then read the board again.

        The whole batch is checked against the board before KiCad opens an
        undo step, so a batch the board refuses, or one that changes
        nothing, opens none. Inside the step the footprints are checked
        again, since one can change in between.
        """
        self._check_reliable()
        if not edits_needed(self._board(), self._dialect, edits):
            return self.snapshot()

        def write() -> None:
            try:
                apply_edits(self._board(), self._dialect, edits)
            except EditRecoveryFailed:
                self._unreliable = True
                raise

        if self._commit is None:
            write()
        else:
            self._commit(write)
        snapshot = self.snapshot()
        # A failed redraw cannot undo a write that was checked and kept.
        try:
            self._pcbnew.Refresh()
        except Exception as error:
            _LOGGER.warning("Unable to redraw the PCB after %s: %s", label, error)
        return snapshot
