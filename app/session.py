"""The plugin's state and operations, for a view to hold.

``PluginSession`` owns the board backend, the snapshot it last read, the
project's store and the settings. Its operations are delegated to one
module per area; this module keeps what they share: reading the board,
writing a batch, and reporting what goes wrong.

It runs on the thread that calls it, like the backend: on the wx main
thread under KiCad.
"""

from collections.abc import Callable, Collection, Iterable, Sequence
from typing import Any, Optional, Protocol

from ..kicad_backend import (
    BOARD_REPLACED,
    BOARD_UNRESTORED,
    BoardBackend,
    BoardChanged,
    BoardSnapshot,
    ComponentEdit,
    EditRecoveryFailed,
)
from ..lcsc import Lcsc
from .assignments import Assignments, Outcome, TypedPart
from .notify import Notice, Notifier
from .preferences import Preferences


class Catalog(Protocol):
    """The parts library, as the session reads it."""

    def available(self) -> bool:
        """Return whether a parts catalog is ready to read."""
        ...

    def updating(self) -> bool:
        """Return whether Update library is rewriting the catalog."""
        ...

    def part_details(self, part: Lcsc) -> dict[str, Any]:
        """Return the catalog's facts for a part, empty when it lists none.

        Raise sqlite3.Error or OSError when the catalog cannot be read.
        """
        ...

    def part_preference(self, footprint: str, value: str) -> Optional[str]:  # noqa: UP045
        """Return the part remembered for a footprint and value, if any."""
        ...

    def save_part_preferences(self, preferences: Sequence[tuple[str, str, str]]) -> int:
        """Remember parts by footprint and value; return how many changed."""
        ...


class PluginSession:
    """The board, the project's assignments and the catalog, behind plain calls.

    ``store`` is the project's parts store, None while project storage is
    unavailable; ``settings`` are the plugin's settings. ``board_token``
    names the board the session edits; without one, it is the first board
    the session's own check sees. ``guard``, if given, checks the board in
    the session's place: it must raise BoardChanged for another board, and
    once ``reliable`` is false.
    """

    def __init__(
        self,
        backend: BoardBackend,
        notifier: Notifier,
        catalog: Catalog,
        *,
        board_token: Optional[str] = None,  # noqa: UP045
        guard: Optional[Callable[[], None]] = None,  # noqa: UP045
    ) -> None:
        self.backend = backend
        self.notifier = notifier
        self.catalog = catalog
        self.store: Optional[Any] = None  # noqa: UP045
        self.settings: dict[str, Any] = {}
        self.board: Optional[BoardSnapshot] = None  # noqa: UP045
        self.board_token = board_token
        self.reliable = True
        self.generating = False
        self._guard = guard
        self.assignments = Assignments(self)
        self.preferences = Preferences(self)

    # The board

    def check_board(self) -> None:
        """Raise BoardChanged unless the board is still the one this session edits.

        After a board change or a write that could not be undone, the
        session refuses the board until the plugin is reopened. A ``guard``
        makes this check in its place.
        """
        if self._guard is not None:
            self._guard()
            return
        token = self.backend.info().board_token
        if self.board_token is None:
            self.board_token = token
        if token != self.board_token:
            raise BoardChanged(BOARD_REPLACED)
        if not self.reliable:
            raise BoardChanged(BOARD_UNRESTORED)

    def _hold(self, snapshot: BoardSnapshot) -> BoardSnapshot:
        """Hold a snapshot, refusing one of another board."""
        if (
            self.board_token is not None
            and snapshot.info.board_token != self.board_token
        ):
            raise BoardChanged(BOARD_REPLACED)
        self.board = snapshot
        return snapshot

    def read_board(self) -> BoardSnapshot:
        """Read the board again, after checking it, and hold the snapshot."""
        self.check_board()
        return self._hold(self.backend.snapshot())

    def held_board(self) -> BoardSnapshot:
        """Return the snapshot last read, once the board is checked.

        A single lookup answers from it: reading the whole board for each
        one would cost a full read per part.
        """
        self.check_board()
        return self.board if self.board is not None else self.read_board()

    def apply(
        self,
        label: str,
        edits: Sequence[ComponentEdit],
        *,
        recheck: Optional[Callable[[], None]] = None,  # noqa: UP045
    ) -> bool:
        """Write one batch as one undo step and hold the board as it now reads.

        Return whether the board holds the batch; a batch that changes
        nothing is not written, since KiCad marks the board modified for
        every write it wraps. ``recheck``, if given, runs once a write is
        known to be needed, and stops it by raising. Problems are reported
        to the notifier, worded by ``label``.
        """
        try:
            self.check_board()
            if self.generating:
                raise RuntimeError("Finish generation before editing part assignments.")
            if not self.backend.edits_needed(edits):
                return True
            if recheck is not None:
                recheck()
            self._hold(self.backend.apply(label, edits))
        except (BoardChanged, EditRecoveryFailed) as error:
            self.reliable = False
            self.notifier.notice(Notice(str(error), error, blocking=True))
            return False
        except Exception as error:
            self.notifier.notice(Notice(f"Unable to {label}: {error}", error))
            return False
        return True

    def report(self, error: BaseException, text: str = "") -> None:
        """Tell the view about the error an operation stopped on.

        A board that was closed or replaced gives a blocking notice, and the
        board check goes on refusing any other board. Unlike a failed write
        in ``apply``, a read leaves the board as it was, so the session stays
        reliable. Any other error is shown with ``text``.
        """
        if isinstance(error, BoardChanged):
            self.notifier.notice(Notice(str(error), error, blocking=True))
        else:
            self.notifier.notice(Notice(text or str(error), error))

    # Assignments

    def assign(
        self,
        assignments: dict[str, str],
        *,
        details: Optional[dict[str, dict[str, Any]]] = None,  # noqa: UP045
        remember: bool = False,
        require_catalog: bool = True,
    ) -> Outcome:
        """Assign LCSC numbers by reference; see ``Assignments.assign``."""
        return self.assignments.assign(
            assignments,
            details=details,
            remember=remember,
            require_catalog=require_catalog,
        )

    def assign_selected(
        self, references: Sequence[str], lcsc: str, part_type: str, stock: object
    ) -> Outcome:
        """Assign the part chosen in the part selector; see ``Assignments``."""
        return self.assignments.assign_selected(references, lcsc, part_type, stock)

    def check_typed_part(self, lcsc: str) -> Optional[TypedPart]:  # noqa: UP045
        """Look a typed number up before it is assigned; see ``Assignments``."""
        return self.assignments.check_typed_part(lcsc)

    def clear(self, references: Sequence[str]) -> Outcome:
        """Clear the LCSC numbers of these references; see ``Assignments``."""
        return self.assignments.clear(references)

    def toggle(self, references: Sequence[str], *, bom: bool, pos: bool) -> Outcome:
        """Flip these references' BOM and position exclusions; see ``Assignments``."""
        return self.assignments.toggle(references, bom=bom, pos=pos)

    # Part preferences

    def remember(
        self, preferences: Iterable[tuple[str, str, str]], *, automatic: bool = False
    ) -> Outcome:
        """Remember parts by footprint and value; see ``Preferences.remember``."""
        return self.preferences.remember(preferences, automatic=automatic)

    def save_preferences(self, references: Sequence[str]) -> Outcome:
        """Remember these references' parts; see ``Preferences.save``."""
        return self.preferences.save(references)

    def apply_preferences(self, references: Sequence[str]) -> Outcome:
        """Assign these references their remembered parts; see ``Preferences``."""
        return self.preferences.apply(references)

    def fill_empty_assignments(self, *, skip: Collection[str] = ()) -> Outcome:
        """Fill empty assignments from part preferences; see ``Preferences``."""
        return self.preferences.fill_empty(skip=skip)
