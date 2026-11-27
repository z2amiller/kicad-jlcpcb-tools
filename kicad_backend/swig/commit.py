"""Give a write from the open window KiCad's undo step and modified flag.

KiCad's SWIG bindings expose neither BOARD_COMMIT nor the editor's OnModify.
KiCad wraps every action plugin's Run() in its own transaction instead: it
records the board for undo before Run() and marks it modified afterwards
(pcbnew/python/scripting/pcbnew_action_plugins.cpp, RunActionPlugin). So a
write from the modeless window is queued here, and the plugin's own menu
command is sent to the PCB editor; KiCad calls Run(), which performs the
queued write inside that transaction.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Optional

import wx

from .. import BoardChanged
from .read import board_identity


class ActionDispatcher:
    """Queue writes for the action plugin's Run() and send its menu command.

    One dispatcher belongs to one plugin. While the plugin's own Run()
    opens the window, writes run at once: KiCad's wrapper is already open.
    """

    def __init__(self, pcbnew: Any, name: str) -> None:
        self._pcbnew = pcbnew
        self._name = name
        self._opening = False
        self._dispatching = False
        self._pending: Optional[Callable[[], None]] = None  # noqa: UP045
        self._error: Optional[BaseException] = None  # noqa: UP045

    def board_id(self) -> str:
        """Identify the open board without keeping a native handle."""
        return board_identity(self._pcbnew.GetBoard())

    def menu_id(self, frame: Any) -> Optional[int]:  # noqa: UP045
        """Resolve the plugin's one current command, including rebuilt menus."""
        menubar = frame.GetMenuBar()
        if not menubar:
            return None

        def matching_ids(menu: Any) -> list[int]:
            ids = []
            for item in menu.GetMenuItems():
                submenu = item.GetSubMenu()
                if submenu:
                    ids.extend(matching_ids(submenu))
                elif item.GetItemLabelText() == self._name:
                    ids.append(item.GetId())
            return ids

        ids = [
            item_id for menu, _ in menubar.GetMenus() for item_id in matching_ids(menu)
        ]
        return ids[0] if len(ids) == 1 else None

    @contextmanager
    def opening(self) -> Iterator[None]:
        """Run writes at once while the plugin's own Run() opens the window."""
        self._opening = True
        try:
            yield
        finally:
            self._opening = False

    def run_pending(self) -> bool:
        """Perform a queued write inside KiCad's wrapper; say whether one was queued.

        A failure is kept, not raised, so KiCad can finish its undo and
        modified-flag transaction; ``run`` raises it once KiCad returns.
        """
        if self._pending is None:
            return False
        action, self._pending = self._pending, None
        try:
            if not self._pcbnew.IsActionRunning():
                raise RuntimeError("KiCad did not enter its native action wrapper")
            action()
        except BaseException as error:
            self._error = error
        return True

    def run(self, action: Callable[[], None], *, context: tuple[Any, str]) -> None:
        """Run ``action`` inside KiCad's action wrapper for the window's board.

        ``context`` is the PCB editor frame and the identity of the board the
        window was opened for; the write is refused if either has gone.
        """
        if self._dispatching:
            raise RuntimeError("A JLCPCB Tools board edit is already running")
        frame, board_id = context
        if not frame or frame.IsBeingDeleted() or self.board_id() != board_id:
            raise BoardChanged(
                "The original PCB is no longer open; reopen JLCPCB Tools"
            )
        if not frame.IsEnabled():
            raise RuntimeError(
                "Close the PCB editor's active dialog before editing parts"
            )
        if self._opening:
            if not self._pcbnew.IsActionRunning():
                raise RuntimeError("KiCad did not enter its native action wrapper")
            action()  # Writes made while opening belong to the original Run.
            return
        if self._pcbnew.IsActionRunning():
            raise RuntimeError("Another KiCad action is already running")
        menu_id = self.menu_id(frame)
        if menu_id is None:
            raise RuntimeError("The PCB editor's JLCPCB Tools command is unavailable")

        def apply_to_original_board() -> None:
            if self.board_id() != board_id:
                raise BoardChanged(
                    "The original PCB is no longer open; reopen JLCPCB Tools"
                )
            action()

        self._dispatching = True
        self._pending = apply_to_original_board
        self._error = None
        try:
            frame.GetEventHandler().ProcessEvent(
                wx.CommandEvent(wx.EVT_MENU.typeId, menu_id)
            )
            if self._pending is not None:
                raise RuntimeError("KiCad did not execute the requested board edit")
            if self._error is not None:
                raise self._error
        finally:
            self._pending = None
            self._error = None
            self._dispatching = False
