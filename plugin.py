"""Contains the Action Plugin."""

from functools import partial
import os

import pcbnew  # pylint: disable=import-error
import wx

from .kicad_backend import Unsupported
from .kicad_backend.swig.commit import ActionDispatcher
from .kicad_backend.swig.dialects import select


class JLCPCBPlugin(pcbnew.ActionPlugin):
    """JLCPCBPlugin instance of ActionPlugin."""

    def defaults(self) -> None:
        """Define defaults."""
        # pylint: disable=attribute-defined-outside-init
        self.name = "JLCPCB Tools"
        self.category = "Fabrication data generation"
        self.description = (
            "Generate JLCPCB-compatible Gerber, Excellon, BOM and CPL files"
        )
        self.show_toolbar_button = True
        path, _ = os.path.split(os.path.abspath(__file__))
        self.icon_file_name = os.path.join(path, "jlcpcb-icon.png")
        self._actions = ActionDispatcher(pcbnew, self.name)

    def Run(self) -> None:
        """Open the modeless window or execute one native menu-dispatched edit."""
        try:
            select(pcbnew)
        except Unsupported as error:
            wx.MessageBox(
                str(error), "Unsupported KiCad version", wx.OK | wx.ICON_ERROR
            )
            return

        if self._actions.run_pending():
            return

        from .mainwindow import JLCPCBTools  # noqa: PLC0415

        try:
            frames = [
                frame
                for frame in wx.GetTopLevelWindows()
                if frame
                and not frame.IsBeingDeleted()
                and hasattr(frame, "GetMenuBar")
                and self._actions.menu_id(frame) is not None
            ]
            if len(frames) != 1:
                raise RuntimeError(
                    "Unable to identify the PCB editor's JLCPCB Tools command"
                )
            context = (frames[0], self._actions.board_id())
            with self._actions.opening():
                dialog = JLCPCBTools(
                    None, commit=partial(self._actions.run, context=context)
                )
                dialog.Center()
                dialog.Show()
        except Exception as exc:
            wx.MessageBox(str(exc), "JLCPCB Tools", wx.OK | wx.ICON_ERROR)
