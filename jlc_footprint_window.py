"""The main window's JLC footprint check: the column, the dialog and the submenu.

``JLCPCBTools`` owns the hooks -- the column, the binds, the tooltip map, the
generate steps -- and hands everything behind them to a ``JlcFootprintPresenter``
built once per window, which reaches back through ``self.window`` for the part
list, the settings and the store.  Keeping the feature here means a rebase onto
upstream's ``mainwindow.py`` touches the hooks only, and an upstream reader sees
one file per feature instead of 300 lines inside a 3,000-line frame.
"""

from __future__ import annotations

from collections.abc import Iterable
import sqlite3
from typing import Any, Optional

import wx  # pylint: disable=import-error

from .datamodel import PartListDataModel
from .jlc_footprint_check import (
    clear_cache as clear_jlc_footprint_cache,
    create_footprint_check,
    is_footprint_check_enabled,
    recheck_board as recheck_jlc_footprint_board,
    refetch_references as refetch_jlc_footprint_references,
    refresh_board_data as refresh_jlc_footprint_board_data,
)
from .jlc_footprint_detail import JlcFootprintDetailDialog
from .jlcfootprint.presentation import cell_help, glyph_state, part_detail

ID_CONTEXT_MENU_JLC_DETAILS = wx.NewIdRef()
ID_CONTEXT_MENU_JLC_REFETCH = wx.NewIdRef()
ID_CONTEXT_MENU_JLC_RECHECK = wx.NewIdRef()
ID_CONTEXT_MENU_JLC_REFRESH = wx.NewIdRef()
ID_CONTEXT_MENU_JLC_CLEAR_CACHE = wx.NewIdRef()


class JlcFootprintPresenter:
    """Everything the main window does for the JLC footprint check.

    One is built per window and kept as ``window.jlc_footprint_presenter``; the
    window keeps a two-line delegator for every name its own hooks, its binds or
    the tests reach, so the feature's behaviour has one home and the frame keeps
    its published surface.
    """

    def __init__(self, window: Any) -> None:
        self.window = window

    def _start_jlc_footprint_check(self) -> None:
        """Start the footprint check for the open board when the setting is on."""
        self._stop_jlc_footprint_check()
        if self.window.store is None or not is_footprint_check_enabled(
            self.window.settings
        ):
            return
        try:
            check = create_footprint_check(self.window, self.window.pcbnew)
            check.start()
            check.scan_board()
        except (sqlite3.Error, OSError) as error:
            self.window.logger.warning("JLC footprint check unavailable: %s", error)
            return
        self.window.jlc_footprint_check = check

    def _stop_jlc_footprint_check(self) -> None:
        """Stop the footprint check's background thread, if one is running."""
        check = getattr(self.window, "jlc_footprint_check", None)
        if check is not None:
            self.window.jlc_footprint_check = None
            check.stop()

    def _enqueue_jlc_footprint_check(self, references: Iterable[str]) -> None:
        """Have newly assigned parts checked."""
        check = getattr(self.window, "jlc_footprint_check", None)
        if check is not None:
            check.enqueue_references(references)

    def on_jlc_footprint_result(self, e):
        """Repaint the Rotation cells of one checked part; a superseded board load's results are dropped."""
        check = getattr(self.window, "jlc_footprint_check", None)
        if check is None or getattr(e, "generation", None) != check.generation:
            return
        references = check.references_for(e.lcsc)
        self.window.logger.debug(
            "JLC footprint check: %s checked for %s",
            e.lcsc,
            ", ".join(references) or "no reference",
        )
        for reference in references:
            self.window.partlist_data_model.set_rotation(
                reference, check.display_text(reference) or "raw"
            )
            self._apply_jlc_cell(reference)

    def _activated_model_column(self, event: Any) -> Optional[int]:  # noqa: UP045
        """Return the model column an activation event names, or None when it names none.

        wxGTK fills both the column object and its index; macOS fills the index only
        (measured 2026-09-17 on wx 4.2.2a1 osx-cocoa), so the index is mapped back
        through the control's own column positions.  A port that reports neither gets
        the old behaviour and the context menu is the way in.
        """
        column = event.GetDataViewColumn()
        if column is None:
            index = event.GetColumn()
            if index is None or index < 0:
                return None
            control = self.window.footprint_list
            column = next(
                (
                    candidate
                    for candidate in control.GetColumns()
                    if control.GetColumnPosition(candidate) == index
                ),
                None,
            )
        return None if column is None else column.GetModelColumn()

    def on_footprint_activated(self, event: Any) -> None:
        """Double-clicking a JLC cell opens the detail dialog; any other cell assigns a part."""
        if self._activated_model_column(event) == PartListDataModel.columns["JLC_COL"]:
            self.show_jlc_footprint_detail()
            return
        self.window.select_part(event)

    def _first_selected_jlc_reference(self) -> Optional[str]:  # noqa: UP045
        """Return the first selected reference that carries an LCSC number."""
        model = self.window.partlist_data_model
        for item in self.window.footprint_list.GetSelections():
            reference = str(model.get_reference(item) or "")
            if reference and str(model.get_lcsc(item) or "").strip():
                return reference
        return None

    def show_jlc_footprint_detail(self, reference: Optional[str] = None) -> None:  # noqa: UP045
        """Open the JLC footprint detail dialog for one part (spec 16.4)."""
        check = self._active_jlc_footprint_check()
        if check is None:
            return
        if reference is None:
            reference = self._first_selected_jlc_reference()
        if reference is None:
            return
        detail = part_detail(check, reference, reread=True)
        if detail is None:
            return
        dialog = JlcFootprintDetailDialog(
            self.window,
            detail,
            set_override=lambda rotation, note: self._set_jlc_override(
                reference, rotation, note
            ),
            refetch=lambda: self._refetch_jlc_references([reference]),
            settings=self.window.settings,
        )
        try:
            dialog.ShowModal()
        finally:
            # A modal dialog dismissed with its Close button or Esc ends the modal
            # loop directly and never sends EVT_CLOSE, so the size is remembered
            # here rather than only from the window's own close box.
            dialog.remember_size()
            dialog.Destroy()
        self.window.save_settings()

    def _set_jlc_override(
        self,
        reference: str,
        rotation: Optional[int],  # noqa: UP045
        note: str,
    ) -> Any:
        """Write an override (or clear it), repaint the rows that share the verdict, return the detail."""
        check = self._active_jlc_footprint_check()
        if check is None or check.set_override(reference, rotation, note) is None:
            return None
        self._repaint_jlc_references(check.references_sharing_verdict(reference))
        return part_detail(check, reference)

    def _refetch_jlc_references(self, references: Iterable[str]) -> Any:
        """Re-fetch these parts' EasyEDA data and return the first one's fresh detail."""
        check = self._active_jlc_footprint_check()
        if check is None:
            return None
        wanted = list(references)
        refetch_jlc_footprint_references(self.window, wanted, check)
        self._repaint_jlc_references(wanted)
        return part_detail(check, wanted[0]) if wanted else None

    def _repaint_jlc_references(self, references: Iterable[str]) -> None:
        """Repaint the Rotation text and the JLC glyph of the given references."""
        check = self._active_jlc_footprint_check()
        if check is None:
            return
        for reference in references:
            self.window.partlist_data_model.set_rotation(
                reference, check.display_text(reference) or "raw"
            )
            self._apply_jlc_cell(reference)

    def _jlc_cell_help(self, item: Any) -> str:
        """Return the hover help for a JLC cell: that part's verdict text (spec 16.3)."""
        check = self._active_jlc_footprint_check()
        if check is None:
            return ""
        return cell_help(check, self.window.partlist_data_model.get_reference(item))

    def _apply_jlc_cell(self, reference: str) -> None:
        """Set one row's JLC glyph from the footprint check, or clear it when off."""
        check = self._active_jlc_footprint_check()
        self.window.partlist_data_model.set_jlc_state(
            reference, "" if check is None else glyph_state(check, reference)
        )

    def _active_jlc_footprint_check(self):
        """Return the running footprint check when the setting is on, else None."""
        if not is_footprint_check_enabled(getattr(self.window, "settings", {})):
            return None
        return getattr(self.window, "jlc_footprint_check", None)

    def _rotation_cell_text(self, part: dict[str, Any], corrections) -> str:
        """Return the Rotation column text: the footprint check's decision, else the rule."""
        check = self._active_jlc_footprint_check()
        if check is not None:
            return check.display_text(part["reference"]) or "raw"
        if corrections is None:
            return "Unresolved"
        return str(self.window.get_correction(part, corrections))

    def _refresh_jlc_rotation_cells(self) -> None:
        """Repaint every Rotation and JLC cell from the footprint check's decisions."""
        check = self._active_jlc_footprint_check()
        if check is None:
            return
        model = self.window.partlist_data_model
        for row in model.get_all():
            reference = str(row[model.columns["REF_COL"]] or "")
            model.set_rotation(reference, check.display_text(reference) or "raw")
            self._apply_jlc_cell(reference)

    def read_corrections_for_summary(self):
        """Read the correction rules for the rotation summary's comparison; None when unavailable."""
        snapshot = self.window.library.read_correction_data()
        self.window.update_correction_status(snapshot)
        return snapshot.corrections

    def _append_jlc_footprint_menu(self, parent_menu: Any) -> Any:
        """Append the "JLC footprint" submenu to the context menu (spec 16.3).

        Every entry is disabled when the check is off or its store is unavailable,
        which is also what a board with no project storage looks like; the two
        per-part entries also need a selected part with an LCSC number.
        """
        submenu = wx.Menu()
        available = self._active_jlc_footprint_check() is not None
        selected = available and self._first_selected_jlc_reference() is not None
        for identifier, label, handler, enabled, separator in (
            (
                ID_CONTEXT_MENU_JLC_DETAILS,
                "Details...",
                self.on_jlc_footprint_details,
                selected,
                False,
            ),
            (
                ID_CONTEXT_MENU_JLC_REFETCH,
                "Re-fetch data",
                self.on_jlc_footprint_refetch,
                selected,
                True,
            ),
            (
                ID_CONTEXT_MENU_JLC_RECHECK,
                "Re-check board",
                self.on_jlc_footprint_recheck,
                available,
                False,
            ),
            (
                ID_CONTEXT_MENU_JLC_REFRESH,
                "Refresh board data",
                self.on_jlc_footprint_refresh,
                available,
                False,
            ),
            (
                ID_CONTEXT_MENU_JLC_CLEAR_CACHE,
                "Clear cache",
                self.on_jlc_footprint_clear_cache,
                available,
                False,
            ),
        ):
            item = wx.MenuItem(submenu, identifier, label)
            submenu.Append(item)
            submenu.Bind(wx.EVT_MENU, handler, item)
            item.Enable(bool(enabled))
            if separator:
                submenu.AppendSeparator()
        parent_menu.AppendSubMenu(submenu, "JLC footprint")
        return submenu

    def on_jlc_footprint_details(self, *_: object) -> None:
        """Open the detail dialog for the first selected part with an LCSC."""
        self.show_jlc_footprint_detail()

    def _selected_jlc_references(self) -> list[str]:
        """Return every selected reference that carries an LCSC number."""
        model = self.window.partlist_data_model
        references = []
        for item in self.window.footprint_list.GetSelections():
            reference = str(model.get_reference(item) or "")
            if reference and str(model.get_lcsc(item) or "").strip():
                references.append(reference)
        return references

    def on_jlc_footprint_refetch(self, *_: object) -> None:
        """Re-fetch the EasyEDA data of the selected parts (spec 16.5)."""
        check = self._active_jlc_footprint_check()
        references = self._selected_jlc_references()
        if check is None or not references:
            return
        refetch_jlc_footprint_references(self.window, references, check)
        self._repaint_jlc_references(references)

    def on_jlc_footprint_recheck(self, *_: object) -> None:
        """Re-resolve every cached part on the board, with no network (spec 16.5)."""
        if self._active_jlc_footprint_check() is None:
            return
        recheck_jlc_footprint_board(self.window)
        self._refresh_jlc_rotation_cells()

    def on_jlc_footprint_refresh(self, *_: object) -> None:
        """Forget and re-fetch every part on the board, after a confirmation (spec 16.5)."""
        if self._active_jlc_footprint_check() is None:
            return
        if refresh_jlc_footprint_board_data(self.window):
            self._refresh_jlc_rotation_cells()

    def on_jlc_footprint_clear_cache(self, *_: object) -> None:
        """Delete the whole EasyEDA cache, after a confirmation (spec 16.5)."""
        if self._active_jlc_footprint_check() is None:
            return
        if clear_jlc_footprint_cache(self.window):
            self._refresh_jlc_rotation_cells()
