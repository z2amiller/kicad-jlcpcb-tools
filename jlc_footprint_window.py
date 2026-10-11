"""The main window's side of the JLC footprint check.

``JLCPCBTools`` owns the hooks -- where the check starts and stops, the binds, the
part list's cells, the generate steps -- and hands everything behind them to a
``JlcFootprintPresenter`` built once per window, which reaches back through
``self.window`` for the part list, the settings and the store.  Keeping the feature
here keeps the window's own changes to small hooks, and gives a reader one file for
the feature instead of a few hundred lines inside a 3,000-line frame.
"""

from __future__ import annotations

from collections.abc import Iterable
import sqlite3
from typing import Any

from .board_context import BoardContextChanged
from .jlc_footprint_check import create_footprint_check, is_footprint_check_enabled


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
        # A board with KiCad design variants is edited in the variant matrix, never
        # through this part list, so the check runs on an ordinary board only, as the
        # opening preferences in _initialize_catalog_parts do.
        if (
            self.window.store is None
            or getattr(self.window, "_variant_controller", None)
            or not is_footprint_check_enabled(self.window.settings)
        ):
            return
        try:
            check = create_footprint_check(self.window, self.window.pcbnew)
            check.start()
            check.scan_board()
        except (sqlite3.Error, OSError) as error:
            self.window.logger.warning("JLC footprint check unavailable: %s", error)
            return
        except BoardContextChanged as error:
            # The scan reads each LCSC through the store, which refuses a closed or
            # replaced board: stop the new check and recover the way every other
            # store read does.
            check.stop()
            self.window._set_project_storage_error(error)
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
        """Repaint the cells the check fills on every row, from its decisions."""
        check = self._active_jlc_footprint_check()
        if check is None:
            return
        model = self.window.partlist_data_model
        for row in model.get_all():
            reference = str(row[model.columns["REF_COL"]] or "")
            model.set_rotation(reference, check.display_text(reference) or "raw")

    def read_corrections_for_summary(self):
        """Read the correction rules for the rotation summary's comparison; None when unavailable."""
        snapshot = self.window.library.read_correction_data()
        self.window.update_correction_status(snapshot)
        return snapshot.corrections
