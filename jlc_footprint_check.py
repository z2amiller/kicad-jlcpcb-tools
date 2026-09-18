"""Wire the JLC footprint check into the plugin (spec sections 9 and 10).

The pure package under ``jlcfootprint/`` knows nothing of wx, pcbnew or the main
window; this module builds one :class:`FootprintCheck` for an open board from the
window's library, store and pcbnew, and posts its results as wx events.
"""

from __future__ import annotations

from collections.abc import Iterable
import logging
import os
from typing import Any

import wx  # pylint: disable=import-error

from .events import JlcFootprintResultEvent, MessageEvent
from .footprint_helpers import get_lcsc_value
from .footprint_metadata import count_pad
from .jlcfootprint.cache import FILENAME, Cache, SeedImportResult
from .jlcfootprint.controller import FootprintCheck, describe_seconds
from .jlcfootprint.kicad_adapter import board_parts
from .jlcfootprint.presentation import describe_board_estimate
from .jlcfootprint.report import CplRotation, format_summary, summarise
from .jlcfootprint.verdicts import VerdictStore
from .jlcfootprint.worker import Buckets

MESSAGE_TITLE = "JLC footprint check"
POLL_MS = 200

logger = logging.getLogger(__name__)

# One request budget per plugin session, however often the check is restarted.
_shared_buckets: Buckets | None = None


def shared_buckets() -> Buckets:
    """Return the session's token buckets, created on first use."""
    global _shared_buckets  # noqa: PLW0603
    if _shared_buckets is None:
        _shared_buckets = Buckets.default()
    return _shared_buckets


def is_footprint_check_enabled(settings: dict) -> bool:
    """Return the setting that switches the resolver path on (spec section 9)."""
    return bool(settings.get("jlcfootprint", {}).get("enabled", True))


def cache_path(datadir: str) -> str:
    """Return the global cache file beside the library's other databases."""
    return os.path.join(datadir, FILENAME)


def create_footprint_check(window: Any, pcbnew: Any) -> FootprintCheck:
    """Build the check for the window's open board; call ``start`` and ``scan_board`` next."""
    cache = Cache(cache_path(window.library.datadir))
    verdicts = VerdictStore(window.store.dbfile)

    def lcsc_of(footprint):
        # The BOM orders the project database's LCSC, which can differ from the
        # footprint's field when the database has priority; judge that part.
        part = window.store.get_part(str(footprint.GetReference()))
        stored = str((part or {}).get("lcsc") or "").strip()
        return stored or get_lcsc_value(footprint)

    def read_board():
        return board_parts(
            pcbnew.GetBoard(), pcbnew=pcbnew, counts=count_pad, lcsc_of=lcsc_of
        )

    def post(lcsc: str, generation: int) -> None:
        wx.PostEvent(window, JlcFootprintResultEvent(lcsc=lcsc, generation=generation))

    def message(text: str) -> None:
        wx.PostEvent(
            window, MessageEvent(title=MESSAGE_TITLE, text=text, style="warning")
        )

    return FootprintCheck(
        cache, verdicts, read_board, post, message=message, buckets=shared_buckets()
    )


def import_seed(window: Any, path: str) -> SeedImportResult:
    """Merge a seed file into the cache and let a running check resolve from it."""
    check = getattr(window, "jlc_footprint_check", None)
    cache = (
        check.cache if check is not None else Cache(cache_path(window.library.datadir))
    )
    result = cache.import_seed(path)
    if check is not None:
        check.scan_board()
    return result


def wait_for_pending_fetches(window: Any, check: FootprintCheck) -> bool:
    """Wait with a cancellable dialog until no part is pending (spec section 8).

    Returns False when the user cancels or the breaker has tripped; the caller then
    emits the pending parts with their raw angle.
    """
    pending = check.pending_references()
    if not pending:
        return True
    total = len(pending)
    dialog = wx.ProgressDialog(
        MESSAGE_TITLE,
        f"Waiting for EasyEDA data for {total} part(s)...",
        maximum=total,
        parent=window,
        style=wx.PD_APP_MODAL | wx.PD_CAN_ABORT | wx.PD_AUTO_HIDE | wx.PD_ELAPSED_TIME,
    )
    try:
        while pending:
            if check.worker.tripped:
                return False
            listed = ", ".join(pending[:6]) + (", ..." if len(pending) > 6 else "")
            requests, seconds = check.queue_estimate()
            keep_going, _skip = dialog.Update(
                total - len(pending),
                f"Waiting for EasyEDA data: {len(pending)} of {total} part(s) left ({listed});"
                f" {requests} request(s) queued, about {describe_seconds(seconds)}",
            )
            if not keep_going:
                return False
            wx.MilliSleep(POLL_MS)
            pending = check.pending_references()
        return True
    finally:
        dialog.Destroy()


# ---------------------------------------------------------------------------
# The four actions of spec 16.5.  The window's menu calls these; the tests call
# them directly against a real cache and verdict store in a temporary directory.
# ---------------------------------------------------------------------------


def _check(window: Any, check: Any = None) -> Any:
    """Return the check to act on: the one passed, else the window's running one."""
    return check if check is not None else getattr(window, "jlc_footprint_check", None)


def confirm(window: Any, question: str) -> bool:
    """Ask a yes/no question before a board-wide action (spec 16.5)."""
    dialog = wx.MessageDialog(
        window, question, MESSAGE_TITLE, wx.YES_NO | wx.NO_DEFAULT | wx.ICON_QUESTION
    )
    try:
        return dialog.ShowModal() == wx.ID_YES
    finally:
        dialog.Destroy()


def refetch_references(
    window: Any, references: Iterable[str], check: Any = None
) -> int:
    """Re-fetch the EasyEDA data of the given parts; return how many were enqueued."""
    check = _check(window, check)
    if check is None:
        return 0
    wanted = list(references)
    summary = check.refetch(wanted)
    logger.info(
        "JLC footprint check: re-fetching %d part(s): %s",
        summary.enqueued,
        ", ".join(wanted) or "none",
    )
    return summary.enqueued


def recheck_board(window: Any, check: Any = None) -> int:
    """Re-resolve every cached part on the board with no network; return how many."""
    check = _check(window, check)
    if check is None:
        return 0
    checked = check.recheck_board()
    logger.info("JLC footprint check: re-checked %d part(s)", checked)
    return checked


def refresh_board_data(window: Any, check: Any = None) -> int:
    """Confirm, then forget and re-fetch every part on the board; return how many."""
    check = _check(window, check)
    if check is None:
        return 0
    parts, seconds = check.board_estimate()
    if not parts:
        logger.info("JLC footprint check: no part on the board carries an LCSC number")
        return 0
    if not confirm(
        window,
        "Re-fetch the EasyEDA data for every part on the board?\n\n"
        f"{describe_board_estimate(parts, seconds)}. Rotation overrides are kept.",
    ):
        return 0
    summary = check.refresh_board()
    logger.info(
        "JLC footprint check: refreshing %d part(s), %d enqueued",
        parts,
        summary.enqueued,
    )
    return parts


def clear_cache(window: Any, check: Any = None) -> int:
    """Confirm, then delete every cached row and rescan; return the rows deleted."""
    check = _check(window, check)
    if check is None:
        return 0
    counts = check.cache.counts()
    parts, seconds = check.board_estimate()
    if not confirm(
        window,
        f"Delete the whole EasyEDA cache ({counts['parts']} part(s), "
        f"{counts['packages']} footprint(s))?\n\nThis board's parts are fetched again: "
        f"{describe_board_estimate(parts, seconds)}. Rotation overrides are kept, and "
        "a seed file can be imported again from Settings.",
    ):
        return 0
    summary = check.clear_cache()
    logger.info(
        "JLC footprint check: cleared the cache (%d part(s), %d footprint(s)), %d enqueued",
        counts["parts"],
        counts["packages"],
        summary.enqueued,
    )
    return counts["parts"]


class GenerateSummaryDialog(wx.Dialog):
    """A scrolled, read-only view of the rotation summary."""

    def __init__(self, parent: Any, text: str) -> None:
        wx.Dialog.__init__(
            self,
            parent,
            id=wx.ID_ANY,
            title="JLC footprint check: rotations in the CPL",
            size=wx.Size(900, 600),
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER,
        )
        view = wx.TextCtrl(
            self,
            wx.ID_ANY,
            text,
            style=wx.TE_MULTILINE | wx.TE_READONLY | wx.TE_DONTWRAP,
        )
        view.SetFont(wx.Font(wx.FontInfo(10).Family(wx.FONTFAMILY_TELETYPE)))
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(view, 1, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.CreateStdDialogButtonSizer(wx.OK), 0, wx.ALL | wx.EXPAND, 5)
        self.SetSizer(sizer)


def show_generate_summary(
    window: Any, rows: list[CplRotation], legacy_available: bool
) -> str:
    """Show the three-group rotation summary after the CPL is written; return its text."""
    text = format_summary(summarise(rows, legacy_available))
    dialog = GenerateSummaryDialog(window, text)
    try:
        dialog.ShowModal()
    finally:
        dialog.Destroy()
    return text
