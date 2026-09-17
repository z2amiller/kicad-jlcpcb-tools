"""Wire the JLC footprint check into the plugin (spec sections 9 and 10).

The pure package under ``jlcfootprint/`` knows nothing of wx, pcbnew or the main
window; this module builds one :class:`FootprintCheck` for an open board from the
window's library, store and pcbnew, and posts its results as wx events.
"""

from __future__ import annotations

import os
from typing import Any

import wx  # pylint: disable=import-error

from .events import JlcFootprintResultEvent, MessageEvent
from .footprint_helpers import get_lcsc_value
from .footprint_metadata import count_pad
from .jlcfootprint.cache import FILENAME, Cache, SeedImportResult
from .jlcfootprint.controller import FootprintCheck, describe_seconds
from .jlcfootprint.kicad_adapter import board_parts
from .jlcfootprint.report import CplRotation, format_summary, summarise
from .jlcfootprint.verdicts import VerdictStore
from .jlcfootprint.worker import Buckets

MESSAGE_TITLE = "JLC footprint check"
POLL_MS = 200

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
