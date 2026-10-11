"""Wire the JLC footprint check into the plugin.

The pure package under ``jlcfootprint/`` knows nothing of wx, pcbnew or the main
window; this module builds one :class:`FootprintCheck` for an open board from the
window's library, store and pcbnew, and posts its results as wx events.  It also
holds the two steps generation adds while the check is on: the wait for parts still
being fetched, and the rotation summary with its question before any file is
written.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import wx  # pylint: disable=import-error

from .events import JlcFootprintResultEvent, MessageEvent
from .footprint_helpers import get_lcsc_value, get_valid_footprints
from .jlcfootprint.cache import FILENAME, Cache
from .jlcfootprint.controller import FootprintCheck, describe_seconds
from .jlcfootprint.kicad_adapter import board_parts
from .jlcfootprint.report import (
    CplRotation,
    GenerateSummary,
    raw_angle_reason,
    summarise,
    summary_lines,
)
from .jlcfootprint.verdicts import VerdictStore
from .jlcfootprint.worker import Buckets

MESSAGE_TITLE = "JLC footprint check"
POLL_MS = 200

logger = logging.getLogger(__name__)

# One request budget per plugin session, however often the check is restarted.
_shared_buckets: Optional[Buckets] = None


def shared_buckets() -> Buckets:
    """Return the session's token buckets, created on first use."""
    global _shared_buckets  # noqa: PLW0603
    if _shared_buckets is None:
        _shared_buckets = Buckets.default()
    return _shared_buckets


def is_footprint_check_enabled(settings: dict) -> bool:
    """Return whether the CPL takes its rotations from the check (``jlcfootprint.enabled``)."""
    return bool(settings.get("jlcfootprint", {}).get("enabled", False))


def is_exact_origin_enabled(settings: dict) -> bool:
    """Return whether the CPL places checked parts at JLC's package origin.

    Off by default, and only ever consulted while the check is on: a part's origin
    comes from its verdict, which generation by the correction rules does not have.
    """
    return bool(settings.get("jlcfootprint", {}).get("exact_origin", False))


def cache_path(datadir: str) -> str:
    """Return the global cache file beside the library's other databases."""
    return os.path.join(datadir, FILENAME)


def create_footprint_check(window: Any, pcbnew: Any) -> FootprintCheck:
    """Build the check for the window's open board; call ``start`` and ``scan_board`` next."""
    cache = Cache(cache_path(window.library.datadir))
    verdicts = VerdictStore(window.store.dbfile)

    def lcsc_of(footprint):
        # Judge the part the BOM orders: the store's LCSC for this reference (the
        # board's own assignment), else the footprint's field.
        part = window.store.get_part(str(footprint.GetReference()))
        stored = str((part or {}).get("lcsc") or "").strip()
        return stored or get_lcsc_value(footprint)

    def read_board():
        return board_parts(
            get_valid_footprints(pcbnew.GetBoard()), pcbnew=pcbnew, lcsc_of=lcsc_of
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


def wait_for_pending_fetches(window: Any, check: FootprintCheck) -> bool:
    """Wait with a cancellable dialog until no part is pending.

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


# At most this many parts are named in the prompt before generation writes any
# file; the log carries every one.
PROMPT_LIMIT = 10


def log_rotation_summary(summary: GenerateSummary) -> None:
    """Log the rotation summary line by line, worst first.

    A part the CPL emits at the raw angle against the check
    (:func:`raw_angle_reason`) is logged as a warning, everything else at info.
    """
    logger.info("JLC footprint check: rotations in the CPL")
    for row, text in summary_lines(summary):
        if not text:
            continue
        warn = row is not None and raw_angle_reason(row) is not None
        logger.log(logging.WARNING if warn else logging.INFO, "%s", text)


def confirm_cpl_rotations(
    window: Any, rows: list[CplRotation], legacy_available: bool
) -> bool:
    """Log the CPL's rotations, then ask before writing if any part goes out raw.

    Called once the placements are prepared and before any file is written.  The
    question comes only when the CPL would place at least one part at KiCad's raw
    angle because the check refused it or could not decide it; Cancel returns
    False and the caller stops.  The window's own settings say whether solved parts
    are placed at JLC's package origin, so the summary can report how many parts
    sit there and how many at the pad-box centre without the caller passing the
    setting down.
    """
    exact_origin = is_exact_origin_enabled(getattr(window, "settings", {}) or {})
    log_rotation_summary(summarise(rows, legacy_available, exact_origin))
    named = [(row, raw_angle_reason(row)) for row in rows]
    named = [(row, reason) for row, reason in named if reason is not None]
    if not named:
        return True
    listed = [
        f"  {row.reference}  {row.lcsc}  {reason}"
        for row, reason in named[:PROMPT_LIMIT]
    ]
    if len(named) > PROMPT_LIMIT:
        listed.append(f"  and {len(named) - PROMPT_LIMIT} more")
    dialog = wx.MessageDialog(
        window,
        f"{len(named)} part(s) would be placed at KiCad's raw angle because the "
        "JLC footprint check did not accept their rotation:\n\n"
        + "\n".join(listed)
        + "\n\nDetails are in the log. Continue?",
        MESSAGE_TITLE,
        wx.YES_NO | wx.NO_DEFAULT | wx.ICON_WARNING | wx.CENTER,
    )
    try:
        dialog.SetYesNoLabels("Continue Anyway", "Cancel Export")
        result = dialog.ShowModal()
    finally:
        dialog.Destroy()
    logger.warning(
        "JLC footprint check: %d part(s) at KiCad's raw angle, user chose to %s export",
        len(named),
        "continue" if result == wx.ID_YES else "stop",
    )
    return result == wx.ID_YES
