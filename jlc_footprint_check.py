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
from .jlcfootprint.controller import FootprintCheck
from .jlcfootprint.kicad_adapter import board_parts
from .jlcfootprint.verdicts import VerdictStore

MESSAGE_TITLE = "JLC footprint check"


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

    def read_board():
        return board_parts(
            pcbnew.GetBoard(), pcbnew=pcbnew, counts=count_pad, lcsc_of=get_lcsc_value
        )

    def post(lcsc: str, generation: int) -> None:
        wx.PostEvent(window, JlcFootprintResultEvent(lcsc=lcsc, generation=generation))

    def message(text: str) -> None:
        wx.PostEvent(
            window, MessageEvent(title=MESSAGE_TITLE, text=text, style="warning")
        )

    return FootprintCheck(cache, verdicts, read_board, post, message=message)


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
