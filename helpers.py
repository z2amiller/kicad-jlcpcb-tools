"""Contains helper function used all over the plugin."""

from __future__ import annotations

import os
from pathlib import Path
import re

import wx  # pylint: disable=import-error
import wx.dataview  # pylint: disable=import-error

PLUGIN_PATH = Path(__file__).resolve().parent


def getWxWidgetsVersion():
    """Get wx widgets version."""
    v = re.search(r"wxWidgets\s([\d\.]+)", wx.version())
    v = int(v.group(1).replace(".", ""))
    return v


def getVersion():
    """READ Version from file."""
    if not os.path.isfile(os.path.join(PLUGIN_PATH, "VERSION")):
        return "unknown"
    with open(os.path.join(PLUGIN_PATH, "VERSION"), encoding="utf-8") as f:
        return f.read().strip()


def GetOS():
    """Get String with OS type."""
    return wx.PlatformInformation.Get().GetOperatingSystemIdName()


def GetScaleFactor(window):
    """Workaround if wxWidgets Version does not support GetDPIScaleFactor, for Mac OS always return 1.0."""
    if "Apple Mac OS" in GetOS():
        return 1.0
    if hasattr(window, "GetDPIScaleFactor"):
        return window.GetDPIScaleFactor()
    return 1.0


def HighResWxSize(window, size):
    """Workaround if wxWidgets Version does not support FromDIP."""
    if hasattr(window, "FromDIP"):
        return window.FromDIP(size)
    return size


# The JLC column's two RGB stops per state (spec 16.3): the first reads on a dark
# window background, the second on a light one, like the stock-concern colour.
JLC_CELL_COLOURS = {
    "red": ((255, 128, 128), (176, 0, 0)),
    "yellow": ((255, 211, 102), (128, 52, 0)),
    "caveat": ((255, 211, 102), (128, 52, 0)),
    "unknown": ((176, 176, 176), (102, 102, 102)),
    "paused": ((176, 176, 176), (102, 102, 102)),
    "pending": ((176, 176, 176), (102, 102, 102)),
    "green": ((96, 200, 120), (0, 112, 48)),
    "override": ((124, 176, 255), (0, 82, 204)),
}


def apply_jlc_cell_style(state: str, attr: wx.dataview.DataViewItemAttr) -> bool:
    """Colour and embolden one JLC glyph cell for its state; False for no state.

    Foreground only, like the Side and stock-concern cells: Cocoa retains a custom
    cell background after its attributes clear, and native row striping must stay.
    """
    stops = JLC_CELL_COLOURS.get(state)
    if stops is None:
        return False
    dark, light = stops
    background = wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOW)
    attr.SetColour(wx.Colour(*(dark if background.GetLuminance() < 0.5 else light)))
    if hasattr(attr, "SetBold"):
        attr.SetBold(True)
    return True


def apply_side_cell_style(side: str, attr: wx.dataview.DataViewItemAttr) -> bool:
    """Apply the shared TOP/BOT text color and weight to a Side cell."""
    side_colours = {
        "TOP": wx.Colour(200, 52, 52),
        "BOT": wx.Colour(77, 127, 196),
    }
    colour = side_colours.get(side)
    if colour is None:
        return False
    attr.SetColour(colour)
    if hasattr(attr, "SetBold"):
        attr.SetBold(True)
    return True


def loadBitmapScaled(filename, scale=1.0, static=False):
    """Load a scaled bitmap, handle differences between Kicad versions."""
    if filename:
        path = os.path.join(PLUGIN_PATH, "icons", filename)
        bmp = wx.Bitmap(path)
        w, h = bmp.GetSize()
        img = bmp.ConvertToImage()
        if hasattr(wx.SystemSettings, "GetAppearance") and hasattr(
            wx.SystemSettings.GetAppearance, "IsUsingDarkBackground"
        ):
            if wx.SystemSettings.GetAppearance().IsUsingDarkBackground():
                img.Replace(0, 0, 0, 255, 255, 255)
            bmp = wx.Bitmap(img.Scale(int(w * scale), int(h * scale)))
    else:
        bmp = wx.Bitmap()
    if getWxWidgetsVersion() > 315 and not static:
        return wx.BitmapBundle(bmp)
    return bmp


def loadIconScaled(filename, scale=1.0):
    """Load a scaled icon, handle differences between Kicad versions."""
    bmp = loadBitmapScaled(filename, scale=scale, static=False)
    if getWxWidgetsVersion() > 315:
        return bmp
    return wx.Icon(bmp)


def natural_sort_collation(a, b):
    """Natural sort collation for use in sqlite."""
    if a == b:
        return 0

    def convert(text):
        return int(text) if text.isdigit() else text.lower()

    def alphanum_key(key):
        return [convert(c) for c in re.split("([0-9]+)", key)]

    natorder = sorted([a, b], key=alphanum_key)
    return -1 if natorder.index(a) == 0 else 1


def dict_factory(cursor, row) -> dict:
    """Row factory that returns a dict."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
