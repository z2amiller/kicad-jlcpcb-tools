"""The JLC footprint detail dialog: the overlay canvas, the facts and the actions (spec 16.4).

The layout of the canvas and every line of text come from the pure package
(``jlcfootprint.overlay`` and ``jlcfootprint.presentation``), so this module only
builds controls, walks primitives onto a device context and calls back into the
window for the three actions.  The size is remembered in the ``jlcfootprint``
settings section, like the part selector's.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

import wx  # pylint: disable=import-error

from .helpers import HighResWxSize
from .jlcfootprint.overlay import DEFAULT_LAYERS, JLC_PLACED, JLC_RAW, KICAD, overlay
from .jlcfootprint.presentation import (
    OVERRIDE_ANGLES,
    banner,
    dialog_title,
    fit_numbers,
    jlc_facts,
    kicad_facts,
    parse_override,
)

MIN_SIZE = (960, 560)  # spec 16.4's minimum
# What the dialog opens at when nothing is remembered: the canvas at its own minimum
# beside both fact columns in full, which eighteen lines of facts need.
DEFAULT_SIZE = (1180, 760)
CANVAS_MIN = (420, 300)
# Wide enough for "Capacitor_THT:C_Rect_L7.2mm_W3.0mm_P5.00mm_FKS2_FKP2_MKS2_MKP2"
# beside its label without wrapping, which is what the dialog's minimum is for; the
# height is the sizer's business, so it stays unconstrained.
FACTS_MIN = (470, -1)
# The banner's own left and right sizer border, which its wrap width allows for,
# and a few pixels of slack: wx's wrap measurement and its final text layout can
# disagree by a pixel, and a line one pixel over the control soft-wraps in the
# native control and loses its tail off the bottom (measured under KiCad's wx).
BANNER_MARGIN_PX = 8
BANNER_SLACK_PX = 6
LAYER_LABELS = (
    (KICAD, "KiCad pads"),
    (JLC_PLACED, "JLC pads transformed"),
    (JLC_RAW, "JLC pads raw"),
)
# Two RGB stops per role: the first for a dark window background, the second for a
# light one, like the Side and the JLC cell colours.
ROLE_COLOURS = {
    "grid": ((64, 64, 64), (222, 222, 222)),
    "kicad_pad": ((236, 236, 236), (32, 32, 32)),
    "kicad_pin1": ((236, 236, 236), (32, 32, 32)),
    "kicad_plus": ((236, 236, 236), (32, 32, 32)),
    "jlc_pad": ((124, 176, 255), (0, 82, 204)),
    # The pin-1 ring and the "+" marks annotate a filled pad, so they take the text
    # colour: an accent-blue mark on an accent-blue pad cannot be seen (checked on a
    # real canvas under KiCad's wx, 2026-09-17).
    "jlc_pin1": ((236, 236, 236), (32, 32, 32)),
    "jlc_plus": ((236, 236, 236), (32, 32, 32)),
    # The cross lands on JLC's own pad whenever the origin sits over one (M3's U1 on
    # a TO-252 tab), where an accent-blue mark on an accent-blue pad loses half of
    # itself, so it takes the annotations' text colour like the pin-1 ring and the
    # "+" marks rather than the JLC colour spec 17.5 names.
    "jlc_origin": ((236, 236, 236), (32, 32, 32)),
    "jlc_raw_pad": ((150, 150, 150), (128, 128, 128)),
    "scale_bar": ((236, 236, 236), (32, 32, 32)),
    "scale_label": ((236, 236, 236), (32, 32, 32)),
    "angle_label": ((236, 236, 236), (32, 32, 32)),
}
BANNER_COLOURS = {
    "red": ((255, 128, 128), (176, 0, 0)),
    "yellow": ((255, 211, 102), (128, 52, 0)),
    "caveat": ((255, 211, 102), (128, 52, 0)),
    "unknown": ((176, 176, 176), (102, 102, 102)),
    "paused": ((176, 176, 176), (102, 102, 102)),
    "pending": ((176, 176, 176), (102, 102, 102)),
    "green": ((96, 200, 120), (0, 112, 48)),
    "override": ((124, 176, 255), (0, 82, 204)),
}


def is_dark_background() -> bool:
    """Return whether the window background is dark, as the cell styles ask it."""
    return wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOW).GetLuminance() < 0.5


def role_colour(role: str, dark: bool) -> Any:
    """Return the colour one primitive role draws in for this theme."""
    stops = ROLE_COLOURS.get(role)
    if stops is None:
        return wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOWTEXT)
    return wx.Colour(*(stops[0] if dark else stops[1]))


def draw_primitives(dc: Any, primitives: Any, dark: Optional[bool] = None) -> int:  # noqa: UP045
    """Draw one overlay's primitives onto a device context; return how many were drawn.

    A recording double can stand in for the context: only ``SetPen``, ``SetBrush``,
    ``SetTextForeground``, ``DrawRectangle``, ``DrawCircle``, ``DrawLine`` and
    ``DrawText`` are used, in that vocabulary.
    """
    theme_dark = is_dark_background() if dark is None else dark
    drawn = 0
    for item in primitives:
        colour = role_colour(item.role, theme_dark)
        if item.kind == "rect":
            dc.SetPen(wx.Pen(colour, 1))
            filled = item.role in ("jlc_pad", "jlc_raw_pad")
            dc.SetBrush(
                wx.Brush(colour) if filled else wx.Brush(colour, wx.TRANSPARENT)
            )
            dc.DrawRectangle(
                int(round(item.x)),
                int(round(item.y)),
                max(1, int(round(item.width))),
                max(1, int(round(item.height))),
            )
        elif item.kind == "circle":
            dc.SetPen(wx.Pen(colour, 1))
            hollow = item.role == "jlc_pin1"
            dc.SetBrush(
                wx.Brush(colour, wx.TRANSPARENT) if hollow else wx.Brush(colour)
            )
            dc.DrawCircle(
                int(round(item.x)), int(round(item.y)), int(round(item.radius))
            )
        elif item.kind == "line":
            dc.SetPen(wx.Pen(colour, 1))
            dc.DrawLine(
                int(round(item.x)),
                int(round(item.y)),
                int(round(item.x2)),
                int(round(item.y2)),
            )
        elif item.kind == "text":
            dc.SetTextForeground(colour)
            dc.DrawText(item.text, int(round(item.x)), int(round(item.y)))
        else:
            continue
        drawn += 1
    return drawn


class OverlayCanvas(wx.Panel):
    """The pad overlay: it asks the pure layout for primitives on every paint."""

    def __init__(self, parent: Any, detail: Any, minimum: tuple = CANVAS_MIN) -> None:
        wx.Panel.__init__(self, parent, style=wx.BORDER_THEME)
        self.detail = detail
        self.layers = list(DEFAULT_LAYERS)
        self.last_overlay = None
        self.SetMinSize(HighResWxSize(parent, wx.Size(*minimum)))
        self.Bind(wx.EVT_PAINT, self.on_paint)
        self.Bind(wx.EVT_SIZE, self.on_size)

    def set_layer(self, layer: str, shown: bool) -> None:
        """Switch one layer on or off and repaint."""
        if shown and layer not in self.layers:
            self.layers.append(layer)
        elif not shown and layer in self.layers:
            self.layers.remove(layer)
        self.Refresh()

    def set_detail(self, detail: Any) -> None:
        """Draw a different detail (after an override or a re-fetch) and repaint."""
        self.detail = detail
        self.Refresh()

    def build_overlay(self) -> Any:
        """Return the primitives for the current size and layers."""
        size = self.GetClientSize()
        width = max(int(size.GetWidth()), 1)
        height = max(int(size.GetHeight()), 1)
        self.last_overlay = overlay(self.detail, (width, height), tuple(self.layers))
        return self.last_overlay

    def on_paint(self, _event: Any) -> None:
        """Paint the overlay."""
        dc = wx.PaintDC(self)
        dc.SetBackground(wx.Brush(wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOW)))
        dc.Clear()
        draw_primitives(dc, self.build_overlay().primitives)

    def on_size(self, event: Any) -> None:
        """Rescale on resize."""
        self.Refresh()
        event.Skip()


class OverrideDialog(wx.Dialog):
    """Ask for an override angle and a note (spec 16.4's "Set override…")."""

    def __init__(
        self,
        parent: Any,
        rotation: Optional[int] = None,  # noqa: UP045
        note: str = "",
    ) -> None:
        wx.Dialog.__init__(
            self,
            parent,
            title="Set the rotation override",
            style=wx.DEFAULT_DIALOG_STYLE,
        )
        self.angle = wx.ComboBox(
            self,
            value="0" if rotation is None else str(rotation),
            choices=[str(angle) for angle in OVERRIDE_ANGLES],
        )
        self.note = wx.TextCtrl(self, value=note)
        self.message = wx.StaticText(self, label="")
        grid = wx.FlexGridSizer(0, 2, 5, 5)
        grid.AddGrowableCol(1)
        grid.Add(
            wx.StaticText(self, label="Rotation (degrees CCW)"),
            0,
            wx.ALIGN_CENTER_VERTICAL,
        )
        grid.Add(self.angle, 1, wx.EXPAND)
        grid.Add(wx.StaticText(self, label="Note"), 0, wx.ALIGN_CENTER_VERTICAL)
        grid.Add(self.note, 1, wx.EXPAND)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(grid, 0, wx.ALL | wx.EXPAND, 8)
        sizer.Add(self.message, 0, wx.LEFT | wx.RIGHT | wx.EXPAND, 8)
        sizer.Add(
            self.CreateStdDialogButtonSizer(wx.OK | wx.CANCEL), 0, wx.ALL | wx.EXPAND, 8
        )
        self.SetSizerAndFit(sizer)
        self.Bind(wx.EVT_BUTTON, self.on_ok, id=wx.ID_OK)

    def override_angle(self) -> Optional[int]:  # noqa: UP045
        """Return the angle the field holds, or None when it is not an angle."""
        return parse_override(self.angle.GetValue())

    def on_ok(self, event: Any) -> None:
        """Accept an integer angle; refuse anything else without closing."""
        if self.override_angle() is None:
            self.message.SetLabel("Enter a whole number of degrees, for example 180.")
            return
        event.Skip()


class JlcFootprintDetailDialog(wx.Dialog):
    """One part's JLC footprint detail (spec 16.4)."""

    def __init__(
        self,
        parent: Any,
        detail: Any,
        set_override: Optional[Callable[[Optional[int], str], Any]] = None,  # noqa: UP045
        refetch: Optional[Callable[[], Any]] = None,  # noqa: UP045
        settings: Optional[dict] = None,  # noqa: UP045
    ) -> None:
        self.detail = detail
        self._set_override = set_override
        self._refetch = refetch
        self.settings = settings if settings is not None else {}
        wx.Dialog.__init__(
            self,
            parent,
            title=dialog_title(detail),
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER,
        )
        self.SetMinSize(HighResWxSize(parent, wx.Size(*MIN_SIZE)))
        self.banner = wx.StaticText(self, label="")
        self._banner_text = ""
        self._wrapped_at = 0
        self.cpl = wx.StaticText(self, label="")
        self.canvas = OverlayCanvas(self, detail)
        self.kicad_panel = self._facts_panel("KiCad")
        self.jlc_panel = self._facts_panel("JLC")
        self.numbers = wx.StaticText(self, label="")
        self.override_button = wx.Button(self, label="Set override…")
        self.clear_button = wx.Button(self, label="Clear override")
        self.refetch_button = wx.Button(self, label="Re-fetch")
        close = wx.Button(self, id=wx.ID_CANCEL, label="Close")
        self.checkboxes = {}
        layer_sizer = wx.BoxSizer(wx.HORIZONTAL)
        for layer, label in LAYER_LABELS:
            box = wx.CheckBox(self, label=label)
            box.SetValue(layer in DEFAULT_LAYERS)
            box.Bind(
                wx.EVT_CHECKBOX,
                lambda event, layer=layer: self.canvas.set_layer(
                    layer, event.IsChecked()
                ),
            )
            self.checkboxes[layer] = box
            layer_sizer.Add(box, 0, wx.RIGHT, 10)
        middle = wx.BoxSizer(wx.HORIZONTAL)
        canvas_sizer = wx.BoxSizer(wx.VERTICAL)
        canvas_sizer.Add(self.canvas, 1, wx.EXPAND)
        canvas_sizer.Add(layer_sizer, 0, wx.TOP, 5)
        canvas_sizer.Add(self.numbers, 0, wx.TOP, 5)
        middle.Add(canvas_sizer, 1, wx.EXPAND | wx.RIGHT, 8)
        facts = wx.BoxSizer(wx.VERTICAL)
        facts.Add(self.kicad_panel, 0, wx.EXPAND | wx.BOTTOM, 8)
        facts.Add(self.jlc_panel, 0, wx.EXPAND)
        middle.Add(facts, 1, wx.EXPAND)
        buttons = wx.BoxSizer(wx.HORIZONTAL)
        buttons.Add(self.override_button, 0, wx.RIGHT, 5)
        buttons.Add(self.clear_button, 0, wx.RIGHT, 5)
        buttons.Add(self.refetch_button, 0, wx.RIGHT, 5)
        buttons.AddStretchSpacer()
        buttons.Add(close, 0)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.banner, 0, wx.ALL | wx.EXPAND, 8)
        sizer.Add(self.cpl, 0, wx.LEFT | wx.RIGHT | wx.BOTTOM | wx.EXPAND, 8)
        sizer.Add(middle, 1, wx.LEFT | wx.RIGHT | wx.EXPAND, 8)
        sizer.Add(buttons, 0, wx.ALL | wx.EXPAND, 8)
        self.SetSizer(sizer)
        self.override_button.Bind(wx.EVT_BUTTON, self.on_set_override)
        self.clear_button.Bind(wx.EVT_BUTTON, self.on_clear_override)
        self.refetch_button.Bind(wx.EVT_BUTTON, self.on_refetch)
        self.Bind(wx.EVT_CLOSE, self.on_close)
        self.Bind(wx.EVT_SIZE, self.on_resize)
        self.update(detail)
        self._restore_size()

    # ------------------------------------------------------------------
    # Contents
    # ------------------------------------------------------------------

    def _facts_panel(self, heading: str) -> Any:
        """Return an empty panel with a heading and a grid the facts fill."""
        panel = wx.Panel(self)
        panel.SetMinSize(HighResWxSize(self, wx.Size(*FACTS_MIN)))
        panel.heading = wx.StaticText(panel, label=heading)
        # Rows 0: as many as the facts need. A fixed row count makes wx assert
        # ("too many items ... in grid sizer") as soon as a panel gains a line.
        panel.grid = wx.FlexGridSizer(0, 2, 3, 8)
        panel.grid.AddGrowableCol(1)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(panel.heading, 0, wx.BOTTOM, 4)
        sizer.Add(panel.grid, 1, wx.EXPAND)
        panel.SetSizer(sizer)
        return panel

    @staticmethod
    def _fill(panel: Any, facts: Any) -> None:
        """Replace one panel's label/value pairs."""
        panel.grid.Clear(delete_windows=True)
        for label, value in facts:
            panel.grid.Add(wx.StaticText(panel, label=f"{label}:"), 0)
            text = wx.StaticText(panel, label=str(value))
            panel.grid.Add(text, 1, wx.EXPAND)
        panel.Layout()

    def update(self, detail: Any) -> None:
        """Show a (possibly new) detail: banner, both columns, the numbers, the canvas."""
        self.detail = detail
        state, text, cpl = banner(detail)
        self.canvas.set_detail(detail)
        # Spec 16.4: when there is no transform, the banner says why.
        note = self.canvas.build_overlay().note
        if note:
            text = f"{text} {note[0].upper()}{note[1:]}."
        self._banner_text = text
        self._wrapped_at = 0
        self._wrap_banner()
        stops = BANNER_COLOURS.get(state)
        if stops is not None:
            dark, light = stops
            self.banner.SetForegroundColour(
                wx.Colour(*(dark if is_dark_background() else light))
            )
        self.cpl.SetLabel(cpl)
        self._fill(self.kicad_panel, kicad_facts(detail))
        self._fill(
            self.jlc_panel,
            jlc_facts(detail, bool(self._section().get("exact_origin", False))),
        )
        numbers = [f"{label} {value}" for label, value in fit_numbers(detail)]
        self.numbers.SetLabel("   ".join(numbers[:3]) + "\n" + "   ".join(numbers[3:]))
        stored = detail.stored
        self.clear_button.Enable(
            stored is not None and stored.override_rotation is not None
        )
        self.override_button.Enable(bool(detail.lcsc))
        self.refetch_button.Enable(bool(detail.lcsc))
        self.Layout()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def on_set_override(self, _event: Any) -> None:
        """Ask for an angle and a note, then hand them to the window."""
        stored = self.detail.stored
        dialog = OverrideDialog(
            self,
            rotation=None if stored is None else stored.override_rotation,
            note="" if stored is None else (stored.override_note or ""),
        )
        try:
            if dialog.ShowModal() != wx.ID_OK:
                return
            rotation = dialog.override_angle()
            note = dialog.note.GetValue()
        finally:
            dialog.Destroy()
        if rotation is None or self._set_override is None:
            return
        updated = self._set_override(rotation, note)
        if updated is not None:
            self.update(updated)

    def on_clear_override(self, _event: Any) -> None:
        """Clear the override and redraw."""
        if self._set_override is None:
            return
        updated = self._set_override(None, "")
        if updated is not None:
            self.update(updated)

    def on_refetch(self, _event: Any) -> None:
        """Re-fetch this part's EasyEDA data and redraw what is known now."""
        if self._refetch is None:
            return
        updated = self._refetch()
        if updated is not None:
            self.update(updated)

    # ------------------------------------------------------------------
    # Geometry
    # ------------------------------------------------------------------

    def _section(self) -> dict:
        """Return the settings section this dialog remembers its size in."""
        return self.settings.setdefault("jlcfootprint", {})

    def _restore_size(self) -> None:
        """Restore the remembered size, never smaller than the minimum."""
        size = self._section().get("detail_size")
        minimum = self.GetMinSize()
        if (
            isinstance(size, list)
            and len(size) == 2
            and all(type(value) is int and value > 0 for value in size)
        ):
            wanted = HighResWxSize(self, wx.Size(size[0], size[1]))
        else:
            wanted = HighResWxSize(self, wx.Size(*DEFAULT_SIZE))
        self.SetSize(
            wx.Size(
                max(wanted.GetWidth(), minimum.GetWidth()),
                max(wanted.GetHeight(), minimum.GetHeight()),
            )
        )
        self.Layout()
        self.Centre(wx.BOTH)

    def remember_size(self) -> None:
        """Store the dialog's size in the layout settings (spec 16.4)."""
        if self.IsMaximized() or self.IsIconized():
            return
        size = self.GetSize()
        to_dip = getattr(self, "ToDIP", None)
        logical = to_dip(size) if callable(to_dip) else size
        self._section()["detail_size"] = [
            int(logical.GetWidth()),
            int(logical.GetHeight()),
        ]

    def _wrap_banner(self) -> None:
        """Re-flow the banner over as many lines as the dialog's width needs.

        A plain ``wx.StaticText`` clips a long verdict sentence rather than
        wrapping it, and spec 16.4 wants the whole banner readable -- including the
        sentence saying why there is no transform, which is the tail that a refusal
        loses.  Wrapping keeps the tint, which ``SetForegroundColour`` sets on the
        same control.
        """
        width = self.GetClientSize().GetWidth() - 2 * BANNER_MARGIN_PX - BANNER_SLACK_PX
        if width <= 0 or width == self._wrapped_at:
            return
        self._wrapped_at = width
        self.banner.SetLabel(self._banner_text)
        self.banner.Wrap(width)

    def on_resize(self, event: Any) -> None:
        """Re-wrap the banner for the new width and lay the dialog out again."""
        self._wrap_banner()
        self.Layout()
        event.Skip()

    def on_close(self, event: Any) -> None:
        """Remember the size, then let the dialog close."""
        self.remember_size()
        event.Skip()
