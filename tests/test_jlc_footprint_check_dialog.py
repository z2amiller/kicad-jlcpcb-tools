"""Tests for the JLC detail dialog and both of its opening paths (spec 16.4), under fake wx."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from jlcfootprint.controller import Decision, FetchState, PartDetail
from jlcfootprint.drawing import DrawingMarks
from jlcfootprint.easyeda_parse import SymbolPin
from jlcfootprint.geometry import Pad
from jlcfootprint.overlay import JLC_PLACED, JLC_RAW, KICAD
from jlcfootprint.resolver import resolve
from jlcfootprint.verdicts import StoredVerdict

from . import test_window_layout as layout
from .wx_harness import load_siblings, package_stubs, wx_stubs

KICAD_PADS = [
    Pad("1", -1.0, 0.0, 1.2, 1.4, 0.0, "+"),
    Pad("2", 1.0, 0.0, 1.2, 1.4, 0.0, "-"),
]
JLC_PADS = [Pad("1", -1.0, 0.0, 1.3, 1.5), Pad("2", 1.0, 0.0, 1.3, 1.5)]
MARKS = DrawingMarks(
    positive_pin="1", positive_pad="1", body_box=(-1.0, -0.7, 1.0, 0.7)
)


# ---------------------------------------------------------------------------
# The wx doubles
# ---------------------------------------------------------------------------


class _Sizer:
    """Record what a sizer was given, in order."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.items: list = []

    def Add(self, item: Any, *_args: Any, **_kwargs: Any) -> None:
        """Keep the control or sizer added."""
        self.items.append(item)

    def AddStretchSpacer(self, *_args: Any) -> None:
        """Accept the spacer."""

    def AddGrowableCol(self, *_args: Any) -> None:
        """Accept the growable column."""

    def Clear(self, delete_windows: bool = False) -> None:
        """Empty the sizer the way the facts panels do."""
        self.items = []


class _Size(tuple):
    """A size with wx's accessors."""

    def __new__(cls, width: int = 0, height: int = 0) -> _Size:
        return super().__new__(cls, (width, height))

    def GetWidth(self) -> int:
        """Return the width."""
        return self[0]

    def GetHeight(self) -> int:
        """Return the height."""
        return self[1]


class _Window:
    """A stateful stand-in for the simple controls the dialog builds."""

    def __init__(self, parent: Any = None, *args: Any, **kwargs: Any) -> None:
        self.parent = parent
        self.label = str(kwargs.get("label", args[1] if len(args) > 1 else ""))
        self.value = str(kwargs.get("value", ""))
        self.choices = list(kwargs.get("choices", ()))
        self.checked = False
        self.enabled = True
        self.colour = None
        self.bindings: dict = {}
        self.sizer = None
        self.size = _Size(0, 0)
        self.min_size = _Size(0, 0)
        self.client_size = _Size(420, 300)
        self.laid_out = 0
        self.refreshed = 0
        self.destroyed = False
        self.maximized = False
        self.iconized = False
        self.wrapped_at = None

    # -- labels and values
    def SetLabel(self, text: str) -> None:
        """Set the label."""
        self.label = str(text)

    def GetLabel(self) -> str:
        """Return the label."""
        return self.label

    def SetValue(self, value: Any) -> None:
        """Set the value (a text field or a checkbox)."""
        if isinstance(value, bool):
            self.checked = value
        else:
            self.value = str(value)

    def GetValue(self) -> Any:
        """Return the value."""
        return (
            self.checked
            if self.choices == [] and self.value == "" and self.checked
            else self.value
        )

    def IsChecked(self) -> bool:
        """Return the checkbox state."""
        return self.checked

    def Enable(self, enabled: bool = True) -> None:
        """Enable or disable the control."""
        self.enabled = bool(enabled)

    def IsEnabled(self) -> bool:
        """Return whether the control is enabled."""
        return self.enabled

    def SetForegroundColour(self, colour: Any) -> None:
        """Keep the colour the banner was tinted with."""
        self.colour = colour

    def Wrap(self, width: int) -> None:
        """Record the width the label was wrapped to, as wx.StaticText.Wrap does."""
        self.wrapped_at = width

    # -- geometry and events
    def Bind(self, event: Any, handler: Any, **_kwargs: Any) -> None:
        """Record a binding so a test can fire it."""
        self.bindings.setdefault(event, []).append(handler)

    def fire(self, event: Any, payload: Any = None) -> None:
        """Call every handler bound to one event."""
        for handler in self.bindings.get(event, []):
            handler(
                payload if payload is not None else SimpleNamespace(Skip=lambda: None)
            )

    def SetMinSize(self, size: Any) -> None:
        """Keep the minimum size."""
        self.min_size = size

    def GetMinSize(self) -> Any:
        """Return the minimum size."""
        return self.min_size

    def SetSize(self, size: Any) -> None:
        """Keep the size."""
        self.size = size

    def GetSize(self) -> Any:
        """Return the size."""
        return self.size

    def GetClientSize(self) -> Any:
        """Return the client size the canvas measures itself by."""
        return self.client_size

    def SetSizer(self, sizer: Any) -> None:
        """Keep the sizer."""
        self.sizer = sizer

    def SetSizerAndFit(self, sizer: Any) -> None:
        """Keep the sizer."""
        self.sizer = sizer

    def Layout(self) -> None:
        """Count layouts."""
        self.laid_out += 1

    def Refresh(self) -> None:
        """Count repaints."""
        self.refreshed += 1

    def Centre(self, *_args: Any) -> None:
        """Accept centring."""

    def Destroy(self) -> None:
        """Mark the window destroyed."""
        self.destroyed = True

    def IsMaximized(self) -> bool:
        """Return whether the window is zoomed."""
        return self.maximized

    def IsIconized(self) -> bool:
        """Return whether the window is minimised."""
        return self.iconized

    def CreateStdDialogButtonSizer(self, _flags: Any) -> _Sizer:
        """Return a sizer standing in for the native button row."""
        return _Sizer()

    def ShowModal(self) -> int:
        """Return whatever a test set as the modal result."""
        return getattr(self, "modal_result", 0)


class RecordingDC:
    """A device-context double: keep every primitive the painter draws, in order."""

    def __init__(self) -> None:
        self.calls: list = []
        self.pen = None
        self.brush = None
        self.text_colour = None

    def SetPen(self, pen: Any) -> None:
        """Keep the current pen."""
        self.pen = pen

    def SetBrush(self, brush: Any) -> None:
        """Keep the current brush."""
        self.brush = brush

    def SetTextForeground(self, colour: Any) -> None:
        """Keep the current text colour."""
        self.text_colour = colour

    def DrawRectangle(self, x: int, y: int, width: int, height: int) -> None:
        """Record a rectangle with its brush."""
        self.calls.append(("rect", x, y, width, height, self.brush))

    def DrawCircle(self, x: int, y: int, radius: int) -> None:
        """Record a circle with its brush."""
        self.calls.append(("circle", x, y, radius, self.brush))

    def DrawLine(self, x1: int, y1: int, x2: int, y2: int) -> None:
        """Record a line."""
        self.calls.append(("line", x1, y1, x2, y2))

    def DrawText(self, text: str, x: int, y: int) -> None:
        """Record a label with its colour."""
        self.calls.append(("text", text, x, y, self.text_colour))

    def of(self, kind: str) -> list:
        """Return the recorded calls of one kind."""
        return [call for call in self.calls if call[0] == kind]


@pytest.fixture
def dialog_module():
    """Load the real dialog module with wx doubles rich enough to construct it."""
    package = "jlc_detail_dialog_tests"
    stubs = wx_stubs(
        Panel=type("Panel", (_Window,), {}),
        Dialog=type("Dialog", (_Window,), {"__init__": _Window.__init__}),
        StaticText=_Window,
        Button=_Window,
        CheckBox=_Window,
        ComboBox=_Window,
        TextCtrl=_Window,
        BoxSizer=_Sizer,
        FlexGridSizer=_Sizer,
        Size=_Size,
        Colour=lambda *rgb: tuple(rgb),
        Pen=lambda colour, width=1: ("pen", colour, width),
        Brush=lambda colour, style=None: ("brush", colour, style),
        SystemSettings=SimpleNamespace(
            GetColour=lambda _key: SimpleNamespace(GetLuminance=lambda: 0.1)
        ),
    )
    stubs.update(package_stubs(package))
    with load_siblings(package, ("jlc_footprint_detail", "helpers"), stubs) as loaded:
        yield SimpleNamespace(
            module=loaded["jlc_footprint_detail"], wx=stubs["wx"], package=package
        )


def make_detail(state: str = "green", **fields: Any) -> PartDetail:
    """Return a part detail in one of the six states the dialog must handle."""
    verdict = resolve(
        KICAD_PADS,
        "Capacitor_SMD:C_0805_2012Metric",
        "ok",
        "CAP-SMD_L2.0-W1.3-FD",
        JLC_PADS,
        [SymbolPin("1", "1"), SymbolPin("2", "2")],
        marks=MARKS,
    )
    stored = StoredVerdict(
        "C7192",
        "hash",
        status="green",
        fit="fits",
        rotation=0,
        method="polarity",
        confidence="high",
    )
    detail = PartDetail(
        "C1",
        "C7192",
        kicad_footprint="Capacitor_SMD:C_0805_2012Metric",
        kicad_pads=list(KICAD_PADS),
        jlc_pads=list(JLC_PADS),
        courtyard=(-1.7, -0.95, 1.7, 0.95),
        package_name="CAP-SMD_L2.0-W1.3-FD",
        puuid="b3b82869fa924bae820e3a6cfb44d689",
        symbol_pins=[SymbolPin("1", "1"), SymbolPin("2", "2")],
        marks=MARKS,
        source="live",
        fetched_at=1_757_000_000,
        verdict=verdict,
        stored=stored,
    )
    if state == "yellow":
        stored.status, stored.polarity_light = "yellow", "yellow"
    elif state == "caveat":
        stored.body_excess_mm = 0.9
    elif state == "red":
        stored.status, stored.fit, stored.rotation = "red", "pitch", None
        stored.notes = "does not fit: pitch"
        detail.verdict = None
    elif state == "unknown":
        stored.status, stored.fit, stored.rotation = "unknown", "no_data", None
        stored.notes = "polarity unknown; check in JLC preview"
        detail.verdict = None
    elif state == "pending":
        stored.status, stored.rotation = "pending", None
        detail.verdict = None
        detail.jlc_pads = []
        detail.fetch = FetchState("queued", ahead=3, seconds=9.0, queued_parts=2)
    elif state == "override":
        stored.override_rotation, stored.override_note = 270, "the preview needed it"
    detail.decision = Decision(
        detail.reference,
        detail.lcsc,
        rotation=stored.emitted_rotation,
        source=stored.source,
        status=stored.status,
        polarity_light=stored.polarity_light,
        fit=stored.fit,
        note=stored.override_note or stored.notes or "",
        pending=stored.status == "pending",
        body_excess=stored.body_excess_mm,
        verdict=stored,
    )
    for key, value in fields.items():
        setattr(detail, key, value)
    return detail


def build(dialog_module, detail, **kwargs):
    """Construct the real dialog over the doubles."""
    parent = _Window()
    return dialog_module.module.JlcFootprintDetailDialog(parent, detail, **kwargs)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state", ["green", "yellow", "caveat", "red", "unknown", "pending", "override"]
)
def test_the_dialog_builds_for_every_state_of_a_row(dialog_module, state):
    """Spec 16.7: construction for green, yellow, red, unknown, pending and override rows."""
    detail = make_detail(state)
    dialog = build(dialog_module, detail)
    assert dialog.banner.GetLabel()
    assert dialog.cpl.GetLabel().startswith("The CPL emits")
    assert dialog.banner.colour is not None
    assert len(dialog.kicad_panel.grid.items) == 2 * 8  # label and value per fact
    assert len(dialog.jlc_panel.grid.items) == 2 * 11  # the origin line since M4
    assert "Pads KiCad/JLC" in dialog.numbers.GetLabel()
    assert dialog.override_button.IsEnabled() is True
    assert dialog.clear_button.IsEnabled() is (state == "override")
    assert set(dialog.checkboxes) == {KICAD, JLC_PLACED, JLC_RAW}
    assert [box.IsChecked() for box in dialog.checkboxes.values()] == [
        True,
        True,
        False,
    ]
    assert dialog.canvas.layers == [KICAD, JLC_PLACED]


def test_the_title_and_the_minimum_size_follow_the_spec(dialog_module):
    """Spec 16.4: "C1 · C7192", 960 x 560 DIP minimum, and a default that shows every fact."""
    dialog = build(dialog_module, make_detail())
    assert dialog.GetMinSize() == (960, 560)
    assert dialog.canvas.GetMinSize() == (420, 300)
    assert dialog_module.module.MIN_SIZE == (960, 560)
    assert dialog_module.module.CANVAS_MIN == (420, 300)
    assert dialog.GetSize() == dialog_module.module.DEFAULT_SIZE
    assert dialog_module.module.DEFAULT_SIZE >= (960, 560)


def test_a_part_without_an_lcsc_cannot_be_overridden_or_refetched(dialog_module):
    """Neither action means anything without a part number."""
    detail = make_detail("unknown", lcsc="", stored=None, decision=None)
    dialog = build(dialog_module, detail)
    assert dialog.override_button.IsEnabled() is False
    assert dialog.clear_button.IsEnabled() is False
    assert dialog.refetch_button.IsEnabled() is False


# ---------------------------------------------------------------------------
# The canvas
# ---------------------------------------------------------------------------


def test_the_canvas_asks_the_pure_layout_for_its_primitives(dialog_module):
    """The canvas measures itself and hands the size to the overlay (spec 16.4)."""
    dialog = build(dialog_module, make_detail())
    dialog.canvas.client_size = _Size(600, 400)
    drawing = dialog.canvas.build_overlay()
    assert dialog.canvas.last_overlay is drawing
    assert drawing.layers == (KICAD, JLC_PLACED)
    assert len(drawing.by_role("kicad_pad")) == 2
    assert len(drawing.by_role("jlc_pad")) == 2
    assert drawing.scale_px_per_mm > 0


def test_the_checkboxes_switch_the_canvas_layers(dialog_module):
    """The three checkboxes of spec 16.4, including the raw JLC pads."""
    dialog = build(dialog_module, make_detail())
    raw_box = dialog.checkboxes[JLC_RAW]
    raw_box.checked = True
    raw_box.fire(dialog_module.wx.EVT_CHECKBOX, SimpleNamespace(IsChecked=lambda: True))
    assert dialog.canvas.layers == [KICAD, JLC_PLACED, JLC_RAW]
    assert len(dialog.canvas.build_overlay().by_role("jlc_raw_pad")) == 2
    kicad_box = dialog.checkboxes[KICAD]
    kicad_box.fire(
        dialog_module.wx.EVT_CHECKBOX, SimpleNamespace(IsChecked=lambda: False)
    )
    assert dialog.canvas.layers == [JLC_PLACED, JLC_RAW]
    assert dialog.canvas.build_overlay().by_role("kicad_pad") == []


def test_the_painter_draws_every_primitive_onto_the_device_context(dialog_module):
    """Spec 16.7: the canvas is checked through a recording device-context double."""
    module = dialog_module.module
    dialog = build(dialog_module, make_detail())
    dialog.canvas.client_size = _Size(420, 300)
    drawing = dialog.canvas.build_overlay()
    dc = RecordingDC()
    drawn = module.draw_primitives(dc, drawing.primitives, dark=True)
    assert drawn == len(drawing.primitives)
    assert len(dc.calls) == len(drawing.primitives)
    # Every pad rectangle reaches the context at its own coordinates, and its brush
    # says which pad set it is: the JLC pads filled in the JLC colour, the KiCad pads
    # outlined in the KiCad colour, not just "some fill happened somewhere".
    rectangles = dc.of("rect")
    assert len(rectangles) == 4

    def rect_for(primitive):
        key = (
            int(round(primitive.x)),
            int(round(primitive.y)),
            max(1, int(round(primitive.width))),
            max(1, int(round(primitive.height))),
        )
        matches = [call for call in rectangles if call[1:5] == key]
        assert matches
        return matches[0]

    for primitive in drawing.by_role("kicad_pad"):
        brush = rect_for(primitive)[5]
        assert brush[1] == module.role_colour("kicad_pad", True)
        assert brush[2] is not None  # outline: a transparent brush
    for primitive in drawing.by_role("jlc_pad"):
        brush = rect_for(primitive)[5]
        assert brush[1] == module.role_colour("jlc_pad", True)
        assert brush[2] is None  # filled: a solid brush
    # Pin 1: a filled dot and a hollow ring.
    circles = dc.of("circle")
    assert len(circles) == 2
    assert circles[0][3] != circles[1][3]
    assert {call[1] for call in dc.of("text")} == {
        "1 mm",
        "KiCad raw 0°",
        "JLC +0° derived",
    }
    assert dc.of("line")


def test_the_painter_uses_one_colour_per_role_and_two_stops_per_theme(dialog_module):
    """Roles pick their own colour, and every role has a dark and a light stop."""
    module = dialog_module.module
    dark = module.role_colour("jlc_pad", True)
    light = module.role_colour("jlc_pad", False)
    assert dark != light
    assert sum(dark) > sum(light)  # the dark theme's stop is the lighter colour
    assert set(module.ROLE_COLOURS) >= {
        "grid",
        "kicad_pad",
        "jlc_pad",
        "jlc_raw_pad",
        "kicad_pin1",
        "jlc_pin1",
        "kicad_plus",
        "jlc_plus",
        "scale_bar",
        "scale_label",
        "angle_label",
    }
    assert all(len(stops) == 2 for stops in module.ROLE_COLOURS.values())
    unknown = module.role_colour("nothing like this", True)
    assert unknown is not None


# ---------------------------------------------------------------------------
# The actions
# ---------------------------------------------------------------------------


def test_setting_an_override_passes_the_angle_and_the_note_and_redraws(
    dialog_module, monkeypatch
):
    """Spec 16.4: "Set override…" asks for an angle and a note, then repaints the dialog."""
    module = dialog_module.module
    asked: list = []
    updated = make_detail("override")

    def fake_override(rotation, note):
        asked.append((rotation, note))
        return updated

    dialog = build(dialog_module, make_detail(), set_override=fake_override)

    class FakeOverrideDialog(_Window):
        """Stand in for the modal override prompt."""

        def __init__(self, _parent, rotation=None, note=""):
            super().__init__()
            self.given = (rotation, note)
            self.note = _Window(value="the preview needed it")
            self.modal_result = dialog_module.wx.ID_OK

        def override_angle(self):
            """Return the angle the user typed."""
            return 270

    monkeypatch.setattr(module, "OverrideDialog", FakeOverrideDialog)
    dialog.override_button.fire(dialog_module.wx.EVT_BUTTON)
    assert asked == [(270, "the preview needed it")]
    assert dialog.detail is updated
    assert dialog.clear_button.IsEnabled() is True
    assert "Override 270° set by you." in dialog.banner.GetLabel()


def test_a_cancelled_override_changes_nothing(dialog_module, monkeypatch):
    """Cancel writes no override and leaves the dialog as it was."""
    module = dialog_module.module
    calls: list = []
    dialog = build(
        dialog_module,
        make_detail(),
        set_override=lambda rotation, note: calls.append((rotation, note)),
    )

    class CancelledDialog(_Window):
        """A prompt the user cancels."""

        def __init__(self, _parent, rotation=None, note=""):
            super().__init__()
            self.note = _Window()
            self.modal_result = dialog_module.wx.ID_CANCEL

        def override_angle(self):
            """Return an angle that must never be used."""
            return 90

    monkeypatch.setattr(module, "OverrideDialog", CancelledDialog)
    dialog.override_button.fire(dialog_module.wx.EVT_BUTTON)
    assert calls == []


def test_clearing_an_override_and_refetching_call_back_and_redraw(dialog_module):
    """Clearing passes None; re-fetching asks the window and redraws the result."""
    cleared: list = []
    refetched: list = []
    plain = make_detail()
    dialog = build(
        dialog_module,
        make_detail("override"),
        set_override=lambda rotation, note: cleared.append((rotation, note)) or plain,
        refetch=lambda: refetched.append(True) or make_detail("pending"),
    )
    assert dialog.clear_button.IsEnabled() is True
    dialog.clear_button.fire(dialog_module.wx.EVT_BUTTON)
    assert cleared == [(None, "")]
    assert dialog.detail is plain
    assert dialog.clear_button.IsEnabled() is False
    dialog.refetch_button.fire(dialog_module.wx.EVT_BUTTON)
    assert refetched == [True]
    assert dialog.detail.fetch.state == "queued"
    assert "Queued for EasyEDA" in dialog.banner.GetLabel()
    # Spec 16.4: with nothing to transform, the banner carries the canvas's reason.
    assert dialog.canvas.build_overlay().note.startswith("no JLC pads cached")
    assert "No JLC pads cached for this part yet." in dialog.banner.GetLabel()


def test_without_callbacks_the_buttons_do_nothing(dialog_module, monkeypatch):
    """A dialog built with no callbacks (a preview) must not raise on its buttons."""
    dialog = build(dialog_module, make_detail("override"))
    monkeypatch.setattr(dialog_module.module, "OverrideDialog", _Window)
    dialog.clear_button.fire(dialog_module.wx.EVT_BUTTON)
    dialog.refetch_button.fire(dialog_module.wx.EVT_BUTTON)
    assert dialog.detail.stored.override_rotation == 270


def test_the_override_prompt_accepts_whole_degrees_only(dialog_module):
    """The prompt refuses anything but an integer, without closing (spec 16.4)."""
    module = dialog_module.module
    prompt = module.OverrideDialog(_Window(), rotation=180, note="keep")
    assert prompt.angle.GetValue() == "180"
    assert prompt.note.GetValue() == "keep"
    assert prompt.override_angle() == 180
    prompt.angle.SetValue("nonsense")
    assert prompt.override_angle() is None
    skipped: list = []
    prompt.on_ok(SimpleNamespace(Skip=lambda: skipped.append(True)))
    assert skipped == []
    assert "whole number" in prompt.message.GetLabel()
    prompt.angle.SetValue("270")
    prompt.on_ok(SimpleNamespace(Skip=lambda: skipped.append(True)))
    assert skipped == [True]


# ---------------------------------------------------------------------------
# The remembered size
# ---------------------------------------------------------------------------


def test_a_long_banner_is_wrapped_to_the_dialog_s_width(dialog_module):
    """Spec 16.4: the whole verdict text is readable, so the banner wraps rather than clips.

    A refusal's banner carries the verdict, the CPL sentence and the reason there is
    no transform; a one-line control would cut the tail off.
    """
    module = dialog_module.module
    dialog = build(dialog_module, make_detail(status="red", fit="numbering"))
    assert dialog.banner.wrapped_at is not None  # wrapped on creation
    margin = 2 * module.BANNER_MARGIN_PX + module.BANNER_SLACK_PX
    assert dialog.banner.wrapped_at == dialog.GetClientSize().GetWidth() - margin
    # The text handed to the control is the whole banner, not a truncation.
    assert dialog.banner.GetLabel() == dialog._banner_text
    assert dialog.banner.GetLabel().endswith(".")
    # A resize re-wraps to the new width.
    dialog.client_size = _Size(700, 500)
    dialog.fire(module.wx.EVT_SIZE)
    assert dialog.banner.wrapped_at == 700 - margin
    # The same width twice does no further work, and a new detail re-wraps.
    dialog.banner.wrapped_at = "untouched"
    dialog.fire(module.wx.EVT_SIZE)
    assert dialog.banner.wrapped_at == "untouched"
    dialog.update(make_detail())
    assert dialog.banner.wrapped_at == 700 - margin


def test_the_size_is_remembered_in_the_layout_settings(dialog_module):
    """Spec 16.4: the size is stored, and never restored below the minimum."""
    settings: dict = {}
    dialog = build(dialog_module, make_detail(), settings=settings)
    assert dialog.GetSize() == dialog_module.module.DEFAULT_SIZE
    dialog.size = _Size(1100, 700)
    dialog.on_close(SimpleNamespace(Skip=lambda: None))
    assert settings["jlcfootprint"]["detail_size"] == [1100, 700]
    reopened = build(dialog_module, make_detail(), settings=settings)
    assert reopened.GetSize() == (1100, 700)
    settings["jlcfootprint"]["detail_size"] = [100, 50]
    small = build(dialog_module, make_detail(), settings=settings)
    assert small.GetSize() == (960, 560)  # never below the minimum
    settings["jlcfootprint"]["detail_size"] = "nonsense"
    broken = build(dialog_module, make_detail(), settings=settings)
    assert broken.GetSize() == dialog_module.module.DEFAULT_SIZE


def test_a_zoomed_window_does_not_overwrite_the_remembered_size(dialog_module):
    """Only a normal-state size is stored, like the part selector's."""
    settings = {"jlcfootprint": {"detail_size": [1000, 600]}}
    dialog = build(dialog_module, make_detail(), settings=settings)
    dialog.maximized = True
    dialog.size = _Size(3000, 2000)
    dialog.on_close(SimpleNamespace(Skip=lambda: None))
    assert settings["jlcfootprint"]["detail_size"] == [1000, 600]


# ---------------------------------------------------------------------------
# Opening it: the double-click and the context menu
# ---------------------------------------------------------------------------


def _enabled(settings: dict) -> bool:
    """Read the shipped setting the way the facade does (the harness stubs it off)."""
    return bool(settings.get("jlcfootprint", {}).get("enabled", True))


def _window(main, check, selections=("C1",), lcscs=("C7192",)):
    """Return a window with a footprint-check double and a selection."""
    window = object.__new__(main.JLCPCBTools)
    window.settings = {"jlcfootprint": {"enabled": True}}
    window.jlc_footprint_check = check
    window.logger = MagicMock()
    window.save_settings = MagicMock()
    window.select_part = MagicMock()
    model = MagicMock()
    model.columns = main.PartListDataModel.columns
    items = [SimpleNamespace(name=name) for name in selections]
    model.get_reference.side_effect = lambda item: item.name
    model.get_lcsc.side_effect = lambda item: dict(zip(selections, lcscs)).get(
        item.name, ""
    )
    window.partlist_data_model = model
    control = MagicMock()
    control.GetSelections.return_value = items
    window.footprint_list = control
    return window, model, control


def _activation(column=None, index=-1):
    """Return an activation event as one port would report it."""
    return SimpleNamespace(
        GetDataViewColumn=lambda: column, GetColumn=lambda: index, Skip=lambda: None
    )


def test_a_double_click_on_the_jlc_cell_opens_the_dialog(monkeypatch):
    """Spec 16.3: the activation event's column decides; any other column assigns a part."""
    main = layout.mainwindow
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    opened: list = []
    monkeypatch.setattr(
        main.JLCPCBTools,
        "show_jlc_footprint_detail",
        lambda self, reference=None: opened.append(reference),
    )
    window, model, control = _window(main, MagicMock())
    jlc = SimpleNamespace(
        GetModelColumn=lambda: main.PartListDataModel.columns["JLC_COL"]
    )
    other = SimpleNamespace(
        GetModelColumn=lambda: main.PartListDataModel.columns["LCSC_COL"]
    )
    # GTK: the event names the column itself.
    main.JLCPCBTools.on_footprint_activated(window, _activation(column=jlc))
    assert opened == [None]
    main.JLCPCBTools.on_footprint_activated(window, _activation(column=other))
    assert opened == [None] and window.select_part.call_count == 1
    # macOS: the event names the index only, so it is mapped through the positions.
    control.GetColumns.return_value = [other, jlc]
    control.GetColumnPosition.side_effect = lambda candidate: [other, jlc].index(
        candidate
    )
    main.JLCPCBTools.on_footprint_activated(window, _activation(index=1))
    assert opened == [None, None]
    main.JLCPCBTools.on_footprint_activated(window, _activation(index=0))
    assert window.select_part.call_count == 2
    # A port that reports neither keeps the old behaviour.
    main.JLCPCBTools.on_footprint_activated(window, _activation(index=-1))
    assert window.select_part.call_count == 3
    assert opened == [None, None]


def test_the_dialog_opens_on_the_first_selected_part_with_an_lcsc(monkeypatch):
    """Spec 16.3: "Details…" takes the first selected part that has a part number."""
    main = layout.mainwindow
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    shown: list = []

    class FakeDialog:
        """Record what the window handed the dialog."""

        def __init__(
            self, parent, detail, set_override=None, refetch=None, settings=None
        ):
            shown.append((parent, detail, set_override, refetch, settings))
            self.destroyed = False

        def ShowModal(self):
            """Return at once."""
            return 0

        def remember_size(self):
            """Stand in for the real dialog's size bookkeeping."""

        def Destroy(self):
            """Mark the dialog destroyed."""
            self.destroyed = True

    monkeypatch.setattr(main, "JlcFootprintDetailDialog", FakeDialog)
    check = MagicMock()
    fake_part_detail = MagicMock(return_value="the detail")
    monkeypatch.setattr(main, "part_detail", fake_part_detail)
    window, model, control = _window(main, check, ("R9", "C1"), ("", "C7192"))
    main.JLCPCBTools.show_jlc_footprint_detail(window)
    (parent, detail, set_override, refetch, settings) = shown[0]
    assert parent is window and detail == "the detail"
    fake_part_detail.assert_called_once_with(check, "C1", reread=True)
    assert settings is window.settings
    window.save_settings.assert_called_once_with()
    # Nothing selected with an LCSC: no dialog.
    window, model, control = _window(main, check, ("R9",), ("",))
    main.JLCPCBTools.show_jlc_footprint_detail(window)
    assert len(shown) == 1
    # The check off: no dialog either.
    window, model, control = _window(main, check)
    window.settings = {"jlcfootprint": {"enabled": False}}
    main.JLCPCBTools.show_jlc_footprint_detail(window)
    assert len(shown) == 1


def test_the_window_remembers_the_size_on_every_way_out(monkeypatch):
    """Spec 16.4: the size is stored however the dialog was dismissed.

    The Close button and Esc end a modal loop without an ``EVT_CLOSE``, so the
    window asks for the size itself once ``ShowModal`` returns.
    """
    main = layout.mainwindow
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    order: list = []

    class FakeDialog:
        """A dialog that is dismissed without ever sending EVT_CLOSE."""

        def __init__(self, parent, detail, **_kwargs):
            self.parent = parent

        def ShowModal(self):
            """Return as the Close button would, with no EVT_CLOSE."""
            order.append("shown")
            return 0

        def remember_size(self):
            """Record that the size was asked for."""
            order.append("remembered")

        def Destroy(self):
            """Record the destroy, which must come after the size."""
            order.append("destroyed")

    monkeypatch.setattr(main, "JlcFootprintDetailDialog", FakeDialog)
    monkeypatch.setattr(main, "part_detail", MagicMock(return_value="the detail"))
    check = MagicMock()
    window, _model, _control = _window(main, check)
    main.JLCPCBTools.show_jlc_footprint_detail(window)
    assert order == ["shown", "remembered", "destroyed"]
    window.save_settings.assert_called_once_with()


def test_the_override_and_refetch_callbacks_repaint_every_shared_row(monkeypatch):
    """A change repaints the Rotation text and the glyph of every row on that verdict."""
    main = layout.mainwindow
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    check = MagicMock()
    check.set_override.return_value = "stored"
    check.references_sharing_verdict.return_value = ["C1", "C2"]
    check.display_text.return_value = "270° set"
    monkeypatch.setattr(main, "glyph_state", lambda check, reference: "override")
    monkeypatch.setattr(main, "part_detail", MagicMock(return_value="fresh"))
    window, model, _control = _window(main, check)
    assert main.JLCPCBTools._set_jlc_override(window, "C1", 270, "note") == "fresh"
    check.set_override.assert_called_once_with("C1", 270, "note")
    assert model.set_rotation.call_args_list == [
        (("C1", "270° set"),),
        (("C2", "270° set"),),
    ]
    assert model.set_jlc_state.call_args_list == [
        (("C1", "override"),),
        (("C2", "override"),),
    ]
    check.set_override.return_value = None
    assert main.JLCPCBTools._set_jlc_override(window, "C1", 90, "") is None
    # The dialog's "Re-fetch" is the same action as the menu's (spec 16.4 -> 16.5),
    # so it goes through the facade, which is what logs the one line the log window
    # shows; calling check.refetch here directly would log nothing at all.
    refetched: list = []
    monkeypatch.setattr(
        main,
        "refetch_jlc_footprint_references",
        lambda *args: refetched.append(args) or 1,
    )
    assert main.JLCPCBTools._refetch_jlc_references(window, ["C1"]) == "fresh"
    assert refetched == [(window, ["C1"], check)]
    check.refetch.assert_not_called()
    assert main.JLCPCBTools._refetch_jlc_references(window, []) is None


def test_the_context_menu_carries_the_jlc_submenu(monkeypatch):
    """Spec 16.3: a "JLC footprint" submenu, its Details entry enabled only when it can open."""
    main = layout.mainwindow
    monkeypatch.setattr(main, "is_footprint_check_enabled", _enabled)
    appended: list = []

    class FakeMenu:
        """Record what was appended to a menu, and the order things arrived in."""

        def __init__(self):
            self.items = []
            self.submenus = []
            self.bindings = []
            self.sequence = []

        def Append(self, item):
            """Keep the item and record it in the append order."""
            self.items.append(item)
            self.sequence.append(item.label)

        def Bind(self, _event, handler, item):
            """Keep the binding."""
            self.bindings.append((handler, item))

        def AppendSeparator(self):
            """Record a separator in the append order."""
            self.sequence.append("separator")

        def AppendSubMenu(self, submenu, label):
            """Keep the submenu and its label."""
            self.submenus.append((label, submenu))
            appended.append(label)

    class FakeItem:
        """A menu item that remembers whether it was enabled."""

        def __init__(self, _menu, _id, label):
            self.label = label
            self.enabled = None

        def Enable(self, enabled):
            """Keep the enablement."""
            self.enabled = enabled

    monkeypatch.setattr(main.wx, "Menu", FakeMenu, raising=False)
    monkeypatch.setattr(main.wx, "MenuItem", FakeItem, raising=False)
    window, _model, _control = _window(main, MagicMock())
    parent_menu = FakeMenu()
    submenu = main.JLCPCBTools._append_jlc_footprint_menu(window, parent_menu)
    assert appended == ["JLC footprint"]
    # Spec 16.3: a separator divides the per-part entries from the board-wide ones,
    # so its position is pinned along with the entries, not just its count.
    assert submenu.sequence == [
        "Details...",
        "Re-fetch data",
        "separator",
        "Re-check board",
        "Refresh board data",
        "Clear cache",
    ]
    assert [item.enabled for item in submenu.items] == [True] * 5
    assert [handler for handler, _item in submenu.bindings] == [
        window.on_jlc_footprint_details,
        window.on_jlc_footprint_refetch,
        window.on_jlc_footprint_recheck,
        window.on_jlc_footprint_refresh,
        window.on_jlc_footprint_clear_cache,
    ]
    # With the check off every entry is disabled.
    window.settings = {"jlcfootprint": {"enabled": False}}
    off = main.JLCPCBTools._append_jlc_footprint_menu(window, FakeMenu())
    assert [item.enabled for item in off.items] == [False] * 5
    # With no selected part number only the two per-part entries are disabled.
    window, _model, _control = _window(main, MagicMock(), ("R9",), ("",))
    bare = main.JLCPCBTools._append_jlc_footprint_menu(window, FakeMenu())
    assert [item.enabled for item in bare.items] == [False, False, True, True, True]


# ---------------------------------------------------------------------------
# M4: JLC's package origin on the canvas and in the facts (spec 17.5)
# ---------------------------------------------------------------------------


def _facts(panel) -> dict:
    """Return a facts panel's label/value pairs as a dict."""
    labels = [item.GetLabel() for item in panel.grid.items]
    return {name.rstrip(":"): value for name, value in zip(labels[::2], labels[1::2])}


@pytest.mark.parametrize("exact_origin", [True, False])
def test_the_jlc_panel_s_origin_line_follows_the_setting(dialog_module, exact_origin):
    """The dialog reads its own section of the window's settings, nothing else."""
    dialog = build(
        dialog_module,
        make_detail(),
        settings={"jlcfootprint": {"exact_origin": exact_origin}},
    )

    origin = _facts(dialog.jlc_panel)["Origin"]

    assert origin.startswith("on the pad-box centre; ")
    assert origin.endswith("used in the CPL" if exact_origin else "the setting is off")


def test_a_dialog_without_settings_says_the_origin_is_not_used(dialog_module):
    """No settings at all reads the setting off rather than crashing on the key."""
    dialog = build(dialog_module, make_detail())

    assert _facts(dialog.jlc_panel)["Origin"].endswith("not used: the setting is off")


def test_the_painter_draws_the_origin_cross_in_the_annotation_colour(dialog_module):
    """The cross can land on a JLC pad, so it takes the text colour, not the pad colour."""
    module = dialog_module.module
    dialog = build(dialog_module, make_detail())
    dialog.canvas.client_size = _Size(420, 300)
    drawing = dialog.canvas.build_overlay()

    assert len(drawing.by_role("jlc_origin")) == 2
    for dark in (True, False):
        assert module.role_colour("jlc_origin", dark) == module.role_colour(
            "jlc_pin1", dark
        )
        assert module.role_colour("jlc_origin", dark) != module.role_colour(
            "jlc_pad", dark
        )
    dc = RecordingDC()
    assert module.draw_primitives(dc, drawing.by_role("jlc_origin"), dark=True) == 2
    assert len(dc.of("line")) == 2
