"""Hints on the part selector's read-only combos skip wxMSW."""

from unittest.mock import MagicMock

import pytest

from . import test_window_layout as layout_ui


@pytest.mark.parametrize(
    ("port", "shown"),
    [("__WXMSW__", False), ("__WXGTK__", True), ("__WXMAC__", True)],
)
def test_a_read_only_combo_gets_its_hint_except_on_wxmsw(
    monkeypatch: pytest.MonkeyPatch, port: str, shown: bool
) -> None:
    """A read-only combo on wxMSW has no edit control to hold a hint."""
    monkeypatch.setattr(layout_ui.partselector.wx, "Platform", port)
    combo = MagicMock()
    layout_ui.partselector.set_read_only_hint(combo, "e.g. Resistors")
    if shown:
        combo.SetHint.assert_called_once_with("e.g. Resistors")
    else:
        combo.SetHint.assert_not_called()
