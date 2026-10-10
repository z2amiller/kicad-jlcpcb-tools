"""The override dialog says which references an override reaches (spec 18.2).

The window's presenter composes the sentence from the check's own
``references_sharing_verdict`` and the part number, adding the variants on a board
with KiCad variants, and hands it to the detail dialog as a callable, which asks
it each time "Set override…" opens and shows it in the override prompt.  Both
boards and both ways into the dialog (the part list and the matrix) go through
``show_jlc_footprint_detail``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .jlc_footprint_wx_support import check_window, facade_symbols, load_window
from .test_jlc_footprint_check_dialog import _Window, build, dialog_module, make_detail

__all__ = ["dialog_module"]
_PACKAGE = "jlc_footprint_check_override_scope_tests"


@pytest.fixture
def main():
    """Load the main window with the facade faked."""
    return load_window(_PACKAGE, facade_symbols())


def _window(main, sharing, *, variants):
    """Return a window whose check gives R1 part C123 and ``sharing`` on its verdict."""
    check = MagicMock()
    check.parts = {"R1": SimpleNamespace(lcsc="C123"), "R9": SimpleNamespace(lcsc="")}
    check.references_sharing_verdict.side_effect = lambda reference: list(sharing)
    window = check_window(main, check)
    window.save_settings = MagicMock()
    # The real window always carries the attribute: None on an ordinary board.
    window._variant_controller = MagicMock() if variants else None
    return window


@pytest.mark.parametrize(
    "sharing,variants,sentence",
    [
        (["R1"], False, "This override applies to R1."),
        (["R5", "R1"], False, "This override applies to R1 and R5."),
        (
            ["R1"],
            True,
            "This override applies to R1, in every variant that orders C123 on "
            "this footprint.",
        ),
        (
            ["R5", "R1"],
            True,
            "This override applies to R1 and R5, in every variant that orders C123 "
            "on this footprint.",
        ),
    ],
    ids=["ordinary_one", "ordinary_several", "variant_one", "variant_several"],
)
def test_the_detail_dialog_gets_the_sentence_for_its_board(
    main, monkeypatch, sharing, variants, sentence
):
    """Ordinary list or variant matrix, the dialog is handed the board's sentence."""
    window = _window(main, sharing, variants=variants)
    presenter = window.jlc_footprint_presenter
    opened: list = []

    class FakeDialog:
        """Record the scope callable the window hands the dialog."""

        def __init__(self, _parent, _detail, **kwargs):
            opened.append(kwargs["override_scope"])

        def ShowModal(self):
            """Return at once."""
            return 0

        def remember_size(self):
            """Stand in for the size bookkeeping."""

        def Destroy(self):
            """Accept the destroy."""

    monkeypatch.setattr(
        main.jlc_footprint_window, "JlcFootprintDetailDialog", FakeDialog
    )
    monkeypatch.setattr(main.jlc_footprint_window, "part_detail", MagicMock())
    presenter.show_jlc_footprint_detail("R1")
    (scope,) = opened
    assert scope() == sentence
    window.jlc_footprint_check.references_sharing_verdict.assert_called_with("R1")


def test_a_part_without_a_number_or_unread_has_no_sentence(main):
    """No part number, or a reference the check has not read: the prompt says nothing."""
    window = _window(main, ["R9"], variants=True)
    presenter = window.jlc_footprint_presenter
    assert presenter._override_scope("R9") == ""
    assert presenter._override_scope("R404") == ""
    window.settings = {"jlcfootprint": {"enabled": False}}
    assert presenter._override_scope("R1") == ""


def test_the_prompt_shows_the_sentence_wrapped_under_the_fields(dialog_module):
    """The override prompt carries the sentence as its own wrapped line."""
    module = dialog_module.module
    sentence = "This override applies to R1 and R5."
    prompt = module.OverrideDialog(_Window(), rotation=180, note="", scope=sentence)
    assert prompt.scope.GetLabel() == sentence
    assert prompt.scope.wrapped_at == module.SCOPE_WRAP_DIP
    assert module.OverrideDialog(_Window()).scope is None


def test_set_override_asks_for_the_sentence_each_time_it_opens(
    dialog_module, monkeypatch
):
    """The dialog asks the window when the prompt opens, so it names today's references."""
    module = dialog_module.module
    sentences = iter(
        ["This override applies to R1.", "This override applies to R1 and R5."]
    )
    scopes: list = []

    class Prompt(_Window):
        """Record the scope the prompt was given, then cancel."""

        def __init__(self, _parent, rotation=None, note="", scope=""):
            super().__init__()
            scopes.append(scope)
            self.note = _Window()
            self.modal_result = dialog_module.wx.ID_CANCEL

        def override_angle(self):
            """Return an angle that is never used."""
            return 90

    monkeypatch.setattr(module, "OverrideDialog", Prompt)
    dialog = build(
        dialog_module,
        make_detail(),
        set_override=lambda rotation, note: None,
        override_scope=lambda: next(sentences),
    )
    dialog.override_button.fire(dialog_module.wx.EVT_BUTTON)
    dialog.override_button.fire(dialog_module.wx.EVT_BUTTON)
    assert scopes == [
        "This override applies to R1.",
        "This override applies to R1 and R5.",
    ]
    plain = build(
        dialog_module, make_detail(), set_override=lambda rotation, note: None
    )
    plain.override_button.fire(dialog_module.wx.EVT_BUTTON)
    assert scopes[-1] == ""
