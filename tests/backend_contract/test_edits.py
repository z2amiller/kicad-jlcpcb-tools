"""How every backend writes an edit batch: all of it, or none of it."""

from collections.abc import Callable, Sequence
from functools import partial
import importlib
from types import SimpleNamespace
from typing import Any

import pytest

from .support import Board, Part
from .swig_board import kicad_editor

MakeBoard = Callable[[Sequence[Part]], Board]


def _fields(component: Any) -> list[tuple[str, str, bool]]:
    """Return a component's Default fields as (name, text, visible), in order."""
    return [
        (field.name, field.text, field.visible)
        for field in component.default.fields.values()
    ]


def _copies(component: Any, name: str) -> list[tuple[str, bool]]:
    """Return (text, visible) of every Default field with this name, in order."""
    state = component.default
    return [
        (record.text, record.visible)
        for record in (*state.replaced_fields, state.fields[name])
        if record.name == name
    ]


def _edit(
    api: SimpleNamespace, component: Any, **operations: Any
) -> Any:  # a ComponentEdit
    """Edit a footprint's own fields and flags, from the state just read."""
    return api.default_edit(component, **operations)


def test_field_edits_read_back(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Set, add, hide and show fields; a new field goes last, the others stay.

    A KiCad that shows no field but Reference and Value is not asked to
    show one, and one that sorts its fields by name holds the new one in
    its place.
    """
    api = backend_modules.api
    made = board(
        [Part("R1", fields=(("LCSC", "C1"), ("MPN", "RC0603"), ("Note", "x")))]
    )
    backend = made.backend
    (r1,) = backend.snapshot().components
    shown = made.shows_fields

    after = backend.apply(
        "edit fields",
        [
            _edit(
                api,
                r1,
                set_fields=(("LCSC", "C25804"), ("JLC", "C2")),
                field_visibility=(
                    ("LCSC", False),
                    ("JLC", False),
                    *((("MPN", True),) if shown else ()),
                ),
            )
        ],
    )

    own = [
        ("LCSC", "C25804", False),
        ("MPN", "RC0603", shown),
        ("Note", "x", shown),
        ("JLC", "C2", False),
    ]
    assert _fields(after.get(r1.key)) == [
        ("Reference", "R1", True),
        ("Value", "10k", True),
        *((name, "", False) for name in made.builtin_fields[2:]),
        *(sorted(own) if made.sorts_fields else own),
    ]
    assert after == backend.snapshot()


def test_a_field_is_shown_only_where_kicad_can_show_one(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Showing a hidden field reads back shown; where KiCad shows none, it is refused."""
    api = backend_modules.api
    made = board([Part("R1", fields=(("LCSC", "C1"),), hidden=("LCSC",))])
    backend = made.backend
    before = backend.snapshot()
    (r1,) = before.components
    show = [_edit(api, r1, field_visibility=(("LCSC", True),))]

    if made.shows_fields:
        after = backend.apply("show", show)
        assert after.get(r1.key).default.fields["LCSC"].visible
    else:
        with pytest.raises(api.EditRejected):
            backend.apply("show", show)
        assert backend.snapshot() == before


def test_flag_edits_read_back_independently(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Each flag changes alone; the others and the fields stay as they were."""
    api = backend_modules.api
    backend = board([Part("R1"), Part("R2"), Part("R3", exclude_from_bom=True)]).backend
    r1, r2, r3 = backend.snapshot().components

    after = backend.apply(
        "edit flags",
        [
            _edit(api, r1, flags=(("exclude_from_bom", True),)),
            _edit(api, r2, flags=(("exclude_from_pos", True),)),
            _edit(api, r3, flags=(("exclude_from_bom", False),)),
        ],
    )

    assert [
        (state.dnp, state.exclude_from_bom, state.exclude_from_pos)
        for state in (component.default for component in after.components)
    ] == [(False, True, False), (False, False, True), (False, False, False)]
    assert [_fields(component) for component in after.components] == [
        _fields(component) for component in (r1, r2, r3)
    ]


def test_edits_needed_tells_a_change_from_a_no_op_without_writing(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Setting what a footprint already holds is no change; nothing is written."""
    api = backend_modules.api
    backend = board([Part("R1", fields=(("LCSC", "C1"),))]).backend
    before = backend.snapshot()
    (r1,) = before.components

    assert not backend.edits_needed([])
    assert not backend.edits_needed(
        [
            _edit(
                api,
                r1,
                set_fields=(("LCSC", "C1"),),
                flags=(("exclude_from_pos", False),),
            )
        ]
    )
    assert backend.edits_needed([_edit(api, r1, set_fields=(("LCSC", "C2"),))])
    assert backend.edits_needed([_edit(api, r1, set_fields=(("JLC", ""),))])
    assert backend.snapshot() == before


def test_a_stale_revision_writes_nothing(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """An edit made from an old state stops the whole batch before any write."""
    api = backend_modules.api
    backend = board([Part("R1"), Part("R2")]).backend
    r1, r2 = backend.snapshot().components
    backend.apply("first", [_edit(api, r2, set_fields=(("LCSC", "C2"),))])
    before = backend.snapshot()
    stale = [
        _edit(api, r1, set_fields=(("LCSC", "C1"),)),
        _edit(api, r2, set_fields=(("LCSC", "C3"),)),
    ]

    with pytest.raises(api.StaleTarget):
        backend.edits_needed(stale)
    with pytest.raises(api.StaleTarget):
        backend.apply("second", stale)
    assert backend.snapshot() == before


@pytest.mark.parametrize(
    "problem",
    ["twice", "flag", "dnp", "hide a missing field"],
)
def test_an_invalid_batch_writes_nothing(
    board: MakeBoard, backend_modules: SimpleNamespace, problem: str
) -> None:
    """A batch no backend may write is rejected whole, before anything changes."""
    api = backend_modules.api
    backend = board([Part("R1"), Part("R2")]).backend
    before = backend.snapshot()
    r1, r2 = before.components
    valid = _edit(api, r1, set_fields=(("LCSC", "C1"),))
    invalid = {
        "twice": _edit(api, r1, flags=(("exclude_from_bom", True),)),
        "flag": _edit(api, r2, flags=(("hidden", True),)),
        "dnp": _edit(api, r2, flags=(("dnp", True),)),
        "hide a missing field": _edit(api, r2, field_visibility=(("LCSC", False),)),
    }[problem]

    with pytest.raises(api.EditRejected):
        backend.edits_needed([valid, invalid])
    with pytest.raises(api.EditRejected):
        backend.apply("invalid", [valid, invalid])
    assert backend.snapshot() == before


def test_a_failed_write_is_undone(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """When a later write fails, the earlier ones are put back and editing goes on."""
    api = backend_modules.api
    made = board([Part("R1", fields=(("LCSC", "C1"),)), Part("R2")])
    backend = made.backend
    before = backend.snapshot()
    r1, r2 = before.components
    made.fail_writes("R2")
    batch = [
        _edit(
            api, r1, set_fields=(("LCSC", "C9"),), flags=(("exclude_from_pos", True),)
        ),
        _edit(
            api, r2, set_fields=(("LCSC", "C9"),), field_visibility=(("LCSC", False),)
        ),
    ]

    with pytest.raises(api.EditRejected):
        backend.apply("assign", batch)
    assert backend.snapshot() == before
    after = backend.apply("assign", batch[:1])
    assert after.get(r1.key).default.fields["LCSC"].text == "C9"


def test_a_write_that_cannot_be_undone_stops_further_edits(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Once a failed write could not be put back, the backend refuses every edit."""
    api = backend_modules.api
    made = board([Part("R1"), Part("R2")])
    backend = made.backend
    r1, r2 = backend.snapshot().components
    made.fail_writes("R2", False)

    with pytest.raises(api.EditRecoveryFailed):
        backend.apply(
            "assign",
            [
                _edit(api, r1, set_fields=(("LCSC", "C1"),)),
                _edit(api, r2, set_fields=(("LCSC", "C2"),)),
            ],
        )
    current = backend.snapshot().get(r1.key)
    assert current.default.fields.get("LCSC") is None
    with pytest.raises(api.EditRecoveryFailed):
        backend.edits_needed([_edit(api, current, flags=(("exclude_from_bom", True),))])
    with pytest.raises(api.EditRecoveryFailed):
        backend.apply(
            "again", [_edit(api, current, flags=(("exclude_from_bom", True),))]
        )


def test_a_footprint_gone_from_the_board_is_stale(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """An edit to a footprint the board no longer has writes nothing."""
    api = backend_modules.api
    backend = board([Part("R1")]).backend
    before = backend.snapshot()
    gone = api.ComponentEdit(
        "not-on-this-board", "revision", flags=(("exclude_from_bom", True),)
    )

    with pytest.raises(api.StaleTarget):
        backend.apply("edit", [gone])
    assert backend.snapshot() == before


def test_the_second_of_two_footprints_sharing_a_uuid_is_edited_by_its_key(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Footprints sharing a UUID are told apart by key; each edit reaches its own."""
    api = backend_modules.api
    backend = board(
        [
            Part("R1", fields=(("LCSC", "C1"),)),
            Part("R2", fields=(("LCSC", "C2"),), uuid_of="R1"),
        ]
    ).backend
    # KiCad opens the file in its own order, so either may come second.
    first, second = backend.snapshot().components
    assert second.uuid == first.uuid and second.key == f"{first.uuid}#2"
    texts = {c.reference: c.default.fields["LCSC"].text for c in (first, second)}

    after = backend.apply("assign", [_edit(api, second, set_fields=(("LCSC", "C9"),))])

    assert {c.reference: c.default.fields["LCSC"].text for c in after.components} == {
        first.reference: texts[first.reference],
        second.reference: "C9",
    }
    first, second = after.components
    after = backend.apply(
        "assign",
        [
            _edit(api, first, set_fields=(("LCSC", "C7"),)),
            _edit(api, second, set_fields=(("LCSC", "C8"),)),
        ],
    )
    assert [c.default.fields["LCSC"].text for c in after.components] == ["C7", "C8"]
    assert after.get(second.key).reference == second.reference


def test_a_repeated_field_is_written_in_every_copy(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Every field of a repeated name takes the new text and visibility; none goes.

    A KiCad that keeps one text per name holds only the later one.
    """
    api = backend_modules.api
    made = board([Part("R1", fields=(("LCSC", "C1"), ("MPN", "X"), ("LCSC", "C2")))])
    backend = made.backend
    (r1,) = backend.snapshot().components
    kept = made.keeps_repeated_fields
    assert r1.default.duplicate_names == (("LCSC",) if kept else ())

    # The later field already holds C2; the earlier one, where kept, does not.
    assert backend.edits_needed([_edit(api, r1, set_fields=(("LCSC", "C2"),))]) is kept
    after = backend.apply(
        "assign",
        [
            _edit(
                api,
                r1,
                set_fields=(("LCSC", "C25804"),),
                field_visibility=(("LCSC", False),),
            )
        ],
    ).get(r1.key)

    assert _copies(after, "LCSC") == [("C25804", False)] * (2 if kept else 1)
    assert after.default.duplicate_names == (("LCSC",) if kept else ())
    assert after.default.fields["MPN"].text == "X"
    assert not backend.edits_needed(
        [_edit(api, after, set_fields=(("LCSC", "C25804"),))]
    )


def test_a_failed_write_puts_back_every_copy(
    board: MakeBoard, backend_modules: SimpleNamespace
) -> None:
    """Undoing a batch restores each field of a repeated name as it was."""
    api = backend_modules.api
    made = board(
        [Part("R1", fields=(("LCSC", "C1"), ("MPN", "X"), ("LCSC", "C2"))), Part("R2")]
    )
    backend = made.backend
    before = backend.snapshot()
    r1, r2 = before.components
    made.fail_writes("R2")

    with pytest.raises(api.EditRejected):
        backend.apply(
            "assign",
            [
                _edit(
                    api,
                    r1,
                    set_fields=(("LCSC", "C9"),),
                    field_visibility=(("LCSC", False),),
                ),
                _edit(api, r2, set_fields=(("LCSC", "C9"),)),
            ],
        )
    assert backend.snapshot() == before
    shown = made.shows_fields
    assert _copies(backend.snapshot().get(r1.key), "LCSC") == [
        *([("C1", shown)] if made.keeps_repeated_fields else []),
        ("C2", shown),
    ]


class _PcbEditor:
    """KiCad's PCB editor frame: its plugin menu, and RunActionPlugin around Run().

    Sending the plugin's menu command opens KiCad's action wrapper, runs the
    plugin, and closes the wrapper. ``seen`` records the Default fields of
    R1 just before the wrapper opens and just after it closes.
    """

    def __init__(self, editor: Any, backend: Any, dispatcher: Any) -> None:
        self.editor, self.backend, self.dispatcher = editor, backend, dispatcher
        self.seen: list[str] = []
        item = SimpleNamespace(
            GetSubMenu=lambda: None,
            GetItemLabelText=lambda: "JLCPCB Tools",
            GetId=lambda: 71,
        )
        menu = SimpleNamespace(GetMenuItems=lambda: [item])
        self.GetMenuBar = lambda: SimpleNamespace(GetMenus=lambda: [(menu, "Tools")])

    def IsBeingDeleted(self) -> bool:
        return False

    def IsEnabled(self) -> bool:
        return True

    def GetEventHandler(self) -> "_PcbEditor":
        return self

    def _lcsc(self) -> str:
        field = self.backend.snapshot().by_reference("R1")[0].default.fields.get("LCSC")
        return field.text if field is not None else ""

    def ProcessEvent(self, _event: Any) -> bool:
        self.seen.append(self._lcsc())
        self.editor.running = True
        try:
            assert self.dispatcher.run_pending()
        finally:
            self.editor.running = False
        self.seen.append(self._lcsc())
        return True


@pytest.mark.native_kicad
@pytest.mark.skipif(
    'os.environ.get("KICAD_JLCPCB_NATIVE_TESTS") != "1"',
    reason="Native KiCad tests require an explicit opt-in",
)
def test_swig_writes_run_inside_kicads_action_wrapper(
    request: pytest.FixtureRequest, backend_modules: SimpleNamespace
) -> None:
    """The write happens while KiCad's wrapper is open, never before it.

    That wrapper is what gives the user one undo step and the modified flag;
    SWIG exposes no undo stack to check directly.
    """
    api, swig = backend_modules.api, backend_modules.swig
    commit = importlib.import_module(f"{swig.__name__}.commit")
    editor = kicad_editor(backend_modules, [Part("R1")], request)
    dispatcher = commit.ActionDispatcher(editor, "JLCPCB Tools")
    frame = _PcbEditor(editor, None, dispatcher)
    backend = swig.SwigBackend(
        editor, commit=partial(dispatcher.run, context=(frame, dispatcher.board_id()))
    )
    frame.backend = backend
    (r1,) = backend.snapshot().components

    after = backend.apply("assign", [_edit(api, r1, set_fields=(("LCSC", "C25804"),))])

    assert frame.seen == ["", "C25804"]
    assert after.get(r1.uuid).default.fields["LCSC"].text == "C25804"
    assert not editor.IsActionRunning()
