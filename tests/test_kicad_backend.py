"""The board records and the in-memory backend."""

from collections.abc import Iterator
from types import ModuleType, SimpleNamespace

import pytest

from .wx_harness import load_siblings


@pytest.fixture
def backend() -> Iterator[SimpleNamespace]:
    """Load the backend package under a scoped name, without KiCad or wx."""
    names = ("kicad_backend", "records", "fake", "edits")
    with load_siblings(
        "_kicad_backend_tests",
        ("kicad_backend", *(f"kicad_backend.{name}" for name in names[1:])),
        {},
    ) as modules:
        yield SimpleNamespace(
            api=modules["kicad_backend"],
            **{name: modules[f"kicad_backend.{name}"] for name in names[1:]},
        )


def _snapshot(backend: SimpleNamespace, *components: object) -> object:
    """Read a fake board holding ``components``."""
    return backend.fake.FakeBackend(components).snapshot()


def test_components_keep_board_order_and_references_may_repeat(
    backend: SimpleNamespace,
) -> None:
    """Lookups by reference return every match in the order the board lists them."""
    fake = backend.fake
    second = fake.component("R1", uuid="b-second", value="22k")
    first = fake.component("R1", uuid="a-first", value="10k")
    other = fake.component("C1")
    snapshot = _snapshot(backend, second, first, other)

    assert snapshot.components == (second, first, other)
    assert snapshot.by_reference("R1") == (second, first)
    assert snapshot.by_reference("R9") == ()
    assert snapshot.get("a-first") is first
    with pytest.raises(KeyError):
        snapshot.get("missing")


@pytest.mark.parametrize(
    "problem, message",
    [
        ("empty uuid", "a UUID for every footprint"),
        ("default not first", "Default first"),
        ("duplicate variant", "Default first"),
        ("missing state", "one state per variant"),
    ],
)
def test_an_incomplete_or_ambiguous_snapshot_is_rejected(
    backend: SimpleNamespace, problem: str, message: str
) -> None:
    """A snapshot lists Default first and gives every footprint a UUID and every variant."""
    records, fake = backend.records, backend.fake
    default = records.VariantDefinition("", "Default")
    variants = (default,)
    components = [fake.component("R1", uuid="u1"), fake.component("R2", uuid="u2")]
    if problem == "empty uuid":
        components[1] = fake.component("R2", uuid="")
    elif problem == "default not first":
        variants = (records.VariantDefinition("A", "A"), default)
    elif problem == "duplicate variant":
        variants = (default, default)
    else:
        variants = (default, records.VariantDefinition("A", "A"))
    with pytest.raises(ValueError, match=message):
        records.BoardSnapshot(fake.board_info(), variants, tuple(components))


def test_states_are_frozen(backend: SimpleNamespace) -> None:
    """Field maps cannot be edited through a record."""
    records = backend.records
    state = records.component_state(
        [records.FieldRecord("LCSC", "C1", False)], dnp=True
    )
    with pytest.raises(TypeError):
        state.fields["LCSC"] = records.FieldRecord("LCSC", "C2", False)  # type: ignore[index]
    assert (state.dnp, state.exclude_from_bom, state.exclude_from_pos) == (
        True,
        False,
        False,
    )
    assert (state.replaced_fields, state.duplicate_names) == ((), ())


def test_a_repeated_field_name_resolves_as_kicad_reopens_the_board(
    backend: SimpleNamespace,
) -> None:
    """The later field takes the earlier one's place; the earlier one is kept apart.

    KiCad's footprint dialog accepts a second field with a name the
    footprint already has. On reopening the board, KiCad sets the first
    field of that name to each later one's text and visibility in turn.
    """
    field = backend.records.FieldRecord
    state = backend.records.component_state(
        [
            field("A", "a1", True),
            field("LCSC", "C1", True),
            field("MPN", "X", True),
            field("LCSC", "C2", False),
            field("A", "a2", True),
            field("LCSC", "C3", True),
        ]
    )

    assert list(state.fields.values()) == [
        field("A", "a2", True),
        field("LCSC", "C3", True),
        field("MPN", "X", True),
    ]
    assert state.replaced_fields == (
        field("LCSC", "C1", True),
        field("A", "a1", True),
        field("LCSC", "C2", False),
    )
    assert state.duplicate_names == ("LCSC", "A")


def test_a_replaced_field_needs_a_field_of_its_name(
    backend: SimpleNamespace,
) -> None:
    """A state built by hand cannot record a replaced field with no successor."""
    records = backend.records
    field = records.FieldRecord("LCSC", "C1", False)
    with pytest.raises(ValueError, match="later field of its name"):
        records.ComponentState(
            {"LCSC": field},
            False,
            False,
            False,
            (records.FieldRecord("MPN", "X", True),),
        )


@pytest.mark.parametrize(
    "change",
    [
        {"fields": {"LCSC": "C2"}},
        {"fields": (("LCSC", "C0"), ("LCSC", "C1"))},
        {"hidden": ()},
        {"dnp": True},
        {"exclude_from_bom": True},
        {"exclude_from_pos": True},
    ],
)
def test_a_revision_names_exactly_one_state(
    backend: SimpleNamespace, change: dict[str, object]
) -> None:
    """Equal states share a revision; any field text, visibility or flag changes it.

    A footprint holding two LCSC fields reads with the later one, as the
    plain footprint does, and still has another revision.
    """
    fake = backend.fake
    options: dict[str, object] = {"fields": {"LCSC": "C1"}, "hidden": ("LCSC",)}
    base = fake.component("R1", **options)
    same = fake.component("R1", uuid="another", **options)
    changed = fake.component("R1", **{**options, **change})

    assert base.default.revision == same.default.revision
    assert len(base.default.revision) == 64
    assert changed.default.revision != base.default.revision


def test_a_revision_covers_field_names(backend: SimpleNamespace) -> None:
    """The same text under another name is another state."""
    records = backend.records
    base = records.component_state([records.FieldRecord("LCSC", "C1", False)])
    renamed = records.component_state([records.FieldRecord("JLCPCB", "C1", False)])

    assert renamed.revision != base.revision


def test_a_state_keys_fields_by_name(backend: SimpleNamespace) -> None:
    """A state built without component_state cannot mislabel a field."""
    records = backend.records
    field = records.FieldRecord("LCSC", "C1", False)
    with pytest.raises(ValueError, match="keyed by their names"):
        records.ComponentState({"MPN": field}, False, False, False)


def test_a_revision_is_computed_when_first_read(backend: SimpleNamespace) -> None:
    """Reads that never quote a revision never compute one; the first read keeps it.

    Equal states built either way share it.
    """
    records = backend.records
    field = records.FieldRecord("LCSC", "C1", False)
    state = records.ComponentState({"LCSC": field}, True, False, False)
    assert state._revision == ""

    revision = state.revision

    assert state._revision == revision
    assert revision == records.component_state([field], dnp=True).revision
    assert state == records.component_state([field], dnp=True)


def test_fake_components_list_fields_as_kicad_does(backend: SimpleNamespace) -> None:
    """Reference and Value, the hidden Datasheet and Description, then user fields."""
    record = backend.fake.component(
        "R1", value="10k", fields={"LCSC": "C1", "MPN": "X"}, hidden=("LCSC",)
    )
    assert [
        (field.name, field.text, field.visible)
        for field in record.default.fields.values()
    ] == [
        ("Reference", "R1", True),
        ("Value", "10k", True),
        ("Datasheet", "", False),
        ("Description", "", False),
        ("LCSC", "C1", False),
        ("MPN", "X", True),
    ]


def test_fake_components_can_repeat_a_field_name(backend: SimpleNamespace) -> None:
    """A name given again adds a second field after the others, as KiCad's dialog can."""
    record = backend.fake.component(
        "R1",
        fields=(("LCSC", "C1"), ("MPN", "X"), ("LCSC", "C2")),
        hidden=("LCSC",),
    )
    state = record.default
    assert [(field.name, field.text) for field in state.fields.values()][4:] == [
        ("LCSC", "C2"),
        ("MPN", "X"),
    ]
    assert state.replaced_fields == (backend.records.FieldRecord("LCSC", "C1", False),)
    assert state.duplicate_names == ("LCSC",)


def test_fake_fields_named_like_a_mandatory_field_repeat_it(
    backend: SimpleNamespace,
) -> None:
    """A footprint field named "Value" comes after KiCad's own Value, as a repeat."""
    state = backend.fake.component("R1", value="10k", fields={"Value": "22k"}).default
    assert state.fields["Value"].text == "22k"
    assert state.replaced_fields == (backend.records.FieldRecord("Value", "10k", True),)
    assert state.duplicate_names == ("Value",)


def test_a_records_states_cannot_change_after_it_is_made(
    backend: SimpleNamespace,
) -> None:
    """A snapshot indexes its records, so their states and pads stay as read."""
    fake = backend.fake
    record = fake.component("R1", pads=[fake.pad()])

    with pytest.raises(TypeError):
        record.states["Alternative"] = record.default  # type: ignore[index]
    assert record.pads == (fake.pad(),)


def test_fake_pad_holes_follow_their_kind(backend: SimpleNamespace) -> None:
    """Plated through and bare holes have holes; SMD and connector pads do not."""
    fake = backend.fake
    assert fake.pad() == backend.records.PadRecord("smd", False)
    assert fake.pad("pth").has_hole and fake.pad("npth").has_hole
    assert not fake.pad("conn").has_hole
    assert fake.pad("smd", has_hole=True).has_hole


def test_a_closed_fake_board_raises_board_changed(backend: SimpleNamespace) -> None:
    """Closing the board fails reads and edits the way a real backend does."""
    board = backend.fake.FakeBackend([backend.fake.component("R1")])
    (r1,) = board.snapshot().components
    assert board.snapshot().info is board.info() is board.board
    assert board.reads == 2
    board.board = None
    with pytest.raises(backend.api.BoardChanged):
        board.info()
    with pytest.raises(backend.api.BoardChanged):
        board.snapshot()
    edit = backend.api.default_edit(r1, flags=(("exclude_from_bom", True),))
    with pytest.raises(backend.api.BoardChanged):
        board.edits_needed([edit])
    with pytest.raises(backend.api.BoardChanged):
        board.apply("edit", [edit])
    assert board.applied == []
    assert issubclass(backend.api.BoardChanged, backend.api.BackendError)
    assert issubclass(backend.api.BackendError, RuntimeError)


def test_footprints_sharing_a_uuid_keep_it_and_are_keyed_apart(
    backend: SimpleNamespace,
) -> None:
    """Each keeps its UUID; the second with one is keyed "#2", in board order.

    KiCad gives every footprint its own UUID, but a board file can hold two
    with one, and KiCad keeps both when it opens the file.
    """
    fake = backend.fake
    first, other, second = (
        fake.component("R1", uuid="same"),
        fake.component("C1", uuid="c1"),
        fake.component("R2", uuid="same"),
    )
    snapshot = fake.FakeBackend([first, other, second]).snapshot()

    assert [(c.reference, c.uuid, c.key) for c in snapshot.components] == [
        ("R1", "same", "same"),
        ("C1", "c1", "c1"),
        ("R2", "same", "same#2"),
    ]
    assert snapshot.get("same") is first
    assert snapshot.get("same#2").reference == "R2"
    assert snapshot.by_reference("R2") == (snapshot.get("same#2"),)


def test_footprint_keys_count_each_uuid_in_board_order(
    backend: SimpleNamespace,
) -> None:
    """The nth footprint with a UUID gets "#n"; the first keeps the bare UUID."""
    assert backend.records.footprint_keys(["a", "b", "a", "a", "b"]) == [
        "a",
        "b",
        "a#2",
        "a#3",
        "b#2",
    ]


def test_variant_definitions_have_one_home(backend: SimpleNamespace) -> None:
    """The variant adapter reuses the record type instead of declaring its own."""
    with load_siblings(
        "_kicad_backend_variant_tests",
        ("variant.native", "kicad_backend.records"),
        {},
    ) as loaded:
        native: ModuleType = loaded["variant.native"]
        records: ModuleType = loaded["kicad_backend.records"]
        assert native.VariantDefinition is records.VariantDefinition


def test_an_edit_holds_its_operations_as_tuples(backend: SimpleNamespace) -> None:
    """Lists given to an edit become tuples, so a batch cannot change once made."""
    edit = backend.records.ComponentEdit(
        "uuid",
        "revision",
        set_fields=[["LCSC", "C1"]],
        field_visibility=[["LCSC", False]],
        flags=[["exclude_from_bom", True]],
    )

    assert edit.set_fields == (("LCSC", "C1"),)
    assert edit.field_visibility == (("LCSC", False),)
    assert edit.flags == (("exclude_from_bom", True),)


def test_a_default_edit_names_the_footprints_key_and_quotes_its_revision(
    backend: SimpleNamespace,
) -> None:
    """The second footprint sharing a UUID is addressed by its own key."""
    fake = backend.fake
    snapshot = fake.FakeBackend(
        [fake.component("R1", uuid="shared"), fake.component("R2", uuid="shared")]
    ).snapshot()
    second = snapshot.components[1]

    edit = backend.records.default_edit(second, flags=(("exclude_from_pos", True),))

    assert (edit.key, edit.based_on, edit.flags) == (
        "shared#2",
        second.default.revision,
        (("exclude_from_pos", True),),
    )


@pytest.mark.parametrize(
    "operations",
    [
        {"set_fields": (("LCSC", 1),)},
        {"set_fields": (("", "C1"),)},
        {"set_fields": (("LCSC", "C1"), ("LCSC", "C2"))},
        {"field_visibility": (("LCSC", 0),)},
        {"flags": (("exclude_from_bom", 1),)},
        {"flags": (("exclude_from_bom", True), ("exclude_from_bom", False))},
        {"flags": (("dnp", True),)},
    ],
)
def test_a_malformed_edit_is_rejected_before_the_board_is_read(
    backend: SimpleNamespace, operations: dict[str, object]
) -> None:
    """Text, names and flags are checked by type, once, for every backend."""
    edit = backend.records.ComponentEdit("uuid", "revision", **operations)

    with pytest.raises(backend.api.EditRejected):
        backend.edits.check_batch([edit])
    with pytest.raises(backend.api.EditRejected):
        backend.edits.check_batch(["not an edit"])


def test_a_set_field_keeps_its_place_and_a_new_one_goes_last(
    backend: SimpleNamespace,
) -> None:
    """Visibility stays unless the edit changes it; new fields follow the rule given."""
    state = backend.fake.component("R1", fields={"LCSC": "C1", "MPN": "X"}).default
    edit = backend.records.ComponentEdit(
        "uuid", state.revision, set_fields=(("JLC", "C2"), ("LCSC", "C3"))
    )

    shown = backend.edits.edited_state(state, edit)
    hidden = backend.edits.edited_state(state, edit, new_fields_shown=False)

    assert [(f.name, f.text, f.visible) for f in shown.fields.values()][4:] == [
        ("LCSC", "C3", True),
        ("MPN", "X", True),
        ("JLC", "C2", True),
    ]
    assert hidden.fields["JLC"].visible is False
    assert backend.edits.same_state(shown, shown)
    assert not backend.edits.same_state(shown, hidden)
    assert (
        backend.edits.edited_state(
            state, backend.records.ComponentEdit("uuid", state.revision)
        ).revision
        == state.revision
    )


def test_same_state_compares_a_repeated_names_fields_in_order(
    backend: SimpleNamespace,
) -> None:
    """Fields of one name holding swapped texts are another state; names may move."""
    records = backend.records

    def state(*fields: tuple[str, str]) -> object:
        return records.component_state(
            records.FieldRecord(name, text, True) for name, text in fields
        )

    first = state(("LCSC", "C1"), ("MPN", "X"), ("LCSC", "C2"))
    swapped = state(("LCSC", "C2"), ("MPN", "X"), ("LCSC", "C1"))
    moved = state(("MPN", "X"), ("LCSC", "C1"), ("LCSC", "C2"))

    assert not backend.edits.same_state(first, swapped)
    assert backend.edits.same_state(first, moved)


def test_the_fake_writes_only_what_changes_and_records_each_batch(
    backend: SimpleNamespace,
) -> None:
    """A no-op edit is not written, so a write failure set on it never fires.

    A batch that changes nothing is not recorded, as KiCad gives it no undo
    step.
    """
    fake = backend.fake
    board = fake.FakeBackend([fake.component("R1"), fake.component("R2")])
    r1, r2 = board.snapshot().components
    board.fail_writes.add(r2.key)
    edits = [
        backend.records.default_edit(r1, flags=(("exclude_from_bom", True),)),
        backend.records.default_edit(r2, flags=(("exclude_from_bom", False),)),
    ]

    after = board.apply("mark R1", edits)

    assert board.applied == ["mark R1"]
    assert after.get(r1.key).default.exclude_from_bom
    assert after.get(r2.key) == r2
    assert board.reads == 2
    r1 = after.get(r1.key)
    unchanged = board.apply(
        "mark R1 again",
        [backend.records.default_edit(r1, flags=(("exclude_from_bom", True),))],
    )
    assert board.applied == ["mark R1"]
    assert unchanged.get(r1.key) == r1


def test_an_edit_to_a_repeated_name_digests_as_the_written_footprint_reads(
    backend: SimpleNamespace,
) -> None:
    """Every field of the name is written, and the revision is the one a read gives.

    The state records each later field where it replaced an earlier one, so
    the edited state resolves and digests like the footprint read in its own
    order once every field is written.
    """
    records = backend.records
    order = [
        ("Reference", "R1"),
        ("LCSC", "C1"),
        ("MPN", "X"),
        ("MPN", "Y"),
        ("LCSC", "C2"),
        ("LCSC", "C3"),
    ]
    state = records.component_state(
        records.FieldRecord(name, text, True) for name, text in order
    )
    edit = records.ComponentEdit(
        "uuid", state.revision, set_fields=(("LCSC", "C9"), ("JLC", "C4"))
    )
    written = [(name, "C9" if name == "LCSC" else text) for name, text in order]

    assert backend.edits.edited_state(state, edit) == records.component_state(
        records.FieldRecord(name, text, True)
        for name, text in (*written, ("JLC", "C4"))
    )
    assert backend.edits.copies(state)["LCSC"] == [
        ("C1", True),
        ("C2", True),
        ("C3", True),
    ]


def test_each_field_of_a_name_held_three_times_keeps_its_own_visibility(
    backend: SimpleNamespace,
) -> None:
    """The third field of a name is rebuilt from the third, not the second."""
    records = backend.records
    order = [("LCSC", "C1", True), ("LCSC", "C2", False), ("LCSC", "C3", True)]
    state = records.component_state(records.FieldRecord(*field) for field in order)
    edit = records.ComponentEdit("uuid", state.revision, set_fields=(("LCSC", "C9"),))

    edited = backend.edits.edited_state(state, edit)

    assert backend.edits.copies(edited)["LCSC"] == [
        ("C9", True),
        ("C9", False),
        ("C9", True),
    ]
    assert edited == records.component_state(
        records.FieldRecord(name, "C9", shown) for name, _text, shown in order
    )
