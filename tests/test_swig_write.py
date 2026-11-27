"""The SWIG backend writes edit batches whole, reads them back, and undoes failures."""

from collections.abc import Callable, Iterator
from copy import deepcopy
import logging
import os
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from . import swig_doubles
from .native_kicad_support import add_native_field, native_field, set_native_field
from .wx_harness import load_siblings


@pytest.fixture(scope="module")
def swig() -> Iterator[SimpleNamespace]:
    """Load the backend under a scoped name, without KiCad or wx."""
    with load_siblings(
        "_swig_write_tests",
        (
            "kicad_backend",
            "kicad_backend.swig",
            "kicad_backend.swig.dialects",
            "kicad_backend.swig.read",
            "kicad_backend.swig.write",
        ),
        {},
    ) as modules:
        yield SimpleNamespace(
            api=modules["kicad_backend"],
            SwigBackend=modules["kicad_backend.swig"].SwigBackend,
            dialects=modules["kicad_backend.swig.dialects"],
            read=modules["kicad_backend.swig.read"],
            write=modules["kicad_backend.swig.write"],
        )


class Footprint(swig_doubles.Footprint):
    """A KiCad 9 or 10 footprint whose writes can be made to fail."""

    def __init__(self, reference: str, fields: Optional[dict[str, str]] = None) -> None:  # noqa: UP045
        super().__init__(reference, fields=fields or {}, attributes=0x41)
        self.modified_count = 0
        self.fail_after = ""
        self.reject_text = ""
        self.silent_failure = False

    def SetField(self, name: str, text: str) -> None:
        """Write a field, or change it and then fail, or quietly do nothing."""
        if self.silent_failure:
            return
        if text == self.reject_text:
            raise RuntimeError("restoration rejected")
        super().SetField(name, text)
        if name == self.fail_after:
            self.fail_after = ""
            raise RuntimeError("native setter changed the field then failed")

    def Remove(self, field: Any) -> None:
        """Remove a field object, as KiCad 8 and later do."""
        del self.fields[field.name]

    def SetModified(self) -> None:
        """Count the batch's modified notifications."""
        self.modified_count += 1


class RepeatedFootprint(Footprint):
    """A footprint given a second field of a name it already has, as KiCad's dialog can."""

    def __init__(
        self, reference: str, fields: dict[str, str], repeated: dict[str, str]
    ) -> None:
        super().__init__(reference, fields)
        self.repeated = [
            swig_doubles.Field(name, text) for name, text in repeated.items()
        ]

    def GetFields(self) -> list[Any]:
        """Return the fields, the repeated ones last."""
        return [*super().GetFields(), *self.repeated]

    def Remove(self, field: Any) -> None:
        """Remove one field object."""
        if field in self.repeated:
            self.repeated.remove(field)
        else:
            super().Remove(field)


def every_field(footprint: Footprint, name: str) -> list[tuple[str, bool]]:
    """Return (text, visible) of every field with this name, in order."""
    return [(f.text, f.visible) for f in footprint.GetFields() if f.GetName() == name]


class LegacyFootprint(Footprint):
    """A KiCad 7 footprint: a property map instead of field objects, and no DNP.

    Reference and Value are text items of their own, and shown.
    """

    def __init__(self, reference: str, properties: dict[str, str]) -> None:
        super().__init__(reference)
        self.properties = dict(properties)
        self.fail_properties = False

    def Reference(self) -> SimpleNamespace:
        """Return the reference's text item."""
        return SimpleNamespace(IsVisible=lambda: True)

    def Value(self) -> SimpleNamespace:
        """Return the value's text item."""
        return SimpleNamespace(IsVisible=lambda: True)

    def GetFields(self) -> list[Any]:
        """KiCad 7 has no field objects."""
        raise AttributeError("GetFields")

    def GetProperties(self) -> dict[str, str]:
        """Read a copy of the property map."""
        return dict(self.properties)

    def SetProperties(self, properties: dict[str, str]) -> None:
        """Replace the property map, optionally failing after changing it."""
        self.properties = dict(properties)
        if self.fail_properties:
            self.fail_properties = False
            raise RuntimeError("legacy native write failed")

    def __getattribute__(self, name: str) -> Any:
        """KiCad 7 footprints have no IsDNP."""
        if name == "IsDNP":
            raise AttributeError(name)
        return super().__getattribute__(name)


def user_fields(footprint: Footprint) -> dict[str, tuple[str, bool]]:
    """Return a footprint's own fields as {name: (text, visible)}."""
    return {name: (f.text, f.visible) for name, f in footprint.fields.items()}


def state(footprint: Footprint) -> tuple[dict[str, tuple[str, bool]], int, bool]:
    """Snapshot what the tests assert: own fields, raw attribute bits and DNP."""
    return user_fields(footprint), footprint.attributes, footprint.dnp


def make(
    swig: SimpleNamespace,
    *footprints: Footprint,
    commit: Any = None,
    version: str = "10.0.4",
) -> Any:
    """Open the footprints on a board of this KiCad version, behind a SWIG backend."""
    board = swig_doubles.Board(footprints)
    return swig.SwigBackend(swig_doubles.editor(board, version), commit=commit)


def edit(swig: SimpleNamespace, backend: Any, reference: str, **operations: Any) -> Any:
    """Edit a footprint from the backend's current read of it."""
    (component,) = backend.snapshot().by_reference(reference)
    return swig.api.default_edit(component, **operations)


def test_a_batch_sets_fields_and_flags_and_marks_each_footprint_modified(
    swig: SimpleNamespace,
) -> None:
    """Fields keep their visibility, other fields and attribute bits stay."""
    fp = Footprint(
        "R1",
        {"LCSC Part #": " c100 ", "LCSC": "C200", "JLCPCB Rotation": "90"},
    )
    fp.fields["LCSC"].visible = False
    backend = make(swig, fp)

    backend.apply(
        "assign",
        [
            edit(
                swig,
                backend,
                "R1",
                set_fields=(("LCSC Part #", "C777"), ("LCSC", "C777")),
                flags=(("exclude_from_bom", True),),
            )
        ],
    )

    assert state(fp) == (
        {
            "LCSC Part #": ("C777", True),
            "LCSC": ("C777", False),
            "JLCPCB Rotation": ("90", True),
        },
        0x49,
        False,
    )
    assert fp.modified_count == 1


def test_a_new_field_can_be_hidden_and_a_no_op_writes_nothing(
    swig: SimpleNamespace,
) -> None:
    """A repeated edit is not written and does not mark the footprint again."""
    fp = Footprint("R1", {"Manufacturer": "Acme"})
    backend = make(swig, fp)
    hidden = {"set_fields": (("LCSC", "C123"),), "field_visibility": (("LCSC", False),)}

    backend.apply("assign", [edit(swig, backend, "R1", **hidden)])
    assert backend.edits_needed([edit(swig, backend, "R1", **hidden)]) is False
    backend.apply(
        "assign",
        [edit(swig, backend, "R1", **hidden, flags=(("exclude_from_bom", False),))],
    )

    assert user_fields(fp)["LCSC"] == ("C123", False)
    assert fp.modified_count == 1


@pytest.mark.parametrize(
    ("fields", "operations", "needed"),
    [
        ({"LCSC": "C123"}, {"set_fields": (("LCSC", "C123"),)}, False),
        ({"LCSC": " c123 "}, {"set_fields": (("LCSC", "C123"),)}, True),
        ({"Manufacturer": "Acme"}, {"set_fields": (("LCSC", ""),)}, True),
        ({"LCSC": "C123"}, {"flags": (("exclude_from_pos", False),)}, False),
        ({"LCSC": "C123"}, {"flags": (("exclude_from_bom", True),)}, True),
        ({"LCSC": "C123"}, {"field_visibility": (("LCSC", True),)}, False),
        ({"LCSC": "C123"}, {"field_visibility": (("LCSC", False),)}, True),
    ],
)
def test_edits_needed_reads_without_writing(
    swig: SimpleNamespace,
    fields: dict[str, str],
    operations: dict[str, Any],
    needed: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KiCad's undo step is skipped for a no-op batch; checking writes nothing."""
    fp = Footprint("R1", fields)
    backend = make(swig, fp)
    batch = [edit(swig, backend, "R1", **operations)]
    before = deepcopy(state(fp))

    def reject_write(*_args: Any) -> None:
        raise AssertionError("A check attempted a native write")

    for method in ("SetField", "SetAttributes", "SetModified", "Remove"):
        monkeypatch.setattr(fp, method, reject_write)
    assert backend.edits_needed(batch) is needed
    assert state(fp) == before


def test_the_whole_batch_is_checked_before_kicads_undo_step_opens(
    swig: SimpleNamespace,
) -> None:
    """An invalid later edit stops the batch before the commit wrapper is entered."""
    entered: list[bool] = []
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    backend = make(swig, first, second, commit=lambda write: entered.append(True))
    batch = [
        edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
        edit(swig, backend, "R2", flags=(("exclude_from_bom", 1),)),
    ]

    with pytest.raises(swig.api.EditRejected):
        backend.apply("assign", batch)
    assert entered == []
    assert first.fields["LCSC"].text == "C100"


@pytest.mark.parametrize("batch", ["stale", "no change", "no such field", "gone"])
def test_a_batch_the_board_refuses_or_that_changes_nothing_opens_no_undo_step(
    swig: SimpleNamespace, batch: str
) -> None:
    """KiCad's wrapper is entered only for a batch that will change the board.

    A refusal that depends on the board, as much as one that does not, is
    raised before the wrapper opens, and a batch that changes nothing does
    not open it: KiCad would record an empty undo step and mark the board
    modified.
    """
    entered: list[bool] = []
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    backend = make(swig, first, second, commit=lambda write: entered.append(True))
    edits = {
        "no change": [edit(swig, backend, "R1", set_fields=(("LCSC", "C100"),))],
        "no such field": [
            edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
            edit(swig, backend, "R2", field_visibility=(("MPN", True),)),
        ],
        "stale": [edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),))],
        "gone": [
            edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
            swig.api.ComponentEdit("not-on-this-board", "revision"),
        ],
    }[batch]
    if batch == "stale":
        first.fields["LCSC"].text = "C101"  # changed in the PCB editor since the read

    if batch == "no change":
        backend.apply("assign", edits)
    else:
        with pytest.raises((swig.api.StaleTarget, swig.api.EditRejected)):
            backend.apply("assign", edits)

    assert entered == []
    assert (first.modified_count, second.modified_count) == (0, 0)


def test_the_second_footprint_sharing_a_uuid_is_written_by_its_key(
    swig: SimpleNamespace,
) -> None:
    """Two footprints with one UUID are found by their own keys, in board order."""
    first = Footprint("R1", {"LCSC": "C100"})
    second = Footprint("R2", {"LCSC": "C200"})
    second.uuid = first.uuid
    backend = make(swig, first, second)

    backend.apply("assign", [edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),))])

    assert (first.fields["LCSC"].text, second.fields["LCSC"].text) == ("C100", "C999")
    assert (first.modified_count, second.modified_count) == (0, 1)


def test_a_footprint_changed_since_it_was_read_writes_nothing(
    swig: SimpleNamespace,
) -> None:
    """An edit quotes the state it was made from; a newer state stops the batch."""
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    backend = make(swig, first, second)
    batch = [
        edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
        edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
    ]
    second.fields["LCSC"].text = "C300"

    with pytest.raises(swig.api.StaleTarget, match="R2 changed"):
        backend.apply("assign", batch)
    assert (first.fields["LCSC"].text, second.fields["LCSC"].text) == ("C100", "C300")


def test_flag_edits_keep_dnp_and_the_other_attribute_bits(
    swig: SimpleNamespace,
) -> None:
    """Only the requested exclusion bits change, including turning one off again."""
    fp = Footprint("R1", {"LCSC": "C100"})
    backend = make(swig, fp)

    backend.apply(
        "exclude",
        [
            edit(
                swig,
                backend,
                "R1",
                flags=(("exclude_from_bom", True), ("exclude_from_pos", True)),
            )
        ],
    )
    assert fp.attributes == 0x4D
    backend.apply(
        "include", [edit(swig, backend, "R1", flags=(("exclude_from_bom", False),))]
    )
    assert (fp.dnp, fp.attributes) == (False, 0x45)


def test_a_later_write_failure_puts_back_every_alias_and_flag(
    swig: SimpleNamespace,
) -> None:
    """Undo restores the raw values, including conflicting aliases and visibility."""
    first = Footprint(
        "R1", {"LCSC": " c100 ", "JLCPCB": "C101", "JLCPCB Rotation": "90"}
    )
    first.fields["JLCPCB"].visible = False
    second = Footprint("R2", {"LCSC": "C200", "JLCPCB": "C201"})
    second.fail_after = "LCSC"
    backend = make(swig, first, second)
    before = [deepcopy(state(first)), deepcopy(state(second))]

    with pytest.raises(swig.api.EditRejected, match="native setter changed"):
        backend.apply(
            "assign",
            [
                edit(
                    swig,
                    backend,
                    "R1",
                    set_fields=(("LCSC", "C999"), ("JLCPCB", "C999")),
                    flags=(("exclude_from_pos", True),),
                ),
                edit(
                    swig,
                    backend,
                    "R2",
                    set_fields=(("LCSC", "C999"), ("JLCPCB", "C999")),
                ),
            ],
        )

    assert [state(first), state(second)] == before
    assert first.modified_count == second.modified_count == 0


def test_a_later_write_failure_puts_back_visibility_and_exclusions(
    swig: SimpleNamespace,
) -> None:
    """A field the batch showed and an exclusion it set go back as they were."""
    first = Footprint("R1", {"LCSC": "C100", "MPN": "RC0603"})
    first.fields["MPN"].visible = False
    second = Footprint("R2", {"LCSC": "C200"})
    second.fail_after = "LCSC"
    backend = make(swig, first, second)
    before = [deepcopy(state(first)), deepcopy(state(second))]

    with pytest.raises(swig.api.EditRejected, match="native setter changed"):
        backend.apply(
            "edit",
            [
                edit(
                    swig,
                    backend,
                    "R1",
                    field_visibility=(("MPN", True),),
                    flags=(("exclude_from_bom", True),),
                ),
                edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
            ],
        )

    assert [state(first), state(second)] == before


def test_undo_removes_a_new_field_even_when_the_setter_failed_after_adding_it(
    swig: SimpleNamespace,
) -> None:
    """An aborted assignment leaves no empty LCSC field behind."""
    fp = Footprint("R1", {"Manufacturer": "Acme"})
    fp.fail_after = "LCSC"
    backend = make(swig, fp)
    before = deepcopy(state(fp))

    with pytest.raises(swig.api.EditRejected):
        backend.apply(
            "assign", [edit(swig, backend, "R1", set_fields=(("LCSC", "C9"),))]
        )
    assert state(fp) == before


def test_a_setter_that_reports_success_but_changes_nothing_is_caught(
    swig: SimpleNamespace,
) -> None:
    """Each write is read back; earlier footprints are put back."""
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    second.silent_failure = True
    backend = make(swig, first, second)

    with pytest.raises(swig.api.EditRejected, match="did not preserve"):
        backend.apply(
            "assign",
            [
                edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
                edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
            ],
        )
    assert (first.fields["LCSC"].text, second.fields["LCSC"].text) == ("C100", "C200")


def test_an_attribute_setter_that_changes_then_fails_is_undone(
    swig: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The LCSC field and both exclusion bits come back together."""
    fp = Footprint("R1", {"LCSC": "C100"})
    backend = make(swig, fp)
    before = deepcopy(state(fp))

    def reject_changed_attributes(value: int) -> None:
        fp.attributes = value
        if value != before[1]:
            raise RuntimeError("attribute native write failed")

    monkeypatch.setattr(fp, "SetAttributes", reject_changed_attributes)
    with pytest.raises(swig.api.EditRejected, match="attribute native write failed"):
        backend.apply(
            "assign",
            [
                edit(
                    swig,
                    backend,
                    "R1",
                    set_fields=(("LCSC", "C999"),),
                    flags=(("exclude_from_bom", True), ("exclude_from_pos", True)),
                )
            ],
        )
    assert state(fp) == before


def test_a_write_that_changes_another_attribute_bit_is_undone(
    swig: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the requested bits may change; losing another one fails the batch."""
    fp = Footprint("R1", {"LCSC": "C100"})
    backend = make(swig, fp)

    def drop_through_hole(value: int) -> None:
        # Putting the original bits back works; the requested write loses one.
        fp.attributes = value if value == 0x41 else value & ~0x01

    monkeypatch.setattr(fp, "SetAttributes", drop_through_hole)
    with pytest.raises(swig.api.EditRejected, match="did not preserve"):
        backend.apply(
            "exclude", [edit(swig, backend, "R1", flags=(("exclude_from_bom", True),))]
        )
    assert fp.attributes == 0x41


def test_an_unreadable_later_footprint_stops_the_first_write(
    swig: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every target is read before any is written."""
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    backend = make(swig, first, second)
    batch = [
        edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
        edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
    ]

    def reject_read() -> list[Any]:
        raise RuntimeError("footprint no longer readable")

    monkeypatch.setattr(second, "GetFields", reject_read)
    with pytest.raises(swig.api.EditRejected, match="no longer readable"):
        backend.apply("assign", batch)
    assert first.fields["LCSC"].text == "C100"
    assert first.modified_count == 0


def test_every_field_of_a_repeated_name_gets_the_number(
    swig: SimpleNamespace,
) -> None:
    """None is deleted, so the edit holds whichever field KiCad keeps."""
    fp = RepeatedFootprint("R1", {"LCSC": "C100", "MPN": "X"}, {"LCSC": "C101"})
    fp.repeated[0].visible = False
    backend = make(swig, fp)
    batch = [
        edit(
            swig,
            backend,
            "R1",
            set_fields=(("LCSC", "C999"),),
            field_visibility=(("LCSC", False),),
        )
    ]

    assert backend.edits_needed(batch)
    backend.apply("assign", batch)

    assert every_field(fp, "LCSC") == [("C999", False), ("C999", False)]
    assert every_field(fp, "MPN") == [("X", True)]
    assert fp.modified_count == 1


def test_a_repeated_name_whose_earlier_field_differs_still_needs_a_write(
    swig: SimpleNamespace,
) -> None:
    """The later field already holds the number; the earlier one does not."""
    fp = RepeatedFootprint("R1", {"LCSC": "C100"}, {"LCSC": "C999"})
    backend = make(swig, fp)
    batch = [edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),))]

    assert backend.edits_needed(batch)
    backend.apply("assign", batch)
    assert every_field(fp, "LCSC") == [("C999", True), ("C999", True)]


def test_a_failed_write_puts_back_each_field_of_a_repeated_name(
    swig: SimpleNamespace,
) -> None:
    """Each field returns to its own text and visibility."""
    first = RepeatedFootprint("R1", {"LCSC": "C100"}, {"LCSC": "C101"})
    first.repeated[0].visible = False
    second = Footprint("R2", {"LCSC": "C200"})
    second.fail_after = "LCSC"
    backend = make(swig, first, second)
    batch = [
        edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
        edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
    ]

    with pytest.raises(swig.api.EditRejected, match="native setter changed"):
        backend.apply("assign", batch)
    assert every_field(first, "LCSC") == [("C100", True), ("C101", False)]
    assert second.fields["LCSC"].text == "C200"


def test_a_repeated_field_that_does_not_keep_its_write_is_caught(
    swig: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every field of the name is read back, not only the one KiCad keeps."""
    fp = RepeatedFootprint("R1", {"LCSC": "C100"}, {"LCSC": "C101"})
    backend = make(swig, fp)
    earlier = fp.fields["LCSC"]
    monkeypatch.setattr(earlier, "SetText", lambda text: None)

    with pytest.raises(swig.api.EditRejected, match="did not preserve"):
        backend.apply(
            "assign", [edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),))]
        )
    assert every_field(fp, "LCSC") == [("C100", True), ("C101", True)]


@pytest.mark.parametrize("method", ["GetAttributes", "GetFields"])
def test_a_failed_read_back_does_not_stop_the_fields_being_put_back(
    swig: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    """Restoring writes the captured text before it reads anything again."""
    fp = Footprint("R1", {"LCSC": "C100"})
    backend = make(swig, fp)
    batch = [edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),))]
    original = getattr(fp, method)

    def read() -> Any:
        if fp.fields["LCSC"].text != "C100":
            raise RuntimeError(f"{method} temporarily unavailable")
        return original()

    monkeypatch.setattr(fp, method, read)
    with pytest.raises(swig.api.EditRejected, match="temporarily unavailable"):
        backend.apply("assign", batch)
    assert fp.fields["LCSC"].text == "C100"


def test_a_failure_that_cannot_be_undone_stops_every_later_edit(
    swig: SimpleNamespace,
) -> None:
    """The other footprints are still put back, and the backend refuses to go on."""
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    second.fail_after = "LCSC"
    second.reject_text = "C200"
    backend = make(swig, first, second)
    batch = [
        edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
        edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
    ]

    with pytest.raises(swig.api.EditRecoveryFailed, match="recovery failed"):
        backend.apply("assign", batch)
    assert (first.fields["LCSC"].text, second.fields["LCSC"].text) == ("C100", "C999")
    retry = [edit(swig, backend, "R1", flags=(("exclude_from_bom", True),))]
    with pytest.raises(swig.api.EditRecoveryFailed):
        backend.edits_needed(retry)
    with pytest.raises(swig.api.EditRecoveryFailed):
        backend.apply("again", retry)
    assert backend.snapshot().components


def test_a_failed_modified_notification_undoes_the_whole_batch(
    swig: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Finishing the batch can still fail; nothing it wrote is kept."""
    first, second = Footprint("R1", {"LCSC": "C100"}), Footprint("R2", {"LCSC": "C200"})
    backend = make(swig, first, second)

    def reject_modified() -> None:
        raise RuntimeError("native dirty notification failed")

    monkeypatch.setattr(second, "SetModified", reject_modified)
    with pytest.raises(swig.api.EditRejected, match="dirty notification failed"):
        backend.apply(
            "assign",
            [
                edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),)),
                edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
            ],
        )
    assert (first.fields["LCSC"].text, second.fields["LCSC"].text) == ("C100", "C200")


@pytest.mark.parametrize(
    ("properties", "lcsc", "needed"),
    [
        ({"LCSC": "C123", "JLCPCB": "C123"}, "C123", False),
        ({"LCSC": "C123", "JLCPCB": "C999"}, "C123", True),
        ({"Manufacturer": "Acme"}, "", True),
    ],
)
def test_kicad7_property_maps_are_checked_without_writing(
    swig: SimpleNamespace,
    properties: dict[str, str],
    lcsc: str,
    needed: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KiCad 7's property maps are read and compared like fields."""
    fp = LegacyFootprint("R1", properties)
    backend = make(swig, fp, version="7.0.11")
    names = [name for name in properties if name != "Manufacturer"] or ["LCSC"]
    batch = [
        edit(swig, backend, "R1", set_fields=tuple((name, lcsc) for name in names))
    ]

    def reject_write(*_args: Any) -> None:
        raise AssertionError("A check attempted a native write")

    monkeypatch.setattr(fp, "SetProperties", reject_write)
    assert backend.edits_needed(batch) is needed
    assert fp.GetProperties() == properties


def test_kicad7_writes_and_undoes_whole_property_maps(swig: SimpleNamespace) -> None:
    """No field or variant APIs are needed to assign on KiCad 7."""
    first = LegacyFootprint("R1", {"Manufacturer": "Acme"})
    second = LegacyFootprint(
        "R2", {"LCSC": " c100 ", "JLCPCB": "C200", "JLCPCB Rotation": "90"}
    )
    second.fail_properties = True
    backend = make(swig, first, second, version="7.0.11")
    before = [first.GetProperties(), second.GetProperties()]
    batch = [
        edit(
            swig,
            backend,
            "R1",
            set_fields=(("LCSC", "C999"),),
            field_visibility=(("LCSC", False),),
        ),
        edit(swig, backend, "R2", set_fields=(("LCSC", "C999"), ("JLCPCB", "C999"))),
    ]

    with pytest.raises(swig.api.EditRejected, match="legacy native write failed"):
        backend.apply("assign", batch)
    assert [first.GetProperties(), second.GetProperties()] == before
    backend.apply("assign", batch[1:])
    assert second.GetProperties() == {
        "LCSC": "C999",
        "JLCPCB": "C999",
        "JLCPCB Rotation": "90",
    }


@pytest.mark.parametrize(
    "operations",
    [
        {"set_fields": (("LCSC", "C1"),), "field_visibility": (("LCSC", True),)},
        {"set_fields": (("Value", "22k"),)},
        {"field_visibility": (("Reference", False),)},
    ],
)
def test_kicad7_refuses_what_it_cannot_store(
    swig: SimpleNamespace, operations: dict[str, Any]
) -> None:
    """Properties are never shown, and Reference and Value are not edited."""
    fp = LegacyFootprint("R1", {"LCSC": "C100"})
    backend = make(swig, fp, version="7.0.11")

    with pytest.raises(swig.api.EditRejected):
        backend.edits_needed([edit(swig, backend, "R1", **operations)])
    assert fp.GetProperties() == {"LCSC": "C100"}


def test_writes_go_through_the_commit_and_the_editor_is_redrawn(
    swig: SimpleNamespace, caplog: pytest.LogCaptureFixture
) -> None:
    """Every write runs inside the commit; a failed redraw is logged, not raised."""
    calls: list[str] = []
    fp = Footprint("R1", {"LCSC": "C100"})
    board = swig_doubles.Board([fp])
    editor = swig_doubles.editor(board)

    def commit(write: Callable[[], None]) -> None:
        calls.append("enter")
        write()
        calls.append("leave")

    def refresh() -> None:
        calls.append("refresh")
        raise RuntimeError("native redraw failed")

    editor.Refresh = refresh
    backend = swig.SwigBackend(editor, commit=commit)
    batch = [edit(swig, backend, "R1", set_fields=(("LCSC", "C999"),))]

    with caplog.at_level(logging.WARNING):
        after = backend.apply("apply LCSC assignments", batch)

    assert calls == ["enter", "leave", "refresh"]
    assert after.components[0].default.fields["LCSC"].text == "C999"
    assert (
        "Unable to redraw the PCB after apply LCSC assignments: native redraw failed"
        in caplog.text
    )


def _running_kicad(pcbnew: Any, board: Any) -> SimpleNamespace:
    """Return the pcbnew module, with this board open in its editor."""
    return SimpleNamespace(
        GetBoard=lambda: board,
        GetBuildVersion=pcbnew.GetBuildVersion,
        GetMajorMinorVersion=pcbnew.GetMajorMinorVersion,
        Refresh=pcbnew.Refresh,
    )


def _texts(dialect: Any, footprint: Any, name: str) -> list[str]:
    """Return the text of every field of a real footprint with this name."""
    if dialect.keeps_repeated_fields:
        return [str(f.GetText()) for f in footprint.GetFields() if f.GetName() == name]
    held = dict(footprint.GetProperties())
    return [str(held[name])] if name in held else []


@pytest.mark.native_kicad
@pytest.mark.skipif(
    os.environ.get("KICAD_JLCPCB_NATIVE_TESTS") != "1",
    reason="Native KiCad tests require an explicit opt-in",
)
def test_native_fields_save_reload_and_rollback(
    swig: SimpleNamespace, tmp_path: Any
) -> None:
    """Real KiCad footprints: a new hidden field and a flag persist; a failure leaves none.

    What this KiCad holds is taken from its dialect's facts.
    """
    pcbnew = pytest.importorskip("pcbnew")
    dialect = swig.dialects.select(pcbnew)
    filename = str(tmp_path / "ordinary.kicad_pcb")
    board = pcbnew.NewBoard(filename)
    first = pcbnew.FOOTPRINT(board)
    first.SetReference("R1")
    set_native_field(first, "JLCPCB Rotation", "90")
    board.Add(first)
    second = pcbnew.FOOTPRINT(board)
    second.SetReference("R2")
    set_native_field(second, "LCSC", "C200")
    board.Add(second)

    class FailingFootprint:
        """The real R2, whose field write changes it and then fails once."""

        def __init__(self, footprint: Any) -> None:
            self._footprint = footprint
            self.failed = False

        def __getattr__(self, name: str) -> Any:
            return getattr(self._footprint, name)

        def _fail_once(self) -> None:
            if not self.failed:
                self.failed = True
                raise RuntimeError("native setter changed the field then failed")

        def SetField(self, name: str, text: str) -> None:
            self._footprint.SetField(name, text)
            self._fail_once()

        def SetProperties(self, properties: dict[str, str]) -> None:
            self._footprint.SetProperties(properties)
            self._fail_once()

    class Board:
        """The real board, with R2 failing its first write."""

        def __init__(self) -> None:
            self.failing = FailingFootprint(second)

        def __getattr__(self, name: str) -> Any:
            return getattr(board, name)

        def GetFootprints(self) -> list[Any]:
            return [
                self.failing if footprint.GetReference() == "R2" else footprint
                for footprint in board.GetFootprints()
            ]

    backend = swig.SwigBackend(_running_kicad(pcbnew, Board()))

    with pytest.raises(swig.api.EditRejected, match="native setter changed"):
        backend.apply(
            "assign",
            [
                edit(
                    swig,
                    backend,
                    "R1",
                    set_fields=(("LCSC", "C999"),),
                    field_visibility=(("LCSC", False),),
                ),
                edit(swig, backend, "R2", set_fields=(("LCSC", "C999"),)),
            ],
        )
    assert _texts(dialect, first, "LCSC") == []
    assert _texts(dialect, second, "LCSC") == ["C200"]
    backend.apply(
        "assign",
        [
            edit(
                swig,
                backend,
                "R1",
                set_fields=(("LCSC", "C123"),),
                field_visibility=(("LCSC", False),),
                flags=(("exclude_from_bom", True),),
            )
        ],
    )
    assert pcbnew.SaveBoard(filename, board)
    loaded = pcbnew.LoadBoard(filename).FindFootprintByReference("R1")
    assert _texts(dialect, loaded, "LCSC") == ["C123"]
    assert _texts(dialect, loaded, "JLCPCB Rotation") == ["90"]
    assert loaded.IsExcludedFromBOM() is True
    if dialect.shows_fields:
        assert native_field(loaded, "LCSC").IsVisible() is False


@pytest.mark.native_kicad
@pytest.mark.skipif(
    os.environ.get("KICAD_JLCPCB_NATIVE_TESTS") != "1",
    reason="Native KiCad tests require an explicit opt-in",
)
def test_native_repeated_fields_are_all_written_and_kicad_keeps_one(
    swig: SimpleNamespace, tmp_path: Any
) -> None:
    """Both LCSC fields take the number; reloaded, KiCad keeps one, with it.

    A KiCad that keeps one text per name holds the later field only.
    """
    pcbnew = pytest.importorskip("pcbnew")
    dialect = swig.dialects.select(pcbnew)
    filename = str(tmp_path / "repeated.kicad_pcb")
    board = pcbnew.NewBoard(filename)
    footprint = pcbnew.FOOTPRINT(board)
    footprint.SetReference("R1")
    set_native_field(footprint, "LCSC", "C100")
    # What KiCad's footprint dialog does when a second field takes a used name.
    if dialect.keeps_repeated_fields:
        add_native_field(pcbnew, footprint, "LCSC", "C101")
    else:
        set_native_field(footprint, "LCSC", "C101")
    board.Add(footprint)
    kept = dialect.keeps_repeated_fields
    backend = swig.SwigBackend(_running_kicad(pcbnew, board))
    (component,) = backend.snapshot().components
    assert component.default.duplicate_names == (("LCSC",) if kept else ())

    backend.apply(
        "assign",
        [swig.api.default_edit(component, set_fields=(("LCSC", "C999"),))],
    )

    assert _texts(dialect, footprint, "LCSC") == ["C999"] * (2 if kept else 1)
    assert pcbnew.SaveBoard(filename, board)
    loaded = pcbnew.LoadBoard(filename).FindFootprintByReference("R1")
    assert _texts(dialect, loaded, "LCSC") == ["C999"]
