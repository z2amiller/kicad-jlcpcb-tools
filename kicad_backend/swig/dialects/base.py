"""What the KiCad majors with field objects share: KiCad 8 and later.

A major's module states its own facts and overrides only what its version
does differently. The operations take live SWIG objects, which stay inside
``kicad_backend.swig``.
"""

from contextlib import suppress
from typing import Any

from ...records import ComponentEdit, FieldRecord


class Dialect:
    """One KiCad major's facts, and the operations that differ between majors.

    The facts say what a footprint holds in this major:

    - ``major``: the KiCad major.
    - ``builtin_fields``: the fields every footprint has, in KiCad's order.
    - ``holds_dnp``: whether a footprint has a DNP flag.
    - ``keeps_repeated_fields``: whether a footprint keeps two fields with
      one name.
    - ``shows_fields``: whether a field other than Reference and Value can
      be shown; where one can, KiCad shows a field it adds.
    - ``sorts_fields``: whether a footprint holds the fields after its
      built-in ones sorted by name, rather than in the order they were added.
    """

    major: int
    builtin_fields: tuple[str, ...]
    holds_dnp: bool
    keeps_repeated_fields: bool
    shows_fields: bool
    sorts_fields: bool

    def fields(self, footprint: Any) -> list[FieldRecord]:
        """Read every field of a footprint in KiCad's order, with its visibility.

        KiCad's footprint dialog accepts a second field with a name the
        footprint already has, and the footprint keeps both until the board
        is reopened; the state built from them resolves the name as
        reopening does.
        """
        return [
            FieldRecord(
                str(field.GetName()), str(field.GetText()), bool(field.IsVisible())
            )
            for field in footprint.GetFields()
        ]

    def is_dnp(self, footprint: Any) -> bool:
        """Read the footprint's DNP flag."""
        return bool(footprint.IsDNP())

    def check_writable(self, edit: ComponentEdit) -> None:
        """Refuse what this KiCad cannot store, before anything is written."""

    def write_fields(self, footprint: Any, edit: ComponentEdit) -> None:
        """Set the fields an edit names, and show or hide them.

        A name the footprint repeats is written into every field of that
        name, and none is removed.
        """
        for name, text in edit.set_fields:
            named = [
                field for field in footprint.GetFields() if field.GetName() == name
            ]
            if len(named) > 1:
                for field in named:
                    field.SetText(text)
            else:
                footprint.SetField(name, text)
        shown = dict(edit.field_visibility)
        for field in footprint.GetFields():
            if str(field.GetName()) in shown:
                field.SetVisible(shown[str(field.GetName())])

    def restore_fields(
        self, footprint: Any, held: dict[str, list[tuple[str, bool]]]
    ) -> None:
        """Put back every field as ``held`` lists it, (text, shown) by name in order.

        Each field is put back independently of failures in the others; the
        caller reads the footprint back to see whether restoring worked.
        """
        for name, occurrences in held.items():
            # A setter can change the board and then raise, so the read-back,
            # not the setter, decides whether restoring worked.
            if len(occurrences) == 1:
                with suppress(Exception):
                    footprint.SetField(name, occurrences[0][0])
        # Enumerate only once the text is back: a field the batch added goes.
        # Fields of a repeated name are put back one by one, in order.
        seen: dict[str, int] = {}
        for field in list(footprint.GetFields()):
            with suppress(Exception):
                name = str(field.GetName())
                index = seen[name] = seen.get(name, -1) + 1
                occurrences = held.get(name, [])
                if index >= len(occurrences):
                    footprint.Remove(field)
                else:
                    text, visible = occurrences[index]
                    if len(occurrences) > 1:
                        field.SetText(text)
                    field.SetVisible(visible)
