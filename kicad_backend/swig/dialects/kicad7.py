"""KiCad 7: Reference and Value stand apart, and the other fields are a property map.

The map holds one text per name, sorted by name, and is never drawn;
Reference and Value are text items that can be shown or hidden. A
footprint has no DNP flag.
"""

from typing import Any

from ... import EditRejected
from ...records import ComponentEdit, FieldRecord
from .base import Dialect


class KiCad7(Dialect):
    """KiCad 7's footprints."""

    major = 7
    builtin_fields = ("Reference", "Value")
    holds_dnp = False
    keeps_repeated_fields = False
    shows_fields = False
    sorts_fields = True

    def fields(self, footprint: Any) -> list[FieldRecord]:
        """Read Reference and Value, then the property map, which is never shown."""
        return [
            FieldRecord(
                "Reference",
                str(footprint.GetReference()),
                bool(footprint.Reference().IsVisible()),
            ),
            FieldRecord(
                "Value", str(footprint.GetValue()), bool(footprint.Value().IsVisible())
            ),
            *(
                FieldRecord(str(name), str(text), False)
                for name, text in footprint.GetProperties().items()
            ),
        ]

    def is_dnp(self, footprint: Any) -> bool:
        """Report every footprint fitted: KiCad 7 has no DNP flag."""
        return False

    def check_writable(self, edit: ComponentEdit) -> None:
        """Refuse Reference and Value, which stand apart, and showing a property."""
        named = {name for name, _text in edit.set_fields}
        named.update(name for name, _shown in edit.field_visibility)
        if named & set(self.builtin_fields):
            raise EditRejected("Reference and Value are not edited here on KiCad 7")
        if any(shown for _name, shown in edit.field_visibility):
            raise EditRejected("KiCad 7 never shows a footprint's other fields")

    def write_fields(self, footprint: Any, edit: ComponentEdit) -> None:
        """Write the property map whole; a hidden property is all it can hold."""
        properties = dict(footprint.GetProperties())
        properties.update(edit.set_fields)
        footprint.SetProperties(properties)

    def restore_fields(
        self, footprint: Any, held: dict[str, list[tuple[str, bool]]]
    ) -> None:
        """Put the property map back whole; a property the batch added goes."""
        footprint.SetProperties(
            {
                name: occurrences[0][0]
                for name, occurrences in held.items()
                if name not in self.builtin_fields
            }
        )
