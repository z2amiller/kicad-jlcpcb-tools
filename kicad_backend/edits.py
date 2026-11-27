"""What an edit batch may ask for, and what a footprint holds once it is written.

Backends differ in how they write a footprint. The rules a batch must meet
and the state each edit leads to are decided here, once, so every backend
accepts the same batches and checks its writes against the same result.
"""

from collections.abc import Iterable, Sequence

from . import EditRejected
from .records import ComponentEdit, ComponentState, FieldRecord, component_state

FLAGS = ("exclude_from_bom", "exclude_from_pos")


def _names(pairs: Iterable[tuple[str, object]], kind: str) -> list[str]:
    """Return the names of name/value pairs, rejecting an empty or repeated one."""
    names = []
    for pair in pairs:
        if len(pair) != 2 or not isinstance(pair[0], str) or not pair[0]:
            raise EditRejected(f"Each {kind} needs a name and a value")
        if pair[0] in names:
            raise EditRejected(f'"{pair[0]}" appears twice in one edit')
        names.append(pair[0])
    return names


def check_batch(edits: Sequence[ComponentEdit]) -> tuple[ComponentEdit, ...]:
    """Reject a batch no backend may write, before the board is read.

    Each footprint takes at most one edit, so a batch cannot depend on the
    order its edits are written in.
    """
    batch = tuple(edits)
    targets = set()
    for edit in batch:
        if not isinstance(edit, ComponentEdit):
            raise EditRejected("A batch holds ComponentEdits only")
        if edit.key in targets:
            raise EditRejected("Combine changes for each footprint into one edit")
        targets.add(edit.key)
        _names(edit.set_fields, "field")
        if any(not isinstance(text, str) for _name, text in edit.set_fields):
            raise EditRejected("Field text must be text")
        _names(edit.field_visibility, "visibility")
        if any(type(shown) is not bool for _name, shown in edit.field_visibility):
            raise EditRejected("Field visibility must be True or False")
        for name in _names(edit.flags, "flag"):
            if name not in FLAGS:
                raise EditRejected(f"Unknown footprint flag: {name}")
        if any(type(value) is not bool for _name, value in edit.flags):
            raise EditRejected("Footprint flags must be True or False")
    return batch


def copies(state: ComponentState) -> dict[str, list[tuple[str, bool]]]:
    """Return (text, visible) of every field of each name, in the footprint's order.

    A name the footprint repeats lists each field of that name in turn.
    """
    held: dict[str, list[tuple[str, bool]]] = {name: [] for name in state.fields}
    for record in (*state.replaced_fields, *state.fields.values()):
        held[record.name].append((record.text, record.visible))
    return held


def edited_state(
    state: ComponentState, edit: ComponentEdit, *, new_fields_shown: bool = True
) -> ComponentState:
    """Return the state a footprint holds once ``edit`` is written to it.

    A field that is set keeps its place and visibility; a new field goes
    last and is shown, as KiCad adds one, unless the edit hides it. A name
    the footprint repeats is set, shown or hidden in every field of that
    name.
    """
    held = copies(state)
    for name, text in edit.set_fields:
        held[name] = [
            (text, shown) for _old, shown in held.get(name, [("", new_fields_shown)])
        ]
    for name, shown in edit.field_visibility:
        if name not in held:
            raise EditRejected(f'There is no "{name}" field to show or hide')
        held[name] = [(text, shown) for text, _old in held[name]]
    # The first field of each name in order, then each later one where it
    # replaced an earlier one: the order the footprint holds them in, as far
    # as the state records it, which resolves and digests the same way.
    records = [FieldRecord(name, *fields[0]) for name, fields in held.items()]
    later = dict.fromkeys(held, 1)
    for replaced in state.replaced_fields:
        if replaced.name in held:
            records.append(
                FieldRecord(replaced.name, *held[replaced.name][later[replaced.name]])
            )
            later[replaced.name] += 1
    flags = {
        "dnp": state.dnp,
        "exclude_from_bom": state.exclude_from_bom,
        "exclude_from_pos": state.exclude_from_pos,
        **dict(edit.flags),
    }
    return component_state(records, **flags)


def same_state(left: ComponentState, right: ComponentState) -> bool:
    """Return whether two states hold the same fields and flags, names in any order.

    Every field of a repeated name is compared, in turn.
    """
    return (
        copies(left),
        (left.dnp, left.exclude_from_bom, left.exclude_from_pos),
    ) == (
        copies(right),
        (right.dnp, right.exclude_from_bom, right.exclude_from_pos),
    )
