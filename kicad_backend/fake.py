"""A board held in memory, read and edited through the same interface as KiCad's.

Tests build components with ``component`` and ``pad`` and change
``FakeBackend.components`` to stand for edits made in the PCB editor.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from typing import Optional, Union
from uuid import NAMESPACE_URL, uuid5

from . import (
    EDIT_RECOVERY_FAILED,
    BoardChanged,
    EditRecoveryFailed,
    EditRejected,
    StaleTarget,
)
from .edits import check_batch, edited_state
from .records import (
    BoardInfo,
    BoardSnapshot,
    ComponentEdit,
    ComponentRecord,
    ComponentState,
    FieldRecord,
    PadRecord,
    VariantDefinition,
    component_state,
    footprint_keys,
)


def board_info(
    file_name: str = "/project/board.kicad_pcb", *, token: str = "fake-board"
) -> BoardInfo:
    """Describe a board saved as ``file_name``."""
    return BoardInfo(token, file_name)


def pad(
    kind: str = "smd",
    *,
    has_hole: Optional[bool] = None,  # noqa: UP045
) -> PadRecord:
    """Build a pad; plated and bare holes have holes unless ``has_hole`` says."""
    return PadRecord(kind, kind in {"pth", "npth"} if has_hole is None else has_hole)


def component(
    reference: str,
    *,
    uuid: Optional[str] = None,  # noqa: UP045
    value: str = "",
    footprint: str = "Lib:Footprint",
    fields: Union[Mapping[str, str], Iterable[tuple[str, str]], None] = None,
    hidden: Iterable[str] = (),
    dnp: bool = False,
    exclude_from_bom: bool = False,
    exclude_from_pos: bool = False,
    schematic_path: str = "",
    side: str = "top",
    pads: Iterable[PadRecord] = (),
) -> ComponentRecord:
    """Build a footprint with Default only, fields in KiCad's order.

    KiCad lists Reference, Value, then the hidden Datasheet and Description,
    then the footprint's own fields: here ``fields`` in order, visible unless
    named in ``hidden``. A name already listed, one of those four or one that
    comes again in (name, text) pairs, adds another field of that name after
    the others, as a KiCad footprint can hold. The UUID defaults to one
    derived from the reference, so two footprints sharing a reference need
    their own.
    """
    hidden_names = {"Datasheet", "Description", *hidden}
    texts = {"Reference": reference, "Value": value, "Datasheet": "", "Description": ""}
    given = set(texts)
    repeated = []
    pairs = fields.items() if isinstance(fields, Mapping) else fields or ()
    for name, text in pairs:
        if name in given:
            repeated.append((name, text))
        else:
            given.add(name)
            texts[name] = text
    state = component_state(
        (
            FieldRecord(name, text, name not in hidden_names)
            for name, text in (*texts.items(), *repeated)
        ),
        dnp=dnp,
        exclude_from_bom=exclude_from_bom,
        exclude_from_pos=exclude_from_pos,
    )
    return ComponentRecord(
        str(uuid5(NAMESPACE_URL, f"fake-footprint:{reference}"))
        if uuid is None
        else uuid,
        reference,
        footprint,
        schematic_path,
        side,
        tuple(pads),
        {"": state},
    )


class FakeBackend:
    """Serve a board from memory with the checks a real backend makes.

    Set ``board`` to None to close it, as the PCB editor can.
    ``fail_writes`` names footprints, by key, whose write changes them and
    then fails, as a KiCad setter can; ``fail_restores`` names those that
    cannot be put back afterwards. ``applied`` records the label of each
    batch that changed the board, as KiCad would give it an undo step; a
    batch that changes nothing is checked and not recorded.
    """

    def __init__(
        self,
        components: Iterable[ComponentRecord] = (),
        *,
        info: Optional[BoardInfo] = None,  # noqa: UP045
    ) -> None:
        self.components = tuple(components)
        self.board: Optional[BoardInfo] = info or board_info()  # noqa: UP045
        self.reads = 0
        self.fail_writes: set[str] = set()
        self.fail_restores: set[str] = set()
        self.applied: list[str] = []
        self.unreliable = False

    def info(self) -> BoardInfo:
        """Describe the board; raise BoardChanged once it is closed."""
        if self.board is None:
            raise BoardChanged("The PCB editor no longer has an open board")
        return self.board

    def snapshot(self) -> BoardSnapshot:
        """Read every footprint, as a real backend reads the live board.

        A closed board raises BoardChanged.
        """
        info = self.info()
        self.reads += 1
        return BoardSnapshot(
            info, (VariantDefinition("", "Default"),), tuple(self.components)
        )

    def _prepare(
        self, edits: Sequence[ComponentEdit]
    ) -> list[tuple[str, int, ComponentState]]:
        """Check a batch against the board.

        Return each target's key and place on the board, with its new state.
        """
        batch = check_batch(edits)
        if self.unreliable:
            raise EditRecoveryFailed(EDIT_RECOVERY_FAILED)
        self.info()
        keys = footprint_keys(component.uuid for component in self.components)
        places = {key: place for place, key in enumerate(keys)}
        prepared = []
        for edit in batch:
            place = places.get(edit.key)
            if place is None:
                raise StaleTarget(
                    "A footprint to edit is no longer on the board; nothing was written."
                )
            component = self.components[place]
            if component.default.revision != edit.based_on:
                raise StaleTarget(
                    f"Footprint {component.reference} changed since it was read; "
                    "nothing was written."
                )
            prepared.append((edit.key, place, edited_state(component.default, edit)))
        return prepared

    def edits_needed(self, edits: Sequence[ComponentEdit]) -> bool:
        """Check a batch without writing; return whether it changes the board."""
        return any(
            state.revision != self.components[place].default.revision
            for _key, place, state in self._prepare(edits)
        )

    def _put(self, place: int, component: ComponentRecord) -> None:
        """Replace the footprint at one place on the board."""
        components = list(self.components)
        components[place] = component
        self.components = tuple(components)

    def apply(self, label: str, edits: Sequence[ComponentEdit]) -> BoardSnapshot:
        """Write a batch footprint by footprint, undoing it all if one write fails."""
        prepared = self._prepare(edits)
        if all(
            state.revision == self.components[place].default.revision
            for _key, place, state in prepared
        ):
            return self.snapshot()
        written = []
        try:
            for key, place, state in prepared:
                component = self.components[place]
                if state.revision == component.default.revision:
                    continue
                written.append((key, place, component))
                self._put(place, replace(component, states={"": state}))
                if key in self.fail_writes:
                    raise RuntimeError(f"Writing {component.reference} failed")
        except RuntimeError as error:
            for key, place, component in reversed(written):
                if key not in self.fail_restores:
                    self._put(place, component)
            if any(key in self.fail_restores for key, _place, _c in written):
                self.unreliable = True
                raise EditRecoveryFailed(EDIT_RECOVERY_FAILED) from error
            raise EditRejected(str(error)) from error
        self.applied.append(label)
        return self.snapshot()
