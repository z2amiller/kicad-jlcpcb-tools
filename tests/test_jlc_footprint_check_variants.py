"""The facade on a board with KiCad design variants: what the check reads (spec 18.2, 18.3).

A variant board has no ordinary store.  The check reads each footprint's part
number from upstream's variant session, the output variant's, matched to the
footprint by its UUID or its reference, and hands the other variants' numbers on
for prefetching; pads still come from the footprint.  The board-wide confirmations
say how many of the codes they would fetch are the other variants'.
"""

from types import SimpleNamespace

import pytest

from .jlc_footprint_wx_support import load_facade
from .test_jlcfootprint_kicad_adapter import FakeFootprint, FakePad
from .variant_native_support import Snapshot, State

_PACKAGE = "jlc_footprint_check_variants_tests"


@pytest.fixture
def facade():
    """Load the real facade with a fake wx."""
    with load_facade(_PACKAGE) as loaded:
        yield loaded.module, loaded.wx


def _footprint(reference, component_id, lcsc_field="C1"):
    """Return a two-pad footprint double carrying a KiCad UUID and an LCSC field."""
    footprint = FakeFootprint(
        reference,
        [FakePad("1", -1, 0), FakePad("2", 1, 0)],
        fields={"LCSC": lcsc_field},
    )
    footprint.m_Uuid = SimpleNamespace(AsString=lambda: component_id)
    return footprint


def _snapshot(numbers, *, names=("", "A", "B"), **changes):
    """Return a real all-variant snapshot: {(component, reference): {variant: lcsc}}."""
    return Snapshot(
        tuple(
            State(component, reference, name, lcsc=lcscs[name], **changes)
            for (component, reference), lcscs in numbers.items()
            for name in names
        ),
        names,
    )


NUMBERS = {
    ("id-r1", "R1"): {"": "C100", "A": "C200", "B": "C300"},
    ("id-r2", "R2"): {"": "C400", "A": "C400", "B": ""},
}


def _window(tmp_path, snapshot, output="A", footprints=None):
    """Return a variant window double: a library, a variant store and the session."""
    board = SimpleNamespace(
        GetFootprints=lambda: footprints
        or [_footprint("R1", "id-r1"), _footprint("R2", "id-r2")]
    )
    pcbnew = SimpleNamespace(
        GetBoard=lambda: board, ToMM=lambda v: v, PAD_ATTRIB_NPTH=3, PAD_SHAPE_CUSTOM=6
    )
    session = SimpleNamespace(snapshot=snapshot, output_variant=output)
    window = SimpleNamespace(
        library=SimpleNamespace(datadir=str(tmp_path)),
        store=SimpleNamespace(dbfile=str(tmp_path / "project.db")),
        settings={},
        _variant_controller=SimpleNamespace(session=session),
    )
    return window, pcbnew, session


def _numbers(check):
    """Return the check's read as {reference: lcsc}."""
    return {part.reference: part.lcsc for part in check.read_board()}


def test_a_variant_board_judges_the_output_variant_s_part_numbers(facade, tmp_path):
    """The read follows the output variant, never the footprint's own field (spec 18.2)."""
    module, _wx = facade
    window, pcbnew, session = _window(tmp_path, _snapshot(NUMBERS))
    check = module.create_footprint_check(window, pcbnew)
    assert _numbers(check) == {"R1": "C200", "R2": "C400"}
    session.output_variant = "B"
    assert _numbers(check) == {"R1": "C300", "R2": ""}
    session.output_variant = ""
    assert _numbers(check) == {"R1": "C100", "R2": "C400"}
    assert [len(part.pads) for part in check.read_board()] == [2, 2]


def test_the_read_follows_the_session_s_current_snapshot(facade, tmp_path):
    """An edit replaces the session's snapshot; the next read sees the new number."""
    module, _wx = facade
    window, pcbnew, session = _window(tmp_path, _snapshot(NUMBERS))
    check = module.create_footprint_check(window, pcbnew)
    assert _numbers(check)["R1"] == "C200"
    numbers = {**NUMBERS, ("id-r1", "R1"): {"": "C100", "A": "C777", "B": "C300"}}
    session.snapshot = _snapshot(numbers)
    assert _numbers(check)["R1"] == "C777"


def test_a_footprint_is_matched_by_uuid_then_by_reference(facade, tmp_path):
    """The UUID wins over a reference the snapshot gives another component."""
    module, _wx = facade
    footprints = [
        _footprint("R1", "id-r2"),  # R1's UUID is the snapshot's R2
        _footprint("R2", "no-such-id"),  # unknown UUID: matched by reference
        _footprint("R9", "unknown"),  # in neither: no part number
    ]
    window, pcbnew, _session = _window(tmp_path, _snapshot(NUMBERS), "", footprints)
    check = module.create_footprint_check(window, pcbnew)
    assert _numbers(check) == {"R1": "C400", "R2": "C400", "R9": ""}


def test_a_part_the_output_variant_does_not_place_keeps_its_number(facade, tmp_path):
    """DNP in the output variant: the CPL drops it, the check still judges it (spec 18.2)."""
    module, _wx = facade
    window, pcbnew, _session = _window(tmp_path, _snapshot(NUMBERS, pop=False))
    check = module.create_footprint_check(window, pcbnew)
    assert _numbers(check) == {"R1": "C200", "R2": "C400"}


def test_a_removed_output_variant_judges_default(facade, tmp_path):
    """A remembered output the board no longer has falls back to Default, like upstream."""
    module, _wx = facade
    window, pcbnew, _session = _window(tmp_path, _snapshot(NUMBERS), "Gone")
    check = module.create_footprint_check(window, pcbnew)
    assert _numbers(check) == {"R1": "C100", "R2": "C400"}
    assert dict(check.other_lcscs()) == {"R1": {"C200", "C300"}}


def test_the_other_variants_numbers_are_handed_on_for_prefetching(facade, tmp_path):
    """Every other variant's number that differs from the judged one, and no blank."""
    module, _wx = facade
    window, pcbnew, session = _window(tmp_path, _snapshot(NUMBERS))
    check = module.create_footprint_check(window, pcbnew)
    assert dict(check.other_lcscs()) == {"R1": {"C100", "C300"}}
    session.output_variant = "B"
    assert dict(check.other_lcscs()) == {"R1": {"C100", "C200"}, "R2": {"C400"}}


def test_an_ordinary_board_has_no_prefetch_source(facade, tmp_path):
    """Without a variant controller the check reads the store and prefetches nothing."""
    module, _wx = facade
    window, pcbnew, _session = _window(tmp_path, _snapshot(NUMBERS))
    del window._variant_controller
    window.store.get_part = lambda reference: {"lcsc": f"S-{reference}"}
    check = module.create_footprint_check(window, pcbnew)
    assert check.other_lcscs is None
    assert _numbers(check) == {"R1": "S-R1", "R2": "S-R2"}


@pytest.mark.parametrize(
    "action,question",
    [
        ("refresh_board_data", "2 part(s) and 2 more for other variants, about"),
        (
            "clear_cache",
            "fetched again: 2 part(s) and 2 more for other variants, about",
        ),
    ],
)
def test_the_board_wide_confirmations_count_the_other_variants_codes(
    facade, tmp_path, action, question
):
    """Refresh and Clear cache say how many codes they fetch for other variants."""
    module, wx = facade
    window, pcbnew, _session = _window(tmp_path, _snapshot(NUMBERS))
    window.jlc_footprint_check = module.create_footprint_check(window, pcbnew)
    asked = []

    class Dialog:
        """Record the question and answer No."""

        def __init__(self, _parent, text, _title, _style):
            asked.append(text)

        def ShowModal(self):
            """Answer No."""
            return wx.ID_NO

        def Destroy(self):
            """Accept the destroy."""

    wx.MessageDialog = Dialog
    assert getattr(module, action)(window) == 0
    assert question in asked[0]


def test_a_variant_board_whose_output_orders_nothing_still_confirms(facade, tmp_path):
    """Refresh asks when only the other variants carry part numbers."""
    module, wx = facade
    numbers = {("id-r1", "R1"): {"": "", "A": "", "B": "C300"}}
    footprints = [_footprint("R1", "id-r1")]
    window, pcbnew, _session = _window(tmp_path, _snapshot(numbers), "A", footprints)
    window.jlc_footprint_check = module.create_footprint_check(window, pcbnew)
    asked = []
    wx.MessageDialog = lambda _parent, text, _title, _style: SimpleNamespace(
        ShowModal=lambda: asked.append(text) or wx.ID_NO, Destroy=lambda: None
    )
    assert module.refresh_board_data(window) == 0
    assert "0 part(s) and 1 more for other variants" in asked[0]
