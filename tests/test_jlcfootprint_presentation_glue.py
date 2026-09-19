"""Tests for the presentation glue that reads a footprint check: part_detail, cell_help, glyph_state."""

import pytest

from jlcfootprint.controller import FetchState
from jlcfootprint.presentation import cell_help, glyph_state, part_detail
from jlcfootprint.verdicts import PENDING
from jlcfootprint.worker import FOOTPRINT

from .jlcfootprint_support import (
    controller_setup,
    footprints_available,
    recorded,
    sot23,
)

pytestmark = pytest.mark.skipif(
    not footprints_available(), reason="KiCad library footprints unavailable"
)


@pytest.fixture
def setup(tmp_path):
    """Return a controller over a fresh cache and verdict store with a fake client."""
    return controller_setup(tmp_path)


def test_the_queue_state_of_one_part_while_it_is_scanned_fetched_and_resolved(setup):
    """A part reads queued, then fetching its own document, then idle once resolved."""
    check, board, _events, _messages, _client = setup
    assert check.fetch_state("C2132") == FetchState()
    assert glyph_state(check, "Q1") == ""
    check.scan_board()
    queued = check.fetch_state("C2132")
    assert (queued.state, queued.ahead, queued.queued_parts) == ("queued", 2, 2)
    assert glyph_state(check, "Q1") == "pending"
    assert "Queued for EasyEDA, 2 request(s) ahead" in cell_help(check, "Q1")
    # The lookup in flight is this part's, so the cell says what is being fetched.
    check.worker.in_flight = ("lookup", ["C2132", "C2286"])
    assert check.fetch_state("C2132").state == "fetching"
    assert cell_help(check, "Q1") == "Looking up the part at EasyEDA…"
    check.worker.in_flight = None
    check.worker.run_pending()
    assert check.fetch_state("C2132") == FetchState(queued_parts=0)
    assert glyph_state(check, "Q1") == "green"
    assert cell_help(check, "Q1") == (
        "Fits; rotation 180° derived from pad geometry (high). "
        f"JLC {recorded('C2132').package_name} on Package_TO_SOT_SMD:SOT-23."
    )
    assert glyph_state(check, "D1") == "yellow"
    assert "pin-1 marker will sit on the other terminal" in cell_help(check, "D1")
    assert glyph_state(check, "R1") == ""
    assert (
        cell_help(check, "R1")
        == "No LCSC number assigned. The CPL emits the raw angle."
    )
    assert cell_help(check, "nope") == ""


def test_a_document_in_flight_and_a_backoff_and_the_breaker(setup):
    """Fetching a document this part waits on, a backoff and the breaker each have their own state."""
    check, _board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    # The state after a lookup has landed: the part waits on its footprint document.
    puuid = recorded("C2132").puuid
    check.waiting[(FOOTPRINT, puuid)] = {"C2132"}
    check.worker.in_flight = (FOOTPRINT, puuid)
    fetching = check.fetch_state("C2132")
    assert (fetching.state, fetching.kind) == ("fetching", FOOTPRINT)
    assert cell_help(check, "Q1") == "Fetching the footprint…"
    check.worker.backoff_until = check.worker.clock() + 45.0
    paused = check.fetch_state("C2132")
    assert (paused.state, round(paused.seconds)) == ("paused", 45)
    assert glyph_state(check, "Q1") == "paused"
    assert cell_help(check, "Q1") == "EasyEDA asked us to wait 45 s; 1 part(s) queued."
    check.worker.backoff_until = None
    check.worker.tripped = True
    assert check.fetch_state("C2132").state == "tripped"
    assert glyph_state(check, "Q1") == "paused"
    assert cell_help(check, "Q1") == (
        "Paused after three failed requests; reopen the plugin to retry."
    )


def test_the_detail_of_a_resolved_part_carries_both_pad_sets_and_a_fresh_placement(
    setup,
):
    """Spec 16.4: the dialog's inputs are the resolver's own, re-run, not the stored row."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    detail = part_detail(check, "Q1")
    assert (detail.reference, detail.lcsc) == ("Q1", "C2132")
    assert detail.kicad_footprint == "Package_TO_SOT_SMD:SOT-23"
    assert len(detail.kicad_pads) == 3 and len(detail.jlc_pads) == 3
    assert detail.package_name == recorded("C2132").package_name
    assert detail.puuid == recorded("C2132").puuid
    assert detail.source == "live" and detail.fetched_at > 0
    assert detail.pin_functions == {"1": "B", "2": "E", "3": "C"}
    assert detail.kicad_pitch == pytest.approx(
        1.9, abs=0.05
    )  # the SOT-23's nearest pads
    assert detail.jlc_pitch is not None
    assert detail.kicad_pin1 is not None and detail.jlc_pin1 is not None
    # The verdict is re-resolved here and carries the placement the canvas needs.
    assert detail.verdict is not None and detail.placement is not None
    assert (detail.verdict.status, detail.verdict.rotation) == ("green", 180)
    assert detail.stored is not None and detail.stored.rotation == 180
    assert detail.emitted_rotation == 180
    assert detail.fetch.state == "idle"
    assert detail.decision is not None and detail.decision.status == "green"
    assert part_detail(check, "nope") is None


def test_the_detail_of_a_part_with_no_data_still_describes_the_footprint(setup):
    """A pending part, and a part with no LCSC, draw their KiCad pads and nothing else."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    pending = part_detail(check, "Q1")
    assert pending.jlc_pads == [] and pending.verdict is None
    assert pending.package_name == "" and pending.source == ""
    assert len(pending.kicad_pads) == 3
    assert pending.fetch.state == "queued"
    assert pending.stored is not None and pending.stored.status == PENDING
    bare = part_detail(check, "R1")
    assert (bare.lcsc, bare.jlc_pads, bare.stored) == ("", [], None)
    assert bare.kicad_pads == []
    # A row with a puuid but no footprint fetched yet ("ok" but no pads) is not
    # resolved either: the `detail` guard keeps it out of the resolver, unlike the
    # "pending" case above where the cache holds no row for the part at all.
    check.cache.store_lookup("C2132", "sym-C2132", "puuid-not-fetched", 1)
    calls: list = []
    original_resolve = check.resolve_part
    check.resolve_part = lambda *args, **kwargs: (
        calls.append(1) or original_resolve(*args, **kwargs)
    )
    incomplete = part_detail(check, "Q1")
    assert incomplete.verdict is None
    assert calls == []


def test_the_detail_rereads_the_board_when_asked(setup):
    """The dialog opens on what the board says now, not on the last scan."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    board["parts"] = [*board["parts"], sot23("Q2")]
    assert part_detail(check, "Q2") is None
    assert part_detail(check, "Q2", reread=True) is not None


def test_an_override_is_written_kept_and_cleared(setup):
    """Spec 16.5: the override lives on the verdict row, survives a re-resolve, and clears."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    part = board["parts"][0]
    stored = check.set_override("Q1", 270, "JLC's preview needed it")
    assert (stored.override_rotation, stored.override_note) == (
        270,
        "JLC's preview needed it",
    )
    assert check.display_text("Q1") == "270° set"
    assert check.decision(part).source == "override"
    assert glyph_state(check, "Q1") == "override"
    assert "Override 270° set by you." in cell_help(check, "Q1")
    # A re-resolve and a re-scan keep it (save and mark_pending both do).
    check.verdicts.mark_pending("C2132", part.footprint_hash, part.footprint_name, 9)
    assert check.verdicts.get("C2132", part.footprint_hash).override_rotation == 270
    check.scan_board()
    check.worker.run_pending()
    assert check.verdicts.get("C2132", part.footprint_hash).override_rotation == 270
    cleared = check.set_override("Q1", None)
    assert (cleared.override_rotation, cleared.override_note) == (None, None)
    assert check.display_text("Q1") == "180°"
    assert check.set_override("R1", 90) is None  # no LCSC, no row
    assert check.set_override("nope", 90) is None


def test_refetching_forgets_the_cached_rows_and_queues_the_parts_again(setup):
    """Spec 16.5: the cache row goes, the verdict is pending again, the override stays."""
    check, board, _events, _messages, _client = setup
    check.scan_board()
    check.worker.run_pending()
    part = board["parts"][0]
    check.set_override("Q1", 270, "keep me")
    assert check.cache.status("C2132") == "ok"
    summary = check.refetch(["Q1"])
    assert check.cache.status("C2132") is None
    assert summary.enqueued == 1
    stored = check.verdicts.get("C2132", part.footprint_hash)
    assert (stored.status, stored.override_rotation) == (PENDING, 270)
    assert glyph_state(check, "Q1") == "pending"
    assert check.package_name("C2132") == ""
    # The queued request answers and the part resolves again, override intact.
    check.worker.run_pending()
    assert check.cache.status("C2132") == "ok"
    assert check.verdicts.get("C2132", part.footprint_hash).override_rotation == 270
    assert check.package_name("C2132") == recorded("C2132").package_name
    assert check.refetch(["nope"]).scanned == 0
