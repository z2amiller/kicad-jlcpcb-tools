"""The check on a board with KiCad design variants: prefetching the other variants (spec 18.3).

The check judges one part per reference, the output variant's.  Every scan also
walks the other variants' part numbers on the same footprints and resolves or
queues them exactly like a judged part, after the judged ones and without making
them judged parts, so an output switch finds their verdicts stored.
"""

from dataclasses import replace

import pytest

from jlcfootprint.verdicts import PENDING
from scripts.kicad_library import footprints_available

from .jlcfootprint_support import alias, controller_setup

pytestmark = pytest.mark.skipif(
    not footprints_available(), reason="KiCad library footprints unavailable"
)


@pytest.fixture
def setup(tmp_path):
    """Return the controller setup with Q1's other variant ordering C9999 (C2132's twin)."""
    check, board, events, messages, client = controller_setup(tmp_path)
    client.records["C9999"] = alias("C9999")
    variants = {"Q1": {"C9999", "C2132", ""}}
    check.other_lcscs = lambda: variants
    return check, board, events, client, variants


def test_a_scan_queues_the_other_variants_parts_after_the_judged_ones(setup):
    """The judged codes reach the lookup queue first; the prefetched code follows."""
    check, _board, _events, _client, _variants = setup
    summary = check.scan_board()
    assert list(check.worker.lookups) == ["C2132", "C2286", "C9999"]
    assert (summary.scanned, summary.enqueued) == (3, 2)
    assert (summary.prefetched, summary.prefetch_enqueued) == (1, 1)
    assert "; prefetched 1 other-variant part(s), 1 enqueued" in str(summary)


def test_a_prefetched_part_is_stored_like_a_judged_one_but_never_judged(setup):
    """Its verdict row lands under its own LCSC and the judged parts' pad hash."""
    check, board, events, _client, _variants = setup
    check.scan_board()
    q1 = board["parts"][0]
    assert check.verdicts.get("C9999", q1.footprint_hash).status == PENDING
    assert check.pending_references() == ["D1", "Q1"]
    check.worker.run_pending()
    stored = check.verdicts.get("C9999", q1.footprint_hash)
    assert (stored.status, stored.rotation) == ("green", 180)
    assert sorted(check.parts) == ["D1", "Q1", "R1"]
    assert check.parts["Q1"].lcsc == "C2132"
    assert list(check.prefetched) == [("Q1", "C9999")]
    assert check.references_for("C9999") == []
    assert ("C9999", 1) in events
    assert check.decisions(reread=False)["Q1"].lcsc == "C2132"


def test_a_cached_prefetched_part_resolves_without_a_request(setup):
    """A code the cache already holds is resolved on the spot, like a judged one."""
    check, board, _events, client, _variants = setup
    check.cache.store(alias("C9999"), now=1)
    summary = check.scan_board()
    assert (summary.prefetched, summary.prefetch_enqueued) == (1, 0)
    assert "C9999" not in check.worker.pending_lookups()
    stored = check.verdicts.get("C9999", board["parts"][0].footprint_hash)
    assert (stored.status, stored.rotation) == ("green", 180)
    assert client.lookups == []


def test_a_reference_scan_prefetches_its_new_codes_and_forgets_its_old_ones(setup):
    """An edit to another variant rescans that reference: its new code is prefetched."""
    check, _board, _events, _client, variants = setup
    check.scan_board()
    variants["Q1"] = {"C2286"}
    summary = check.enqueue_references(["Q1"])
    assert list(check.prefetched) == [("Q1", "C2286")]
    assert summary.prefetched == 1
    assert check.generation == 1


def test_the_judged_part_s_own_code_and_empty_numbers_are_not_prefetched(setup):
    """A variant ordering what the output orders, or nothing, adds nothing to fetch."""
    check, _board, _events, _client, variants = setup
    variants["Q1"] = {"C2132", ""}
    summary = check.scan_board()
    assert (summary.prefetched, check.prefetched) == (0, {})
    assert "prefetched" not in str(summary)


def test_the_board_estimate_counts_the_board_and_times_both(setup):
    """The confirmation's part count is the board's; the time covers the prefetch too."""
    check, _board, _events, _client, _variants = setup
    parts, seconds = check.board_estimate()
    other_lcscs, check.other_lcscs = check.other_lcscs, None
    assert check.board_estimate()[0] == 2
    assert check.other_variant_codes() == 0
    alone = check.board_estimate()[1]
    check.other_lcscs = other_lcscs
    assert (parts, check.other_variant_codes()) == (2, 1)
    assert seconds > alone


def test_rechecking_resolves_the_prefetched_parts_too(setup):
    """Re-check board re-resolves every cached code, the other variants' included."""
    check, board, _events, _client, _variants = setup
    check.cache.store(alias("C9999"), now=1)
    assert check.recheck_board() == 1
    stored = check.verdicts.get("C9999", board["parts"][0].footprint_hash)
    assert stored.status == "green"
    # Re-resolved, never judged: Q1 is still the output variant's part.
    assert check.parts["Q1"].lcsc == "C2132"
    assert check.decisions(reread=False)["Q1"].lcsc == "C2132"


def test_refreshing_the_board_forgets_the_other_variants_codes_too(setup):
    """Refresh board data fetches every variant's parts again (spec 18.3)."""
    check, _board, _events, _client, _variants = setup
    check.cache.store(alias("C9999"), now=1)
    check.refresh_board()
    assert check.cache.status("C9999") is None
    assert "C9999" in check.worker.pending_lookups()


def test_a_board_without_variants_prefetches_nothing(tmp_path):
    """No variant source: the scan and its log line are exactly the ordinary board's."""
    check, _board, _events, _messages, _client = controller_setup(tmp_path)
    assert check.other_lcscs is None
    summary = check.scan_board()
    assert (summary.prefetched, check.prefetched) == (0, {})
    assert "prefetched" not in str(summary)
    assert check.other_variant_codes() == 0


def test_another_variant_s_part_on_the_same_pads_shares_the_verdict(setup):
    """Q2's other variant orders Q1's part on the same pads: an override reaches both (spec 18.2)."""
    check, board, _events, _client, variants = setup
    board["parts"].append(replace(board["parts"][0], reference="Q2", lcsc="C2286"))
    variants["Q2"] = {"C2132"}
    check.scan_board()
    assert check.references_sharing_verdict("Q1") == ["Q1", "Q2"]
    assert check.references_sharing_verdict("Q2") == ["Q2"]


def test_an_ordinary_board_is_not_read_for_other_variants_codes(tmp_path):
    """Refresh and Clear cache ask for the other variants' codes; an ordinary board has none."""
    check, _board, _events, _messages, _client = controller_setup(tmp_path)
    reads = []
    read_board = check.read_board
    check.read_board = lambda: reads.append(1) or read_board()
    assert check.other_variant_codes() == 0
    assert reads == []
