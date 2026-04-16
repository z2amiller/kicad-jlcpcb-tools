"""Tests for assembly enrichment helper logic."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from assembly_enrichment import (
    STATUS_DONE,
    STATUS_NO_DATA,
    STATUS_PENDING,
    STATUS_QUEUED,
    get_enrichment_result_status,
    get_enrichment_status_label,
    run_assembly_enrichment_worker,
    start_assembly_enrichment,
)


def test_get_enrichment_status_label_handles_unassigned_pending_done_and_queued():
    """Row status helper reflects assignment, pending work, and persisted metadata."""
    assert get_enrichment_status_label({"lcsc": ""}, set()) == ""
    assert get_enrichment_status_label({"lcsc": "C1"}, {"C1"}) == STATUS_PENDING
    assert (
        get_enrichment_status_label(
            {"lcsc": "C1", "assembly_process": "SMT", "component_product_type": None},
            set(),
        )
        == STATUS_DONE
    )
    assert (
        get_enrichment_status_label(
            {"lcsc": "C1", "assembly_process": "", "component_product_type": 2},
            set(),
        )
        == STATUS_DONE
    )
    assert (
        get_enrichment_status_label(
            {"lcsc": "C1", "assembly_process": "", "component_product_type": None},
            set(),
        )
        == STATUS_QUEUED
    )


def test_get_enrichment_result_status_distinguishes_done_from_no_data():
    """Result status helper reports successful metadata fetches separately from empty results."""
    assert get_enrichment_result_status({"assembly_process": "SMT"}) == STATUS_DONE
    assert get_enrichment_result_status({"component_product_type": 1}) == STATUS_DONE
    assert (
        get_enrichment_result_status(
            {"assembly_process": "", "component_product_type": None}
        )
        == STATUS_NO_DATA
    )


def test_run_assembly_enrichment_worker_posts_progress_for_each_target():
    """Worker fetches one LCSC at a time and emits one progress callback each."""
    targets = {"C1": ["R1"], "C2": ["R2", "R3"]}
    events = []

    def fake_fetch(codes):
        code = list(codes)[0]
        return {
            code: {
                "assembly_process": "SMT",
                "component_product_type": 2,
            }
        }

    run_assembly_enrichment_worker(
        targets,
        post_progress=lambda lcsc, refs, metadata: events.append((lcsc, refs, metadata)),
        fetch_metadata=fake_fetch,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )

    assert events == [
        (
            "C1",
            ["R1"],
            {"assembly_process": "SMT", "component_product_type": 2},
        ),
        (
            "C2",
            ["R2", "R3"],
            {"assembly_process": "SMT", "component_product_type": 2},
        ),
    ]


def test_run_assembly_enrichment_worker_logs_and_posts_empty_metadata_on_error():
    """Worker should swallow fetch failures and report empty metadata payloads."""
    log_calls = []
    events = []

    class _Logger:
        def exception(self, msg, *args):
            log_calls.append((msg, args))

    run_assembly_enrichment_worker(
        {"C1": ["R1"]},
        post_progress=lambda lcsc, refs, metadata: events.append((lcsc, refs, metadata)),
        fetch_metadata=lambda _codes: (_ for _ in ()).throw(RuntimeError("boom")),
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
        logger=_Logger(),
    )

    assert log_calls and log_calls[0][1] == ("C1",)
    assert events == [("C1", ["R1"], {})]


def test_start_assembly_enrichment_filters_pending_and_marks_rows_pending():
    """Starter filters already-pending LCSCs, marks rows, and starts worker thread."""
    pending = {"C2"}
    status_updates = []
    started = []

    class _Thread:
        def __init__(self, *, target, args, kwargs, daemon):
            self.target = target
            self.args = args
            self.kwargs = kwargs
            self.daemon = daemon

        def start(self):
            started.append((self.target, self.args, self.kwargs, self.daemon))

    did_start = start_assembly_enrichment(
        references=["R1", "R2", "R3"],
        get_targets=lambda _refs: {"C1": ["R1"], "C2": ["R2"], "C3": ["R3"]},
        pending_lcsc_codes=pending,
        set_pending_status=lambda ref, status: status_updates.append((ref, status)),
        post_progress=lambda *_args: None,
        thread_factory=_Thread,
    )

    assert did_start is True
    assert pending == {"C1", "C2", "C3"}
    assert status_updates == [("R1", STATUS_PENDING), ("R3", STATUS_PENDING)]
    assert len(started) == 1
    assert started[0][1][0] == {"C1": ["R1"], "C3": ["R3"]}


def test_start_assembly_enrichment_returns_false_when_no_work():
    """Starter returns False and skips thread creation when nothing is queued."""
    did_start = start_assembly_enrichment(
        references=["R1"],
        get_targets=lambda _refs: {"C1": ["R1"]},
        pending_lcsc_codes={"C1"},
        set_pending_status=lambda _ref, _status: None,
        post_progress=lambda *_args: None,
    )

    assert did_start is False
