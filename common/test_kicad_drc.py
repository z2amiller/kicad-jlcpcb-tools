"""Unit tests for DRC report parsing."""

from pathlib import Path

from kicad_drc import parse_drc_report


def test_parse_drc_report_counts_unconnected_item_errors(tmp_path: Path):
    """Unconnected-item error blocks should be counted as DRC errors."""
    report = tmp_path / "drc.rpt"
    report.write_text(
        """
** Drc report for demo.kicad_pcb **
** Created on 2026-04-26T22:39:33 **
** Report includes: Errors **

** Found 0 DRC violations **

** Found 1 unconnected pads **
[unconnected_items]: Missing connection between items
    Local override; error
    @(106.5000 mm, 111.5000 mm): Zone [Net-(Q3-B)] on F.Cu, priority 8

** Found 0 Footprint errors **

** End of Report **
""".strip()
        + "\n",
        encoding="utf-8",
    )

    count, messages = parse_drc_report(str(report))

    assert count == 1
    assert messages == ["[unconnected_items]: Missing connection between items"]


def test_parse_drc_report_ignores_warning_blocks(tmp_path: Path):
    """Warning-only blocks should not increment error count."""
    report = tmp_path / "drc_warning_only.rpt"
    report.write_text(
        """
** Drc report for demo.kicad_pcb **
** Created on 2026-04-26T22:39:33 **
** Report includes: Errors **

** Found 2 DRC violations **
[clearance]: Copper clearance too small
    Rule area; warning

[shorting]: Net short-circuit
    Local override; error

** End of Report **
""".strip()
        + "\n",
        encoding="utf-8",
    )

    count, messages = parse_drc_report(str(report))

    assert count == 1
    assert messages == ["[shorting]: Net short-circuit"]
