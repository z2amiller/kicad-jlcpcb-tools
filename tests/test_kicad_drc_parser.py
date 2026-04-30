"""Unit tests for DRC report parsing logic."""

from pathlib import Path

from kicad_drc import parse_drc_report


def _write_report(tmp_path: Path, text: str) -> Path:
    report_path = tmp_path / "drc_report.rpt"
    report_path.write_text(text, encoding="utf-8")
    return report_path


def test_parse_drc_report_extracts_error_severity_items(tmp_path):
    """Parser returns only error-severity items from modern report blocks."""
    report_text = """
** Found 3 DRC violations **
[clearance]: Track too close to pad; error
  @ (10.0 mm, 20.0 mm)
[annular_width]: Via annular width warning; warning
  @ (11.0 mm, 21.0 mm)
[courtyard_overlap]: Courtyard overlap; error
  @ (12.0 mm, 22.0 mm)
** End of Report **
""".strip()
    report_path = _write_report(tmp_path, report_text)

    count, messages = parse_drc_report(str(report_path))

    assert count == 2
    assert messages == [
        "[clearance]: Track too close to pad; error",
        "[courtyard_overlap]: Courtyard overlap; error",
    ]


def test_parse_drc_report_falls_back_to_total_count_when_no_severity(tmp_path):
    """Parser falls back to header violation count for older/non-detailed reports."""
    report_text = """
** Found 4 DRC violations **
Some legacy report format text without per-item severity.
** End of Report **
""".strip()
    report_path = _write_report(tmp_path, report_text)

    count, messages = parse_drc_report(str(report_path))

    assert count == 4
    assert messages == []


def test_parse_drc_report_raises_when_count_not_parseable(tmp_path):
    """Parser raises runtime error when report has no parseable violation header."""
    report_text = """
No DRC section available in this output.
""".strip()
    report_path = _write_report(tmp_path, report_text)

    try:
        parse_drc_report(str(report_path))
        raise AssertionError("Expected parse_drc_report to raise RuntimeError")
    except RuntimeError as exc:
        assert "Could not parse DRC violation count" in str(exc)
