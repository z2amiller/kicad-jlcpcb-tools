"""Tests for the generate-time rotation summary."""

from jlcfootprint.report import CplRotation, format_summary, summarise


def _rows():
    return [
        CplRotation(
            "Q1",
            "C2132",
            "SOT-23",
            "BC847",
            0,
            180,
            180,
            "derived",
            "green",
            None,
            "fits",
            legacy_correction=180,
        ),
        CplRotation(
            "D1",
            "C2286",
            "LED_0603",
            "LED",
            90,
            90,
            0,
            "derived",
            "yellow",
            "yellow",
            "fits",
            note="marker",
            legacy_correction=180,
        ),
        CplRotation(
            "U1",
            "C7950",
            "SOIC-8",
            "OPA",
            0,
            270,
            270,
            "override",
            "red",
            None,
            "pitch",
            note="checked",
            legacy_correction=None,
        ),
        CplRotation(
            "D2",
            "C1234",
            "SOD-123",
            "D",
            45,
            45,
            None,
            "raw",
            "red",
            None,
            "count",
            note="does not fit: 2 vs 3 pads",
        ),
        CplRotation("R1", "", "R_0603", "10k", 0, 0, None, "raw", "no-lcsc"),
        CplRotation(
            "C1",
            "C999",
            "C_0603",
            "100n",
            180,
            180,
            None,
            "raw",
            "pending",
            pending=True,
        ),
        CplRotation("C2", "C998", "C_0603", "1u", 0, 0, None, "raw", "no-verdict"),
    ]


def test_rows_fall_into_three_groups():
    """Applied covers derived and override values; yellow is a subset; the rest is unresolved."""
    summary = summarise(_rows())
    assert [row.reference for row in summary.applied] == ["Q1", "D1", "U1"]
    assert [row.reference for row in summary.yellow] == ["D1"]
    assert [row.reference for row in summary.unresolved] == ["D2", "R1", "C1", "C2"]
    assert [row.reference for row in summary.differs_from_legacy] == ["D1", "U1"]


def test_summary_text_marks_migration_differences_and_explains_unresolved_parts():
    """The text lists counts, marks rows the rules would have set differently, and says why parts are raw."""
    text = format_summary(summarise(_rows()))
    lines = text.splitlines()
    assert lines[0] == "Applied rotations (3)"
    assert "* marks a value the correction rules would have set differently" in lines[1]
    assert lines[2].startswith(
        "  Q1       C2132      SOT-23: 0° -> 180° (derived +180°, rules 180°)"
    )
    assert lines[3].startswith(
        "* D1       C2286      LED_0603: 90° -> 90° (derived +0°, rules 180°)"
    )
    assert lines[4].startswith(
        "* U1       C7950      SOIC-8: 0° -> 270° (override +270°, rules 0°)"
    )
    assert "JLC pin-1 marker will look wrong (1)" in text
    assert (
        "  D1       C2286      LED_0603: numbering difference, not a rotation error"
        in text
    )
    assert "Unresolved, raw angle emitted (4)" in text
    assert (
        "  D2       C1234      SOD-123: 45° (red, count: does not fit: 2 vs 3 pads)"
        in text
    )
    assert "  R1       -          R_0603: 0° (no LCSC number)" in text
    assert "  C1       C999       C_0603: 180° (EasyEDA data still pending)" in text
    assert "  C2       C998       C_0603: 0° (not checked yet)" in text


def test_summary_without_the_legacy_rules_shows_no_comparison():
    """When the correction rules could not be read the applied rows carry no rules column."""
    text = format_summary(summarise(_rows()[:1], legacy_available=False))
    assert "(correction rules unavailable; no comparison)" in text
    assert "rules" not in text.splitlines()[2]
    assert not text.splitlines()[2].startswith("*")
    assert format_summary(summarise([])) == "\n".join(
        [
            "Applied rotations (0)",
            "",
            "JLC pin-1 marker will look wrong (0)",
            "",
            "Unresolved, raw angle emitted (0)",
        ]
    )
