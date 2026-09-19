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


def test_rows_fall_into_four_groups():
    """Applied covers derived and override values; yellow is a subset; red is its own group; the rest is unresolved."""
    summary = summarise(_rows())
    assert [row.reference for row in summary.applied] == ["Q1", "D1", "U1"]
    assert [row.reference for row in summary.yellow] == ["D1"]
    assert [row.reference for row in summary.red] == ["D2"]
    assert [row.reference for row in summary.unresolved] == ["R1", "C1", "C2"]
    assert [row.reference for row in summary.differs_from_legacy] == ["D1", "U1"]
    assert summary.caveated == []


def test_a_red_verdict_under_an_override_is_applied():
    """An override wins over a red verdict: the row is applied, not listed as a misfit."""
    row = CplRotation(
        "U9", "C1", "SO-8", "X", 0, 90, 90, "override", "red", None, "pitch"
    )
    summary = summarise([row])
    assert summary.applied == [row] and summary.red == []


def test_caveated_fits_are_marked_in_the_applied_group():
    """A fit whose JLC body overhangs the courtyard is applied, listed and marked with !."""
    row = CplRotation(
        "C9",
        "C88744",
        "CP_Elec_4x5.8",
        "100u",
        0,
        0,
        0,
        "derived",
        "green",
        None,
        "fits",
        note="fits, JLC body 0.9 mm larger than the KiCad courtyard",
        legacy_correction=0,
        body_excess=0.9,
    )
    summary = summarise([row])
    assert summary.caveated == [row]
    text = format_summary(summary)
    assert "! marks a part whose JLC body is larger than the KiCad courtyard" in text
    assert (
        "  C9       C88744     CP_Elec_4x5.8: 0° -> 0° (derived +0°, rules 0°)"
        " ! body 0.9 mm larger than the courtyard"
    ) in text


def test_summary_text_marks_migration_differences_and_explains_unresolved_parts():
    """The text lists counts, marks rows the rules would have set differently, and says why parts are raw."""
    text = format_summary(summarise(_rows()))
    lines = text.splitlines()
    assert lines[0] == "Does not fit (1)"
    assert lines[1] == (
        "  D2       C1234      SOD-123: 45° raw (red, count: does not fit: 2 vs 3 pads)"
    )
    assert lines[2] == ""
    assert lines[3] == "Applied rotations (3)"
    assert "* marks a value the correction rules would have set differently" in lines[4]
    assert lines[5].startswith(
        "  Q1       C2132      SOT-23: 0° -> 180° (derived +180°, rules 180°)"
    )
    assert lines[6].startswith(
        "* D1       C2286      LED_0603: 90° -> 90° (derived +0°, rules 180°)"
    )
    assert lines[7].startswith(
        "* U1       C7950      SOIC-8: 0° -> 270° (override +270°, rules 0°)"
    )
    assert "JLC pin-1 marker will look wrong (1)" in text
    assert (
        "  D1       C2286      LED_0603: numbering difference, not a rotation error"
        in text
    )
    assert "Unresolved, raw angle emitted (3)" in text
    assert "  D2       C1234      SOD-123: 45° (red" not in text
    assert "  R1       -          R_0603: 0° (no LCSC number)" in text
    assert "  C1       C999       C_0603: 180° (EasyEDA data still pending)" in text
    assert "  C2       C998       C_0603: 0° (not checked yet)" in text


def test_summary_without_the_legacy_rules_shows_no_comparison():
    """When the correction rules could not be read the applied rows carry no rules column."""
    text = format_summary(summarise(_rows()[:1], legacy_available=False))
    assert "(correction rules unavailable; no comparison)" in text
    assert "rules" not in text.splitlines()[5]
    assert not text.splitlines()[5].startswith("*")
    assert format_summary(summarise([])) == "\n".join(
        [
            "Does not fit (0)",
            "",
            "Applied rotations (0)",
            "",
            "JLC pin-1 marker will look wrong (0)",
            "",
            "Unresolved, raw angle emitted (0)",
        ]
    )


def test_the_summary_counts_the_two_position_sources_when_the_setting_is_on():
    """Spec 17.4: with the setting on, the header line says where Mid X/Y came from."""
    rows = _rows()
    rows[0].position_source = "origin"

    text = format_summary(summarise(rows, exact_origin=True))

    assert text.splitlines()[0] == (
        "Positions: 1 at JLC's package origin, 6 at the pad-box centre"
    )
    assert text.splitlines()[1] == ""
    assert text.splitlines()[2].startswith("Does not fit (")


def test_the_summary_says_nothing_about_positions_when_the_setting_is_off():
    """Off, the summary is exactly M3's, whatever a row happens to carry."""
    rows = _rows()
    rows[0].position_source = "origin"

    assert format_summary(summarise(rows)).splitlines()[0].startswith("Does not fit (")


def test_the_position_counts_cover_every_placed_part_once():
    """Red and unresolved parts are counted too: the two numbers add up to the CPL."""
    rows = _rows()
    for row in rows:
        row.position_source = "origin"
    rows[-1].position_source = "pad-box"

    summary = summarise(rows, exact_origin=True)

    assert len(summary.at_origin) + len(summary.at_pad_box) == len(rows)
    assert len(summary.at_pad_box) == 1
