"""Tests for strict part-check matching helpers."""

from dataview_highlight import encode_highlighted_value
from strict_checks import build_strict_check_failures, evaluate_part_strict_check


def test_evaluate_part_strict_check_returns_none_without_lcsc():
    """Rows without LCSC assignment are ignored by strict checks."""
    result = evaluate_part_strict_check(
        reference="R1",
        value="10K",
        footprint="Resistor_SMD:R_0603_1608Metric",
        lcsc="",
        params_value="10KΩ 1% 0603",
    )
    assert result is None


def test_evaluate_part_strict_check_matches_value_and_footprint_terms():
    """Strict checks should pass when both value and footprint terms are present."""
    params = encode_highlighted_value("10KΩ ±1% 0603", ["10K", "0603"])

    result = evaluate_part_strict_check(
        reference="R1",
        value="10K",
        footprint="Resistor_SMD:R_0603_1608Metric",
        lcsc="C12345",
        params_value=params,
    )

    assert result is not None
    assert result.value_ok is True
    assert result.footprint_ok is True
    assert result.passes is True


def test_evaluate_part_strict_check_detects_missing_footprint_match():
    """Strict checks should fail when footprint terms are absent from params text."""
    result = evaluate_part_strict_check(
        reference="R2",
        value="10K",
        footprint="Resistor_SMD:R_0603_1608Metric",
        lcsc="C54321",
        params_value="10KΩ ±1%",
    )

    assert result is not None
    assert result.value_ok is True
    assert result.footprint_ok is False
    assert result.passes is False


def test_build_strict_check_failures_marks_exemptions_per_check_type():
    """Aggregated failures preserve per-check exemption state for each row."""
    columns = {
        "REF_COL": 0,
        "VALUE_COL": 1,
        "FP_COL": 2,
        "LCSC_COL": 3,
        "PARAMS_COL": 4,
    }
    rows = [
        [
            "R1",
            "10K",
            "Resistor_SMD:R_0603_1608Metric",
            "C123",
            encode_highlighted_value("10KΩ ±1%", ["10K"]),
        ],
        [
            "C1",
            "1uF",
            "Capacitor_SMD:CP_Elec_6.3x7.7",
            "C456",
            encode_highlighted_value("SMD,D6.3", ["SMD,D6.3"]),
        ],
    ]

    def exemption_getter(reference, lcsc):
        if (reference, lcsc) == ("R1", "C123"):
            return {"value": False, "footprint": True}
        if (reference, lcsc) == ("C1", "C456"):
            return {"value": False, "footprint": False}
        return {"value": False, "footprint": False}

    failures = build_strict_check_failures(rows, columns, exemption_getter)

    assert failures == [
        {
            "reference": "R1",
            "lcsc": "C123",
            "check_type": "footprint",
            "value": "10K",
            "footprint": "Resistor_SMD:R_0603_1608Metric",
            "params_text": "10KΩ ±1%",
            "exempted": True,
        },
        {
            "reference": "C1",
            "lcsc": "C456",
            "check_type": "value",
            "value": "1uF",
            "footprint": "Capacitor_SMD:CP_Elec_6.3x7.7",
            "params_text": "SMD,D6.3",
            "exempted": False,
        },
    ]
