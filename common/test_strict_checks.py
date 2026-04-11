"""Tests for strict part-check matching helpers."""

from dataview_highlight import encode_highlighted_value
from strict_checks import evaluate_part_strict_check


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
