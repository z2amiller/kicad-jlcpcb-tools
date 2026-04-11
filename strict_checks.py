"""Strict part-check matching utilities."""

from __future__ import annotations

from dataclasses import dataclass

try:
    from .dataview_highlight import (  # noqa: I001
        decode_highlighted_value,
        expand_footprint,
        expand_value,
    )
except ImportError:  # pragma: no cover - direct module import in tests
    from dataview_highlight import (  # noqa: I001
        decode_highlighted_value,
        expand_footprint,
        expand_value,
    )


@dataclass(frozen=True)
class StrictCheckResult:
    """Structured strict-check result for one part row."""

    reference: str
    lcsc: str
    params_text: str
    value_terms: list[str]
    footprint_terms: list[str]
    matched_value_terms: list[str]
    matched_footprint_terms: list[str]

    @property
    def value_ok(self) -> bool:
        """Return whether at least one expanded value term matched params text."""
        return bool(self.matched_value_terms)

    @property
    def footprint_ok(self) -> bool:
        """Return whether at least one expanded footprint term matched params text."""
        return bool(self.matched_footprint_terms)

    @property
    def passes(self) -> bool:
        """Return overall strict-check pass state for this row."""
        return self.value_ok and self.footprint_ok


def _matched_terms(text: str, terms: list[str]) -> list[str]:
    """Return unique terms that are present in `text`, preserving term order."""
    if not text:
        return []

    lowered = text.casefold()
    matched = []
    for term in terms:
        cleaned = "" if term is None else str(term).strip()
        if cleaned and cleaned.casefold() in lowered and cleaned not in matched:
            matched.append(cleaned)
    return matched


def evaluate_part_strict_check(
    reference: str,
    value: str,
    footprint: str,
    lcsc: str,
    params_value: str,
) -> StrictCheckResult | None:
    """Evaluate strict value/footprint matching for one assigned part.

    Returns `None` for rows without an assigned LCSC number.
    """
    lcsc_text = "" if lcsc is None else str(lcsc).strip()
    if not lcsc_text:
        return None

    params_text, _ = decode_highlighted_value(params_value)
    value_terms = expand_value(reference, value)
    footprint_terms = expand_footprint(reference, footprint)

    return StrictCheckResult(
        reference="" if reference is None else str(reference),
        lcsc=lcsc_text,
        params_text=params_text,
        value_terms=value_terms,
        footprint_terms=footprint_terms,
        matched_value_terms=_matched_terms(params_text, value_terms),
        matched_footprint_terms=_matched_terms(params_text, footprint_terms),
    )


def build_strict_check_failures(
    rows: list[list],
    columns: dict[str, int],
    exemption_getter,
) -> list[dict]:
    """Build strict-check failure records for rows with assigned LCSC values."""
    failures = []

    for row in rows:
        result = evaluate_part_strict_check(
            reference=row[columns["REF_COL"]],
            value=row[columns["VALUE_COL"]],
            footprint=row[columns["FP_COL"]],
            lcsc=row[columns["LCSC_COL"]],
            params_value=row[columns["PARAMS_COL"]],
        )
        if result is None:
            continue

        exemptions = exemption_getter(result.reference, result.lcsc)
        if not result.value_ok:
            failures.append(
                {
                    "reference": result.reference,
                    "lcsc": result.lcsc,
                    "check_type": "value",
                    "value": row[columns["VALUE_COL"]],
                    "footprint": row[columns["FP_COL"]],
                    "params_text": result.params_text,
                    "exempted": exemptions.get("value", False),
                }
            )
        if not result.footprint_ok:
            failures.append(
                {
                    "reference": result.reference,
                    "lcsc": result.lcsc,
                    "check_type": "footprint",
                    "value": row[columns["VALUE_COL"]],
                    "footprint": row[columns["FP_COL"]],
                    "params_text": result.params_text,
                    "exempted": exemptions.get("footprint", False),
                }
            )

    return failures
