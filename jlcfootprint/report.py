"""The generate-time rotation summary (spec section 8): three groups of placed parts.

Pure formatting over what the CPL path recorded for each placed part, so the
dialog in the plugin only shows text.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CplRotation:
    """One placed part's rotation as the CPL emitted it."""

    reference: str
    lcsc: str
    footprint: str
    value: str
    raw: float  # KiCad's angle after the bottom mirror, before any correction
    emitted: float  # the angle written to the CPL
    correction: int | None = (
        None  # the correction applied; None when the raw angle was kept
    )
    source: str = "raw"  # 'override' | 'derived' | 'raw'
    status: str = "unknown"
    polarity_light: str | None = None
    fit: str | None = None
    note: str = ""
    pending: bool = False
    legacy_correction: int | None = None  # what the correction rules would have applied


@dataclass
class GenerateSummary:
    """The three groups of the summary dialog."""

    applied: list[CplRotation] = field(default_factory=list)
    yellow: list[CplRotation] = field(default_factory=list)
    unresolved: list[CplRotation] = field(default_factory=list)
    legacy_available: bool = True

    @property
    def differs_from_legacy(self) -> list[CplRotation]:
        """Return the applied rows the correction rules would have rotated differently."""
        return [
            row
            for row in self.applied
            if (row.legacy_correction or 0) % 360 != (row.correction or 0) % 360
        ]


def summarise(
    rows: list[CplRotation], legacy_available: bool = True
) -> GenerateSummary:
    """Sort the CPL rows into applied, yellow and unresolved."""
    summary = GenerateSummary(legacy_available=legacy_available)
    for row in rows:
        if row.source in ("override", "derived"):
            summary.applied.append(row)
            if row.polarity_light == "yellow":
                summary.yellow.append(row)
        else:
            summary.unresolved.append(row)
    return summary


def _reason(row: CplRotation) -> str:
    if not row.lcsc:
        return "no LCSC number"
    if row.pending:
        return "EasyEDA data still pending"
    if row.status == "no-verdict":
        return "not checked yet"
    return f"{row.status}, {row.fit or 'no fit'}" + (
        f": {row.note}" if row.note else ""
    )


def format_summary(summary: GenerateSummary) -> str:
    """Render the three groups as the dialog shows them."""
    lines: list[str] = []
    differs = {id(row) for row in summary.differs_from_legacy}
    lines.append(f"Applied rotations ({len(summary.applied)})")
    if summary.applied:
        if summary.legacy_available:
            lines.append(
                "  * marks a value the correction rules would have set differently"
                " (the migration triage)"
            )
        else:
            lines.append("  (correction rules unavailable; no comparison)")
    for row in summary.applied:
        mark = "*" if summary.legacy_available and id(row) in differs else " "
        legacy = (
            ""
            if not summary.legacy_available
            else f", rules {row.legacy_correction if row.legacy_correction is not None else 0:g}°"
        )
        source = "override" if row.source == "override" else "derived"
        lines.append(
            f"{mark} {row.reference:<8} {row.lcsc:<10} {row.footprint}: "
            f"{row.raw:g}° -> {row.emitted:g}° ({source} {row.correction:+d}°{legacy})"
        )
    lines.append("")
    lines.append(f"JLC pin-1 marker will look wrong ({len(summary.yellow)})")
    for row in summary.yellow:
        lines.append(
            f"  {row.reference:<8} {row.lcsc:<10} {row.footprint}: numbering difference, "
            "not a rotation error; do not renumber the footprint"
        )
    lines.append("")
    lines.append(f"Unresolved, raw angle emitted ({len(summary.unresolved)})")
    for row in summary.unresolved:
        lines.append(
            f"  {row.reference:<8} {row.lcsc or '-':<10} {row.footprint}: "
            f"{row.emitted:g}° ({_reason(row)})"
        )
    return "\n".join(lines)
