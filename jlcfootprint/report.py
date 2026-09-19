"""The generate-time rotation summary: four groups, worst first (spec 8).

"Does not fit" first, then the applied rotations with caveated fits marked, then
the pin-1 marker warnings, then the unresolved parts.  Pure formatting over what
the CPL path recorded for each placed part, so the dialog in the plugin only
shows text.
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
    # How far the JLC body overhangs the KiCad courtyard, in mm (spec 16.6).
    body_excess: float | None = None
    # Where Mid X/Y came from: 'origin' is JLC's package origin and 'pad-box' is
    # upstream's pad bounding-box centre, which is every part's while
    # ``jlcfootprint.exact_origin`` is off (spec 17.4).
    position_source: str = "pad-box"


@dataclass
class GenerateSummary:
    """The groups of the summary dialog.

    ``red`` holds the parts the check found not to fit (the raw angle emitted);
    ``unresolved`` the parts it could not judge: unknown, pending, no LCSC.
    """

    applied: list[CplRotation] = field(default_factory=list)
    yellow: list[CplRotation] = field(default_factory=list)
    red: list[CplRotation] = field(default_factory=list)
    unresolved: list[CplRotation] = field(default_factory=list)
    legacy_available: bool = True
    exact_origin: bool = False  # whether the setting asked for JLC's origin (17.4)

    @property
    def at_origin(self) -> list[CplRotation]:
        """Return the rows the CPL placed at JLC's package origin."""
        return [row for row in self.rows if row.position_source == "origin"]

    @property
    def at_pad_box(self) -> list[CplRotation]:
        """Return the rows the CPL placed at upstream's pad-bounding-box centre."""
        return [row for row in self.rows if row.position_source != "origin"]

    @property
    def rows(self) -> list[CplRotation]:
        """Return every placed part, in the four groups' order."""
        return self.red + self.applied + self.unresolved

    @property
    def caveated(self) -> list[CplRotation]:
        """Return the applied rows whose JLC body overhangs the KiCad courtyard."""
        return [row for row in self.applied if row.body_excess is not None]

    @property
    def differs_from_legacy(self) -> list[CplRotation]:
        """Return the applied rows the correction rules would have rotated differently."""
        return [
            row
            for row in self.applied
            if (row.legacy_correction or 0) % 360 != (row.correction or 0) % 360
        ]


def summarise(
    rows: list[CplRotation],
    legacy_available: bool = True,
    exact_origin: bool = False,
) -> GenerateSummary:
    """Sort the CPL rows into applied (with yellow), red and unresolved."""
    summary = GenerateSummary(
        legacy_available=legacy_available, exact_origin=exact_origin
    )
    for row in rows:
        if row.source in ("override", "derived"):
            summary.applied.append(row)
            if row.polarity_light == "yellow":
                summary.yellow.append(row)
        elif row.status == "red":
            summary.red.append(row)
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
    """Render the groups as the dialog shows them, worst first."""
    lines: list[str] = []
    differs = {id(row) for row in summary.differs_from_legacy}
    if summary.exact_origin:
        # With the setting on, say how the positions were split before anything
        # else, because that is what changed about this CPL (spec 17.4).
        lines.append(
            f"Positions: {len(summary.at_origin)} at JLC's package origin, "
            f"{len(summary.at_pad_box)} at the pad-box centre"
        )
        lines.append("")
    lines.append(f"Does not fit ({len(summary.red)})")
    for row in summary.red:
        lines.append(
            f"  {row.reference:<8} {row.lcsc:<10} {row.footprint}: "
            f"{row.emitted:g}° raw ({_reason(row)})"
        )
    lines.append("")
    lines.append(f"Applied rotations ({len(summary.applied)})")
    if summary.applied:
        if summary.legacy_available:
            lines.append(
                "  * marks a value the correction rules would have set differently"
                " (the migration triage)"
            )
        else:
            lines.append("  (correction rules unavailable; no comparison)")
        if summary.caveated:
            lines.append(
                "  ! marks a part whose JLC body is larger than the KiCad courtyard;"
                " check the preview"
            )
    for row in summary.applied:
        mark = "*" if summary.legacy_available and id(row) in differs else " "
        legacy = (
            ""
            if not summary.legacy_available
            else f", rules {row.legacy_correction if row.legacy_correction is not None else 0:g}°"
        )
        source = "override" if row.source == "override" else "derived"
        caveat = (
            f" ! body {row.body_excess:g} mm larger than the courtyard"
            if row.body_excess is not None
            else ""
        )
        lines.append(
            f"{mark} {row.reference:<8} {row.lcsc:<10} {row.footprint}: "
            f"{row.raw:g}° -> {row.emitted:g}° ({source} {int(row.correction):+d}°{legacy}){caveat}"
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
