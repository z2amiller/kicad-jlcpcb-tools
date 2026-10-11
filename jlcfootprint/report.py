"""The generate-time rotation summary: four groups, worst first.

"Does not fit" first, then the applied rotations with caveated fits marked, then
the pin-1 marker warnings, then the unresolved parts.  Pure formatting over what
the CPL path recorded for each placed part, so the plugin only logs the lines and
asks before writing when :func:`raw_angle_reason` names a part.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .resolver import SINGLE_PAD_NOTE


@dataclass
class CplRotation:
    """One placed part's rotation as the CPL emitted it."""

    reference: str
    lcsc: str
    footprint: str
    value: str
    raw: float  # KiCad's angle after the bottom mirror, before any correction
    emitted: float  # the angle written to the CPL
    # The correction applied; None when the raw angle was kept.
    correction: Optional[int] = None
    source: str = "raw"  # 'override' | 'derived' | 'raw'
    status: str = "unknown"
    polarity_light: Optional[str] = None
    fit: Optional[str] = None
    note: str = ""
    pending: bool = False
    # What the correction rules would have applied, read for comparison only.
    legacy_correction: Optional[int] = None
    # How far the JLC body overhangs the KiCad courtyard, in mm.
    body_excess: Optional[float] = None
    # Where Mid X/Y came from: 'origin' is JLC's package origin and 'pad-box' the
    # centre of the soldered pads' bounding box, which every part gets while
    # ``jlcfootprint.exact_origin`` is off.
    position_source: str = "pad-box"


@dataclass
class GenerateSummary:
    """The groups of the rotation summary.

    ``applied`` holds the parts the CPL turned (derived or overridden), and
    ``yellow`` those of them whose JLC pin-1 marker will look wrong; ``red`` the
    parts the check found not to fit, emitted at the raw angle; ``unresolved`` the
    rest, emitted raw too: unknown, still pending, not checked, no LCSC, or placing
    another part than the one the check judged.
    """

    applied: list[CplRotation] = field(default_factory=list)
    yellow: list[CplRotation] = field(default_factory=list)
    red: list[CplRotation] = field(default_factory=list)
    unresolved: list[CplRotation] = field(default_factory=list)
    legacy_available: bool = True
    exact_origin: bool = False  # whether the setting asked for JLC's origin

    @property
    def at_origin(self) -> list[CplRotation]:
        """Return the rows the CPL placed at JLC's package origin."""
        return [row for row in self.rows if row.position_source == "origin"]

    @property
    def at_pad_box(self) -> list[CplRotation]:
        """Return the rows the CPL placed at the pad-box centre."""
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


def raw_angle_reason(row: CplRotation) -> Optional[str]:
    """Return why the CPL emits this part at KiCad's raw angle against the check, or None.

    A part counts when it has an LCSC number and keeps the raw angle because the
    check refused it (it does not fit, or EasyEDA's data contradicts itself), could
    not decide it (no data, polarity unknown, never checked), was still being
    fetched when generation went on, or its row places another part than the one
    the check judged.  An explicit override replaces the raw angle, so a part carrying
    one never counts; a part whose LCSC changed is emitted raw whatever the old
    part's override says, so it still does.  Parts without an LCSC and parts the
    check placed are never named, and neither is an unknown part the resolver
    left undecided because a side has fewer than two named pads
    (``SINGLE_PAD_NOTE``): a one-pad part's angle rarely matters, and the log
    still lists it.  The resolver records that reason only as its note, so the
    note is matched whole.
    """
    if not row.lcsc or row.correction is not None:
        return None
    if row.status == "lcsc-mismatch":
        return "part number changed since it was checked"
    if row.pending:
        return "still fetching"
    if row.status == "red":
        return "EasyEDA data inconsistent" if row.fit == "no_data" else "does not fit"
    if row.status == "unknown":
        return None if SINGLE_PAD_NOTE in row.note.split("; ") else "unknown"
    if row.status == "no-verdict":
        return "not checked"
    return None


def summary_lines(
    summary: GenerateSummary,
) -> list[tuple[Optional[CplRotation], str]]:
    """Return the summary's lines worst first, each with the row it lists (None for headers)."""
    lines: list[tuple[Optional[CplRotation], str]] = []
    differs = {id(row) for row in summary.differs_from_legacy}
    if summary.exact_origin:
        # With the setting on, say how the positions were split before anything
        # else, because that is what the setting changes about this CPL.
        lines.append(
            (
                None,
                f"Positions: {len(summary.at_origin)} at JLC's package origin, "
                f"{len(summary.at_pad_box)} at the pad-box centre",
            )
        )
        lines.append((None, ""))
    lines.append((None, f"Does not fit ({len(summary.red)})"))
    for row in summary.red:
        lines.append(
            (
                row,
                f"  {row.reference:<8} {row.lcsc:<10} {row.footprint}: "
                f"{row.emitted:g}° raw ({_reason(row)})",
            )
        )
    lines.append((None, ""))
    lines.append((None, f"Applied rotations ({len(summary.applied)})"))
    if summary.applied:
        if summary.legacy_available:
            lines.append(
                (
                    None,
                    "  * marks a value the correction rules would have set differently"
                    " (the migration triage)",
                )
            )
        else:
            lines.append((None, "  (correction rules unavailable; no comparison)"))
        if summary.caveated:
            lines.append(
                (
                    None,
                    "  ! marks a part whose JLC body is larger than the KiCad courtyard;"
                    " check the preview",
                )
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
            (
                row,
                f"{mark} {row.reference:<8} {row.lcsc:<10} {row.footprint}: "
                f"{row.raw:g}° -> {row.emitted:g}° ({source} {int(row.correction):+d}°{legacy}){caveat}",
            )
        )
    lines.append((None, ""))
    lines.append((None, f"JLC pin-1 marker will look wrong ({len(summary.yellow)})"))
    for row in summary.yellow:
        lines.append(
            (
                row,
                f"  {row.reference:<8} {row.lcsc:<10} {row.footprint}: numbering difference, "
                "not a rotation error; do not renumber the footprint",
            )
        )
    lines.append((None, ""))
    lines.append((None, f"Unresolved, raw angle emitted ({len(summary.unresolved)})"))
    for row in summary.unresolved:
        lines.append(
            (
                row,
                f"  {row.reference:<8} {row.lcsc or '-':<10} {row.footprint}: "
                f"{row.emitted:g}° ({_reason(row)})",
            )
        )
    return lines


def format_summary(summary: GenerateSummary) -> str:
    """Render the summary as text, worst first: the lines the plugin logs."""
    return "\n".join(text for _row, text in summary_lines(summary))
