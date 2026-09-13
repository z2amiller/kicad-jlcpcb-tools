"""Validate the resolver against a board and recorded EasyEDA responses (spec section 11).

Usage:
    python3 scripts/validate_board.py BOARD.kicad_pcb [--fixtures DIR] [--truth truth.csv]
                                      [--flip-y] [--report out.txt]

Without --truth it prints one verdict per part that has an LCSC field.  With
--truth it compares the CPL rotation the resolver would emit against the
rotation JLC's preview settled on, and exits 1 on any disagreement.  This is
the M0 gate.

truth.csv columns: reference,observed_rotation.  observed_rotation is JLC's
final rotation for the part after it was aligned in the placement preview.
Leave it blank for parts that must NOT get a derived rotation (wrong picks,
checkerboard parts).
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from jlcfootprint.boardfile import (  # noqa: E402
    KiCadFootprint,
    footprint_pads,
    parse_kicad_pcb,
)
from jlcfootprint.easyeda_parse import (  # noqa: E402
    ComponentRecord,
    parse_component_response,
)
from jlcfootprint.geometry import easyeda_pads_to_mm, mirror_y  # noqa: E402
from jlcfootprint.resolver import Verdict, resolve  # noqa: E402

DEFAULT_FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"


def expected_cpl_rotation(placed: float, is_bottom: bool, correction: int) -> float:
    """Reproduce upstream Fabrication: bottom parts mirror first, the correction is added after."""
    rotation = placed % 360
    if is_bottom:
        rotation = (180 - rotation) % 360
    return (rotation + correction % 360) % 360


def load_record(fixtures: Path, lcsc: str) -> ComponentRecord | None:
    """Parse the recorded response for one part, or None when no fixture exists."""
    path = fixtures / f"{lcsc}.json"
    if not path.exists():
        return None
    return parse_component_response(json.loads(path.read_text(encoding="utf-8")), lcsc)


def evaluate_footprints(
    footprints: list[KiCadFootprint], fixtures: Path, flip_y: bool = False
) -> list[dict]:
    """Resolve every footprint with an LCSC field; each row keeps the board facts and the verdict."""
    rows: list[dict] = []
    for fp in footprints:
        if not fp.lcsc:
            continue
        row = {
            "reference": fp.reference,
            "lcsc": fp.lcsc,
            "footprint": fp.footprint_name,
            "placed": fp.placed_rotation,
            "bottom": fp.is_bottom,
            "package": "",
            "verdict": None,
        }
        record = load_record(fixtures, fp.lcsc)
        if record is not None:
            pads = footprint_pads(fp)
            if fp.is_bottom:
                pads = mirror_y(pads)
            row["package"] = record.package_name
            row["verdict"] = resolve(
                pads,
                fp.footprint_name,
                record.status,
                record.package_name,
                easyeda_pads_to_mm(record.pads, flip_y=flip_y),
                record.symbol_pins,
            )
        rows.append(row)
    return rows


def reference_key(reference: str) -> tuple:
    """Sort key that orders Q2 before Q10: letters, then the number, then any suffix."""
    match = re.match(r"^([A-Za-z_]*)(\d*)(.*)$", reference)
    letters, digits, rest = match.groups() if match else (reference, "", "")
    return (letters.upper(), int(digits) if digits else -1, rest)


def evaluate(board: Path, fixtures: Path, flip_y: bool = False) -> list[dict]:
    """Parse the board file and evaluate it, rows in reference order.

    pcbnew writes footprints in the order of their fresh internal ids, so a
    regenerated board lists the same parts in a different order; sorting keeps
    the report and the gate deterministic.
    """
    footprints = sorted(
        parse_kicad_pcb(str(board)), key=lambda fp: reference_key(fp.reference)
    )
    return evaluate_footprints(footprints, fixtures, flip_y)


def load_truth(path: Path) -> dict[str, str]:
    """Return ``{reference: observed_rotation text}`` from the truth CSV."""
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            row["reference"].strip(): row["observed_rotation"].strip()
            for row in csv.DictReader(handle)
        }


def compare(rows: list[dict], truth: dict[str, str]) -> list[tuple[dict, str]]:
    """Return ``(row, reason)`` for every part whose emitted rotation would disagree with JLC."""
    failures: list[tuple[dict, str]] = []
    for row in rows:
        if row["reference"] not in truth:
            continue
        observed = truth[row["reference"]]
        verdict: Verdict | None = row["verdict"]
        if verdict is None:
            failures.append((row, "no recorded EasyEDA response"))
            continue
        if observed == "":
            if verdict.rotation is not None:
                failures.append(
                    (row, f"expected no derived rotation, got {verdict.rotation}")
                )
            continue
        if verdict.rotation is None:
            failures.append(
                (row, f"no derived rotation ({verdict.status}); JLC shows {observed}")
            )
            continue
        expected = expected_cpl_rotation(row["placed"], row["bottom"], verdict.rotation)
        if expected != float(observed) % 360:
            failures.append((row, f"would emit {expected:g}, JLC shows {observed}"))
    return failures


def format_rows(rows: list[dict]) -> str:
    """Render one line per part."""
    header = f"{'ref':<6} {'lcsc':<10} {'status':<8} {'fit':<17} {'rot':>4} {'name':>4}  footprint / package / notes"
    lines = [header]
    for row in rows:
        verdict = row["verdict"]
        if verdict is None:
            lines.append(
                f"{row['reference']:<6} {row['lcsc']:<10} {'no-fix':<8} {'':<17} {'':>4} {'':>4}  {row['footprint']}"
            )
            continue
        rot = "" if verdict.rotation is None else str(verdict.rotation)
        name = "" if verdict.name_rotation is None else str(verdict.name_rotation)
        lines.append(
            f"{row['reference']:<6} {row['lcsc']:<10} {verdict.status:<8} {verdict.fit:<17} "
            f"{rot:>4} {name:>4}  {row['footprint']} / {row['package']} / {verdict.note_text}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Run the validator from the command line."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("board", type=Path)
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--truth", type=Path)
    parser.add_argument(
        "--flip-y",
        action="store_true",
        help="flip EasyEDA Y before matching (spec section 6 calibration)",
    )
    parser.add_argument("--report", type=Path, help="also write the table to this file")
    args = parser.parse_args(argv)
    rows = evaluate(args.board, args.fixtures, args.flip_y)
    table = format_rows(rows)
    print(table)
    if args.report:
        args.report.write_text(table + "\n", encoding="utf-8")
    if not args.truth:
        return 0
    truth = load_truth(args.truth)
    failures = compare(rows, truth)
    checked = sum(1 for row in rows if row["reference"] in truth)
    print(f"\n{checked} parts checked against JLC, {len(failures)} disagree")
    for row, reason in failures:
        side = " bottom" if row["bottom"] else ""
        print(
            f"  {row['reference']:<6} {row['lcsc']:<10} placed {row['placed']:g}{side}: {reason}"
        )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
