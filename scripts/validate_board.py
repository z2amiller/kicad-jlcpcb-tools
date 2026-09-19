"""Validate the resolver against a board and recorded EasyEDA responses (spec section 11).

Usage:
    python3 scripts/validate_board.py BOARD.kicad_pcb [--fixtures DIR] [--truth truth.csv]
                                      [--flip-y] [--report out.txt] [--pro-fixtures DIR]

Without --truth it prints one verdict per part that has an LCSC field.  With
--truth it compares the CPL rotation the resolver would emit against the
rotation JLC's preview settled on, and exits 1 on any disagreement.  This is
the M0 gate.  With --pro-fixtures the parts are read the way the plugin fetches
them live (spec section 15): the recorded batch answers ``devices_*.json`` give
each part's uuids, ``footprint_<uuid>.json`` its pads and ``symbol_<uuid>.json``
its pins; the classic fixtures are not consulted.

truth.csv columns: reference,observed_rotation, optionally origin_dx_mm and
origin_dy_mm.  observed_rotation is JLC's final rotation for the part after it
was aligned in the placement preview.  Leave it blank for parts that must NOT
get a derived rotation (wrong picks, checkerboard parts).  The origin columns
pin JLC's package origin in the KiCad footprint frame (spec 17.6), compared
within ORIGIN_TOLERANCE_MM; blank means no derived origin is expected.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.boardfile import (  # noqa: E402
    KiCadFootprint,
    footprint_pads,
    parse_kicad_pcb,
)
from jlcfootprint.drawing import drawing_marks  # noqa: E402
from jlcfootprint.easyeda_parse import (  # noqa: E402
    ComponentRecord,
    DeviceHit,
    parse_component_response,
    parse_devices_response,
    parse_puuid_response,
    parse_symbol_response,
    resolve_record,
)
from jlcfootprint.geometry import (  # noqa: E402
    easyeda_pads_to_mm,
    mirror_box,
    mirror_y,
    pad_box_centre,
)
from jlcfootprint.resolver import Verdict, resolve  # noqa: E402

DEFAULT_FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"
# How far a derived origin may sit from the truth row before the gate fails (spec 17.6).
ORIGIN_TOLERANCE_MM = 0.005


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
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as error:
        raise SystemExit(
            f"{path}: not valid JSON ({error}); delete it and re-record"
        ) from error
    return parse_component_response(body, lcsc)


def load_pro_index(pro_dir: Path) -> dict[str, DeviceHit | None]:
    """Return every code a recorded batch answer asked, mapped to its hit or None for a miss."""
    index: dict[str, DeviceHit | None] = {}
    for path in sorted(pro_dir.glob("devices_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            codes = list(data["codes"])
            body = data["body"]
        except (ValueError, KeyError, TypeError) as error:
            raise SystemExit(
                f"{path}: not a recorded batch answer ({error})"
            ) from error
        result = parse_devices_response(body, codes)
        if result.error:
            raise SystemExit(f"{path}: {result.error}")
        for code in codes:
            index[code] = result.hits.get(code)
    return index


def load_pro_record(
    pro_dir: Path, index: dict[str, DeviceHit | None], lcsc: str
) -> ComponentRecord | None:
    """Assemble one part from the Pro fixtures as the plugin's cache would, or None when unrecorded.

    A code the batch was asked about but did not know is ``none``; a footprint that
    answered nothing makes the part ``none`` too; a missing symbol file leaves the
    part unrecorded, a symbol that answered nothing leaves it without pins.
    """
    if lcsc not in index:
        return None
    hit = index[lcsc]
    if hit is None:
        return ComponentRecord(lcsc=lcsc, status="none")
    record = ComponentRecord(
        lcsc=lcsc,
        status="ok",
        symbol_uuid=hit.symbol_uuid,
        puuid=hit.puuid,
        package_name=hit.package_name,
        footprint_source="puuid-endpoint",
    )
    footprint_path = pro_dir / f"footprint_{hit.puuid}.json"
    if not footprint_path.exists():
        return None
    footprint = parse_puuid_response(
        json.loads(footprint_path.read_text(encoding="utf-8")), hit.puuid
    )
    if footprint.status != "ok":
        return ComponentRecord(lcsc=lcsc, status="none")
    record.package_name = footprint.package_name or hit.package_name
    record.pads = footprint.pads
    record.footprint_shapes = footprint.footprint_shapes
    record.footprint_origin = footprint.footprint_origin
    if hit.symbol_uuid:
        symbol_path = pro_dir / f"symbol_{hit.symbol_uuid}.json"
        if not symbol_path.exists():
            return None
        symbol = parse_symbol_response(
            json.loads(symbol_path.read_text(encoding="utf-8")), hit.symbol_uuid
        )
        if symbol.status == "ok":
            record.symbol_pins = symbol.pins
            record.symbol_shapes = symbol.shapes
    return record


def evaluate_footprints(
    footprints: list[KiCadFootprint],
    fixtures: Path,
    flip_y: bool | None = None,
    pro_fixtures: Path | None = None,
) -> list[dict]:
    """Resolve every footprint with an LCSC field; each row keeps the board facts and the verdict.

    ``flip_y`` None uses the plugin's own ``FLIP_EASYEDA_Y`` constant, so the gate
    tests the convention the plugin ships; ``--flip-y`` overrides it for calibration.
    ``pro_fixtures`` reads the parts from the Pro-host recordings instead.
    """
    index = load_pro_index(pro_fixtures) if pro_fixtures is not None else None
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
            "centre": None,
            "verdict": None,
        }
        if pro_fixtures is not None and index is not None:
            record = load_pro_record(pro_fixtures, index, fp.lcsc)
        else:
            record = load_record(fixtures, fp.lcsc)
        if record is not None:
            pads = footprint_pads(fp)
            courtyard = fp.courtyard
            if fp.is_bottom:
                pads = mirror_y(pads)
                courtyard = None if courtyard is None else mirror_box(courtyard)
            row["package"] = record.package_name
            row["centre"] = pad_box_centre(pads)
            if flip_y is None:
                row["verdict"] = resolve_record(
                    pads, fp.footprint_name, record, kicad_courtyard=courtyard
                )
            else:
                # resolve_record has no flip_y override; --flip-y is a calibration
                # escape hatch (pinned by test_jlcfootprint_validate.py's direct
                # flip_y=False call), so that path keeps building the call by hand.
                row["verdict"] = resolve(
                    pads,
                    fp.footprint_name,
                    record.status,
                    record.package_name,
                    easyeda_pads_to_mm(record.pads, flip_y=flip_y),
                    record.symbol_pins,
                    marks=drawing_marks(
                        record.symbol_shapes,
                        record.footprint_shapes,
                        record.footprint_origin,
                    ),
                    kicad_courtyard=courtyard,
                )
        rows.append(row)
    return rows


def reference_key(reference: str) -> tuple:
    """Sort key that orders Q2 before Q10: letters, then the number, then any suffix."""
    match = re.match(r"^([A-Za-z_]*)(\d*)(.*)$", reference)
    letters, digits, rest = match.groups()  # the pattern matches every string
    return (letters.upper(), int(digits) if digits else -1, rest)


def evaluate(
    board: Path,
    fixtures: Path,
    flip_y: bool | None = None,
    pro_fixtures: Path | None = None,
) -> list[dict]:
    """Parse the board file and evaluate it, rows in reference order.

    pcbnew writes footprints in the order of their fresh internal ids, so a
    regenerated board lists the same parts in a different order; sorting keeps
    the report and the gate deterministic.
    """
    footprints = sorted(
        parse_kicad_pcb(str(board)), key=lambda fp: reference_key(fp.reference)
    )
    return evaluate_footprints(footprints, fixtures, flip_y, pro_fixtures)


def load_truth(path: Path) -> dict[str, str]:
    """Return ``{reference: observed_rotation text}`` from the truth CSV.

    Header names match case-insensitively, a UTF-8 BOM (what spreadsheets write) is
    tolerated, a missing cell reads as blank, and a missing column stops the run.
    """
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = {name.strip().lower(): name for name in reader.fieldnames or []}
        for wanted in ("reference", "observed_rotation"):
            if wanted not in columns:
                raise SystemExit(
                    f"{path}: no '{wanted}' column (header: {reader.fieldnames})"
                )
        truth: dict[str, str] = {}
        for row in reader:
            reference = (row.get(columns["reference"]) or "").strip()
            if reference:
                truth[reference] = (row.get(columns["observed_rotation"]) or "").strip()
        return truth


def load_truth_origins(path: Path) -> dict[str, tuple[float, float] | None] | None:
    """Return ``{reference: (dx, dy)}`` from the truth CSV, or None without the columns.

    The two columns are optional (spec 17.6), so a truth file written before M4
    still gates the rotations; an empty pair means the part must derive no origin,
    and a half-filled pair is a mistake in the file rather than a silent pass.
    """
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = {name.strip().lower(): name for name in reader.fieldnames or []}
        if "origin_dx_mm" not in columns or "origin_dy_mm" not in columns:
            return None
        origins: dict[str, tuple[float, float] | None] = {}
        for row in reader:
            reference = (row.get(columns["reference"]) or "").strip()
            if not reference:
                continue
            values = [
                (row.get(columns[f"origin_d{axis}_mm"]) or "").strip()
                for axis in ("x", "y")
            ]
            if not any(values):
                origins[reference] = None
                continue
            try:
                origins[reference] = (float(values[0]), float(values[1]))
            except ValueError as error:
                raise SystemExit(
                    f"{path}: {reference} has a non-numeric origin {values}"
                ) from error
        return origins


def origin_offset(row: dict) -> tuple[float, float] | None:
    """Return the derived origin's offset from the part's pad-box centre, or None.

    This is how far the CPL position moves when ``jlcfootprint.exact_origin`` is on,
    because upstream's ``get_position`` is that same pad box in board coordinates.
    """
    verdict: Verdict | None = row["verdict"]
    origin = None if verdict is None else verdict.origin
    centre = row.get("centre")
    if origin is None or centre is None:
        return None
    return (origin[0] - centre[0], origin[1] - centre[1])


def compare(
    rows: list[dict],
    truth: dict[str, str],
    origins: dict[str, tuple[float, float] | None] | None = None,
) -> list[tuple[dict, str]]:
    """Return ``(row, reason)`` for every part whose emitted rotation would disagree with JLC.

    A truth reference that is not on the board is reported too, so a typo in
    ``truth.csv`` cannot silently pass the gate.
    """
    failures: list[tuple[dict, str]] = []
    on_board = {row["reference"] for row in rows}
    for reference in sorted(set(truth) - on_board, key=reference_key):
        placeholder = {
            "reference": reference,
            "lcsc": "",
            "footprint": "",
            "placed": 0.0,
            "bottom": False,
            "package": "",
            "centre": None,
            "verdict": None,
        }
        failures.append((placeholder, "not on the board (typo in truth.csv?)"))
    for row in rows:
        if row["reference"] not in truth:
            continue
        observed = truth[row["reference"]]
        verdict: Verdict | None = row["verdict"]
        if origins is not None and row["reference"] in origins:
            reason = _origin_reason(
                origins[row["reference"]],
                None if verdict is None else verdict.origin,
            )
            if reason:
                failures.append((row, reason))
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
        try:
            observed_angle = float(observed) % 360
        except ValueError:
            failures.append((row, f"truth value {observed!r} is not a number"))
            continue
        expected = expected_cpl_rotation(row["placed"], row["bottom"], verdict.rotation)
        # A non-polar part (aligned by axis, or a marked inductor whose token only
        # places JLC's cosmetic pin-1 dot) is the same placement 180 degrees apart.
        modulus = 180 if verdict.non_polar else 360
        if expected % modulus != observed_angle % modulus:
            failures.append((row, f"would emit {expected:g}, JLC shows {observed}"))
    return failures


def _origin_reason(
    expected: tuple[float, float] | None, derived: tuple[float, float] | None
) -> str:
    """Return why a derived origin disagrees with its truth row, or "" when it agrees."""
    if expected is None:
        return (
            ""
            if derived is None
            else f"expected no derived origin, got ({derived[0]:.3f}, {derived[1]:.3f})"
        )
    if derived is None:
        return f"no derived origin; truth says ({expected[0]:.3f}, {expected[1]:.3f})"
    if any(abs(a - b) > ORIGIN_TOLERANCE_MM for a, b in zip(derived, expected)):
        return (
            f"origin ({derived[0]:.3f}, {derived[1]:.3f}) differs from truth "
            f"({expected[0]:.3f}, {expected[1]:.3f}) by more than "
            f"{ORIGIN_TOLERANCE_MM} mm"
        )
    return ""


def format_rows(rows: list[dict]) -> str:
    """Render one line per part."""
    header = (
        f"{'ref':<6} {'lcsc':<10} {'status':<8} {'fit':<17} {'rot':>4} {'name':>4} "
        f"{'orgdx':>6} {'orgdy':>6}  footprint / package / notes"
    )
    lines = [header]
    for row in rows:
        verdict = row["verdict"]
        if verdict is None:
            lines.append(
                f"{row['reference']:<6} {row['lcsc']:<10} {'no-fix':<8} {'':<17} {'':>4} {'':>4} "
                f"{'':>6} {'':>6}  {row['footprint']}"
            )
            continue
        rot = "" if verdict.rotation is None else str(verdict.rotation)
        name = "" if verdict.name_rotation is None else str(verdict.name_rotation)
        offset = origin_offset(row)
        # The origin as the preview sees it: how far it sits from the pad-box centre
        # the CPL emits today (spec 17.6), two decimals, blank without one.
        # -0.0 normalised to 0.0, so a sign of rounding noise cannot change the file.
        dx, dy = (
            ("", "")
            if offset is None
            else tuple(f"{round(value, 2) + 0.0:.2f}" for value in offset)
        )
        lines.append(
            f"{row['reference']:<6} {row['lcsc']:<10} {verdict.status:<8} {verdict.fit:<17} "
            f"{rot:>4} {name:>4} {dx:>6} {dy:>6}  {row['footprint']} / {row['package']} / {verdict.note_text}"
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
    parser.add_argument(
        "--report",
        type=Path,
        help="also write the table (without the truth summary) to this file",
    )
    parser.add_argument(
        "--pro-fixtures",
        type=Path,
        help="read the parts from Pro-host recordings (devices_*, footprint_*, symbol_*) instead",
    )
    args = parser.parse_args(argv)
    rows = evaluate(
        args.board, args.fixtures, True if args.flip_y else None, args.pro_fixtures
    )
    table = format_rows(rows)
    print(table)
    if args.report:
        args.report.write_text(table + "\n", encoding="utf-8")
    if not args.truth:
        return 0
    truth = load_truth(args.truth)
    failures = compare(rows, truth, load_truth_origins(args.truth))
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
