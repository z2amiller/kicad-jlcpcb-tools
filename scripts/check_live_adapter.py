r"""Check the live pcbnew adapter against the board-file parser on one board.

Needs KiCad's bundled Python (it imports pcbnew):

    /Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3 \\
        scripts/check_live_adapter.py scripts/corner_case/corner_case.kicad_pcb

For every footprint the adapter's pads (footprint frame, bottom parts un-mirrored,
pad rotation modulo 180) and its courtyard box must equal the validator's file pads
and box and give the same verdict key; where a recorded EasyEDA response exists the
resolver must return the same status, fit, rotation and package origin from both,
the last to 1 nanometre (spec 17.6: the origin is read out of the same frame the
pads are).  ``--pro-fixtures`` reads the parts from the Pro recordings instead, which
is how the boards whose parts have no classic fixture reach the resolver.  Exits 1 on
any difference.  This is how the adapter's frame was confirmed on 2026-09-16 (47 of 47
parts) and its courtyard reader on 2026-09-17.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb  # noqa: E402
from jlcfootprint.geometry import (  # noqa: E402
    Pad,
    easyeda_pads_to_mm,
    mirror_box,
    mirror_y,
)
from jlcfootprint.kicad_adapter import board_parts, verdict_key  # noqa: E402
from jlcfootprint.resolver import Verdict, resolve  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate_board import load_pro_index, load_pro_record, load_record  # noqa: E402

# The origin comes out of the same pads, so the two readings must agree exactly;
# a nanometre allows for float noise and nothing else.
ORIGIN_EPSILON_MM = 1e-6

DEFAULT_FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"


def pad_key(pads: list[Pad]) -> list[tuple]:
    """Return the pads as comparable rows (positions and sizes to 0.1 um, angle modulo 180)."""
    return sorted(
        (
            p.number,
            round(p.x, 4),
            round(p.y, 4),
            round(p.width, 4),
            round(p.height, 4),
            round(p.rotation % 180, 1),
            p.pin_function,
        )
        for p in pads
    )


def origin_differs(from_file: Verdict, from_live: Verdict) -> bool:
    """Return whether the two readings put JLC's package origin in different places."""
    one, other = from_file.origin, from_live.origin
    if (one is None) != (other is None):
        return True
    return one is not None and any(
        abs(a - b) > ORIGIN_EPSILON_MM for a, b in zip(one, other)
    )


def main(argv: list[str] | None = None) -> int:
    """Compare the live adapter with the file parser on every footprint of the board."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("board", type=Path)
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument(
        "--pro-fixtures",
        type=Path,
        help="read the parts from Pro-host recordings instead of the classic fixtures",
    )
    args = parser.parse_args(argv)
    import pcbnew  # noqa: PLC0415

    file_parts = {fp.reference: fp for fp in parse_kicad_pcb(str(args.board))}
    live = {
        part.reference: part
        for part in board_parts(pcbnew.LoadBoard(str(args.board)), pcbnew=pcbnew)
    }
    index = load_pro_index(args.pro_fixtures) if args.pro_fixtures else None
    mismatches = verdict_differences = 0
    resolved = 0
    for reference, fp in sorted(file_parts.items()):
        part = live.get(reference)
        if part is None:
            print(f"MISSING {reference}: the adapter did not read it")
            mismatches += 1
            continue
        pads = footprint_pads(fp)
        courtyard = fp.courtyard
        if fp.is_bottom:
            pads = mirror_y(pads)
            courtyard = None if courtyard is None else mirror_box(courtyard)
        if (
            pad_key(pads) != pad_key(part.pads)
            or verdict_key(pads) != part.footprint_hash
        ):
            mismatches += 1
            print(f"MISMATCH {reference} {fp.footprint_name} bottom={fp.is_bottom}")
            for file_row, live_row in zip(pad_key(pads), pad_key(part.pads)):
                if file_row != live_row:
                    print(f"   file {file_row}\n   live {live_row}")
        if (courtyard is None) != (part.courtyard is None) or (
            courtyard is not None
            and any(abs(a - b) > 1e-4 for a, b in zip(courtyard, part.courtyard))
        ):
            mismatches += 1
            print(
                f"COURTYARD {reference} {fp.footprint_name} bottom={fp.is_bottom}:"
                f" file {courtyard} live {part.courtyard}"
            )
        if not fp.lcsc:
            continue
        if index is not None:
            record = load_pro_record(args.pro_fixtures, index, fp.lcsc)
        else:
            record = load_record(args.fixtures, fp.lcsc)
        if record is None or record.status != "ok":
            continue
        resolved += 1
        jlc = easyeda_pads_to_mm(record.pads)
        from_file = resolve(
            pads,
            fp.footprint_name,
            record.status,
            record.package_name,
            jlc,
            record.symbol_pins,
        )
        from_live = resolve(
            part.pads,
            part.footprint_name,
            record.status,
            record.package_name,
            jlc,
            record.symbol_pins,
        )
        if (from_file.status, from_file.rotation, from_file.fit) != (
            from_live.status,
            from_live.rotation,
            from_live.fit,
        ):
            verdict_differences += 1
            print(
                f"VERDICT {reference}: file {(from_file.status, from_file.rotation, from_file.fit)}"
                f" live {(from_live.status, from_live.rotation, from_live.fit)}"
            )
        if origin_differs(from_file, from_live):
            verdict_differences += 1
            print(
                f"ORIGIN {reference}: file {from_file.origin} live {from_live.origin}"
            )
    print(
        f"checked {len(file_parts)} footprints ({resolved} resolved): "
        f"{mismatches} pad-set mismatches, {verdict_differences} verdict differences"
    )
    return 1 if mismatches or verdict_differences else 0


if __name__ == "__main__":
    sys.exit(main())
