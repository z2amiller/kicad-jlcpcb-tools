r"""Check the live pcbnew adapter against the board-file parser on one board.

Needs KiCad's bundled Python (it imports pcbnew):

    /Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3 \\
        scripts/check_live_adapter.py scripts/corner_case/corner_case.kicad_pcb

For every footprint the adapter's pads (footprint frame, bottom parts un-mirrored,
pad rotation modulo 180) must equal the validator's file pads and give the same pad
hash; where a recorded EasyEDA response exists the resolver must return the same
status, fit and rotation from both.  Exits 1 on any difference.  This is how the
adapter's frame was confirmed on 2026-09-16 (47 of 47 parts).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb  # noqa: E402
from jlcfootprint.easyeda_parse import parse_component_response  # noqa: E402
from jlcfootprint.geometry import (  # noqa: E402
    Pad,
    easyeda_pads_to_mm,
    mirror_y,
    pad_hash,
)
from jlcfootprint.kicad_adapter import board_parts  # noqa: E402
from jlcfootprint.resolver import resolve  # noqa: E402

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


def main(argv: list[str] | None = None) -> int:
    """Compare the live adapter with the file parser on every footprint of the board."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("board", type=Path)
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    args = parser.parse_args(argv)
    import pcbnew  # noqa: PLC0415

    file_parts = {fp.reference: fp for fp in parse_kicad_pcb(str(args.board))}
    live = {
        part.reference: part
        for part in board_parts(pcbnew.LoadBoard(str(args.board)), pcbnew=pcbnew)
    }
    mismatches = verdict_differences = 0
    for reference, fp in sorted(file_parts.items()):
        part = live.get(reference)
        if part is None:
            print(f"MISSING {reference}: the adapter did not read it")
            mismatches += 1
            continue
        pads = footprint_pads(fp)
        if fp.is_bottom:
            pads = mirror_y(pads)
        if pad_key(pads) != pad_key(part.pads) or pad_hash(pads) != part.footprint_hash:
            mismatches += 1
            print(f"MISMATCH {reference} {fp.footprint_name} bottom={fp.is_bottom}")
            for file_row, live_row in zip(pad_key(pads), pad_key(part.pads)):
                if file_row != live_row:
                    print(f"   file {file_row}\n   live {live_row}")
        fixture = args.fixtures / f"{fp.lcsc}.json"
        if not fp.lcsc or not fixture.exists():
            continue
        record = parse_component_response(
            json.loads(fixture.read_text(encoding="utf-8")), fp.lcsc
        )
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
    print(
        f"checked {len(file_parts)} footprints: {mismatches} pad-set mismatches, "
        f"{verdict_differences} verdict differences"
    )
    return 1 if mismatches or verdict_differences else 0


if __name__ == "__main__":
    sys.exit(main())
