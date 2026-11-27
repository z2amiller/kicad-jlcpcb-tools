r"""Check the live board reader against the board-file parser on one board.

Needs KiCad's bundled Python (it loads the board with pcbnew):

    /Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3 \
        tools/jlcfootprint/check_live_adapter.py tests/fixtures/jlcfootprint/boards/corner_case/corner_case.kicad_pcb

The board is read the way the plugin reads the open board: through
``kicad_backend``'s SWIG backend into records, then ``kicad_adapter`` into
resolver inputs.  For every footprint the reader's LCSC number, side, placed
rotation, footprint name and value, its pads (footprint frame, bottom parts
mirrored back, pad rotation modulo 180) and its courtyard box must equal the
validator's reading of the file and give the same verdict key; where a
recorded EasyEDA response exists the resolver, given each reading's own pads
and courtyard, must return the same status, fit, rotation and package origin
from both, the origin to 1 nanometre, because the origin is read out of the
same frame the pads are.  ``--pro-fixtures`` reads the parts from the Pro
recordings instead, which is how a board whose parts have no classic recording
reaches the resolver.  Exits 1 on any difference, and on a reference either
reading holds twice.  It is for boards whose footprints sit on quarter turns,
as the gate boards' do: the live courtyard is the box of each shape's box on
the board, which is larger for a footprint turned off a multiple of 90 degrees.
On the corner-case board all 47 footprints agree, and so do the 16 of the
second gate board.
``tests/test_jlcfootprint_live_reader_native.py`` runs the same comparison in
the KiCad 10 test lane.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb  # noqa: E402
from jlcfootprint.geometry import Pad, mirror_box, mirror_y  # noqa: E402
from jlcfootprint.kicad_adapter import board_parts  # noqa: E402
from jlcfootprint.resolver import Verdict  # noqa: E402
from jlcfootprint.resolving import resolve_record  # noqa: E402
from jlcfootprint.verdicts import verdict_key  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate_board import load_pro_index, load_pro_record, load_record  # noqa: E402

# The origin comes out of the same pads, so the two readings must agree exactly;
# a nanometre allows for float noise and nothing else.
ORIGIN_EPSILON_MM = 1e-6

DEFAULT_FIXTURES = ROOT / "tests" / "fixtures" / "jlcfootprint" / "easyeda"


@dataclass
class Comparison:
    """What comparing the two readings of one board found."""

    footprints: int = 0
    resolved: int = 0
    mismatches: int = 0
    verdict_differences: int = 0
    lines: list[str] = field(default_factory=list)

    @property
    def agrees(self) -> bool:
        """Return whether the two readings agree on every footprint."""
        return not (self.mismatches or self.verdict_differences)

    def summary(self) -> str:
        """Return the one-line result."""
        return (
            f"checked {self.footprints} footprints ({self.resolved} resolved): "
            f"{self.mismatches} mismatches, "
            f"{self.verdict_differences} verdict differences"
        )


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


def compare(
    board_file: Path,
    components: Iterable[Any],
    fixtures: Path = DEFAULT_FIXTURES,
    pro_fixtures: Optional[Path] = None,
) -> Comparison:
    """Compare the reader's parts from ``components`` with the file parser's reading of ``board_file``.

    ``components`` are the backend's records of the same board, every footprint.
    """
    result = Comparison()
    file_list = parse_kicad_pcb(str(board_file))
    live_list = board_parts(components)
    file_parts = {fp.reference: fp for fp in file_list}
    live = {part.reference: part for part in live_list}
    # The readings are matched by reference, so a reference held twice cannot be.
    for name, readings in (("file", file_list), ("live", live_list)):
        references = [reading.reference for reading in readings]
        for reference in sorted(set(references)):
            if references.count(reference) > 1:
                result.lines.append(
                    f"DUPLICATE {reference}: {name} reading holds it more than once"
                )
                result.mismatches += 1
    index = load_pro_index(pro_fixtures) if pro_fixtures else None
    result.footprints = len(file_parts.keys() | live.keys())
    for reference in sorted(live.keys() - file_parts.keys()):
        result.lines.append(f"EXTRA {reference}: the file parser did not read it")
        result.mismatches += 1
    for reference, fp in sorted(file_parts.items()):
        part = live.get(reference)
        if part is None:
            result.lines.append(f"MISSING {reference}: the reader did not read it")
            result.mismatches += 1
            continue
        file_facts = (
            fp.lcsc or "",
            fp.is_bottom,
            round(fp.placed_rotation % 360, 6),
            fp.footprint_name,
            fp.value,
        )
        live_facts = (
            part.lcsc,
            part.is_bottom,
            round(part.placed_rotation % 360, 6),
            part.footprint_name,
            part.value,
        )
        if file_facts != live_facts:
            result.mismatches += 1
            result.lines.append(
                f"PART {reference}: (lcsc, bottom, rotation, footprint, value)"
                f" file {file_facts} live {live_facts}"
            )
        pads = footprint_pads(fp)
        courtyard = fp.courtyard
        if fp.is_bottom:
            pads = mirror_y(pads)
            courtyard = None if courtyard is None else mirror_box(courtyard)
        if (
            pad_key(pads) != pad_key(part.pads)
            or verdict_key(pads) != part.footprint_hash
        ):
            result.mismatches += 1
            result.lines.append(
                f"MISMATCH {reference} {fp.footprint_name} bottom={fp.is_bottom}"
            )
            file_rows, live_rows = pad_key(pads), pad_key(part.pads)
            for file_row, live_row in zip(file_rows, live_rows):
                if file_row != live_row:
                    result.lines.append(f"   file {file_row}\n   live {live_row}")
            if len(file_rows) != len(live_rows):
                result.lines.append(
                    f"   file {len(file_rows)} pads, live {len(live_rows)}"
                )
            if file_rows == live_rows:
                result.lines.append(
                    f"   key: file {verdict_key(pads)} live {part.footprint_hash}"
                )
        if (courtyard is None) != (part.courtyard is None) or (
            courtyard is not None
            and any(abs(a - b) > 1e-4 for a, b in zip(courtyard, part.courtyard))
        ):
            result.mismatches += 1
            result.lines.append(
                f"COURTYARD {reference} {fp.footprint_name} bottom={fp.is_bottom}:"
                f" file {courtyard} live {part.courtyard}"
            )
        if not fp.lcsc:
            continue
        if index is not None:
            record = load_pro_record(pro_fixtures, index, fp.lcsc)
        else:
            record = load_record(fixtures, fp.lcsc)
        if record is None or record.status != "ok":
            continue
        result.resolved += 1
        from_file = resolve_record(pads, fp.footprint_name, record, courtyard)
        from_live = resolve_record(
            part.pads, part.footprint_name, record, part.courtyard
        )
        file_verdict = (from_file.status, from_file.rotation, from_file.fit)
        live_verdict = (from_live.status, from_live.rotation, from_live.fit)
        if file_verdict != live_verdict:
            result.verdict_differences += 1
            result.lines.append(
                f"VERDICT {reference}: file {file_verdict} live {live_verdict}"
            )
        if origin_differs(from_file, from_live):
            result.verdict_differences += 1
            result.lines.append(
                f"ORIGIN {reference}: file {from_file.origin} live {from_live.origin}"
            )
    return result


class _Editor:
    """The pcbnew module with a loaded board standing in for the editor's."""

    def __init__(self, pcbnew: Any, board: Any) -> None:
        self._pcbnew = pcbnew
        self._board = board

    def GetBoard(self) -> Any:
        """Return the loaded board."""
        return self._board

    def __getattr__(self, name: str) -> Any:
        """Reach every other pcbnew function."""
        return getattr(self._pcbnew, name)


def read_components(pcbnew: Any, board_file: Path) -> tuple[Any, ...]:
    """Load a board with pcbnew and read its footprints through the SWIG backend."""
    from kicad_backend.swig import SwigBackend  # noqa: PLC0415

    board = pcbnew.LoadBoard(str(board_file))
    if board is None:
        raise RuntimeError(f"pcbnew could not open {board_file}")
    return SwigBackend(_Editor(pcbnew, board)).snapshot().components


def main(argv: Optional[list[str]] = None) -> int:
    """Compare the live reader with the file parser on every footprint of the board."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("board", type=Path)
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument(
        "--pro-fixtures",
        type=Path,
        help="read the parts from Pro-host recordings instead of the classic fixtures",
    )
    args = parser.parse_args(argv)
    import pcbnew  # noqa: PLC0415 -- KiCad's Python only

    result = compare(
        args.board,
        read_components(pcbnew, args.board),
        args.fixtures,
        args.pro_fixtures,
    )
    for line in result.lines:
        print(line)
    print(result.summary())
    return 0 if result.agrees else 1


if __name__ == "__main__":
    sys.exit(main())
