"""Generate the corner-case validation board (run with KiCad's bundled Python).

    /Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3 \
        scripts/make_corner_case_board.py

Reads scripts/corner_case/cases.csv, loads each footprint from KiCad's library,
places the parts on a grid at the requested rotation and side, sets the LCSC
field and any pin functions, draws the outline and writes
scripts/corner_case/corner_case.kicad_pcb.  Spec section 11.
"""

from __future__ import annotations

import csv
from pathlib import Path
import sys

import pcbnew

HERE = Path(__file__).resolve().parent
CASES = HERE / "corner_case" / "cases.csv"
BLANK = HERE / "corner_case" / "blank.kicad_pcb"
OUT = HERE / "corner_case" / "corner_case.kicad_pcb"
LIBRARY_ROOT = Path("/Applications/KiCad/KiCad.app/Contents/SharedSupport/footprints")
COLUMNS = 6
PITCH_MM = 12.0
MARGIN_MM = 10.0


def load_cases() -> list[dict]:
    """Return the non-empty rows of cases.csv."""
    with CASES.open(newline="", encoding="utf-8") as handle:
        return [row for row in csv.DictReader(handle) if row["reference"].strip()]


def place(board, row: dict, index: int) -> None:
    """Load one footprint, add it to the board and configure it from its row."""
    footprint = pcbnew.FootprintLoad(
        str(LIBRARY_ROOT / f"{row['lib']}.pretty"), row["footprint"]
    )
    if footprint is None:
        raise SystemExit(f"footprint not found: {row['lib']}:{row['footprint']}")
    footprint.SetFPID(pcbnew.LIB_ID(row["lib"], row["footprint"]))
    footprint.SetReference(row["reference"])
    footprint.SetValue(row["value"])
    board.Add(footprint)  # before Flip, or pcbnew segfaults
    footprint.SetField("LCSC", row["lcsc"])
    x = MARGIN_MM + (index % COLUMNS) * PITCH_MM
    y = MARGIN_MM + (index // COLUMNS) * PITCH_MM
    footprint.SetPosition(pcbnew.VECTOR2I_MM(x, y))
    if row["layer"].strip().upper() == "B":
        footprint.Flip(footprint.GetPosition(), pcbnew.FLIP_DIRECTION_TOP_BOTTOM)
    footprint.SetOrientationDegrees(float(row["rotation"] or 0))
    functions = dict(
        item.split("=", 1) for item in row["pinfunctions"].split(";") if "=" in item
    )
    for pad in footprint.Pads():
        function = functions.get(str(pad.GetNumber()))
        if function:
            pad.SetPinFunction(function)


def outline(board, count: int) -> None:
    """Draw the Edge.Cuts rectangle around the grid."""
    rows = (count + COLUMNS - 1) // COLUMNS
    width = 2 * MARGIN_MM + (COLUMNS - 1) * PITCH_MM
    height = 2 * MARGIN_MM + (rows - 1) * PITCH_MM
    rect = pcbnew.PCB_SHAPE(board)
    rect.SetShape(pcbnew.SHAPE_T_RECT)
    rect.SetStart(pcbnew.VECTOR2I_MM(0, 0))
    rect.SetEnd(pcbnew.VECTOR2I_MM(width, height))
    rect.SetLayer(pcbnew.Edge_Cuts)
    rect.SetWidth(pcbnew.FromMM(0.1))
    board.Add(rect)


def main() -> int:
    """Generate the board."""
    board = pcbnew.LoadBoard(str(BLANK))
    if board is None:
        raise SystemExit(f"could not load {BLANK}")
    cases = load_cases()
    for index, row in enumerate(cases):
        place(board, row, index)
    outline(board, len(cases))
    pcbnew.SaveBoard(str(OUT), board)
    print(f"wrote {OUT} with {len(cases)} parts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
