r"""Generate the two-variant board for the variant read check (run with KiCad's bundled Python).

    /Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3 \\
        scripts/make_variant_board.py [--out BOARD]

Loads five footprints from KiCad's library onto the corner-case board's blank, sets
each one's Default LCSC field, adds the named variants A and B and gives each
footprint the per-variant part numbers of ``PARTS``: an inherited number, an
explicit other part, an explicit blank, and a part variant B does not place.  The
board is written to scripts/variant_board/variant_board.kicad_pcb by default;
``scripts/check_variant_read.py`` reads it back through the check (spec 18.6).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
BLANK = HERE / "corner_case" / "blank.kicad_pcb"
OUT = HERE / "variant_board" / "variant_board.kicad_pcb"
LIBRARY_ROOT = Path("/Applications/KiCad/KiCad.app/Contents/SharedSupport/footprints")
VARIANTS = ("A", "B")
PITCH_MM = 12.0
MARGIN_MM = 10.0

# reference, library, footprint, value, Default LCSC, and per named variant either
# None (inherit Default), a part number, "" (an explicit blank) or "DNP" (inherit
# the number, do not place).  The expected read is derived from this table alone.
PARTS = (
    ("Q1", "Package_TO_SOT_SMD", "SOT-23", "2SA812", "C2132", {"A": None, "B": "DNP"}),
    (
        "LED1",
        "LED_SMD",
        "LED_0603_1608Metric",
        "KT-0603R",
        "C2286",
        {"A": None, "B": "C50494"},
    ),
    (
        "C1",
        "Capacitor_SMD",
        "CP_Elec_6.3x7.7",
        "100u",
        "C134220",
        {"A": "C3338", "B": "C88744"},
    ),
    ("R2", "Resistor_SMD", "R_0603_1608Metric", "10k", "C25804", {"A": "", "B": None}),
    ("D4", "Diode_SMD", "D_SOD-123", "1N4148W", "C81598", {"A": None, "B": None}),
)


def expected_numbers() -> dict[str, dict[str, str]]:
    """Return {variant: {reference: the LCSC the check must read}}, Default as ""."""
    numbers: dict[str, dict[str, str]] = {"": {}}
    for name in VARIANTS:
        numbers[name] = {}
    for reference, _lib, _footprint, _value, default, overrides in PARTS:
        numbers[""][reference] = default
        for name in VARIANTS:
            override = overrides[name]
            numbers[name][reference] = (
                default if override in (None, "DNP") else override
            )
    return numbers


def place(pcbnew, board, row: tuple, index: int) -> None:
    """Load one footprint, add it to the board and give it its variant records."""
    reference, lib, name, value, default, overrides = row
    footprint = pcbnew.FootprintLoad(str(LIBRARY_ROOT / f"{lib}.pretty"), name)
    if footprint is None:
        raise SystemExit(f"footprint not found: {lib}:{name}")
    footprint.SetFPID(pcbnew.LIB_ID(lib, name))
    footprint.SetReference(reference)
    footprint.SetValue(value)
    board.Add(footprint)
    footprint.SetField("LCSC", default)
    footprint.SetPosition(pcbnew.VECTOR2I_MM(MARGIN_MM + index * PITCH_MM, MARGIN_MM))
    for variant_name in VARIANTS:
        override = overrides[variant_name]
        if override is None:
            continue
        variant = footprint.AddVariant(variant_name)
        if override == "DNP":
            variant.SetDNP(True)
        else:
            variant.SetFieldValue("LCSC", override)


def main(argv: list[str] | None = None) -> int:
    """Generate the board."""
    import pcbnew  # noqa: PLC0415 -- KiCad's Python only; tests import PARTS without it

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--blank", type=Path, default=BLANK)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args(argv)
    board = pcbnew.LoadBoard(str(args.blank))
    if board is None:
        raise SystemExit(f"could not load {args.blank}")
    for name in VARIANTS:
        board.AddVariant(name)
    for index, row in enumerate(PARTS):
        place(pcbnew, board, row, index)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pcbnew.SaveBoard(str(args.out), board)
    print(
        f"wrote {args.out} with {len(PARTS)} parts and variants {', '.join(VARIANTS)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
