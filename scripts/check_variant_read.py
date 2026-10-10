r"""Check that the JLC footprint check reads each variant's part numbers (spec 18.6).

Needs KiCad's bundled Python (it imports pcbnew and the plugin's wx-side facade):

    /Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3 \\
        scripts/check_variant_read.py [scripts/variant_board/variant_board.kicad_pcb]

Loads the generated two-variant board with pcbnew, reads it through upstream's
``VariantNativeAdapter`` and, for every variant as the output variant, builds the
check with the plugin's own ``create_footprint_check`` and calls its ``read_board``:
each footprint's part number must be the one ``make_variant_board.PARTS`` gives that
variant, and the pads must come from the footprint.  It also prints what each output
prefetches for the other variants.  The cache and the verdict store go to a
temporary directory; nothing is fetched.  Exits 1 on any difference.
"""

from __future__ import annotations

import argparse
import importlib
from pathlib import Path
import sys
import tempfile
import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_variant_board import LIBRARY_ROOT, OUT, PARTS, expected_numbers  # noqa: E402

PACKAGE = "variant_read_check"


class PcbnewShim:
    """pcbnew with ``GetBoard`` answering the loaded board, as the open editor would."""

    def __init__(self, pcbnew, board):
        self._pcbnew = pcbnew
        self._board = board

    def GetBoard(self):  # noqa: N802 -- pcbnew's spelling
        """Return the board this check loaded."""
        return self._board

    def __getattr__(self, name):
        """Answer every other pcbnew name from the real module."""
        return getattr(self._pcbnew, name)


def load_plugin():
    """Import the plugin tree as a package without running its ``__init__``."""
    package = types.ModuleType(PACKAGE)
    package.__path__ = [str(ROOT)]
    sys.modules[PACKAGE] = package
    native = importlib.import_module(f"{PACKAGE}.variant.native")
    facade = importlib.import_module(f"{PACKAGE}.jlc_footprint_check")
    return native, facade


def main(argv: list[str] | None = None) -> int:
    """Read the board once per output variant and compare with the generator's table."""
    import pcbnew  # noqa: PLC0415 -- KiCad's Python only

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("board", type=Path, nargs="?", default=OUT)
    args = parser.parse_args(argv)
    native, facade = load_plugin()
    board = pcbnew.LoadBoard(str(args.board))
    if board is None:
        raise SystemExit(f"could not load {args.board}")
    snapshot = native.VariantNativeAdapter(board, "variant-read-check").snapshot()
    expected = expected_numbers()
    pads = {
        reference: len(
            pcbnew.FootprintLoad(str(LIBRARY_ROOT / f"{lib}.pretty"), name).Pads()
        )
        for reference, lib, name, _value, _default, _overrides in PARTS
    }
    differences = 0
    work = Path(tempfile.mkdtemp(prefix="variant-read-"))
    for variant in snapshot.variants:
        session = types.SimpleNamespace(snapshot=snapshot, output_variant=variant.name)
        window = types.SimpleNamespace(
            library=types.SimpleNamespace(datadir=str(work)),
            store=types.SimpleNamespace(dbfile=str(work / "project.db")),
            settings={},
            _variant_controller=types.SimpleNamespace(session=session),
        )
        check = facade.create_footprint_check(window, PcbnewShim(pcbnew, board))
        placed = {
            part.reference: part.pop for part in snapshot.for_variant(variant.name)
        }
        read = sorted(check.read_board(), key=lambda part: part.reference)
        cells = []
        for part in read:
            want = expected[variant.name].get(part.reference)
            bad = part.lcsc != want or len(part.pads) != pads[part.reference]
            differences += bad
            cells.append(
                f"{part.reference} {part.lcsc or '(none)'}"
                + ("" if placed[part.reference] else " (not placed)")
                + (f" [expected {want or '(none)'}]" if bad else "")
            )
        print(f"variant {variant.label}: " + ", ".join(cells))
        prefetch = check.other_lcscs()
        print(
            f"  prefetch for {variant.label}: "
            + (
                ", ".join(
                    f"{reference} {{{', '.join(sorted(codes))}}}"
                    for reference, codes in sorted(prefetch.items())
                )
                or "none"
            )
        )
        check.stop()
    count = len(snapshot.variants) * len(PARTS)
    print(
        f"read {len(snapshot.variants)} variants x {len(PARTS)} footprints "
        f"({count} part numbers): {differences} difference(s)"
    )
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
