"""Sweep the private crawl: resolver verdicts on KiCad library twins versus the crawl's own rotations.

Usage: python3 scripts/sweep_crawl.py --db ~/Downloads/parts.db --top 150 [--csv out.csv]

For each of the most-shared EasyEDA footprints (puuids) that has an obvious KiCad
standard-library twin, run the resolver (KiCad library pads vs the crawled EasyEDA
footprint, symbol pin-1 polarity from the crawl) and compare its rotation with the
crawl's packages.rotation_correction.  Read-only; dev-time only; needs the KiCad
footprint libraries installed.

What it proves: the alignment logic against 174k parts' worth of footprints.  What
it does not: the crawl's blobs are almost all EasyEDA Pro text, parsed here by
``pro_pads`` (mils, Y up), so the plugin's classic-format path
(``parse_footprint_pads`` + ``easyeda_pads_to_mm`` and ``FLIP_EASYEDA_Y``) is
exercised only by the recorded live responses: the corner-case board and JLC's
preview are the check of that frame.  The QFN twin takes the first exposed-pad
variant KiCad offers, which is enough for rotation.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.easyeda_parse import SymbolPin, parse_footprint_pads  # noqa: E402
from jlcfootprint.geometry import Pad, easyeda_pads_to_mm  # noqa: E402
from jlcfootprint.resolver import resolve  # noqa: E402

# ``tests`` must resolve to this repository's package (ROOT is first on sys.path and
# upstream ships tests/__init__.py); an editable install elsewhere can also own that name.
from tests.jlcfootprint_support import (  # noqa: E402
    KICAD_FOOTPRINTS,
    library_pads,
    with_functions,
)

MIL_MM = 0.0254


def pro_pads(text: str) -> list[Pad]:
    """Parse EasyEDA Pro footprint text: ["PAD", id, 0, "", layer, number, x, y, rot, hole, [shape, w, h, ...], ...].

    Units are mils and the Y axis points up, so Y is flipped into KiCad's Y-down frame.
    """
    pads = []
    for line in text.split("\n"):
        if not line.startswith('["PAD"'):
            continue
        try:
            rec = json.loads(line)
            shape = rec[10]
            pads.append(
                Pad(
                    str(rec[5]),
                    float(rec[6]) * MIL_MM,
                    -float(rec[7]) * MIL_MM,
                    float(shape[1]) * MIL_MM,
                    float(shape[2]) * MIL_MM,
                    float(rec[8] or 0),
                )
            )
        except (ValueError, IndexError, TypeError):
            continue
    return pads


CHIP = {
    "0201": "0201_0603Metric",
    "0402": "0402_1005Metric",
    "0603": "0603_1608Metric",
    "0805": "0805_2012Metric",
    "1206": "1206_3216Metric",
    "1210": "1210_3225Metric",
    "2010": "2010_5025Metric",
    "2512": "2512_6332Metric",
}
ELEC = {"4": "4x5.4", "5": "5x5.4", "6.3": "6.3x7.7", "8": "8x10.5", "10": "10x10.5"}
TANT = {
    "CASE-A": "CP_EIA-3216-18_Kemet-A",
    "CASE-B": "CP_EIA-3528-21_Kemet-B",
    "CASE-C": "CP_EIA-6032-28_Kemet-C",
    "CASE-D": "CP_EIA-7343-31_Kemet-D",
}
DIODE_FUNCTIONS = {"1": "K", "2": "A"}


def twin(name: str) -> tuple[str, str, dict] | None:
    """Return (library, footprint, pin functions) for a package name with an obvious KiCad twin."""
    m = re.match(r"^(R|C|L|LED)(\d{4})(?:$|-)", name)
    if m and m.group(2) in CHIP:
        kind, size = m.group(1), CHIP[m.group(2)]
        return {
            "R": ("Resistor_SMD", f"R_{size}", {}),
            "C": ("Capacitor_SMD", f"C_{size}", {}),
            "L": ("Inductor_SMD", f"L_{size}", {}),
            "LED": ("LED_SMD", f"LED_{size}", DIODE_FUNCTIONS),
        }[kind]
    family = name.split("_", 1)[0]
    if family == "SOT-23-3":
        return ("Package_TO_SOT_SMD", "SOT-23", {})
    if family in (
        "SOT-23-5",
        "SOT-23-6",
        "SOT-89-3",
        "SOT-323-3",
        "SOT-363",
        "TO-252-2",
        "TO-263-2",
        "SOT-223-3",
        "TO-252-3",
    ):
        return (
            "Package_TO_SOT_SMD",
            {
                "SOT-323-3": "SOT-323_SC-70",
                "SOT-363": "SOT-363_SC-70-6",
                "SOT-223-3": "SOT-223-3_TabPin2",
                "TO-252-3": "TO-252-3_TabPin2",
            }.get(family, family),
            {},
        )
    if family in (
        "SMA",
        "SMB",
        "SMC",
        "SOD-123",
        "SOD-123F",
        "SOD-123FL",
        "SOD-323",
        "SOD-523",
        "SOD-882",
        "SMF",
        "SMAF",
        "SMBF",
    ):
        return ("Diode_SMD", "D_" + family, DIODE_FUNCTIONS)
    if family == "LL-34":
        return ("Diode_SMD", "D_MiniMELF", DIODE_FUNCTIONS)
    if family == "DO-214AC":
        return ("Diode_SMD", "D_SMA", DIODE_FUNCTIONS)
    if family == "DO-214AA":
        return ("Diode_SMD", "D_SMB", DIODE_FUNCTIONS)
    if family == "DO-214AB":
        return ("Diode_SMD", "D_SMC", DIODE_FUNCTIONS)
    m = re.match(r"^(SOIC|SOP)-(8|14|16)_L(\d+\.\d)-W(\d\.\d)-P1\.27", name)
    if m and m.group(4) == "3.9":
        return (
            "Package_SO",
            {
                "8": "SOIC-8_3.9x4.9mm_P1.27mm",
                "14": "SOIC-14_3.9x8.7mm_P1.27mm",
                "16": "SOIC-16_3.9x9.9mm_P1.27mm",
            }[m.group(2)],
            {},
        )
    m = re.match(r"^TSSOP-(8|14|16|20|24|28)_L(\d+\.\d)-W(\d\.\d)-P0\.65", name)
    if m and m.group(3) == "4.4":
        return (
            "Package_SO",
            {
                "8": "TSSOP-8_4.4x3mm_P0.65mm",
                "14": "TSSOP-14_4.4x5mm_P0.65mm",
                "16": "TSSOP-16_4.4x5mm_P0.65mm",
                "20": "TSSOP-20_4.4x6.5mm_P0.65mm",
                "24": "TSSOP-24_4.4x7.8mm_P0.65mm",
                "28": "TSSOP-28_4.4x9.7mm_P0.65mm",
            }[m.group(1)],
            {},
        )
    m = re.match(r"^MSOP-(8|10)_L3\.0-W3\.0-P(0\.65|0\.50)", name)
    if m:
        return (
            "Package_SO",
            {
                ("8", "0.65"): "MSOP-8_3x3mm_P0.65mm",
                ("10", "0.50"): "MSOP-10_3x3mm_P0.5mm",
            }.get((m.group(1), m.group(2)), ""),
            {},
        )
    m = re.match(
        r"^(QFN|DFN|WQFN|VQFN)-(\d+)_L(\d+\.\d)-W(\d+\.\d)-P(\d\.\d+)(?:-(TL|TR|BL|BR))?-EP",
        name,
    )
    if m:
        count, length, width, pitch = m.group(2), m.group(3), m.group(4), m.group(5)
        prefix = f"QFN-{count}-1EP_{length.rstrip('0').rstrip('.')}x{width.rstrip('0').rstrip('.')}mm_P{pitch.rstrip('0').rstrip('.')}mm"
        for candidate in sorted(
            (KICAD_FOOTPRINTS / "Package_DFN_QFN.pretty").glob(prefix + "*.kicad_mod")
        ):
            return ("Package_DFN_QFN", candidate.stem, {})
    m = re.match(r"^CAP-SMD_BD(\d+(?:\.\d)?)-", name)
    if m and m.group(1) in ELEC:
        return ("Capacitor_SMD", "CP_Elec_" + ELEC[m.group(1)], {})
    if family in TANT:
        return ("Capacitor_Tantalum_SMD", TANT[family], {})
    m = re.match(r"^DIP-(\d+)_L\d+\.\d-W(6\.\d|7\.\d)-P2\.54-LS7\.6", name)
    if m:
        return ("Package_DIP", f"DIP-{m.group(1)}_W7.62mm", {})
    return None


def main(argv: list[str] | None = None) -> int:
    """Run the sweep."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--db", type=Path, default=Path.home() / "Downloads" / "parts.db"
    )
    parser.add_argument("--top", type=int, default=150)
    parser.add_argument("--csv", type=Path)
    args = parser.parse_args(argv)
    if not KICAD_FOOTPRINTS.is_dir():
        print(
            f"no KiCad footprint libraries at {KICAD_FOOTPRINTS}; nothing would resolve"
        )
        return 1
    if not args.db.is_file():
        print(f"no database at {args.db}")
        return 1
    conn = sqlite3.connect(f"file:{args.db}?mode=ro&immutable=1", uri=True)
    rows = conn.execute(
        """select pu.puuid, pu.package_name, pu.observation_count, pu.pin1_polarity, pu.footprint_json,
                  pk.rotation_correction, pk.rotation_source, pk.parser_confidence
           from puuids pu left join packages pk on pk.package_name = pu.package_name
           order by pu.observation_count desc limit ?""",
        (args.top,),
    ).fetchall()
    out = []
    stats = {
        "rows": 0,
        "no_twin": 0,
        "pro_format": 0,
        "resolved": 0,
        "agree": 0,
        "disagree": 0,
        "unresolved": 0,
        "parts_agree": 0,
        "parts_total": 0,
    }
    for _puuid, name, count, polarity, blob, crawl_rot, crawl_src, crawl_conf in rows:
        stats["rows"] += 1
        pair = twin(name)
        if pair is None or not pair[1]:
            stats["no_twin"] += 1
            continue
        lib, fp, functions = pair
        if not (KICAD_FOOTPRINTS / f"{lib}.pretty" / f"{fp}.kicad_mod").exists():
            stats["no_twin"] += 1
            continue
        text = blob.decode() if isinstance(blob, bytes) else blob
        if text.lstrip().startswith("{"):
            data = json.loads(text)
            head = data.get("head") or {}
            raw, _ = parse_footprint_pads(
                data.get("shape") or [],
                float(head.get("x", 0) or 0),
                float(head.get("y", 0) or 0),
            )
            jlc = easyeda_pads_to_mm(raw)
        else:
            stats["pro_format"] += 1
            jlc = pro_pads(text)
        pins = [SymbolPin("1", polarity)] if polarity else []
        kicad = with_functions(library_pads(lib, fp), functions)
        v = resolve(kicad, f"{lib}:{fp}", "ok", name, jlc, pins, "seed-puuid")
        stats["parts_total"] += count
        agree = (
            v.rotation is not None and crawl_rot is not None and v.rotation == crawl_rot
        )
        if v.rotation is None:
            stats["unresolved"] += 1
        elif agree:
            stats["agree"] += 1
            stats["parts_agree"] += count
        else:
            stats["disagree"] += 1
        stats["resolved"] += int(v.rotation is not None)
        out.append(
            {
                "package": name,
                "parts": count,
                "twin": f"{lib}:{fp}",
                "status": v.status,
                "fit": v.fit,
                "resolver": v.rotation,
                "crawl": crawl_rot,
                "crawl_src": crawl_src,
                "crawl_conf": crawl_conf,
                "polarity": polarity or "",
                "light": v.polarity_light or "",
                "notes": v.note_text,
            }
        )
    print(stats)
    print(
        f"{'package':<44} {'parts':>6} {'status':<8} {'fit':<16} {'res':>4} {'crawl':>5} {'src':<12} twin / notes"
    )
    for r in out:
        if r["resolver"] is None:
            flag = "  <-- unresolved"
        else:
            flag = "" if r["resolver"] == r["crawl"] else "  <-- differs"
        print(
            f"{r['package'][:44]:<44} {r['parts']:>6} {r['status']:<8} {r['fit']:<16} {str(r['resolver']):>4} {str(r['crawl']):>5} {str(r['crawl_src']):<12} {r['twin'].split(':')[1]} / {r['notes'][:70]}{flag}"
        )
    if args.csv and out:
        with args.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(out[0]))
            writer.writeheader()
            writer.writerows(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
