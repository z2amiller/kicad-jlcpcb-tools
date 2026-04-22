"""End-to-end validation: compare auto-derived rotation vs CPL ground truth.

Usage:
    python scripts/validate_board.py \
        --pcb /path/to/board.kicad_pcb \
        --cpl /path/to/CPL-board.csv \
        --cache-dir .easyeda_cache \
        [--limit N] [--only REF[,REF...]] [--lcsc-override REF=LCSC,...]

For every footprint with an LCSC code it:
  1. Extracts KiCad pad geometry (local footprint coords)
  2. Fetches EasyEDA pad geometry (disk-cached, rate-limited 1/sec)
  3. Runs solve_transform + assess_quality
  4. Compares our derived rotation to (cpl_rotation - kicad_placed_rotation) % 360
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from autorotation.easyeda_api import fetch_easyeda_pads, parse_easyeda_response
from autorotation.tests.kicad_pcb_parser import footprint_pad_dict, parse_kicad_pcb
from autorotation.quality import assess_quality
from autorotation.solver import solve_transform


RATE_LIMIT_SEC = 1.0


def load_cpl(cpl_path: Path) -> Dict[str, Tuple[float, float, float, str]]:
    """Return {ref: (mid_x, mid_y, rotation_deg, layer)} from a JLCPCB CPL CSV."""
    out: Dict[str, Tuple[float, float, float, str]] = {}
    with cpl_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ref = row["Designator"].strip()
            out[ref] = (
                float(row["Mid X"]),
                float(row["Mid Y"]),
                float(row["Rotation"]),
                row["Layer"].strip(),
            )
    return out


def fetch_with_cache(lcsc: str, cache_dir: Path, last_fetch: list):
    """Disk-cached EasyEDA fetch with global rate limit.

    last_fetch is a 1-element list used as a mutable timestamp ref.
    Returns a FootprintResult.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{lcsc}.json"
    if cache_file.exists():
        with cache_file.open(encoding="utf-8") as f:
            return parse_easyeda_response(json.load(f), lcsc)

    elapsed = time.monotonic() - last_fetch[0]
    if elapsed < RATE_LIMIT_SEC:
        time.sleep(RATE_LIMIT_SEC - elapsed)
    last_fetch[0] = time.monotonic()

    result = fetch_easyeda_pads(lcsc)
    # Re-fetch raw JSON so we can cache it. fetch_easyeda_pads doesn't expose
    # the raw dict; simplest to refetch via urllib here — but since we just
    # called it, instead write a minimal cache of the derived result.
    # (Refactor opportunity: have easyeda_api.fetch return raw json too.)
    if result.success:
        cache_file.write_text(
            json.dumps(
                {
                    "__cached_result__": True,
                    "success": result.success,
                    "lcsc_code": result.lcsc_code,
                    "package": result.package,
                    "title": result.title,
                    "pads": result.pads,
                }
            )
        )
    return result


def load_cached_result(cache_file: Path, lcsc: str):
    """Load a FootprintResult from our derived-result cache format."""
    from autorotation.easyeda_api import FootprintResult

    data = json.loads(cache_file.read_text())
    if data.get("__cached_result__"):
        return FootprintResult(
            success=data["success"],
            lcsc_code=data["lcsc_code"],
            package=data["package"],
            title=data["title"],
            pads=data["pads"],
            error=None,
        )
    return parse_easyeda_response(data, lcsc)


def get_easyeda(lcsc: str, cache_dir: Path, last_fetch: list):
    cache_file = cache_dir / f"{lcsc}.json"
    if cache_file.exists():
        return load_cached_result(cache_file, lcsc)

    elapsed = time.monotonic() - last_fetch[0]
    if elapsed < RATE_LIMIT_SEC:
        time.sleep(RATE_LIMIT_SEC - elapsed)
    last_fetch[0] = time.monotonic()

    result = fetch_easyeda_pads(lcsc)
    if result.success:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(
            json.dumps(
                {
                    "__cached_result__": True,
                    "success": result.success,
                    "lcsc_code": result.lcsc_code,
                    "package": result.package,
                    "title": result.title,
                    "pads": result.pads,
                }
            )
        )
    return result


def pad_geom_dict(fp_pad_dict: Dict[str, Tuple[float, float, float, float]]):
    """Identity — parser already returns the right (x,y,w,h) shape."""
    return dict(fp_pad_dict)


def pad_center_dict(fp_pad_dict: Dict[str, Tuple[float, float, float, float]]):
    """Project to {num: (x,y)} for the solver."""
    return {k: (v[0], v[1]) for k, v in fp_pad_dict.items()}


def easyeda_pads_to_dicts(result):
    """Convert FootprintResult.pads -> (centers_dict, geom_dict)."""
    centers = {}
    geoms = {}
    for p in result.pads:
        num = str(p["number"])
        centers[num] = (p["x"], p["y"])
        geoms[num] = (p["x"], p["y"], p["width"], p["height"])
    return centers, geoms


def angle_diff(a: float, b: float) -> float:
    """Smallest signed difference (a - b) wrapped to (-180, 180]."""
    d = (a - b + 180.0) % 360.0 - 180.0
    return d


def parse_overrides(s: Optional[str]) -> Dict[str, str]:
    if not s:
        return {}
    out = {}
    for pair in s.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pcb", type=Path, required=True)
    ap.add_argument("--cpl", type=Path, required=True)
    ap.add_argument("--cache-dir", type=Path, default=Path(".easyeda_cache"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--only", type=str, default=None, help="comma-sep refs")
    ap.add_argument("--lcsc-override", type=str, default=None,
                    help="comma-sep REF=LCSC pairs")
    args = ap.parse_args()

    only = set(r.strip() for r in args.only.split(",")) if args.only else None
    overrides = parse_overrides(args.lcsc_override)

    footprints = parse_kicad_pcb(args.pcb)
    cpl = load_cpl(args.cpl)

    last_fetch = [0.0]
    rows = []
    tier_counts: Dict[str, int] = {}
    match_count = 0
    total = 0

    for fp in footprints:
        if only and fp.reference not in only:
            continue

        lcsc = overrides.get(fp.reference) or fp.lcsc
        if not lcsc:
            continue
        if fp.reference not in cpl:
            continue

        if args.limit and total >= args.limit:
            break
        total += 1

        cpl_x, cpl_y, cpl_rot, cpl_layer = cpl[fp.reference]
        expected = (cpl_rot - fp.placed_rotation) % 360.0

        kicad_pads = footprint_pad_dict(fp)
        if not kicad_pads:
            rows.append((fp.reference, fp.footprint_name, lcsc,
                         fp.placed_rotation, cpl_rot, expected,
                         None, "no_kicad_pads", False))
            continue

        result = get_easyeda(lcsc, args.cache_dir, last_fetch)
        if not result.success:
            rows.append((fp.reference, fp.footprint_name, lcsc,
                         fp.placed_rotation, cpl_rot, expected,
                         None, f"fetch_fail:{result.error}", False))
            continue

        jlc_centers, jlc_geoms = easyeda_pads_to_dicts(result)
        if not jlc_centers:
            rows.append((fp.reference, fp.footprint_name, lcsc,
                         fp.placed_rotation, cpl_rot, expected,
                         None, "no_jlc_pads", False))
            continue

        transform = solve_transform(pad_center_dict(kicad_pads), jlc_centers)
        qa = assess_quality(pad_geom_dict(kicad_pads), jlc_geoms, transform)

        our_rot = transform.rotation_deg % 360.0
        diff = angle_diff(our_rot, expected)
        is_match = abs(diff) < 5.0  # degrees

        if is_match:
            match_count += 1
        tier_counts[qa.tier] = tier_counts.get(qa.tier, 0) + 1

        rows.append((fp.reference, fp.footprint_name, lcsc,
                     fp.placed_rotation, cpl_rot, expected,
                     our_rot, qa.tier, is_match))

    # Print table
    print()
    print(f"{'REF':<6} {'FOOTPRINT':<40} {'LCSC':<9} "
          f"{'KiCad°':>7} {'CPL°':>7} {'Exp°':>7} {'Our°':>7} "
          f"{'TIER':<24} {'MATCH'}")
    print("-" * 130)
    for r in rows:
        ref, fpn, lcsc, k_rot, c_rot, exp, our, tier, m = r
        our_s = f"{our:7.1f}" if our is not None else "    n/a"
        mark = "OK" if m else "--"
        print(f"{ref:<6} {fpn[:40]:<40} {lcsc:<9} "
              f"{k_rot:7.1f} {c_rot:7.1f} {exp:7.1f} {our_s} "
              f"{tier[:24]:<24} {mark}")

    print()
    print(f"Total evaluated: {total}")
    print(f"Matches (|Δ|<5°): {match_count} / {total}")
    print("Quality tiers:")
    for tier, n in sorted(tier_counts.items()):
        print(f"  {tier:<30} {n}")


if __name__ == "__main__":
    main()
