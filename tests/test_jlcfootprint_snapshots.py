"""The recorded KiCad footprint snapshots parse, and match the installed library when present."""

import json

import pytest

from jlcfootprint.geometry import Pad
from tests.jlcfootprint_support import (
    KICAD_FOOTPRINTS,
    KICAD_SNAPSHOTS,
    installed_library_pads,
    library_pads,
)

SNAPSHOTS = sorted(KICAD_SNAPSHOTS.glob("*.json")) if KICAD_SNAPSHOTS.is_dir() else []


def test_snapshots_exist():
    """The library-backed tests need their snapshots committed."""
    assert SNAPSHOTS, "no snapshots under tests/fixtures/jlcfootprint/kicad"


@pytest.mark.parametrize("path", SNAPSHOTS, ids=lambda p: p.stem)
def test_snapshot_parses_to_pads(path):
    """Every snapshot names its footprint and loads as Pads through library_pads."""
    data = json.loads(path.read_text(encoding="utf-8"))
    assert path.stem == f"{data['library']}__{data['name']}"
    pads = library_pads(data["library"], data["name"])
    assert pads and all(isinstance(pad, Pad) for pad in pads)
    assert all(pad.width > 0 and pad.height > 0 for pad in pads)


@pytest.mark.skipif(
    not KICAD_FOOTPRINTS.is_dir(), reason="KiCad libraries not installed"
)
@pytest.mark.parametrize("path", SNAPSHOTS, ids=lambda p: p.stem)
def test_snapshot_matches_installed_library(path):
    """A KiCad library update shows up here; re-record with scripts/snapshot_kicad_footprints.py --all."""
    data = json.loads(path.read_text(encoding="utf-8"))
    assert library_pads(data["library"], data["name"]) == installed_library_pads(
        data["library"], data["name"]
    )
