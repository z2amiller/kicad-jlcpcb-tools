"""Headless integration tests for KiCad pcbnew SWIG bindings.

These tests are intentionally small sanity checks:
- load/save a board via pcbnew
- enumerate footprints
- assign/read LCSC-like fields on a real SWIG footprint
- persist a fake enrichment field for later plugin-side use
"""

import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import types

import pytest

pytestmark = pytest.mark.kicad_integration

pcbnew = pytest.importorskip("pcbnew")
helpers = pytest.importorskip("helpers")
kicad_drc = pytest.importorskip("kicad_drc")

get_lcsc_value = helpers.get_lcsc_value
set_lcsc_value = helpers.set_lcsc_value


def _load_store_module():
    """Load store.py under a synthetic package so relative imports resolve in tests."""
    root = Path(__file__).resolve().parent.parent
    pkg = types.ModuleType("kicadplugin")
    pkg.__path__ = [str(root)]
    sys.modules["kicadplugin"] = pkg
    sys.modules["kicadplugin.helpers"] = helpers

    spec = importlib.util.spec_from_file_location("kicadplugin.store", root / "store.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "kicadplugin"
    sys.modules["kicadplugin.store"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _new_board():
    """Create an empty board using whichever constructor the KiCad version exposes."""
    board_cls = getattr(pcbnew, "BOARD", None)
    if board_cls is not None:
        return board_cls()

    create_empty_board = getattr(pcbnew, "CreateEmptyBoard", None)
    if create_empty_board is not None:
        return create_empty_board()

    pytest.skip("No BOARD/CreateEmptyBoard API in this pcbnew build")
    raise AssertionError("unreachable")


def _new_footprint(board):
    """Create a footprint object across supported KiCad SWIG class names."""
    for class_name in ("FOOTPRINT", "PCB_FOOTPRINT", "MODULE"):
        cls = getattr(pcbnew, class_name, None)
        if cls is None:
            continue
        try:
            return cls(board)
        except TypeError:
            return cls()

    pytest.skip("No FOOTPRINT/PCB_FOOTPRINT/MODULE API in this pcbnew build")
    raise AssertionError("unreachable")


def _board_footprints(board):
    """Return footprints list across SWIG API variants."""
    if hasattr(board, "GetFootprints"):
        return list(board.GetFootprints())
    if hasattr(board, "GetModules"):
        return list(board.GetModules())
    pytest.skip("No GetFootprints/GetModules API in this pcbnew build")
    raise AssertionError("unreachable")


def _add_footprint(board, footprint):
    """Attach footprint to board across SWIG API variants."""
    if hasattr(board, "Add"):
        board.Add(footprint)
        return
    if hasattr(board, "AddFootprint"):
        board.AddFootprint(footprint)
        return
    pytest.skip("No Add/AddFootprint API in this pcbnew build")
    raise AssertionError("unreachable")


def _save_board(path: Path, board):
    """Persist board to disk across SWIG API variants."""
    if hasattr(pcbnew, "SaveBoard"):
        pcbnew.SaveBoard(str(path), board)
        return
    if hasattr(board, "Save"):
        board.Save(str(path))
        return
    pytest.skip("No SaveBoard/board.Save API in this pcbnew build")
    raise AssertionError("unreachable")


def _set_reference_value(footprint, reference: str = "R1", value: str = "10k"):
    """Populate common footprint identity fields when available."""
    if hasattr(footprint, "SetReference"):
        footprint.SetReference(reference)
    if hasattr(footprint, "SetValue"):
        footprint.SetValue(value)


def _get_named_field_text(footprint, name: str) -> str:
    """Read a footprint field/property by name across SWIG API variants."""
    if hasattr(footprint, "GetFields"):
        for field in footprint.GetFields():
            if field.GetName() == name:
                return field.GetText()
    if hasattr(footprint, "GetProperties"):
        return str(footprint.GetProperties().get(name, ""))
    return ""


def _create_round_trip_board(tmp_path: Path):
    """Create, save, and reload a board with one footprint."""
    board = _new_board()
    footprint = _new_footprint(board)
    _set_reference_value(footprint)
    _add_footprint(board, footprint)

    board_path = tmp_path / "swig_roundtrip.kicad_pcb"
    _save_board(board_path, board)

    loaded = pcbnew.LoadBoard(str(board_path))
    return loaded, _board_footprints(loaded)


def _fixture_board_path(*parts: str) -> Path:
    """Build an absolute path to a checked-in board fixture."""
    return Path(__file__).parent / "fixtures" / Path(*parts)


def _fixture_manifest() -> dict:
    """Load fixture manifest from tests/fixtures."""
    manifest_path = _fixture_board_path("manifest.json")
    with open(manifest_path, encoding="utf-8") as manifest_file:
        return json.load(manifest_file)


def _fixture_path_from_id(fixture_id: str) -> Path:
    """Resolve a fixture board path by manifest fixture id."""
    for fixture in _fixture_manifest().get("fixtures", []):
        if fixture.get("id") == fixture_id:
            return _fixture_board_path(*str(fixture.get("path", "")).split("/"))
    raise AssertionError(f"Fixture id not found in manifest: {fixture_id}")


def _fixtures_by_intent(intent: str) -> list[dict]:
    """Return manifest fixtures filtered by intent."""
    fixtures = _fixture_manifest().get("fixtures", [])
    return [fixture for fixture in fixtures if fixture.get("intent") == intent]


def _runtime_major() -> int:
    """Best-effort extraction of current KiCad runtime major version."""
    version_text = ""
    if hasattr(pcbnew, "GetBuildVersion"):
        version_text = str(pcbnew.GetBuildVersion())
    match = re.search(r"(\d+)", version_text)
    return int(match.group(1)) if match else 0


def _fixture_matches_runtime(fixture: dict) -> bool:
    """Return whether a fixture is declared compatible with current runtime major."""
    major = _runtime_major()
    min_major = fixture.get("min_runtime_major")
    max_major = fixture.get("max_runtime_major")
    if min_major is not None and major < int(min_major):
        return False
    if max_major is not None and major > int(max_major):
        return False
    return True


def _fixture_path(fixture: dict) -> Path:
    """Resolve a fixture path from a fixture manifest entry."""
    return _fixture_board_path(*str(fixture.get("path", "")).split("/"))


def _drc_integration_enabled() -> bool:
    """Return whether expensive/fragile DRC integration checks should run."""
    return os.getenv("KICAD_DRC_INTEGRATION", "0") in {"1", "true", "TRUE", "yes", "YES"}


def _run_drc_in_subprocess(board_path: Path, tmp_path: Path) -> tuple[int, list[str]]:
    """Run DRC in a subprocess to isolate native crashes from the main test process."""
    payload_path = tmp_path / f"drc_payload_{board_path.stem}.json"

    script = (
        "import json\n"
        "from pathlib import Path\n"
        "import pcbnew\n"
        "from kicad_drc import run_drc, parse_drc_report\n"
        f"board = Path({str(board_path)!r})\n"
        f"report = Path({str(tmp_path / (board_path.stem + '.rpt'))!r})\n"
        f"payload = Path({str(payload_path)!r})\n"
        "ok = run_drc(pcbnew, str(board), str(report))\n"
        "count, messages = parse_drc_report(str(report))\n"
        "payload.write_text(json.dumps({'ok': bool(ok), 'count': count, 'messages': messages}), encoding='utf-8')\n"
    )

    python_exec = os.environ.get("PYTHON_FOR_DRC") or os.environ.get("PYTHON") or sys.executable

    result = subprocess.run(
        [
            python_exec,
            "-c",
            script,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        pytest.skip(
            "DRC subprocess failed/crashed in this environment; "
            "set up a GUI-capable runtime (or CI container) for DRC checks. "
            f"returncode={result.returncode}"
        )

    if not payload_path.exists():
        pytest.skip("DRC subprocess did not produce payload output")

    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    return int(payload.get("count", 0)), list(payload.get("messages", []))


def test_load_board_and_list_footprints(tmp_path):
    """A board created via SWIG can be saved, reloaded, and enumerated."""
    loaded_board, footprints = _create_round_trip_board(tmp_path)
    assert loaded_board is not None
    assert len(footprints) >= 1


def test_load_checked_in_k9_fixture_board():
    """A real KiCad 9 fixture board can be loaded and enumerated headlessly."""
    board_path = _fixture_path_from_id("k9_smoke_full125")
    assert board_path.exists(), f"Missing fixture board: {board_path}"

    loaded_board = pcbnew.LoadBoard(str(board_path))
    footprints = _board_footprints(loaded_board)
    assert loaded_board is not None
    assert len(footprints) >= 1


def test_fixture_manifest_paths_exist():
    """All fixture files declared in manifest exist in the repository."""
    manifest = _fixture_manifest()
    fixtures = manifest.get("fixtures", [])
    assert fixtures, "Fixture manifest should declare at least one fixture"

    for fixture in fixtures:
        path = _fixture_path(fixture)
        assert path.exists(), f"Missing fixture path in manifest: {path}"


def test_manifest_has_k9_smoke_fixture():
    """Manifest declares at least one KiCad 9 smoke fixture."""
    smoke = _fixtures_by_intent("smoke_ok")
    assert smoke, "Expected at least one smoke_ok fixture in manifest"


def test_fixture_board_has_reference_fields():
    """Loaded fixture exposes non-empty footprint reference strings."""
    board_path = _fixture_path_from_id("k9_smoke_full125")
    loaded_board = pcbnew.LoadBoard(str(board_path))
    footprints = _board_footprints(loaded_board)
    assert footprints, "Fixture board should contain at least one footprint"

    refs = [fp.GetReference() for fp in footprints if hasattr(fp, "GetReference")]
    assert refs, "No readable footprint references found"
    assert any(bool(str(ref).strip()) for ref in refs)


def test_all_smoke_fixtures_load_and_enumerate():
    """All smoke fixtures in manifest should load and expose at least one footprint."""
    smoke_fixtures = [f for f in _fixtures_by_intent("smoke_ok") if _fixture_matches_runtime(f)]
    assert smoke_fixtures, "Expected at least one smoke_ok fixture in manifest"

    for fixture in smoke_fixtures:
        board_path = _fixture_path(fixture)
        assert board_path.exists(), f"Missing fixture board: {board_path}"

        board = pcbnew.LoadBoard(str(board_path))
        footprints = _board_footprints(board)
        assert footprints, f"No footprints found in smoke fixture {board_path}"


def test_compat_fixtures_open_in_current_runtime():
    """Compatibility fixtures (e.g. KiCad 8) should open in current runtime when provided."""
    compat_fixtures = [
        fixture for fixture in _fixtures_by_intent("compat_open_in_k9") if _fixture_matches_runtime(fixture)
    ]
    if not compat_fixtures:
        pytest.skip("No compatibility fixtures declared yet")

    for fixture in compat_fixtures:
        board_path = _fixture_path(fixture)
        assert board_path.exists(), f"Missing compatibility fixture: {board_path}"

        board = pcbnew.LoadBoard(str(board_path))
        footprints = _board_footprints(board)
        assert footprints, f"No footprints found in compatibility fixture {board_path}"


def test_fixture_lcsc_assignment_round_trip_on_existing_footprint():
    """LCSC helper functions work on a real footprint from checked-in fixture."""
    board_path = _fixture_path_from_id("k9_smoke_full125")
    loaded_board = pcbnew.LoadBoard(str(board_path))
    footprints = _board_footprints(loaded_board)
    assert footprints, "Fixture board should contain at least one footprint"

    footprint = footprints[0]
    set_lcsc_value(footprint, "C999999")
    assert get_lcsc_value(footprint) == "C999999"


def test_fixture_drc_path_smoke(tmp_path):
    """DRC path runs on fixture board and returns a non-negative count when supported."""
    if not _drc_integration_enabled():
        pytest.skip("Set KICAD_DRC_INTEGRATION=1 to enable DRC integration checks")

    if not hasattr(pcbnew, "WriteDRCReport"):
        pytest.skip("WriteDRCReport not available in this pcbnew build")

    board_path = _fixture_path_from_id("k9_smoke_full125")
    count, _ = _run_drc_in_subprocess(board_path, tmp_path)
    assert isinstance(count, int)
    assert count >= 0


def test_drc_fail_fixtures_match_expected_patterns(tmp_path):
    """DRC-fail fixtures should match stable expected error patterns when configured."""
    if not _drc_integration_enabled():
        pytest.skip("Set KICAD_DRC_INTEGRATION=1 to enable DRC integration checks")

    if not hasattr(pcbnew, "WriteDRCReport"):
        pytest.skip("WriteDRCReport not available in this pcbnew build")

    drc_fail_fixtures = [f for f in _fixtures_by_intent("drc_fail") if _fixture_matches_runtime(f)]
    if not drc_fail_fixtures:
        pytest.skip("No drc_fail fixtures declared yet")

    for fixture in drc_fail_fixtures:
        board_path = _fixture_path(fixture)
        assert board_path.exists(), f"Missing DRC fixture board: {board_path}"

        error_count, error_messages = _run_drc_in_subprocess(board_path, tmp_path)
        assert error_count > 0, f"Expected DRC errors for fixture {board_path}"

        expected_patterns = fixture.get("expected_drc_patterns", []) or []
        if not expected_patterns:
            continue

        combined = "\n".join(error_messages)
        for pattern in expected_patterns:
            assert pattern in combined, (
                f"Expected DRC pattern {pattern!r} not found for fixture {board_path}"
            )


def test_assign_lcsc_to_real_footprint_field(tmp_path):
    """LCSC assignment helper round-trips on a real pcbnew footprint object."""
    _, footprints = _create_round_trip_board(tmp_path)
    footprint = footprints[0]

    set_lcsc_value(footprint, "C123456")
    assert get_lcsc_value(footprint) == "C123456"


def test_fake_enrichment_field_can_be_persisted(tmp_path):
    """A fake enrichment status field can be stored for plugin-side experimentation."""
    loaded_board, footprints = _create_round_trip_board(tmp_path)
    footprint = footprints[0]

    if not hasattr(footprint, "SetField"):
        pytest.skip("Footprint SetField API not available in this pcbnew build")

    footprint.SetField("JLC_ENRICHMENT", "Pending")

    board_path = tmp_path / "swig_enrichment_roundtrip.kicad_pcb"
    _save_board(board_path, loaded_board)

    loaded = pcbnew.LoadBoard(str(board_path))
    loaded_footprint = _board_footprints(loaded)[0]
    assert _get_named_field_text(loaded_footprint, "JLC_ENRICHMENT") == "Pending"


class _FakeStoreParent:
    def __init__(self, lcsc_priority=False):
        self.settings = {"general": {"lcsc_priority": lcsc_priority}}


def test_store_imports_fixture_board_into_project_database(tmp_path):
    """Store should import real fixture footprints into a per-project sqlite database."""
    store_module = _load_store_module()
    board_path = _fixture_path_from_id("k9_smoke_full125")
    board = pcbnew.LoadBoard(str(board_path))

    store = store_module.Store(_FakeStoreParent(), str(tmp_path), board)
    rows = store.read_all()

    assert rows, "Expected fixture board parts to be imported into sqlite store"
    assert (tmp_path / "jlcpcb" / "project.db").exists()
    assert len(rows) == len(helpers.get_valid_footprints(board))
    assert all(row["reference"] for row in rows)
    assert all(row["footprint"] for row in rows)


def test_store_generation_count_persists_across_reloads(tmp_path):
    """Store metadata should persist generation counts for a project directory."""
    store_module = _load_store_module()
    board_path = _fixture_path_from_id("k9_smoke_full125")
    board = pcbnew.LoadBoard(str(board_path))

    store = store_module.Store(_FakeStoreParent(), str(tmp_path), board)
    assert store.get_generation_count() == 0
    assert store.increment_generation_count() == 1
    assert store.increment_generation_count() == 2

    reloaded_store = store_module.Store(_FakeStoreParent(), str(tmp_path), board)
    assert reloaded_store.get_generation_count() == 2


def test_store_reads_lcsc_value_from_real_board_footprint(tmp_path):
    """Store import should capture LCSC values present on real pcbnew footprints."""
    store_module = _load_store_module()
    board = _new_board()
    footprint = _new_footprint(board)
    _set_reference_value(footprint, reference="R7", value="4k7")
    set_lcsc_value(footprint, "C424242")
    _add_footprint(board, footprint)

    store = store_module.Store(_FakeStoreParent(), str(tmp_path), board)
    part = store.get_part("R7")

    assert part is not None
    assert part["lcsc"] == "C424242"
