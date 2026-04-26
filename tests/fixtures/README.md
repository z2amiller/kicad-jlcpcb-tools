# KiCad SWIG integration fixtures

This folder holds small board fixtures used by `pytest -m kicad_integration`.

## Fixture goals

- Keep fixtures small and quick to parse in CI.
- Prefer representative real-world examples when practical.
- Include one intentionally DRC-failing board for stable-path/category assertions.

## Version compatibility policy

- Primary compatibility test direction: **KiCad 8 board opened in KiCad 9 runtime**.
- Reverse compatibility (KiCad 9 board in KiCad 8 runtime) is not required.

## Suggested fixture set

- `k9_smoke_ok/` – small KiCad 9 board that loads cleanly.
- `k9_drc_fail/` – small KiCad 9 board with intentional DRC issues.
- `k8_compat_ok/` – KiCad 8 board expected to open in KiCad 9 runtime.

## Current checked-in fixtures

- `k9_smoke_ok/fx-Full125B.kicad_pcb` – initial real-world KiCad 9 smoke fixture.
- `k9_drc_fail/Normal125B-DRCFail.kicad_pcb` – real-world KiCad 9 fixture intended to fail DRC.
- `k8_compat_ok/KiCad8-project.kicad_pcb` – KiCad 8 board for open-in-KiCad-9 compatibility checks.
- `k8_drc_fail/KiCad8-project-fail-DRC.kicad_pcb` – KiCad 8 board intended to fail DRC checks.

Fixture metadata is tracked in `tests/fixtures/manifest.json` and consumed by integration tests.

### Manifest conventions

- `intent: "smoke_ok"` for basic load/enumeration fixtures.
- `intent: "compat_open_in_k9"` for cross-version open tests (e.g. KiCad 8 -> KiCad 9).
- `intent: "drc_fail"` for intentional DRC-failing fixtures.
- `expected_drc_patterns` can optionally list stable substrings that should appear in parsed DRC error messages.

### DRC integration execution

DRC integration checks are opt-in by default. Enable them with:

```sh
KICAD_DRC_INTEGRATION=1 pytest -m kicad_integration tests/test_kicad_swig_integration.py
```

## Notes

- Store only non-sensitive board data.
- Keep external dependencies out of fixture projects where possible.
- If exact DRC violation counts drift by KiCad version, tests should assert stable categories/paths instead of exact totals.
