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

## Notes

- Store only non-sensitive board data.
- Keep external dependencies out of fixture projects where possible.
- If exact DRC violation counts drift by KiCad version, tests should assert stable categories/paths instead of exact totals.
