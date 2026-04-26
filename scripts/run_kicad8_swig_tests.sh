#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

KICAD8_PYTHON_DEFAULT="/Applications/KiCad8/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/3.9/bin/python3"
KICAD8_PYTHON="${KICAD8_PYTHON:-$KICAD8_PYTHON_DEFAULT}"

if [[ ! -x "$KICAD8_PYTHON" ]]; then
  echo "KiCad 8 Python not found/executable: $KICAD8_PYTHON" >&2
  echo "Set KICAD8_PYTHON to your KiCad 8 python path and retry." >&2
  exit 1
fi

cd "$REPO_ROOT"

"$KICAD8_PYTHON" -m pip install --user pytest >/dev/null 2>&1 || true

# Default DRC checks off for local KiCad 8 runtime unless explicitly requested.
export KICAD_DRC_INTEGRATION="${KICAD_DRC_INTEGRATION:-0}"

exec "$KICAD8_PYTHON" -m pytest -m kicad_integration tests/test_kicad_swig_integration.py "$@"
