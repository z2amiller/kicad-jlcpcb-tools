#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

KICAD9_PYTHON_DEFAULT="/Applications/KiCad9/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/3.9/bin/python3"
KICAD9_PYTHON="${KICAD9_PYTHON:-$KICAD9_PYTHON_DEFAULT}"

if [[ ! -x "$KICAD9_PYTHON" ]]; then
  echo "KiCad 9 Python not found/executable: $KICAD9_PYTHON" >&2
  echo "Set KICAD9_PYTHON to your KiCad 9 python path and retry." >&2
  exit 1
fi

cd "$REPO_ROOT"

"$KICAD9_PYTHON" -m pip install --user pytest >/dev/null 2>&1 || true

export PYTHON_FOR_DRC="$KICAD9_PYTHON"
export KICAD_DRC_INTEGRATION="${KICAD_DRC_INTEGRATION:-1}"

exec "$KICAD9_PYTHON" -m pytest -m kicad_integration tests/test_kicad_swig_integration.py "$@"
