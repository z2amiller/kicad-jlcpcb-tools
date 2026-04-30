#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

KICAD10_PYTHON_DEFAULT="/Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/3.9/bin/python3"
KICAD10_PYTHON="${KICAD10_PYTHON:-$KICAD10_PYTHON_DEFAULT}"

if [[ ! -x "$KICAD10_PYTHON" ]]; then
  echo "KiCad 10 Python not found/executable: $KICAD10_PYTHON" >&2
  echo "Set KICAD10_PYTHON to your KiCad 10 python path and retry." >&2
  exit 1
fi

cd "$REPO_ROOT"

"$KICAD10_PYTHON" -m pip install --user pytest >/dev/null 2>&1 || true

export PYTHON_FOR_DRC="$KICAD10_PYTHON"
export KICAD_DRC_INTEGRATION="${KICAD_DRC_INTEGRATION:-1}"

exec "$KICAD10_PYTHON" -m pytest -m kicad_integration tests/test_kicad_swig_integration.py "$@"
