"""Read one KiCad library footprint: from this machine's libraries or a recorded snapshot.

The resolver's library-backed tests and two dev-time scripts all need the pads and
the courtyard of a named ``Library:Footprint``.  Where KiCad is installed they come
from its own ``.pretty`` directories; where it is not (CI, and any machine without
it) they come from the JSON snapshots under ``tests/fixtures/jlcfootprint/kicad``,
recorded by ``scripts/snapshot_kicad_footprints.py``.

It lives beside the scripts rather than in ``tests/`` so that no script has to
import from the test package, and not in ``jlcfootprint/`` because none of it is
something the shipped plugin ever reads: two of the paths name the test fixtures,
and the third names a developer's KiCad install behind a test environment
variable.  The parsing itself is the package's (``boardfile``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jlcfootprint.boardfile import footprint_pads, parse_kicad_pcb_text  # noqa: E402
from jlcfootprint.geometry import Pad  # noqa: E402

Box = tuple[float, float, float, float]

KICAD_SNAPSHOTS = ROOT / "tests" / "fixtures" / "jlcfootprint" / "kicad"
# Set JLCFOOTPRINT_NO_KICAD=1 to hide the installed libraries and prove the snapshots
# alone carry the library-backed tests, as they must in CI.
KICAD_FOOTPRINTS = (
    Path("/nonexistent/kicad/footprints")
    if os.environ.get("JLCFOOTPRINT_NO_KICAD")
    else Path("/Applications/KiCad/KiCad.app/Contents/SharedSupport/footprints")
)


def snapshot_path(library: str, name: str) -> Path:
    """Return where the recorded copy of one library footprint's pads lives."""
    return KICAD_SNAPSHOTS / f"{library}__{name}.json"


def footprints_available() -> bool:
    """Return True when library footprints can be read: recorded snapshots or an installed KiCad."""
    return KICAD_SNAPSHOTS.is_dir() or KICAD_FOOTPRINTS.is_dir()


def installed_library_footprint(
    library: str, name: str
) -> tuple[list[Pad], Box | None]:
    """Read one footprint's pads and courtyard box from the installed KiCad libraries."""
    text = (KICAD_FOOTPRINTS / f"{library}.pretty" / f"{name}.kicad_mod").read_text(
        encoding="utf-8"
    )
    (footprint,) = parse_kicad_pcb_text(f"(kicad_pcb {text})")
    return footprint_pads(footprint), footprint.courtyard


def installed_library_pads(library: str, name: str) -> list[Pad]:
    """Read one footprint's pads from the KiCad libraries installed on this machine."""
    return installed_library_footprint(library, name)[0]


def library_footprint(library: str, name: str) -> tuple[list[Pad], Box | None]:
    """Return a KiCad library footprint's pads and courtyard box, footprint frame, unplaced.

    The recorded snapshot under ``tests/fixtures/jlcfootprint/kicad`` is used when it
    exists, so the tests run without KiCad; otherwise the installed library is read.
    Record a snapshot with ``python3 scripts/snapshot_kicad_footprints.py Library:Name``.
    A snapshot recorded before courtyards were kept has no box (None).
    """
    snapshot = snapshot_path(library, name)
    if snapshot.exists():
        data = json.loads(snapshot.read_text(encoding="utf-8"))
        courtyard = data.get("courtyard")
        return (
            [Pad(**pad) for pad in data["pads"]],
            None if courtyard is None else tuple(courtyard),
        )
    if KICAD_FOOTPRINTS.is_dir():
        return installed_library_footprint(library, name)
    raise FileNotFoundError(
        f"no snapshot {snapshot.name} and no KiCad footprint libraries at {KICAD_FOOTPRINTS}; "
        f"run scripts/snapshot_kicad_footprints.py {library}:{name} on a machine with KiCad"
    )


def library_pads(library: str, name: str) -> list[Pad]:
    """Return a KiCad library footprint's pads in the footprint frame (see ``library_footprint``)."""
    return library_footprint(library, name)[0]


def with_functions(pads: list[Pad], functions: dict[str, str]) -> list[Pad]:
    """Return the pads with pin functions assigned by pad number, as a schematic would."""
    return [pad._replace(pin_function=functions.get(pad.number, "")) for pad in pads]
