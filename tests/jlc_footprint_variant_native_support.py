"""A real JLC footprint check inside upstream's native variant window (spec 18).

Upstream's window fixture turns the check off, because its board double has no
pads to read.  ``checked_window`` turns it on for a board double whose one
footprint carries KiCad's SOT-23 pads, and lets the window build its check with
the plugin's own facade (the variant read through the session included) over a
cache that already holds every part number the test names, so the first scan
resolves everything locally and the worker thread is never started.
"""

from __future__ import annotations

from typing import Any

import pytest

from .jlcfootprint_support import alias
from .test_jlcfootprint_kicad_adapter import FakePad

# KiCad's Package_TO_SOT_SMD:SOT-23 pads, in the double's nanometres.
SOT23 = (("1", -937_500, -950_000), ("2", -937_500, 950_000), ("3", 937_500, 0))


class NativePad(FakePad):
    """KiCad's SOT-23 pad as both upstream's pad metadata and the check's adapter read it."""

    def HasHole(self) -> bool:
        """Report a surface-mount pad."""
        return False


def sot23_pads() -> list[NativePad]:
    """Return the three SOT-23 pads, roundrect, 1.475 x 0.6 mm."""
    return [NativePad(number, x, y, 1_475_000, 600_000) for number, x, y in SOT23]


# Part numbers the cache answers for: a SOT-23 transistor (C2132's recording, green
# at 180 on these pads) and an SOIC-8 op-amp (C7950's: red, pad count).
FITS = ("C1", "C2132")
MISFITS = ("C2", "C7950")


def checked_window(ui: Any, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Turn the check on for ``ui``'s window and return the checks it builds.

    The board's footprint R1 orders C1 in Default and A and C2 in B, so the
    output switch from A to B turns its JLC glyph from green to red.
    """
    ui.settings["jlcfootprint"]["enabled"] = True
    footprint = ui.board.parts[0]
    footprint.Pads = sot23_pads
    footprint.AddVariant("B").SetFieldValue("LCSC", "C2")
    presenter = ui.main.jlc_footprint_window
    create = presenter.create_footprint_check
    checks: list[Any] = []

    def build(window: Any, pcbnew: Any) -> Any:
        # The fixture's editor wrapper omits pcbnew's ToMM; the double is in nm.
        pcbnew.ToMM = ui.pcbnew.ToMM
        check = create(window, pcbnew)
        for code, source in (FITS, MISFITS):
            check.cache.store(alias(code, source), now=1)
        check.start = lambda: None
        checks.append(check)
        return check

    monkeypatch.setattr(presenter, "create_footprint_check", build)
    return checks
