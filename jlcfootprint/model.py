"""The records the controller hands to the rest of the plugin.

Each is plain data with a few derived properties and no behaviour of its own: the
controller builds it from the cache and the verdict store, and the code that writes
or shows it reads it without needing the fetch session itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .verdicts import StoredVerdict


@dataclass
class Decision:
    """What the CPL emits for one reference, and the verdict it comes from."""

    reference: str
    lcsc: str
    rotation: Optional[int] = None  # the correction applied; None emits the raw angle
    source: str = "raw"  # 'override' | 'derived' | 'raw'
    # The verdict's status ('pending' while its data is fetched), 'no-lcsc' or
    # 'no-verdict'; the CPL's stand-in for a row placing another part says
    # 'lcsc-mismatch'.
    status: str = "unknown"
    polarity_light: Optional[str] = None
    fit: Optional[str] = None
    note: str = ""
    pending: bool = False
    # How far the JLC body overhangs the KiCad courtyard, in mm.
    body_excess: Optional[float] = None
    # JLC's package origin in the KiCad footprint frame, None without one; the CPL
    # places the part there only while ``jlcfootprint.exact_origin`` is on.
    origin: Optional[tuple] = None
    verdict: Optional[StoredVerdict] = field(default=None, repr=False)

    @property
    def display(self) -> str:
        """Return the Rotation column text: what the CPL emits, never the queue's state.

        A part still waiting for its EasyEDA data reads as the CPL would emit it
        meanwhile: its override, else "raw".
        """
        if self.verdict is not None:
            return self.verdict.display_text
        return "raw"
