"""The records the controller hands to the CPL, the column and the dialog.

``FetchState``, ``Decision`` and ``PartDetail`` are plain data with a few
derived properties and no behaviour of their own: the controller builds them
from the cache and the verdict store, and the CPL (``fabrication.py``), the
part list column (``presentation.py``) and the detail dialog and its overlay
(``presentation.py``, ``overlay.py``) read them without needing the fetch
session itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .geometry import Pad, named_pads, pad_pitch
from .records import DrawingMarks
from .resolver import Verdict
from .verdicts import StoredVerdict


@dataclass
class FetchState:
    """Where one part stands in the fetch queue: the column's clock and pause rows (spec 16.3).

    ``state`` is ``idle`` (nothing outstanding), ``queued``, ``fetching`` (this
    part's own request is in flight, ``kind`` says which document), ``paused`` (a
    backoff is running, ``seconds`` is what is left of it) or ``tripped`` (the
    session's breaker).  ``ahead`` counts the requests queued before this part's and
    ``seconds`` how long they take; ``queued_parts`` is how many parts wait in all.
    """

    state: str = "idle"
    kind: str = ""
    ahead: int = 0
    seconds: float = 0.0
    queued_parts: int = 0


@dataclass
class Decision:
    """What the CPL emits for one reference (spec 8)."""

    reference: str
    lcsc: str
    rotation: int | None = None  # the correction applied; None emits the raw angle
    source: str = "raw"  # 'override' | 'derived' | 'raw'
    status: str = "unknown"  # verdict status, 'pending', or 'no-lcsc' / 'no-verdict'
    polarity_light: str | None = None
    fit: str | None = None
    note: str = ""
    pending: bool = False
    # How far the JLC body overhangs the KiCad courtyard, in mm (spec 16.6).
    body_excess: float | None = None
    # JLC's package origin in the KiCad footprint frame (spec 17.2), None without one;
    # the CPL places the part there only while ``jlcfootprint.exact_origin`` is on.
    origin: tuple | None = None
    verdict: StoredVerdict | None = field(default=None, repr=False)

    @property
    def display(self) -> str:
        """Return the Rotation column text: what the CPL emits, never the queue's state.

        A pending part's Rotation cell reads the raw angle; the JLC column's clock
        shows why (spec 16.3).
        """
        if self.verdict is not None:
            return self.verdict.display_text
        return "raw"


@dataclass
class PartDetail:
    """Everything the detail dialog shows for one part (spec 16.4).

    The two pad sets and the placement are the resolver's own inputs and output,
    re-run here rather than read back from the verdict row, so the canvas draws what
    the check decided and not a rounded copy of it.  ``verdict`` is None when the
    cache has nothing to resolve against (a part still being fetched, a part EasyEDA
    does not know), and then only the KiCad pads and whatever raw JLC pads the cache
    holds can be drawn.
    """

    reference: str
    lcsc: str
    kicad_footprint: str = ""
    kicad_pads: list[Pad] = field(default_factory=list)
    courtyard: tuple | None = None
    is_bottom: bool = False
    placed_rotation: float = 0.0
    package_name: str = ""
    puuid: str = ""
    jlc_pads: list[Pad] = field(default_factory=list)
    symbol_pins: list = field(default_factory=list)
    marks: DrawingMarks | None = None
    source: str = ""  # 'live' | 'seed' | '' when nothing is cached
    fetched_at: int = 0
    verdict: Verdict | None = None  # re-resolved, carries the placement
    stored: StoredVerdict | None = None
    decision: Decision | None = None
    fetch: FetchState = field(default_factory=FetchState)

    @property
    def pin_functions(self) -> dict:
        """Return the pin function per KiCad pad number, where the schematic gave one."""
        return {
            pad.number: pad.pin_function
            for pad in named_pads(self.kicad_pads)
            if pad.pin_function
        }

    @property
    def kicad_pitch(self) -> float | None:
        """Return the KiCad footprint's nearest-terminal pitch in millimetres."""
        return pad_pitch(self.kicad_pads)

    @property
    def jlc_pitch(self) -> float | None:
        """Return the JLC drawing's nearest-terminal pitch in millimetres."""
        return pad_pitch(self.jlc_pads)

    @property
    def kicad_pin1(self) -> Pad | None:
        """Return the KiCad pad numbered 1, if the footprint has one."""
        return next(
            (pad for pad in named_pads(self.kicad_pads) if pad.number == "1"), None
        )

    @property
    def jlc_pin1(self) -> Pad | None:
        """Return the JLC pad numbered 1, if the drawing has one."""
        return next(
            (pad for pad in named_pads(self.jlc_pads) if pad.number == "1"), None
        )

    @property
    def placement(self):
        """Return the placement the resolver solved, or None when there is none."""
        return None if self.verdict is None else self.verdict.placement

    @property
    def emitted_rotation(self) -> int | None:
        """Return the correction the CPL applies for this part, None for the raw angle."""
        return None if self.stored is None else self.stored.emitted_rotation

    @property
    def origin(self) -> tuple | None:
        """Return JLC's package origin from the placement solved here (spec 17.2).

        Re-resolved like everything else the dialog draws, so it is the origin the
        next save would store rather than a rounded copy from the row.
        """
        return None if self.verdict is None else self.verdict.origin
