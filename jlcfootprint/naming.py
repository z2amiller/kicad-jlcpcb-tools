"""EasyEDA Footprint Naming Rule suffix parser (3.9 port of jlc-footprint-data's suffix.py).

Implements the rotation-correction lookup defined by the EasyEDA Footprint Naming
Rule Reference (MIT licensed, jointly authored by LCSC and EasyEDA engineering).

The naming rule encodes the footprint's pin-1 orientation in the package name:
  {family}[{dimensions}]-{orientation-token*}

  Family examples: C, R, LED, SOIC-8, CAP-SMD, QFN-48
  Dimension tokens (filtered, not orientation): L{n}, W{n}, LS{n}, P{n}, BD{n}, EP
  Orientation tokens:
    TL / TR / BL / BR  -- pin-1 at a corner of the IC outline
    L / R / T / B      -- pin-1 at an edge (2-pin or axial packages)
    FD / RD / BI       -- polarity direction for 2-terminal polarized parts

Rotation conventions:
  - Angles are CCW-positive degrees, matching KiCad's CPL convention.
  - The correction is added to the KiCad footprint rotation to produce the
    JLCPCB-correct orientation.
  - Standard JLCPCB reference: pin-1 at top-left → 0° correction.
  - For 2-pin polarized parts: pin-1 (positive terminal) on left → 0° correction.

Cathode-is-pin-1 exception (LEDs and standard diodes):
  In KiCad, both LED footprints and standard diode footprints (Diode_SMD, Diode_TH)
  use pin-1 = cathode (K), not anode.  EasyEDA's FD (anode on left) therefore places
  K on the right → 180° correction.  RD (anode on right) places K on the left → 0°.
  This inverts the FD/RD mapping relative to capacitors.
  Affected families: LED, LED-SMD, LED-TH, DO-1x/DO-3x/DO-41, SMA/SMB/SMC,
    SOD-123/323/523/923, MELF/MINIMELF/MICROMELF.
"""

from __future__ import annotations

import re
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Rotation correction lookup tables
# ---------------------------------------------------------------------------

# Pin-1 corner/edge position → correction (all non-LED families)
_PIN1_CORRECTION: dict[str, int] = {
    "TL": 0,  # top-left  → already standard → 0°
    "TR": 270,  # KiCad's top-left pin 1 lands on JLC's top-right after 270° CCW
    "BL": 90,  # KiCad's top-left pin 1 lands on JLC's bottom-left after 90° CCW
    "BR": 180,  # KiCad's top-left pin 1 lands on JLC's bottom-right after 180°
    "L": 0,  # left → standard 2-pin horizontal → 0°
    "R": 180,  # right → flipped → 180°
    "T": 270,  # KiCad's left pin 1 lands at the top after 270° CCW
    "B": 90,  # KiCad's left pin 1 lands at the bottom after 90° CCW
}

# Polarity direction → correction (standard: positive terminal = KiCad pin-1)
_POLARITY_CORRECTION_STANDARD: dict[str, int] = {
    "FD": 0,  # forward direction: positive on left → KiCad standard → 0°
    "RD": 180,  # reverse direction: positive on right → 180°
    "BI": 0,  # bidirectional → no preferred orientation → 0°
}

# Polarity direction → correction for LED family:
# KiCad LED footprint has pin-1 = cathode (K), not anode.
# EasyEDA FD (anode on left) → K on right → 180° correction.
# EasyEDA RD (anode on right) → K on left → 0° correction.
_POLARITY_CORRECTION_LED: dict[str, int] = {
    "FD": 180,
    "RD": 0,
    "BI": 0,
}

# All recognised orientation tokens
_ALL_ORIENTATION_TOKENS = set(_PIN1_CORRECTION) | set(_POLARITY_CORRECTION_STANDARD)

# ---------------------------------------------------------------------------
# MPN-suffix family denylist
# ---------------------------------------------------------------------------
#
# These families always use a free-form manufacturer part number (MPN) as their
# package suffix (the `_[SN/MPN]` format in the EasyEDA naming rule reference).
# Any token that looks like an orientation token inside an MPN is a product
# code, not an EasyEDA geometric orientation indicator.
#
# Matching is done on the first hyphen-separated word of the extracted family,
# so "CONN-SMD", "CONN-TH", and "CONN" all map to root "CONN".
#
# Source: EasyEDA Footprint Naming Rule Reference §§3.2, 4.2, 4.3, 5.x
#
# Excluded from this list (they DO carry valid orientation tokens):
#   - LED, LED-SMD, LED-TH, LED-SEG   — have TL/TR/BL/BR or FD/RD tokens
#   - BUZ, PSK, MIC                    — structured format with L/R-(FD/RD)
#   - OSC                              — square packages use TL/TR/BL/BR
#   - all standard passives (R,C,L,D)  — handled by family_default path
#
# Note: older or third-party EasyEDA footprints occasionally deviate from the
# naming rule, so this filter may suppress a small number of valid tokens.
_MPN_SUFFIX_FAMILY_ROOTS: frozenset[str] = frozenset(
    {
        # §4.1 Standard Header — V/H/R/C/S params, no orientation tokens in format
        "HDR",
        # §4.2 Non-Standard Header / Connector — _[SN/MPN] format
        "CONN",
        "IDC",
        # §4.3 Special Function Connector — _[SN/MPN] for all non-regular shapes
        # Note: MICRO-USB-SMD → root "MICRO"; USB-C-SMD → root "USB".  Both are connectors.
        "FPC",
        "MICRO",
        "USB",
        "AUDIO",
        "DP",
        "HDMI",
        "DVI",
        "DSUB",
        "VGA",
        "SD",
        "TF",
        "SIM",
        "RF",
        "DIMM",
        "ATX",
        "PCI",
        "SATA",
        "ATA",
        "RJ11",
        "RJ12",
        "RJ22",
        "RJ45",
        "RJ48",
        "FIBER",
        "LPC",
        # Note: root matching uses first hyphen-segment (e.g. RJ45-TH → root RJ45),
        # so "RJ45" must be listed rather than just "RJ".
        # §3.2 Non-standard semiconductor packages — _[SN/MPN] format
        "TRS",
        "IC",
        "MCU",
        "DRAM",
        "SEMI",
        # §5 Other discrete devices — _[SN/MPN] format
        "RELAY",
        "SENSOR",
        "OPTO",
        "XFMR",
        "BAT",
        "ANT",
        # §5.8 Switch/Key — EH (extra hole) token but no orientation token
        "SW",
        "KEY",
    }
)

# Families where KiCad footprints use pin-1 = cathode (K), not anode.
# For these families FD/RD polarity is inverted relative to the standard table:
#   RD (anode on right → cathode on left) → 0° correction
#   FD (anode on left  → cathode on right) → 180° correction
#
# LED families: KiCad LED footprints have always used pin-1 = K.
#   LED-ARRAY is deliberately excluded: multi-LED packages often use pin-1 = anode.
#
# Standard diode packages: KiCad Diode_TH and Diode_SMD footprints also use
#   pin-1 = K (cathode).  Geometry cross-checks on DO-15, SMA, and SOD-323
#   confirmed the inversion (geo sees pad-1 on left for RD packages, matching 0°).
_CATHODE_PIN1_FAMILIES = frozenset(
    {
        # LED families
        "LED",
        "LED-SMD",
        "LED-TH",
        # Through-hole axial diodes (DO-2xx series)
        "DO-14",
        "DO-15",
        "DO-27",
        "DO-34",
        "DO-35",
        "DO-41",
        # DO-201 series (large-body high-current rectifiers, same TH axial convention)
        "DO-201",
        "DO-201AD",
        "DO-201AE",
        # Generic EasyEDA diode family names
        "DIO-TH",  # through-hole generic
        "DIO-SMD",  # SMD generic (same cathode-is-pin-1 convention as other SMD diodes)
        # SMD diodes — DO-214 variants with standardised numeric names
        # DO-214AA = SMA, DO-214AB = SMB, DO-214AC = SMC (some EasyEDA footprints use the DO name)
        "DO-214AA",
        "DO-214AB",
        "DO-214AC",
        # SMD diodes — DO-214 named variants (SMA=AA, SMB=AB, SMC=AC) and flat variants
        "SMA",
        "SMB",
        "SMC",
        "SMAF",
        "SMBF",
        "SMCF",  # flat/low-profile DO-219 variants
        "SMAG",
        "SMBG",
        "SMCG",  # guard-ring variants (same footprint/convention as SMA/B/C)
        # Large SMD power diode (Microsemi/Vishay)
        "DO-218AB",
        # DO-219AB / SMF: TVS and Schottky diodes; the crawler resolved these through the
        # LCSC "Diodes" category, which the plugin does not see at resolve time.
        "SMF",
        "DO-219AB",
        # SOD-882 / SOD-882D: 2-lead DFN diodes (KiCad D_SOD-882 has pin 1 = K); the crawl's
        # table lacked them, so its RD entries for this family read 180 where 0 is right.
        "SOD-882",
        "SOD-882D",
        # Vishay PowerDI series (SOD-123 footprint-compatible)
        "POWERDI-123",
        "POWERDI-323",
        # 2-terminal diode DFN/TSSLP variants (polarity token implies diode use)
        "DFN1610-2L",  # 1.6×1.0mm 2-lead DFN — almost exclusively diodes at this size
        "TSSLP-2-3",  # Thin Stacked SLP 2- or 3-pad — diode/TVS packages
        # Small SMD diodes — SOD (Small Outline Diode) series
        "SOD-123",
        "SOD-123F",
        "SOD-123FL",
        "SOD-123W",
        "SOD-323",
        "SOD-323F",
        "SOD-523",
        "SOD-523F",
        "SOD-923",
        # MELF diodes (MELF for resistors carries no polarity token, so safe to include)
        "MELF",
        "MINIMELF",
        "MICROMELF",
    }
)

# Public alias — kept for rotation-correction logic; do NOT add polarized caps here.
# Adding a family here inverts the FD/RD rotation correction (cathode-is-pin-1 exception).
CATHODE_PIN1_FAMILIES: frozenset[str] = _CATHODE_PIN1_FAMILIES

# Polarized capacitor families — aluminum electrolytic and tantalum.
# These have a meaningful pin-1 polarity (positive terminal) that is worth detecting,
# but they use the STANDARD FD/RD rotation correction (pin-1 = anode/positive),
# so they must NOT appear in _CATHODE_PIN1_FAMILIES.
#
# Used only for priority ordering in fetch-easyeda-polarity and the status display.
# CP-* prefix = "Capacitor Polarized" in EasyEDA naming conventions.
# CASE-* = EIA standard tantalum case codes (A=3216, B=3528, C=6032, D=7343, E=7360).
POLARIZED_CAP_FAMILIES: frozenset[str] = frozenset(
    {
        # Through-hole aluminum electrolytic
        "CP-TH",
        # SMD aluminum electrolytic (cylindrical; CP = Capacitor Polarized)
        "CP-SMD",
        # Tantalum SMD — EIA standard case codes
        "CASE-A",
        "CASE-B",
        "CASE-C",
        "CASE-D",
        "CASE-E",
    }
)


# Families that must never be treated as cathode-is-pin-1 even when a caller says the part
# is a diode: multi-LED arrays usually put the anode on pin 1.
_NEVER_CATHODE_FAMILIES: frozenset[str] = frozenset({"LED-ARRAY"})

# ---------------------------------------------------------------------------
# Dimensional token filter
# ---------------------------------------------------------------------------

# A token is dimensional (not an orientation) if it matches one of these patterns.
# Note: bare "L" and "T" are orientation tokens — only "L{digit}" is dimensional.
_DIMENSIONAL_RE = re.compile(
    r"^(?:"
    r"L\d"  # body length: L4.9, L1.6, ...
    r"|W\d"  # body width
    r"|LS\d"  # lead span
    r"|P\d"  # pin pitch
    r"|BD\d"  # body diameter
    r"|EP"  # exposed pad (no number)
    r")"
)


def _is_dimensional(token: str) -> bool:
    """Return True for dimensional tokens such as L4.9, LS6.0 or EP."""
    return bool(_DIMENSIONAL_RE.match(token))


# ---------------------------------------------------------------------------
# Family extraction
# ---------------------------------------------------------------------------

# Package names with underscore: everything before _ is the family.
# Package names without underscore: leading alphabetic-and-dash prefix, stopping at
# the first digit.  e.g. "LED0603-RD" → "LED",  "C0603" → "C".
# Hyphens connecting alpha segments are included (e.g. "CAP-SMD" → "CAP-SMD" via _
# split; this regex covers the no-underscore simpler cases).
_LEADING_ALPHA_RE = re.compile(r"^([A-Z]+(?:-[A-Z]+)*)(?=\d|$)")
# A head that carries its own orientation token looks like LED0603-RD: letters, digits, token.
_HEAD_WITH_DIGITS_RE = re.compile(r"^[A-Z]+(?:-[A-Z]+)*\d")


def extract_family(package_name: str) -> str:
    """Return the family prefix of a package name.

    Examples:
        "C0603"                          → "C"
        "LED0603-RD"                     → "LED"
        "SOIC-8_L4.9-W3.9-P1.27-BL"    → "SOIC-8"
        "CAP-SMD_BD6.3-L6.6-W6.6-FD"   → "CAP-SMD"
        "QFN-48_L7.0-W7.0-P0.5-BL"     → "QFN-48"

    """
    package_name = package_name.upper()
    head = package_name.split("_", 1)[0]
    if "_" in package_name and not _head_tokens(head):
        return head
    m = _LEADING_ALPHA_RE.match(head)
    if m:
        return m.group(1).rstrip("-")
    return head  # fallback: return as-is


def _head_tokens(head: str) -> list[str]:
    """Return orientation tokens carried by a pre-underscore head such as ``LED0603-RD``.

    Only a head shaped family-plus-digits qualifies; a bare code such as ``CASE-B``
    (a tantalum case size) has no digits and its ``B`` is not an orientation.
    """
    if not _HEAD_WITH_DIGITS_RE.match(head):
        return []
    return _tokens_in(head)


def _tokens_in(section: str) -> list[str]:
    """Return the orientation tokens found in one hyphen-separated section, in order."""
    return [
        tok.upper()
        for tok in section.split("-")
        if tok.upper() in _ALL_ORIENTATION_TOKENS and not _is_dimensional(tok.upper())
    ]


# ---------------------------------------------------------------------------
# Orientation token extraction
# ---------------------------------------------------------------------------


def extract_orientation_tokens(package_name: str) -> list[str]:
    """Return all orientation tokens found in the package name suffix.

    Dimensional tokens (L4.9, W3.9, LS6.0, etc.) are filtered out.
    Returns tokens in the order they appear (last one wins if lookup differs).

    For families in _MPN_SUFFIX_FAMILY_ROOTS (connectors, relays, etc.) the
    entire suffix is treated as a free-form manufacturer part number and no
    orientation tokens are extracted, avoiding false positives from product
    codes that happen to look like EasyEDA orientation tokens.
    """
    package_name = package_name.upper()
    family = extract_family(package_name)
    family_root = family.split("-")[0]
    if family_root in _MPN_SUFFIX_FAMILY_ROOTS:
        return []

    # The naming rule uses a second underscore to separate the geometric suffix from a
    # trailing MPN (VSON-10_L3.0-W3.0-TL-EP_TPS61230DRCR); only the segment between the
    # first and second underscore carries valid orientation tokens.  Some names put the
    # tokens before the first underscore instead and a colour or MPN after it
    # (LED0603-RD_GREEN); when the middle segment yields nothing, the head is scanned.
    head, _, rest = package_name.partition("_")
    if rest:
        tokens = _tokens_in(rest.split("_", 1)[0])
        if tokens:
            return tokens
        return _head_tokens(head)
    return _tokens_in(head)


# ---------------------------------------------------------------------------
# Parse result
# ---------------------------------------------------------------------------


class SuffixParseResult(NamedTuple):
    """Rotation derived from a package name, with provenance."""

    rotation_correction: int  # 0, 90, 180, or 270
    rotation_source: str  # 'naming_rule' | 'family_default' | 'none'
    parser_confidence: str  # 'high' | 'medium' | 'low' | 'none'
    family: str
    parsed_suffix: str | None  # the orientation token(s) found, comma-joined
    notes: str | None


def parse_package_name(
    package_name: str,
    cathode_is_pin1: bool | None = None,
) -> SuffixParseResult:
    """Parse a package name and return the rotation correction.

    This is the main entry point for the suffix parser.

    Args:
        package_name: The EasyEDA package name string.
        cathode_is_pin1: A caller's knowledge that the part is a diode or LED whose
            KiCad footprint has pin 1 = cathode (for example from the parts database's
            category, or from the schematic's pin functions).  Used only when the family
            is not already listed, and never for families in _NEVER_CATHODE_FAMILIES.

    Lookup order:
    1. POLARITY_CORRECTION (FD/RD/BI) when a polarity token is present: it names a
       physical direction, so it wins; cathode-is-pin-1 families (LEDs, standard
       diodes) invert the mapping.
    2. PIN1_CORRECTION (TL/TR/BL/BR/L/R/T/B) otherwise: a positional token names
       where pin 1 is drawn, which for a two-terminal part is only as good as the
       library's choice of which terminal is pin 1.
    3. No orientation token found: default to 0° with 'family_default' source.
    4. Unrecognised package name: 0° with 'none' source and no confidence.

    When both kinds of token are present and disagree, a note records the
    positional value.

    """
    family = extract_family(package_name)
    tokens = extract_orientation_tokens(package_name)

    positional = [t for t in tokens if t in _PIN1_CORRECTION]
    polarity = [t for t in tokens if t in _POLARITY_CORRECTION_STANDARD]

    if not tokens:
        # No orientation token at all.
        # Standard passive families (R, C, L, D) default to 0°; others unknown.
        is_standard_passive = family.upper() in {"R", "C", "L", "D", "F"}
        if is_standard_passive:
            return SuffixParseResult(
                rotation_correction=0,
                rotation_source="family_default",
                parser_confidence="low",
                family=family,
                parsed_suffix=None,
                notes="no orientation token; standard passive assumed 0°",
            )
        return SuffixParseResult(
            rotation_correction=0,
            rotation_source="none",
            parser_confidence="none",
            family=family,
            parsed_suffix=None,
            notes=None,
        )

    family_upper = family.upper()
    cathode_via_family = family_upper in _CATHODE_PIN1_FAMILIES
    cathode_via_hint = (
        not cathode_via_family
        and bool(cathode_is_pin1)
        and family_upper not in _NEVER_CATHODE_FAMILIES
    )
    is_cathode_family = cathode_via_family or cathode_via_hint
    pol_table = (
        _POLARITY_CORRECTION_LED if is_cathode_family else _POLARITY_CORRECTION_STANDARD
    )

    rotation: int | None = None
    used_token: str | None = None
    confidence = "high"
    notes = None

    if polarity:
        used_token = polarity[-1]
        rotation = pol_table[used_token]
        if cathode_via_family:
            confidence = "medium"
            notes = "cathode-is-pin1 family: FD/RD polarity inverted relative to standard convention"
        elif cathode_via_hint:
            confidence = "medium"
            notes = "cathode-is-pin1 by caller's hint: FD/RD polarity inverted"
        if positional:
            pos_rotation = _PIN1_CORRECTION[positional[-1]]
            if pos_rotation != rotation:
                notes = (
                    f"polarity {used_token}→{rotation}° used; positional "
                    f"{positional[-1]}→{pos_rotation}° disagrees"
                )
    else:
        used_token = positional[-1]
        rotation = _PIN1_CORRECTION[used_token]

    all_tokens = positional + [t for t in polarity if t not in positional]
    parsed_suffix = ",".join(all_tokens) if all_tokens else None

    return SuffixParseResult(
        rotation_correction=rotation,
        rotation_source="naming_rule",
        parser_confidence=confidence,
        family=family,
        parsed_suffix=parsed_suffix,
        notes=notes,
    )
