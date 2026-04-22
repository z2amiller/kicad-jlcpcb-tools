"""Minimal KiCad .kicad_pcb s-expression parser.

Extracts footprint data only — skips tracks, zones, nets, and all other
board-level constructs.  Python 3.9 compatible, stdlib only.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class KiCadPad:
    number: str          # pad number as string, e.g. "1", "A1"
    x: float             # footprint-local x in mm (pre-rotation of the placed footprint)
    y: float             # footprint-local y in mm
    width: float         # pad width in mm
    height: float        # pad height in mm


@dataclass
class KiCadFootprint:
    reference: str                # e.g. "Q2"
    footprint_name: str           # the library+name from (footprint "Lib:Name" …)
    value: str                    # the Value property
    placed_x: float               # board position in mm
    placed_y: float
    placed_rotation: float        # the footprint-level rotation in degrees
    layer: str                    # e.g. "F.Cu" (top) or "B.Cu" (bottom)
    lcsc: Optional[str]           # from (property "LCSC" "C...") if present
    pads: List[KiCadPad] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

# Tokenize into: '(', ')', quoted strings (content only), atoms
_TOKEN_RE = re.compile(
    r'#[^\n]*'          # line comment (discard)
    r'|"(?:[^"\\]|\\.)*"'   # quoted string (keep quotes for detection)
    r'|[()]'            # paren
    r'|[^\s()"#]+'     # atom
)


def _tokenize(text: str) -> List[str]:
    return [m.group() for m in _TOKEN_RE.finditer(text)
            if not m.group().startswith('#')]


def _unescape(s: str) -> str:
    """Strip surrounding quotes and process escape sequences."""
    inner = s[1:-1]  # remove leading/trailing "
    # Handle \" and \\ — only escapes KiCad actually uses
    inner = inner.replace('\\"', '"').replace('\\\\', '\\')
    return inner


# ---------------------------------------------------------------------------
# S-expression parser
# ---------------------------------------------------------------------------

def _parse_tokens(tokens: List[str], pos: int = 0) -> Tuple[object, int]:
    """Recursively parse tokens into nested lists.

    Returns (node, next_pos) where node is either a str (atom/quoted) or
    a list representing an s-expr.
    """
    tok = tokens[pos]
    if tok == '(':
        pos += 1
        lst: List[object] = []
        while tokens[pos] != ')':
            child, pos = _parse_tokens(tokens, pos)
            lst.append(child)
        return lst, pos + 1  # consume ')'
    elif tok == ')':
        raise ValueError(f"Unexpected ')' at position {pos}")
    elif tok.startswith('"'):
        return _unescape(tok), pos + 1
    else:
        return tok, pos + 1


def _parse_sexp(text: str) -> List[object]:
    """Parse the entire file as a list of top-level s-expressions."""
    tokens = _tokenize(text)
    if not tokens:
        return []
    # The .kicad_pcb file is a single top-level s-expression
    result, _ = _parse_tokens(tokens, 0)
    return result  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Tree-walking helpers
# ---------------------------------------------------------------------------

def _tag(node: object) -> Optional[str]:
    """Return the tag (first atom element) of a list node, or None."""
    if isinstance(node, list) and node and isinstance(node[0], str):
        return node[0]
    return None


def _children(node: object) -> List[object]:
    """Return child nodes (all elements except the tag)."""
    if isinstance(node, list):
        return node[1:]
    return []


def _find_direct(node: object, tag: str) -> Optional[List[object]]:
    """Find the first direct child list with the given tag."""
    for child in _children(node):
        if isinstance(child, list) and _tag(child) == tag:
            return child  # type: ignore[return-value]
    return None


def _find_all_direct(node: object, tag: str) -> List[List[object]]:
    """Find all direct child lists with the given tag."""
    return [child for child in _children(node)  # type: ignore[misc]
            if isinstance(child, list) and _tag(child) == tag]


def _str_arg(node: object, idx: int = 1) -> Optional[str]:
    """Return the idx-th element as str, or None if missing/wrong type."""
    if isinstance(node, list) and len(node) > idx:
        v = node[idx]
        if isinstance(v, str):
            return v
    return None


def _float_arg(node: object, idx: int = 1) -> Optional[float]:
    """Return the idx-th element as float, or None if missing/wrong type."""
    if isinstance(node, list) and len(node) > idx:
        v = node[idx]
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# Footprint extraction
# ---------------------------------------------------------------------------

def _parse_pad(pad_node: List[object]) -> Optional[KiCadPad]:
    """Parse a (pad ...) node into a KiCadPad, or None if it should be skipped."""
    # pad_node[0] = "pad", [1] = number, [2] = type, [3] = shape, ...
    if len(pad_node) < 4:
        return None

    number = pad_node[1]
    if not isinstance(number, str) or number == "":
        return None  # skip unnamed pads (paste stencil, mech)

    at_node = _find_direct(pad_node, 'at')
    if at_node is None:
        return None
    px = _float_arg(at_node, 1)
    py = _float_arg(at_node, 2)
    if px is None or py is None:
        return None

    size_node = _find_direct(pad_node, 'size')
    if size_node is None:
        return None
    pw = _float_arg(size_node, 1)
    ph = _float_arg(size_node, 2)
    if pw is None or ph is None:
        return None

    return KiCadPad(number=number, x=px, y=py, width=pw, height=ph)


def _parse_footprint(fp_node: List[object]) -> Optional[KiCadFootprint]:
    """Parse a (footprint ...) node into a KiCadFootprint."""
    # fp_node[0] = "footprint", fp_node[1] = library+name string
    if len(fp_node) < 2:
        return None

    footprint_name = fp_node[1]
    if not isinstance(footprint_name, str):
        return None

    # Layer — first direct (layer ...) child
    layer_node = _find_direct(fp_node, 'layer')
    layer = _str_arg(layer_node) if layer_node is not None else "F.Cu"
    if layer is None:
        layer = "F.Cu"

    # Position — first direct (at x y [rot]) child
    at_node = _find_direct(fp_node, 'at')
    placed_x = _float_arg(at_node, 1) or 0.0
    placed_y = _float_arg(at_node, 2) or 0.0
    placed_rotation = 0.0
    if at_node is not None and len(at_node) > 3:
        r = _float_arg(at_node, 3)
        if r is not None:
            placed_rotation = r

    # Properties — iterate over all (property name value ...) children
    reference = ""
    value = ""
    lcsc: Optional[str] = None

    for prop_node in _find_all_direct(fp_node, 'property'):
        prop_name = _str_arg(prop_node, 1)
        prop_value = _str_arg(prop_node, 2)
        if prop_name == "Reference":
            reference = prop_value or ""
        elif prop_name == "Value":
            value = prop_value or ""
        elif prop_name == "LCSC":
            lcsc = prop_value if prop_value else None

    # Pads
    pads: List[KiCadPad] = []
    for pad_node in _find_all_direct(fp_node, 'pad'):
        pad = _parse_pad(pad_node)
        if pad is not None:
            pads.append(pad)

    return KiCadFootprint(
        reference=reference,
        footprint_name=footprint_name,
        value=value,
        placed_x=placed_x,
        placed_y=placed_y,
        placed_rotation=placed_rotation,
        layer=layer,
        lcsc=lcsc,
        pads=pads,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_kicad_pcb(path: str) -> List[KiCadFootprint]:
    """Parse a .kicad_pcb file and return a list of KiCadFootprint objects.

    Only footprints are extracted; tracks, zones, nets, setup, etc. are ignored.
    """
    with open(path, 'r', encoding='utf-8') as fh:
        text = fh.read()

    root = _parse_sexp(text)
    # root is (kicad_pcb ...) — its children include footprint nodes
    footprints: List[KiCadFootprint] = []
    for child in _children(root):
        if _tag(child) == 'footprint':
            fp = _parse_footprint(child)  # type: ignore[arg-type]
            if fp is not None:
                footprints.append(fp)

    return footprints


def footprint_pad_dict(
    fp: KiCadFootprint,
) -> Dict[str, Tuple[float, float, float, float]]:
    """Convenience: pads as {number: (x, y, w, h)} matching quality.py's PadGeom type."""
    return {pad.number: (pad.x, pad.y, pad.width, pad.height) for pad in fp.pads}
