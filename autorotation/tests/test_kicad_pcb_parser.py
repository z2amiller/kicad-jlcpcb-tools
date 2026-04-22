"""Tests for the test-only kicad_pcb s-expression parser.

Covers:
  - Synthetic fixture: happy-path tokenizer, parser, footprint extraction.
  - Real board smoke tests against fx-SixSeven.kicad_pcb.
"""
from __future__ import annotations

import os
import pytest
from typing import List

from autorotation.tests.kicad_pcb_parser import (
    KiCadFootprint,
    KiCadPad,
    _tokenize,
    _unescape,
    _parse_sexp,
    footprint_pad_dict,
    parse_kicad_pcb,
)


# ---------------------------------------------------------------------------
# Synthetic fixture
# ---------------------------------------------------------------------------

SYNTHETIC_PCB = r"""
(kicad_pcb
  (version 20260206)
  (general (thickness 1.6))

  # This is a comment — should be ignored

  (footprint "TestLib:SOT-23"
    (layer "F.Cu")
    (at 10.0 20.0 90)
    (property "Reference" "Q1"
      (at 0 -2.4 90)
      (layer "F.SilkS")
    )
    (property "Value" "2N3904"
      (at 0 2.4 90)
      (layer "F.Fab")
    )
    (property "LCSC" "C2830807"
      (at 0 0 0)
      (layer "F.SilkS")
      (hide yes)
    )
    (pad "1" smd roundrect
      (at -0.9375 -0.95 90)
      (size 1.475 0.6)
      (layers "F.Cu" "F.Mask" "F.Paste")
    )
    (pad "2" smd roundrect
      (at -0.9375 0.95 90)
      (size 1.475 0.6)
      (layers "F.Cu" "F.Mask" "F.Paste")
    )
    (pad "3" smd roundrect
      (at 0.9375 0 90)
      (size 1.475 0.6)
      (layers "F.Cu" "F.Mask" "F.Paste")
    )
  )

  (footprint "Capacitor_SMD:C_0603"
    (layer "F.Cu")
    (at 30.0 40.0)
    (property "Reference" "C1"
      (at 0 -1.0 0)
      (layer "F.SilkS")
    )
    (property "Value" "100n"
      (at 0 1.0 0)
      (layer "F.Fab")
    )
    (pad "1" smd rect
      (at -1.0 0.0)
      (size 0.8 1.0)
      (layers "F.Cu" "F.Mask" "F.Paste")
    )
    (pad "2" smd rect
      (at 1.0 0.0)
      (size 0.8 1.0)
      (layers "F.Cu" "F.Mask" "F.Paste")
    )
    (pad "" np_thru_hole circle
      (at 0.0 0.0)
      (size 0.5 0.5)
      (layers "*.Cu")
    )
  )

  (footprint "MechanicalParts:MountingHole"
    (layer "F.Cu")
    (at 5.0 5.0)
    (property "Reference" "H1"
      (at 0 0 0)
      (layer "F.SilkS")
    )
    (property "Value" "MountingHole"
      (at 0 0 0)
      (layer "F.Fab")
    )
  )
)
"""


# ---------------------------------------------------------------------------
# Tokenizer tests
# ---------------------------------------------------------------------------

class TestTokenizer:
    def test_basic_atoms_and_parens(self):
        tokens = _tokenize('(footprint "Lib:FP" (at 10.0 20.0))')
        assert tokens[0] == '('
        assert tokens[1] == 'footprint'
        assert tokens[2] == '"Lib:FP"'
        assert tokens[3] == '('
        assert tokens[4] == 'at'
        assert tokens[5] == '10.0'
        assert tokens[6] == '20.0'
        assert tokens[7] == ')'
        assert tokens[8] == ')'

    def test_comment_ignored(self):
        tokens = _tokenize('(a # comment\nb)')
        assert '# comment' not in tokens
        assert 'b' in tokens

    def test_quoted_string_with_escapes(self):
        tokens = _tokenize(r'"hello \"world\""')
        assert len(tokens) == 1
        assert tokens[0] == r'"hello \"world\""'

    def test_unescape(self):
        assert _unescape('"hello"') == 'hello'
        assert _unescape(r'"say \"hi\""') == 'say "hi"'
        assert _unescape(r'"back\\slash"') == 'back\\slash'

    def test_empty(self):
        assert _tokenize('') == []
        assert _tokenize('   ') == []


# ---------------------------------------------------------------------------
# Synthetic parser tests
# ---------------------------------------------------------------------------

class TestSyntheticParser:
    """Test footprint extraction from the synthetic PCB string."""

    @pytest.fixture(scope='class')
    def footprints(self) -> List[KiCadFootprint]:
        import io
        import tempfile
        import os
        # Write to a temp file and parse
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.kicad_pcb', delete=False, encoding='utf-8'
        ) as fh:
            fh.write(SYNTHETIC_PCB)
            tmp_path = fh.name
        try:
            fps = parse_kicad_pcb(tmp_path)
        finally:
            os.unlink(tmp_path)
        return fps

    def test_footprint_count(self, footprints):
        assert len(footprints) == 3

    def test_q1_reference(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert q1.reference == 'Q1'

    def test_q1_footprint_name(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert q1.footprint_name == 'TestLib:SOT-23'

    def test_q1_value(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert q1.value == '2N3904'

    def test_q1_placed_position(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert q1.placed_x == pytest.approx(10.0)
        assert q1.placed_y == pytest.approx(20.0)
        assert q1.placed_rotation == pytest.approx(90.0)

    def test_q1_layer(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert q1.layer == 'F.Cu'

    def test_q1_lcsc(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert q1.lcsc == 'C2830807'

    def test_q1_pads(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        assert len(q1.pads) == 3
        numbers = {p.number for p in q1.pads}
        assert numbers == {'1', '2', '3'}

    def test_q1_pad_positions(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        pad1 = next(p for p in q1.pads if p.number == '1')
        assert pad1.x == pytest.approx(-0.9375)
        assert pad1.y == pytest.approx(-0.95)
        assert pad1.width == pytest.approx(1.475)
        assert pad1.height == pytest.approx(0.6)

    def test_c1_no_lcsc(self, footprints):
        c1 = next(fp for fp in footprints if fp.reference == 'C1')
        assert c1.lcsc is None

    def test_c1_pads_skip_unnamed(self, footprints):
        """C1 has pads "1", "2", and unnamed "" which must be skipped."""
        c1 = next(fp for fp in footprints if fp.reference == 'C1')
        assert len(c1.pads) == 2
        assert {p.number for p in c1.pads} == {'1', '2'}

    def test_c1_default_rotation(self, footprints):
        """C1 has no rotation in its (at) — should default to 0.0."""
        c1 = next(fp for fp in footprints if fp.reference == 'C1')
        assert c1.placed_rotation == pytest.approx(0.0)

    def test_h1_no_pads(self, footprints):
        h1 = next(fp for fp in footprints if fp.reference == 'H1')
        assert h1.pads == []

    def test_footprint_pad_dict(self, footprints):
        q1 = next(fp for fp in footprints if fp.reference == 'Q1')
        d = footprint_pad_dict(q1)
        assert set(d.keys()) == {'1', '2', '3'}
        x, y, w, h = d['1']
        assert x == pytest.approx(-0.9375)
        assert y == pytest.approx(-0.95)
        assert w == pytest.approx(1.475)
        assert h == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# Real board smoke tests
# ---------------------------------------------------------------------------

REAL_PCB_PATH = "/Users/andrewmiller/Documents/repos/fx-SixSeven/fx-SixSeven.kicad_pcb"

# Mark entire class to skip if real board is unavailable
_real_board_available = os.path.exists(REAL_PCB_PATH)


@pytest.mark.skipif(not _real_board_available, reason="real board not present")
class TestRealBoardSmoke:
    """Smoke tests against the real fx-SixSeven.kicad_pcb board."""

    @pytest.fixture(scope='class')
    def footprints(self) -> List[KiCadFootprint]:
        return parse_kicad_pcb(REAL_PCB_PATH)

    @pytest.fixture(scope='class')
    def fp_by_ref(self, footprints) -> dict:
        return {fp.reference: fp for fp in footprints}

    def test_found_many_footprints(self, footprints):
        # We don't know the exact count yet; just sanity-check it's substantial
        assert len(footprints) > 50, f"Expected >50 footprints, got {len(footprints)}"

    def test_q2_reference(self, fp_by_ref):
        assert 'Q2' in fp_by_ref

    def test_q2_footprint_name_contains_sot23(self, fp_by_ref):
        q2 = fp_by_ref['Q2']
        assert 'SOT-23' in q2.footprint_name

    def test_q2_lcsc(self, fp_by_ref):
        q2 = fp_by_ref['Q2']
        assert q2.lcsc == 'C2830807'

    def test_q2_placed_rotation(self, fp_by_ref):
        q2 = fp_by_ref['Q2']
        assert q2.placed_rotation == pytest.approx(90.0)

    def test_q2_three_pads(self, fp_by_ref):
        q2 = fp_by_ref['Q2']
        assert len(q2.pads) == 3

    def test_q2_pad_numbers(self, fp_by_ref):
        q2 = fp_by_ref['Q2']
        numbers = {p.number for p in q2.pads}
        assert numbers == {'1', '2', '3'}

    def test_c22_reference(self, fp_by_ref):
        assert 'C22' in fp_by_ref

    def test_c22_footprint_name_contains_handsolder(self, fp_by_ref):
        c22 = fp_by_ref['C22']
        assert 'HandSolder' in c22.footprint_name

    def test_c22_has_lcsc(self, fp_by_ref):
        c22 = fp_by_ref['C22']
        assert c22.lcsc == 'C28260'

    def test_u2_eight_pads(self, fp_by_ref):
        u2 = fp_by_ref['U2']
        assert len(u2.pads) == 8

    def test_u2_pad_numbers(self, fp_by_ref):
        u2 = fp_by_ref['U2']
        numbers = {p.number for p in u2.pads}
        assert numbers == {'1', '2', '3', '4', '5', '6', '7', '8'}

    def test_footprint_layer_values(self, footprints):
        """All footprints should have a layer string."""
        for fp in footprints:
            assert fp.layer in ('F.Cu', 'B.Cu'), (
                f"{fp.reference} has unexpected layer {fp.layer!r}"
            )

    def test_no_empty_references(self, footprints):
        """Every footprint should have a non-empty reference."""
        for fp in footprints:
            assert fp.reference != "", f"Footprint with name {fp.footprint_name!r} has empty reference"

    def test_print_summary(self, footprints, fp_by_ref):
        """Print a summary table for human inspection (always passes)."""
        total = len(footprints)
        with_lcsc = sum(1 for fp in footprints if fp.lcsc)
        print(f"\nTotal footprints: {total}")
        print(f"Footprints with LCSC: {with_lcsc}")
        print(f"Q2: ref={fp_by_ref['Q2'].reference!r} lcsc={fp_by_ref['Q2'].lcsc!r} "
              f"rot={fp_by_ref['Q2'].placed_rotation} pads={len(fp_by_ref['Q2'].pads)}")
        print(f"C22: ref={fp_by_ref['C22'].reference!r} lcsc={fp_by_ref['C22'].lcsc!r} "
              f"pads={len(fp_by_ref['C22'].pads)}")
        print(f"U2: pads={len(fp_by_ref['U2'].pads)}")
