"""Tests for the most-specific-match correction logic in Fabrication._find_correction."""

import importlib.util
from pathlib import Path
import sys
import types
from unittest.mock import MagicMock

_ROOT = Path(__file__).parent.parent

# Mock KiCad modules before importing fabrication
for _mod in ["pcbnew", "wx", "wx.dataview"]:
    sys.modules[_mod] = MagicMock()

# fabrication.py uses relative imports, so give it a fake parent package
_pkg = types.ModuleType("kicadplugin")
_pkg.__path__ = [str(_ROOT)]
sys.modules["kicadplugin"] = _pkg

_footprint_helpers = types.ModuleType("kicadplugin.footprint_helpers")
_footprint_helpers.get_is_dnp = lambda fp: False  # type: ignore[attr-defined]
sys.modules["kicadplugin.footprint_helpers"] = _footprint_helpers

_spec = importlib.util.spec_from_file_location(
    "kicadplugin.fabrication", _ROOT / "fabrication.py"
)
assert _spec is not None and _spec.loader is not None
_fab_mod = importlib.util.module_from_spec(_spec)
_fab_mod.__package__ = "kicadplugin"
sys.modules["kicadplugin.fabrication"] = _fab_mod
_spec.loader.exec_module(_fab_mod)  # type: ignore[union-attr]

Fabrication = _fab_mod.Fabrication  # type: ignore[attr-defined]


def make_fab(corrections):
    """Create a bare Fabrication instance with the given corrections list."""
    fab = object.__new__(Fabrication)
    fab.corrections = corrections
    return fab


# ---------------------------------------------------------------------------
# Most-specific-match conflict resolution
# ---------------------------------------------------------------------------


class TestFindCorrectionConflictResolution:
    """_find_correction prefers the pattern that consumes the most of the value."""

    def test_specific_pattern_wins_over_prefix(self):
        """SOT-23-3 correction wins over the shorter SOT-23 pattern."""
        fab = make_fab(
            [
                ("SOT-23", 10, (0.0, 0.0)),
                ("SOT-23-3", 20, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23-3")
        assert rotation == 20

    def test_shorter_pattern_still_matches_its_own_value(self):
        """SOT-23 correction is used when the value is exactly SOT-23."""
        fab = make_fab(
            [
                ("SOT-23", 10, (0.0, 0.0)),
                ("SOT-23-3", 20, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23")
        assert rotation == 10

    def test_order_in_list_does_not_matter(self):
        """The more specific pattern wins regardless of which is listed first."""
        fab = make_fab(
            [
                ("SOT-23-3", 20, (0.0, 0.0)),
                ("SOT-23", 10, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23-3")
        assert rotation == 20

    def test_three_way_conflict_most_specific_wins(self):
        """Most specific (longest exact-suffix) pattern wins in a three-way conflict."""
        fab = make_fab(
            [
                ("SOT", 5, (0.0, 0.0)),
                ("SOT-23", 10, (0.0, 0.0)),
                ("SOT-23-3", 20, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23-3")
        assert rotation == 20


# ---------------------------------------------------------------------------
# Substring matches
# ---------------------------------------------------------------------------


class TestFindCorrectionSubstringMatches:
    """A pattern that only matches part of the value is still a candidate."""

    def test_substring_match_used_when_no_conflict(self):
        """A substring pattern matches when it is the only candidate."""
        fab = make_fab([("SOT-23", 10, (0.0, 0.0))])
        rotation, _ = fab._find_correction("Package_TO_SOT_SMD:SOT-23")
        assert rotation == 10

    def test_no_match_returns_none(self):
        """Returns None when no pattern matches the value."""
        fab = make_fab([("SOT-23", 10, (0.0, 0.0))])
        assert fab._find_correction("QFP-100") is None

    def test_empty_corrections_returns_none(self):
        """Returns None when the corrections list is empty."""
        fab = make_fab([])
        assert fab._find_correction("SOT-23-3") is None


# ---------------------------------------------------------------------------
# Alternation patterns
# ---------------------------------------------------------------------------


class TestFindCorrectionAlternation:
    """Alternation patterns (|) are ranked by the branch that actually matched."""

    def test_alternation_matches_first_branch(self):
        """An alternation pattern matches the first branch correctly."""
        fab = make_fab([("SOT-23-3|SOT-23-5", 20, (0.0, 0.0))])
        rotation, _ = fab._find_correction("SOT-23-3")
        assert rotation == 20

    def test_alternation_matches_second_branch(self):
        """An alternation pattern matches the second branch correctly."""
        fab = make_fab([("SOT-23-3|SOT-23-5", 20, (0.0, 0.0))])
        rotation, _ = fab._find_correction("SOT-23-5")
        assert rotation == 20

    def test_alternation_loses_to_a_longer_match(self):
        """SOT-23-30 wins over the SOT-23-3 branch of an alternation."""
        fab = make_fab(
            [
                ("SOT-23-3|SOT-23-5", 20, (0.0, 0.0)),
                ("SOT-23-30", 30, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23-30")
        assert rotation == 30

    def test_alternation_matches_as_a_substring(self):
        """An alternation branch still matches inside a longer value."""
        fab = make_fab([("SOT-23-3|SOT-23-5", 20, (0.0, 0.0))])
        rotation, _ = fab._find_correction("Package_TO_SOT_SMD:SOT-23-3")
        assert rotation == 20


# ---------------------------------------------------------------------------
# Patterns that carry their own anchors
# ---------------------------------------------------------------------------


class TestFindCorrectionExistingAnchors:
    """Patterns that end with $ keep their own anchoring semantics."""

    def test_pre_anchored_pattern_matches_correctly(self):
        """A pattern ending in $ still matches correctly when wrapped."""
        fab = make_fab([("SOT-23$", 10, (0.0, 0.0))])
        rotation, _ = fab._find_correction("SOT-23")
        assert rotation == 10

    def test_pre_anchored_pattern_does_not_match_longer_value(self):
        """A pre-anchored SOT-23$ pattern does not match SOT-23-3."""
        fab = make_fab([("SOT-23$", 10, (0.0, 0.0))])
        assert fab._find_correction("SOT-23-3") is None

    def test_pre_anchored_and_bare_patterns_coexist(self):
        """Pre-anchored SOT-23$ and bare SOT-23-3 resolve correctly."""
        fab = make_fab(
            [
                ("SOT-23$", 10, (0.0, 0.0)),
                ("SOT-23-3", 20, (0.0, 0.0)),
            ]
        )
        assert fab._find_correction("SOT-23")[0] == 10
        assert fab._find_correction("SOT-23-3")[0] == 20


# ---------------------------------------------------------------------------
# Offset (position correction) passthrough
# ---------------------------------------------------------------------------


class TestFindCorrectionOffset:
    """_find_correction returns the offset tuple as well as rotation."""

    def test_offset_returned_with_rotation(self):
        """Both rotation and offset are returned in the result tuple."""
        fab = make_fab([("SOT-23-3", 45, (1.5, -0.5))])
        rotation, offset = fab._find_correction("SOT-23-3")
        assert rotation == 45
        assert offset == (1.5, -0.5)


# ---------------------------------------------------------------------------
# Specificity is measured on the match, not on the pattern
# ---------------------------------------------------------------------------


class TestFindCorrectionSpecificityIsMatchLength:
    """Ranking uses how much of the value a pattern consumed, not its own length."""

    def test_zero_width_lookahead_does_not_win_on_pattern_length(self):
        """^SOP-4_ beats the longer ^SOP-(?!18_), whose lookahead consumes nothing.

        These are both shipped corrections.  ^SOP-(?!18_) carves out SOP-18 but
        not SOP-4, so ranking by pattern length would hand every SOP-4 footprint
        the generic 270 degree correction instead of its own 0 degrees.
        """
        fab = make_fab(
            [
                ("^SOP-(?!18_)", 270, (0.0, 0.0)),
                ("^SOP-4_", 0, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOP-4_4.4x4.4mm_P2.54mm")
        assert rotation == 0

    def test_lookahead_still_applies_to_the_packages_it_covers(self):
        """A SOP-8 footprint still gets the generic correction."""
        fab = make_fab(
            [
                ("^SOP-(?!18_)", 270, (0.0, 0.0)),
                ("^SOP-4_", 0, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOP-8_5.2x5.3mm_P1.27mm")
        assert rotation == 270

    def test_short_suffix_pattern_loses_to_a_longer_prefix_match(self):
        """A two-character suffix pattern no longer beats a full-name pattern.

        The old anchored-first pass preferred any pattern matching the end of
        the value, so 'BR' outranked a pattern matching the whole footprint
        name.  Consuming more of the value now decides it.
        """
        fab = make_fab(
            [
                ("BR", 180, (0.0, 0.0)),
                ("^SOT-23-3_L2.9-W1.3-P1.90-LS2.4-BR", 0, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23-3_L2.9-W1.3-P1.90-LS2.4-BR")
        assert rotation == 0

    def test_equal_length_matches_keep_database_order(self):
        """Two patterns that consume the same text resolve to the first one."""
        fab = make_fab(
            [
                ("SOT-23", 10, (0.0, 0.0)),
                ("SOT.23", 20, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23")
        assert rotation == 10
