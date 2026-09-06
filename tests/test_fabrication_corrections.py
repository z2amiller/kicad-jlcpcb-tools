"""Tests for the two-pass correction matching logic in Fabrication._find_correction."""

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
_footprint_helpers.get_lcsc_value = lambda fp: getattr(  # type: ignore[attr-defined]
    fp, "lcsc", ""
)
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
# Anchored-first conflict resolution
# ---------------------------------------------------------------------------


class TestFindCorrectionConflictResolution:
    """_find_correction prefers exact-suffix (anchored) matches over substring matches."""

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
        """Anchored match wins regardless of which pattern is listed first."""
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
# Unanchored fallback
# ---------------------------------------------------------------------------


class TestFindCorrectionUnanchoredFallback:
    """When no anchored match exists, the first substring match is used."""

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
    """Alternation patterns (|) are wrapped so $ anchors all branches."""

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

    def test_alternation_anchored_does_not_match_extended_value(self):
        """SOT-23-3|SOT-23-5 does not match SOT-23-30 via the first branch."""
        fab = make_fab(
            [
                ("SOT-23-3|SOT-23-5", 20, (0.0, 0.0)),
                ("SOT-23-30", 30, (0.0, 0.0)),
            ]
        )
        rotation, _ = fab._find_correction("SOT-23-30")
        assert rotation == 30

    def test_alternation_falls_back_to_unanchored_when_needed(self):
        """Alternation pattern still matches as substring in the fallback pass."""
        fab = make_fab([("SOT-23-3|SOT-23-5", 20, (0.0, 0.0))])
        rotation, _ = fab._find_correction("Package_TO_SOT_SMD:SOT-23-3")
        assert rotation == 20


# ---------------------------------------------------------------------------
# Patterns that already carry anchors
# ---------------------------------------------------------------------------


class TestFindCorrectionExistingAnchors:
    """Patterns that already end with $ are wrapped as (?:pattern)$ harmlessly."""

    def test_pre_anchored_pattern_matches_correctly(self):
        """A pattern ending in $ still matches correctly when wrapped."""
        fab = make_fab([("SOT-23$", 10, (0.0, 0.0))])
        rotation, _ = fab._find_correction("SOT-23")
        assert rotation == 10

    def test_pre_anchored_pattern_does_not_match_longer_value(self):
        """A pre-anchored SOT-23$ pattern does not match SOT-23-3."""
        fab = make_fab([("SOT-23$", 10, (0.0, 0.0))])
        assert fab._find_correction("SOT-23-3") is None

    def test_pre_anchored_and_unanchored_coexist(self):
        """Pre-anchored SOT-23$ and unanchored SOT-23-3 resolve correctly."""
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
# Per-LCSC corrections take precedence over the regex table
# ---------------------------------------------------------------------------


class FakeOrientation:
    """Stand-in for the EDA_ANGLE returned by footprint.GetOrientation()."""

    def __init__(self, degrees):
        self._degrees = degrees

    def AsDegrees(self):
        """Return the orientation in degrees, like KiCad >= 6.99."""
        return self._degrees


class FakeFootprint:
    """Minimal footprint stub exposing only what the correction code reads."""

    def __init__(self, lcsc="", reference="U1", value="LM358", name="SOT-23-3"):
        self.lcsc = lcsc
        self._reference = reference
        self._value = value
        self._name = name

    def GetOrientation(self):
        """Footprints in these tests are always placed at 0 degrees."""
        return FakeOrientation(0)

    def GetLayer(self):
        """Top layer, so no bottom-side mirroring is applied."""
        return 0

    def GetReference(self):
        """Return the reference designator."""
        return self._reference

    def GetValue(self):
        """Return the footprint value."""
        return self._value

    def GetFPID(self):
        """Return self, since GetLibItemName is implemented here as well."""
        return self

    def GetLibItemName(self):
        """Return the footprint name."""
        return self._name


def make_fab_with_lcsc(corrections, lcsc_corrections):
    """Create a bare Fabrication instance holding both correction tables."""
    fab = make_fab(corrections)
    fab.lcsc_corrections = lcsc_corrections
    fab.logger = MagicMock()
    return fab


class TestFindLcscCorrection:
    """_find_lcsc_correction looks the footprint's part number up exactly."""

    def test_returns_the_correction_for_that_part(self):
        """A part with a stored correction resolves to it."""
        fab = make_fab_with_lcsc([], {"C12345": (90, (0.5, -0.5))})
        assert fab._find_lcsc_correction(FakeFootprint(lcsc="C12345")) == (
            90,
            (0.5, -0.5),
        )

    def test_returns_none_for_an_unknown_part(self):
        """A part with no stored correction resolves to None."""
        fab = make_fab_with_lcsc([], {"C12345": (90, (0.0, 0.0))})
        assert fab._find_lcsc_correction(FakeFootprint(lcsc="C99999")) is None

    def test_returns_none_when_the_footprint_has_no_lcsc(self):
        """A part with no LCSC number never matches an LCSC correction."""
        fab = make_fab_with_lcsc([], {"C12345": (90, (0.0, 0.0))})
        assert fab._find_lcsc_correction(FakeFootprint(lcsc="")) is None

    def test_matching_is_exact_not_a_prefix(self):
        """C1234 does not pick up the correction stored for C12345."""
        fab = make_fab_with_lcsc([], {"C12345": (90, (0.0, 0.0))})
        assert fab._find_lcsc_correction(FakeFootprint(lcsc="C1234")) is None

    def test_part_numbers_are_not_treated_as_patterns(self):
        """A stored key is compared literally, so regex syntax cannot match."""
        fab = make_fab_with_lcsc([], {"C.2345": (90, (0.0, 0.0))})
        assert fab._find_lcsc_correction(FakeFootprint(lcsc="C12345")) is None


class TestFixRotationPrecedence:
    """fix_rotation prefers the per-part correction over the regex table."""

    def test_lcsc_correction_wins_over_the_footprint_family(self):
        """The part-specific rotation overrides the family rotation."""
        fab = make_fab_with_lcsc(
            [("SOT-23-3", 180, (0.0, 0.0))], {"C12345": (90, (0.0, 0.0))}
        )
        assert fab.fix_rotation(FakeFootprint(lcsc="C12345")) == 90

    def test_zero_lcsc_correction_mutes_the_family_correction(self):
        """A 0 degree entry for one part suppresses its family correction."""
        fab = make_fab_with_lcsc(
            [("SOT-23-3", 180, (0.0, 0.0))], {"C12345": (0, (0.0, 0.0))}
        )
        assert fab.fix_rotation(FakeFootprint(lcsc="C12345")) == 0

    def test_sibling_part_keeps_the_family_correction(self):
        """Another part on the same footprint is unaffected.

        This is the case the regex table cannot express: both parts share a
        footprint name, so no pattern can tell them apart.
        """
        fab = make_fab_with_lcsc(
            [("SOT-23-3", 180, (0.0, 0.0))], {"C12345": (0, (0.0, 0.0))}
        )
        assert fab.fix_rotation(FakeFootprint(lcsc="C99999")) == 180

    def test_part_without_lcsc_keeps_the_family_correction(self):
        """A footprint with no LCSC number falls through to the regex table."""
        fab = make_fab_with_lcsc([("SOT-23-3", 180, (0.0, 0.0))], {})
        assert fab.fix_rotation(FakeFootprint(lcsc="")) == 180

    def test_lcsc_correction_wins_over_reference_and_value_rules(self):
        """The per-part rule outranks reference and value rules too."""
        fab = make_fab_with_lcsc(
            [("^U1$", 45, (0.0, 0.0)), ("^LM358$", 135, (0.0, 0.0))],
            {"C12345": (90, (0.0, 0.0))},
        )
        assert fab.fix_rotation(FakeFootprint(lcsc="C12345")) == 90


class TestFixPositionPrecedence:
    """fix_position resolves its offset with the same precedence."""

    def _offset_used(self, fab, footprint):
        """Run fix_position and report the offset it selected."""
        used = []
        fab.reposition = lambda fp, position, offset: used.append(offset)
        fab.fix_position(footprint, position=None)
        return used

    def test_lcsc_offset_wins_over_the_footprint_family(self):
        """The part-specific offset overrides the family offset."""
        fab = make_fab_with_lcsc(
            [("SOT-23-3", 0, (1.0, 1.0))], {"C12345": (0, (0.5, -0.5))}
        )
        assert self._offset_used(fab, FakeFootprint(lcsc="C12345")) == [(0.5, -0.5)]

    def test_part_without_lcsc_keeps_the_family_offset(self):
        """A footprint with no LCSC number falls through to the regex table."""
        fab = make_fab_with_lcsc([("SOT-23-3", 0, (1.0, 1.0))], {})
        assert self._offset_used(fab, FakeFootprint(lcsc="")) == [(1.0, 1.0)]

    def test_no_match_leaves_the_position_untouched(self):
        """With no matching correction the original position is returned."""
        fab = make_fab_with_lcsc([], {"C99999": (0, (1.0, 1.0))})
        sentinel = object()
        assert fab.fix_position(FakeFootprint(lcsc="C12345"), sentinel) is sentinel
