"""Tests for jlcfootprint.naming: family extraction and the (family, token) rotation table."""

import pytest

from jlcfootprint.naming import (
    CATHODE_PIN1_FAMILIES,
    POLARIZED_CAP_FAMILIES,
    extract_family,
    extract_orientation_tokens,
    parse_package_name,
)


@pytest.mark.parametrize(
    ("name", "family"),
    [
        ("C0603", "C"),
        ("LED0603-RD", "LED"),
        ("SOIC-8_L4.9-W3.9-P1.27-LS6.0-BL", "SOIC-8"),
        ("CAP-SMD_BD6.3-L6.6-W6.6-FD", "CAP-SMD"),
        ("SOT-23-3_L2.9-W1.6-P1.90-LS2.8-BR", "SOT-23-3"),
    ],
)
def test_extract_family(name, family):
    """The family is everything before the underscore, or the leading letters."""
    assert extract_family(name) == family


@pytest.mark.parametrize(
    ("name", "rotation", "source", "confidence"),
    [
        ("SOIC-8_L4.9-W3.9-P1.27-LS6.0-BL", 90, "naming_rule", "high"),
        ("SOT-23-3_L2.9-W1.6-P1.90-LS2.8-BR", 180, "naming_rule", "high"),
        ("QFN-48_L7.0-W7.0-P0.5-TL", 0, "naming_rule", "high"),
        ("SOT-223-3_L6.5-W3.5-P2.30-LS7.0-BR", 180, "naming_rule", "high"),
        ("CAP-SMD_BD6.3-L6.6-W6.6-FD", 0, "naming_rule", "high"),
        ("CAP-SMD_BD6.3-L6.6-W6.6-LS7.2-R-RD", 180, "naming_rule", "high"),
        ("LED-SMD_L1.6-W0.8-R-RD", 0, "naming_rule", "medium"),
        ("LED0603-RD_GREEN", 0, "naming_rule", "medium"),
        ("led0603-rd", 0, "naming_rule", "medium"),
        ("SOIC-8_L4.9-W3.9-P1.27-LS6.0-TR", 270, "naming_rule", "high"),
        ("CAP-SMD_BD5.0-L5.3-W5.3-T", 270, "naming_rule", "high"),
        ("CAP-SMD_BD5.0-L5.3-W5.3-B", 90, "naming_rule", "high"),
        ("SOD-882_L1.0-W0.6-RD", 0, "naming_rule", "medium"),
        ("LED0603-RD", 0, "naming_rule", "medium"),
        ("LED0603-FD", 180, "naming_rule", "medium"),
        ("SMA_L4.3-W2.6-LS5.2-RD", 0, "naming_rule", "medium"),
        ("VSON-10_L3.0-W3.0-TL-EP_TPS61230DRCR", 0, "naming_rule", "high"),
        ("C0603", 0, "family_default", "low"),
        ("R0402", 0, "family_default", "low"),
        ("USB-SMD_TYPECSMD8P-C9900161134", 0, "none", "none"),
        ("CONN-SMD_2P-P2.54-R-RD", 0, "none", "none"),
    ],
)
def test_parse_package_name(name, rotation, source, confidence):
    """Known names map to the rotations the crawler validated on the 56-part board."""
    result = parse_package_name(name)
    assert (
        result.rotation_correction,
        result.rotation_source,
        result.parser_confidence,
    ) == (
        rotation,
        source,
        confidence,
    )


def test_smf_is_treated_as_a_diode_family():
    """The Reddit TVS pair lives in SMF, which the crawler resolved through the LCSC category."""
    assert parse_package_name("SMF_L2.8-W1.8-LS3.7-RD").rotation_correction == 0
    assert parse_package_name("SMF_L2.8-W1.8-LS3.7-FD-1").rotation_correction == 180
    assert "SMF" in CATHODE_PIN1_FAMILIES


def test_lcsc_category_fallback_inverts_polarity_for_unlisted_diode_families():
    """A family the table does not know still inverts FD/RD when LCSC calls it a diode."""
    assert parse_package_name("SOD-110_L2.0-W1.2-FD").rotation_correction == 0
    assert (
        parse_package_name("SOD-110_L2.0-W1.2-FD", "Diodes").rotation_correction == 180
    )


def test_orientation_tokens_skip_dimensions_and_mpn_suffixes():
    """Dimensional tokens are not orientation, and MPN families yield nothing."""
    assert extract_orientation_tokens("SOIC-8_L4.9-W3.9-P1.27-LS6.0-BL") == ["BL"]
    assert extract_orientation_tokens("CAP-SMD_BD6.3-L6.6-W6.6-LS7.2-R-RD") == [
        "R",
        "RD",
    ]
    assert extract_orientation_tokens("HDR-TH_2P-P2.54-V-R") == []
    assert extract_orientation_tokens("CASE-B_3528") == []
    assert extract_orientation_tokens("CASE-A_3216") == []
    assert extract_family("CASE-B_3528") == "CASE-B"


def test_polarized_cap_families_are_not_cathode_families():
    """Electrolytics and tantalums keep the standard FD/RD mapping."""
    assert not (POLARIZED_CAP_FAMILIES & CATHODE_PIN1_FAMILIES)
