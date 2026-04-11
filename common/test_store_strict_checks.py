"""Tests for strict-check exemption persistence in project store."""

import importlib
import logging
import sqlite3
import sys
import types
from types import SimpleNamespace


def _import_store_class():
    helpers_stub = types.ModuleType("helpers")
    setattr(
        helpers_stub,
        "dict_factory",
        lambda cursor, row: {
            description[0]: row[index]
            for index, description in enumerate(cursor.description)
        },
    )
    setattr(helpers_stub, "get_exclude_from_bom", lambda _fp: 0)
    setattr(helpers_stub, "get_exclude_from_pos", lambda _fp: 0)
    setattr(helpers_stub, "get_lcsc_value", lambda _fp: "")
    setattr(helpers_stub, "get_valid_footprints", lambda _board: [])
    setattr(
        helpers_stub,
        "natural_sort_collation",
        lambda left, right: (left > right) - (left < right),
    )
    sys.modules.setdefault("helpers", helpers_stub)
    return importlib.import_module("store").Store


def _build_store(tmp_path):
    Store = _import_store_class()
    store = Store.__new__(Store)
    store.logger = logging.getLogger(__name__)
    store.parent = SimpleNamespace(settings={})
    store.project_path = str(tmp_path)
    store.board = None
    store.datadir = str(tmp_path)
    store.dbfile = str(tmp_path / "project.db")
    store.order_by = "reference"
    store.order_dir = "ASC"
    store.create_db()
    return store


def _insert_part(store, reference="R1", lcsc="C1000"):
    with sqlite3.connect(store.dbfile) as con:
        con.execute(
            "INSERT INTO part_info(reference, value, footprint, lcsc, stock, exclude_from_bom, exclude_from_pos)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (reference, "10K", "R_0603_1608Metric", lcsc, None, 0, 0),
        )


def test_store_strict_check_exemption_roundtrip(tmp_path):
    """Exemptions can be set, read and cleared per reference+LCSC."""
    store = _build_store(tmp_path)

    store.set_strict_check_exemption("R1", "C123", "value", True)
    store.set_strict_check_exemption("R1", "C123", "footprint", True)

    exemptions = store.get_strict_check_exemptions("R1", "C123")
    assert exemptions == {"value": True, "footprint": True}

    store.set_strict_check_exemption("R1", "C123", "value", False)
    exemptions = store.get_strict_check_exemptions("R1", "C123")
    assert exemptions == {"value": False, "footprint": True}


def test_store_set_lcsc_clears_exemptions_when_lcsc_changes(tmp_path):
    """Changing LCSC clears strict-check exemptions for the part."""
    store = _build_store(tmp_path)
    _insert_part(store, reference="R2", lcsc="C111")

    store.set_strict_check_exemption("R2", "C111", "value", True)
    assert store.get_strict_check_exemptions("R2", "C111")["value"] is True

    store.set_lcsc("R2", "C222")

    assert store.get_strict_check_exemptions("R2", "C111") == {
        "value": False,
        "footprint": False,
    }


def test_store_set_lcsc_keeps_exemptions_when_lcsc_unchanged(tmp_path):
    """Re-saving same LCSC leaves existing exemptions untouched."""
    store = _build_store(tmp_path)
    _insert_part(store, reference="R3", lcsc="C333")

    store.set_strict_check_exemption("R3", "C333", "footprint", True)
    store.set_lcsc("R3", "C333")

    assert store.get_strict_check_exemptions("R3", "C333") == {
        "value": False,
        "footprint": True,
    }


def test_store_clear_strict_check_exemptions_can_target_single_lcsc(tmp_path):
    """Clearing exemptions by LCSC leaves other LCSC acknowledgements intact."""
    store = _build_store(tmp_path)

    store.set_strict_check_exemption("R4", "C444", "value", True)
    store.set_strict_check_exemption("R4", "C555", "footprint", True)
    store.clear_strict_check_exemptions("R4", "C444")

    assert store.get_strict_check_exemptions("R4", "C444") == {
        "value": False,
        "footprint": False,
    }
    assert store.get_strict_check_exemptions("R4", "C555") == {
        "value": False,
        "footprint": True,
    }
