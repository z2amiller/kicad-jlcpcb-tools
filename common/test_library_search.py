"""Regression coverage for issue #578: short ohm searches returning no parts.

Exercise the real Library.search against the production FTS5 trigram schema.
Only wx imports and Library's filesystem/download initialization are bypassed.
These tests use the running interpreter's SQLite; the original report used
KiCad's SQLite 3.37.2 on macOS. On that runtime, the reported LIKE queries
without an ESCAPE clause return no rows for this catalog; the current escaped
queries return the expected parts.
"""

from contextlib import closing
import importlib
import logging
from pathlib import Path
import sqlite3
import sys
import types

import pytest

from common.partsdb import _CREATE_STATEMENTS

_ROOT = Path(__file__).resolve().parent.parent
_PACKAGE = "_issue_578_library_search_tests"


@pytest.fixture
def search_library(tmp_path, monkeypatch):
    """Load real search code with isolated GUI imports and a small catalog."""
    package = types.ModuleType(_PACKAGE)
    package.__path__ = [str(_ROOT)]
    monkeypatch.setitem(sys.modules, _PACKAGE, package)

    wx = types.ModuleType("wx")
    wx.__path__ = []
    wx.dataview = types.ModuleType("wx.dataview")
    monkeypatch.setitem(sys.modules, "wx", wx)
    monkeypatch.setitem(sys.modules, "wx.dataview", wx.dataview)

    try:
        module = importlib.import_module(f"{_PACKAGE}.library")
        library = module.Library.__new__(module.Library)
        library.logger = logging.getLogger(__name__)
        library.order_by = "LCSC Part"
        library.order_dir = "ASC"
        library.partsdb_file = str(tmp_path / "parts-fts5.db")

        # Synthetic catalog; the two reported part IDs anchor the regression.
        # Packages deliberately appear only in Package, as in generated catalogs.
        rows = [
            ("C17477", "0402", "0Ω ±1% Chip Resistor", "Resistors"),
            ("C578805", "0805", "0Ω ±1% Chip Resistor", "Resistors"),
            ("C578603", "0603", "0Ω ±1% Chip Resistor", "Resistors"),
            ("C25077", "0402", "10Ω ±1% Chip Resistor", "Resistors"),
            ("C578100", "0402", "100nF Ceramic Capacitor", "Capacitors"),
            *[
                (f"C57800{value}", "0402", f"{value}Ω ±1% Chip Resistor", "Resistors")
                for value in range(1, 10)
            ],
        ]
        with closing(sqlite3.connect(library.partsdb_file)) as con, con:
            for statement in _CREATE_STATEMENTS:
                con.execute(statement)
            con.executemany(
                'INSERT INTO parts ("LCSC Part", "Package", "Description", '
                '"First Category", "Library Type", "Stock") '
                "VALUES (?, ?, ?, ?, 'Basic', '1000')",
                rows,
            )

        yield library
    finally:
        # Imports of sibling modules must not leak this private package into
        # other tests. monkeypatch restores any pre-existing wx modules.
        for name in tuple(sys.modules):
            if name.startswith(f"{_PACKAGE}."):
                sys.modules.pop(name)


def _search_ids(library, keyword, **filters):
    """Return catalog IDs using the same default switches as the part selector."""
    parameters = {
        "keyword": keyword,
        "basic": True,
        "extended": True,
        "preferred": True,
        "stock": False,
        **filters,
    }
    return {row[0] for row in library.search(parameters)}


@pytest.mark.parametrize(
    ("keyword", "expected"),
    [
        pytest.param(
            "0Ω",
            {"C17477", "C578805", "C578603", "C25077"},
            id="zero-ohm-alone",
        ),
        pytest.param("0Ω 0805", {"C578805"}, id="zero-ohm-0805"),
        pytest.param("0Ω 0402", {"C17477", "C25077"}, id="zero-ohm-0402"),
    ],
)
def test_zero_ohm_search_returns_parts(search_library, keyword, expected):
    """Both short-only and mixed-length queries must find zero-ohm parts."""
    # Preserve substring search semantics: 0Ω also matches the 10Ω description.
    assert _search_ids(search_library, keyword) == expected, (
        f"Issue #578 regression with SQLite {sqlite3.sqlite_version}"
    )


@pytest.mark.parametrize("value", range(1, 10))
@pytest.mark.parametrize("suffix", ["", " 0402"], ids=["alone", "with-package"])
def test_single_digit_ohm_search_returns_parts(search_library, value, suffix):
    """All single-digit ohm values must work alone and with a package keyword."""
    assert _search_ids(search_library, f"{value}Ω{suffix}") == {f"C57800{value}"}, (
        f"Issue #578 regression with SQLite {sqlite3.sqlite_version}"
    )


@pytest.mark.parametrize("keyword", ["10Ω", "10Ω 0402"])
def test_two_digit_ohm_search_control(search_library, keyword):
    """Retain the successful longer query reported alongside the regression."""
    assert _search_ids(search_library, keyword) == {"C25077"}


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        pytest.param({"package": "0402"}, {"C17477", "C25077"}, id="package-0402"),
        pytest.param({"package": "0805"}, {"C578805"}, id="package-0805"),
        pytest.param(
            {"package": "0402", "category": "Resistors"},
            {"C17477", "C25077"},
            id="resistor-category",
        ),
        pytest.param(
            {"package": "0402", "category": "Capacitors"},
            set(),
            id="exclude-other-category",
        ),
    ],
)
def test_short_ohm_search_respects_filters(search_library, filters, expected):
    """Short keywords must still combine correctly with scoped MATCH filters."""
    assert _search_ids(search_library, "0Ω", **filters) == expected
