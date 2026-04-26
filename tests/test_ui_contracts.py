"""Contract tests for core UI table wiring.

These tests avoid pixel assertions and instead lock index/schema contracts that
are easy to regress when changing DataView columns.
"""

# ruff: noqa: I001

from __future__ import annotations

import ast
import pathlib
import re


ROOT = pathlib.Path(__file__).resolve().parent.parent
MAINWINDOW_PATH = ROOT / "mainwindow.py"
DATAMODEL_PATH = ROOT / "datamodel.py"


def _extract_mainwindow_columns() -> list[tuple[str, int]]:
    """Return ``(label, model_index)`` pairs from DataView column declarations."""
    source = MAINWINDOW_PATH.read_text(encoding="utf-8")
    pattern = re.compile(
        r"Append(?:Text|IconText)Column\(\s*\"([^\"]+)\"\s*,\s*(\d+)",
        re.MULTILINE,
    )
    return [(label, int(index)) for label, index in pattern.findall(source)]


def _extract_partlist_columns_dict() -> dict[str, int]:
    """Parse ``PartListDataModel.__init__`` and return ``self.columns`` mapping."""
    source = DATAMODEL_PATH.read_text(encoding="utf-8")
    module = ast.parse(source)

    for node in module.body:
        if not isinstance(node, ast.ClassDef) or node.name != "PartListDataModel":
            continue
        for item in node.body:
            if not isinstance(item, ast.FunctionDef) or item.name != "__init__":
                continue
            for stmt in item.body:
                if not isinstance(stmt, ast.Assign):
                    continue
                if len(stmt.targets) != 1:
                    continue
                target = stmt.targets[0]
                if not isinstance(target, ast.Attribute):
                    continue
                if not isinstance(target.value, ast.Name) or target.value.id != "self":
                    continue
                if target.attr != "columns":
                    continue
                return ast.literal_eval(stmt.value)

    raise AssertionError("Unable to locate PartListDataModel.columns in datamodel.py")


def _extract_partlist_column_types() -> tuple[str, ...]:
    """Parse ``PartListDataModel.GetColumnType`` and return ``columntypes`` tuple."""
    source = DATAMODEL_PATH.read_text(encoding="utf-8")
    module = ast.parse(source)

    for node in module.body:
        if not isinstance(node, ast.ClassDef) or node.name != "PartListDataModel":
            continue
        for item in node.body:
            if not isinstance(item, ast.FunctionDef) or item.name != "GetColumnType":
                continue
            for stmt in item.body:
                if not isinstance(stmt, ast.Assign):
                    continue
                if len(stmt.targets) != 1:
                    continue
                target = stmt.targets[0]
                if not isinstance(target, ast.Name) or target.id != "columntypes":
                    continue
                value = ast.literal_eval(stmt.value)
                return tuple(value)

    raise AssertionError("Unable to locate PartListDataModel columntypes in datamodel.py")


def test_mainwindow_dataview_column_indices_are_stable():
    """Main table column labels should map to the expected model indices."""
    actual = dict(_extract_mainwindow_columns())

    expected = {
        "Ref": 0,
        "Value (Name)": 1,
        "Footprint": 2,
        "LCSC": 3,
        "Type": 4,
        "Stock": 5,
        "BOM": 6,
        "POS": 7,
        "POP": 8,
        "Correction": 9,
        "Side": 10,
        "LCSC Params": 11,
    }

    assert actual == expected


def test_partlist_model_columns_are_contiguous_zero_based_indices():
    """Part list model columns should remain contiguous from 0..N-1."""
    columns = _extract_partlist_columns_dict()
    indices = sorted(columns.values())

    assert indices == list(range(len(columns)))


def test_mainwindow_columns_and_model_indices_stay_in_sync():
    """The main window and PartListDataModel should expose the same index set."""
    mainwindow_indices = {index for _, index in _extract_mainwindow_columns()}
    model_indices = set(_extract_partlist_columns_dict().values())

    assert mainwindow_indices == model_indices


def test_partlist_model_column_types_match_ui_schema():
    """Part list model column types should remain aligned with UI expectations."""
    expected = (
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
        "wxDataViewIconText",
        "wxDataViewIconText",
        "wxDataViewIconText",
        "string",
        "wxDataViewIconText",
        "string",
    )

    assert _extract_partlist_column_types() == expected
