"""Behavior tests for PartListDataModel UI-facing state transitions."""

# ruff: noqa: D103
# ruff: noqa: I001

import importlib.util
import sys
import types
from pathlib import Path


ROOT = Path(__file__).parent.parent


class _FakePyDataViewModel:
    def ObjectToItem(self, obj):
        return obj

    def ItemToObject(self, item):
        return item

    def ItemAdded(self, _parent, _item):
        return None

    def ItemChanged(self, _item):
        return None

    def Cleared(self):
        return None


class _FakeDataViewIconText:
    def __init__(self, text, icon):
        self.text = text
        self.icon = icon


def _load_datamodel_module():
    """Load datamodel.py under a fake package with lightweight wx stubs."""
    wx_mod = types.ModuleType("wx")
    dv_mod = types.ModuleType("wx.dataview")
    dv_mod.PyDataViewModel = _FakePyDataViewModel  # type: ignore[attr-defined]
    dv_mod.DataViewIconText = _FakeDataViewIconText  # type: ignore[attr-defined]
    dv_mod.NullDataViewItem = object()  # type: ignore[attr-defined]
    wx_mod.dataview = dv_mod  # type: ignore[attr-defined]

    sys.modules["wx"] = wx_mod
    sys.modules["wx.dataview"] = dv_mod

    pkg = types.ModuleType("kicadplugin")
    pkg.__path__ = [str(ROOT)]
    sys.modules["kicadplugin"] = pkg

    helpers_mod = types.ModuleType("kicadplugin.helpers")
    helpers_mod.loadIconScaled = (  # type: ignore[attr-defined]
        lambda filename, scale=1.0: f"icon:{filename}:{scale}"
    )
    sys.modules["kicadplugin.helpers"] = helpers_mod

    spec = importlib.util.spec_from_file_location("kicadplugin.datamodel", ROOT / "datamodel.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "kicadplugin"
    sys.modules["kicadplugin.datamodel"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _sample_row():
    return [
        "R1",
        "10k",
        "Resistor_SMD:R_0603_1608Metric",
        "C123",
        "Basic",
        "100",
        "1",
        "1",
        "0",
        "0",
        "0",
        "0603 1%",
    ]


def test_partlist_add_entry_and_basic_accessors():
    datamodel = _load_datamodel_module()
    model = datamodel.PartListDataModel(scale_factor=1.0)
    model.AddEntry(_sample_row())

    assert len(model.get_all()) == 1

    item = model.ObjectToItem(model.get_all()[0])
    assert model.get_reference(item) == "R1"
    assert model.get_value(item) == "10k"
    assert model.get_lcsc(item) == "C123"


def test_partlist_toggle_bom_and_pos_changes_icon_state():
    datamodel = _load_datamodel_module()
    model = datamodel.PartListDataModel(scale_factor=1.0)
    model.AddEntry(_sample_row())

    item = model.ObjectToItem(model.get_all()[0])
    obj = model.ItemToObject(item)

    bom_before = obj[model.columns["BOM_COL"]]
    pos_before = obj[model.columns["POS_COL"]]

    model.toggle_bom(item)
    model.toggle_pos(item)

    assert obj[model.columns["BOM_COL"]] != bom_before
    assert obj[model.columns["POS_COL"]] != pos_before


def test_partlist_set_and_remove_lcsc_updates_expected_columns():
    datamodel = _load_datamodel_module()
    model = datamodel.PartListDataModel(scale_factor=1.0)
    model.AddEntry(_sample_row())

    item = model.ObjectToItem(model.get_all()[0])
    model.set_lcsc("R1", "C999999", "Extended", "42", "0402 5%")

    obj = model.ItemToObject(item)
    assert obj[model.columns["LCSC_COL"]] == "C999999"
    assert obj[model.columns["TYPE_COL"]] == "Extended"
    assert obj[model.columns["STOCK_COL"]] == "42"
    assert obj[model.columns["PARAMS_COL"]] == "0402 5%"

    model.remove_lcsc_number(item)
    assert obj[model.columns["LCSC_COL"]] == ""
    assert obj[model.columns["TYPE_COL"]] == ""
    assert obj[model.columns["STOCK_COL"]] == ""
    assert obj[model.columns["PARAMS_COL"]] == ""
