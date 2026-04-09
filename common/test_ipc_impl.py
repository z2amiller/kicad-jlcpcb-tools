"""Tests for IPC-backed adapter implementations."""

from ipc_impl import IPCBoardAdapter, IPCFootprintAdapter, IPCPoint, IPCUtilityAdapter


class FakeIPCClient:
    """Very small fake IPC client for adapter tests."""

    def __init__(self):
        self.calls = []
        self.responses = {
            "board.get_open": {
                "id": "board-1",
                "path": "/example/test-board.kicad_pcb",
                "copper_layer_count": 4,
            },
            "board.list_footprints": [
                {
                    "id": "fp-2",
                    "reference": "U1",
                    "value": "MCU",
                    "fpid_name": "QFN-32",
                    "layer": 0,
                    "orientation": 90.0,
                    "position": {"x": 1200, "y": 3400},
                    "fields": {"LCSC": "C1234"},
                    "exclude_from_bom": False,
                    "exclude_from_pos": True,
                    "is_dnp": False,
                    "pads": [{"id": "pad-1"}],
                },
                {
                    "id": "fp-1",
                    "reference": "R1",
                    "value": "10k",
                    "fpid_name": "0402",
                    "layer": 1,
                    "orientation": 180.0,
                    "position": {"x": 100, "y": 200},
                    "fields": {"JLC": "C55"},
                    "exclude_from_bom": True,
                    "exclude_from_pos": False,
                    "is_dnp": True,
                    "pads": [{"id": "pad-r1"}],
                },
            ],
            "board.get_enabled_layers": [0, 1, 9],
            "board.get_layer_name": "Edge_Cuts",
            "board.get_selection": [{"id": "fp-1"}],
            "board.get_aux_origin": {"x": 11, "y": 22},
            "board.get_drawings": [{"kind": "shape"}],
            "board.get_design_settings": {"use_aux_origin": True},
            "board.find_footprint_by_reference": {"id": "fp-1", "reference": "R1"},
        }

    def call(self, method, params=None):
        """Record a call and return a canned response."""
        self.calls.append((method, params or {}))
        return self.responses.get(method)


def test_ipc_board_adapter_maps_board_calls():
    """Board adapter should translate board methods into IPC calls."""
    client = FakeIPCClient()
    adapter = IPCBoardAdapter(client)

    assert adapter.get_board_filename() == "/example/test-board.kicad_pcb"
    assert adapter.get_copper_layer_count() == 4
    assert [fp["reference"] for fp in adapter.get_all_footprints()] == ["R1", "U1"]
    assert [fp["reference"] for fp in adapter.get_footprints()] == ["U1", "R1"]
    assert adapter.get_enabled_layers() == [0, 1, 9]
    assert adapter.get_layer_name(9) == "Edge_Cuts"
    assert adapter.get_current_selection() == [{"id": "fp-1"}]
    assert adapter.get_aux_origin().x == 11
    assert adapter.get_aux_origin().y == 22
    assert adapter.get_drawings() == [{"kind": "shape"}]
    assert adapter.get_design_settings() == {"use_aux_origin": True}
    assert adapter.get_footprint_by_reference("R1")["id"] == "fp-1"


def test_ipc_footprint_adapter_reads_metadata_and_fields():
    """Footprint adapter should expose metadata from IPC payloads."""
    client = FakeIPCClient()
    adapter = IPCFootprintAdapter(client)
    footprint = client.responses["board.list_footprints"][0]

    assert adapter.get_reference(footprint) == "U1"
    assert adapter.get_value(footprint) == "MCU"
    assert adapter.get_fpid_name(footprint) == "QFN-32"
    assert adapter.get_layer(footprint) == 0
    assert adapter.get_orientation(footprint) == 90.0
    assert adapter.get_position(footprint) == (1200.0, 3400.0)
    assert adapter.get_lcsc_value(footprint) == "C1234"
    assert adapter.get_exclude_from_pos(footprint) is True
    assert adapter.get_exclude_from_bom(footprint) is False
    assert adapter.get_is_dnp(footprint) is False
    assert adapter.get_pads(footprint) == [{"id": "pad-1"}]


def test_ipc_footprint_adapter_mutations_call_ipc():
    """Footprint mutation helpers should dispatch IPC calls and update local payloads."""
    client = FakeIPCClient()
    adapter = IPCFootprintAdapter(client)
    footprint = client.responses["board.list_footprints"][1]

    adapter.set_lcsc_value(footprint, "C777")
    assert footprint["fields"]["LCSC"] == "C777"

    assert adapter.toggle_exclude_from_pos(footprint) is True
    assert adapter.toggle_exclude_from_bom(footprint) is False
    adapter.set_selected(footprint)
    adapter.clear_selected(footprint)

    methods = [method for method, _params in client.calls]
    assert "footprint.set_field" in methods
    assert "footprint.set_exclude_from_pos" in methods
    assert "footprint.set_exclude_from_bom" in methods
    assert "selection.add" in methods
    assert "selection.remove" in methods


def test_ipc_utility_adapter_helpers_are_stable():
    """Utility adapter should provide deterministic helpers and point wrappers."""
    client = FakeIPCClient()
    adapter = IPCUtilityAdapter(client)

    assert adapter.from_mm(1.25) == 1250000
    assert adapter.to_mm(2500000) == 2.5
    assert adapter.get_layer_constants()["F_Cu"] == 0
    assert adapter.get_plot_format_gerber() == 1

    point = adapter.create_vector2i(10, 20)
    assert isinstance(point, IPCPoint)
    assert (point.x, point.y) == (10, 20)

    adapter.refill_zones({"id": "board-1"})
    assert client.calls[-1] == ("board.refill_zones", {"board_id": "board-1"})
