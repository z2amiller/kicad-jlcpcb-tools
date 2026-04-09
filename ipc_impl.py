"""IPC-backed adapter implementations.

These adapters satisfy the existing local adapter contracts while talking to the
KiCad IPC transport client. Provider selection is intentionally left unchanged in
this slice; these classes are exercised only via unit tests until the next step.
"""

import re
from typing import Any, Optional

from ipc_client import KiCadIPCClient
from kicad_api import (
    EXCLUDE_FROM_BOM,
    EXCLUDE_FROM_POS,
    BoardAPI,
    FootprintAPI,
    UtilityAPI,
)


class IPCPoint:
    """Simple point object compatible with x/y access and tuple unpacking."""

    def __init__(self, x: float, y: float):
        self.x = x
        self.y = y

    def __iter__(self):
        """Yield x/y coordinates for tuple-style unpacking."""
        yield self.x
        yield self.y


class IPCBoardAdapter(BoardAPI):
    """IPC implementation of board-level adapter operations."""

    def __init__(self, client: KiCadIPCClient):
        self.client = client
        self._board_cache: Optional[dict[str, Any]] = None

    def get_board(self) -> Any:
        """Get the active board payload from IPC."""
        if self._board_cache is None:
            board = self.client.call("board.get_open")
            self._board_cache = board if isinstance(board, dict) else {}
        return self._board_cache

    def get_board_filename(self) -> str:
        """Get the active board filename."""
        return str(self.get_board().get("path", ""))

    def get_all_footprints(self) -> list[Any]:
        """Get all footprints sorted by reference."""
        return sorted(
            self.get_footprints(),
            key=lambda footprint: str(footprint.get("reference", "")),
        )

    def get_footprints(self) -> list[Any]:
        """Get footprints in board iteration order."""
        board_id = self.get_board().get("id")
        footprints = self.client.call("board.list_footprints", {"board_id": board_id})
        return list(footprints or [])

    def get_footprint_by_reference(self, reference: str) -> Optional[dict[str, Any]]:
        """Find a footprint by reference designator."""
        for footprint in self.get_footprints():
            if str(footprint.get("reference", "")) == reference:
                return footprint
        board_id = self.get_board().get("id")
        result = self.client.call(
            "board.find_footprint_by_reference",
            {"board_id": board_id, "reference": reference},
        )
        if isinstance(result, dict):
            return result
        return None

    def get_enabled_layers(self) -> list[int]:
        """Get enabled layer ids."""
        board_id = self.get_board().get("id")
        return list(self.client.call("board.get_enabled_layers", {"board_id": board_id}) or [])

    def get_layer_name(self, layer_id: int) -> str:
        """Get the display name for a layer id."""
        board_id = self.get_board().get("id")
        return str(
            self.client.call(
                "board.get_layer_name", {"board_id": board_id, "layer_id": layer_id}
            )
        )

    def get_design_settings(self) -> Any:
        """Get board design settings payload."""
        board_id = self.get_board().get("id")
        return self.client.call("board.get_design_settings", {"board_id": board_id})

    def get_drawings(self) -> list[Any]:
        """Get drawing payloads."""
        board_id = self.get_board().get("id")
        return list(self.client.call("board.get_drawings", {"board_id": board_id}) or [])

    def refresh_display(self) -> None:
        """Refresh the KiCad UI if supported."""
        self.client.call("ui.refresh")

    def get_current_selection(self) -> list[Any]:
        """Get currently selected items."""
        board_id = self.get_board().get("id")
        return list(self.client.call("board.get_selection", {"board_id": board_id}) or [])

    def get_copper_layer_count(self) -> int:
        """Get copper layer count."""
        board = self.get_board()
        if "copper_layer_count" in board:
            return int(board["copper_layer_count"])
        board_id = board.get("id")
        return int(self.client.call("board.get_copper_layer_count", {"board_id": board_id}))

    def get_aux_origin(self) -> IPCPoint:
        """Get the auxiliary origin as a point."""
        board_id = self.get_board().get("id")
        payload = self.client.call("board.get_aux_origin", {"board_id": board_id})
        return _coerce_point(payload)


class IPCFootprintAdapter(FootprintAPI):
    """IPC implementation of footprint-level adapter operations."""

    def __init__(self, client: KiCadIPCClient):
        self.client = client

    def get_reference(self, footprint: Any) -> str:
        """Get footprint reference."""
        return str(_get_or_call(self.client, footprint, "reference", "footprint.get_reference"))

    def get_value(self, footprint: Any) -> str:
        """Get footprint value."""
        return str(_get_or_call(self.client, footprint, "value", "footprint.get_value"))

    def get_fpid_name(self, footprint: Any) -> str:
        """Get footprint library/package identifier."""
        return str(
            _get_or_call(
                self.client,
                footprint,
                "fpid_name",
                "footprint.get_fpid_name",
                fallback_keys=("footprint",),
            )
        )

    def get_layer(self, footprint: Any) -> int:
        """Get footprint layer id."""
        return int(_get_or_call(self.client, footprint, "layer", "footprint.get_layer"))

    def get_orientation(self, footprint: Any) -> float:
        """Get orientation in degrees."""
        return float(
            _get_or_call(
                self.client,
                footprint,
                "orientation",
                "footprint.get_orientation",
                fallback_keys=("orientation_degrees",),
            )
        )

    def get_position(self, footprint: Any) -> tuple[float, float]:
        """Get position in board units."""
        payload = _get_or_call(
            self.client,
            footprint,
            "position",
            "footprint.get_position",
        )
        point = _coerce_point(payload)
        return (point.x, point.y)

    def get_attributes(self, footprint: Any) -> int:
        """Get attribute bitmask."""
        payload = _get_or_call(self.client, footprint, "attributes", "footprint.get_attributes")
        return int(payload or 0)

    def set_attributes(self, footprint: Any, attributes: int) -> None:
        """Set attribute bitmask."""
        self.client.call(
            "footprint.set_attributes",
            {"footprint_id": _footprint_id(footprint), "attributes": attributes},
        )
        if isinstance(footprint, dict):
            footprint["attributes"] = attributes

    def get_lcsc_value(self, footprint: Any) -> str:
        """Get normalized LCSC field value if present."""
        fields = footprint.get("fields") if isinstance(footprint, dict) else None
        if fields is None:
            fields = self.client.call(
                "footprint.get_fields", {"footprint_id": _footprint_id(footprint)}
            )
        return _extract_lcsc(fields)

    def set_lcsc_value(self, footprint: Any, lcsc: str) -> None:
        """Set the LCSC field value."""
        self.client.call(
            "footprint.set_field",
            {
                "footprint_id": _footprint_id(footprint),
                "name": "LCSC",
                "value": lcsc,
                "visible": False,
            },
        )
        if isinstance(footprint, dict):
            fields = footprint.setdefault("fields", {})
            if isinstance(fields, dict):
                fields["LCSC"] = lcsc

    def get_exclude_from_pos(self, footprint: Any) -> bool:
        """Check exclude-from-POS state."""
        if isinstance(footprint, dict) and "exclude_from_pos" in footprint:
            return bool(footprint["exclude_from_pos"])
        return bool(self.get_attributes(footprint) & (1 << EXCLUDE_FROM_POS))

    def get_exclude_from_bom(self, footprint: Any) -> bool:
        """Check exclude-from-BOM state."""
        if isinstance(footprint, dict) and "exclude_from_bom" in footprint:
            return bool(footprint["exclude_from_bom"])
        return bool(self.get_attributes(footprint) & (1 << EXCLUDE_FROM_BOM))

    def get_is_dnp(self, footprint: Any) -> bool:
        """Check DNP state."""
        if isinstance(footprint, dict) and "is_dnp" in footprint:
            return bool(footprint["is_dnp"])
        return bool(self.client.call("footprint.get_is_dnp", {"footprint_id": _footprint_id(footprint)}))

    def set_selected(self, footprint: Any) -> None:
        """Select the footprint in the editor."""
        self.client.call("selection.add", {"footprint_id": _footprint_id(footprint)})

    def clear_selected(self, footprint: Any) -> None:
        """Deselect the footprint in the editor."""
        self.client.call("selection.remove", {"footprint_id": _footprint_id(footprint)})

    def toggle_exclude_from_pos(self, footprint: Any) -> bool:
        """Toggle exclude-from-POS and return the new state."""
        new_state = not self.get_exclude_from_pos(footprint)
        self.client.call(
            "footprint.set_exclude_from_pos",
            {"footprint_id": _footprint_id(footprint), "value": new_state},
        )
        if isinstance(footprint, dict):
            footprint["exclude_from_pos"] = new_state
        return new_state

    def toggle_exclude_from_bom(self, footprint: Any) -> bool:
        """Toggle exclude-from-BOM and return the new state."""
        new_state = not self.get_exclude_from_bom(footprint)
        self.client.call(
            "footprint.set_exclude_from_bom",
            {"footprint_id": _footprint_id(footprint), "value": new_state},
        )
        if isinstance(footprint, dict):
            footprint["exclude_from_bom"] = new_state
        return new_state

    def get_pads(self, footprint: Any) -> list[Any]:
        """Get footprint pads payloads."""
        if isinstance(footprint, dict) and "pads" in footprint:
            return list(footprint["pads"] or [])
        return list(
            self.client.call("footprint.get_pads", {"footprint_id": _footprint_id(footprint)}) or []
        )


class IPCUtilityAdapter(UtilityAPI):
    """IPC implementation of utility helpers."""

    def __init__(self, client: Optional[KiCadIPCClient] = None):
        self.client = client

    def from_mm(self, value: float) -> int:
        """Convert millimeters to board units."""
        return int(round(value * 1_000_000))

    def to_mm(self, value: int) -> float:
        """Convert board units to millimeters."""
        return float(value) / 1_000_000.0

    def get_layer_constants(self) -> dict[str, int]:
        """Return common layer ids matching current adapter expectations."""
        return {
            "F_Cu": 0,
            "B_Cu": 1,
            "F_SilkS": 2,
            "B_SilkS": 3,
            "F_Mask": 4,
            "B_Mask": 5,
            "F_Paste": 7,
            "B_Paste": 8,
            "Edge_Cuts": 9,
        }

    def get_pcb_constants(self) -> dict[str, Any]:
        """Return PCB object/type constants consumed by business logic."""
        constants: dict[str, Any] = dict(self.get_layer_constants())
        constants.update(
            {
                "PCB_TEXT": "PCB_TEXT",
                "PCB_SHAPE": "PCB_SHAPE",
                "S_RECT": 0,
            }
        )
        return constants

    def get_no_drill_shape(self) -> int:
        """Return no-drill-shape sentinel value."""
        return 0

    def get_plot_format_gerber(self) -> int:
        """Return Gerber plot format sentinel."""
        return 1

    def get_inner_cu_layer(self, layer: int) -> int:
        """Return an inner-layer id placeholder matching current expectations."""
        return layer

    def create_vector2i(self, x: int, y: int) -> IPCPoint:
        """Create a point-like value with x/y attributes."""
        return IPCPoint(x, y)

    def create_wx_point(self, x: float, y: float) -> IPCPoint:
        """Create a point-like value with x/y attributes."""
        return IPCPoint(x, y)

    def refill_zones(self, board: Any) -> None:
        """Request a zone refill via IPC when a client is available."""
        if self.client is None:
            return
        board_id = board.get("id") if isinstance(board, dict) else board
        self.client.call("board.refill_zones", {"board_id": board_id})


def _coerce_point(payload: Any) -> IPCPoint:
    """Normalize point payloads from IPC responses."""
    if isinstance(payload, IPCPoint):
        return payload
    if isinstance(payload, dict):
        return IPCPoint(float(payload.get("x", 0)), float(payload.get("y", 0)))
    if isinstance(payload, (tuple, list)) and len(payload) >= 2:
        return IPCPoint(float(payload[0]), float(payload[1]))
    return IPCPoint(0, 0)


def _footprint_id(footprint: Any) -> Any:
    """Return a stable footprint identifier for IPC calls."""
    if isinstance(footprint, dict) and "id" in footprint:
        return footprint["id"]
    return footprint


def _get_or_call(
    client: KiCadIPCClient,
    footprint: Any,
    key: str,
    method: str,
    fallback_keys: tuple[str, ...] = (),
) -> Any:
    """Get a property from a footprint payload or fetch it over IPC."""
    if isinstance(footprint, dict):
        if key in footprint:
            return footprint[key]
        for fallback_key in fallback_keys:
            if fallback_key in footprint:
                return footprint[fallback_key]
    return client.call(method, {"footprint_id": _footprint_id(footprint)})


def _extract_lcsc(fields: Any) -> str:
    """Extract a normalized LCSC code from a field payload."""
    if isinstance(fields, dict):
        items = fields.items()
    elif isinstance(fields, list):
        items = []
        for field in fields:
            if isinstance(field, dict):
                items.append((field.get("name", ""), field.get("text", "")))
    else:
        items = []

    for key, value in items:
        if re.match(r"lcsc|jlc", str(key), re.IGNORECASE) and re.match(
            r"^C\d+$", str(value)
        ):
            return str(value)
    return ""
