"""Lightweight KiCad IPC transport client.

This module intentionally contains only a small stdlib-only JSON-RPC transport
wrapper. It does not change runtime behavior until the provider is updated to use
it in a later migration slice.
"""

import json
import os
from pathlib import Path
import socket
import sys
from typing import Any, Optional
import uuid


class KiCadIPCError(RuntimeError):
    """Raised when an IPC transport or RPC error occurs."""


class KiCadIPCClient:
    """Minimal JSON-RPC client for KiCad's scripting IPC server."""

    def __init__(self, socket_path: Optional[str] = None, timeout: float = 1.0):
        self.socket_path = socket_path or self.default_socket_path()
        self.timeout = timeout

    @staticmethod
    def default_socket_path() -> Optional[str]:
        """Return the default KiCad IPC socket path for the current platform."""
        api_socket = os.getenv("KICAD_API_SOCKET")
        if api_socket:
            return api_socket

        override = os.getenv("KICAD_IPC_SOCKET")
        if override:
            return override

        home = Path.home()
        if sys.platform == "darwin":
            return str(
                home
                / "Library"
                / "Application Support"
                / "kicad"
                / "scripting"
                / "kicad-ipc.sock"
            )
        if sys.platform.startswith("linux"):
            return str(
                home
                / ".local"
                / "share"
                / "kicad"
                / "scripting"
                / "kicad-ipc.sock"
            )
        return None

    def is_available(self) -> bool:
        """Check whether the IPC socket appears reachable."""
        if not self.socket_path:
            return False
        if not os.path.exists(self.socket_path):
            return False

        try:
            conn = self._open_socket()
        except OSError:
            return False
        conn.close()
        return True

    def call(self, method: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Send a JSON-RPC request and return the result payload."""
        if not method:
            raise ValueError("method is required")

        request_id = str(uuid.uuid4())
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {},
        }

        conn = self._open_socket()
        try:
            conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            response = self._read_response(conn)
        finally:
            conn.close()

        if response.get("id") not in (None, request_id):
            raise KiCadIPCError("IPC response id mismatch")

        error = response.get("error")
        if error:
            if isinstance(error, dict):
                message = error.get("message", "Unknown IPC error")
            else:
                message = str(error)
            raise KiCadIPCError(message)

        return response.get("result")

    def _open_socket(self) -> socket.socket:
        """Open and connect a Unix domain socket to the KiCad IPC server."""
        if not self.socket_path:
            raise KiCadIPCError("No KiCad IPC socket path is configured")

        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        conn.settimeout(self.timeout)
        conn.connect(self.socket_path)
        return conn

    def _read_response(self, conn: socket.socket) -> dict[str, Any]:
        """Read a single JSON-RPC response object from the socket."""
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break

        if not chunks:
            raise KiCadIPCError("No response received from KiCad IPC server")

        raw = b"".join(chunks).decode("utf-8").strip()
        try:
            response = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise KiCadIPCError("Invalid JSON response from KiCad IPC server") from exc

        if not isinstance(response, dict):
            raise KiCadIPCError("Unexpected IPC response payload")
        return response
