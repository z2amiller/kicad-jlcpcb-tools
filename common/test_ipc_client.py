"""Tests for the KiCad IPC transport scaffold."""

import json
from unittest.mock import patch

import pytest

from ipc_client import KiCadIPCClient, KiCadIPCError


class FakeSocket:
    """Minimal socket double for JSON-RPC client tests."""

    def __init__(self, *responses: bytes):
        self.responses = list(responses)
        self.sent = []
        self.closed = False

    def sendall(self, data: bytes) -> None:
        """Record sent payloads."""
        self.sent.append(data)

    def recv(self, _size: int) -> bytes:
        """Return the next response chunk."""
        if self.responses:
            return self.responses.pop(0)
        return b""

    def close(self) -> None:
        """Mark socket closed."""
        self.closed = True


def test_default_socket_path_uses_env_override(monkeypatch):
    """Environment override should win over platform defaults."""
    monkeypatch.setenv("KICAD_IPC_SOCKET", "/tmp/kicad-ipc-test.sock")

    assert KiCadIPCClient.default_socket_path() == "/tmp/kicad-ipc-test.sock"


def test_is_available_false_when_socket_missing(monkeypatch):
    """Availability should be false if the socket path does not exist."""
    client = KiCadIPCClient(socket_path="/tmp/missing-kicad-ipc.sock")
    monkeypatch.setattr("ipc_client.os.path.exists", lambda _path: False)

    assert client.is_available() is False


def test_call_sends_json_rpc_request_and_returns_result():
    """call() should send a JSON-RPC request and return the result payload."""
    fake_socket = FakeSocket(
        b'{"jsonrpc":"2.0","id":"fixed-id","result":{"status":"ok"}}\n'
    )
    client = KiCadIPCClient(socket_path="/tmp/kicad-ipc.sock")

    with (
        patch.object(client, "_open_socket", return_value=fake_socket),
        patch("ipc_client.uuid.uuid4", return_value="fixed-id"),
    ):
        result = client.call("board.get_open", {"include": ["path"]})

    assert result == {"status": "ok"}
    assert fake_socket.closed is True

    payload = json.loads(fake_socket.sent[0].decode("utf-8"))
    assert payload == {
        "jsonrpc": "2.0",
        "id": "fixed-id",
        "method": "board.get_open",
        "params": {"include": ["path"]},
    }


def test_call_raises_for_rpc_error():
    """JSON-RPC error payloads should surface as KiCadIPCError."""
    fake_socket = FakeSocket(
        b'{"jsonrpc":"2.0","id":"fixed-id","error":{"message":"boom"}}\n'
    )
    client = KiCadIPCClient(socket_path="/tmp/kicad-ipc.sock")

    with (
        patch.object(client, "_open_socket", return_value=fake_socket),
        patch("ipc_client.uuid.uuid4", return_value="fixed-id"),
        pytest.raises(KiCadIPCError, match="boom"),
    ):
        client.call("board.get_open")
