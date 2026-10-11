"""How the plugin words the footprint check for the user; pure and stdlib only."""

from __future__ import annotations


def describe_seconds(seconds: float) -> str:
    """Render a queue estimate as seconds under a minute, else minutes.

    Every estimate the plugin shows goes through it, the scan's log line and the
    generate-time wait among them, so they all read alike.
    """
    if seconds < 60:
        return f"{int(round(seconds))} s"
    return f"{int(round(seconds / 60.0))} min"
