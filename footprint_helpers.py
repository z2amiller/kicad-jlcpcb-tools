"""Helpers for reading KiCad footprint and board state."""

from collections.abc import Iterator
from typing import Any


def iter_board_items(container: Any) -> Iterator[Any]:
    """Yield the concrete board items held by a pcbnew container."""
    try:
        items = list(container)
    except AttributeError:
        items = []
        for index in range(len(container)):
            item = container[index]
            cast = getattr(item, "Cast", None)
            items.append(cast() if callable(cast) else item)
    return iter(items)


def get_is_dnp(footprint):
    """Get the runtime 'Do not place' state of a footprint."""
    if not footprint:
        return False
    is_dnp = getattr(footprint, "IsDNP", None)
    if not callable(is_dnp):
        return False
    return bool(is_dnp())
