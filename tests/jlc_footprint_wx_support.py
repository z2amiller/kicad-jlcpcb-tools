"""Builders the JLC footprint check's plugin-root tests share.

``jlcfootprint_support`` and its fetch and controller siblings hold the pure
package's helpers; this module holds what the tests around the plugin root share:
the storage doubles the CPL tests run against and the duck-typed decision they pass
through ``prepare_cpl``.

Everything here is a builder, not a fixture, so a test file keeps naming its own
fixtures and pytest keeps reporting them against that file.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from .correction_test_support import make_library
from .wx_harness import load_correction_modules, module


def cpl_decision(
    rotation: Any = None,
    *,
    source: str = "derived",
    status: str = "green",
    light: Any = None,
    fit: Any = "fits",
    note: str = "",
    pending: bool = False,
    lcsc: str = "C123",
    body_excess: Any = None,
    origin: Any = None,
) -> SimpleNamespace:
    """Return what the controller's decision carries for one reference.

    Duck-typed on purpose: ``prepare_cpl`` takes whatever the check hands it, and a
    real ``Decision`` always has every field, so it could not stand for a decision
    that lacks one.
    """
    return SimpleNamespace(
        rotation=rotation,
        source=source,
        status=status,
        polarity_light=light,
        fit=fit,
        note=note,
        pending=pending,
        lcsc=lcsc,
        body_excess=body_excess,
        origin=origin,
    )


@contextmanager
def fabrication_modules(package: str, point: Any) -> Iterator[SimpleNamespace]:
    """Keep real storage and placement modules registered with one set of doubles."""
    pcbnew = MagicMock()
    pcbnew.FromMM = lambda value: value
    pcbnew.ToMM = lambda value: value
    pcbnew.wxPoint = point
    pcbnew.VECTOR2I = point
    with load_correction_modules(
        package=package,
        pcbnew=pcbnew,
        names=("fabrication",),
        replacements={
            f"{package}.footprint_helpers": module(
                f"{package}.footprint_helpers", get_is_dnp=lambda _footprint: False
            )
        },
    ) as loaded:
        yield loaded


def correction_library(modules: SimpleNamespace, tmp_path: Any) -> Any:
    """Create actual SQLite correction storage away from user databases."""
    return make_library(modules.library, tmp_path)
