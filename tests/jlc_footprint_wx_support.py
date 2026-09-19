"""Builders the JLC footprint check's plugin-root and script tests share.

``jlcfootprint_support`` holds the pure package's fixtures; this module holds what
the tests around the plugin root need and had each written for themselves: the
fake-wx loaders for the facade and for the main window, the window double the
column, dialog and generate tests build on, the storage doubles the two CPL tests
run against, the duck-typed decision they pass through ``prepare_cpl``, and the
importer three script tests use to load a ``scripts/`` module without its CLI.

Everything here is a builder, not a fixture, so a test file keeps naming its own
fixtures and pytest keeps reporting them against that file.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import importlib.util
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from .correction_test_support import make_library
from .wx_harness import (
    ROOT,
    load,
    load_correction_modules,
    load_mainwindow,
    load_siblings,
    module,
    package_stubs,
    wx_stubs,
)


def jlcfootprint_enabled(settings: dict) -> bool:
    """Read the shipped setting the way the facade does (the harness stubs it off)."""
    return bool(settings.get("jlcfootprint", {}).get("enabled", True))


@contextmanager
def load_facade(package: str, **wx_symbols: Any) -> Iterator[SimpleNamespace]:
    """Load the real facade under a fake wx, with the real events module beside it.

    The events module is loaded rather than stubbed so its event names cannot drift
    out of step with a copy; ``wx_symbols`` adds to the two every caller needs.
    """
    stubs = wx_stubs(PostEvent=MagicMock(), Dialog=type("Dialog", (), {}), **wx_symbols)
    stubs.update(package_stubs(package))
    stubs[f"{package}.events"] = load(package, "events", stubs)
    with load_siblings(package, ("jlc_footprint_check",), stubs) as loaded:
        yield SimpleNamespace(
            module=loaded["jlc_footprint_check"],
            wx=stubs["wx"],
            events=stubs[f"{package}.events"],
        )


def facade_symbols(**overrides: Any) -> dict:
    """Return the facade functions the window imports: inert, but answering truthfully.

    Only ``is_footprint_check_enabled`` is a real function, because the window
    branches on it; the rest report the "nothing happened" value each action returns.
    """
    symbols = {
        "clear_cache": MagicMock(return_value=0),
        "create_footprint_check": MagicMock(),
        "is_footprint_check_enabled": jlcfootprint_enabled,
        "recheck_board": MagicMock(return_value=0),
        "refetch_references": MagicMock(return_value=0),
        "refresh_board_data": MagicMock(return_value=0),
        "show_generate_summary": MagicMock(return_value="summary"),
        "wait_for_pending_fetches": MagicMock(return_value=True),
    }
    symbols.update(overrides)
    return symbols


def load_window(package: str, facade: dict, **wx_symbols: Any) -> ModuleType:
    """Load ``mainwindow`` with the JLC facade faked and the fake wx a window needs."""
    return load_mainwindow(
        package,
        wx=wx_stubs(
            Frame=type("Frame", (), {}),
            NewIdRef=MagicMock(side_effect=object),
            **wx_symbols,
        ),
        jlc_footprint_check=facade,
    )


def check_window(main: ModuleType, check: Any, *, enabled: bool = True) -> Any:
    """Return a main window carrying one footprint-check double, and nothing else.

    ``__init__`` is skipped on purpose: these tests exercise one method at a time,
    so a window built by hand says exactly which attributes that method reads.
    """
    window = object.__new__(main.JLCPCBTools)
    window.settings = {"jlcfootprint": {"enabled": enabled}}
    window.jlc_footprint_check = check
    window.logger = MagicMock()
    return window


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
    test that has to build a real ``Decision`` cannot say "a decision missing this
    field" any more.
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


def load_script(name: str) -> ModuleType:
    """Import one ``scripts/`` module as a module, without running its CLI."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded
