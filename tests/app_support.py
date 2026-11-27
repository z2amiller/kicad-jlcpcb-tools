"""A session over the in-memory board, with a recording view and catalog.

Session tests load the application layer for real, with no wx at all: the
board is a ``FakeBackend``, the store is the real ``Store`` reading the
session's snapshots, and the catalog and the notifier are the stand-ins
below.
"""

from collections.abc import Iterator, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from .wx_harness import load_siblings, module


class Notices(list):
    """The view's notifier: every notice the session sends, in order."""

    def notice(self, notice: Any) -> None:
        """Record a notice."""
        self.append(notice)

    @property
    def texts(self) -> list[str]:
        """Return each notice's text."""
        return [notice.text for notice in self]


class Catalog:
    """A parts catalog of listed parts and remembered preferences.

    ``failure`` makes every read raise it; ``save_failure`` makes saving
    preferences raise it.
    """

    def __init__(
        self,
        parts: Optional[dict[str, dict[str, Any]]] = None,  # noqa: UP045
        preferences: Optional[dict[tuple[str, str], str]] = None,  # noqa: UP045
    ) -> None:
        self.parts = dict(parts or {})
        self.preferences = dict(preferences or {})
        self.ready = True
        self.busy = False
        self.failure: Optional[BaseException] = None  # noqa: UP045
        self.save_failure: Optional[BaseException] = None  # noqa: UP045
        self.saved: list[list[tuple[str, str, str]]] = []
        self.reads: list[str] = []

    def available(self) -> bool:
        """Return whether the catalog is ready."""
        return self.ready

    def updating(self) -> bool:
        """Return whether Update library is running."""
        return self.busy

    def part_details(self, part: Any) -> dict[str, Any]:
        """Return a listed part's facts, or nothing."""
        self.reads.append(str(part))
        if self.failure is not None:
            raise self.failure
        return dict(self.parts.get(str(part), {}))

    def part_preference(self, footprint: str, value: str) -> Optional[str]:  # noqa: UP045
        """Return the remembered part for a footprint and value."""
        if self.failure is not None:
            raise self.failure
        return self.preferences.get((footprint, value))

    def save_part_preferences(self, preferences: Sequence[tuple[str, str, str]]) -> int:
        """Remember parts; count the keys whose part changed."""
        if self.save_failure is not None:
            raise self.save_failure
        self.saved.append(list(preferences))
        changed = 0
        for footprint, value, lcsc in preferences:
            if self.preferences.get((footprint, value)) != lcsc:
                self.preferences[(footprint, value)] = lcsc
                changed += 1
        return changed


@pytest.fixture
def app(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """Load the application layer, the backend and the store, without wx."""
    package = "_app_session_tests"
    with load_siblings(
        package,
        (
            "app.session",
            "app.assignments",
            "app.notify",
            "kicad_backend",
            "kicad_backend.fake",
            "store",
        ),
        {
            f"{package}.helpers": module(
                "helpers", natural_sort_collation=lambda a, b: (a > b) - (a < b)
            ),
            f"{package}.derive_params": module(
                "derive_params",
                params_for_part=lambda row: row.get("description", ""),
            ),
        },
    ) as modules:
        fake = modules["kicad_backend.fake"]

        def session(
            *components: Any,
            catalog: Optional[Catalog] = None,  # noqa: UP045
            settings: Optional[dict[str, Any]] = None,  # noqa: UP045
            store: bool = True,
        ) -> Any:
            backend = fake.FakeBackend(components)
            notices = Notices()
            made = modules["app.session"].PluginSession(
                backend, notices, catalog or Catalog(), board_token="fake-board"
            )
            made.settings = {} if settings is None else settings
            if store:
                parent = SimpleNamespace(
                    _read_board=made.read_board, _held_board=made.held_board
                )
                made.store = modules["store"].Store(parent, str(tmp_path))
            made.notices = notices
            return made

        yield SimpleNamespace(
            api=modules["kicad_backend"],
            fake=fake,
            assignments=modules["app.assignments"],
            notify=modules["app.notify"],
            session=session,
            Catalog=Catalog,
        )


def lcsc(session: Any, reference: str) -> str:
    """Return the LCSC field text of the first footprint with this reference."""
    component = session.backend.snapshot().by_reference(reference)[0]
    field = component.default.fields.get("LCSC")
    return field.text if field is not None else ""
