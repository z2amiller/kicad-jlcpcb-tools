#!/usr/bin/env python3

"""Progress bar management for nested operations.

Provides abstract and concrete implementations for tracking nested progress
during file operations.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import contextmanager

import tqdm


class NestedProgressBar(ABC):
    """Abstract base class for nested progress bar management.

    Provides context manager methods for managing outer and inner progress bars
    with automatic cleanup.
    """

    @abstractmethod
    def outer(self, total: int, description: str = ""):
        """Context manager for outer progress bar.

        Args:
            total: Total number of items for outer progress.
            description: Description for the outer progress bar.

        Yields:
            ProgressCallback: Object with update() method for outer progress.

        """

    @abstractmethod
    def inner(self, total: int | None = None, description: str = ""):
        """Context manager for inner progress bar.

        Args:
            total: Total number of items for inner progress (default: None).
            description: Description for the inner progress bar.

        Yields:
            ProgressCallback: Object with update() and set_total() methods.

        """


class ProgressCallback:
    """Helper class to track progress updates."""

    def __init__(self, callback: Callable[[int], None]) -> None:
        """Initialize with a callback function.

        Args:
            callback: Function to call with increment amount.

        """
        self.callback = callback

    def update(self, increment: int = 1) -> None:
        """Update progress.

        Args:
            increment: Amount to advance.

        """
        self.callback(increment)

    def __call__(self, increment: int = 1) -> None:
        """Allow direct calling as function."""
        self.update(increment)


class InnerProgressCallback(ProgressCallback):
    """Extended progress callback with ability to update total."""

    def __init__(self, bar: tqdm.tqdm | None, callback: Callable[[int], None]) -> None:
        """Initialize with bar reference and callback.

        Args:
            bar: The tqdm progress bar instance.
            callback: Function to call with increment amount.

        """
        super().__init__(callback)
        self.bar = bar

    def set_total(self, total: int) -> None:
        """Update the total size.

        Args:
            total: New total size.

        """
        if self.bar is not None:
            self.bar.total = total


class TqdmNestedProgressBar(NestedProgressBar):
    """Nested progress bar implementation using tqdm with context managers."""

    def __init__(self) -> None:
        """Initialize the nested progress bar."""
        self.outer_bar: tqdm.tqdm | None = None
        self.inner_bar: tqdm.tqdm | None = None

    @contextmanager
    def outer(self, total: int, description: str = ""):
        """Context manager for outer progress bar.

        Usage:
            with progress.outer(10, "Processing") as pbar:
                for i in range(10):
                    pbar.update()

        Args:
            total: Total number of items for outer progress.
            description: Description for the outer progress bar.

        Yields:
            ProgressCallback: Object with update() method.

        """
        self.outer_bar = tqdm.tqdm(
            total=total,
            desc=description,
            unit="item",
            position=0,
        )
        try:

            def _update_outer(inc: int) -> None:
                if self.outer_bar is not None:
                    self.outer_bar.update(inc)

            yield ProgressCallback(_update_outer)
        finally:
            if self.outer_bar is not None:
                self.outer_bar.close()
                self.outer_bar = None

    @contextmanager
    def inner(self, total: int | None = None, description: str = ""):
        """Context manager for inner progress bar.

        Usage:
            with progress.inner(1024, "File") as pbar:
                pbar.update(256)
                pbar.set_total(2048)  # Update total if needed

        Args:
            total: Total number of items for inner progress.
            description: Description for the inner progress bar.

        Yields:
            InnerProgressCallback: Object with update() and set_total() methods.

        """
        if total is None:
            total = 0

        self.inner_bar = tqdm.tqdm(
            total=total,
            desc=description,
            unit_divisor=1024,
            miniters=1,
            leave=False,
            position=1,
        )
        try:

            def _update_inner(inc: int) -> None:
                if self.inner_bar is not None:
                    self.inner_bar.update(inc)

            yield InnerProgressCallback(self.inner_bar, _update_inner)
        finally:
            if self.inner_bar is not None:
                self.inner_bar.close()
                self.inner_bar = None


class NoOpProgressBar(NestedProgressBar):
    """No-operation progress bar that does nothing.

    Useful as a default when progress tracking is not desired.
    """

    @contextmanager
    def outer(self, total: int, description: str = ""):
        """Context manager for outer progress bar (no-op).

        Args:
            total: Total number of items for outer progress (ignored).
            description: Description for the outer progress bar (ignored).

        Yields:
            ProgressCallback: Silent callback object.

        """

        def _update_outer(inc: int) -> None:
            pass

        yield ProgressCallback(_update_outer)

    @contextmanager
    def inner(self, total: int | None = None, description: str = ""):
        """Context manager for inner progress bar (no-op).

        Args:
            total: Total number of items for inner progress (ignored).
            description: Description for the inner progress bar (ignored).

        Yields:
            InnerProgressCallback: Silent callback object.

        """

        class NoOpInnerCallback(InnerProgressCallback):
            """Silent inner callback."""

            def __init__(self) -> None:
                """Initialize silent callback."""
                super().__init__(None, lambda _: None)

            def set_total(self, total: int) -> None:
                """Update total (no-op)."""
                pass

        yield NoOpInnerCallback()
