"""What the application layer tells the user.

An operation returns its ``Message``s with its result, for the view to show
with the rows that changed. A ``Notice`` reports a problem an operation met,
through the view's ``Notifier``: the view implements it, and only notices
reach it today.
"""

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class Message:
    """A line for the user's log: ``level`` is "info" or "warning"."""

    level: str
    text: str

    @classmethod
    def info(cls, text: str) -> "Message":
        """Report what an operation did."""
        return cls("info", text)

    @classmethod
    def warning(cls, text: str) -> "Message":
        """Report what an operation left out, and why."""
        return cls("warning", text)


@dataclass(frozen=True)
class Notice:
    """A problem an operation met, with the error behind it.

    ``blocking`` means the project cannot be edited until the problem is
    fixed: the board was closed or replaced, or holds a write that could
    not be undone. Other notices leave editing available.
    """

    text: str
    error: Optional[BaseException] = None  # noqa: UP045
    blocking: bool = False


class Notifier(Protocol):
    """What a view is told without asking for it."""

    def notice(self, notice: Notice) -> None:
        """Show a problem an operation met."""
        ...
