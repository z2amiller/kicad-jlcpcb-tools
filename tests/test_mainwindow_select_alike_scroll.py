"""Regression tests for footprint-list scrolling during auto-select-alike.

Selecting the alike rows leaves the last of them focused, and the list scrolls
the focused row into view once the click that triggered the selection has been
handled - which drags the row the user clicked out from under the mouse
pointer. Focusing the clicked row instead points that scroll somewhere already
on screen.

``mainwindow.py`` only imports with a full set of GUI stubs in place, so these
tests reuse the harness that ``test_mainwindow_stale_footprints`` builds rather
than standing up a second copy of it.
"""

from unittest.mock import MagicMock

import pytest

from .test_mainwindow_stale_footprints import JLCPCBTools, mainwindow


class _Item:
    """Stand-in for a wx.dataview.DataViewItem."""

    def __init__(self, row):
        self.row = row

    def IsOk(self):
        """Report the item as valid, as a real model item would."""
        return True


class _FakeList:
    """A list that scrolls its focused row into view after a click.

    The scroll is deferred rather than immediate because that is what the real
    control does: it lands after the click has been handled, which is why
    ``settle`` is separate from the calls that change the selection.
    """

    def __init__(self, rows, page, top=0):
        self.items = [_Item(row) for row in range(rows)]
        self.page = page
        self.top = top
        self.selected = set()
        self.current = None
        self.set_selections_calls = 0

    def settle(self):
        """Scroll the focused row into view, as the platform does post-click."""
        if self.current is None:
            return
        if self.current < self.top:
            self.top = self.current
        elif self.current >= self.top + self.page:
            self.top = self.current - self.page + 1

    def GetSelection(self):
        """Return the single selected row."""
        return self.items[min(self.selected)]

    def GetSelectedItemsCount(self):
        """Return the size of the current selection."""
        return len(self.selected)

    def Select(self, item):
        """Select one more row, and focus it."""
        self.selected.add(item.row)
        self.current = item.row

    def SetSelections(self, items):
        """Replace the selection, leaving the last row of it focused."""
        self.set_selections_calls += 1
        self.selected = {item.row for item in items}
        if items:
            self.current = max(item.row for item in items)

    def SetCurrentItem(self, item):
        """Focus a row without changing the selection."""
        self.current = item.row


@pytest.fixture(autouse=True)
def _item_array(monkeypatch):
    """Stand in for the wx item array the selection is handed as."""
    monkeypatch.setattr(mainwindow.dv, "DataViewItemArray", list, raising=False)


def _window(footprint_list, alike_rows):
    """Build the state surface select_alike_parts touches."""
    window = object.__new__(JLCPCBTools)
    window.footprint_list = footprint_list
    window.logger = MagicMock()
    window.select_alike_in_progress = False
    window.partlist_data_model = MagicMock()
    window.partlist_data_model.select_alike.return_value = [
        footprint_list.items[row] for row in alike_rows
    ]
    return window


def test_matches_below_the_fold_do_not_scroll_the_clicked_row_away():
    """Alike rows further down the list must leave the viewport alone."""
    footprint_list = _FakeList(rows=300, page=25, top=0)
    footprint_list.Select(footprint_list.items[7])
    window = _window(footprint_list, alike_rows=[7, 47, 187, 247])

    JLCPCBTools.select_alike_parts(window)
    footprint_list.settle()

    assert footprint_list.top == 0
    assert footprint_list.selected == {7, 47, 187, 247}


def test_matches_above_the_fold_do_not_scroll_the_clicked_row_away():
    """Alike rows further up the list must leave the viewport alone."""
    footprint_list = _FakeList(rows=300, page=25, top=150)
    footprint_list.Select(footprint_list.items[152])
    window = _window(footprint_list, alike_rows=[3, 92, 152])

    JLCPCBTools.select_alike_parts(window)
    footprint_list.settle()

    assert footprint_list.top == 150
    assert footprint_list.selected == {3, 92, 152}


def test_the_clicked_row_keeps_the_focus():
    """Focus drives the platform scroll, so it must stay on the clicked row."""
    footprint_list = _FakeList(rows=300, page=25, top=150)
    footprint_list.Select(footprint_list.items[152])
    window = _window(footprint_list, alike_rows=[152, 260])

    JLCPCBTools.select_alike_parts(window)

    assert footprint_list.current == 152


def test_the_selection_is_replaced_in_a_single_call():
    """One selection change, not one per row, so the list reacts once."""
    footprint_list = _FakeList(rows=300, page=25, top=0)
    footprint_list.Select(footprint_list.items[7])
    window = _window(footprint_list, alike_rows=[7, 47, 187])

    JLCPCBTools.select_alike_parts(window)

    assert footprint_list.set_selections_calls == 1


def test_an_existing_multi_row_selection_is_left_alone():
    """Expanding an already-expanded selection would have nothing to start from."""
    footprint_list = _FakeList(rows=300, page=25, top=0)
    footprint_list.Select(footprint_list.items[7])
    footprint_list.Select(footprint_list.items[47])
    window = _window(footprint_list, alike_rows=[7, 47, 187])

    JLCPCBTools.select_alike_parts(window)

    assert footprint_list.selected == {7, 47}
    assert footprint_list.set_selections_calls == 0
    assert window.logger.warning.called
