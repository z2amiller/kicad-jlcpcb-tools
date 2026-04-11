"""Dialog for reviewing strict part-check failures and exemptions."""

import wx  # pylint: disable=import-error
import wx.dataview  # pylint: disable=import-error

from .helpers import HighResWxSize


class StrictCheckDialog(wx.Dialog):
    """Show strict-check failures and allow selecting exemptions per issue."""

    def __init__(self, parent, failures: list[dict], continue_label: str):
        wx.Dialog.__init__(
            self,
            parent,
            id=wx.ID_ANY,
            title="Strict part checks",
            pos=wx.DefaultPosition,
            size=HighResWxSize(parent.window, wx.Size(1180, 760)),
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER | wx.MAXIMIZE_BOX,
        )
        self.parent = parent
        self.failures = failures

        self.issue_list = wx.dataview.DataViewListCtrl(
            self,
            wx.ID_ANY,
            style=wx.dataview.DV_ROW_LINES | wx.dataview.DV_MULTIPLE,
        )
        self.issue_list.AppendToggleColumn("Exempt", width=70)
        self.issue_list.AppendTextColumn("Ref", width=90)
        self.issue_list.AppendTextColumn("LCSC", width=110)
        self.issue_list.AppendTextColumn("Missing", width=110)
        self.issue_list.AppendTextColumn("Value", width=130)
        self.issue_list.AppendTextColumn("Footprint", width=160)
        self.issue_list.AppendTextColumn("LCSC Params", width=430)

        for failure in failures:
            self.issue_list.AppendItem(
                [
                    bool(failure.get("exempted", False)),
                    str(failure.get("reference", "")),
                    str(failure.get("lcsc", "")),
                    str(failure.get("check_type", "")).capitalize(),
                    str(failure.get("value", "")),
                    str(failure.get("footprint", "")),
                    str(failure.get("params_text", "")),
                ]
            )

        self.help_text = wx.StaticText(
            self,
            wx.ID_ANY,
            "Select issues to exempt for this reference+LCSC. Unselected issues remain blocking.",
        )

        button_sizer = self.CreateSeparatedButtonSizer(wx.OK | wx.CANCEL)
        ok_button = self.FindWindowById(wx.ID_OK)
        if ok_button is not None:
            ok_button.SetLabel(continue_label)

        root = wx.BoxSizer(wx.VERTICAL)
        root.Add(self.help_text, 0, wx.ALL, 8)
        root.Add(self.issue_list, 1, wx.ALL | wx.EXPAND, 8)
        if button_sizer is not None:
            root.Add(button_sizer, 0, wx.ALL | wx.EXPAND, 8)
        self.SetSizer(root)
        self.Layout()

    def get_selected_exemptions(self) -> list[tuple[str, str, str]]:
        """Return `(reference, lcsc, check_type)` tuples selected for exemption."""
        selected = []
        for row in range(self.issue_list.GetItemCount()):
            if not self.issue_list.GetToggleValue(row, 0):
                continue
            check_type = self.issue_list.GetTextValue(row, 3).strip().lower()
            selected.append(
                (
                    self.issue_list.GetTextValue(row, 1).strip(),
                    self.issue_list.GetTextValue(row, 2).strip(),
                    check_type,
                )
            )
        return selected
