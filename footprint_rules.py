"""Dialog for managing footprint highlight aliases and extraction rules."""

import wx  # pylint: disable=import-error
import wx.dataview  # pylint: disable=import-error

from .dataview_highlight import (
    DEFAULT_FOOTPRINT_ALIAS_FORWARD,
    DEFAULT_FOOTPRINT_EXTRACTION_RULES,
)
from .helpers import HighResWxSize


class FootprintRulesManagerDialog(wx.Dialog):
    """Manage footprint aliases and regex extraction rules used for highlighting."""

    def __init__(self, parent):
        wx.Dialog.__init__(
            self,
            parent,
            id=wx.ID_ANY,
            title="Footprint Highlight Rules",
            pos=wx.DefaultPosition,
            size=HighResWxSize(parent.window, wx.Size(900, 760)),
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER | wx.MAXIMIZE_BOX,
        )
        self.parent = parent

        quitid = wx.NewId()
        self.Bind(wx.EVT_MENU, self.quit_dialog, id=quitid)
        entries = [wx.AcceleratorEntry(), wx.AcceleratorEntry(), wx.AcceleratorEntry()]
        entries[0].Set(wx.ACCEL_CTRL, ord("W"), quitid)
        entries[1].Set(wx.ACCEL_CTRL, ord("Q"), quitid)
        entries[2].Set(wx.ACCEL_SHIFT, wx.WXK_ESCAPE, quitid)
        self.SetAcceleratorTable(wx.AcceleratorTable(entries))

        self.notebook = wx.Notebook(self)
        alias_panel = wx.Panel(self.notebook)
        rules_panel = wx.Panel(self.notebook)
        self.notebook.AddPage(alias_panel, "Aliases")
        self.notebook.AddPage(rules_panel, "Extraction Regex")

        self._build_alias_tab(alias_panel)
        self._build_rules_tab(rules_panel)
        self._load_from_settings()

        buttons = self.CreateSeparatedButtonSizer(wx.OK | wx.CANCEL)
        self.Bind(wx.EVT_BUTTON, self.save_and_close, id=wx.ID_OK)
        self.Bind(wx.EVT_BUTTON, self.quit_dialog, id=wx.ID_CANCEL)
        save_button = self.FindWindowById(wx.ID_OK)
        if save_button is not None:
            save_button.SetLabel("Save")

        root = wx.BoxSizer(wx.VERTICAL)
        root.Add(
            wx.StaticText(
                self,
                wx.ID_ANY,
                "Manage aliases and extraction regexes used for LCSC Params highlighting.",
            ),
            0,
            wx.ALL,
            8,
        )
        root.Add(self.notebook, 1, wx.ALL | wx.EXPAND, 8)
        if buttons is not None:
            root.Add(buttons, 0, wx.ALL | wx.EXPAND, 8)
        self.SetSizer(root)
        self.Layout()

    def _build_alias_tab(self, panel):
        self.alias_source = wx.TextCtrl(panel, wx.ID_ANY, "")
        self.alias_target = wx.TextCtrl(panel, wx.ID_ANY, "")

        form = wx.FlexGridSizer(2, 2, 8, 8)
        form.Add(wx.StaticText(panel, wx.ID_ANY, "Footprint token"), 0, wx.ALIGN_CENTER_VERTICAL)
        form.Add(self.alias_source, 1, wx.EXPAND)
        form.Add(
            wx.StaticText(panel, wx.ID_ANY, "Alias token(s), comma-separated"),
            0,
            wx.ALIGN_CENTER_VERTICAL,
        )
        form.Add(self.alias_target, 1, wx.EXPAND)
        form.AddGrowableCol(1, 1)

        self.alias_list = wx.dataview.DataViewListCtrl(
            panel,
            wx.ID_ANY,
            style=wx.dataview.DV_ROW_LINES | wx.dataview.DV_SINGLE,
        )
        self.alias_list.AppendTextColumn("Footprint token", width=280)
        self.alias_list.AppendTextColumn("Alias token(s)", width=280)
        self.alias_list.Bind(
            wx.dataview.EVT_DATAVIEW_SELECTION_CHANGED,
            self.on_alias_selected,
        )

        add_button = wx.Button(panel, wx.ID_ANY, "Add / Update")
        delete_button = wx.Button(panel, wx.ID_ANY, "Delete")
        add_button.Bind(wx.EVT_BUTTON, self.add_or_update_alias)
        delete_button.Bind(wx.EVT_BUTTON, self.delete_alias)

        buttons = wx.BoxSizer(wx.HORIZONTAL)
        buttons.Add(add_button, 0, wx.RIGHT, 8)
        buttons.Add(delete_button, 0)

        layout = wx.BoxSizer(wx.VERTICAL)
        layout.Add(form, 0, wx.ALL | wx.EXPAND, 8)
        layout.Add(
            wx.StaticText(
                panel,
                wx.ID_ANY,
                "Example: SOT-23 -> TO-236, SOT23",
            ),
            0,
            wx.LEFT | wx.RIGHT | wx.BOTTOM,
            8,
        )
        layout.Add(buttons, 0, wx.LEFT | wx.RIGHT | wx.BOTTOM, 8)
        layout.Add(self.alias_list, 1, wx.ALL | wx.EXPAND, 8)
        panel.SetSizer(layout)

    def _build_rules_tab(self, panel):
        self.rule_reference_prefix = wx.TextCtrl(panel, wx.ID_ANY, "")
        self.rule_pattern = wx.TextCtrl(panel, wx.ID_ANY, "")
        self.rule_replacement = wx.TextCtrl(panel, wx.ID_ANY, "")

        form = wx.FlexGridSizer(3, 2, 8, 8)
        form.Add(wx.StaticText(panel, wx.ID_ANY, "Reference prefix (optional)"), 0, wx.ALIGN_CENTER_VERTICAL)
        form.Add(self.rule_reference_prefix, 1, wx.EXPAND)
        form.Add(wx.StaticText(panel, wx.ID_ANY, "Regex pattern"), 0, wx.ALIGN_CENTER_VERTICAL)
        form.Add(self.rule_pattern, 1, wx.EXPAND)
        form.Add(wx.StaticText(panel, wx.ID_ANY, "Replacement (use $1, $2...)"), 0, wx.ALIGN_CENTER_VERTICAL)
        form.Add(self.rule_replacement, 1, wx.EXPAND)
        form.AddGrowableCol(1, 1)

        self.rules_list = wx.dataview.DataViewListCtrl(
            panel,
            wx.ID_ANY,
            style=wx.dataview.DV_ROW_LINES | wx.dataview.DV_SINGLE,
        )
        self.rules_list.AppendTextColumn("Ref Prefix", width=120)
        self.rules_list.AppendTextColumn("Regex", width=360)
        self.rules_list.AppendTextColumn("Replacement", width=220)
        self.rules_list.Bind(
            wx.dataview.EVT_DATAVIEW_SELECTION_CHANGED,
            self.on_rule_selected,
        )

        add_button = wx.Button(panel, wx.ID_ANY, "Add / Update")
        delete_button = wx.Button(panel, wx.ID_ANY, "Delete")
        add_button.Bind(wx.EVT_BUTTON, self.add_or_update_rule)
        delete_button.Bind(wx.EVT_BUTTON, self.delete_rule)

        buttons = wx.BoxSizer(wx.HORIZONTAL)
        buttons.Add(add_button, 0, wx.RIGHT, 8)
        buttons.Add(delete_button, 0)

        hint = wx.StaticText(
            panel,
            wx.ID_ANY,
            "Example: CP_ELEC_([0-9]+(?:\\.[0-9]+)?)X[0-9]+(?:\\.[0-9]+)? -> SMD,D$1",
        )

        layout = wx.BoxSizer(wx.VERTICAL)
        layout.Add(form, 0, wx.ALL | wx.EXPAND, 8)
        layout.Add(hint, 0, wx.LEFT | wx.RIGHT | wx.BOTTOM, 8)
        layout.Add(buttons, 0, wx.LEFT | wx.RIGHT | wx.BOTTOM, 8)
        layout.Add(self.rules_list, 1, wx.ALL | wx.EXPAND, 8)
        panel.SetSizer(layout)

    def _load_from_settings(self):
        if hasattr(self.parent, "get_footprint_highlight_rules_config"):
            aliases, rules = self.parent.get_footprint_highlight_rules_config()
        else:
            aliases = DEFAULT_FOOTPRINT_ALIAS_FORWARD
            rules = DEFAULT_FOOTPRINT_EXTRACTION_RULES

        if isinstance(aliases, dict):
            for source, target in aliases.items():
                if isinstance(target, list):
                    target_text = ", ".join(str(item) for item in target)
                else:
                    target_text = str(target)
                self.alias_list.AppendItem([str(source), target_text])

        if isinstance(rules, list):
            for rule in rules:
                if not isinstance(rule, dict):
                    continue
                self.rules_list.AppendItem(
                    [
                        str(rule.get("reference_prefix", "")),
                        str(rule.get("pattern", "")),
                        str(rule.get("replacement", "")),
                    ]
                )

    def on_alias_selected(self, _):
        """Load selected alias row into edit fields."""
        item = self.alias_list.GetSelection()
        if not item.IsOk():
            return
        row = self.alias_list.ItemToRow(item)
        self.alias_source.SetValue(self.alias_list.GetTextValue(row, 0))
        self.alias_target.SetValue(self.alias_list.GetTextValue(row, 1))

    def on_rule_selected(self, _):
        """Load selected regex rule row into edit fields."""
        item = self.rules_list.GetSelection()
        if not item.IsOk():
            return
        row = self.rules_list.ItemToRow(item)
        self.rule_reference_prefix.SetValue(self.rules_list.GetTextValue(row, 0))
        self.rule_pattern.SetValue(self.rules_list.GetTextValue(row, 1))
        self.rule_replacement.SetValue(self.rules_list.GetTextValue(row, 2))

    def add_or_update_alias(self, _):
        """Append a new alias row or update the currently selected alias."""
        self._upsert_alias_from_inputs()

    def _upsert_alias_from_inputs(self):
        """Insert or update one alias row from current alias input fields."""
        source = self.alias_source.GetValue().strip()
        target = self.alias_target.GetValue().strip()
        if not source or not target:
            return False

        item = self.alias_list.GetSelection()
        if item.IsOk():
            row = self.alias_list.ItemToRow(item)
            self.alias_list.SetTextValue(source, row, 0)
            self.alias_list.SetTextValue(target, row, 1)
        else:
            self.alias_list.AppendItem([source, target])
        return True

    def delete_alias(self, _):
        """Delete the selected alias row."""
        item = self.alias_list.GetSelection()
        if item.IsOk():
            self.alias_list.DeleteItem(self.alias_list.ItemToRow(item))

    def add_or_update_rule(self, _):
        """Append a new extraction rule row or update the selected row."""
        self._upsert_rule_from_inputs()

    def _upsert_rule_from_inputs(self):
        """Insert or update one extraction rule row from current input fields."""
        reference_prefix = self.rule_reference_prefix.GetValue().strip()
        pattern = self.rule_pattern.GetValue().strip()
        replacement = self.rule_replacement.GetValue().strip()
        if not pattern or not replacement:
            return False

        item = self.rules_list.GetSelection()
        if item.IsOk():
            row = self.rules_list.ItemToRow(item)
            self.rules_list.SetTextValue(reference_prefix, row, 0)
            self.rules_list.SetTextValue(pattern, row, 1)
            self.rules_list.SetTextValue(replacement, row, 2)
        else:
            self.rules_list.AppendItem([reference_prefix, pattern, replacement])
        return True

    def delete_rule(self, _):
        """Delete the selected extraction rule row."""
        item = self.rules_list.GetSelection()
        if item.IsOk():
            self.rules_list.DeleteItem(self.rules_list.ItemToRow(item))

    def save_and_close(self, _):
        """Persist configured aliases/rules into settings and close dialog."""
        self._upsert_alias_from_inputs()
        self._upsert_rule_from_inputs()

        aliases = {}
        for row in range(self.alias_list.GetItemCount()):
            source = self.alias_list.GetTextValue(row, 0).strip()
            target = self.alias_list.GetTextValue(row, 1).strip()
            if source and target:
                aliases[source] = target

        rules = []
        for row in range(self.rules_list.GetItemCount()):
            pattern = self.rules_list.GetTextValue(row, 1).strip()
            replacement = self.rules_list.GetTextValue(row, 2).strip()
            if not pattern or not replacement:
                continue
            rules.append(
                {
                    "reference_prefix": self.rules_list.GetTextValue(row, 0).strip(),
                    "pattern": pattern,
                    "replacement": replacement,
                }
            )

        if hasattr(self.parent, "set_footprint_highlight_rules_config"):
            self.parent.set_footprint_highlight_rules_config(aliases, rules, persist=True)

        self.EndModal(wx.ID_OK)

    def quit_dialog(self, *_):
        """Close the dialog without saving."""
        self.EndModal(wx.ID_CANCEL)
