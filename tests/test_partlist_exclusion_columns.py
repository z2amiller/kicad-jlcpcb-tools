"""The parts list shows the BOM and position exclusions the board holds."""

from .test_standard_only_indicator import PartListDataModel, _row


def test_a_row_shows_the_exclusions_it_is_given() -> None:
    """Each column takes the icon for its own flag, whatever it showed before."""
    model = PartListDataModel(scale_factor=1.0)
    model.bom_pos_icons = ["included", "excluded"]
    model.AddEntry(_row("R1"))
    (row,) = model.data
    columns = model.columns

    for bom, pos in [(True, False), (False, True), (True, True), (False, False)]:
        model.set_bom_pos(row, exclude_from_bom=bom, exclude_from_pos=pos)
        assert (row[columns["BOM_COL"]], row[columns["POS_COL"]]) == (
            "excluded" if bom else "included",
            "excluded" if pos else "included",
        )
