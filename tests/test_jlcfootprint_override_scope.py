"""The override dialog's sentence on what an override reaches (spec 18.2).

An override lives on the verdict row of one part on one set of pads, so it applies
to every reference sharing that row and, on a board with variants, in every variant
that orders the part there.  Pure: the wording only.
"""

import pytest

from jlcfootprint.presentation import SCOPE_NAMED, override_scope


@pytest.mark.parametrize(
    "references,variants,sentence",
    [
        (["R1"], False, "This override applies to R1."),
        (["R5", "R1"], False, "This override applies to R1 and R5."),
        (["R10", "R1", "R2"], False, "This override applies to R1, R2 and R10."),
        (
            ["R1"],
            True,
            "This override applies to R1, in every variant that orders C123 on "
            "this footprint.",
        ),
        (
            ["R5", "R1"],
            True,
            "This override applies to R1 and R5, in every variant that orders C123 "
            "on this footprint.",
        ),
    ],
    ids=["one", "two", "natural_order", "one_variant", "two_variant"],
)
def test_the_sentence_names_the_references_and_on_a_variant_board_the_variants(
    references, variants, sentence
):
    """One reference reads "applies to R1"; several are joined with "and"."""
    assert override_scope(references, "C123", variants=variants) == sentence


def test_a_long_list_names_the_first_references_and_counts_the_rest():
    """More references than fit in a sentence: the first six, then how many more."""
    references = [f"C{index}" for index in range(1, SCOPE_NAMED + 4)]
    assert SCOPE_NAMED == 6
    assert override_scope(references, "C1525", variants=True) == (
        "This override applies to C1, C2, C3, C4, C5, C6 and 3 more, in every "
        "variant that orders C1525 on this footprint."
    )


def test_nothing_to_name_says_nothing():
    """No reference or no part number: no sentence; repeats and blanks are dropped."""
    assert override_scope([], "C123") == ""
    assert override_scope(["R1"], "") == ""
    assert override_scope(["R1", "", "R1"], "C123") == "This override applies to R1."
