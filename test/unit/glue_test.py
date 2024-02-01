"""Unit tests for the glue subpackage."""

import pandas as pd

from pudl.glue.ferc1_eia import get_missing_ids


def test_get_missing_ids():
    """Test that missing IDs grabs the missing IDs in the right table only."""
    id_col = "id_col"
    ids_left = pd.DataFrame({id_col: [1, 2, 3, 4]})
    ids_right = pd.DataFrame({id_col: [2, 3, 4, 5]})

    pd.testing.assert_index_equal(
        pd.Index([5], name=id_col),
        get_missing_ids(ids_left=ids_left, ids_right=ids_right, id_cols=[id_col]),
    )
