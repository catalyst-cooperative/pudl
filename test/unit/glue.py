"""Unit tests for the glue subpackage."""

import pandas as pd

from pudl.glue.xbrl_dbf_ferc1 import autoincrement_from_max


def test_auto_increment():
    """Test the autoincrementer."""
    test = pd.DataFrame(
        columns=["id", "fake_data"],
        data=[(1, "hi"), (2, "hello"), (2, "idk"), (pd.NA, "fill me")],
    ).convert_dtypes()

    expected = pd.DataFrame(
        columns=["id", "fake_data"],
        data=[(1, "hi"), (2, "hello"), (2, "idk"), (3, "fill me")],
    ).convert_dtypes()

    pd.testing.assert_frame_equal(expected, autoincrement_from_max(test, "id"))
