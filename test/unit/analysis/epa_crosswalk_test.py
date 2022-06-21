"""Unit tests for the :mod:`pudl.analysis.epa_crosswalk` module."""
from collections.abc import Sequence

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import pudl.analysis.epa_crosswalk as cw


def df_from_product(inputs: dict[str, Sequence], as_index=True) -> pd.DataFrame:
    """Make a dataframe from cartesian product of input sequences.

    Args:
        inputs (Dict[str, Sequence]): dataframe column names mapped
            to their unique values.
        as_index (bool): whether to set the product as the index

    Return:
        df (pd.DataFrame): cartesian product dataframe
    """
    names, iterables = zip(*inputs.items())
    df = pd.MultiIndex.from_product(iterables, names=names).to_frame(index=as_index)

    return df


@pytest.fixture()
def mock_crosswalk():
    """Minimal EPA Crosswalk.

    The crosswalk is basically a list of graph edges linking CAMD units (combustors) to EIA generators within plants.

       CAMD_PLANT_ID CAMD_UNIT_ID  EIA_GENERATOR_ID MATCH_TYPE_GEN
    0             10            a                 0           asdf
    1             12            a                 0           asdf
    2             12            a                 1           asdf
    3             11            a                 0           asdf
    4             11            b                 0           asdf
    5             10            b                 1           asdf
    6             10            c                 1           asdf
    7             10            c                 2           asdf
    """
    columns = ["CAMD_PLANT_ID", "CAMD_UNIT_ID", "EIA_GENERATOR_ID"]
    one_to_one = pd.DataFrame(
        dict(
            zip(
                columns,
                [
                    [10],
                    ["a"],
                    [0],
                ],
            )
        )
    )
    many_to_one = pd.DataFrame(dict(zip(columns, [[11, 11], ["a", "b"], [0, 0]])))
    one_to_many = pd.DataFrame(dict(zip(columns, [[12, 12], ["a", "a"], [0, 1]])))
    many_to_many = pd.DataFrame(
        dict(zip(columns, [[10, 10, 10], ["b", "c", "c"], [1, 1, 2]]))
    )
    crosswalk = pd.concat(
        [one_to_one, one_to_many, many_to_one, many_to_many], axis=0, ignore_index=True
    )
    crosswalk["MATCH_TYPE_GEN"] = "asdf"

    return crosswalk


@pytest.fixture()
def mock_cems_extended():
    """EPA CEMS with more units. Needed to cover all the cases in graph analysis.

    Timestamps are reduced to 2 for this test.

    NOTE: Only unique IDs are in the table below. The actual dataframe has 2 timestamps per row here.
    unit_id_epa     operating_datetime_utc  plant_id_eia  unitid  gross_load_mw
              0  2019-12-31 22:00:00+00:00            10       a              0
              1  2019-12-31 22:00:00+00:00            10       b              0
              2  2019-12-31 22:00:00+00:00            10       c              0
              3  2019-12-31 22:00:00+00:00            11       a              0
              4  2019-12-31 22:00:00+00:00            11       b              0
              5  2019-12-31 22:00:00+00:00            12       a              0
    """
    inputs = dict(
        unit_id_epa=range(6),
        operating_datetime_utc=pd.date_range(
            start="2019-12-31 22:00", end="2019-12-31 23:00", freq="h", tz="UTC"
        ),
    )
    cems = df_from_product(inputs, as_index=False)
    # add composite keys
    # (duplicate each entry for other timestamp)
    cems["plant_id_eia"] = [10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 12, 12]
    cems["unitid"] = ["a", "a", "b", "b", "c", "c", "a", "a", "b", "b", "a", "a"]
    cems["gross_load_mw"] = 0  # not needed for crosswalk testing

    cems = cems.set_index(["unit_id_epa", "operating_datetime_utc"], drop=False)
    return cems


def test__get_unique_keys(mock_cems_extended):
    """Test that dask and pandas dataframes give the same unique keys."""
    mock_cems_extended = mock_cems_extended.reset_index(
        drop=True
    )  # this func is used before index is set. Dask doesn't support MultiIndex
    # sensititve to column order
    expected = mock_cems_extended[::2][["plant_id_eia", "unitid", "unit_id_epa"]]
    actual = cw._get_unique_keys(mock_cems_extended)
    assert_frame_equal(actual, expected)

    dask_cems = dd.from_pandas(mock_cems_extended, npartitions=2)
    actual = cw._get_unique_keys(dask_cems)
    assert_frame_equal(actual, expected)


def test__convert_global_id_to_composite_id(mock_crosswalk, mock_cems_extended):
    """Test conversion of global_subplant_id to a composite subplant_id.

    The global_subplant_id should be equivalent to the composite (CAMD_PLANT_ID, subplant_id).

       global_subplant_id  subplant_id  ...  CAMD_PLANT_ID CAMD_UNIT_ID  EIA_GENERATOR_ID  ...
    0                   0            0  ...             10            a                 0  ... # one to one
    1                   1            1  ...             10            b                 1  ... # many to many
    2                   1            1  ...             10            c                 1  ...
    3                   1            1  ...             10            c                 2  ...
    4                   2            0  ...             11            a                 0  ... # one to many
    5                   2            0  ...             11            b                 0  ...
    6                   3            0  ...             12            a                 0  ... # many to one
    7                   3            0  ...             12            a                 1  ...
    """
    cols = ["plant_id_eia", "unitid", "unit_id_epa"]
    uniques = mock_cems_extended.loc[
        pd.IndexSlice[:, "2019-12-31 22:00:00+00:00"], cols
    ].copy()

    # simulate join by duplicating rows as appropriate
    one_to_many = uniques.query('plant_id_eia == 12 and unitid == "a"')
    many_to_many = uniques.query('plant_id_eia == 10 and unitid == "c"')
    expected = (
        pd.concat([uniques, one_to_many, many_to_many])
        .sort_index()
        .reset_index(drop=True)
    )

    expected = expected.assign(
        CAMD_PLANT_ID=expected["plant_id_eia"],
        CAMD_UNIT_ID=expected["unitid"],
        EIA_GENERATOR_ID=[0, 1, 1, 2, 0, 0, 0, 1],
        MATCH_TYPE_GEN="asdf",
        global_subplant_id=[0, 1, 1, 1, 2, 2, 3, 3],
        subplant_id=[0, 1, 1, 1, 0, 0, 0, 0],
    )
    # fix column order
    expected = expected[
        cols
        + ["CAMD_PLANT_ID", "CAMD_UNIT_ID", "EIA_GENERATOR_ID", "MATCH_TYPE_GEN"]
        + ["global_subplant_id", "subplant_id"]
    ]

    input_ = expected.drop(columns=["subplant_id"])
    actual = cw._convert_global_id_to_composite_id(input_)
    assert_frame_equal(actual, expected)


def test_make_subplant_ids(mock_crosswalk, mock_cems_extended):
    """Integration test for the subplant_id assignment process.

    The new subplant_id column is half of the compound key (CAMD_PLANT_ID, subplant_id) that
    should identify disjoint subgraphs of units and generators.

       subplant_id  ...  CAMD_PLANT_ID CAMD_UNIT_ID  EIA_GENERATOR_ID  ...
    0            0  ...             10            a                 0  ... # one to one
    1            1  ...             10            b                 1  ... # many to many
    2            1  ...             10            c                 1  ...
    3            1  ...             10            c                 2  ...
    4            0  ...             11            a                 0  ... # one to many
    5            0  ...             11            b                 0  ...
    6            0  ...             12            a                 0  ... # many to one
    7            0  ...             12            a                 1  ...
    """
    cols = ["plant_id_eia", "unitid", "unit_id_epa"]
    uniques = mock_cems_extended.loc[
        pd.IndexSlice[:, "2019-12-31 22:00:00+00:00"], cols
    ].copy()

    # simulate join by duplicating rows as appropriate
    one_to_many = uniques.query('plant_id_eia == 12 and unitid == "a"')
    many_to_many = uniques.query('plant_id_eia == 10 and unitid == "c"')
    expected = (
        pd.concat([uniques, one_to_many, many_to_many])
        .sort_index()
        .reset_index(drop=True)
    )

    expected = expected.assign(
        CAMD_PLANT_ID=expected["plant_id_eia"],
        CAMD_UNIT_ID=expected["unitid"],
        EIA_GENERATOR_ID=[0, 1, 1, 2, 0, 0, 0, 1],
        MATCH_TYPE_GEN="asdf",
        subplant_id=[0, 1, 1, 1, 0, 0, 0, 0],
    )
    # fix column order
    expected = expected[
        ["subplant_id"]
        + cols
        + ["CAMD_PLANT_ID", "CAMD_UNIT_ID", "EIA_GENERATOR_ID", "MATCH_TYPE_GEN"]
    ]

    # should be two separate tests but I ran out of time
    actual = cw.filter_crosswalk(mock_crosswalk, mock_cems_extended)
    actual = cw.make_subplant_ids(actual)
    assert_frame_equal(actual, expected)
