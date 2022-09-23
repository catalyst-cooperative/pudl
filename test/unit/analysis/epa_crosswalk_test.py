"""Unit tests for the :mod:`pudl.analysis.epa_crosswalk` module."""

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import pudl.analysis.epacamd_eia as cw_analysis


@pytest.fixture()
def mock_crosswalk():
    """Create Minimal EPA Crosswalk for testing.

    The crosswalk is basically a list of graph edges linking CAMD units (combustors)
    to EIA generators within plants.

        plant_id_eia  emissions_unit_id_epa  generator_id
    0             10            a                 0
    1             12            a                 0
    2             12            a                 1
    3             11            a                 0
    4             11            b                 0
    5             10            b                 1
    6             10            c                 1
    7             10            c                 2
    """
    columns = ["plant_id_eia", "emissions_unit_id_epa", "generator_id"]
    one_to_one = pd.DataFrame(dict(zip(columns, [[10], ["a"], [0]])))
    many_to_one = pd.DataFrame(dict(zip(columns, [[11, 11], ["a", "b"], [0, 0]])))
    one_to_many = pd.DataFrame(dict(zip(columns, [[12, 12], ["a", "a"], [0, 1]])))
    many_to_many = pd.DataFrame(
        dict(zip(columns, [[10, 10, 10], ["b", "c", "c"], [1, 1, 2]]))
    )
    crosswalk = pd.concat(
        [one_to_one, one_to_many, many_to_one, many_to_many], axis=0, ignore_index=True
    )

    return crosswalk


@pytest.fixture()
def mock_cems_extended():
    """EPA CEMS with more units. Needed to cover all the cases in graph analysis.

    Timestamps are reduced to 2 for this test.

    NOTE: Only unique IDs are in the table below. The actual dataframe has 2 timestamps
    per row here.

    operating_datetime_utc     plant_id_eia  emissions_unit_id_epa  gross_load_mw
    2019-12-31 22:00:00+00:00       10                 a              0
    2019-12-31 22:00:00+00:00       10                 b              0
    2019-12-31 22:00:00+00:00       10                 c              0
    2019-12-31 22:00:00+00:00       11                 a              0
    2019-12-31 22:00:00+00:00       11                 b              0
    2019-12-31 22:00:00+00:00       12                 a              0

    """
    cems = pd.DataFrame()
    cems["operating_datetime_utc"] = [
        "2019-12-31 22:00:00+00:00",
        "2019-12-31 23:00:00+00:00",
        "2019-12-31 22:00:00+00:00",
        "2019-12-31 23:00:00+00:00",
        "2019-12-31 22:00:00+00:00",
        "2019-12-31 23:00:00+00:00",
        "2019-12-31 22:00:00+00:00",
        "2019-12-31 23:00:00+00:00",
        "2019-12-31 22:00:00+00:00",
        "2019-12-31 23:00:00+00:00",
        "2019-12-31 22:00:00+00:00",
        "2019-12-31 23:00:00+00:00",
    ]
    cems["operating_datetime_utc"] = cems["operating_datetime_utc"].astype(
        "datetime64[ns, UTC]"
    )
    cems["plant_id_eia"] = [10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 12, 12]
    cems["emissions_unit_id_epa"] = [
        "a",
        "a",
        "b",
        "b",
        "c",
        "c",
        "a",
        "a",
        "b",
        "b",
        "a",
        "a",
    ]
    cems["gross_load_mw"] = 0  # not needed for crosswalk testing

    return cems


def test__get_unique_keys(mock_cems_extended):
    """Test that dask and pandas dataframes give the same unique keys."""
    expected = mock_cems_extended[::2][["plant_id_eia", "emissions_unit_id_epa"]]
    actual = cw_analysis._get_unique_keys(mock_cems_extended)
    assert_frame_equal(actual, expected)

    dask_cems = dd.from_pandas(mock_cems_extended, npartitions=2)
    actual = cw_analysis._get_unique_keys(dask_cems)
    assert_frame_equal(actual, expected)


def test__convert_global_id_to_composite_id(mock_cems_extended):
    """Test conversion of global_subplant_id to a composite subplant_id.

    The global_subplant_id should be equivalent to the composite (plant_id_eia,
    subplant_id).

      global_subplant_id  subplant_id  plant_id_eia  emissions_unit_id_epa  generator_id
    0         0               0               10            a                 0   # one to one
    1         1               1               10            b                 1   # many to many
    2         1               1               10            c                 1
    3         1               1               10            c                 2
    4         2               0               11            a                 0   # one to many
    5         2               0               11            b                 0
    6         3               0               12            a                 0   # many to one
    7         3               0               12            a                 1

    """
    uniques = mock_cems_extended[
        mock_cems_extended["operating_datetime_utc"] == "2019-12-31 22:00:00+00:00"
    ]

    # simulate join by duplicating rows as appropriate
    one_to_many = uniques.query('plant_id_eia == 12 and emissions_unit_id_epa == "a"')
    many_to_many = uniques.query('plant_id_eia == 10 and emissions_unit_id_epa == "c"')
    expected = (
        pd.concat([uniques, one_to_many, many_to_many])
        .sort_index()
        .reset_index(drop=True)
    )

    expected = expected.assign(
        plant_id_eia=expected["plant_id_eia"],
        emissions_unit_id_epa=expected["emissions_unit_id_epa"],
        generator_id=[0, 1, 1, 2, 0, 0, 0, 1],
        global_subplant_id=[0, 1, 1, 1, 2, 2, 3, 3],
        subplant_id=[0, 1, 1, 1, 0, 0, 0, 0],
    )

    input_ = expected.drop(columns=["subplant_id"])
    actual = cw_analysis._convert_global_id_to_composite_id(input_)
    assert_frame_equal(actual, expected)


def test_make_subplant_ids(mock_crosswalk, mock_cems_extended):
    """Integration test for the subplant_id assignment process.

    The new subplant_id column is half of the compound key (plant_id_eia, subplant_id)
    that should identify disjoint subgraphs of units and generators.

       subplant_id  ...  plant_id_eia  emissions_unit_id_epa  generator_id
    0            0  ...        10            a                 0          # one to one
    1            1  ...        10            b                 1          # many to many
    2            1  ...        10            c                 1
    3            1  ...        10            c                 2
    4            0  ...        11            a                 0          # one to many
    5            0  ...        11            b                 0
    6            0  ...        12            a                 0          # many to one
    7            0  ...        12            a                 1

    """
    uniques = mock_cems_extended[
        mock_cems_extended["operating_datetime_utc"] == "2019-12-31 22:00:00+00:00"
    ]

    # simulate join by duplicating rows as appropriate
    one_to_many = uniques.query('plant_id_eia == 12 and emissions_unit_id_epa == "a"')
    many_to_many = uniques.query('plant_id_eia == 10 and emissions_unit_id_epa == "c"')
    expected = (
        pd.concat([uniques, one_to_many, many_to_many])
        .sort_index()
        .reset_index(drop=True)
    )

    expected = expected.assign(
        plant_id_eia=expected["plant_id_eia"],
        emissions_unit_id_epa=expected["emissions_unit_id_epa"],
        generator_id=[0, 1, 1, 2, 0, 0, 0, 1],
        subplant_id=[0, 1, 1, 1, 0, 0, 0, 0],
    )
    # fix column order and drop some columns
    expected = expected[
        ["plant_id_eia", "generator_id", "emissions_unit_id_epa", "subplant_id"]
    ]

    # should be two separate tests but I ran out of time
    actual1 = cw_analysis.filter_crosswalk(mock_crosswalk, mock_cems_extended)
    actual = cw_analysis.make_subplant_ids(actual1)
    assert_frame_equal(actual, expected)
