"""Unit tests for the glue subpackage."""

import pandas as pd

from pudl.glue.ferc1_eia import (
    get_core_eia923_plant_ids,
    get_missing_ids,
    get_util_ids_eia_unmapped,
    get_utility_most_recent_capacity,
    label_missing_ids_for_manual_mapping,
)


def test_get_missing_ids():
    """Test that missing IDs grabs the missing IDs in the right table only."""
    id_col = "id_col"
    ids_left = pd.DataFrame({id_col: [1, 2, 3, 4]})
    ids_right = pd.DataFrame({id_col: [2, 3, 4, 5]})

    pd.testing.assert_index_equal(
        pd.Index([5], name=id_col),
        get_missing_ids(ids_left=ids_left, ids_right=ids_right, id_cols=[id_col]),
    )


def test_label_missing_ids_for_manual_mapping():
    """Missing IDs get labeled with the corresponding rows of the label table."""
    id_col = "id_col"
    missing_ids = pd.Index([2, 3], name=id_col)
    label_df = pd.DataFrame({id_col: [1, 2, 3], "name": ["a", "b", "c"]})

    labeled = label_missing_ids_for_manual_mapping(missing_ids, label_df)

    pd.testing.assert_frame_equal(
        labeled,
        pd.DataFrame({"name": ["b", "c"]}, index=pd.Index([2, 3], name=id_col)),
    )


def test_get_utility_most_recent_capacity():
    """Only the most recent report_date's capacity should be summed per utility."""
    core_eia860__scd_generators = pd.DataFrame(
        {
            "utility_id_eia": [1, 1, 1, 2],
            "capacity_mw": [10.0, 20.0, 5.0, 100.0],
            "report_date": pd.to_datetime(
                ["2020-01-01", "2020-01-01", "2019-01-01", "2020-01-01"]
            ),
        }
    )

    util_recent_cap = get_utility_most_recent_capacity(core_eia860__scd_generators)

    pd.testing.assert_series_equal(
        util_recent_cap.sort_index(),
        pd.Series(
            [30.0, 100.0],
            index=pd.Index([1, 2], name="utility_id_eia"),
            name="capacity_mw",
        ),
    )


def test_get_core_eia923_plant_ids():
    """Plant IDs are pooled across all provided EIA-923 tables."""
    eia923_dfs = {
        "core_eia923__monthly_generation_fuel": pd.DataFrame({"plant_id_eia": [1, 2]}),
        "core_eia923__monthly_boiler_fuel": pd.DataFrame({"plant_id_eia": [2, 3]}),
    }

    assert get_core_eia923_plant_ids(eia923_dfs) == {1, 2, 3}


def test_get_util_ids_eia_unmapped():
    """Unmapped EIA utilities are labeled with capacity and FERC1-linkage flags."""
    out_eia__yearly_utilities = pd.DataFrame(
        {
            "utility_id_eia": [1, 2, 3],
            "utility_name_eia": ["mapped_util", "linked_util", "unlinked_util"],
        }
    )
    out_eia__yearly_generators = pd.DataFrame(
        {
            "utility_id_eia": [2, 3],
            "plant_id_eia": [100, 200],
        }
    )
    utilities_eia_mapped = pd.DataFrame({"utility_id_eia": [1]})
    eia923_plant_ids = {100}
    util_recent_cap = pd.Series(
        [5.0, 7.0], index=pd.Index([2, 3], name="utility_id_eia"), name="capacity_mw"
    )

    unmapped = get_util_ids_eia_unmapped(
        out_eia__yearly_utilities=out_eia__yearly_utilities,
        out_eia__yearly_generators=out_eia__yearly_generators,
        utilities_eia_mapped=utilities_eia_mapped,
        eia923_plant_ids=eia923_plant_ids,
        util_recent_cap=util_recent_cap,
    )

    assert set(unmapped.index) == {2, 3}
    assert unmapped.loc[2, "link_to_ferc1"]
    assert not unmapped.loc[3, "link_to_ferc1"]
