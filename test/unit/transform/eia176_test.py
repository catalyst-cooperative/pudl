import pandas as pd

from pudl.transform.eia176 import _compare_totals, _core_eia176__data


def get_test_df():
    col_names = [
        "area",
        "atype",
        "company",
        "id",
        "line",
        "report_year",
        "value",
        "itemsort",
        "item",
    ]
    df = pd.DataFrame(columns=col_names)
    df.loc[0] = [
        "New Mexico",
        "VL",
        "ZIA NATURAL GAS",
        "17635019NM",
        "1010",
        "2022",
        2013231.0,
        "itemsort",
        "item",
    ]
    df.loc[1] = [
        "New Mexico",
        "VL",
        " Total of All Companies",
        "17635019NM",
        "1010",
        "2022",
        2013231.0,
        "itemsort",
        "item",
    ]

    return df


def test_core_eia176__data():
    eav_model = get_test_df()

    wide_table = _core_eia176__data(eav_model)
    assert wide_table.shape == (1, 4)
    row = wide_table.loc[0]
    assert list(row.index) == ["report_year", "area", "id", "1010_VL"]
    assert list(row.values) == ["2022", "New Mexico", "17635019NM", 2013231.0]


def test_compare_totals_no_diff():
    states_df = get_test_df()
    states_df.loc[2] = [
        "New Mexico",
        "VL",
        "ANOTHER COMPANY",
        "12345679NM",
        "1010",
        "2022",
        1.0,
        "itemsort",
        "item",
    ]
    states_df.loc[1, "value"] += 1

    reported_state_totals = states_df[states_df["company"] == " Total of All Companies"]
    state_companies_df = states_df[states_df["company"] != " Total of All Companies"]
    state_level_cols = ["report_year", "area", "line", "atype"]
    calculated_state_totals = state_companies_df.groupby(state_level_cols).sum()
    assert _compare_totals(
        reported_state_totals, calculated_state_totals, state_level_cols
    ).empty


# TODO: Test on specific details here
def test_compare_totals_diff():
    states_df = get_test_df()
    states_df.loc[2] = [
        "New Mexico",
        "VL",
        "ANOTHER COMPANY",
        "12345679NM",
        "1010",
        "2022",
        1.0,
        "itemsort",
        "item",
    ]

    reported_state_totals = states_df[states_df["company"] == " Total of All Companies"]
    state_companies_df = states_df[states_df["company"] != " Total of All Companies"]
    state_level_cols = ["report_year", "area", "line", "atype"]
    calculated_state_totals = state_companies_df.groupby(state_level_cols).sum()
    assert not _compare_totals(
        reported_state_totals, calculated_state_totals, state_level_cols
    ).empty


# TODO: Implement, if we can even unit-test a function annotated as an asset check
def test_validate__totals():
    pass
