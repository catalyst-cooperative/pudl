import pandas as pd

from pudl.transform.eia176 import _core_eia176__data


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
        2013232.0,
        "itemsort",
        "item",
    ]

    return df


def test_core_eia176__data():
    eav_model = get_test_df()

    wide_company, wide_aggregate = _core_eia176__data(eav_model)
    assert wide_company.shape == (1, 4)
    company_row = wide_company.loc[0]
    assert list(company_row.index) == ["report_year", "area", "id", "1010_VL"]
    assert list(company_row.values) == ["2022", "New Mexico", "17635019NM", 2013231.0]

    assert wide_aggregate.shape == (1, 3)
    aggregate_row = wide_aggregate.loc[0]
    assert list(aggregate_row.index) == ["report_year", "area", "1010_VL"]
    assert list(aggregate_row.values) == ["2022", "New Mexico", 2013232.0]


# TODO: Implement
def test_validate__totals():
    pass
