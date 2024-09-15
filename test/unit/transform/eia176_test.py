import pandas as pd

from pudl.transform.eia176 import _core_eia176__data, get_wide_table

COLUMN_NAMES = [
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

ID_1 = "17673850NM"
COMPANY_1 = [
    "New Mexico",
    "VL",
    "NEW MEXICO GAS COMPANY",
    ID_1,
    "1010",
    "2022",
    30980426.0,
    "[10.1]",
    "Residential Sales Volume",
]
COMPANY_DROP_COLS = ["itemsort", "item", "atype", "line", "company"]

AGGREGATE = [
    "New Mexico",
    "VL",
    " Total of All Companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_1,
    "1010",
    "2022",
    36612851.0,
    "[10.1]",
    "Residential Sales Volume",
]

ID_2 = "17635017NM"
COMPANY_2 = [
    "New Mexico",
    "VL",
    "WEST TEXAS GAS INC",
    ID_2,
    "1810",
    "2022",
    532842.0,
    "[18.1]",
    "Disposition to Distribution Companies Volume",
]


def test_core_eia176__data():
    eav_model = pd.DataFrame(columns=COLUMN_NAMES)
    eav_model.loc[0] = COMPANY_1
    eav_model.loc[1] = AGGREGATE

    wide_company, wide_aggregate = _core_eia176__data(eav_model)
    assert wide_company.shape == (1, 4)

    company_row = wide_company.loc[0]
    assert list(company_row.index) == ["report_year", "area", "id", "1010_VL"]
    assert list(company_row.values) == ["2022", "New Mexico", ID_1, 30980426.0]

    assert wide_aggregate.shape == (1, 3)
    aggregate_row = wide_aggregate.loc[0]
    assert list(aggregate_row.index) == ["report_year", "area", "1010_VL"]
    assert list(aggregate_row.values) == ["2022", "New Mexico", 36612851.0]


def test_get_wide_table():
    long_table = pd.DataFrame(columns=COLUMN_NAMES)
    long_table.loc[0] = COMPANY_1
    long_table.loc[1] = COMPANY_2
    long_table["variable_name"] = long_table["line"] + "_" + long_table["atype"]

    drop_columns = COMPANY_DROP_COLS
    primary_key = ["report_year", "area", "id"]
    wide_table = get_wide_table(long_table, primary_key, drop_columns)
    wide_table = wide_table.fillna(0)

    assert wide_table.shape == (2, 5)
    assert list(wide_table.loc[0].index) == [
        "report_year",
        "area",
        "id",
        "1010_VL",
        "1810_VL",
    ]
    assert list(wide_table.loc[0].values) == ["2022", "New Mexico", ID_2, 0.0, 532842.0]
    assert list(wide_table.loc[1].values) == [
        "2022",
        "New Mexico",
        ID_1,
        30980426.0,
        0.0,
    ]


# TODO: Implement
def test_validate__totals():
    pass
