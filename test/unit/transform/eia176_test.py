import pandas as pd

from pudl.transform.eia176 import _core_eia176__data, get_wide_table, validate_totals

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
VOLUME_1 = 30980426.0
COMPANY_1 = [
    "New Mexico",
    "VL",
    "NEW MEXICO GAS COMPANY",
    ID_1,
    "1010",
    "2022",
    VOLUME_1,
    "[10.1]",
    "Residential Sales Volume",
]

ID_2 = "17635017NM"
VOLUME_2 = 532842.0
COMPANY_2 = [
    "New Mexico",
    "VL",
    "WEST TEXAS GAS INC",
    ID_2,
    "1010",
    "2022",
    VOLUME_2,
    "[10.1]",
    "Residential Sales Volume",
]

NM_VOLUME = VOLUME_1 + VOLUME_2
NM_AGGREGATE = [
    "New Mexico",
    "VL",
    " Total of All Companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_1,
    "1010",
    "2022",
    NM_VOLUME,
    "[10.1]",
    "Residential Sales Volume",
]

ID_3 = "17635017TX"
VOLUME_3 = 1.0
COMPANY_3 = [
    "Texas",
    "VL",
    "WEST TEXAS GAS INC",
    ID_3,
    "1010",
    "2022",
    VOLUME_3,
    "[10.1]",
    "Residential Sales Volume",
]

TX_VOLUME = VOLUME_3
TX_AGGREGATE = [
    "Texas",
    "VL",
    " Total of All Companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_3,
    "1010",
    "2022",
    VOLUME_3,
    "[10.1]",
    "Residential Sales Volume",
]

US_VOLUME = NM_VOLUME + TX_VOLUME
US_AGGREGATE = [
    " U.S. Total",
    "VL",
    " Total of All Companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_1,
    "1010",
    "2022",
    US_VOLUME,
    "[10.1]",
    "Residential Sales Volume",
]

DROP_COLS = ["itemsort", "item", "atype", "line", "company"]


def test_core_eia176__data():
    eav_model = pd.DataFrame(columns=COLUMN_NAMES)
    eav_model.loc[0] = COMPANY_1
    eav_model.loc[1] = NM_AGGREGATE

    wide_company, wide_aggregate = _core_eia176__data(eav_model)
    assert wide_company.shape == (1, 4)

    company_row = wide_company.loc[0]
    assert list(company_row.index) == ["report_year", "area", "id", "1010_VL"]
    assert list(company_row.values) == ["2022", "New Mexico", ID_1, VOLUME_1]

    assert wide_aggregate.shape == (1, 3)
    aggregate_row = wide_aggregate.loc[0]
    assert list(aggregate_row.index) == ["report_year", "area", "1010_VL"]
    assert list(aggregate_row.values) == ["2022", "New Mexico", NM_VOLUME]


def test_get_wide_table():
    long_table = pd.DataFrame(columns=COLUMN_NAMES)
    long_table.loc[0] = COMPANY_1
    long_table.loc[1] = COMPANY_2
    long_table["variable_name"] = long_table["line"] + "_" + long_table["atype"]

    drop_columns = DROP_COLS
    primary_key = ["report_year", "area", "id"]
    wide_table = get_wide_table(long_table, primary_key, drop_columns)
    wide_table = wide_table.fillna(0)

    assert wide_table.shape == (2, 4)
    assert list(wide_table.loc[0].index) == ["report_year", "area", "id", "1010_VL"]
    assert list(wide_table.loc[0].values) == ["2022", "New Mexico", ID_2, VOLUME_2]
    assert list(wide_table.loc[1].values) == ["2022", "New Mexico", ID_1, VOLUME_1]


def test_validate__totals():
    # Our test data will have only measurements for this 1010_VL variable
    validation_cols = COLUMN_NAMES + ["1010_VL"]

    company_data = pd.DataFrame(columns=validation_cols)
    # Add the value for the 1010_VL variable
    company_data.loc[0] = COMPANY_1 + [f"{VOLUME_1}"]
    company_data.loc[1] = COMPANY_2 + [f"{VOLUME_2}"]
    company_data.loc[2] = COMPANY_3 + [f"{VOLUME_3}"]
    company_data = company_data.drop(columns=DROP_COLS)

    aggregate_data = pd.DataFrame(columns=validation_cols)
    # Add the value for the 1010_VL variable
    aggregate_data.loc[0] = NM_AGGREGATE + [f"{NM_VOLUME}"]
    aggregate_data.loc[1] = TX_AGGREGATE + [f"{TX_VOLUME}"]
    aggregate_data.loc[2] = US_AGGREGATE + [f"{US_VOLUME}"]
    aggregate_data = aggregate_data.drop(columns=DROP_COLS + ["id"])

    validate_totals(company_data, aggregate_data)
