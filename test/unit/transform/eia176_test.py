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
    "new mexico",
    "vl",
    "new mexico gas company",
    ID_1,
    "1010",
    "2022",
    VOLUME_1,
    "[10.1]",
    "residential sales volume",
]

ID_2 = "17635017NM"
VOLUME_2 = 532842.0
COMPANY_2 = [
    "new mexico",
    "vl",
    "west texas gas inc",
    ID_2,
    "1010",
    "2022",
    VOLUME_2,
    "[10.1]",
    "residential sales volume",
]

NM_VOLUME = VOLUME_1 + VOLUME_2
NM_AGGREGATE = [
    "new mexico",
    "vl",
    "total of all companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_1,
    "1010",
    "2022",
    NM_VOLUME,
    "[10.1]",
    "residential sales volume",
]

ID_3 = "17635017TX"
VOLUME_3 = 1.0
COMPANY_3 = [
    "texas",
    "vl",
    "west texas gas inc",
    ID_3,
    "1010",
    "2022",
    VOLUME_3,
    "[10.1]",
    "residential sales volume",
]

TX_VOLUME = VOLUME_3
TX_AGGREGATE = [
    "texas",
    "vl",
    "total of all companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_3,
    "1010",
    "2022",
    VOLUME_3,
    "[10.1]",
    "residential sales volume",
]

US_VOLUME = NM_VOLUME + TX_VOLUME
US_AGGREGATE = [
    "u.s. total",
    "vl",
    "total of all companies",
    # Aggregates appear to reuse an arbitrary company ID
    ID_1,
    "1010",
    "2022",
    US_VOLUME,
    "[10.1]",
    "residential sales volume",
]

ID_4 = "4"
VOLUME_4 = 4.0
COMPANY_4 = [
    "alaska",
    "vl",
    "alaska gas inc",
    ID_4,
    "1020",
    "2022",
    VOLUME_4,
    "[10.2]",
    "some other volume",
]

DROP_COLS = ["itemsort", "item", "atype", "line", "company"]


def test_core_eia176__data():
    eav_model = pd.DataFrame(columns=COLUMN_NAMES)
    eav_model.loc[0] = COMPANY_1
    eav_model.loc[1] = NM_AGGREGATE

    wide_company, wide_aggregate = _core_eia176__data(eav_model)
    assert wide_company.shape == (1, 4)

    company_row = wide_company.loc[0]
    assert list(company_row.index) == ["report_year", "area", "id", "1010_vl"]
    assert list(company_row.values) == [2022, "new mexico", ID_1, VOLUME_1]

    assert wide_aggregate.shape == (1, 3)
    aggregate_row = wide_aggregate.loc[0]
    assert list(aggregate_row.index) == ["report_year", "area", "1010_vl"]
    assert list(aggregate_row.values) == [2022, "new mexico", NM_VOLUME]


def test_get_wide_table():
    long_table = pd.DataFrame(columns=COLUMN_NAMES)
    long_table.loc[0] = COMPANY_1
    long_table.loc[1] = COMPANY_2
    # We need a row measuring a different variable to test filling NAs
    long_table.loc[3] = COMPANY_4
    long_table["variable_name"] = long_table["line"] + "_" + long_table["atype"]
    long_table = long_table.drop(columns=DROP_COLS)

    primary_key = ["report_year", "area", "id"]
    wide_table = get_wide_table(long_table, primary_key)

    assert wide_table.shape == (3, 5)
    assert list(wide_table.loc[0].index) == [
        "report_year",
        "area",
        "id",
        "1010_vl",
        "1020_vl",
    ]
    assert list(wide_table.loc[0].values) == ["2022", "alaska", ID_4, 0, VOLUME_4]
    assert list(wide_table.loc[1].values) == ["2022", "new mexico", ID_2, VOLUME_2, 0]
    assert list(wide_table.loc[2].values) == ["2022", "new mexico", ID_1, VOLUME_1, 0]


def test_validate__totals():
    # Our test data will have only measurements for this 1010_vl variable
    validation_cols = COLUMN_NAMES + ["1010_vl"]

    company_data = pd.DataFrame(columns=validation_cols)
    # Add the value for the 1010_vl variable
    company_data.loc[0] = COMPANY_1 + [f"{VOLUME_1}"]
    company_data.loc[1] = COMPANY_2 + [f"{VOLUME_2}"]
    company_data.loc[2] = COMPANY_3 + [f"{VOLUME_3}"]
    company_data = company_data.drop(columns=DROP_COLS)

    aggregate_data = pd.DataFrame(columns=validation_cols)
    # Add the value for the 1010_vl variable
    aggregate_data.loc[0] = NM_AGGREGATE + [f"{NM_VOLUME}"]
    aggregate_data.loc[1] = TX_AGGREGATE + [f"{TX_VOLUME}"]
    aggregate_data.loc[2] = US_AGGREGATE + [f"{US_VOLUME}"]
    aggregate_data = aggregate_data.drop(columns=DROP_COLS + ["id"])

    validate_totals(company_data, aggregate_data)
