"""Unit tests specific to the FERC Form 1 table transformations."""

from io import StringIO

import pandas as pd
import pytest

from pudl.settings import Ferc1Settings
from pudl.transform.ferc1 import (
    DropDuplicateRowsDbf,
    Ferc1AbstractTableTransformer,
    TableIdFerc1,
    UnstackBalancesToReportYearInstantXbrl,
    WideToTidy,
    drop_duplicate_rows_dbf,
    fill_dbf_to_xbrl_map,
    read_dbf_to_xbrl_map,
    unstack_balances_to_report_year_instant_xbrl,
    wide_to_tidy,
)

TEST_DBF_XBRL_MAP = pd.read_csv(
    StringIO(
        """
sched_table_name,report_year,row_literal,row_number,row_type,xbrl_factoid
test_table1,2000,"Header 1",1,header,"N/A"
test_table1,2000,"Account A",2,ferc_account,account_a
test_table1,2000,"Account B",3,ferc_account,account_b
test_table1,2000,"Header 2",4,header,"N/A"
test_table1,2000,"Account C",5,ferc_account,account_c
test_table1,2002,"Header 1",1,header,"N/A"
test_table1,2002,"Account A",2,ferc_account,account_a
test_table1,2002,"Account B",3,ferc_account,account_b
test_table1,2002,"Account B1",4,ferc_account,account_b1
test_table1,2002,"Header 2",5,header,"N/A"
test_table1,2002,"Account C",6,ferc_account,account_c
"""
    ),
)

TEST_MUTLI_TABLE_DBF_XBRL_MAP = pd.read_csv(
    StringIO(
        """
sched_table_name,report_year,row_literal,row_number,row_type,xbrl_factoid
test_table2,2000,"Start of Page 2",6,header,"N/A"
test_table2,2000,"Account D",7,ferc_account,account_d
test_table2,2000,"Account E",8,ferc_account,account_e
test_table2,2002,"Start of Page 2",7,header,"N/A"
test_table2,2002,"Account D",8,ferc_account,account_d
test_table2,2002,"Account E",9,ferc_account,account_e
"""
    ),
)


@pytest.mark.parametrize(
    "dbf_table_name",
    [
        "f1_plant_in_srvce",
        "f1_elctrc_erg_acct",
    ],
)
def test_dbf_to_xbrl_mapping_is_unique(dbf_table_name):
    """Verify that our DBF to XBRL mapping results in at most 1 mapping per year."""
    dbf_xbrl_map = fill_dbf_to_xbrl_map(
        df=read_dbf_to_xbrl_map(dbf_table_names=[dbf_table_name]),
        dbf_years=Ferc1Settings().dbf_years,
    )
    dbf_xbrl_map = dbf_xbrl_map[dbf_xbrl_map.xbrl_factoid != "HEADER_ROW"]
    dbf_to_xbrl_mapping_is_unique = (
        dbf_xbrl_map.groupby(["report_year", "xbrl_factoid"])["row_number"]
        .nunique()
        .le(1)
        .all()
    )

    assert dbf_to_xbrl_mapping_is_unique  # nosec: B101


def test_fill_dbf_to_xbrl_map():
    """Minimal unit test for our DBF to XBRL map filling function."""
    expected = pd.read_csv(
        StringIO(
            """
report_year,row_number,xbrl_factoid
2000,2,account_a
2000,3,account_b
2000,5,account_c
2001,2,account_a
2001,3,account_b
2001,5,account_c
2002,2,account_a
2002,3,account_b
2002,4,account_b1
2002,6,account_c
2003,2,account_a
2003,3,account_b
2003,4,account_b1
2003,6,account_c
"""
        )
    )
    test_map = TEST_DBF_XBRL_MAP.drop(["row_literal"], axis="columns").reset_index(
        drop=True
    )
    actual = fill_dbf_to_xbrl_map(df=test_map, dbf_years=sorted(range(2000, 2004)))
    actual = actual[actual.xbrl_factoid != "HEADER_ROW"].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


def test_two_table_fill_dbf_to_xbrl_map():
    """Test for filling DBF to XBRL map with two tables."""
    expected = pd.read_csv(
        StringIO(
            """
report_year,row_number,xbrl_factoid
2000,2,account_a
2000,3,account_b
2000,5,account_c
2000,7,account_d
2000,8,account_e
2001,2,account_a
2001,3,account_b
2001,5,account_c
2001,7,account_d
2001,8,account_e
2002,2,account_a
2002,3,account_b
2002,4,account_b1
2002,6,account_c
2002,8,account_d
2002,9,account_e
2003,2,account_a
2003,3,account_b
2003,4,account_b1
2003,6,account_c
2003,8,account_d
2003,9,account_e
"""
        )
    )
    test_map = (
        pd.concat([TEST_DBF_XBRL_MAP, TEST_MUTLI_TABLE_DBF_XBRL_MAP])
        .drop(["row_literal"], axis="columns")
        .reset_index(drop=True)
    )
    actual = fill_dbf_to_xbrl_map(df=test_map, dbf_years=sorted(range(2000, 2004)))
    actual = actual[actual.xbrl_factoid != "HEADER_ROW"].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


WIDE_TO_TIDY_DF = pd.read_csv(
    StringIO(
        """
idx,x_test_value,y_test_value,z_test_value
A,10,100,1000
B,11,110,1100
C,12,120,1200
D,13,130,1300
"""
    ),
)


def test_wide_to_tidy():
    """Test :func:`wide_to_tidy_xbrl`."""
    params = WideToTidy(
        idx_cols=["idx"],
        value_types=["test_value"],
        stacked_column_name="xbrl_factoid",
    )
    df_out = wide_to_tidy(df=WIDE_TO_TIDY_DF, params=params)

    df_expected = pd.read_csv(
        StringIO(
            """
idx,xbrl_factoid,test_value
A,x,10
A,y,100
A,z,1000
B,x,11
B,y,110
B,z,1100
C,x,12
C,y,120
C,z,1200
D,x,13
D,y,130
D,z,1300
"""
        ),
    )
    pd.testing.assert_frame_equal(df_out, df_expected)


def test_wide_to_tidy_fail():
    """Test the :func:`wide_to_tidy_xbrl` fails with a bad rename."""
    params = WideToTidy(
        idx_cols=["idx"],
        value_types=["test_value"],
        stacked_column_name="xbrl_factoid",
    )
    df_renamed = WIDE_TO_TIDY_DF.rename(columns={"z_test_value": "z_test_values"})
    with pytest.raises(AssertionError):
        wide_to_tidy(df=df_renamed, params=params)


def test_wide_to_tidy_rename():
    """Test the updated ``expected_drop_cols`` params for :func:`wide_to_tidy_xbrl`."""
    params_renamed = WideToTidy(
        idx_cols=["idx"],
        value_types=["test_value"],
        expected_drop_cols=1,
        stacked_column_name="xbrl_factoid",
    )
    df_renamed = WIDE_TO_TIDY_DF.rename(columns={"z_test_value": "z_test_values"})
    df_out = wide_to_tidy(df=df_renamed, params=params_renamed)
    # everything but the xbrl_factoid == "c"
    df_expected = pd.read_csv(
        StringIO(
            """
idx,xbrl_factoid,test_value
A,x,10
A,y,100
B,x,11
B,y,110
C,x,12
C,y,120
D,x,13
D,y,130
"""
        ),
    )
    pd.testing.assert_frame_equal(df_out, df_expected)


def test_select_current_year_annual_records_duration_xbrl():
    """Test :meth:`select_current_year_annual_records_duration_xbrl` date selection."""
    df = pd.read_csv(
        StringIO(
            """
report_year,start_date,end_date,values
2021,2020-01-01,2020-12-31,bad
2021,2021-01-01,2021-12-31,good
2021,2021-06-01,2022-12-31,bad
2022,2020-01-01,2020-12-31,bad
2022,2022-01-01,2022-12-31,good
2022,2022-06-01,2022-12-31,bad
"""
        )
    )

    class FakeTransformer(Ferc1AbstractTableTransformer):
        # just need any table name here so that one method is callable
        table_id = TableIdFerc1.FUEL_FERC1

    fake_transformer = FakeTransformer()
    df_out = fake_transformer.select_current_year_annual_records_duration_xbrl(df=df)
    df_expected = df[df.to_numpy() == "good"].astype(
        {"start_date": "datetime64[s]", "end_date": "datetime64[s]"}
    )
    pd.testing.assert_frame_equal(df_out, df_expected)


def test_drop_duplicate_rows_dbf():
    """Tests :func:`drop_duplicate_rows_dbf` outputs and fails as expected."""
    df = pd.read_csv(
        StringIO(
            """
report_year,utility_id_ferc1,asset_type,data_col1,data_col2
2021,71,stuff,70,700
2021,71,stuff,70,700
2021,81,junk,80,800
2021,81,junk,80,800
2021,91,big stuff,90,900
2021,91,big stuff,90,900
2021,101,stuff,1,10
2022,111,things,.5,.75
2022,111,things,,.75
2022,111,nada,,
2022,111,nada,,
"""
        )
    )
    params = DropDuplicateRowsDbf(
        table_name="balance_sheet_assets_ferc1", data_columns=["data_col1", "data_col2"]
    )
    df_out = drop_duplicate_rows_dbf(df, params=params).reset_index(drop=True)
    df_expected = pd.read_csv(
        StringIO(
            """
report_year,utility_id_ferc1,asset_type,data_col1,data_col2
2021,71,stuff,70,700
2021,81,junk,80,800
2021,91,big stuff,90,900
2021,101,stuff,1,10
2022,111,things,.5,.75
2022,111,nada,,
"""
        )
    )
    pd.testing.assert_frame_equal(df_out, df_expected)

    # if the PK dupes have different data an assertion should raise
    df_unique_dupes = df.copy()
    df_unique_dupes.loc[0, "data_col1"] = 74
    with pytest.raises(AssertionError):
        drop_duplicate_rows_dbf(df_unique_dupes, params=params)

    # if a PK dupes has mismatched null data
    df_nulls = pd.read_csv(
        StringIO(
            """
report_year,utility_id_ferc1,asset_type,data_col1,data_col2
2021,71,stuff,,700
2021,71,stuff,70,
"""
        )
    )
    with pytest.raises(AssertionError):
        drop_duplicate_rows_dbf(df_nulls, params=params)

    # if a PK dupes has mismatched null data
    df_null_w_unique_data = pd.read_csv(
        StringIO(
            """
report_year,utility_id_ferc1,asset_type,data_col1,data_col2
2021,71,stuff,,701
2021,71,stuff,70,700
2022,111,things,.5,.75
2022,111,things,,.75
"""
        )
    )
    with pytest.raises(AssertionError):
        drop_duplicate_rows_dbf(df_null_w_unique_data, params=params)


def test_unstack_balances_to_report_year_instant_xbrl():
    """Test :func:`unstack_balances_to_report_year_instant_xbrl`."""
    df = pd.read_csv(
        StringIO(
            """
idx,entity_id,date,report_year,sched_table_name,test_value
0,1,2021-12-31,2021,table_name,2000
1,1,2020-12-31,2021,table_name,1000
2,2,2021-12-31,2021,table_name,21000
3,2,2020-12-31,2021,table_name,8000
"""
        ),
    )
    params = UnstackBalancesToReportYearInstantXbrl(
        unstack_balances_to_report_year=True
    )
    pk_cols = ["entity_id", "report_year"]
    df_out = unstack_balances_to_report_year_instant_xbrl(
        df=df.copy(),
        params=params,
        primary_key_cols=pk_cols,
    )
    df_expected = pd.read_csv(
        StringIO(
            """
entity_id,report_year,sched_table_name,idx_ending_balance,idx_starting_balance,test_value_ending_balance,test_value_starting_balance
1,2021,table_name,0,1,2000,1000
2,2021,table_name,2,3,21000,8000
"""
        ),
    )
    pd.testing.assert_frame_equal(df_out, df_expected)

    # If there is more than one value per year (not report year) an AssertionError
    # should raise
    df_non_unique_years = df.copy()
    df_non_unique_years.loc[4] = [4, 2, "2020-12-31", 2021, "table_name", 500]
    with pytest.raises(AssertionError):
        unstack_balances_to_report_year_instant_xbrl(
            df_non_unique_years, params=params, primary_key_cols=pk_cols
        )

    # If there are mid-year values an AssertionError should raise
    df_mid_year = df.copy()
    df_mid_year.loc[
        (df_mid_year["entity_id"] == 2) & (df_mid_year["date"] == "2020-12-31"), "date"
    ] = "2020-06-30"
    with pytest.raises(AssertionError):
        unstack_balances_to_report_year_instant_xbrl(
            df_non_unique_years, params=params, primary_key_cols=pk_cols
        )
