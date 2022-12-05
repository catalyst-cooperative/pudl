"""Unit tests specific to the FERC Form 1 table transformations."""

from io import StringIO

import pandas as pd
import pytest

from pudl.settings import Ferc1Settings
from pudl.transform.ferc1 import (
    Ferc1AbstractTableTransformer,
    Ferc1TableId,
    WideToTidyDataSource,
    fill_dbf_to_xbrl_map,
    read_dbf_to_xbrl_map,
    wide_to_tidy,
)

TEST_DBF_XBRL_MAP = pd.read_csv(
    StringIO(
        """
sched_column_name,report_year,row_literal,row_number,row_type,xbrl_factoid
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
        df=read_dbf_to_xbrl_map(dbf_table_name=dbf_table_name),
        dbf_years=Ferc1Settings().dbf_years,
    )
    dbf_xbrl_map = dbf_xbrl_map[dbf_xbrl_map.xbrl_factoid != "HEADER_ROW"]
    dbf_to_xbrl_mapping_is_unique = (
        dbf_xbrl_map.groupby(["report_year", "xbrl_factoid"])["row_number"].nunique()
        <= 1
    ).all()

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
    test_map = TEST_DBF_XBRL_MAP.drop(
        ["sched_column_name", "row_literal"], axis="columns"
    ).reset_index(drop=True)
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
    params = WideToTidyDataSource(
        **{
            "idx_cols": ["idx"],
            "value_types": ["test_value"],
            "stacked_column_name": "xbrl_factoid",
        }
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
    params = WideToTidyDataSource(
        **{
            "idx_cols": ["idx"],
            "value_types": ["test_value"],
            "stacked_column_name": "xbrl_factoid",
        }
    )
    df_renamed = WIDE_TO_TIDY_DF.rename(columns={"z_test_value": "z_test_values"})
    with pytest.raises(AssertionError):
        wide_to_tidy(df=df_renamed, params=params)


def test_wide_to_tidy_rename():
    """Test the updated ``expected_drop_cols`` params for :func:`wide_to_tidy_xbrl`."""
    params_renamed = WideToTidyDataSource(
        **{
            "idx_cols": ["idx"],
            "value_types": ["test_value"],
            "expected_drop_cols": 1,
            "stacked_column_name": "xbrl_factoid",
        }
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
        table_id = Ferc1TableId.FUEL_FERC1

    fake_transformer = FakeTransformer()
    df_out = fake_transformer.select_current_year_annual_records_duration_xbrl(df=df)
    df_expected = df[df.values == "good"].astype(
        {"start_date": "datetime64", "end_date": "datetime64"}
    )
    pd.testing.assert_frame_equal(df_out, df_expected)
