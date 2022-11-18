"""Unit tests specific to the FERC Form 1 table transformations."""

from io import StringIO

import pandas as pd
import pytest

from pudl.settings import Ferc1Settings
from pudl.transform.ferc1 import fill_dbf_to_xbrl_map, read_dbf_to_xbrl_map

TEST_DBF_XBRL_MAP = pd.read_csv(
    StringIO(
        """
sched_column_name,report_year,row_literal,row_number,row_type,xbrl_column_stem
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
    dbf_xbrl_map = dbf_xbrl_map[dbf_xbrl_map.xbrl_column_stem != "HEADER_ROW"]
    dbf_to_xbrl_mapping_is_unique = (
        dbf_xbrl_map.groupby(["report_year", "xbrl_column_stem"])[
            "row_number"
        ].nunique()
        <= 1
    ).all()

    assert dbf_to_xbrl_mapping_is_unique  # nosec: B101


def test_fill_dbf_to_xbrl_map():
    """Minimal unit test for our DBF to XBRL map filling function."""
    expected = pd.read_csv(
        StringIO(
            """
report_year,row_number,xbrl_column_stem
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
    actual = fill_dbf_to_xbrl_map(df=test_map, dbf_years=range(2000, 2004))
    actual = actual[actual.xbrl_column_stem != "HEADER_ROW"].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)
