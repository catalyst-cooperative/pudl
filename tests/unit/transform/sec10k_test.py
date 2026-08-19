"""Unit tests for SEC 10-K transform functions."""

from io import StringIO

import pandas as pd

from pudl.transform.sec10k import _match_ex21_subsidiaries_to_filer_company


def test_match_ex21_subsidiaries_to_filer_company():
    """Test if subsidiaries are correctly matched to their own filing information."""
    ownership_df_test = pd.read_csv(
        StringIO(
            """
subsidiary_company_id_sec10k,subsidiary_company_name,subsidiary_company_location,report_date
0000940181_skillsoft corporation_delaware,skillsoft corporation,delaware,2010-01-01
0001774675_skillsoft corp_,skillsoft corp,,2014-01-01
"0001774675_skillsoft corp_delaware, usa",skillsoft corp,"delaware, usa",2023-01-01
"0000017797_duke energy florida, inc_florida","duke energy florida, inc",,2015-01-01
"0000017797_duke energy florida, llc_florida","duke energy florida, llc",florida,2023-01-01
"0001326160_duke energy corp",duke energy corp,,2015-01-01
0000016918_stevens point beverage co_,stevens point beverage co,,2000-04-01
0000016918_stevens point beverage co_illinois,stevens point beverage co,illinois,1997-04-01
0000016918_stevens point beverage co_new york,stevens point beverage co,new york,1996-04-01
"""
        ),
        parse_dates=["report_date"],
    )
    # notably, skillsoft corporation moves location and re-incorporates
    # itself with a new CIK over time
    info_df_test = pd.read_csv(
        StringIO(
            """
central_index_key,company_name,incorporation_state,report_date
0001094451,skillsoft corporation,DE,2000-04-01
0001774675,skillsoft corp.,DE,2023-04-01
0000940181,skillsoft public limited co,,2000-01-01
0000037637,"duke energy florida, llc.",FL,2023-01-01
0000037637,"duke energy florida, inc.",FL,2020-01-01
0000000001,duke energy corporation,,2023-01-01
0000000002,duke energy corporation,OH,2015-01-01
0000914175,"Steven's Point Beverage Company",NY,1997-04-01
0000914175,"Steven's Point Beverage Company",WI,2001-04-01
"""
        ),
        parse_dates=["report_date"],
    )
    subs_to_filers_match_expected = pd.read_csv(
        StringIO(
            """
subsidiary_company_id_sec10k,subsidiary_company_name,subsidiary_company_location,report_date,subsidiary_company_central_index_key
0000940181_skillsoft corporation_delaware,skillsoft corporation,delaware,2010-01-01,0001094451
0001774675_skillsoft corp_,skillsoft corporation,,2014-01-01,0001774675
"0001774675_skillsoft corp_delaware, usa",skillsoft corporation,"delaware, usa",2023-01-01,0001774675
"0000017797_duke energy florida, inc_florida","duke energy florida incorporated",,2015-01-01,0000037637
"0000017797_duke energy florida, llc_florida","duke energy florida limited liability company",florida,2023-01-01,0000037637
"0001326160_duke energy corp",duke energy corporation,,2015-01-01,0000000002
0000016918_stevens point beverage co_,stevens point beverage company,,2000-04-01,0000914175
0000016918_stevens point beverage co_illinois,stevens point beverage company,illinois,1997-04-01,0000914175
0000016918_stevens point beverage co_new york,stevens point beverage company,new york,1996-04-01,0000914175
"""
        ),
        dtype={"subsidiary_company_central_index_key": "string"},
        parse_dates=["report_date"],
    )
    actual = _match_ex21_subsidiaries_to_filer_company(
        filer_info_df=info_df_test, ownership_df=ownership_df_test
    )
    actual["subsidiary_company_central_index_key"] = (
        actual["subsidiary_company_central_index_key"].astype("string").str.zfill(10)
    )
    pd.testing.assert_frame_equal(actual, subs_to_filers_match_expected)
