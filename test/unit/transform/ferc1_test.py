"""Unit tests specific to the FERC Form 1 table transformations."""

import datetime
import itertools
from io import StringIO

import hypothesis
import numpy as np
import pandas as pd
import pandera
import pytest

import pudl.logging_helpers
from pudl.output.ferc1 import NodeId, XbrlCalculationForestFerc1
from pudl.settings import Ferc1Settings
from pudl.transform.ferc1 import (
    AddColumnsWithUniformValues,
    AddColumnWithUniformValue,
    DropDuplicateRowsDbf,
    Ferc1AbstractTableTransformer,
    Ferc1TableTransformParams,
    GroupMetricChecks,
    GroupMetricTolerances,
    MetricTolerances,
    ReconcileTableCalculations,
    TableIdFerc1,
    UnstackBalancesToReportYearInstantXbrl,
    WideToTidy,
    add_columns_with_uniform_values,
    assign_parent_dimensions,
    calculate_values_from_components,
    drop_duplicate_rows_dbf,
    fill_dbf_to_xbrl_map,
    filter_for_freshest_data_xbrl,
    infer_intra_factoid_totals,
    make_xbrl_factoid_dimensions_explicit,
    read_dbf_to_xbrl_map,
    reconcile_one_type_of_table_calculations,
    select_current_year_annual_records_duration_xbrl,
    unexpected_total_components,
    unstack_balances_to_report_year_instant_xbrl,
    wide_to_tidy,
)

logger = pudl.logging_helpers.get_logger(__name__)

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


def canonicalize(df):
    return (
        df.convert_dtypes()
        .sort_index(axis="columns")
        .pipe(lambda df: df.sort_values(list(df.columns)))
        .reset_index(drop=True)
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
sched_table_name,report_year,row_literal,row_number,xbrl_factoid
test_table1,2000,"Account A",2,account_a
test_table1,2000,"Account B",3,account_b
test_table1,2000,"Account C",5,account_c
test_table1,2001,"Account A",2,account_a
test_table1,2001,"Account B",3,account_b
test_table1,2001,"Account C",5,account_c
test_table1,2002,"Account A",2,account_a
test_table1,2002,"Account B",3,account_b
test_table1,2002,"Account B1",4,account_b1
test_table1,2002,"Account C",6,account_c
test_table1,2003,"Account A",2,account_a
test_table1,2003,"Account B",3,account_b
test_table1,2003,"Account B1",4,account_b1
test_table1,2003,"Account C",6,account_c
"""
        )
    )
    test_map = TEST_DBF_XBRL_MAP.reset_index(drop=True)
    actual = fill_dbf_to_xbrl_map(df=test_map, dbf_years=sorted(range(2000, 2004)))
    actual = actual[actual.xbrl_factoid != "HEADER_ROW"].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_like=True)


def test_two_table_fill_dbf_to_xbrl_map():
    """Test for filling DBF to XBRL map with two tables."""
    expected = pd.read_csv(
        StringIO(
            """
sched_table_name,report_year,row_number,xbrl_factoid,row_literal
test_table1,2000,2,account_a,"Account A"
test_table1,2000,3,account_b,"Account B"
test_table1,2000,5,account_c,"Account C"
test_table2,2000,7,account_d,"Account D"
test_table2,2000,8,account_e,"Account E"
test_table1,2001,2,account_a,"Account A"
test_table1,2001,3,account_b,"Account B"
test_table1,2001,5,account_c,"Account C"
test_table2,2001,7,account_d,"Account D"
test_table2,2001,8,account_e,"Account E"
test_table1,2002,2,account_a,"Account A"
test_table1,2002,3,account_b,"Account B"
test_table1,2002,4,account_b1,"Account B1"
test_table1,2002,6,account_c,"Account C"
test_table2,2002,8,account_d,"Account D"
test_table2,2002,9,account_e,"Account E"
test_table1,2003,2,account_a,"Account A"
test_table1,2003,3,account_b,"Account B"
test_table1,2003,4,account_b1,"Account B1"
test_table1,2003,6,account_c,"Account C"
test_table2,2003,8,account_d,"Account D"
test_table2,2003,9,account_e,"Account E"
"""
        )
    )
    test_map = pd.concat(
        [TEST_DBF_XBRL_MAP, TEST_MUTLI_TABLE_DBF_XBRL_MAP]
    ).reset_index(drop=True)
    actual = fill_dbf_to_xbrl_map(df=test_map, dbf_years=sorted(range(2000, 2004)))
    actual = actual[actual.xbrl_factoid != "HEADER_ROW"].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_like=True)


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
    """Test :func:`select_current_year_annual_records_duration_xbrl` date selection."""
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

    df_out = select_current_year_annual_records_duration_xbrl(
        df=df, table_name="fake_table"
    )
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
        table_name="core_ferc1__yearly_balance_sheet_assets_sched110",
        data_columns=["data_col1", "data_col2"],
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
0,1,2022-12-31,2022,table_name,2022.1
1,1,2021-12-31,2021,table_name,2021.1
2,1,2020-12-31,2020,table_name,2020.1
3,2,2021-12-31,2021,table_name,2021.2
4,2,2020-12-31,2020,table_name,2020.2
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
    # because there are NaNs in idx when we unstack, both idx balances are floats.
    df_expected = pd.read_csv(
        StringIO(
            """
entity_id,report_year,sched_table_name,idx_ending_balance,idx_starting_balance,test_value_ending_balance,test_value_starting_balance
1,2021,table_name,1.0,2.0,2021.1,2020.1
1,2022,table_name,0.0,1.0,2022.1,2021.1
2,2021,table_name,3.0,4.0,2021.2,2020.2
2,2022,table_name,,3.0,,2021.2
"""
        ),
    )
    pd.testing.assert_frame_equal(df_out, df_expected)

    # If there is more than one value per year (not report year) an AssertionError
    # should raise
    df_non_unique_years = df.copy()
    df_non_unique_years.loc[len(df_non_unique_years.index)] = [
        5,
        2,
        "2020-12-31",
        2020,
        "table_name",
        2020.15,
    ]

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


def test_add_columns_with_uniform_values():
    """Test :func:`add_columns_with_uniform_values`."""
    df = pd.DataFrame(index=[0, 1, 2])
    params = AddColumnsWithUniformValues(
        columns_to_add={
            "utility_type": {"column_value": "electric"},
            "plant_status": {"column_value": "in_service"},
        }
    )
    df_expected = pd.DataFrame(
        {"utility_type": ["electric"] * 3, "plant_status": ["in_service"] * 3}
    )
    df_out = add_columns_with_uniform_values(df, params)
    pd.testing.assert_frame_equal(df_expected, df_out)

    # test with a mixed is_dimension flag
    params2 = AddColumnsWithUniformValues(
        columns_to_add={
            "utility_type": {"column_value": "electric", "is_dimension": True},
            "plant_status": {"column_value": "in_service", "is_dimension": False},
        }
    )
    df_out2 = add_columns_with_uniform_values(df, params2)
    pd.testing.assert_frame_equal(df_expected, df_out2)


def test_dimension_columns():
    """Test the :meth:`Ferc1TableTransformParams.dimension_columns`.

    Can ``dimension_columns`` grab a column from :class:`AddColumnsWithUniformValues` and ignore
    a column labled as ``is_dimension=False``?

    Can ``dimension_columns`` also grab a different ``subdimension_column``
    from :class:`ReconcileTableCalculations`?

    Will ``dimension_columns`` return only one column if the dimension column from
    :class:`AddColumnsWithUniformValues` and :class:`ReconcileTableCalculations` are the same?
    """
    add_columns_with_uniform_values = AddColumnsWithUniformValues(
        columns_to_add={
            "added_dim": AddColumnWithUniformValue(
                column_value="i'm dim", is_dimension=True
            ),
            "not_a_dim": AddColumnWithUniformValue(
                column_value="nope", is_dimension=False
            ),
        }
    )
    params1 = Ferc1TableTransformParams(
        add_columns_with_uniform_values=add_columns_with_uniform_values,
    )
    assert params1.dimension_columns == ["added_dim"]

    reconcile_table_calculations = ReconcileTableCalculations(
        subdimension_column="sub_dim"
    )
    params2 = Ferc1TableTransformParams(
        add_columns_with_uniform_values=add_columns_with_uniform_values,
        reconcile_table_calculations=reconcile_table_calculations,
    )
    assert sorted(params2.dimension_columns) == sorted(["sub_dim", "added_dim"])
    reconcile_table_calculations = ReconcileTableCalculations(
        subdimension_column="added_dim"
    )
    params3 = Ferc1TableTransformParams(
        add_columns_with_uniform_values=add_columns_with_uniform_values,
        reconcile_table_calculations=reconcile_table_calculations,
    )
    assert params3.dimension_columns == ["added_dim"]


def test_calculate_values_from_components():
    """Test :func:`calculate_values_from_components`."""
    # drawing inspo from kim stanley robinson books
    calculation_components_ksr = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,planet,planet_parent,weight
books,big_fact,books,lil_fact_x,venus,venus,1
books,big_fact,books,lil_fact_z,venus,venus,1
books,big_fact,books,lil_fact_y,venus,venus,1
books,big_fact,books,lil_fact_x,earth,earth,1
books,big_fact,books,lil_fact_z,earth,earth,1
books,big_fact,books,lil_fact_y,earth,earth,1
"""
        )
    )
    data_ksr = pd.read_csv(
        StringIO(
            f"""
table_name,xbrl_factoid,planet,value,utility_id_ferc1,report_year
books,lil_fact_x,venus,10,44,2312
books,lil_fact_z,venus,11,44,2312
books,lil_fact_y,venus,12,44,2312
books,big_fact,venus,{10 + 11 + 12},44,2312
books,lil_fact_x,earth,3,44,2312
books,lil_fact_z,earth,4,44,2312
books,lil_fact_y,earth,5,44,2312
books,big_fact,earth,{3 + 4 + 5},44,2312
"""
        )
    )
    expected_ksr = pd.read_csv(
        StringIO(
            f"""
table_name,xbrl_factoid,planet,value,utility_id_ferc1,report_year,calculated_value
books,lil_fact_x,venus,10.0,44,2312,
books,lil_fact_z,venus,11.0,44,2312,
books,lil_fact_y,venus,12.0,44,2312,
books,big_fact,venus,33.0,44,2312,{10 + 11 + 12}
books,lil_fact_x,earth,3.0,44,2312,
books,lil_fact_z,earth,4.0,44,2312,
books,lil_fact_y,earth,5.0,44,2312,
books,big_fact,earth,12.0,44,2312,{3 + 4 + 5}
"""
        )
    ).convert_dtypes()
    actual_ksr = calculate_values_from_components(
        calculation_components=calculation_components_ksr,
        data=data_ksr,
        calc_idx=["table_name", "xbrl_factoid", "planet"],
        value_col="value",
    )[list(expected_ksr.columns)].convert_dtypes()
    idx = ["xbrl_factoid", "planet"]
    pd.testing.assert_frame_equal(
        actual_ksr.set_index(idx).sort_index(),
        expected_ksr.set_index(idx).sort_index(),
    )


TABLE_NAME = "table_a"
FACT_NAME = "my_cool_fact"
VALUE_COL = "value"
DIMENSION_VALUES = {
    "utility_type": ["electric", "gas", "total"],
    "plant_status": ["future", "in_service", "total"],
}


def _sample_multi_subdimension_calculation_componets(
    dimension_values: dict[str, list[str]] = DIMENSION_VALUES,
    fact_name: str = FACT_NAME,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    """Build a sample calculation componet table with :func:`infer_intra_factoid_totals`.
    This will build a calculation component table with one fact's total to sub-total
    dimension calculations with the given ``dimension_values``.

    Args:
        dimension_values: dictionary of dimension column names (keys) to dimension values.
        fact_name: name of ``xbrl_factoid``.
        table_name: name of table.
    """
    dimension_pairs = list(itertools.product(*dimension_values.values()))
    dimension_cols = list(dimension_values.keys())

    meta_w_dims = pd.DataFrame(columns=dimension_cols, data=dimension_pairs).assign(
        xbrl_factoid=fact_name, table_name=table_name
    )
    calc_comps = infer_intra_factoid_totals(
        calc_components=pd.DataFrame(),
        meta_w_dims=meta_w_dims,
        # we can use the meta w/ dims bc its the same structure
        # and assumes that every combo of these dims exist in the table
        table_dimensions=meta_w_dims,
        dimensions=dimension_cols,
    )
    return calc_comps


def _ungrouped_all_errors_group_metric_check():
    # just one check w/ a 100% error rate so we never trip the checks
    return GroupMetricChecks(
        groups_to_check=["ungrouped"],
        metrics_to_check=["error_frequency"],
        group_metric_tolerances=GroupMetricTolerances(
            ungrouped=MetricTolerances(error_frequency=1)
        ),
    )


def _sample_mutli_subdimension_reconciled_data(
    data,
    dimension_values=DIMENSION_VALUES,
    fact_name=FACT_NAME,
    table_name=TABLE_NAME,
    value_col=VALUE_COL,
):
    """Builds stock calculation components table and reconciles input data."""

    dimension_cols = list(dimension_values.keys())
    calc_idx = ["table_name", "xbrl_factoid"] + dimension_cols
    calc_comps = _sample_multi_subdimension_calculation_componets(
        dimension_values, fact_name, table_name
    )

    group_metric_checks = _ungrouped_all_errors_group_metric_check()
    data = data.assign(
        table_name=table_name,
        xbrl_factoid=fact_name,
        utility_id_ferc1=144,
        report_year=2021,
        row_type_xbrl=lambda x: np.where(
            (x[dimension_cols] == "total").any(axis="columns"),
            "calculated_value",
            pd.NA,
        ),
    )

    return reconcile_one_type_of_table_calculations(
        data=data,
        calculation_components=calc_comps,
        calc_idx=calc_idx,
        value_col=value_col,
        group_metric_checks=group_metric_checks,
        table_name=table_name,
        is_subdimension=True,
        calc_to_data_merge_validation="many_to_many",
    )


def test_multi_subdimension_corrections_when_only_double_has_data():
    """Test several iterations of reconciling calculations with two dimensions with subtotals.

    The total to subdimension calculcations are constructed in
    :func:`infer_intra_factoid_totals` never includes any "total" dimension in
    the child calculcation component.

    This is reasonable because that means there is never any double counting
    of the elements of a total (i.e. the calculation for a big total of a calc w/ two
    dimension cols never has both the total and the subdimensions of that total).
    It makes thinking about the total to subdimension calculations challenging in some
    ways (it feels intuative to be able to calculate a parent w/ a total & total dimensions
    from the total & subdims *as well as* the sumdims & total). But that would be duplicative
    and that's not how :func:`infer_intra_factoid_totals` works. We calculate a parent
    total & total calculation from the subdims & subdims. Fully skipping over the intermideary
    mixed total/subdim calculations.

    """
    # Test if a big total/total calculation w/ null children gets a correction
    total_total_value = 100
    data1 = pd.read_csv(
        StringIO(
            f"""
utility_type,plant_status,value
electric,future,
electric,in_service,
electric,total,
gas,future,
gas,in_service,
gas,total,
total,future,
total,in_service,
total,total,{total_total_value}
    """
        )
    )
    out1 = _sample_mutli_subdimension_reconciled_data(data1)
    corrections1 = out1.loc[
        out1.row_type_xbrl == "subdimension_correction", "value"
    ].to_numpy()
    assert len(corrections1) == 1
    assert corrections1[0] == total_total_value


def test_multi_subdimension_corrections_when_double_total_and_subdimension_has_data():
    # test the big total/total with non-null but off leaves get a correction
    total_total_value = 100
    data2 = pd.read_csv(
        StringIO(
            f"""
utility_type,plant_status,value
electric,future,5
electric,in_service,70
gas,future,5
gas,in_service,5
total,total,{total_total_value}
    """
        )
    )
    out2 = _sample_mutli_subdimension_reconciled_data(data2)
    corrections2 = out2.loc[
        out2.row_type_xbrl == "subdimension_correction", "value"
    ].to_numpy()
    assert len(corrections2) == 1
    assert corrections2[0] == total_total_value - 70 - 5 - 5 - 5


def test_multi_subdimension_corrections_when_only_double_and_mixed_has_data():
    # demonstrate that a big total/total records does not get calculated by the total/subdim records
    total_total_value = 100
    data3 = pd.read_csv(
        StringIO(
            f"""
utility_type,plant_status,value
electric,future,
electric,in_service,
electric,total,
gas,future,
gas,in_service,
gas,total,
total,future,5
total,in_service,70
total,total,{total_total_value}
    """
        )
    )

    out3 = _sample_mutli_subdimension_reconciled_data(data3)
    out3_sub = out3.loc[
        out3.row_type_xbrl == "subdimension_correction",
        ["xbrl_factoid", "utility_type", "plant_status", "value"],
    ].reset_index(drop=True)
    expected3 = (
        pd.read_csv(
            StringIO(
                f"""
xbrl_factoid,utility_type,plant_status,value
my_cool_fact_subdimension_correction,total,future,5
my_cool_fact_subdimension_correction,total,in_service,70
my_cool_fact_subdimension_correction,total,total,{total_total_value}
    """
            )
        )
        .convert_dtypes()
        .astype({"value": float})
    )
    pd.testing.assert_frame_equal(out3_sub, expected3)


def test_multi_subdimension_corrections():
    total_total_value = 100
    eis = 70
    ef = 5
    et = 85
    gf = 4
    gis = 5
    gt = 10
    tf = 10
    tis = 70
    data4 = pd.read_csv(
        StringIO(
            f"""
utility_type,plant_status,value
electric,future,{ef}
electric,in_service,{eis}
electric,total,{et}
gas,future,{gf}
gas,in_service,{gis}
gas,total,{gt}
total,future,{tf}
total,in_service,{tis}
total,total,{total_total_value}
    """
        )
    )
    expected4 = (
        pd.read_csv(
            StringIO(
                f"""
xbrl_factoid,utility_type,plant_status,value
my_cool_fact_subdimension_correction,electric,total,{et - eis - ef}
my_cool_fact_subdimension_correction,gas,total,{gt - gis - gf}
my_cool_fact_subdimension_correction,total,future,{tf - ef - gf}
my_cool_fact_subdimension_correction,total,in_service,{tis - eis - gis}
my_cool_fact_subdimension_correction,total,total,{total_total_value - eis - ef - gis - gf}
    """
            )
        )
        .convert_dtypes()
        .astype({"value": float})
    )
    out4 = _sample_mutli_subdimension_reconciled_data(data4)
    out4_sub = out4.loc[
        out4.row_type_xbrl == "subdimension_correction",
        ["xbrl_factoid", "utility_type", "plant_status", "value"],
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(expected4, out4_sub)


def test_pruned_mixed_totals():
    calc_comps_for_forest = _sample_multi_subdimension_calculation_componets().assign(
        plant_function="i_hate_nulls", plant_function_parent="i_hate_nulls"
    )
    forest = XbrlCalculationForestFerc1(
        exploded_calcs=calc_comps_for_forest,
        seeds=[
            NodeId(
                table_name=TABLE_NAME,
                xbrl_factoid=FACT_NAME,
                utility_type="total",
                plant_status="total",
                plant_function="i_hate_nulls",
            )
        ],
        tags=pd.DataFrame(columns=list(NodeId._fields)),
        group_metric_checks=_ungrouped_all_errors_group_metric_check(),
    )
    dims = ["utility_type", "plant_status"]
    pruned = pd.DataFrame(forest.pruned)[dims].sort_values(dims).reset_index(drop=True)
    # we expect all of the mixed-total records to be pruned
    pruned_expected = pd.read_csv(
        StringIO(
            """
utility_type,plant_status
electric,total
gas,total
total,future
total,in_service
    """
        )
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(pruned, pruned_expected)


def test_apply_xbrl_calculation_fixes():
    """Test :meth:`Ferc1AbstractTableTransformer.apply_xbrl_calculation_fixes`."""
    calc_comps_fix_test = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,weight
table_a,fact_1,table_a,replace_me,-1
table_a,fact_1,table_a,keep_me,1
table_a,fact_1,table_a,delete_me,1
"""
        )
    )

    calc_fixes_test = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,weight
table_a,fact_1,table_a,replace_me,1
table_a,fact_1,table_a,delete_me,
"""
        )
    )

    calc_comps_fixed_expected = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,weight
table_a,fact_1,table_a,keep_me,1.0
table_a,fact_1,table_a,replace_me,1.0
"""
        )
    )

    class FakeTransformer(Ferc1AbstractTableTransformer):
        # just need any table name here so that one method is callable
        table_id = TableIdFerc1.STEAM_PLANTS_FUEL

    calc_comps_fixed_out = FakeTransformer().apply_xbrl_calculation_fixes(
        calc_components=calc_comps_fix_test, calc_fixes=calc_fixes_test
    )
    pd.testing.assert_frame_equal(calc_comps_fixed_expected, calc_comps_fixed_out)


def test_make_xbrl_factoid_dimensions_explicit():
    """Test :func:`make_xbrl_factoid_dimensions_explicit`"""
    calc_comp_idx = [
        "table_name_parent",
        "xbrl_factoid_parent",
        "table_name",
        "xbrl_factoid",
    ]
    calc_comps_trek = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,dim_x,dim_y
table_a,fact_1,table_a,fact_3,voyager,
table_a,fact_1,table_a,fact_4,voyager,
table_a,fact_1,table_a,fact_5,ds9,
table_a,fact_2,table_b,fact_6,,futile
table_a,fact_2,table_b,fact_7,,futile
table_a,fact_2,table_b,fact_8,,
"""
        )
    )
    table_dimensions_trek = pd.read_csv(
        StringIO(
            """
table_name,xbrl_factoid,dim_x,dim_y
table_a,fact_3,voyager,coffee
table_a,fact_3,voyager,in
table_a,fact_3,voyager,that
table_a,fact_3,voyager,nebula
table_a,fact_3,voyager,total
table_a,fact_4,voyager,coffee
table_a,fact_4,voyager,in
table_a,fact_4,voyager,that
table_a,fact_4,voyager,nebula
table_a,fact_4,voyager,total
table_a,fact_5,ds9,
table_b,fact_6,next_gen,resistance
table_b,fact_6,next_gen,is
table_b,fact_6,next_gen,futile
table_b,fact_7,next_gen,resistance
table_b,fact_7,next_gen,is
table_b,fact_7,next_gen,futile
table_b,fact_8,next_gen,resistance
table_b,fact_8,next_gen,is
table_b,fact_8,next_gen,futile
"""
        )
    )
    out_trek = (
        make_xbrl_factoid_dimensions_explicit(
            df_w_xbrl_factoid=calc_comps_trek,
            table_dimensions_ferc1=table_dimensions_trek,
            dimensions=["dim_x", "dim_y"],
        )
        .convert_dtypes()
        .sort_values(calc_comp_idx)
        .reset_index(drop=True)
    )
    expected_trek = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,dim_x,dim_y
table_a,fact_1,table_a,fact_3,voyager,coffee
table_a,fact_1,table_a,fact_3,voyager,in
table_a,fact_1,table_a,fact_3,voyager,that
table_a,fact_1,table_a,fact_3,voyager,nebula
table_a,fact_1,table_a,fact_3,voyager,total
table_a,fact_1,table_a,fact_4,voyager,coffee
table_a,fact_1,table_a,fact_4,voyager,in
table_a,fact_1,table_a,fact_4,voyager,that
table_a,fact_1,table_a,fact_4,voyager,nebula
table_a,fact_1,table_a,fact_4,voyager,total
table_a,fact_1,table_a,fact_5,ds9,
table_a,fact_2,table_b,fact_6,next_gen,futile
table_a,fact_2,table_b,fact_7,next_gen,futile
table_a,fact_2,table_b,fact_8,next_gen,resistance
table_a,fact_2,table_b,fact_8,next_gen,is
table_a,fact_2,table_b,fact_8,next_gen,futile
"""
        )
    ).convert_dtypes()
    pd.testing.assert_frame_equal(out_trek, expected_trek)
    # swap the order of the dims to test whether the input order effects the result
    out_reordered = (
        make_xbrl_factoid_dimensions_explicit(
            df_w_xbrl_factoid=calc_comps_trek,
            table_dimensions_ferc1=table_dimensions_trek,
            dimensions=["dim_y", "dim_x"],
        )
        .sort_values(calc_comp_idx)
        .reset_index(drop=True)
        .convert_dtypes()
    )
    pd.testing.assert_frame_equal(out_trek, out_reordered, check_like=True)


def test_adding_parent_dimensions():
    """Test :func:`assign_parent_dimensions` & :func:`infer_intra_factoid_totals`.

    These two parent dimension steps are related so we test them in the same process.
    """

    # existing calc comps - these should remain unmolested throughout
    # table_a:fact_1 -> table_a:fact_3[dim_x[voyager],dim_y[coffee,in,that,nebula,total]]
    calc_comps_trek = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,table_name,xbrl_factoid,dim_x,dim_y,is_within_table_calc,weight
table_a,fact_1,table_a,fact_3,voyager,coffee,True,2
table_a,fact_1,table_a,fact_3,voyager,in,True,2
table_a,fact_1,table_a,fact_3,voyager,that,True,2
table_a,fact_1,table_a,fact_3,voyager,nebula,True,2
table_a,fact_1,table_a,fact_3,voyager,total,True,2
"""
        )
    )

    table_dimensions_same_trek = pd.read_csv(
        StringIO(
            """
table_name,xbrl_factoid,dim_x,dim_y
table_a,fact_1,voyager,coffee
table_a,fact_1,voyager,in
table_a,fact_1,voyager,that
table_a,fact_1,voyager,nebula
table_a,fact_1,voyager,total
table_a,fact_3,voyager,coffee
table_a,fact_3,voyager,in
table_a,fact_3,voyager,that
table_a,fact_3,voyager,nebula
table_a,fact_3,voyager,total
"""
        )
    )

    out_parent_dim_same_trek = canonicalize(
        assign_parent_dimensions(
            calc_components=calc_comps_trek,
            table_dimensions=table_dimensions_same_trek,
            dimensions=["dim_x", "dim_y"],
        )
    )

    # For fact_1, attach the fact_3 dimensions of the child components
    expected_parent_dim_trek = canonicalize(
        pd.read_csv(
            StringIO(
                """
table_name,xbrl_factoid,dim_x,dim_y,is_within_table_calc,dim_x_parent,table_name_parent,xbrl_factoid_parent,dim_y_parent,weight
table_a,fact_3,voyager,coffee,True,voyager,table_a,fact_1,coffee,2
table_a,fact_3,voyager,in,True,voyager,table_a,fact_1,in,2
table_a,fact_3,voyager,that,True,voyager,table_a,fact_1,that,2
table_a,fact_3,voyager,nebula,True,voyager,table_a,fact_1,nebula,2
table_a,fact_3,voyager,total,True,voyager,table_a,fact_1,total,2
"""
            )
        )
    )
    pd.testing.assert_frame_equal(out_parent_dim_same_trek, expected_parent_dim_trek)

    expected_total_to_subdim = canonicalize(
        pd.read_csv(
            StringIO(
                """
table_name_parent,xbrl_factoid_parent,dim_x_parent,dim_y_parent,table_name,xbrl_factoid,dim_x,dim_y,is_within_table_calc,weight,is_total_to_subdimensions_calc
table_a,fact_1,voyager,coffee,table_a,fact_3,voyager,coffee,True,2,False
table_a,fact_1,voyager,in,table_a,fact_3,voyager,in,True,2,False
table_a,fact_1,voyager,that,table_a,fact_3,voyager,that,True,2,False
table_a,fact_1,voyager,nebula,table_a,fact_3,voyager,nebula,True,2,False
table_a,fact_1,voyager,total,table_a,fact_3,voyager,total,True,2,False
table_a,fact_1,voyager,total,table_a,fact_1,voyager,coffee,True,1,True
table_a,fact_1,voyager,total,table_a,fact_1,voyager,in,True,1,True
table_a,fact_1,voyager,total,table_a,fact_1,voyager,that,True,1,True
table_a,fact_1,voyager,total,table_a,fact_1,voyager,nebula,True,1,True
table_a,fact_3,voyager,total,table_a,fact_3,voyager,coffee,True,1,True
table_a,fact_3,voyager,total,table_a,fact_3,voyager,in,True,1,True
table_a,fact_3,voyager,total,table_a,fact_3,voyager,that,True,1,True
table_a,fact_3,voyager,total,table_a,fact_3,voyager,nebula,True,1,True
"""
            )
        )
    )

    out_total_to_subdim = canonicalize(
        infer_intra_factoid_totals(
            calc_components=out_parent_dim_same_trek,
            meta_w_dims=table_dimensions_same_trek,
            table_dimensions=table_dimensions_same_trek,
            dimensions=["dim_x", "dim_y"],
        )
    )

    pd.testing.assert_frame_equal(
        out_total_to_subdim,
        expected_total_to_subdim,
    )


def test_multi_dims_totals():
    # observed dimension: values
    # utility_type: electric
    # plant_status: future, in_service, total
    # plant_function: steam_production, general, total
    table_dims = pd.read_csv(
        StringIO(
            """
table_name,xbrl_factoid,utility_type,plant_status,plant_function
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,steam_production
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,steam_production
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,steam_production
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,general
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,general
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,general
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,total
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,total
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,total
"""
        )
    )

    # metadata dimension: values
    # utility_type: electric
    # plant_status: future, in_service, total
    # plant_function: steam_production, general, bogus, total

    meta_w_dims = pd.read_csv(
        StringIO(
            """
table_name,xbrl_factoid,utility_type,plant_status,plant_function
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,steam_production
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,general
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,bogus
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,total
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,steam_production
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,general
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,bogus
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,total
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,steam_production
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,general
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,bogus
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,total
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,NA,NA
"""
        )
    )
    calcs = pd.DataFrame(
        columns=[
            "table_name_parent",
            "xbrl_factoid_parent",
            "utility_type_parent",
            "plant_status_parent",
            "plant_function_parent",
            "table_name",
            "xbrl_factoid",
            "utility_type",
            "plant_status",
            "plant_function",
        ]
    )

    dimensions = ["utility_type", "plant_status", "plant_function"]
    calc_comps = (
        calcs.astype({dim: pd.StringDtype() for dim in dimensions})
        .pipe(
            make_xbrl_factoid_dimensions_explicit,
            table_dims,
            dimensions=dimensions,
        )
        .pipe(
            assign_parent_dimensions,
            table_dimensions=table_dims,
            dimensions=dimensions,
        )
    )
    # calc_components
    assert calc_comps.empty

    calc_components_w_totals = calc_comps.pipe(
        infer_intra_factoid_totals,
        meta_w_dims=meta_w_dims,
        table_dimensions=table_dims,
        dimensions=dimensions,
    ).pipe(canonicalize)

    # total/total has the 4 components we expect ([future, in_service] X [steam_production, general])
    # all 4 1-dimensional totals have 2 components each

    calc_components_w_totals_expected = canonicalize(
        pd.read_csv(
            StringIO(
                """
table_name_parent,xbrl_factoid_parent,utility_type_parent,plant_status_parent,plant_function_parent,table_name,xbrl_factoid,utility_type,plant_status,plant_function,is_within_table_calc,weight,is_total_to_subdimensions_calc
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,steam_production,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,general,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,steam_production,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,general,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,steam_production,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,steam_production,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,steam_production,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,steam_production,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,general,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,general,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,total,general,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,general,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,steam_production,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,in_service,general,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,steam_production,True,1,True
electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,total,electric_plant_depreciation_change_ferc1,accumulated_depreciation,electric,future,general,True,1,True
"""
            )
        )
    )

    pd.testing.assert_frame_equal(
        calc_components_w_totals_expected,
        calc_components_w_totals,
    )


def test_unexpected_total_components():
    dimensions = ["utility_type", "plant_status", "plant_function"]

    has_extra_components = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,utility_type_parent,plant_status_parent,plant_function_parent,table_name,xbrl_factoid,utility_type,plant_status,plant_function
table_1,factoid_1,electric,total,total,table_1,factoid_1,electric,in_service,steam_production
table_1,factoid_1,electric,total,total,table_1,factoid_1,gas,in_service,steam_production
table_1,factoid_1,electric,total,total,table_1,factoid_1,total,in_service,steam_production
"""
        )
    )
    assert len(unexpected_total_components(has_extra_components, dimensions)) == 2

    no_extra_components = pd.read_csv(
        StringIO(
            """
table_name_parent,xbrl_factoid_parent,utility_type_parent,plant_status_parent,plant_function_parent,table_name,xbrl_factoid,utility_type,plant_status,plant_function
table_1,factoid_1,electric,total,general,table_1,factoid_2,electric,in_service,steam_production
table_1,factoid_1,electric,total,general,table_1,factoid_2,electric,in_service,general
table_1,factoid_1,electric,total,general,table_1,factoid_2,electric,in_service,total
table_1,factoid_1,electric,total,general,table_1,factoid_2,electric,in_service,general
table_1,factoid_1,electric,total,total,table_1,factoid_1,electric,in_service,steam_production
table_1,factoid_1,electric,total,total,table_1,factoid_1,electric,in_service,general
table_1,factoid_1,electric,total,total,table_1,factoid_1,electric,future,steam_production
table_1,factoid_1,electric,total,total,table_1,factoid_1,electric,future,general
"""
        )
    )
    assert unexpected_total_components(no_extra_components, dimensions).empty


def test_filter_for_freshest_data_xbrl_simple():
    df = pd.DataFrame.from_records(
        [
            {
                "entity_id": "C000001",
                "utility_type_axis": "electric",
                "filing_name": "Utility_Co_0001",
                "date": datetime.date(2021, 12, 31),
                "publication_time": datetime.datetime(2022, 2, 1, 0, 0, 0),
                "str_factoid": "original 2021 EOY value",
            },
            {
                "entity_id": "C000001",
                "utility_type_axis": "electric",
                "filing_name": "Utility_Co_0002",
                "date": datetime.date(2021, 12, 31),
                "publication_time": datetime.datetime(2022, 2, 1, 1, 1, 1),
                "str_factoid": "updated 2021 EOY value",
            },
        ]
    )
    observed_table = filter_for_freshest_data_xbrl(
        df,
        ["entity_id", "filing_name", "publication_time", "date", "utility_type_axis"],
    )

    assert len(observed_table) == 1
    assert observed_table.str_factoid.to_numpy().item() == "updated 2021 EOY value"


example_schema = pandera.DataFrameSchema(
    {
        "entity_id": pandera.Column(
            str, pandera.Check.isin("C0123456789"), nullable=False
        ),
        "date": pandera.Column("datetime64[ns]", nullable=False),
        "utility_type": pandera.Column(
            str,
            pandera.Check.isin(["electric", "gas", "total", "other"]),
            nullable=False,
        ),
        "publication_time": pandera.Column("datetime64[ns]", nullable=False),
        "int_factoid": pandera.Column(int),
        "float_factoid": pandera.Column(float),
        "str_factoid": pandera.Column(str),
    }
)


# ridiculous deadline - dataframe generation is always slow and sometimes
# *very* slow
@pytest.mark.slow
@hypothesis.settings(print_blob=True, deadline=2_000)
@hypothesis.given(example_schema.strategy(size=3))
def test_filter_for_freshest_data_xbrl(df):
    # XBRL context is the identifying metadata for reported values
    xbrl_context_cols = ["entity_id", "date", "utility_type"]
    filing_metadata_cols = ["publication_time", "filing_name"]
    primary_keys = xbrl_context_cols + filing_metadata_cols
    deduped = filter_for_freshest_data_xbrl(df, primary_keys)
    deduped_schema = example_schema.remove_columns(["publication_time"])
    deduped_schema.validate(deduped)

    # every post-deduplication row exists in the original rows
    assert (deduped.merge(df, how="left", indicator=True)._merge != "left_only").all()
    # for every [entity_id, utility_type, date] - there is only one row
    assert (~deduped.duplicated(subset=xbrl_context_cols)).all()
    # for every *context* in the input there is a corresponding row in the output
    original_contexts = df.groupby(xbrl_context_cols, as_index=False).last()
    paired_by_context = original_contexts.merge(
        deduped,
        on=xbrl_context_cols,
        how="outer",
        suffixes=["_in", "_out"],
        indicator=True,
    ).set_index(xbrl_context_cols)
    hypothesis.note(
        f"Found these contexts ({xbrl_context_cols}) in input data:\n{original_contexts[xbrl_context_cols]}"
    )
    hypothesis.note(f"The freshest data:\n{deduped}")
    hypothesis.note(f"Paired by context:\n{paired_by_context}")
    assert (paired_by_context._merge == "both").all()
