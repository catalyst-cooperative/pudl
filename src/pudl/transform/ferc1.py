"""Classes & functions to process FERC Form 1 data before loading into the PUDL DB.

Note that many of the classes/objects here inherit from/are instances of classes defined
in :mod:`pudl.transform.classes`. Their design and relationships to each other are
documented in that module.

See :mod:`pudl.transform.params.ferc1` for the values that parameterize many of these
transformations.
"""
import enum
import importlib.resources
import re
from collections import namedtuple
from collections.abc import Mapping
from typing import Any, Literal

import numpy as np
import pandas as pd
import sqlalchemy as sa
from dagster import AssetIn, AssetsDefinition, asset
from pandas.core.groupby import DataFrameGroupBy
from pydantic import validator

import pudl
from pudl.analysis.classify_plants_ferc1 import (
    plants_steam_assign_plant_ids,
    plants_steam_validate_ids,
)
from pudl.extract.ferc1 import TABLE_NAME_MAP_FERC1
from pudl.helpers import convert_cols_dtypes
from pudl.settings import Ferc1Settings
from pudl.transform.classes import (
    AbstractTableTransformer,
    InvalidRows,
    RenameColumns,
    TableTransformParams,
    TransformParams,
    cache_df,
    enforce_snake_case,
)

logger = pudl.logging_helpers.get_logger(__name__)


################################################################################
# FERC 1 Transform Parameter Models
################################################################################
@enum.unique
class SourceFerc1(enum.Enum):
    """Enumeration of allowed FERC 1 raw data sources."""

    XBRL = "xbrl"
    DBF = "dbf"


@enum.unique
class TableIdFerc1(enum.Enum):
    """Enumeration of the allowed FERC 1 table IDs.

    Hard coding this doesn't seem ideal. Somehow it should be either defined in the
    context of the Package, the Ferc1Settings, an etl_group, or DataSource. All of the
    table transformers associated with a given data source should have a table_id that's
    from that data source's subset of the database. Where should this really happen?
    Alternatively, the allowable values could be derived *from* the structure of the
    Package. But this works for now.
    """

    FUEL_FERC1 = "fuel_ferc1"
    PLANTS_STEAM_FERC1 = "plants_steam_ferc1"
    PLANTS_HYDRO_FERC1 = "plants_hydro_ferc1"
    PLANTS_SMALL_FERC1 = "plants_small_ferc1"
    PLANTS_PUMPED_STORAGE_FERC1 = "plants_pumped_storage_ferc1"
    PLANT_IN_SERVICE_FERC1 = "plant_in_service_ferc1"
    PURCHASED_POWER_FERC1 = "purchased_power_ferc1"
    TRANSMISSION_STATISTICS_FERC1 = "transmission_statistics_ferc1"
    ELECTRIC_ENERGY_SOURCES_FERC1 = "electric_energy_sources_ferc1"
    ELECTRIC_ENERGY_DISPOSITIONS_FERC1 = "electric_energy_dispositions_ferc1"
    UTILITY_PLANT_SUMMARY_FERC1 = "utility_plant_summary_ferc1"
    ELECTRIC_OPERATING_EXPENSES_FERC1 = "electric_operating_expenses_ferc1"
    BALANCE_SHEET_LIABILITIES = "balance_sheet_liabilities_ferc1"
    DEPRECIATION_AMORTIZATION_SUMMARY_FERC1 = "depreciation_amortization_summary_ferc1"
    BALANCE_SHEET_ASSETS_FERC1 = "balance_sheet_assets_ferc1"
    RETAINED_EARNINGS_FERC1 = "retained_earnings_ferc1"
    INCOME_STATEMENT_FERC1 = "income_statement_ferc1"
    ELECTRIC_PLANT_DEPRECIATION_CHANGES_FERC1 = (
        "electric_plant_depreciation_changes_ferc1"
    )
    ELECTRIC_OPERATING_REVENUES_FERC1 = "electric_operating_revenues_ferc1"
    ELECTRIC_PLANT_DEPRECIATION_FUNCTIONAL_FERC1 = (
        "electric_plant_depreciation_functional_ferc1"
    )
    CASH_FLOW_FERC1 = "cash_flow_ferc1"
    ELECTRICITY_SALES_BY_RATE_SCHEDULE_FERC1 = (
        "electricity_sales_by_rate_schedule_ferc1"
    )
    OTHER_REGULATORY_LIABILITIES_FERC1 = "other_regulatory_liabilities_ferc1"


################################################################################
# FERC 1 specific Column, MultiColumn, and Table Transform Functions
################################################################################


class RenameColumnsFerc1(TransformParams):
    """Dictionaries for renaming either XBRL or DBF derived FERC 1 columns.

    This is FERC 1 specific, because we need to store both DBF and XBRL rename
    dictionaires separately. Note that this parameter model does not have its own unique
    transform function. Like the generic :class:`pudl.transform.classes.RenameColumns`
    it depends on the build in :meth:`pd.rename` method, which is called with the values
    DBF or XBRL parameters depending on the context.

    Potential parameters validations that could be implemented

    * Validate that all keys appear in the original dbf/xbrl sources.
      This has to be true, but right now we don't have stored metadata enumerating all
      of the columns that exist in the raw data, so we don't have anything to check
      against. Implement once when we have schemas defined for after the extract step.

    * Validate all values appear in PUDL tables, and all expected PUDL names are mapped.
      Actually we can't require that the rename values appear in the PUDL tables,
      because there will be cases in which the original column gets dropped or modified,
      e.g. in the case of unit conversions with a column rename.
    """

    dbf: RenameColumns = RenameColumns()
    xbrl: RenameColumns = RenameColumns()
    duration_xbrl: RenameColumns = RenameColumns()
    instant_xbrl: RenameColumns = RenameColumns()


class WideToTidy(TransformParams):
    """Parameters for converting a wide table to a tidy table with value types."""

    idx_cols: list[str] | None
    """List of column names to treat as the table index."""

    stacked_column_name: str | None = None
    """Name of column that will contain the stacked categories."""

    value_types: list[str] | None
    """List of names of value types that will end up being the column names.

    Some of the FERC tables have multiple data types spread across many different
    categories.  In the input dataframe given to :func:`wide_to_tidy`, the value types
    must be the suffixes of the column names. If the table does not natively have the
    pattern of "{to-be stacked category}_{value_type}", rename the columns using a
    ``rename_columns.duration_xbrl``, ``rename_columns.instant_xbrl`` or
    ``rename_columns.dbf`` parameter which will be employed in
    :meth:`process_duration_xbrl`, :meth:`process_instant_xbrl` or :meth:`process_dbf`.
    """

    expected_drop_cols: int = 0
    """The number of columns that are expected to be dropped.

    :func:`wide_to_tidy_xbrl` will generate a regex pattern assuming the ``value_types``
    are the column name's suffixes. If a column does not conform to that pattern, it
    will be filtered out. This is helpful for us to not include a bunch of columns from
    the input dataframe incorrectly included in the stacking process. We could enumerate
    every column that we want to drop, but this could be tedious and potentially error
    prone. But this does mean that if a column is incorrectly named - or barely missing
    the pattern, it will be dropped. This parameter enables us to lock the number of
    expected columns. If the dropped columns are a different number, an error will be
    raised.
    """


class WideToTidySourceFerc1(TransformParams):
    """Parameters for converting either or both XBRL and DBF table from wide to tidy."""

    xbrl: WideToTidy | list[WideToTidy] = WideToTidy()
    dbf: WideToTidy | list[WideToTidy] = WideToTidy()


def wide_to_tidy(df: pd.DataFrame, params: WideToTidy) -> pd.DataFrame:
    """Reshape wide tables with FERC account columns to tidy format.

    The XBRL table coming into this method could contain all the data from both the
    instant and duration tables in a wide format -- with one column for every
    combination of value type (e.g. additions, ending_balance) and value category, which
    means ~500 columns for some tables.

    We tidy this into a long table with one column for each of the value types in
    ``params.value_types`` and a new column named ``xbrl_factoid`` that contains
    categories that were previously the XBRL column name stems.

    This allows aggregations of multiple ``xbrl_factoid`` categories in a columnar
    fashion such as aggregation across groups of rows to total up various hierarchical
    accounting categories (hydraulic turbines -> hydraulic production plant -> all
    production plant -> all electric utility plant) though the categorical columns
    required for that aggregation are added later.

    For table that have a internal relationship between the values in the
    ``params.value_types``, such as the :ref:`plant_in_service_ferc1` table, this also
    enables aggregation across columns to calculate the ending balance based on the
    starting balance and all of the reported changes.
    """
    suffixes = "|".join(params.value_types)
    pat = r"(^.*)_(" + suffixes + r"$)"
    # filter out any columns that don't match the pattern
    df_out = df.set_index(params.idx_cols).filter(regex=pat)
    # check if there are unexpected columns being dropped
    dropped_cols = [col for col in df if col not in df_out.reset_index().columns]
    logger.debug(f"dropping: {dropped_cols}")
    if params.expected_drop_cols != len(dropped_cols):
        raise AssertionError(
            f"Unexpected number of columns dropped: ({len(dropped_cols)}) instead of "
            f"({params.expected_drop_cols}). Columns dropped: {dropped_cols}"
        )

    new_cols = pd.MultiIndex.from_tuples(
        [(re.sub(pat, r"\1", col), re.sub(pat, r"\2", col)) for col in df_out.columns],
        names=[params.stacked_column_name, "value_type"],
    )
    df_out.columns = new_cols
    df_out = (
        df_out.stack(params.stacked_column_name, dropna=False)
        .loc[:, params.value_types]
        .reset_index()
    )
    # remove the name of the columns which was the name of the renaming layer of the
    # multi-index
    df_out.columns.name = None
    return df_out


class MergeXbrlMetadata(TransformParams):
    """Parameters for merging in XBRL metadata."""

    rename_columns: dict[str, str] = {}
    """Dictionary to rename columns in the normalized metadata before merging.

    This dictionary will be passed as :func:`pd.DataFrame.rename` ``columns`` parameter.
    """

    on: str | None = None
    """Column name to merge on in :func:`merge_xbrl_metadata`."""


def merge_xbrl_metadata(
    df: pd.DataFrame, xbrl_metadata: pd.DataFrame, params: MergeXbrlMetadata
) -> pd.DataFrame:
    """Merge metadata based on params."""
    return pd.merge(
        df,
        xbrl_metadata.rename(columns=params.rename_columns),
        on=params.on,
        how="left",
        validate="many_to_one",
    )


class DropDuplicateRowsDbf(TransformParams):
    """Parameter for dropping duplicate DBF rows."""

    table_name: TableIdFerc1 | None = None
    """Name of table used to grab primary keys of PUDL table to check for duplicates."""

    data_columns: list = []
    """List of data column names to ensure primary key duplicates have the same data."""


def drop_duplicate_rows_dbf(
    df: pd.DataFrame,
    params: DropDuplicateRowsDbf,
    return_dupes_w_unique_data: bool = False,
) -> pd.DataFrame:
    """Drop duplicate DBF rows if duplicates have indentical data or one row has nulls.

    There are several instances of the DBF data reporting the same value on multiple
    rows. This function checks to see if all of the duplicate values that have the same
    primary keys have reported the same data or have records with null data in any of
    the data columns while the other record has complete data. If the duplicates have no
    unique data, the duplicates are dropped with ``keep="first"``. If any duplicates do
    not contain the same data or half null data, an assertion will be raised.

    Args:
        df: DBF table containing PUDL primary key columns
        params: an instance of :class:`DropDuplicateRowsDbf`
        return_dupes_w_unique_data: Boolean flag used for debuging only which returns
            the duplicates which contain actually unique data instead of raising
            assertion. Default is False.
    """
    pks = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource(params.table_name.value)
        .schema.primary_key
    )
    # add a column that indicates whether or not any of the data columns contain null data
    df.loc[:, "null_data"] = df[params.data_columns].isnull().any(axis="columns")

    # checks to make sure the drop is targeted as expected
    # of the PK dupes, drop all instances when the data *is also the same*
    dupes_w_possible_unique_data = df.drop_duplicates(
        pks + params.data_columns, keep=False
    )
    dupes_w_possible_unique_data = dupes_w_possible_unique_data[
        dupes_w_possible_unique_data.duplicated(pks, keep=False)
    ]
    # if there are pk+data dupes, is there one record with some null data
    # an other with completely non-null data??
    # OR are there any records that have some null data and some actually unique
    # data
    nunique_data_columns = [f"{col}_nunique" for col in params.data_columns]
    dupes_w_possible_unique_data.loc[
        :, nunique_data_columns + ["null_data_nunique"]
    ] = (
        dupes_w_possible_unique_data.groupby(pks)[params.data_columns + ["null_data"]]
        .transform("nunique")
        .add_suffix("_nunique")
    )
    dupes_w_unique_data = dupes_w_possible_unique_data[
        (dupes_w_possible_unique_data.null_data_nunique != 2)
        | (
            dupes_w_possible_unique_data[
                dupes_w_possible_unique_data[nunique_data_columns] != 1
            ].any(axis="columns")
        )
    ].sort_values(by=pks)
    if not dupes_w_unique_data.empty:
        if return_dupes_w_unique_data:
            logger.warning("Returning duplicate records for debugging.")
            return dupes_w_unique_data
        else:
            raise AssertionError(
                "Duplicates have unique data and should not be dropped. Unique data: "
                f"{len(dupes_w_unique_data)}: \n{dupes_w_unique_data.sort_values(by=pks)}"
            )
    len_og = len(df)
    df = (
        df.sort_values(by=["null_data"], ascending=True)
        .drop_duplicates(pks, keep="first")
        .drop(columns=["null_data"])
    )
    logger.info(
        f"Dropped {len_og - len(df)} duplicate records: {(len_og - len(df))/len_og:.1%}"
        " of total rows."
    )
    return df


class AlignRowNumbersDbf(TransformParams):
    """Parameters for aligning DBF row numbers with metadata from mannual maps."""

    dbf_table_names: list[str] | None = None
    """DBF table to use to grab the row map in :func:`align_row_numbers_dbf`.

    Default is ``None``.
    """


def align_row_numbers_dbf(df: pd.DataFrame, params: AlignRowNumbersDbf) -> pd.DataFrame:
    """Rename the xbrl_factoid column after :meth:`align_row_numbers_dbf`."""
    if params.dbf_table_names:
        logger.info(
            f"Aligning row numbers from DBF row to XBRL map for {params.dbf_table_names}"
        )
        row_map = read_dbf_to_xbrl_map(dbf_table_names=params.dbf_table_names).pipe(
            fill_dbf_to_xbrl_map
        )
        if row_map.isnull().any(axis=None):
            raise ValueError(
                "Filled DBF-XBRL map contains NA values, which should never happen:"
                f"{row_map}"
            )

        df = pd.merge(df, row_map, on=["report_year", "row_number"], how="left")
        if df.xbrl_factoid.isna().any():
            raise ValueError(
                rf"Found null row labeles after aligning DBF/XBRL rows. n\ {df[df.xbrl_factoid.isna()]}"
            )
        # eliminate the header rows since they (should!) contain no data in either the
        # DBF or XBRL records:
        df = df[df.xbrl_factoid != "HEADER_ROW"]
    return df


class SelectDbfRowsByCategory(TransformParams):
    """Parameters for :func:`select_dbf_rows_by_category`."""

    column_name: str | None = None
    """The column name containing categories to select by."""
    select_by_xbrl_categories: bool = False
    """Boolean flag to indicate whether or not to use the categories in the XBRL table.

    If True, :func:`select_dbf_rows_by_category` will find the list of categories that
    exist in the passed in ``processed_xbrl`` to select by.
    """
    additional_categories: list[str] = []
    """List of additional categories to select by.

    If ``select_by_xbrl_categories`` is ``True``, these categories will be added to the
    XBRL categories and both will be used to select rows from the DBF data. If
    ``select_by_xbrl_categories`` is ``False``, only the "additional" categories will be
    the used to select rows from the DBF data.
    """
    len_expected_categories_to_drop: int = 0
    """Number of categories that are expected to be dropped from the DBF data.

    This is here to ensure no unexpected manipulations to the categories have occured. A
    warning will be flagged if this number is different than the number of categories
    that are being dropped.
    """


def select_dbf_rows_by_category(
    processed_dbf: pd.DataFrame,
    processed_xbrl: pd.DataFrame,
    params: SelectDbfRowsByCategory,
) -> pd.DataFrame:
    """Select DBF rows with values listed or found in XBRL in a categorical-like column.

    The XBRL data often breaks out sub-sections of DBF tables into their own table.
    These breakout tables are often messy, unstructured portions of a particular
    schedule or page on the FERC1 PDF. We often want to preserve some of the ways the
    XBRL data is segmented so we need to be able to select only portions of the DBF
    table to be concatenated with the XBRL data.

    In mapping DBF data to XBRL data for the tables that rely on their ``row_number``
    we map each row to its corresponding ``xbrl_factoid``. The standard use of this
    transformer is to use the ``column_name`` that corresponds to the ``xbrl_factoid``
    that was merged into the DBF data via :func:`align_row_numbers_dbf` and was
    converted into a column in the XBRL data via :func:`wide_to_tidy`.

    Note: Often, the unstructured portion of the DBF table that (possibly) sums up into
    a single value in structured data has the same ``xbrl_factoid`` name in the XBRL
    tables. By convention, we are employing a pattern in the ``dbf_to_xbrl.csv`` map
    that involves adding an ``_unstructed`` suffix to the rows that correspond to the
    unstructured portion of the table. This enables a simple selection of the structured
    part of the table. When processing the unstructured table, you can either rename the
    XBRL data's factoid name to include an ``_unstructed`` suffix or you can specify
    the categories with ``_unstructed`` suffixes using the ``additional_categories``
    parameter.
    """
    # compile the list of categories from the possible options.
    categories_to_select = []
    if params.select_by_xbrl_categories:
        categories_to_select = categories_to_select + list(
            processed_xbrl[params.column_name].unique()
        )
    if params.additional_categories:
        categories_to_select = categories_to_select + params.additional_categories

    # check if we are getting the same number of expected categories to drop
    categories_to_drop = [
        cat
        for cat in processed_dbf[params.column_name].unique()
        if cat not in categories_to_select
    ]
    if len(categories_to_drop) != params.len_expected_categories_to_drop:
        logger.warning(
            f"Dropping {len(categories_to_drop)} DBF categories that contain the "
            f"following values in {params.column_name} but expected "
            f"{params.len_expected_categories_to_drop}:"
            f"{categories_to_drop}"
        )
    # now select only the rows which contain the categories we want to include in the
    # column that we care about. Copy bc this is a slice of the og dataframe.
    category_mask = processed_dbf[params.column_name].isin(categories_to_select)
    return processed_dbf.loc[category_mask].copy()


class UnstackBalancesToReportYearInstantXbrl(TransformParams):
    """Parameters for :func:`unstack_balances_to_report_year_instant_xbrl`."""

    unstack_balances_to_report_year: bool = False
    """If True unstack balances to a single year (the report year)."""


def unstack_balances_to_report_year_instant_xbrl(
    df: pd.DataFrame,
    params: UnstackBalancesToReportYearInstantXbrl,
    primary_key_cols: list[str],
) -> pd.DataFrame:
    """Turn start year end year rows into columns for each value type.

    Called in :meth:`Ferc1AbstractTableTransformer.process_instant_xbrl`.

    Some instant tables report year-end data, with their datestamps in different years,
    but we want year-start and year-end data within a single report_year (which is
    equivalent) stored in two separate columns for compatibility with the DBF data.

    This function unstacks that table and adds the suffixes ``_starting_balance`` and
    ``_ending_balance`` to each of the columns. These can then be used as
    ``value_types`` in :func:`wide_to_tidy` to normalize the table.

    There are two checks in place:

    First, it will make sure that there are not duplicate entries for a single year +
    other primary key fields. Ex: a row for 2020-12-31 and 2020-06-30 for entitiy_id X
    means that the data isn't annually unique. We could just drop these mid-year
    values, but we might want to keep them or at least check that there is no funny
    business with the data.

    We also check that there are no mid-year dates at all. If an entity reports a value
    from the middle of the year, we can't identify it as a start/end of year value.

    Params:
        primary_key_cols: The columns that should be used to check for duplicated data,
            and also for unstacking the balance -- these are set to be the index before
            unstack is called. These are typically set by the wrapping method and
            generated automatically based on other class transformation parameters via
            :meth:`Ferc1AbstractTableTransformer.source_table_primary_key`.
    """
    if params.unstack_balances_to_report_year:
        df["year"] = pd.to_datetime(df["date"]).dt.year
        # Check that the originally reported records are annually unique.
        # year and report_year aren't necessarily the same since previous year data
        # is often reported in the current report year, but we're constructing a table
        # where report_year is part of the primary key, so we have to do this:
        unique_cols = [c for c in primary_key_cols if c != "report_year"] + ["year"]
        if df.duplicated(unique_cols).any():
            raise AssertionError(
                "Looks like there are multiple entries per year--not sure which to use "
                f"for the start/end balance. {params=} {primary_key_cols=}"
            )
        if not pd.to_datetime(df["date"]).dt.is_year_end.all():
            raise AssertionError(
                "Looks like there are some values in here that aren't from the end of "
                "the year. We can't use those to calculate start and end balances."
            )
        df.loc[df.report_year == (df.year + 1), "balance_type"] = "starting_balance"
        df.loc[df.report_year == df.year, "balance_type"] = "ending_balance"
        if df.balance_type.isna().any():
            # Remove rows from years that are not representative of start/end dates
            # for a given report year (i.e., the report year and one year prior).
            logger.warning(
                f"Dropping unexpected years: "
                f"{df.loc[df.balance_type.isna(), 'year'].unique()}"
            )
            df = df[df["balance_type"].notna()].copy()
        df = (
            df.drop(["year", "date"], axis="columns")
            .set_index(primary_key_cols + ["balance_type", "sched_table_name"])
            .unstack("balance_type")
        )
        # This turns a multi-index into a single-level index with tuples of strings
        # as the keys, and then converts the tuples of strings into a single string
        # by joining their values with an underscore. This results in column labels
        # like boiler_plant_equipment_steam_production_starting_balance
        df.columns = ["_".join(items) for items in df.columns.to_flat_index()]
        df = df.reset_index()
        return df


class CombineAxisColumnsXbrl(TransformParams):
    """Parameters for :func:`combine_axis_columns_xbrl`."""

    axis_columns_to_combine: list | None = None
    """List of axis columns to combine."""

    new_axis_column_name: str | None = None
    """The name of the combined axis column -- must end with the suffix ``_axis``!."""

    @validator("new_axis_column_name")
    def doesnt_end_with_axis(cls, v):
        """Ensure that new axis column ends in _axis."""
        if v is not None:
            if not v.endswith("_axis"):
                raise ValueError(
                    "The new axis column name must end with the suffix '_axis'!"
                )
        return v


def combine_axis_columns_xbrl(
    df: pd.DataFrame, params: CombineAxisColumnsXbrl
) -> pd.DataFrame:
    """Combine axis columns from squished XBRL tables into one column with no NAs.

    Called in :meth:`Ferc1AbstractTableTransformer.process_xbrl`.

    There are instances (ex: sales_by_rate_schedule_ferc1) where the DBF table is equal
    to several concatenated XBRL tables. These XBRL tables are extracted together with
    the function :func:`extract_xbrl_concat`. Once combined, we need to deal with their
    axis columns.

    We use the axis columns (the primary key for the raw XBRL tables) in the creation
    of ``record_id``s for each of the rows. If each of the concatinated XBRL tables has
    the same axis column name then there's no need to fret. However, if the columns have
    slightly different names (ex: ``residential_sales_axis`` vs.
    ``industrial_sales_axis``), we'll need to combine them. We combine them to get rid
    of NA values which aren't allowed in primary keys. Otherwise it would look like
    this:

        +-------------------------+-------------------------+
        | residential_sales_axis  | industrial_sales_axis   |
        +=========================+=========================+
        | value1                  | NA                      |
        +-------------------------+-------------------------+
        | value2                  | NA                      |
        +-------------------------+-------------------------+
        | NA                      | valueA                  |
        +-------------------------+-------------------------+
        | NA                      | valueB                  |
        +-------------------------+-------------------------+

    vs. this:

        +-------------------------+
        | sales_axis              |
        +=========================+
        | value1                  |
        +-------------------------+
        | value2                  |
        +-------------------------+
        | valueA                  |
        +-------------------------+
        | valueB                  |
        +-------------------------+
    """
    # First, make sure that the new_axis_column_name param as the word axis in it
    if not params.new_axis_column_name.endswith("_axis"):
        raise ValueError(
            "Your new_axis_column_name must end with the suffix _axis so that it gets "
            "included in the record_id."
        )
    # Now, make sure there are no overlapping axis columns
    if (df[params.axis_columns_to_combine].count(axis=1) > 1).any():
        raise AssertionError(
            "You're trying to combine axis columns, but there's more than one axis "
            "column value per row."
        )
    # Now combine all of the columns into one and remove the old axis columns
    df[params.new_axis_column_name] = pd.NA
    for col in params.axis_columns_to_combine:
        df[params.new_axis_column_name] = df[params.new_axis_column_name].fillna(
            df[col]
        )
    df = df.drop(columns=params.axis_columns_to_combine)
    return df


class Ferc1TableTransformParams(TableTransformParams):
    """A model defining what TransformParams are allowed for FERC Form 1.

    This adds additional parameter models beyond the ones inherited from the
    :class:`pudl.transform.classes.AbstractTableTransformer` class.
    """

    rename_columns_ferc1: RenameColumnsFerc1 = RenameColumnsFerc1(
        dbf=RenameColumns(),
        xbrl=RenameColumns(),
        instant_xbrl=RenameColumns(),
        duration_xbrl=RenameColumns(),
    )
    wide_to_tidy: WideToTidySourceFerc1 = WideToTidySourceFerc1(
        dbf=WideToTidy(), xbrl=WideToTidy()
    )
    merge_xbrl_metadata: MergeXbrlMetadata = MergeXbrlMetadata()
    align_row_numbers_dbf: AlignRowNumbersDbf = AlignRowNumbersDbf()
    drop_duplicate_rows_dbf: DropDuplicateRowsDbf = DropDuplicateRowsDbf()
    select_dbf_rows_by_category: SelectDbfRowsByCategory = SelectDbfRowsByCategory()
    unstack_balances_to_report_year_instant_xbrl: UnstackBalancesToReportYearInstantXbrl = (
        UnstackBalancesToReportYearInstantXbrl()
    )
    combine_axis_columns_xbrl: CombineAxisColumnsXbrl = CombineAxisColumnsXbrl()


################################################################################
# FERC 1 transform helper functions. Probably to be integrated into a class
# below as methods or moved to a different module once it's clear where they belong.
################################################################################
def get_ferc1_dbf_rows_to_map(ferc1_engine: sa.engine.Engine) -> pd.DataFrame:
    """Identify DBF rows that need to be mapped to XBRL columns.

    Select all records in the ``f1_row_lit_tbl`` where the row literal associated with a
    given combination of table and row number is different from the preceeding year.
    This is the smallest set of records which we can use to reproduce the whole table by
    expanding the time series to include all years, and forward filling the row
    literals.
    """
    idx_cols = ["sched_table_name", "row_number", "report_year"]
    data_cols = ["row_literal"]
    row_lit = pd.read_sql(
        "f1_row_lit_tbl", con=ferc1_engine, columns=idx_cols + data_cols
    ).sort_values(idx_cols)
    row_lit["shifted"] = row_lit.groupby(
        ["sched_table_name", "row_number"]
    ).row_literal.shift()
    row_lit["changed"] = row_lit.row_literal != row_lit.shifted
    return row_lit.loc[row_lit.changed, idx_cols + data_cols]


def update_dbf_to_xbrl_map(ferc1_engine: sa.engine.Engine) -> pd.DataFrame:
    """Regenerate the FERC 1 DBF+XBRL glue while retaining existing mappings.

    Reads all rows that need to be mapped out of the ``f1_row_lit_tbl`` and appends
    columns containing any previously mapped values, returning the resulting dataframe.
    """
    idx_cols = ["sched_table_name", "row_number", "report_year"]
    all_rows = get_ferc1_dbf_rows_to_map(ferc1_engine).set_index(idx_cols)
    with importlib.resources.open_text(
        "pudl.package_data.ferc1", "dbf_to_xbrl.csv"
    ) as file:
        mapped_rows = (
            pd.read_csv(file).set_index(idx_cols).drop(["row_literal"], axis="columns")
        )
    return (
        pd.concat([all_rows, mapped_rows], axis="columns")
        .reset_index()
        .sort_values(["sched_table_name", "report_year", "row_number"])
    )


def read_dbf_to_xbrl_map(dbf_table_names: list[str]) -> pd.DataFrame:
    """Read the manually compiled DBF row to XBRL column mapping for a given table.

    Args:
        dbf_table_name: The original name of the table in the FERC Form 1 DBF database
            whose mapping to the XBRL data you want to extract. for example
            ``f1_plant_in_srvce``.

    Returns:
        DataFrame with columns ``[sched_table_name, report_year, row_number, row_type, xbrl_factoid]``
    """
    with importlib.resources.open_text(
        "pudl.package_data.ferc1", "dbf_to_xbrl.csv"
    ) as file:
        row_map = pd.read_csv(
            file,
            usecols=[
                "sched_table_name",
                "report_year",
                "row_number",
                "row_type",
                "xbrl_factoid",
            ],
        )
    # Select only the rows that pertain to dbf_table_name
    row_map = row_map.loc[row_map.sched_table_name.isin(dbf_table_names)]
    return row_map


def fill_dbf_to_xbrl_map(
    df: pd.DataFrame, dbf_years: list[int] | None = None
) -> pd.DataFrame:
    """Forward-fill missing years in the minimal, manually compiled DBF to XBRL mapping.

    The relationship between a DBF row and XBRL column/fact/entity/whatever is mostly
    consistent from year to year. To minimize the amount of manual mapping work we have
    to do, we only map the years in which the relationship changes. In the end we do
    need a complete correspondence for all years though, and this function uses the
    minimal information we've compiled to fill in all the gaps, producing a complete
    mapping across all requested years.

    One complication is that we need to explicitly indicate which DBF rows have headers
    in them (which don't exist in XBRL), to differentiate them from null values in the
    exhaustive index we create below. We set a ``HEADER_ROW`` sentinel value so we can
    distinguish between two different reasons that we might find NULL values in the
    ``xbrl_factoid`` field:

    1. It's NULL because it's between two valid mapped values (the NULL was created
       in our filling of the time series) and should thus be filled in, or

    2. It's NULL because it was a header row in the DBF data, which means it should
       NOT be filled in. Without the ``HEADER_ROW`` value, when a row number from year X
       becomes associated with a non-header row in year X+1 the ffill will keep right on
       filling, associating all of the new header rows with the value of
       ``xbrl_factoid`` that was associated with the old row number.

    Args:
        df: A dataframe containing a DBF row to XBRL mapping for a single FERC 1 DBF
            table.
        dbf_years: The list of years that should have their DBF row to XBRL mapping
            filled in. This defaults to all available years of DBF data for FERC 1. In
            general this parameter should only be set to a non-default value for testing
            purposes.

    Returns:
        A complete mapping of DBF row number to XBRL columns for all years of data
        within a single FERC 1 DBF table. Has columns of
        ``[report_year, row_number, xbrl_factoid]``
    """
    if not dbf_years:
        dbf_years = Ferc1Settings().dbf_years
    # If the first year that we're trying to produce isn't mapped, we won't be able to
    # forward fill.
    if min(dbf_years) not in df.report_year.unique():
        raise ValueError(
            "Invalid combination of years and DBF-XBRL mapping. The first year cannot\n"
            "be filled and **must** be mapped.\n"
            f"First year: {min(dbf_years)}, "
            f"Mapped years: {sorted(df.report_year.unique())}\n"
            f"{df}"
        )

    if df.loc[(df.row_type == "header"), "xbrl_factoid"].notna().any():
        raise ValueError("Found non-null XBRL column value mapped to a DBF header row.")
    df.loc[df.row_type == "header", "xbrl_factoid"] = "HEADER_ROW"

    if df["xbrl_factoid"].isna().any():
        raise ValueError(
            "Found NA XBRL values in the DBF-XBRL mapping, which shouldn't happen. \n"
            f"{df[df['xbrl_factoid'].isna()]}"
        )
    df = df.drop(["row_type"], axis="columns")

    # Create an index containing all combinations of report_year and row_number
    idx_cols = ["report_year", "row_number", "sched_table_name"]
    idx = pd.MultiIndex.from_product(
        [dbf_years, df.row_number.unique(), df.sched_table_name.unique()],
        names=idx_cols,
    )

    # Concatenate the row map with the empty index, so we have blank spaces to fill:
    df = pd.concat(
        [
            pd.DataFrame(index=idx),
            df.set_index(idx_cols),
        ],
        axis="columns",
    ).reset_index()

    # Forward fill missing XBRL column names, until a new definition for the row
    # number is encountered:
    df["xbrl_factoid"] = df.groupby(
        ["row_number", "sched_table_name"]
    ).xbrl_factoid.transform("ffill")
    # Drop NA values produced in the broadcasting merge onto the exhaustive index.
    df = df.dropna(subset="xbrl_factoid").drop(columns=["sched_table_name"])
    # There should be no NA values left at this point:
    if df.isnull().any(axis=None):
        raise ValueError(
            "Filled DBF-XBRL map contains NA values, which should never happen:"
            f"\n{df[df.isnull().any(axis='columns')]}"
        )
    return df


def get_data_cols_raw_xbrl(
    raw_xbrl_instant: pd.DataFrame,
    raw_xbrl_duration: pd.DataFrame,
) -> list[str]:
    """Get a list of all XBRL data columns appearing in a given XBRL table.

    Returns:
        A list of all the data columns found in the original XBRL DB that correspond to
        the given PUDL table. Includes columns from both the instant and duration tables
        but excludes structural columns that appear in all XBRL tables.
    """
    excluded_cols = [
        "date",
        "end_date",
        "entity_id",
        "filing_name",
        "index",
        "report_year",
        "start_date",
    ]
    return sorted(
        set(raw_xbrl_instant.columns)
        .union(raw_xbrl_duration.columns)
        .difference(excluded_cols)
    )


################################################################################
# FERC 1 specific TableTransformer classes
################################################################################
class Ferc1AbstractTableTransformer(AbstractTableTransformer):
    """An abstract class defining methods common to many FERC Form 1 tables.

    This subclass remains abstract because it does not define transform_main(), which
    is always going to be table-specific.

    * Methods that only apply to XBRL data should end with _xbrl
    * Methods that only apply to DBF data should end with _dbf
    """

    table_id: TableIdFerc1
    parameter_model = Ferc1TableTransformParams
    params: parameter_model

    has_unique_record_ids: bool = True
    """True if each record in the transformed table corresponds to one input record.

    For tables that have been transformed from wide-to-tidy format, or undergone other
    kinds of reshaping, there is not a simple one-to-one relationship between input and
    output records, and so we should not expect record IDs to be unique. In those cases
    they serve only a forensic purpose, telling us where to find the original source of
    the transformed data.
    """

    xbrl_metadata: pd.DataFrame = pd.DataFrame()
    """Dataframe combining XBRL metadata for both instant and duration table columns."""

    def __init__(
        self,
        params: TableTransformParams | None = None,
        cache_dfs: bool = False,
        clear_cached_dfs: bool = True,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]]
        | None = None,
    ) -> None:
        """Augment inherited initializer to store XBRL metadata in the class."""
        super().__init__(
            params=params,
            cache_dfs=cache_dfs,
            clear_cached_dfs=clear_cached_dfs,
        )
        if xbrl_metadata_json:
            self.xbrl_metadata = self.process_xbrl_metadata(xbrl_metadata_json)

    @cache_df(key="start")
    def transform_start(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Process the raw data until the XBRL and DBF inputs have been unified."""
        processed_dbf = self.process_dbf(raw_dbf)
        processed_xbrl = self.process_xbrl(raw_xbrl_instant, raw_xbrl_duration)
        processed_dbf = self.select_dbf_rows_by_category(processed_dbf, processed_xbrl)
        logger.info(f"{self.table_id.value}: Concatenating DBF + XBRL dataframes.")
        return pd.concat([processed_dbf, processed_xbrl]).reset_index(drop=True)

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generic FERC1 main table transformer.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.
        """
        df = (
            self.spot_fix_values(df)
            .pipe(self.normalize_strings)
            .pipe(self.categorize_strings)
            .pipe(self.convert_units)
            .pipe(self.strip_non_numeric_values)
            .pipe(self.nullify_outliers)
            .pipe(self.replace_with_na)
            .pipe(self.drop_invalid_rows)
            .pipe(
                pudl.metadata.classes.Package.from_resource_ids()
                .get_resource(self.table_id.value)
                .encode
            )
            .pipe(self.merge_xbrl_metadata)
        )
        return df

    @cache_df(key="end")
    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardized final cleanup after the transformations are done.

        Enforces dataframe schema. Checks for empty dataframes and null columns.
        """
        df = self.enforce_schema(df)
        if df.empty:
            raise ValueError(f"{self.table_id.value}: Final dataframe is empty!!!")
        for col in df:
            if df[col].isna().all():
                raise ValueError(
                    f"{self.table_id.value}: Column {col} is entirely NULL!"
                )
        return df

    def select_dbf_rows_by_category(
        self,
        processed_dbf: pd.DataFrame,
        processed_xbrl: pd.DataFrame,
        params: SelectDbfRowsByCategory | None = None,
    ) -> pd.DataFrame:
        """Wrapper method for :func:`select_dbf_rows_by_category`."""
        if not params:
            params = self.params.select_dbf_rows_by_category
        if params.column_name:
            logger.info(
                f"{self.table_id.value}: Selecting DBF rows with desired values in {params.column_name}."
            )
            processed_dbf = select_dbf_rows_by_category(
                processed_dbf=processed_dbf,
                processed_xbrl=processed_xbrl,
                params=params,
            )
        return processed_dbf

    @cache_df(key="process_xbrl_metadata")
    def process_xbrl_metadata(self, xbrl_metadata_json) -> pd.DataFrame:
        """Normalize the XBRL JSON metadata, turning it into a dataframe.

        This process concatenates and deduplicates the metadata which is associated with
        the instant and duration tables, since the metadata is only combined with the
        data after the instant and duration (and DBF) tables have been merged. This
        happens in :meth:`Ferc1AbstractTableTransformer.merge_xbrl_metadata`.
        """
        logger.info(f"{self.table_id.value}: Processing XBRL metadata.")
        return (
            pd.concat(
                [
                    pd.json_normalize(xbrl_metadata_json["instant"]),
                    pd.json_normalize(xbrl_metadata_json["duration"]),
                ]
            )
            .drop("references.form_location", axis="columns")
            .drop_duplicates(subset="name")
            .rename(
                columns={
                    "name": "xbrl_factoid",
                    "references.account": "ferc_account",
                }
            )
            .assign(
                # Flag metadata record types
                row_type_xbrl=lambda x: np.where(
                    x.calculations.astype(bool), "calculated_value", "reported_value"
                ),
            )
            .astype(
                {
                    "xbrl_factoid": pd.StringDtype(),
                    "balance": pd.StringDtype(),
                    "ferc_account": pd.StringDtype(),
                    "calculations": pd.StringDtype(),
                    "row_type_xbrl": pd.StringDtype(),
                }
            )
        )

    @cache_df(key="merge_xbrl_metadata")
    def merge_xbrl_metadata(
        self, df: pd.DataFrame, params: MergeXbrlMetadata | None = None
    ) -> pd.DataFrame:
        """Combine XBRL-derived metadata with the data it pertains to.

        While the metadata we're using to annotate the data comes from the more recent
        XBRL data, it applies generally to all the historical DBF data as well! This
        method reads the normalized metadata out of an attribute.
        """
        if not params:
            params = self.params.merge_xbrl_metadata
        if params.on:
            logger.info(f"{self.table_id.value}: Merging metadata")
            df = merge_xbrl_metadata(df, self.xbrl_metadata, params)
        return df

    @cache_df(key="dbf")
    def align_row_numbers_dbf(
        self, df: pd.DataFrame, params: AlignRowNumbersDbf | None = None
    ) -> pd.DataFrame:
        """Align historical FERC1 DBF row numbers with XBRL account IDs.

        Additional Parameterization TBD with additional experience. See:
        https://github.com/catalyst-cooperative/pudl/issues/2012
        """
        if params is None:
            params = self.params.align_row_numbers_dbf
        if params.dbf_table_names:
            df = align_row_numbers_dbf(df, params=params)
        return df

    @cache_df(key="dbf")
    def drop_duplicate_rows_dbf(
        self, df: pd.DataFrame, params: DropDuplicateRowsDbf | None = None
    ) -> pd.DataFrame:
        """Drop the DBF rows where the PKs and data columns are duplicated.

        Wrapper function for :func:`drop_duplicate_rows_dbf`.
        """
        if params is None:
            params = self.params.drop_duplicate_rows_dbf
        if params.table_name:
            logger.info(
                f"{self.table_id.value}: Dropping rows where primary key and data "
                "columns are duplicated."
            )
            df = drop_duplicate_rows_dbf(df, params=params)
        return df

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """DBF-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing DBF data pre-concatenation.")
        return (
            raw_dbf.drop_duplicates()
            .pipe(self.select_annual_rows_dbf)
            .pipe(self.drop_footnote_columns_dbf)
            .pipe(self.align_row_numbers_dbf)
            .pipe(self.rename_columns, rename_stage="dbf")
            .pipe(self.assign_record_id, source_ferc1=SourceFerc1.DBF)
            .pipe(self.drop_unused_original_columns_dbf)
            .pipe(self.assign_utility_id_ferc1, source_ferc1=SourceFerc1.DBF)
            .pipe(self.wide_to_tidy, source_ferc1=SourceFerc1.DBF)
            .pipe(self.drop_duplicate_rows_dbf)
        )

    @cache_df(key="xbrl")
    def process_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """XBRL-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing XBRL data pre-concatenation.")
        return (
            self.merge_instant_and_duration_tables_xbrl(
                raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.wide_to_tidy, source_ferc1=SourceFerc1.XBRL)
            .pipe(self.rename_columns, rename_stage="xbrl")
            .pipe(self.combine_axis_columns_xbrl)
            .pipe(self.assign_record_id, source_ferc1=SourceFerc1.XBRL)
            .pipe(self.assign_utility_id_ferc1, source_ferc1=SourceFerc1.XBRL)
        )

    def rename_columns(
        self,
        df: pd.DataFrame,
        rename_stage: Literal["dbf", "xbrl", "xbrl_instant", "xbrl_duration"]
        | None = None,
        params: RenameColumns | None = None,
    ):
        """Grab the params based on the rename stage and run default rename_columns.

        Args:
            df: Table to be renamed.
            rename_stage: Name of stage in the transform process. Used to get specific
                stage's parameters if None have been passed.
            params: Rename column parameters.
        """
        if not params:
            params = self.params.rename_columns_ferc1.__getattribute__(rename_stage)
        df = super().rename_columns(df, params=params)
        return df

    @cache_df(key="dbf")
    def select_annual_rows_dbf(self, df):
        """Select only annually reported DBF Rows.

        There are some DBF tables that include a mix of reporting frequencies. For now,
        the default for PUDL tables is to have only the annual records.
        """
        if "report_prd" in df and list(df.report_prd.unique()) != [12]:
            len_og = len(df)
            df = df[df.report_prd == 12].copy()
            logger.info(
                f"{self.table_id.value}: After selection only annual records,"
                f" we have {len(df)/len_og:.1%} of the original table."
            )
        return df

    def unstack_balances_to_report_year_instant_xbrl(
        self,
        df: pd.DataFrame,
        params: UnstackBalancesToReportYearInstantXbrl | None = None,
    ) -> pd.DataFrame:
        """Turn start year end year rows into columns for each value type."""
        logger.info(f"{self.table_id.value}: Unstacking balances to the report years.")
        if params is None:
            params = self.params.unstack_balances_to_report_year_instant_xbrl
        if params.unstack_balances_to_report_year:
            df = unstack_balances_to_report_year_instant_xbrl(
                df,
                params=params,
                primary_key_cols=self.source_table_primary_key(
                    source_ferc1=SourceFerc1.XBRL
                ),
            )
        return df

    def wide_to_tidy(
        self,
        df: pd.DataFrame,
        source_ferc1: SourceFerc1,
        params: WideToTidy | None = None,
    ) -> pd.DataFrame:
        """Reshape wide tables with FERC account columns to tidy format.

        The XBRL table coming into this method contains all the data from both the
        instant and duration tables in a wide format -- with one column for every
        combination of value type (e.g. additions, ending_balance) and accounting
        category, which means ~500 columns.

        We tidy this into a long table with one column for each of the value types (6 in
        all), and a new column that contains the accounting categories. This allows
        aggregation across columns to calculate the ending balance based on the starting
        balance and all of the reported changes, and aggregation across groups of rows
        to total up various hierarchical accounting categories (hydraulic turbines ->
        hydraulic production plant -> all  production plant -> all electric utility
        plant) though the categorical columns required for that aggregation are added
        later.
        """
        if not params:
            params = self.params.wide_to_tidy.__getattribute__(source_ferc1.value)

        if isinstance(params, WideToTidy):
            multiple_params = [params]
        else:
            multiple_params = params
        for single_params in multiple_params:
            if single_params.idx_cols or single_params.value_types:
                logger.info(
                    f"{self.table_id.value}: applying wide_to_tidy for {source_ferc1.value}"
                )
                df = wide_to_tidy(df, single_params)
        return df

    def combine_axis_columns_xbrl(
        self,
        df: pd.DataFrame,
        params: CombineAxisColumnsXbrl | None = None,
    ) -> pd.DataFrame:
        """Combine axis columns from squished XBRL tables into one column with no NA."""
        if params is None:
            params = self.params.combine_axis_columns_xbrl
        if params.axis_columns_to_combine:
            logger.info(
                f"{self.table_id.value}: Combining axis columns: "
                f"{params.axis_columns_to_combine} into {params.new_axis_column_name}"
            )
            df = combine_axis_columns_xbrl(df, params=params)
        return df

    @cache_df(key="xbrl")
    def merge_instant_and_duration_tables_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge XBRL instant and duration tables, reshaping instant as needed.

        FERC1 XBRL instant period signifies that it is true as of the reported date,
        while a duration fact pertains to the specified time period. The ``date`` column
        for an instant fact corresponds to the ``end_date`` column of a duration fact.

        When merging the instant and duration tables, we need to preserve row order.
        For the small generators table, row order is how we label and extract
        information from header and note rows. Outer merging messes up the order, so we
        need to use a one-sided merge. So far, it seems like the duration df contains
        all the index values in the instant df. To be sure, there's a check that makes
        sure there are no unique intant df index values. If that passes, we merge the
        instant table into the duration table, and the row order is preserved.

        Note: This should always be applied before :meth:``rename_columns``

        Args:
            raw_xbrl_instant: table representing XBRL instant facts.
            raw_xbrl_duration: table representing XBRL duration facts.

        Returns:
            A unified table combining the XBRL duration and instant facts, if both types
            of facts were present. If either input dataframe is empty, the other
            dataframe is returned unchanged, except that several unused columns are
            dropped. If both input dataframes are empty, an empty dataframe is returned.
        """
        drop_cols = ["filing_name", "index"]
        # Ignore errors in case not all drop_cols are present.
        instant = raw_xbrl_instant.drop(columns=drop_cols, errors="ignore")
        duration = raw_xbrl_duration.drop(columns=drop_cols, errors="ignore")

        instant_axes = [
            col for col in raw_xbrl_instant.columns if col.endswith("_axis")
        ]
        duration_axes = [
            col for col in raw_xbrl_duration.columns if col.endswith("_axis")
        ]
        if (
            bool(instant_axes)
            & bool(duration_axes)
            & (set(instant_axes) != set(duration_axes))
        ):
            raise ValueError(
                f"{self.table_id.value}: Instant and Duration XBRL Axes do not match.\n"
                f"    instant: {instant_axes}\n"
                f"    duration: {duration_axes}"
            )

        # Do any table-specific preprocessing of the instant and duration tables
        instant = self.process_instant_xbrl(instant)
        duration = self.process_duration_xbrl(duration)

        if instant.empty:
            logger.info(f"{self.table_id.value}: No XBRL instant table found.")
            out_df = duration
        elif duration.empty:
            logger.info(f"{self.table_id.value}: No XBRL duration table found.")
            out_df = instant
        else:
            logger.info(
                f"{self.table_id.value}: Both XBRL instant & duration tables found."
            )
            instant_merge_keys = [
                "entity_id",
                "report_year",
                "sched_table_name",
            ] + instant_axes
            duration_merge_keys = [
                "entity_id",
                "report_year",
                "sched_table_name",
            ] + duration_axes
            # See if there are any values in the instant table that don't show up in the
            # duration table.
            unique_instant_rows = instant.set_index(
                instant_merge_keys
            ).index.difference(duration.set_index(duration_merge_keys).index)
            if unique_instant_rows.empty:
                logger.info(
                    f"{self.table_id.value}: Combining XBRL instant & duration tables "
                    "using RIGHT-MERGE."
                )
                # Merge instant into duration.
                out_df = pd.merge(
                    instant,
                    duration,
                    how="right",
                    left_on=instant_merge_keys,
                    right_on=duration_merge_keys,
                    validate="1:1",
                )
            else:
                # TODO: Check whether our assumptions about these tables hold before
                # concatenating them. May need to be table specific. E.g.
                # * What fraction of their index values overlap? (it should be high!)
                # * Do the instant/duration columns conform to expected naming conventions?
                logger.info(
                    f"{self.table_id.value}: Combining XBRL instant & duration tables "
                    "using CONCATENATION."
                )
                out_df = pd.concat(
                    [
                        instant.set_index(instant_merge_keys),
                        duration.set_index(duration_merge_keys),
                    ],
                    axis="columns",
                ).reset_index()
        return out_df

    @cache_df("process_instant_xbrl")
    def process_instant_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make instant and duration tables compatible.

        Column renaming is sometimes required because a few columns in the instant and
        duration tables do not have corresponding names that follow the naming
        conventions of ~95% of all the columns, which we rely on programmatically when
        reshaping and concatenating these tables together.
        """
        df = self.rename_columns(df, rename_stage="instant_xbrl").pipe(
            self.unstack_balances_to_report_year_instant_xbrl
        )
        return df

    @cache_df("process_duration_xbrl")
    def process_duration_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make instant and duration tables compatible.

        Column renaming is sometimes required because a few columns in the instant and
        duration tables do not have corresponding names that follow the naming
        conventions of ~95% of all the columns, which we rely on programmatically when
        reshaping and concatenating these tables together.
        """
        if not df.empty:
            df = self.rename_columns(df, rename_stage="duration_xbrl").pipe(
                self.select_current_year_annual_records_duration_xbrl
            )
        return df

    def select_current_year_annual_records_duration_xbrl(self, df):
        """Select for annual records within their report_year.

        Select only records that have a start_date at begining of the report_year and
        have an end_date at the end of the report_year.
        """
        len_og = len(df)
        df = df.astype({"start_date": "datetime64[s]", "end_date": "datetime64[s]"})
        df = df[
            (df.start_date.dt.year == df.report_year)
            & (df.start_date.dt.month == 1)
            & (df.start_date.dt.day == 1)
            & (df.end_date.dt.year == df.report_year)
            & (df.end_date.dt.month == 12)
            & (df.end_date.dt.day == 31)
        ]
        len_out = len(df)
        logger.info(
            f"{self.table_id.value}: After selection of dates based on the report year,"
            f" we have {len_out/len_og:.1%} of the original table."
        )
        return df

    @cache_df(key="dbf")
    def drop_footnote_columns_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop DBF footnote reference columns, which all end with _f."""
        logger.debug(f"{self.table_id.value}: Dropping DBF footnote columns.")
        return df.drop(columns=df.filter(regex=r".*_f$").columns)

    def source_table_primary_key(self, source_ferc1: SourceFerc1) -> list[str]:
        """Look up the pre-renaming source table primary key columns."""
        if source_ferc1 == SourceFerc1.DBF:
            pk_cols = [
                "report_year",
                "report_prd",
                "respondent_id",
                "spplmnt_num",
                "row_number",
            ]
        else:
            assert source_ferc1 == SourceFerc1.XBRL  # nosec: B101
            cols = self.params.rename_columns_ferc1.xbrl.columns
            pk_cols = ["report_year", "entity_id"]
            # Sort to avoid dependence on the ordering of rename_columns.
            # Doing the sorting here because we have a particular ordering
            # hard coded for the DBF primary keys.
            pk_cols += sorted(col for col in cols if col.endswith("_axis"))
        return pk_cols

    def renamed_table_primary_key(self, source_ferc1: SourceFerc1) -> list[str]:
        """Look up the post-renaming primary key columns."""
        if source_ferc1 == SourceFerc1.DBF:
            cols = self.params.rename_columns_ferc1.dbf.columns
        else:
            assert source_ferc1 == SourceFerc1.XBRL  # nosec: B101
            cols = self.params.rename_columns_ferc1.xbrl.columns
        pk_cols = self.source_table_primary_key(source_ferc1=source_ferc1)
        # Translate to the renamed columns
        return [cols[col] for col in pk_cols]

    @cache_df(key="dbf")
    def drop_unused_original_columns_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove residual DBF specific columns."""
        unused_cols = [
            "report_prd",
            "spplmnt_num",
            "row_number",
            "row_seq",
            "row_prvlg",
        ]
        logger.debug(
            f"{self.table_id.value}: Dropping unused DBF structural columns: "
            f"{unused_cols}"
        )
        missing_cols = set(unused_cols).difference(df.columns)
        if missing_cols:
            raise ValueError(
                f"{self.table_id.value}: Trying to drop missing original DBF columns:"
                f"{missing_cols}"
            )
        return df.drop(columns=unused_cols)

    def assign_record_id(
        self, df: pd.DataFrame, source_ferc1: SourceFerc1
    ) -> pd.DataFrame:
        """Add a column identifying the original source record for each row.

        It is often useful to be able to tell exactly which record in the FERC Form 1
        database a given record within the PUDL database came from.

        Within each FERC Form 1 DBF table, each record is supposed to be uniquely
        identified by the combination of: report_year, report_prd, utility_id_ferc1_dbf,
        spplmnt_num, row_number.

        The FERC Form 1 XBRL tables do not have these supplement and row number
        columns, so we construct an id based on:
        report_year, utility_id_ferc1_xbrl, and the primary key columns of the XBRL table

        Args:
            df: table to assign `record_id` to
            table_name: name of table
            source_ferc1: data source of raw ferc1 database.

        Raises:
            ValueError: If any of the primary key columns are missing from the DataFrame
                being processed.
            ValueError: If there are any null values in the primary key columns.
            ValueError: If the resulting `record_id` column is non-unique.
        """
        logger.debug(
            f"{self.table_id.value}: Assigning {source_ferc1.value} source record IDs."
        )
        pk_cols = self.renamed_table_primary_key(source_ferc1)
        missing_pk_cols = set(pk_cols).difference(df.columns)
        if missing_pk_cols:
            raise ValueError(
                f"{self.table_id.value} ({source_ferc1.value}): Missing primary key "
                "columns in dataframe while assigning source record_id: "
                f"{missing_pk_cols}"
            )
        if df[pk_cols].isnull().any(axis=None):
            raise ValueError(
                f"{self.table_id.value} ({source_ferc1.value}): Found null primary key "
                "values.\n"
                f"{df[pk_cols].isnull().any()}"
            )
        df = df.assign(
            record_id=lambda x: x.sched_table_name.str.cat(
                x[pk_cols].astype(str), sep="_"
            ),
        )
        if df.sched_table_name.isnull().any():
            raise ValueError(
                f"{self.table_id.value}: Null sched_table_name's were found where none "
                "were expected."
            )
        df.record_id = enforce_snake_case(df.record_id)

        dupe_ids = df.record_id[df.record_id.duplicated()].values
        if dupe_ids.any() and self.has_unique_record_ids:
            logger.warning(
                f"{self.table_id.value}: Found {len(dupe_ids)} duplicate record_ids: \n"
                f"{dupe_ids}."
            )
        df = df.drop(columns="sched_table_name")
        return df

    def assign_utility_id_ferc1(
        self, df: pd.DataFrame, source_ferc1: SourceFerc1
    ) -> pd.DataFrame:
        """Assign the PUDL-assigned utility_id_ferc1 based on the native utility ID.

        We need to replace the natively reported utility ID from each of the two FERC1
        sources with a PUDL-assigned utilty. The mapping between the native ID's and
        these PUDL-assigned ID's can be accessed in the database tables
        ``utilities_dbf_ferc1`` and ``utilities_xbrl_ferc1``.

        Args:
            df: the input table with the native utilty ID column.
            source_ferc1: the

        Returns:
            an augemented version of the input ``df`` with a new column that replaces
            the natively reported utility ID with the PUDL-assigned utility ID.
        """
        logger.debug(
            f"{self.table_id.value}: Assigning {source_ferc1.value} source utility IDs."
        )
        utility_map_ferc1 = pudl.glue.ferc1_eia.get_utility_map_ferc1()
        # use the source utility ID column to get a unique map and for merging
        util_id_col = f"utility_id_ferc1_{source_ferc1.value}"
        util_map_series = (
            utility_map_ferc1.dropna(subset=[util_id_col])
            .set_index(util_id_col)
            .utility_id_ferc1
        )

        df["utility_id_ferc1"] = df[util_id_col].map(util_map_series)
        return df


class FuelFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the :ref:`fuel_ferc1` table.

    The :ref:`fuel_ferc1` table reports data about fuel consumed by large thermal power
    plants in the :ref:`plants_steam_ferc1` table. Each record in the steam table is
    typically associated with several records in the fuel table, with each fuel record
    reporting data for a particular type of fuel consumed by that plant over the course
    of a year. The fuel table presents several challenges.

    The type of fuel, which is part of the primary key for the table, is a freeform
    string with hundreds of different nonstandard values. These strings are categorized
    manually and converted to ``fuel_type_code_pudl``. Some values cannot be categorized
    and are set to ``other``. In other string categorizations we set the unidentifiable
    values to NA, but in this table the fuel type is part of the primary key and primary
    keys cannot contain NA values.

    This simplified categorization occasionally results in records with duplicate
    primary keys. In those cases the records are aggregated into a single record if they
    have the same apparent physical units. If the fuel units are different, only the
    first record is retained.

    Several columns have unspecified, inconsistent, fuel-type specific units of measure
    associated with them. In order for records to be comparable and aggregatable, we
    have to infer and standardize these units.

    In the raw FERC Form 1 data there is a ``fuel_units`` column which describes the
    units of fuel delivered or consumed. Most commonly this is short tons for solid
    fuels (coal), thousands of cubic feet (Mcf) for gaseous fuels, and barrels (bbl) for
    liquid fuels.  However, the ``fuel_units`` column is also a freeform string with
    hundreds of nonstandard values which we have to manually categorize, and many of the
    values do not map directly to the most commonly used units for fuel quantities. E.g.
    some solid fuel quantities are reported in pounds, or thousands of pounds, not tons;
    some liquid fuels are reported in gallons or thousands of gallons, not barrels; and
    some gaseous fuels are reported in cubic feet not thousands of cubic feet.

    Two additional columns report fuel price per unit of heat content and fuel heat
    content per physical unit of fuel. The units of those columns are not explicitly
    reported, vary by fuel, and are inconsistent within individual fuel types.

    We adopt standardized units and attempt to convert all reported values in the fuel
    table into those units. For physical fuel units we adopt those that are used by the
    EIA: short tons (tons) for solid fuels, barrels (bbl) for liquid fuels, and
    thousands of cubic feet (mcf) for gaseous fuels. For heat content per (physical)
    unit of fuel, we use millions of British thermal units (mmbtu). All fuel prices are
    converted to US dollars, while many are reported in cents.

    Because the reported fuel price and heat content units are implicit, we have to
    infer them based on observed values. This is only possible because these quantities
    are ratios with well defined ranges of valid values. The common units that we
    observe and attempt to standardize include:

    * coal: primarily BTU/pound, but also MMBTU/ton and MMBTU/pound.
    * oil: primarily BTU/gallon.
    * gas: reported in a mix of MMBTU/cubic foot, and MMBTU/thousand cubic feet.
    """

    table_id: TableIdFerc1 = TableIdFerc1.FUEL_FERC1

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Table specific transforms for fuel_ferc1.

        Args:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.
        """
        return (
            self.spot_fix_values(df)
            .pipe(self.drop_invalid_rows)
            .pipe(self.correct_units)
        )

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Start with inherited method and do some fuel-specific processing.

        We have to do most of the transformation before the DBF and XBRL data have been
        concatenated because the fuel type column is part of the primary key and it is
        extensively modified in the cleaning process.
        """
        df = (
            super()
            .process_dbf(raw_dbf)
            .pipe(self.convert_units)
            .pipe(self.normalize_strings)
            .pipe(self.categorize_strings)
            .pipe(self.standardize_physical_fuel_units)
        )
        return df

    @cache_df(key="xbrl")
    def process_xbrl(
        self, raw_xbrl_instant: pd.DataFrame, raw_xbrl_duration: pd.DataFrame
    ) -> pd.DataFrame:
        """Special pre-concat treatment of the :ref:`fuel_ferc1` table.

        We have to do most of the transformation before the DBF and XBRL data have been
        concatenated because the fuel type column is part of the primary key and it is
        extensively modified in the cleaning process. For the XBRL data, this means we
        can't create a record ID until that fuel type value is clean. In addition, the
        categorization of fuel types results in a number of duplicate fuel records which
        need to be aggregated.

        Args:
            raw_xbrl_instant: Freshly extracted XBRL instant fact table.
            raw_xbrl_duration: Freshly extracted XBRL duration fact table.

        Returns:
            Almost fully transformed XBRL data table, with instant and duration facts
            merged together.
        """
        return (
            self.merge_instant_and_duration_tables_xbrl(
                raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.rename_columns, rename_stage="xbrl")
            .pipe(self.convert_units)
            .pipe(self.normalize_strings)
            .pipe(self.categorize_strings)
            .pipe(self.standardize_physical_fuel_units)
            .pipe(self.aggregate_duplicate_fuel_types_xbrl)
            .pipe(self.assign_record_id, source_ferc1=SourceFerc1.XBRL)
            .pipe(
                self.assign_utility_id_ferc1,
                source_ferc1=SourceFerc1.XBRL,
            )
        )

    def standardize_physical_fuel_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert reported fuel quantities to standard units depending on fuel type.

        Use the categorized fuel type and reported fuel units to convert all fuel
        quantities to the following standard units, depending on whether the fuel is a
        solid, liquid, or gas. When a single fuel reports its quantity in fundamentally
        different units, convert based on typical values. E.g. 19.85 MMBTU per ton of
        coal, 1.037 Mcf per MMBTU of natural gas, 7.46 barrels per ton of oil.

          * solid fuels (coal and waste): short tons [ton]
          * liquid fuels (oil): barrels [bbl]
          * gaseous fuels (gas): thousands of cubic feet [mcf]

        Columns to which these physical units apply:

          * fuel_consumed_units (tons, bbl, mcf)
          * fuel_cost_per_unit_burned (usd/ton, usd/bbl, usd/mcf)
          * fuel_cost_per_unit_delivered (usd/ton, usd/bbl, usd/mcf)

        One remaining challenge in this standardization is that nuclear fuel is reported
        in both mass of Uranium and fuel heat content, and it's unclear if there's any
        reasonable typical conversion between these units, since available heat content
        depends on the degree of U235 enrichement, the type of reactor, and whether the
        fuel is just Uranium, or a mix of Uranium and Plutonium from decommissioned
        nuclear weapons. See:

        https://world-nuclear.org/information-library/facts-and-figures/heat-values-of-various-fuels.aspx
        """
        df = df.copy()

        FuelFix = namedtuple("FuelFix", ["fuel", "from_unit", "to_unit", "mult"])
        fuel_fixes = [
            # US average coal heat content is 19.85 mmbtu/short ton
            FuelFix("coal", "mmbtu", "ton", (1.0 / 19.85)),
            FuelFix("coal", "btu", "ton", (1.0 / 19.85e6)),
            # 2000 lbs per short ton
            FuelFix("coal", "lbs", "ton", (1.0 / 2000.0)),
            FuelFix("coal", "klbs", "ton", (1.0 / 2.0)),
            # 42 gallons per barrel. Seriously, who makes up these units?
            FuelFix("oil", "gal", "bbl", (1.0 / 42.0)),
            FuelFix("oil", "kgal", "bbl", (1000.0 / 42.0)),
            # On average a "ton of oil equivalent" is 7.46 barrels
            FuelFix("oil", "ton", "bbl", 7.46),
            FuelFix("gas", "mmbtu", "mcf", (1.0 / 1.037)),
            # Nuclear plants report either heat content or mass of heavy metal
            # MW*days thermal to MWh thermal
            FuelFix("nuclear", "mwdth", "mwhth", 24.0),
            # Straight energy equivalence between BTU and MWh here:
            FuelFix("nuclear", "mmbtu", "mwhth", (1.0 / 3.412142)),
            FuelFix("nuclear", "btu", "mwhth", (1.0 / 3412142)),
            # Unclear if it's possible to convert heavy metal to heat reliably
            FuelFix("nuclear", "grams", "kg", (1.0 / 1000)),
        ]
        for fix in fuel_fixes:
            fuel_mask = df.fuel_type_code_pudl == fix.fuel
            unit_mask = df.fuel_units == fix.from_unit
            df.loc[(fuel_mask & unit_mask), "fuel_consumed_units"] *= fix.mult
            # Note: The 2 corrections below DIVIDE by the multiplier because the units
            # are in the denominator ("per_unit") rather than the numerator.
            df.loc[(fuel_mask & unit_mask), "fuel_cost_per_unit_burned"] /= fix.mult
            df.loc[(fuel_mask & unit_mask), "fuel_cost_per_unit_delivered"] /= fix.mult
            df.loc[(fuel_mask & unit_mask), "fuel_units"] = fix.to_unit

        # Set all remaining non-standard units and affected columns to NA.
        FuelAllowedUnits = namedtuple("FuelAllowedUnits", ["fuel", "allowed_units"])
        fuel_allowed_units = [
            FuelAllowedUnits("coal", ("ton",)),
            FuelAllowedUnits("oil", ("bbl",)),
            FuelAllowedUnits("gas", ("mcf",)),
            FuelAllowedUnits("nuclear", ("kg", "mwhth")),
            FuelAllowedUnits("waste", ("ton",)),
            # All unidentified fuel types ("other") get units set to NA
            FuelAllowedUnits("other", ()),
        ]
        physical_units_cols = [
            "fuel_consumed_units",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_unit_delivered",
        ]
        for fau in fuel_allowed_units:
            fuel_mask = df.fuel_type_code_pudl == fau.fuel
            invalid_unit_mask = ~df.fuel_units.isin(fau.allowed_units)
            df.loc[(fuel_mask & invalid_unit_mask), physical_units_cols] = np.nan
            df.loc[(fuel_mask & invalid_unit_mask), "fuel_units"] = pd.NA

        return df

    @cache_df(key="xbrl")
    def aggregate_duplicate_fuel_types_xbrl(
        self, fuel_xbrl: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate the fuel records having duplicate primary keys."""
        pk_cols = self.renamed_table_primary_key(source_ferc1=SourceFerc1.XBRL) + [
            "sched_table_name"
        ]
        fuel_xbrl.loc[:, "fuel_units_count"] = fuel_xbrl.groupby(pk_cols, dropna=False)[
            "fuel_units"
        ].transform("nunique")

        # split
        dupe_mask = fuel_xbrl.duplicated(subset=pk_cols, keep=False)
        multi_unit_mask = fuel_xbrl.fuel_units_count != 1

        fuel_pk_dupes = fuel_xbrl[dupe_mask & ~multi_unit_mask].copy()
        fuel_multi_unit = fuel_xbrl[dupe_mask & multi_unit_mask].copy()
        fuel_non_dupes = fuel_xbrl[~dupe_mask & ~multi_unit_mask]

        logger.info(
            f"{self.table_id.value}: Aggregating {len(fuel_pk_dupes)} rows with "
            f"duplicate primary keys out of {len(fuel_xbrl)} total rows."
        )
        logger.info(
            f"{self.table_id.value}: Dropping {len(fuel_multi_unit)} records with "
            "inconsistent fuel units preventing aggregation "
            f"out of {len(fuel_xbrl)} total rows."
        )
        agg_row_fraction = (len(fuel_pk_dupes) + len(fuel_multi_unit)) / len(fuel_xbrl)
        if agg_row_fraction > 0.15:
            logger.error(
                f"{self.table_id.value}: {agg_row_fraction:.0%} of all rows are being "
                "aggregated. Higher than the allowed value of 15%!"
            )
        data_cols = [
            "fuel_consumed_units",
            "fuel_mmbtu_per_unit",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_mmbtu",
            "fuel_cost_per_mwh",
            "fuel_mmbtu_per_mwh",
        ]
        # apply
        fuel_pk_dupes = pudl.helpers.sum_and_weighted_average_agg(
            df_in=fuel_pk_dupes,
            by=pk_cols + ["start_date", "end_date", "fuel_units"],
            sum_cols=["fuel_consumed_units"],
            wtavg_dict={
                k: "fuel_consumed_units"
                for k in data_cols
                if k != "fuel_consumed_units"
            },
        )
        # We can't aggregate data when fuel units are inconsistent, but we don't want
        # to lose the records entirely, so we'll keep the first one.
        fuel_multi_unit.loc[:, data_cols] = np.nan
        fuel_multi_unit = fuel_multi_unit.drop_duplicates(subset=pk_cols, keep="first")
        # combine
        return pd.concat([fuel_non_dupes, fuel_pk_dupes, fuel_multi_unit]).drop(
            columns=["fuel_units_count"]
        )

    def drop_total_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows that represent plant totals rather than individual fuels.

        This is an imperfect, heuristic process. The rows we identify as probably
        representing totals rather than individual fuels:

        * have zero or null values in all of their numerical data columns
        * have no identifiable fuel type
        * have no identifiable fuel units
        * DO report a value for MMBTU / MWh (heat rate)

        In the case of the fuel_ferc1 table, we drop any row where all the data columns
        are null AND there's a non-null value in the ``fuel_mmbtu_per_mwh`` column, as
        it typically indicates a "total" row for a plant. We also require a null value
        for the fuel_units and an "other" value for the fuel type.
        """
        data_cols = [
            "fuel_consumed_units",
            "fuel_mmbtu_per_unit",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_mmbtu",
            "fuel_cost_per_mwh",
        ]
        total_rows_idx = df[
            df[data_cols].isna().all(axis="columns")  # No normal numerical data
            & df.fuel_units.isna()  # no recognizable fuel units
            & (df.fuel_type_code_pudl == "other")  # No recognizable fuel type
            & df.fuel_mmbtu_per_mwh.notna()  # But it DOES report heat rate!
        ].index
        logger.info(
            f"{self.table_id.value}: Dropping "
            f"{len(total_rows_idx)}/{len(df)}"
            "rows representing plant-level all-fuel totals."
        )
        return df.drop(index=total_rows_idx)

    def drop_invalid_rows(
        self, df: pd.DataFrame, params: InvalidRows | None = None
    ) -> pd.DataFrame:
        """Drop invalid rows from the fuel table.

        This method both drops rows in which all required data columns are null (using
        the inherited parameterized method) and then also drops those rows we believe
        represent plant totals. See :meth:`FuelFerc1TableTransformer.drop_total_rows`.
        """
        return super().drop_invalid_rows(df, params).pipe(self.drop_total_rows)


class PlantsSteamFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for the :ref:`plants_steam_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.PLANTS_STEAM_FERC1

    @cache_df(key="main")
    def transform_main(
        self, df: pd.DataFrame, transformed_fuel: pd.DataFrame
    ) -> pd.DataFrame:
        """Perform table transformations for the :ref:`plants_steam_ferc1` table.

        Note that this method has a non-standard call signature, since the
        :ref:`plants_steam_ferc1` table depends on the :ref:`fuel_ferc1` table.

        Args:
            df: The pre-processed steam plants table.
            transformed_fuel: The fully transformed :ref:`fuel_ferc1` table. This is
                required because fuel consumption information is used to help link
                steam plant records together across years using
                :func:`plants_steam_assign_plant_ids`
        """
        fuel_categories = list(
            FuelFerc1TableTransformer()
            .params.categorize_strings["fuel_type_code_pudl"]
            .categories.keys()
        )
        plants_steam = (
            super()
            .transform_main(df)
            .pipe(
                plants_steam_assign_plant_ids,
                ferc1_fuel_df=transformed_fuel,
                fuel_categories=fuel_categories,
            )
            .pipe(plants_steam_validate_ids)
        )
        return plants_steam

    def transform(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
        transformed_fuel: pd.DataFrame,
    ) -> pd.DataFrame:
        """Redfine the transform method to accommodate the use of transformed_fuel.

        This is duplicating code from the parent class, but is necessary because the
        steam table needs the fuel table for its transform. Is there a better way to do
        this that doesn't require cutting and pasting the whole method just to stick the
        extra dataframe input into transform_main()?
        """
        df = (
            self.transform_start(
                raw_dbf=raw_dbf,
                raw_xbrl_instant=raw_xbrl_instant,
                raw_xbrl_duration=raw_xbrl_duration,
            )
            .pipe(self.transform_main, transformed_fuel=transformed_fuel)
            .pipe(self.transform_end)
        )
        if self.clear_cached_dfs:
            logger.debug(
                f"{self.table_id.value}: Clearing cached dfs: "
                f"{sorted(self._cached_dfs.keys())}"
            )
            self._cached_dfs.clear()
        return df


class PlantsHydroFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the :ref:`plants_hydro_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.PLANTS_HYDRO_FERC1

    def transform_main(self, df):
        """Add bespoke removal of duplicate record after standard transform_main."""
        return super().transform_main(df).pipe(self.targeted_drop_duplicates)

    def targeted_drop_duplicates(self, df):
        """Targeted removal of known duplicate record.

        There are two records in 2019 with a ``utility_id_ferc1`` of 200 and a
        ``plant_name_ferc1`` of "marmet". The records are nearly duplicates of
        eachother, except one have nulls in the capex columns. Surgically remove the
        record with the nulls.
        """
        null_columns = [
            "capex_land",
            "capex_structures",
            "capex_facilities",
            "capex_equipment",
            "capex_roads",
            "asset_retirement_cost",
            "capex_total",
            "capex_per_mw",
        ]
        dupe_mask = (
            (df.report_year == 2019)
            & (df.utility_id_ferc1 == 200)
            & (df.plant_name_ferc1 == "marmet")
        )
        null_maks = df[null_columns].isnull().all(axis="columns")

        possible_dupes = df.loc[dupe_mask]
        if (len(possible_dupes) != 2) & (2019 in df.report_year.unique()):
            raise AssertionError(
                f"{self.table_id}: Expected 2 records for found: {possible_dupes}"
            )
        dropping = df.loc[(dupe_mask & null_maks)]
        logger.debug(
            f"Dropping {len(dropping)} duplicate record with null data in {null_columns}"
        )
        df = df.loc[~(dupe_mask & null_maks)].copy()
        return df


class PlantsPumpedStorageFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`plants_pumped_storage_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.PLANTS_PUMPED_STORAGE_FERC1


class PurchasedPowerFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`purchased_power_ferc1` table.

    This table has data about inter-utility power purchases into the PUDL DB. This
    includes how much electricty was purchased, how much it cost, and who it was
    purchased from. Unfortunately the field describing which other utility the power was
    being bought from is poorly standardized, making it difficult to correlate with
    other data. It will need to be categorized by hand or with some fuzzy matching
    eventually.
    """

    table_id: TableIdFerc1 = TableIdFerc1.PURCHASED_POWER_FERC1


class PlantInServiceFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A transformer for the :ref:`plant_in_service_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.PLANT_IN_SERVICE_FERC1
    has_unique_record_ids: bool = False

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(self, xbrl_metadata_json) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        The XBRL Taxonomy metadata as extracted pertains to the XBRL data as extracted.
        When we re-shape the data, we also need to adjust the metadata to be usable
        alongside the reshaped data. For the plant in service table, this means
        selecting metadata fields that pertain to the "stem" column name (not
        differentiating between starting/ending balance, retirements, additions, etc.)

        We fill in some gaps in the metadata, e.g. for FERC accounts that have been
        split across multiple rows, or combined without being calculated. We also need
        to rename the XBRL metadata categories to conform to the same naming convention
        that we are using in the data itself (since FERC doesn't quite follow their own
        naming conventions...). We use the same rename dictionary, but as an argument to
        :meth:`pd.Series.replace` instead of :meth:`pd.DataFrame.rename`.
        """
        pis_meta = (
            super()
            .process_xbrl_metadata(xbrl_metadata_json)
            .assign(
                xbrl_factoid=lambda x: x.xbrl_factoid.replace(
                    self.params.rename_columns_ferc1.instant_xbrl.columns
                )
            )
        )

        # Set pseudo-account numbers for rows that split or combine FERC accounts, but
        # which are not calculated values.
        pis_meta.loc[
            pis_meta.xbrl_factoid == "electric_plant_purchased", "ferc_account"
        ] = "102_purchased"
        pis_meta.loc[
            pis_meta.xbrl_factoid == "electric_plant_sold", "ferc_account"
        ] = "102_sold"
        pis_meta.loc[
            pis_meta.xbrl_factoid
            == "electric_plant_in_service_and_completed_construction_not_classified_electric",
            "ferc_account",
        ] = "101_and_106"
        return pis_meta

    def apply_sign_conventions(self, df) -> pd.DataFrame:
        """Adjust rows and column sign conventsion to enable aggregation by summing.

        Columns have uniform sign conventions, which we have manually inferred from the
        original metadata. This can and probably should be done programmatically in the
        future. If not, we'll probably want to store the column_weights as a parameter
        rather than hard-coding it in here.
        """
        column_weights = {
            "starting_balance": 1.0,
            "additions": 1.0,
            "retirements": -1.0,
            "transfers": 1.0,
            "adjustments": 1.0,
            "ending_balance": 1.0,
        }

        # Set row weights based on the value of the "balance" field
        df.loc[df.balance == "debit", "row_weight"] = 1.0
        df.loc[df.balance == "credit", "row_weight"] = -1.0

        # Apply column weightings. Can this be done all at once in a vectorized way?
        for col in column_weights:
            df.loc[:, col] *= column_weights[col]
            df.loc[:, col] *= df["row_weight"]

        return df

    def targeted_drop_duplicates_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop bad duplicate records from a specific utility in 2018.

        This is a very specific fix, meant to get rid of a particular observed set of
        duplicate records: FERC Respondent ID 187 in 2018 has two sets of plant in
        service records, one of which contains a bunch of null data.

        This method is part of the DBF processing because we want to be able to
        hard-code a specific value of ``utility_id_ferc1_dbf`` and those IDs are no
        longer available later in the process. I think.
        """
        # A single utility has double reported data in 2018.
        pk = ["report_year", "utility_id_ferc1", "ferc_account_label"]
        dupe_mask = (
            df.duplicated(subset=pk, keep=False)
            & (df.report_year == 2018)
            & (df.utility_id_ferc1_dbf == 187)
        )
        all_dupes = df[dupe_mask]
        # The observed pairs of duplicate records have NA values in all of the
        # additions, retirements, adjustments, and transfers columns. This selects
        # only those duplicates that have *any* non-null value in those rows.
        good_dupes = all_dupes[
            all_dupes[["additions", "retirements", "adjustments", "transfers"]]
            .notnull()
            .any(axis="columns")
        ]
        # Make sure that the good and bad dupes have exactly the same indices:
        pd.testing.assert_index_equal(
            good_dupes.set_index(pk).index,
            all_dupes.set_index(pk).index.drop_duplicates(),
        )
        deduped = pd.concat([df[~dupe_mask], good_dupes], axis="index")
        remaining_dupes = deduped[deduped.duplicated(subset=pk)]
        logger.info(
            f"{self.table_id.value}: {len(remaining_dupes)} dupes remaining after "
            "targeted deduplication."
        )
        return deduped

    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Drop targeted duplicates in the DBF data so we can use FERC respondent ID."""
        return super().process_dbf(raw_dbf).pipe(self.targeted_drop_duplicates_dbf)

    @cache_df("main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """The main table-specific transformations, affecting contents not structure.

        Annotates and alters data based on information from the XBRL taxonomy metadata.
        """
        return super().transform_main(df).pipe(self.apply_sign_conventions)


class PlantsSmallFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the :ref:`plants_small_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.PLANTS_SMALL_FERC1

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Table specific transforms for plants_small_ferc1.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.
        """
        df = (
            self.spot_fix_values(df)
            .pipe(self.normalize_strings)
            .pipe(self.nullify_outliers)
            .pipe(self.convert_units)
            .pipe(self.extract_ferc1_license)
            .pipe(self.label_row_types)
            .pipe(self.prep_header_fuel_and_plant_types)
            .pipe(self.map_plant_name_fuel_types)
            .pipe(self.categorize_strings)
            .pipe(self.map_header_fuel_and_plant_types)
            .pipe(self.associate_notes_with_values)
            .pipe(self.spot_fix_rows)
            .pipe(self.drop_invalid_rows)
            # Now remove the row_type columns because we've already moved totals to a
            # different column
            .drop(columns=["row_type"])
        )

        return df

    def extract_ferc1_license(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract FERC license number from ``plant_name_ferc1``.

        Many FERC license numbers are embedded in the ``plant_name_ferc1`` column, but
        not all numbers in the ``plant_name_ferc1`` column are FERC licenses. Some are
        dates, dollar amounts, page numbers, or numbers of wind turbines. This function
        extracts valid FERC license numbers and puts them in a new column called
        ``license_id_ferc1``.

        Potential FERC license numbers are valid when:

        - Two or more integers were found.
        - The found integers were accompanied by key phrases such as:
          ``["license", "no.", "ferc", "project"]``.
        - The accompanying name does not contain phrases such as:
          ``["page", "pg", "$",  "wind", "units"]``.
        - The found integers don't fall don't fall within the range of a valid year,
          defined as: 1900-2050.
        - The plant record is categorized as ``hydro`` or not categorized via the
          ``plant_type`` and ``fuel_type`` columns.

        This function also fills ``other`` fuel types with ``hydro`` for all plants with
        valid FERC licenses because only hydro plants have FERC licenses.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            The same input DataFrame but with a new column called ``license_id_ferc1``
            that contains FERC 1 license infromation extracted from
            ``plant_name_ferc1``.
        """
        logger.info(f"{self.table_id.value}: Extracting FERC license from plant name")
        # Extract all numbers greater than 2 digits from plant_name_ferc1 and put them
        # in a new column as integers.
        out_df = df.assign(
            license_id_ferc1=lambda x: (
                x.plant_name_ferc1.str.extract(r"(\d{3,})")
                .astype("float")
                .astype("Int64")
            ),
        )
        # Define what makes a good license
        obvious_license = out_df.plant_name_ferc1.str.contains(
            r"no\.|license|ferc|project", regex=True
        )
        not_license = out_df.plant_name_ferc1.str.contains(
            r"page|pg|\$|wind|solar|nuclear|nonutility|units|surrendered", regex=True
        )
        exceptions_to_is_year = out_df.plant_name_ferc1.str.contains(
            r"tomahawk|otter rapids|wausau|alexander|hooksett|north umpqua", regex=True
        )
        is_year = out_df["license_id_ferc1"].between(1900, 2050)
        not_hydro = ~out_df["plant_type"].isin(["hydro", np.nan, None]) | ~out_df[
            "fuel_type"
        ].isin(["hydro", "other"])
        # Replace all the non-license numbers with NA
        out_df.loc[
            (not_hydro & ~obvious_license)
            | not_license
            | (is_year & ~obvious_license & ~exceptions_to_is_year),
            "license_id_ferc1",
        ] = np.nan
        # Fill fuel type with hydro
        out_df.loc[
            out_df["license_id_ferc1"].notna() & (out_df["fuel_type"] == "other"),
            "fuel_type",
        ] = "hydro"

        return out_df

    def _find_possible_header_or_note_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Find and label rows that might be headers or notes.

        Called by the coordinating function :func:`label_row_types`.

        This function creates a column called ``possible_header_or_note`` that is either
        True or False depending on whether a group of columns are all NA. Rows labeled
        as True will be further scrutinized in the :func:`_label_header_rows` and
        :func:`_label_note_rows` functions to determine whether they are actually
        headers or notes.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            The same input DataFrame but with a new column called
            ``possible_header_or_note`` that flags rows that might contain useful header
            or note information.
        """
        # Define header qualifications
        possible_header_or_note_if_cols_na = [
            "construction_year",
            "net_generation_mwh",
            "total_cost_of_plant",
            "capex_total",
            "capex_per_mw",
            "opex_total",
            "opex_fuel",
            "opex_maintenance",
            "fuel_cost_per_mmbtu",
            # "peak_demand_mw",
            # "opex_operations"
        ]
        # Label possible header or note rows
        df["possible_header_or_note"] = (
            df.filter(possible_header_or_note_if_cols_na).isna().all(1)
        )
        return df

    def _find_note_clumps(
        self, group: DataFrameGroupBy
    ) -> tuple[DataFrameGroupBy, pd.DataFrame]:
        """Find groups of rows likely to be notes.

        Once the :func:`_find_possible_header_or_note_rows` function identifies rows
        that are either headers or notes, we must deterine which one they are. As
        described in the :func:`_label_note_rows` function, notes rows are usually
        adjecent rows with no content.

        This function itentifies instances of two or more adjecent rows where
        ``possible_header_or_note`` = True. It takes individual utility-year groups as a
        parameter as opposed to the entire dataset because adjecent rows are only
        meaningful if they are from the same reporting entity in the same year. If we
        were to run this on the whole dataframe, we would see "note clumps" that are
        actually notes from the end of one utility's report and headers from the
        beginning of another. For this reason, we run this function from within the
        :func:`_label_note_rows_group` function.

        The output of this function is not a modified version of the original
        utility-year group, rather, it is a DataFrame containing information about the
        nature of the ``possible_header_or_note`` = True rows that is used to determine
        if that row is a note or not. It also returns the original utility-year-group as
        groupby objects seperated by each time ``possible_header_or_note`` changes from
        True to False or vice versa.

        If you pass in the following df:

        +-------------------+-------------------------+
        | plant_name_ferc1  | possible_header_or_note |
        +===================+=========================+
        | HYDRO:            | True                    |
        +-------------------+-------------------------+
        | rainbow falls (b) | False                   |
        +-------------------+-------------------------+
        | cadyville (a)     | False                   |
        +-------------------+-------------------------+
        | keuka (c)         | False                   |
        +-------------------+-------------------------+
        | (a) project #2738 | True                    |
        +-------------------+-------------------------+
        | (b) project #2835 | True                    |
        +-------------------+-------------------------+
        | (c) project #2852 | True                    |
        +-------------------+-------------------------+

        You will get the following output (in addition to the groupby objects for each
        clump):

        +----------------+----------------+
        | header_or_note | rows_per_clump |
        +================+================+
        | True           | 1              |
        +----------------+----------------+
        | False          | 3              |
        +----------------+----------------+
        | True           | 3              |
        +----------------+----------------+

        This shows each clump of adjecent records where ``possible_header_or_note`` is
        True or False and how many records are in each clump.

        Params:
            group: A utility-year grouping of the concatenated FERC XBRL and DBF tables.
                This table must have been run through the
                :func:`_find_possible_header_or_note_rows` function and contain the
                column ``possible_header_or_note``.

        Returns:
            A tuple containing groupby objects for each of the note and non-note clumps
            and a DataFrame indicating the number of rows in each note or non-note
            clump.
        """
        # Make groups based on consecutive sections where the group_col is alike.
        clump_groups = group.groupby(
            (
                group["possible_header_or_note"].shift()
                != group["possible_header_or_note"]
            ).cumsum(),
            as_index=False,
        )

        # Identify the first (and only) group_col value for each group and count
        # how many rows are in each group.
        clump_groups_df = clump_groups.agg(
            header_or_note=("possible_header_or_note", "first"),
            rows_per_clump=("possible_header_or_note", "count"),
        )

        return clump_groups, clump_groups_df

    def _label_header_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Label header rows by adding ``header`` to ``row_type`` column.

        Called by the coordinating function :func:`label_row_types`.

        Once possible header or notes rows have been identified via the
        :func:`_find_possible_header_or_note_rows` function, this function sorts out
        which ones are headers. It does this by identifying a list of strings that, when
        found in the ``plant_name_ferc1`` column, indicate that the row is or is not a
        header.

        Sometimes this function identifies a header that is acutally a note. For this
        reason, it's important that the function be called before
        :func:`_label_note_rows` so that the bad header values get overridden by the
        ``note`` designation.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data that has been run through
            the :func:`_find_possible_header_or_note_rows` function and contains the
            column ``possible_header_or_note``.

        Returns:
            The same input DataFrame but with likely headers rows containing the string
            ``header`` in the ``row_type`` column.
        """
        # Possible headers/note rows that contains these strings are headers
        header_strings = [
            "hydro",
            "hyrdo",
            "internal",
            "wind",
            "solar",
            "gas",
            "diesel",
            "diesal",
            "steam",
            "other",
            "combustion",
            "combustine",
            "fuel cell",
            "hydraulic",
            "waste",
            "landfill",
            "photovoltaic",
            "nuclear",
            "oil",
            "renewable",
            "facilities",
            "combined cycle",
        ]
        # Possible headers/note rows that contains these strings are not headers
        nonheader_strings = [
            "#",
            r"\*",
            "pg",
            "solargenix",
            "solargennix",
            r"\@",
            "rockton",
            "albany steam",
            "other general ops. supervision & engineering",
        ]
        # Any rows that contains these strings are headers
        header_exceptions = [
            "hydro plants: licensed proj. no.",
            "hydro license no.",
            "hydro: license no.",
            "hydro plants: licensed proj no.",
            "photo voltaic generating plants:",
        ]

        logger.info(f"{self.table_id.value}: Labeling header rows")

        # Label good header rows (based on whether they contain key strings)
        possible_header = df["possible_header_or_note"]
        good_header = df["plant_name_ferc1"].str.contains("|".join(header_strings))
        bad_header = df["plant_name_ferc1"].str.contains("|".join(nonheader_strings))
        df.loc[possible_header & good_header & ~bad_header, "row_type"] = "header"
        # There are some headers that don't pass the possible_header test but are
        # still definitely headers.
        df.loc[df["plant_name_ferc1"].isin(header_exceptions), "row_type"] = "header"

        return df

    def _label_note_rows_group(
        self, util_year_group: DataFrameGroupBy
    ) -> DataFrameGroupBy:
        """Label note rows by adding ``note`` to ``row_type`` column.

        Called within the wraper function :func:`_label_note_rows`

        This function breaks the data down by reporting unit (utility and year) and
        determines whether a ``possible_header_note`` = True row is a note based on two
        criteria:

        - Clumps of 2 or more adjecent rows where ``possible_header_or_note`` is True.
        - Instances where the last row in a utility-year group has
          ``possible_header_or_note`` as True.

        There are a couple of important exceptions that this function also
        addresses. Utilities often have multiple headers in a single utility-year
        grouping. You might see something like: ``pd.Series([header, plant1, plant2,
        note, header, plant3, plant4])``. In this case, a note clump is actually
        comprised of a note followed by a header. This function will not override the
        header as a note. Unfortunately, there is always the possability that a header
        row is followed by a plant that had no values reported. This would look like,
        and therefore be categorized as a note clump. I haven't built a work around, but
        hopefully there aren't very many of these.

        Params:
            util_year_group: A groupby object that contains a single year and utility.

        Returns:
            The same input but with likely note rows containing the string ``note`` in
            the ``row_type`` column.
        """
        # Create mini groups that count pockets of true and false for each
        # utility and year. See _find_note_clumps docstring.
        clump_group, clump_count = self._find_note_clumps(util_year_group)

        # Used later to enable exceptions
        max_df_val = util_year_group.index.max()

        # Create a list of the index values where there is a note clump! This also
        # includes instances where the last row in a group is a note.
        note_clump_idx_list = list(
            clump_count[
                (clump_count["header_or_note"])
                & (
                    (clump_count["rows_per_clump"] > 1)
                    | (clump_count.tail(1)["rows_per_clump"] == 1)
                )
            ].index
        )
        # If there are any clumped/end headers:
        if note_clump_idx_list:
            for idx in note_clump_idx_list:
                # If the last row in a clump looks like a header, and the clump is
                # not the last clump in the utility_year group, then drop the last
                # row from the note clump index range because it's a header!
                note_clump_idx_range = clump_group.groups[idx + 1]
                not_last_clump = clump_group.groups[idx + 1].max() < max_df_val
                is_good_header = (
                    util_year_group.loc[
                        util_year_group.index.isin(clump_group.groups[idx + 1])
                    ]
                    .tail(1)["row_type"]
                    .str.contains("header")
                    .all()
                )
                if not_last_clump & is_good_header:
                    note_clump_idx_range = [
                        x
                        for x in note_clump_idx_range
                        if x != note_clump_idx_range.max()
                    ]
                # Label the note clump as a note
                util_year_group.loc[
                    util_year_group.index.isin(note_clump_idx_range), "row_type"
                ] = "note"

        return util_year_group

    def _label_note_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Wrapper for :func:`_label_note_rows_group`.

        The small plants table has lots of note rows that contain useful information.
        Unfortunately, the notes are in their own row rather than their own column! This
        means that useful information pertaining to plant rows is floating around as a
        junk row with no other information except the note in the ``plant_name_ferc1``
        field. Luckily, the data are reported just like they would be on paper. I.e.,
        The headers are at the top, and the notes are at the bottom. See the table in
        :func:`label_row_types` for more detail. This function labels note rows.

        Note rows are determined by row location within a given report, so we must break
        the data into reporting units (utility and year) and then apply note-finding
        methodology defined in :func:`_label_note_rows_group` to each group.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data that has been run through
            the :func:`_find_possible_header_or_note_rows` function and contains the
            column ``possible_header_or_note``.

        Returns:
            The same input DataFrame but with likely note rows containing the string
            ``note`` in the ``row_type`` column.
        """
        logger.info(f"{self.table_id.value}: Labeling notes rows")

        util_groups = df.groupby(["utility_id_ferc1", "report_year"])

        return util_groups.apply(lambda x: self._label_note_rows_group(x))

    def _label_total_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Label total rows by adding ``total`` to ``row_type`` column.

        Called within the wraper function :func:`_label_note_rows`

        For the most part, when ``plant_name_ferc1`` contains the string ``total``, the
        values therein are duplicates of what is already reported, i.e.: a total value.
        However, there are some cases where that's not true. For example, the phrase
        ``amounts are for the total`` appears when chunks of plants (usually but not
        always wind) are reported together. It's a total, but it's not double counting
        which is the reason for the ``total`` flag.

        Similar to :func:`_label_header_rows`, it's important that this be called before
        :func:`_label_note_rows` in :func:`label_row_types` so that not clumps can
        override certain non-totals that are mistakenly labeled as such.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            The same input DataFrame but with likely total rows containing the string
            ``total`` in the ``row_type`` column.
        """
        # Label totals in row_type in case it overwrites any headers
        logger.info(f"{self.table_id.value}: Labeling total rows")
        df.loc[
            df["plant_name_ferc1"].str.contains("total")
            & ~df["plant_name_ferc1"].str.contains("amounts are for the total"),
            "row_type",
        ] = "total"

        # This one gets overridden by notes: total solar operation/maintenance

        return df

    def label_row_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coordinate labeling of ``row_types`` as headers, notes, or totals.

        The small plants table is more like a digitized PDF than an actual data table.
        The rows contain all sorts of information in addition to what the columns might
        suggest. For instance, there are header rows, note rows, and total rows that
        contain useful information, but cause confusion in their current state, mixed in
        with the rest of the data.

        Here's an example of what you might find in the small plants table:

        +-------------------+------------+-----------------+
        | plant_name_ferc1  | plant_type | capacity_mw     |
        +===================+============+=================+
        | HYDRO:            | NA         | NA              |
        +-------------------+------------+-----------------+
        | rainbow falls (b) | NA         | 30              |
        +-------------------+------------+-----------------+
        | cadyville (a)     | NA         | 100             |
        +-------------------+------------+-----------------+
        | keuka (c)         | NA         | 80              |
        +-------------------+------------+-----------------+
        | total plants      | NA         | 310             |
        +-------------------+------------+-----------------+
        | (a) project #2738 | NA         | NA              |
        +-------------------+------------+-----------------+
        | (b) project #2835 | NA         | NA              |
        +-------------------+------------+-----------------+
        | (c) project #2852 | NA         | NA              |
        +-------------------+------------+-----------------+

        Notice how misleading it is to have all this infomration in one column. The
        goal of this function is to coordinate labeling functions so that we can
        identify which rows contain specific plant information and which rows are
        headers, notes, or totals.

        Once labeled, other functions can either remove rows that might cause double
        counting, extract useful plant or fuel type information from headers, and
        extract useful context or license id information from notes.

        Coordinates :func:`_label_header_rows`, :func:`_label_total_rows`,
        :func:`_label_note_rows`.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data that has been run through
            the :func:`_find_possible_header_or_note_rows` function and contains the
            column ``possible_header_or_note``.

        Returns:
            The same input DataFrame but with a column called ``row_type`` containg the
            strings ``header``, ``note``, ``total``, or NA to indicate what type of row
            it is.
        """
        # Add a column to show final row type
        df.insert(3, "row_type", np.nan)

        # Label the row types
        df_labeled = (
            df.pipe(self._find_possible_header_or_note_rows)
            .pipe(self._label_header_rows)
            .pipe(self._label_total_rows)
            .pipe(self._label_note_rows)
            .drop(columns=["possible_header_or_note"])
        )

        # Move total lables to a different column
        df_labeled.loc[df_labeled["row_type"] == "total", "is_total"] = True
        df_labeled["is_total"] = df_labeled.filter(["row_type"]).isin(["total"]).all(1)

        return df_labeled

    def prep_header_fuel_and_plant_types(
        self, df: pd.DataFrame, show_unmapped_headers=False
    ) -> pd.DataFrame:
        """Forward fill header rows to prep for fuel and plant type extraction.

        The headers we've identified in :func:`_label_header_rows` can be used to
        supplement the values in the ``plant_type`` and ``fuel_type`` columns.

        This function groups the data by utility, year, and header; extracts the header
        into a new column; and forward fills the headers so that each record in the
        header group is associated with that header. Because the headers map to
        different fuel types and plant types (ex: ``solar pv`` maps to fuel type
        ``solar`` and plant type ``photovoltaic``), the new forward-filled header column
        is duplicated and called ``fuel_type_from_header`` and
        ``plant_type_from_header``. In :func:`map_header_fuel_and_plant_types`, these
        columns will be mapped to their respective fuel and plant types, used
        to fill in blank values in the ``plant_type`` and ``fuel_type``, and then
        eventually removed.

        Why separate the prep step from the map step?

        We trust the values originally reported in the ``fuel_type`` and ``plant_type``
        columns more than the extracted and forward filled header values, so we only
        want to replace ``fuel_type`` and ``plant_type`` values that are labeled as
        ``pd.NA`` or ``other``. The values reported to those columns are extremely messy
        and must be cleaned via :func:`pudl.transform.classes.categorize_strings` in
        order for us to know which are truely ``pd.NA`` or ``other``. Because we also
        use :func:`pudl.transform.classes.categorize_strings` to map the headers to fuel
        and plant types, it makes sense to clean all four columns at once and then
        combine them.

        Here's a look at what this function does. It starts with the following table:

        +-------------------+------------+------------+----------+
        | plant_name_ferc1  | plant_type | fuel_type  | row_type |
        +===================+============+============+==========+
        | HYDRO:            | NA         | NA         | header   |
        +-------------------+------------+------------+----------+
        | rainbow falls (b) | NA         | NA         | NA       |
        +-------------------+------------+------------+----------+
        | cadyville (a)     | NA         | NA         | NA       |
        +-------------------+------------+------------+----------+
        | keuka (c)         | NA         | NA         | NA       |
        +-------------------+------------+------------+----------+
        | Wind Turbines:    | NA         | NA         | header   |
        +-------------------+------------+------------+----------+
        | sunny grove       | NA         | NA         | NA       |
        +-------------------+------------+------------+----------+
        | green park wind   | NA         | wind       | NA       |
        +-------------------+------------+------------+----------+

        And ends with this:

        +-------------------+---------+---------+----------------+--------------------+
        | plant_name_ferc1  | plant   | fuel    | plant_type     | fuel_type          |
        |                   | _type   | _type   | _from_header   | _from_header       |
        +===================+=========+=========+================+====================+
        | HYDRO:            | NA      | NA      | HYDRO:         | HYDRO:             |
        +-------------------+---------+---------+----------------+--------------------+
        | rainbow falls (b) | NA      | NA      | HYDRO:         | HYDRO:             |
        +-------------------+---------+---------+----------------+--------------------+
        | cadyville (a)     | NA      | NA      | HYDRO:         | HYDRO:             |
        +-------------------+---------+---------+----------------+--------------------+
        | keuka (c)         | NA      | NA      | HYDRO:         | HYDRO:             |
        +-------------------+---------+---------+----------------+--------------------+
        | Wind Turbines:    | NA      | NA      | Wind Turbines: | Wind Turbines:     |
        +-------------------+---------+---------+----------------+--------------------+
        | sunny grove       | NA      | NA      | Wind Turbines: | Wind Turbines:     |
        +-------------------+---------+---------+----------------+--------------------+
        | green park wind   | NA      | wind    | Wind Turbines: | Wind Turbines:     |
        +-------------------+---------+---------+----------------+--------------------+

        NOTE: If a utility's ``plant_name_ferc1`` values look like this: ``["STEAM",
        "coal_plant1", "coal_plant2", "wind_turbine1"]``, then this algorythem will
        think that last wind turbine is a steam plant. Luckily, when a utility embeds
        headers in the data it usually includes them for all plant types: ``["STEAM",
        "coal_plant1", "coal_plant2", "WIND", "wind_turbine"]``.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data that has been run through
            :func:`_label_row_type` and contains the columns ``row_type``.

        Returns:
            The same input DataFrame but with new columns ``plant_type_from_header``
            and ``fuel_type_from_header`` that forward fill the values in the header
            rows by utility, year, and header group.
        """
        logger.info(
            f"{self.table_id.value}: Forward filling header fuel and plant types"
        )

        # Create a column of just headers
        df.loc[df["row_type"] == "header", "header"] = df["plant_name_ferc1"]

        # Make groups based on utility, year, and header.
        # The .cumsum() creates a new series with values that go up from 1 whenever
        # there is a new header. So imagine row_type["header", NA, NA, "header", NA].
        # this creates a series of [1,1,1,2,2] so that the data can be grouped by
        # header.
        header_groups = df.groupby(
            [
                "utility_id_ferc1",
                "report_year",
                (df["row_type"] == "header").cumsum(),
            ]
        )
        # Forward fill based on headers
        df.loc[df["row_type"] != "note", "header"] = header_groups.header.ffill()

        # Create temporary columns for plant type and fuel type
        df["plant_type_from_header"] = df["header"]
        df["fuel_type_from_header"] = df["header"]
        df = df.drop(columns=["header"])

        return df

    def map_header_fuel_and_plant_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill ``pd.NA`` and ``other`` plant and fuel types with cleaned headers.

        :func:`prep_header_fuel_and_plant_types` extracted and forward filled the header
        values; :func:`pudl.transform.params.categorize_strings` cleaned them according
        to both the fuel and plant type parameters. This function combines the
        ``fuel_type_from_header`` with ``fuel_type`` and ``plant_type_from_header`` with
        ``plant_type`` when the reported, cleaned values are ``pd.NA`` or ``other``.

        To understand more about why these steps are necessary read the docstrings for
        :func:`prep_header_fuel_and_plant_types`.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data that has been run through
            :func:`prep_header_fuel_and_plant_types` and contains the columns
            ``fuel_type_from_header`` and ``plant_type_from_header``.

        Returns:
            The same input DataFrame but with rows with ``pd.NA`` or ``other`` in the
            ``fuel_type`` and ``plant_type`` columns filled in with the respective
            values from ``fuel_type_from_header`` and ``plant_type_from_header`` when
            available. ``fuel_type_from_header`` and ``plant_type_from_header`` columns
            removed.
        """
        logger.info(
            f"{self.table_id.value}: Filling NA and 'other' fuel and plant types with"
            " header info"
        )

        # Stash the amount of NA values to check that the filling worked.
        old_fuel_type_count = len(
            df[~df["fuel_type"].isin([pd.NA, "other"]) & df["row_type"].isna()]
        )
        old_plant_type_count = len(
            df[~df["plant_type"].isin([pd.NA, "other"]) & df["row_type"].isna()]
        )

        # Fill NA and "other" fields
        df.loc[
            df["plant_type"].isin([pd.NA, "other"]), "plant_type"
        ] = df.plant_type_from_header
        df.loc[
            df["fuel_type"].isin([pd.NA, "other"]), "fuel_type"
        ] = df.fuel_type_from_header

        # Remove _from_header fields
        df = df.drop(columns=["plant_type_from_header", "fuel_type_from_header"])

        # Check that this worked!
        new_fuel_type_count = len(
            df[~df["fuel_type"].isin([pd.NA, "other"]) & df["row_type"].isna()]
        )
        new_plant_type_count = len(
            df[~df["plant_type"].isin([pd.NA, "other"]) & df["row_type"].isna()]
        )

        if not old_fuel_type_count < new_fuel_type_count:
            raise AssertionError("No header fuel types added when there should be")
        if not old_plant_type_count < new_plant_type_count:
            raise AssertionError("No header plant types added when there should be")

        useful_rows_len = len(df[df["row_type"].isna()])

        logger.info(
            f"Added fuel types to {new_fuel_type_count-old_fuel_type_count} plant rows "
            f"({round((new_fuel_type_count-old_fuel_type_count)/useful_rows_len*100)}%). "
            f"Added plant types to {new_plant_type_count-old_plant_type_count} plant "
            f"rows ({round((new_plant_type_count-old_plant_type_count)/useful_rows_len*100)}%)."
        )

        return df

    def map_plant_name_fuel_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Suppliment ``fuel_type`` with information in ``plant_name_ferc1``.

        Sometimes fuel type is embedded in a plant name (not just headers). In this case
        we can identify that what that fuel is from the name and fill in empty
        ``fuel_type`` values. Right now, this only works for hydro plants because the
        rest are complicated and have a slew of exceptions. This could probably be
        applied to the ``plant_type`` column in the future too.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            The same input DataFrame but with rows with ``other`` in the
            ``fuel_type`` column filled in notable fuel types extracted from the the
            ``plant_name_ferc1`` column.
        """
        logger.info(f"{self.table_id.value}: Getting fuel type (hydro) from plant name")
        df.loc[
            (
                df["plant_name_ferc1"].str.contains("hydro")
                & (df["fuel_type"] == "other")
            ),
            "fuel_type",
        ] = "hydro"

        return df

    def associate_notes_with_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Use footnote indicators to map notes and FERC licenses to plant rows.

        There are many utilities that report a bunch of mostly empty note rows at the
        bottom of their yearly entry. These notes often pertain to specific plant rows
        above. Sometimes the notes and their respective plant rows are linked by a
        common footnote indicator such as (a) or (1) etc.

        This function takes this:

        +-------------------+------------+------------------+
        | plant_name_ferc1  | row_type   | license_id_ferc1 |
        +===================+============+==================+
        | HYDRO:            | header     | NA               |
        +-------------------+------------+------------------+
        | rainbow falls (b) | NA         | NA               |
        +-------------------+------------+------------------+
        | cadyville (a)     | NA         | NA               |
        +-------------------+------------+------------------+
        | keuka (c)         | NA         | NA               |
        +-------------------+------------+------------------+
        | total plants      | total      | NA               |
        +-------------------+------------+------------------+
        | (a) project #2738 | note       | 2738             |
        +-------------------+------------+------------------+
        | (b) project #2835 | note       | 2738             |
        +-------------------+------------+------------------+
        | (c) project #2852 | note       | 2738             |
        +-------------------+------------+------------------+

        Finds the note rows with footnote indicators, maps the content from the note row
        into a new note column that's associated with the value row, and maps any FERC
        license extracted from this note column to the ``license_id_ferc1`` column in
        the value row.

        +-------------------+------------+-------------------+------------------+
        | plant_name_ferc1  | row_type   | notes             | license_id_ferc1 |
        +===================+============+===================+==================+
        | HYDRO:            | header     | NA                | NA               |
        +-------------------+------------+-------------------+------------------+
        | rainbow falls (b) | NA         | (b) project #2835 | 2835             |
        +-------------------+------------+-------------------+------------------+
        | cadyville (a)     | NA         | (a) project #2738 | 2738             |
        +-------------------+------------+-------------------+------------------+
        | keuka (c)         | NA         | (c) project #2852 | 2752             |
        +-------------------+------------+-------------------+------------------+
        | total plants      | total      | NA                | NA               |
        +-------------------+------------+-------------------+------------------+
        | (a) project #2738 | note       | NA                | 2738             |
        +-------------------+------------+-------------------+------------------+
        | (b) project #2835 | note       | NA                | 2835             |
        +-------------------+------------+-------------------+------------------+
        | (c) project #2852 | note       | NA                | 2752             |
        +-------------------+------------+-------------------+------------------+

        (Header and note rows are removed later).

        NOTE: Note rows that don't have a footnote indicator or note rows with a
        footnote indicator that don't have a cooresponding plant row with the same
        indicator are not captured. They will ultimately get removed and their content
        will not be preserved.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data that has been run through
            :func:`label_row_types` and contains the column ``row_type``.

        Returns:
            The same input DataFrame but with a column called ``notes`` that contains
            notes, reported below, in the same row as the plant values they pertain to.
            Also, any further additions to the ``license_id_ferc1`` field as extracted
            from these newly associated notes.
        """
        logger.info(
            f"{self.table_id.value}: Mapping notes and ferc license from notes rows"
        )

        def associate_notes_with_values_group(group):
            """Map footnotes within a given utility year group.

            Because different utilities may use the same footnotes or the same utility
            could reuse footnotes each year, we must do the footnote association within
            utility-year groups.
            """
            regular_row = group["row_type"].isna()
            has_note = group["row_type"] == "note"

            # Shorten execution time by only looking at groups with discernable
            # footnotes
            if group.footnote.any():
                # Make a df that combines notes and ferc license with the same footnote
                footnote_df = (
                    group[has_note]
                    .groupby("footnote")
                    .agg({"plant_name_ferc1": ", ".join, "license_id_ferc1": "first"})
                    .rename(columns={"plant_name_ferc1": "notes"})
                )

                # Map these new license and note values onto the original df
                updated_ferc_license_col = group.footnote.map(
                    footnote_df["license_id_ferc1"]
                )
                notes_col = group.footnote.map(footnote_df["notes"])
                # We update the ferc lic col because some were already there from the
                # plant name extraction. However, we want to override with the notes
                # ferc licenses because they are more likely to be accurate.
                group.license_id_ferc1.update(updated_ferc_license_col)
                group.loc[regular_row, "notes"] = notes_col

            return group

        footnote_pattern = r"(\(\d?[a-z]?[A-Z]?\))"
        df["notes"] = pd.NA
        # Create new footnote column
        df.loc[:, "footnote"] = df.plant_name_ferc1.str.extract(
            footnote_pattern, expand=False
        )
        # Group by year and utility and run footnote association
        groups = df.groupby(["report_year", "utility_id_ferc1"])
        sg_notes = groups.apply(lambda x: associate_notes_with_values_group(x))
        # Remove footnote column now that rows are associated
        sg_notes = sg_notes.drop(columns=["footnote"])

        notes_added = len(
            sg_notes[sg_notes["notes"].notna() & sg_notes["row_type"].isna()]
        )
        logger.info(f"Mapped {notes_added} notes to plant rows.")

        return sg_notes

    def spot_fix_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix one-off row errors.

        In 2004, utility_id_ferc1 251 reports clumps of units together. Each unit clump
        looks something like this: ``intrepid wind farm (107 units @ 1.5 mw each)`` and
        is followed by a row that looks like this: ``(amounts are for the total of all
        107 units)``. For the most part, these rows are useless note rows. However,
        there is one instance where important values are reported in this note row
        rather than in the actual plant row above.

        There are probably plenty of other spot fixes one could add here.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            The same input DataFrame but with some spot fixes corrected.
        """
        logger.info(f"{self.table_id.value}: Spot fixing some rows")
        # Define rows and columns to change
        cols_to_change = df.select_dtypes(include=np.number).columns.tolist() + [
            "row_type"
        ]
        row_with_info = (df["report_year"] == 2004) & (
            df["plant_name_ferc1"] == "(amounts are for the total of all 107 units)"
        )
        row_missing_info = (df["report_year"] == 2004) & (
            df["plant_name_ferc1"] == "intrepid wind farm (107 units @ 1.5 mw each)"
        )

        # Replace row missing information with data from row containing information
        df.loc[row_missing_info, cols_to_change] = df[row_with_info][
            cols_to_change
        ].values

        # Remove row_with_info so there is no duplicate information
        df = df[~row_with_info]

        return df


class TransmissionStatisticsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer for the :ref:`transmission_statistics_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.TRANSMISSION_STATISTICS_FERC1
    has_unique_record_ids: bool = False


class ElectricEnergySourcesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`electric_energy_sources_ferc1` table.

    The raw DBF and XBRL table will be split up into two tables. This transformer
    generates the sources of electricity for utilities, dropping the information about
    dispositions. For XBRL, this is a duration-only table. Right now we are merging in
    the metadata but not actually keeping anything from it. We are also not yet doing
    anything with the sign.
    """

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_ENERGY_SOURCES_FERC1
    has_unique_record_ids: bool = False


class ElectricEnergyDispositionsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`electric_energy_dispositions_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_ENERGY_DISPOSITIONS_FERC1
    has_unique_record_ids: bool = False


class UtilityPlantSummaryFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`utility_plant_summary_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.UTILITY_PLANT_SUMMARY_FERC1
    has_unique_record_ids: bool = False


class BalanceSheetLiabilitiesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`balance_sheet_liabilities_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.BALANCE_SHEET_LIABILITIES
    has_unique_record_ids: bool = False


class BalanceSheetAssetsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`balance_sheet_assets_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.BALANCE_SHEET_ASSETS_FERC1
    has_unique_record_ids: bool = False


class IncomeStatementFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for the :ref:`income_statement_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.INCOME_STATEMENT_FERC1
    has_unique_record_ids: bool = False

    def process_dbf(self, raw_dbf):
        """Drop incorrect row numbers from f1_incm_stmnt_2 before standard processing.

        In 2003, two rows were added to the ``f1_income_stmnt`` dbf table, which bumped
        the starting ``row_number`` of ``f1_incm_stmnt_2`` from 25 to 27. A small
        handfull of respondents seem to have not gotten the memo about this this in
        2003 and have information on these row numbers that shouldn't exist at all for
        this table.

        This step necessitates the ability to know which source table each record
        actually comes from, which required adding a column (``sched_table_name``) in
        the extract step before these two dbf input tables were concatenated.

        Right now we are just dropping these bad row numbers. Should we actually be
        bumping the whole respondent's row numbers - assuming they reported incorrectly
        for the whole table? See: https://github.com/catalyst-cooperative/pudl/issues/471
        """
        len_og = len(raw_dbf)
        known_bad_income2_rows = [25, 26]
        raw_dbf = raw_dbf[
            ~(
                (raw_dbf.sched_table_name == "f1_incm_stmnt_2")
                & (raw_dbf.report_year == 2003)
                & (raw_dbf.row_number.isin(known_bad_income2_rows))
            )
        ].copy()
        logger.info(
            f"Dropped {len_og - len(raw_dbf)} records ({(len_og - len(raw_dbf))/len_og:.1%} of"
            "total) records from 2003 from the f1_incm_stmnt_2 DBF table that have "
            "known incorrect row numbers."
        )
        raw_dbf = super().process_dbf(raw_dbf)
        return raw_dbf


class RetainedEarningsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`retained_earnings_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.RETAINED_EARNINGS_FERC1
    has_unique_record_ids: bool = False

    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Preform generic :meth:`process_dbf`, plus deal with duplicates.

        Along with the standard processing in
        :meth:`Ferc1AbstractTableTransformer.process_dbf`, this method runs:
        * :meth:`targeted_drop_duplicates_dbf`
        * :meth:`condense_double_year_earnings_types_dbf`
        """
        processed_dbf = (
            super()
            .process_dbf(raw_dbf)
            .pipe(self.targeted_drop_duplicates_dbf)
            .pipe(self.condense_double_year_earnings_types_dbf)
        )
        return processed_dbf

    def targeted_drop_duplicates_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop duplicates with truly duplicate data.

        There are instances of utilities that reported multiple values for several
        earnings types for a specific year (utility_id_ferc1 68 in 1998 &
        utility_id_ferc1 296 in 2015). We are taking the largest value reported and
        dropping the rest. There very well could be a better strategey here, but there
        are only 25 records that have this problem, so we've going with this.
        """
        pks = (
            pudl.metadata.classes.Package.from_resource_ids()
            .get_resource(self.table_id.value)
            .schema.primary_key
        )
        # we are not going to check all of the unstructed earnings types for dupes bc
        # we will drop these later
        dupe_mask = ~df.earnings_type.str.endswith("_unstructured") & df.duplicated(
            subset=pks, keep=False
        )
        dupes = df[dupe_mask]
        if len(dupes) > 25:
            raise AssertionError(
                f"{self.table_id.value}: Too many duplicates found ({len(dupes)}). "
                "Expected 25 or less."
            )
        # we are simply sorting to get the biggest value and dropping the rest.
        dupes = dupes.sort_values(
            ["starting_balance", "amount"], ascending=False
        ).drop_duplicates(subset=pks)
        df = pd.concat([df[~dupe_mask], dupes])
        return df

    def condense_double_year_earnings_types_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Condense current and past year data reported in 1 report_year into 1 record.

        The DBF table includes two different earnings types that have: "Begining of
        Period" and "End of Period" rows. But the table has both an amount column that
        corresponds to a balance and a starting balance column. For these two earnings
        types, this means that there is in effect two years of data in this table for
        each report year: a starting and ending balance for the pervious year and a
        starting and ending balance for the current year. The ending balance for the
        previous year should be the same as the starting balance for the current year.

        We don't actually want two years of data for each report year, so we want to
        check these assumptions, extract as much information from these two years of
        data, but end up with only one annual record for each of these two earnings
        types for each utility.

        Raises:
            AssertionError: There are a very small number of instances in which the
                ending balance from the previous year does not match the starting
                balance from the current year. The % of these non-matching instances
                should be less than 2% of the records with these date duplicative
                earnings types.
        """
        logger.info(f"{self.table_id.value}: Removing previous year's data.")
        current_year_types = [
            "unappropriated_undistributed_subsidiary_earnings_current_year",
            "unappropriated_retained_earnings_current_year",
        ]
        previous_year_types = [
            "unappropriated_undistributed_subsidiary_earnings_previous_year",
            "unappropriated_retained_earnings_previous_year",
        ]
        # assign copies so no need to double copy when extracting this slice
        current_year = df[df.earnings_type.isin(current_year_types)].assign(
            earnings_type=lambda x: x.earnings_type.str.removesuffix("_current_year")
        )
        previous_year = df[df.earnings_type.isin(previous_year_types)].assign(
            earnings_type=lambda x: x.earnings_type.str.removesuffix("_previous_year")
        )
        idx = [
            "utility_id_ferc1_dbf",
            "report_year",
            "utility_id_ferc1",
            "earnings_type",
        ]
        data_columns = ["amount", "starting_balance"]
        date_dupe_types = pd.merge(
            current_year,
            previous_year[idx + data_columns],
            on=idx,
            how="outer",
            suffixes=("", "_previous_year"),
        )

        date_dupe_types.loc[:, "ending_balance"] = pd.NA
        # check if the starting balance from the current year is actually
        # the amount from the previous year
        date_mismatch = date_dupe_types[
            ~np.isclose(
                date_dupe_types.starting_balance,
                date_dupe_types.amount_previous_year,
                equal_nan=True,
            )
            & (date_dupe_types.starting_balance.notnull())
            & (date_dupe_types.amount_previous_year.notnull())
        ]
        data_mismatch_ratio = len(date_mismatch) / len(date_dupe_types)
        if data_mismatch_ratio > 0.02:
            raise AssertionError(
                "More records than expected have data that is not the same in "
                "the starting_balance vs the amount column for the earnings_type "
                "that reports both current and previous year. % of mismatch records: "
                f"{data_mismatch_ratio:.01%} (expected less than 1%)"
            )

        # the amount from the current year values should be the ending balance.
        # the amount from the previous year should fill in the starting balance
        # then drop all of the _previous_year columns
        date_dupe_types = date_dupe_types.assign(
            ending_balance=lambda x: x.amount,
            amount=pd.NA,
            starting_balance=lambda x: x.starting_balance.fillna(
                x.amount_previous_year
            ),
        ).drop(columns=["amount_previous_year", "starting_balance_previous_year"])

        df = pd.concat(
            [
                df[~df.earnings_type.isin(current_year_types + previous_year_types)],
                date_dupe_types,
            ]
        )
        return df

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(self, xbrl_metadata_json) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Run the generic :func:`process_xbrl_metadata` and then remove some suffixes that
        were removed during :meth:`wide_to_tidy`.
        """
        meta = (
            super()
            .process_xbrl_metadata(xbrl_metadata_json)
            .assign(
                # there are many instances of factiods with these stems cooresponding
                # to several value types (amount/start or end balance). but we end up
                # with only one so we want to drop these stems and then drop dupes
                # plus there is one suffix that is named weird!
                xbrl_factoid=lambda x: x.xbrl_factoid.str.removesuffix(
                    "_contra_primary_account_affected"
                ).str.removesuffix("_primary_contra_account_affected")
            )
            .drop_duplicates(subset=["xbrl_factoid"], keep="first")
        )
        return meta


class DepreciationAmortizationSummaryFerc1TableTransformer(
    Ferc1AbstractTableTransformer
):
    """Transformer class for :ref:`depreciation_amortization_summary_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.DEPRECIATION_AMORTIZATION_SUMMARY_FERC1
    has_unique_record_ids: bool = False

    def merge_xbrl_metadata(
        self, df: pd.DataFrame, params: MergeXbrlMetadata | None = None
    ) -> pd.DataFrame:
        """Annotate data using manually compiled FERC accounts & ``ferc_account_label``.

        The FERC accounts are not provided in the XBRL taxonomy like they are for other
        tables. However, there are only 4 of them, so a hand-compiled mapping is
        hardcoded here, and merged onto the data similar to how the XBRL taxonomy
        derived metadata is merged on for other tables.
        """
        xbrl_metadata = pd.DataFrame(
            {
                "ferc_account_label": [
                    "depreciation_expense",
                    "depreciation_expense_asset_retirement",
                    "amortization_limited_term_electric_plant",
                    "amortization_other_electric_plant",
                ],
                "ferc_account": ["403", "403.1", "404", "405"],
            }
        )
        if not params:
            params = self.params.merge_xbrl_metadata
        if params.on:
            logger.info(f"{self.table_id.value}: merging metadata")
            df = merge_xbrl_metadata(df, xbrl_metadata, params)
        return df


class ElectricPlantDepreciationChangesFerc1TableTransformer(
    Ferc1AbstractTableTransformer
):
    """Transformer class for :ref:`electric_plant_depreciation_changes_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_PLANT_DEPRECIATION_CHANGES_FERC1
    has_unique_record_ids: bool = False

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(self, xbrl_metadata_json) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Replace the name of the balance column reported in the XBRL Instant table with
        starting_balance / ending_balance since we pull those two values into their own
        separate labeled rows, each of which should get the original metadata for the
        Instant column.
        """
        meta = (
            super()
            .process_xbrl_metadata(xbrl_metadata_json)
            .assign(
                xbrl_factoid=lambda x: x.xbrl_factoid.replace(
                    {
                        "accumulated_provision_for_depreciation_of_electric_utility_plant": "starting_balance"
                    }
                )
            )
        )
        ending_balance = meta[meta.xbrl_factoid == "starting_balance"].assign(
            xbrl_factoid="ending_balance"
        )
        return pd.concat([meta, ending_balance])

    @cache_df("dbf")
    def process_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Accumulated Depreciation table specific DBF cleaning operations.

        The XBRL reports a utility_type which is always electric in this table, but
        which may be necessary for differentiating between different values when this
        data is combined with other tables. The DBF data doesn't report this value so
        we are adding it here for consistency across the two data sources.

        Also rename the ``ending_balance_accounts`` to ``ending_balance``
        """
        df = super().process_dbf(df).assign(utility_type="electric")
        df.loc[
            df["depreciation_type"] == "ending_balance_accounts", "depreciation_type"
        ] = "ending_balance"
        return df

    @cache_df("process_instant_xbrl")
    def process_instant_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make the instant and duration tables compatible.

        This table has a rename that needs to take place in an unusual spot -- after the
        starting / ending balances have been usntacked, but before the instant &
        duration tables are merged. This method just reversed the order in which these
        operations happen, comapared to the inherited method.
        """
        df = self.unstack_balances_to_report_year_instant_xbrl(df).pipe(
            self.rename_columns, rename_stage="instant_xbrl"
        )
        return df


class ElectricPlantDepreciationFunctionalFerc1TableTransformer(
    Ferc1AbstractTableTransformer
):
    """Transformer for :ref:`electric_plant_depreciation_functional_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_PLANT_DEPRECIATION_FUNCTIONAL_FERC1
    has_unique_record_ids: bool = False

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(self, xbrl_metadata_json) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Transform the xbrl factoid values so that they match the final plant functional
        classification categories and can be merged with the output dataframe.
        """
        df = (
            super()
            .process_xbrl_metadata(xbrl_metadata_json)
            .assign(
                xbrl_factoid=lambda x: x.xbrl_factoid.str.replace(
                    r"^accumulated_depreciation_", "", regex=True
                ),
            )
        )
        df.loc[
            df.xbrl_factoid
            == "accumulated_provision_for_depreciation_of_electric_utility_plant",
            "xbrl_factoid",
        ] = "total"
        return df

    @cache_df("dbf")
    def process_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Accumulated Depreciation table specific DBF cleaning operations.

        The XBRL reports a utility_type which is always electric in this table, but
        which may be necessary for differentiating between different values when this
        data is combined with other tables. The DBF data doesn't report this value so we
        are adding it here for consistency across the two data sources.
        """
        return super().process_dbf(df).assign(utility_type="electric")

    @cache_df("process_instant_xbrl")
    def process_instant_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make the instant and duration tables compatible.

        This table has a rename that needs to take place in an unusual spot -- after the
        starting / ending balances have been usntacked, but before the instant &
        duration tables are merged. This method reverses the order in which these
        operations happen comapared to the inherited method. We also want to strip the
        ``accumulated_depreciation`` that appears on every plant functional class.
        """
        df = self.unstack_balances_to_report_year_instant_xbrl(df).pipe(
            self.rename_columns, rename_stage="instant_xbrl"
        )
        return df


class ElectricOperatingExpensesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`electric_operating_expenses_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_OPERATING_EXPENSES_FERC1
    has_unique_record_ids: bool = False

    def targeted_drop_duplicates_dbf(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Drop incorrect duplicate from 2002.

        In 2002, utility_id_ferc1_dbf 96 reported two values for
        administrative_and_general_operation_expense. I found the correct value by
        looking at the prev_yr_amt value in 2003. This removes the incorrect row.
        """
        start_len = len(raw_df)
        raw_df = raw_df[
            ~((raw_df["report_year"] == 2002) & (raw_df["crnt_yr_amt"] == 35990321))
        ]
        if (dropped := start_len - len(raw_df)) > 1:
            raise AssertionError(f"More rows dropped than expected: {dropped}")
        logger.info("Heyyyy dropping that one row")
        return raw_df

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Process DBF but drop a bad row that is flagged by drop_duplicates."""
        return super().process_dbf(self.targeted_drop_duplicates_dbf(raw_dbf))


class ElectricOperatingRevenuesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`electric_operating_revenues_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_OPERATING_REVENUES_FERC1
    has_unique_record_ids: bool = False

    @cache_df("main")
    def transform_main(self, df):
        """Add duplicate removal after standard transform_main."""
        return super().transform_main(df).pipe(self.targeted_drop_duplicates)

    @cache_df("main")
    def targeted_drop_duplicates(self, df):
        """Drop one duplicate records from 2011, utility_id_ferc1 295."""
        dupe_mask = (
            (df.utility_id_ferc1 == 295)
            & (df.report_year == 2011)
            & ((df.amount == 3.33e8) | (df.amount == 3.333e9))
        )

        return df[~dupe_mask].copy()


class CashFlowFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transform class for :ref:`cash_flow_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.CASH_FLOW_FERC1
    has_unique_record_ids: bool = False

    @cache_df("process_instant_xbrl")
    def process_instant_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make the instant and duration tables compatible.

        This table has a rename that needs to take place in an unusual spot -- after the
        starting / ending balances have been usntacked, but before the instant &
        duration tables are merged. This method just reversed the order in which these
        operations happen, comapared to the inherited method.
        """
        df = self.unstack_balances_to_report_year_instant_xbrl(df).pipe(
            self.rename_columns, rename_stage="instant_xbrl"
        )
        return df

    @cache_df("main")
    def transform_main(self, df):
        """Add duplicate removal and validation after standard transform_main."""
        return (
            super()
            .transform_main(df)
            .pipe(self.targeted_drop_duplicates)
            .pipe(self.validate_start_end_balance)
        )

    @cache_df("main")
    def targeted_drop_duplicates(self, df):
        """Drop one duplicate record from 2020, utility_id_ferc1 2037.

        Note: This step could be avoided if we employed a :meth:`drop_invalid_rows`
        transform step with ``required_valid_cols = ["amount"]``
        """
        dupe_mask = (
            (df.utility_id_ferc1 == 237)
            & (df.report_year == 2020)
            & (df.amount_type == "dividends_on_common_stock")
            & (df.amount.isnull())
        )
        if (len_dupes := dupe_mask.value_counts().loc[True]) != 1:
            raise ValueError(f"Expected to find 1 duplicate record. Found {len_dupes}")
        return df[~dupe_mask].copy()

    @cache_df("main")
    def validate_start_end_balance(self, df):
        """Validate of start balance + net = end balance.

        Add a quick check to ensure the vast majority of the ending balances are
        calculable from the net change + the starting balance = the ending balance.
        """
        # calculate ending balance
        df.amount = pd.to_numeric(df.amount)
        end_bal_calc = (
            df[
                df.amount_type.isin(
                    [
                        "starting_balance",
                        "net_increase_decrease_in_cash_and_cash_equivalents",
                    ]
                )
            ]
            .groupby(["utility_id_ferc1", "report_year"])[["amount"]]
            .sum(min_count=2, numeric_only=True)
            .add_suffix("_ending_balace")
        )
        # grab reported ending balance & squish with the calculated version
        end_bal = df[df.amount_type == "ending_balance"].set_index(
            ["utility_id_ferc1", "report_year"]
        )
        logger.info(end_bal_calc.columns)
        end_bal.loc[:, "amount_ending_balace"] = end_bal_calc.amount_ending_balace

        # when both exist, are they close?
        end_bal_off = end_bal[
            ~np.isclose(end_bal.amount, end_bal.amount_ending_balace)
            & end_bal[["amount", "amount_ending_balace"]].notnull().all(axis="columns")
        ]
        if (end_bal_off_ratio := len(end_bal_off) / len(end_bal)) > 0.005:
            raise ValueError(
                f"Ahhh!! The ending balance isn't calculable in {end_bal_off_ratio:.2%}"
                " of records. Expected under 0.5%."
            )
        return df

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(self, xbrl_metadata_json) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Replace the name of the balance column reported in the XBRL Instant table with
        starting_balance / ending_balance since we pull those two values into their own
        separate labeled rows, each of which should get the original metadata for the
        Instant column.
        """
        meta = (
            super()
            .process_xbrl_metadata(xbrl_metadata_json)
            .assign(
                xbrl_factoid=lambda x: x.xbrl_factoid.replace(
                    {"cash_and_cash_equivalents": "starting_balance"}
                )
            )
        )
        ending_balance = meta[meta.xbrl_factoid == "starting_balance"].assign(
            xbrl_factoid="ending_balance"
        )
        return pd.concat([meta, ending_balance])


class ElectricitySalesByRateScheduleFerc1TableTransformer(
    Ferc1AbstractTableTransformer
):
    """Transform class for :ref:`electricity_sales_by_rate_schedule_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRICITY_SALES_BY_RATE_SCHEDULE_FERC1
    has_unique_record_ids: bool = False

    def add_axis_to_total_table_rows(self, df: pd.DataFrame):
        """Add total to the axis column for rows from the total table.

        Because we're adding the
        sales_of_electricity_by_rate_schedules_account_totals_304 table into the mix,
        we have a bunch of total values that get mixed in with all the _billed columns
        from the individual tables. If left alone, these totals aren't labeled in any
        way becuse they don't have the same _axis columns explaining what each of the
        values are. In order to distinguish them from the rest of the sub-total data we
        use this function to create an _axis value for them noting that they are totals.

        It's worth noting that there are also some total values in there already.
        Those would be hard to clean. The idea is that if you want the actual totals,
        don't try and sum the sub-components, look at the actual labeled total rows.

        This function relies on the ``sched_table_name`` column, so it must be called
        before that gets dropped.

        Args:
            df: The sales table with a ``sched_table_name`` column.
        """
        logger.info(f"{self.table_id.value}: Labeling total values.")
        df.loc[
            df["sched_table_name"]
            == "sales_of_electricity_by_rate_schedules_account_totals_304",
            ["sales_axis", "rate_schedule_description"],
        ] = "total"
        return df

    @cache_df(key="xbrl")
    def process_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Rename columns before running wide_to_tidy."""
        logger.info(f"{self.table_id.value}: Processing XBRL data pre-concatenation.")
        return (
            self.merge_instant_and_duration_tables_xbrl(
                raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.rename_columns, rename_stage="xbrl")
            .pipe(self.combine_axis_columns_xbrl)
            .pipe(self.add_axis_to_total_table_rows)
            .pipe(self.wide_to_tidy, source_ferc1=SourceFerc1.XBRL)
            .pipe(self.assign_record_id, source_ferc1=SourceFerc1.XBRL)
            .pipe(self.assign_utility_id_ferc1, source_ferc1=SourceFerc1.XBRL)
        )


class OtherRegulatoryLiabilitiesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`other_regulatory_liabilities_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.OTHER_REGULATORY_LIABILITIES_FERC1
    has_unique_record_ids = False


def ferc1_transform_asset_factory(
    table_name: str,
    ferc1_tfr_classes: Mapping[str, type[Ferc1AbstractTableTransformer]],
    io_manager_key: str = "pudl_sqlite_io_manager",
    convert_dtypes: bool = True,
    generic: bool = False,
) -> AssetsDefinition:
    """Create an asset that pulls in raw ferc Form 1 assets and applies transformations.

    This is a convenient way to create assets for tables that only depend on raw dbf,
    raw xbrl instant and duration tables and xbrl metadata. For tables with additional
    upstream dependencies, create a stand alone asset using an asset decorator. See
    the plants_steam_ferc1 asset.

    Args:
        table_name: The name of the table to create an asset for.
        ferc1_tfr_classes: A dictionary of table names to the corresponding transformer class.
        io_manager_key: the dagster io_manager key to use. None defaults
            to the fs_io_manager.
        convert_dtypes: convert dtypes of transformed dataframes.
        generic: If using GenericPlantFerc1TableTransformer pass table_id to constructor.

    Return:
        An asset for the clean table.
    """
    ins: Mapping[str, AssetIn] = {}

    listify = lambda x: x if isinstance(x, list) else [x]  # noqa: E731
    dbf_tables = listify(TABLE_NAME_MAP_FERC1[table_name]["dbf"])
    xbrl_tables = listify(TABLE_NAME_MAP_FERC1[table_name]["xbrl"])

    ins = {f"raw_dbf__{tn}": AssetIn(tn) for tn in dbf_tables}
    ins |= {f"raw_xbrl_instant__{tn}": AssetIn(f"{tn}_instant") for tn in xbrl_tables}
    ins |= {f"raw_xbrl_duration__{tn}": AssetIn(f"{tn}_duration") for tn in xbrl_tables}
    ins["xbrl_metadata_json"] = AssetIn("xbrl_metadata_json")

    tfr_class = ferc1_tfr_classes[table_name]
    table_id = TableIdFerc1(table_name)

    @asset(name=table_name, ins=ins, io_manager_key=io_manager_key)
    def ferc1_transform_asset(**kwargs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Transform a FERC Form 1 table.

        Args:
            raw_dbf: raw dbf table.
            raw_xbrl_instant: raw XBRL instant table.
            raw_xbrl_duration: raw XBRL duration table.
            xbrl_metadata_json: XBRL metadata json for all tables.

        Returns:
            transformed FERC Form 1 table.
        """
        # TODO: split the key by __, then groupby, then concatenate
        xbrl_metadata_json = kwargs["xbrl_metadata_json"]
        if generic:
            transformer = tfr_class(
                xbrl_metadata_json=xbrl_metadata_json[table_name], table_id=table_id
            )
        else:
            transformer = tfr_class(xbrl_metadata_json=xbrl_metadata_json[table_name])

        raw_dbf = pd.concat(
            [df for key, df in kwargs.items() if key.startswith("raw_dbf__")]
        )
        raw_xbrl_instant = pd.concat(
            [df for key, df in kwargs.items() if key.startswith("raw_xbrl_instant__")]
        )
        raw_xbrl_duration = pd.concat(
            [df for key, df in kwargs.items() if key.startswith("raw_xbrl_duration__")]
        )
        df = transformer.transform(
            raw_dbf=raw_dbf,
            raw_xbrl_instant=raw_xbrl_instant,
            raw_xbrl_duration=raw_xbrl_duration,
        )
        if convert_dtypes:
            df = convert_cols_dtypes(df, data_source="ferc1")
        return df

    return ferc1_transform_asset


def create_ferc1_transform_assets() -> list[AssetsDefinition]:
    """Create a list of transformed FERC Form 1 assets.

    Returns:
        A list of AssetsDefinitions where each asset is a clean ferc form 1 table.
    """
    ferc1_tfr_classes = {
        "fuel_ferc1": FuelFerc1TableTransformer,
        "plants_small_ferc1": PlantsSmallFerc1TableTransformer,
        "plants_hydro_ferc1": PlantsHydroFerc1TableTransformer,
        "plant_in_service_ferc1": PlantInServiceFerc1TableTransformer,
        "plants_pumped_storage_ferc1": PlantsPumpedStorageFerc1TableTransformer,
        "transmission_statistics_ferc1": TransmissionStatisticsFerc1TableTransformer,
        "purchased_power_ferc1": PurchasedPowerFerc1TableTransformer,
        "electric_energy_sources_ferc1": ElectricEnergySourcesFerc1TableTransformer,
        "electric_energy_dispositions_ferc1": ElectricEnergyDispositionsFerc1TableTransformer,
        "utility_plant_summary_ferc1": UtilityPlantSummaryFerc1TableTransformer,
        "electric_operating_expenses_ferc1": ElectricOperatingExpensesFerc1TableTransformer,
        "balance_sheet_liabilities_ferc1": BalanceSheetLiabilitiesFerc1TableTransformer,
        "depreciation_amortization_summary_ferc1": DepreciationAmortizationSummaryFerc1TableTransformer,
        "balance_sheet_assets_ferc1": BalanceSheetAssetsFerc1TableTransformer,
        "income_statement_ferc1": IncomeStatementFerc1TableTransformer,
        "electric_plant_depreciation_changes_ferc1": ElectricPlantDepreciationChangesFerc1TableTransformer,
        "electric_plant_depreciation_functional_ferc1": ElectricPlantDepreciationFunctionalFerc1TableTransformer,
        "retained_earnings_ferc1": RetainedEarningsFerc1TableTransformer,
        "electric_operating_revenues_ferc1": ElectricOperatingRevenuesFerc1TableTransformer,
        "cash_flow_ferc1": CashFlowFerc1TableTransformer,
        "electricity_sales_by_rate_schedule_ferc1": ElectricitySalesByRateScheduleFerc1TableTransformer,
        "other_regulatory_liabilities_ferc1": OtherRegulatoryLiabilitiesFerc1TableTransformer,
    }

    assets = []
    for table_name in ferc1_tfr_classes:
        # Bespoke exception. fuel must come before steam b/c fuel proportions are used to
        # aid in FERC plant ID assignment.
        if table_name != "plants_steam_ferc1":
            assets.append(ferc1_transform_asset_factory(table_name, ferc1_tfr_classes))
    return assets


ferc1_assets = create_ferc1_transform_assets()


@asset(io_manager_key="pudl_sqlite_io_manager")
def plants_steam_ferc1(
    xbrl_metadata_json: dict[str, dict[str, list[dict[str, Any]]]],
    f1_steam: pd.DataFrame,
    steam_electric_generating_plant_statistics_large_plants_402_duration: pd.DataFrame,
    steam_electric_generating_plant_statistics_large_plants_402_instant: pd.DataFrame,
    fuel_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Create the clean plants_steam_ferc1 table.

    Args:
            xbrl_metadata_json: XBRL metadata json for all tables.
            f1_steam: Raw f1_steam table.
            steam_electric_generating_plant_statistics_large_plants_402_duration: raw XBRL duration table.
            steam_electric_generating_plant_statistics_large_plants_402_instant: raw XBRL instant table.
            fuel_ferc1: Transformed fuel_ferc1 table.

    Returns:
        Clean plants_steam_ferc1 table.
    """
    df = PlantsSteamFerc1TableTransformer(
        xbrl_metadata_json=xbrl_metadata_json["plants_steam_ferc1"]
    ).transform(
        raw_dbf=f1_steam,
        raw_xbrl_instant=steam_electric_generating_plant_statistics_large_plants_402_instant,
        raw_xbrl_duration=steam_electric_generating_plant_statistics_large_plants_402_duration,
        transformed_fuel=fuel_ferc1,
    )
    return convert_cols_dtypes(df, data_source="ferc1")
