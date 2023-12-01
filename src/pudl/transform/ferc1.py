"""Classes & functions to process FERC Form 1 data before loading into the PUDL DB.

Note that many of the classes/objects here inherit from/are instances of classes defined
in :mod:`pudl.transform.classes`. Their design and relationships to each other are
documented in that module.

See :mod:`pudl.transform.params.ferc1` for the values that parameterize many of these
transformations.
"""
import enum
import importlib.resources
import itertools
import json
import re
from abc import abstractmethod
from collections import namedtuple
from collections.abc import Mapping
from typing import Annotated, Any, Literal, Self

import numpy as np
import pandas as pd
import sqlalchemy as sa
from dagster import AssetIn, AssetsDefinition, asset
from pandas.core.groupby import DataFrameGroupBy
from pydantic import BaseModel, Field, field_validator

import pudl
from pudl.analysis.classify_plants_ferc1 import (
    plants_steam_assign_plant_ids,
    plants_steam_validate_ids,
)
from pudl.extract.ferc1 import TABLE_NAME_MAP_FERC1
from pudl.helpers import assert_cols_areclose, convert_cols_dtypes
from pudl.metadata.fields import apply_pudl_dtypes
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


@asset
def clean_xbrl_metadata_json(
    raw_xbrl_metadata_json: dict[str, dict[str, list[dict[str, Any]]]]
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Generate cleaned json xbrl metadata.

    For now, this only runs :func:`add_source_tables_to_xbrl_metadata`.
    """
    return add_source_tables_to_xbrl_metadata(raw_xbrl_metadata_json)


def add_source_tables_to_xbrl_metadata(
    raw_xbrl_metadata_json: dict[str, dict[str, list[dict[str, Any]]]]
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Add a ``source_tables`` field into metadata calculation components.

    When a particular component of a calculation does not originate from the table in
    which the calculated field is being reported, label the source table.
    """

    def all_fields_in_table(table_meta) -> list:
        """Compile a list of all of the fields reported in a table."""
        return [
            field["name"] for meta_list in table_meta.values() for field in meta_list
        ]

    def extract_tables_to_fields(xbrl_meta: dict) -> dict[str : list[str]]:
        """Compile a dictionary of table names (keys) to list of fields."""
        return {
            table_name: all_fields_in_table(table_meta)
            for table_name, table_meta in xbrl_meta.items()
        }

    def label_source_tables(calc_component: dict, tables_to_fields: str) -> dict:
        """Add a ``source_tables`` element to the calculation component."""
        calc_component["source_tables"] = [
            other_table_name
            for other_table_name, fields in tables_to_fields.items()
            if calc_component["name"] in fields
        ]
        # weirdly there are a number of nuclear xbrl_factoid calc components that seem
        # to have no source_tables.
        if not calc_component["source_tables"]:
            logger.debug(f"Found no source table for {calc_component['name']}.")
        return calc_component

    tables_to_fields = extract_tables_to_fields(raw_xbrl_metadata_json)
    # for each table loop through all of the calculations within each field
    for table_name, table_meta in raw_xbrl_metadata_json.items():
        for list_of_facts in table_meta.values():
            for xbrl_fact in list_of_facts:
                # all facts have ``calculations``, but they are empty lists when null
                for calc_component in xbrl_fact["calculations"]:
                    # does the calc component show up in the table? if not, add a label
                    if calc_component["name"] not in tables_to_fields[table_name]:
                        calc_component = label_source_tables(
                            calc_component, tables_to_fields
                        )
                    else:
                        calc_component["source_tables"] = [table_name]
    return raw_xbrl_metadata_json


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

    @property
    def rename_dicts_xbrl(self):
        """Compile all of the XBRL rename dictionaries into an ordered list."""
        # add the xbrl last bc it happens after the inst/dur renames
        return [self.duration_xbrl, self.instant_xbrl, self.xbrl]


class WideToTidy(TransformParams):
    """Parameters for converting a wide table to a tidy table with value types."""

    idx_cols: list[str] | None = None
    """List of column names to treat as the table index."""

    stacked_column_name: str | None = None
    """Name of column that will contain the stacked categories."""

    value_types: list[str] | None = None
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

    @property
    def value_types(self) -> list[str]:
        """Compile a list of all of the ``value_types`` from ``wide_to_tidy``."""
        # each wtt source could be either a list of WideToTidy or a WideToTidy.
        # so we need to drill in to either grab the value_types
        value_types = []
        for wide_to_tidy in [self.xbrl, self.dbf]:
            if isinstance(wide_to_tidy, WideToTidy):
                value_types.append(wide_to_tidy.value_types)
            elif isinstance(wide_to_tidy, list):
                for rly_wide_to_tidy in wide_to_tidy:
                    value_types.append(rly_wide_to_tidy.value_types)
        # remove None's & flatten/dedupe
        value_types = [v for v in value_types if v is not None]
        flattened_values = []
        for item in value_types:
            if isinstance(item, list):
                flattened_values += list(item)
            elif isinstance(item, str):
                flattened_values.append(item)
        return flattened_values


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
    pks = pudl.metadata.classes.Resource.from_id(
        params.table_name.value
    ).schema.primary_key
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
        row_map = (
            read_dbf_to_xbrl_map(dbf_table_names=params.dbf_table_names)
            .pipe(fill_dbf_to_xbrl_map)
            .drop(columns=["sched_table_name", "row_literal"])
        )
        if row_map.isnull().any(axis=None):
            raise ValueError(
                "Filled DBF-XBRL map contains NA values, which should never happen:"
                f"{row_map}"
            )

        df = pd.merge(df, row_map, on=["report_year", "row_number"], how="left")
        if df.xbrl_factoid.isna().any():
            raise ValueError(
                "Found null row labels after aligning DBF/XBRL rows.\n"
                f"{df[df.xbrl_factoid.isna()]}"
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
    if not params.unstack_balances_to_report_year:
        return df

    # report year always corresponds to the year of "date"
    unique_cols = set(primary_key_cols).union({"report_year"})
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

    ending_balances = df.assign(balance_type="ending_balance")
    starting_balances = df.assign(
        report_year=df.report_year + 1, balance_type="starting_balance"
    )
    all_balances = pd.concat([starting_balances, ending_balances])
    # for the first year, we expect no starting balances; for the last year, we expect no ending balances.
    first_last_year_stripped = all_balances.loc[
        lambda df: ~df.report_year.isin({df.report_year.min(), df.report_year.max()})
    ]
    unstacked_by_year = (
        first_last_year_stripped.drop(columns=["date"])
        .set_index(primary_key_cols + ["balance_type", "sched_table_name"])
        .unstack("balance_type")
    )
    # munge multi-index into flat index, separated by _
    unstacked_by_year.columns = [
        "_".join(items) for items in unstacked_by_year.columns.to_flat_index()
    ]
    return unstacked_by_year.reset_index()


class CombineAxisColumnsXbrl(TransformParams):
    """Parameters for :func:`combine_axis_columns_xbrl`."""

    axis_columns_to_combine: list | None = None
    """List of axis columns to combine."""

    new_axis_column_name: str | None = None
    """The name of the combined axis column -- must end with the suffix ``_axis``!."""

    @field_validator("new_axis_column_name")
    @classmethod
    def doesnt_end_with_axis(cls, v):
        """Ensure that new axis column ends in _axis."""
        if v is not None and not v.endswith("_axis"):
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


class IsCloseTolerance(TransformParams):
    """Info for testing a particular check."""

    isclose_rtol: Annotated[float, Field(ge=0.0)] = 1e-5
    """Relative tolerance to use in :func:`np.isclose` for determining equality."""

    isclose_atol: Annotated[float, Field(ge=0.0, le=0.01)] = 1e-8
    """Absolute tolerance to use in :func:`np.isclose` for determining equality."""


class CalculationIsCloseTolerance(TransformParams):
    """Calc params organized by check type."""

    error_frequency: IsCloseTolerance = IsCloseTolerance()
    relative_error_magnitude: IsCloseTolerance = IsCloseTolerance(isclose_atol=1e-3)
    null_calculated_value_frequency: IsCloseTolerance = IsCloseTolerance()
    absolute_error_magnitude: IsCloseTolerance = IsCloseTolerance()
    null_reported_value_frequency: IsCloseTolerance = IsCloseTolerance()


class MetricTolerances(TransformParams):
    """Tolerances for all data checks to be preformed within a grouped df."""

    error_frequency: Annotated[float, Field(ge=0.0, le=1.0)] = 0.01
    relative_error_magnitude: Annotated[float, Field(ge=0.0)] = 0.02
    null_calculated_value_frequency: Annotated[float, Field(ge=0.0, le=1.0)] = 0.7
    """Fraction of records with non-null reported values and null calculated values."""
    absolute_error_magnitude: Annotated[float, Field(ge=0.0)] = np.inf
    null_reported_value_frequency: Annotated[float, Field(ge=0.0, le=1.0)] = 1.0
    # ooof this one is just bad


class GroupMetricTolerances(TransformParams):
    """Data quality expectations related to FERC 1 calculations.

    We are doing a lot of comparisons between calculated and reported values to identify
    reporting errors in the data, errors in FERC's metadata, and bugs in our own code.
    This class provides a structure for encoding our expectations about the level of
    acceptable (or at least expected) errors, and allows us to pass them around.

    In the future we might also want to specify much more granular expectations,
    pertaining to individual tables, years, utilities, or facts to ensure that we don't
    have low overall error rates, but a problem with the way the data or metadata is
    reported in a particular year.  We could also define per-filing and per-table error
    tolerances to help us identify individual utilities that have e.g. used an outdated
    version of Form 1 when filing.
    """

    ungrouped: MetricTolerances = MetricTolerances(
        error_frequency=0.0005,
        relative_error_magnitude=0.0086,
        null_calculated_value_frequency=0.50,
        null_reported_value_frequency=0.68,
    )
    xbrl_factoid: MetricTolerances = MetricTolerances(
        error_frequency=0.018,
        relative_error_magnitude=0.0086,
        null_calculated_value_frequency=1.0,
    )
    utility_id_ferc1: MetricTolerances = MetricTolerances(
        error_frequency=0.038,
        relative_error_magnitude=0.04,
        null_calculated_value_frequency=1.0,
    )
    report_year: MetricTolerances = MetricTolerances(
        error_frequency=0.006,
        relative_error_magnitude=0.04,
        null_calculated_value_frequency=0.7,
    )
    table_name: MetricTolerances = MetricTolerances(
        error_frequency=0.0005,
        relative_error_magnitude=0.001,
        null_calculated_value_frequency=0.50,
        null_reported_value_frequency=0.68,
    )


class GroupMetricChecks(TransformParams):
    """Input for checking calculations organized by group and test."""

    groups_to_check: list[
        Literal[
            "ungrouped", "table_name", "xbrl_factoid", "utility_id_ferc1", "report_year"
        ]
    ] = [
        "ungrouped",
        "report_year",
        "xbrl_factoid",
        "utility_id_ferc1",
    ]
    metrics_to_check: list[str] = [
        "error_frequency",
        "relative_error_magnitude",
        "null_calculated_value_frequency",
        "null_reported_value_frequency",
    ]
    group_metric_tolerances: GroupMetricTolerances = GroupMetricTolerances()
    is_close_tolerance: CalculationIsCloseTolerance = CalculationIsCloseTolerance()

    # TODO: The mechanics of this validation are a pain, given the bajillion combos
    # of tolerances we have in the matrix of checks. It works, but actually specifying
    # all of the relative values is not currently ergonomic, so it is disabled for the
    # moment.
    # @model_validator(mode="after")
    def grouped_tol_ge_ungrouped_tol(self: Self):
        """Grouped tolerance should always be greater than or equal to ungrouped."""
        for group in self.groups_to_check:
            metric_tolerances = self.group_metric_tolerances.model_dump().get(group)
            for metric_name, tolerance in metric_tolerances.items():
                ungrouped_tolerance = self.group_metric_tolerances.model_dump()[
                    "ungrouped"
                ].get(metric_name)
                if tolerance < ungrouped_tolerance:
                    raise AssertionError(
                        f"In {group=}, {tolerance=} for {metric_name} should be "
                        f"greater than {ungrouped_tolerance=}."
                    )
        return self


class ReconcileTableCalculations(TransformParams):
    """Parameters for reconciling xbrl-metadata based calculations within a table."""

    column_to_check: str | None = None
    """Name of data column to check.

    This will typically be ``dollar_value`` or ``ending_balance`` column for the income
    statement and the balance sheet tables.
    """
    group_metric_checks: GroupMetricChecks = GroupMetricChecks()
    """Fraction of calculated values which we allow not to match reported values."""

    subtotal_column: str | None = None
    """Sub-total column name (e.g. utility type) to compare calculations against in
    :func:`reconcile_table_calculations`."""

    subtotal_calculation_tolerance: float = 0.05
    """Fraction of calculated sub-totals allowed not to match reported values."""


def reconcile_table_calculations(
    df: pd.DataFrame,
    calculation_components: pd.DataFrame,
    xbrl_metadata: pd.DataFrame,
    xbrl_factoid_name: str,
    table_name: str,
    params: ReconcileTableCalculations,
) -> pd.DataFrame:
    """Ensure intra-table calculated values match reported values within a tolerance.

    In addition to checking whether all reported "calculated" values match the output
    of our repaired calculations, this function adds a correction record to the
    dataframe that is included in the calculations so that after the fact the
    calculations match exactly. This is only done when the fraction of records that
    don't match within the tolerances of :func:`numpy.isclose` is below a set
    threshold.

    Note that only calculations which are off by a significant amount result in the
    creation of a correction record. Many calculations are off from the reported values
    by exaclty one dollar, presumably due to rounding errrors. These records typically
    do not fail the :func:`numpy.isclose()` test and so are not corrected.

    Args:
        df: processed table containing data values to check.
        calculation_components: processed calculation component metadata.
        xbrl_metadata: A dataframe of fact-level metadata, required for inferring the
            sub-dimension total calculations.
        xbrl_factoid_name: The name of the column which contains XBRL factoid values in
            the processed table.
        table_name: name of the PUDL table whose data and metadata is being processed.
            This is necessary so we can ensure the metadata has the same structure as
            the calculation components, which at a minimum need both ``table_name`` and
            ``xbrl_factoid`` to identify them.
        params: :class:`ReconcileTableCalculations` parameters.

    Returns:
        A dataframe that includes new ``*_correction`` records with values that ensure
        the calculations all match to within the required tolerance. It will also
        contain columns created by the calculation checking process like ``abs_diff``
        and ``rel_diff``.
    """
    # If we don't have this value, we aren't doing any calculation checking:
    if params.column_to_check is None or calculation_components.empty:
        return df

    # Use the calculation components which reference ONLY values within the table
    intra_table_calcs = calculation_components[
        calculation_components.is_within_table_calc
    ]
    # To interact with the calculation components, we need uniformly named columns
    # for xbrl_factoid, and table_name
    df = df.rename(columns={xbrl_factoid_name: "xbrl_factoid"}).assign(
        table_name=table_name
    )
    dim_cols = [
        dim
        for dim in other_dimensions(table_names=list(FERC1_TFR_CLASSES))
        if dim in df.columns
    ]
    calc_idx = ["xbrl_factoid", "table_name"] + dim_cols

    if dim_cols:
        table_dims = df[calc_idx].drop_duplicates(keep="first")
        intra_table_calcs = _add_intra_table_calculation_dimensions(
            intra_table_calcs=intra_table_calcs,
            table_dims=table_dims,
            dim_cols=dim_cols,
        )
        # Check the subdimension totals, but don't add correction records for these
        # intra-fact calculations:
        if params.subtotal_column:
            calc_comps_w_totals = _calculation_components_subtotal_calculations(
                intra_table_calcs=intra_table_calcs,
                table_dims=table_dims,
                xbrl_metadata=xbrl_metadata,
                dim_cols=dim_cols,
                table_name=table_name,
            )
            _check_subtotal_calculations(
                df=df,
                params=params,
                calc_comps_w_totals=calc_comps_w_totals,
                calc_idx=calc_idx,
            )

    calculated_df = (
        calculate_values_from_components(
            data=df,
            calculation_components=intra_table_calcs,
            calc_idx=calc_idx,
            value_col=params.column_to_check,
        )
        .pipe(
            check_calculation_metrics,
            group_metric_checks=params.group_metric_checks,
        )
        .pipe(
            add_corrections,
            value_col=params.column_to_check,
            is_close_tolerance=IsCloseTolerance(),
            table_name=table_name,
        )
        # Rename back to the original xbrl_factoid column name before returning:
        .rename(columns={"xbrl_factoid": xbrl_factoid_name})
    )

    return calculated_df


def _calculation_components_subtotal_calculations(
    intra_table_calcs: pd.DataFrame,
    table_dims: pd.DataFrame,
    xbrl_metadata: pd.DataFrame,
    dim_cols: list[str],
    table_name: str,
) -> pd.DataFrame:
    """Add total to subtotal calculations into calculation components."""
    meta_w_dims = xbrl_metadata.assign(
        **{dim: pd.NA for dim in dim_cols} | {"table_name": table_name}
    ).pipe(
        make_calculation_dimensions_explicit,
        table_dimensions_ferc1=table_dims,
        dimensions=dim_cols,
    )
    calc_comps_w_totals = infer_intra_factoid_totals(
        intra_table_calcs,
        meta_w_dims=meta_w_dims,
        table_dimensions=table_dims,
        dimensions=dim_cols,
    )
    return calc_comps_w_totals


def _check_subtotal_calculations(
    df: pd.DataFrame,
    params: "Ferc1TableTransformParams",
    calc_comps_w_totals: pd.DataFrame,
    calc_idx: list[str],
) -> None:
    """Check that sub-dimension calculations sum to the reported totals.

    No correction records are added to the sub-dimensions calculations. This is only an
    error check, and returns nothing.
    """
    logger.info(f"Checking total-to-subtotal calculations in {params.subtotal_column}")
    subtotal_calcs = calculate_values_from_components(
        data=df,
        calculation_components=calc_comps_w_totals[
            calc_comps_w_totals.is_total_to_subdimensions_calc
        ],
        calc_idx=calc_idx,
        value_col=params.column_to_check,
    )
    subtotal_calcs = check_calculation_metrics(
        calculated_df=subtotal_calcs,
        group_metric_checks=params.group_metric_checks,
    )


def _add_intra_table_calculation_dimensions(
    intra_table_calcs: pd.DataFrame,
    table_dims: pd.DataFrame,
    dim_cols: list[str],
) -> pd.DataFrame:
    """Add all observed subdimensions into the calculation components."""
    ######## Add all observed subdimensions into the calculation components!!!
    # First determine what dimensions matter in this table:
    # - usually params.subtotal_column has THE ONE dimension in the table...
    # - BUT some tables have more than one dimension so we grab from all of the
    #   the dims in the transformers.

    # need to add in the correction dimensions. they don't show up in the data at
    # this point so we don't have the dimensions yet. NOTE: this could have been
    # done by adding the dims into table_dims..... maybe would have been more
    # straightforward
    correction_mask = intra_table_calcs.xbrl_factoid.str.endswith("_correction")
    intra_table_calcs = pd.concat(
        [
            intra_table_calcs[~correction_mask],
            pd.merge(
                intra_table_calcs[correction_mask].drop(columns=dim_cols),
                table_dims[["table_name"] + dim_cols].drop_duplicates(),
                on=["table_name"],
            ),
        ]
    )
    intra_table_calcs = make_calculation_dimensions_explicit(
        intra_table_calcs,
        table_dimensions_ferc1=table_dims,
        dimensions=dim_cols,
    ).pipe(
        assign_parent_dimensions,
        table_dimensions=table_dims,
        dimensions=dim_cols,
    )
    # this is for the income statement table specifically, but is general:
    # remove all the bits where we have a child dim but not a parent dim
    # sometimes there are child dimensions that have utility_type == "other2" etc
    # where the parent dimension has nothing
    for dim in dim_cols:
        intra_table_calcs = intra_table_calcs[
            ~(
                intra_table_calcs[dim].notnull()
                & intra_table_calcs[f"{dim}_parent"].isnull()
            )
        ]
    return intra_table_calcs


def calculate_values_from_components(
    calculation_components: pd.DataFrame,
    data: pd.DataFrame,
    calc_idx: list[str],
    value_col: str,
) -> pd.DataFrame:
    """Apply calculations derived from XBRL metadata to reported XBRL data.

    Args:
        calculation_components: Table defining the calculations, with each row defining
            a single component, including its weight. Groups of rows identified by
            ``table_name_parent`` and ``xbrl_factoid_parent`` indicate the values being
            calculated.
        data: exploded FERC data to apply the calculations to. Primary key should be
            ``report_year``, ``utility_id_ferc1``, ``table_name``, ``xbrl_factoid``, and
            whatever additional dimensions are relevant to the data.
        calc_idx: primary key columns that uniquely identify a calculation component
            (not including the ``_parent`` columns).
        value_col: label of the column in ``data`` that contains the values to apply the
            calculations to (typically ``dollar_value`` or ``ending_balance``).
    """
    # Merge the reported data and the calculation component metadata to enable
    # validation of calculated values. Here the data table exploded is supplying the
    # values associated with individual calculation components, and the table_name
    # and xbrl_factoid to which we aggregate are coming from the calculation
    # components table. After merging we use the weights to adjust the reported
    # values so they can be summed directly. This gives us aggregated calculated
    # values that can later be compared to the higher level reported values.

    # infer the pks of the data by adding in the util/year
    data_idx = calc_idx + ["utility_id_ferc1", "report_year"]
    # we are going to merge the data onto the calc components with the _parent
    # column names, so the groupby after the merge needs a set of by cols with the
    # _parent suffix
    gby_parent = [f"{col}_parent" for col in calc_idx] + [
        "utility_id_ferc1",
        "report_year",
    ]
    try:
        calc_df = (
            pd.merge(
                calculation_components,
                data,
                validate="one_to_many",
                on=calc_idx,
            )
            # apply the weight from the calc to convey the sign before summing.
            .assign(calculated_value=lambda x: x[value_col] * x.weight)
            .groupby(gby_parent, as_index=False, dropna=False)[["calculated_value"]]
            .sum(min_count=1)
        )
    except pd.errors.MergeError:  # Make debugging easier.
        raise pd.errors.MergeError(
            f"Merge failed, duplicated merge keys in left dataset: \n{calculation_components[calculation_components.duplicated(calc_idx)]}"
        )
    # remove the _parent suffix so we can merge these calculated values back onto
    # the data using the original pks
    calc_df.columns = calc_df.columns.str.removesuffix("_parent")
    calculated_df = pd.merge(
        data,
        calc_df,
        on=data_idx,
        how="outer",
        validate="1:1",
        indicator=True,
    )

    assert calculated_df[
        (calculated_df._merge == "right_only") & (calculated_df[value_col].notnull())
    ].empty

    calculated_df = calculated_df.drop(columns=["_merge"])
    # Force value_col to be a float to prevent any hijinks with calculating differences.
    # Data types were very messy here, including pandas Float64 for the
    # calculated_value columns which did not work with the np.isclose(). Not sure
    # why these are cropping up.
    calculated_df = calculated_df.convert_dtypes(convert_floating=False).astype(
        {value_col: "float64", "calculated_value": "float64"}
    )
    calculated_df = calculated_df.assign(
        abs_diff=lambda x: abs(x[value_col] - x.calculated_value),
        rel_diff=lambda x: np.where(
            (x[value_col] != 0.0),
            abs(x.abs_diff / x[value_col]),
            np.nan,
        ),
    )
    # Uniformity here helps keep the error checking functions simpler:
    calculated_df["reported_value"] = calculated_df[value_col]
    return calculated_df


def check_calculation_metrics_by_group(
    calculated_df: pd.DataFrame,
    group_metric_checks: GroupMetricChecks,
) -> pd.DataFrame:
    """Tabulate the results of the calculation checks by group.

    Convert all of the groups' checks into a big df. This will have two indexes: first
    for the group name (group) and one for the groups values. the columns will include
    three for each test: the test mertic that is the same name as the test (ex:
    error_frequency), the tolerance for that group/test and a boolean indicating
    whether or not that metric failed to meet the tolerance.
    """
    results_dfs = {}
    # for each groupby grouping: calculate metrics for each test
    # then check if each test is within acceptable tolerance levels
    for group_name in group_metric_checks.groups_to_check:
        group_metrics = {}
        for (
            metric_name,
            metric_tolerance,
        ) in group_metric_checks.group_metric_tolerances.model_dump()[
            group_name
        ].items():
            if metric_name in group_metric_checks.metrics_to_check:
                # this feels icky. the param name for the metrics are all snake_case while
                # the metric classes are all TitleCase. So we convert to TitleCase
                title_case_test = metric_name.title().replace("_", "")
                group_metric_checker = globals()[title_case_test](
                    by=group_name,
                    is_close_tolerance=group_metric_checks.is_close_tolerance.model_dump()[
                        metric_name
                    ],
                    metric_tolerance=metric_tolerance,
                )
                group_metric = group_metric_checker.check(
                    calculated_df=calculated_df
                ).rename(columns={group_name: "group_value"})
                # we want to set the index values as the same for all groups, but both the
                # ungrouped and table_name group require a special exception because we need
                # to add table_name into the columns
                if group_name == "ungrouped":
                    group_metric = group_metric.assign(table_name="ungrouped")
                if group_name == "table_name":
                    # we end up having two columns w/ table_name values in order to keep all the
                    # outputs having the same indexes.
                    group_metric = group_metric.assign(
                        table_name=lambda x: x["group_value"]
                    )
                # make a uniform multi-index w/ group name and group values
                group_metrics[metric_name] = group_metric.set_index(
                    ["group", "table_name", "group_value"]
                )
        results_dfs[group_name] = pd.concat(group_metrics.values(), axis="columns")
    results = pd.concat(results_dfs.values(), axis="index")
    return results


def check_calculation_metrics(
    calculated_df: pd.DataFrame,
    group_metric_checks: GroupMetricChecks,
) -> pd.DataFrame:
    """Run the calculation metrics and determine if calculations are within tolerance."""
    # DO ERROR CHECKS
    results = check_calculation_metrics_by_group(calculated_df, group_metric_checks)
    # get the records w/ errors in any! of their checks
    errors = results[results.filter(like="is_error").any(axis=1)]
    if not errors.empty:
        # it miiight be good to isolate just the error columns..
        raise AssertionError(
            f"Found errors while running tests on the calculations:\n{errors}"
        )
    return calculated_df


########################################################################################
# Calculation Error Checking Functions
# - These functions all take a dataframe and return a float.
# - They are intended to be used in GroupBy.apply() (or on a whole dataframe).
# - They require a uniform `reported_value` column so that they can all have the same
#   call signature, which allows us to iterate over all of them in a matrix.
########################################################################################


class ErrorMetric(BaseModel):
    """Base class for checking a particular metric within a group."""

    by: Literal[
        "ungrouped", "table_name", "xbrl_factoid", "utility_id_ferc1", "report_year"
    ]
    """Name of group to check the metric based on.

    With the exception of the ungrouped case, all groups depend on table_name as well as
    the other column specified via by.

    If by=="table_name" then that is the only column used in the groupby().

    If by=="ungrouped" then all records are included in the "group" (via a dummy column
    named ungrouped that contains only the value ungrouped). This allows us to use the
    same infrastructure for applying the metrics to grouped and ungrouped data.
    """

    is_close_tolerance: IsCloseTolerance
    """Inputs for the metric to determine :meth:`is_not_close`. Instance of :class:`IsCloseTolerance`."""

    metric_tolerance: float
    """Tolerance for checking the metric within the ``by`` group."""

    required_cols: list[str] = [
        "table_name",
        "xbrl_factoid",
        "report_year",
        "utility_id_ferc1",
        "reported_value",
        "calculated_value",
        "abs_diff",
        "rel_diff",
    ]

    def has_required_cols(self: Self, df: pd.DataFrame):
        """Check that the input dataframe has all required columns."""
        missing_required_cols = [
            col for col in self.required_cols if col not in df.columns
        ]
        if missing_required_cols:
            raise AssertionError(
                f"The table is missing the following required columns: {missing_required_cols}"
            )
        return True

    @abstractmethod
    def metric(self: Self, gb: DataFrameGroupBy) -> pd.Series:
        """Metric function that will be applied to each group of values being checked."""
        ...

    def is_not_close(self, df: pd.DataFrame) -> pd.Series:
        """Flag records where reported and calculated values differ significantly.

        We only want to check this metric when there is a non-null ``abs_diff`` because
        we want to avoid the instances in which there are either null reported or
        calculated values.
        """
        return pd.Series(
            ~np.isclose(
                df["calculated_value"],
                df["reported_value"],
                rtol=self.is_close_tolerance.isclose_rtol,
                atol=self.is_close_tolerance.isclose_atol,
            )
            & df["abs_diff"].notnull()
        )

    def groupby_cols(self: Self) -> list[str]:
        """The list of columns to group by.

        We want to default to adding the table_name into all groupby's, but two of our
        ``by`` options need special treatment.
        """
        gb_by = ["table_name", self.by]
        if self.by in ["ungrouped", "table_name"]:
            gb_by = [self.by]
        return gb_by

    def apply_metric(self: Self, df: pd.DataFrame) -> pd.Series:
        """Generate the metric values within each group through an apply method.

        This method adds a column ``is_not_close`` into the df before the groupby
        because that column is used in many of the :meth:`metric`.
        """
        # return a df instead of a series
        df["is_not_close"] = self.is_not_close(df)
        return df.groupby(by=self.groupby_cols()).apply(self.metric)

    def _snake_case_metric_name(self: Self) -> str:
        """Convert the TitleCase class name to a snake_case string."""
        class_name = self.__class__.__name__
        return re.sub("(?!^)([A-Z]+)", r"_\1", class_name).lower()

    def check(self: Self, calculated_df) -> pd.DataFrame:
        """Make a df w/ the metric, tolerance and is_error columns."""
        self.has_required_cols(calculated_df)
        # ungrouped is special because the rest of the stock group names are column
        # names.
        if self.by == "ungrouped":
            calculated_df = calculated_df.assign(
                ungrouped="ungrouped",
            )

        metric_name = self._snake_case_metric_name()
        df = (
            pd.DataFrame(self.apply_metric(calculated_df), columns=[metric_name])
            .assign(
                **{  # totolerance_ is just for reporting so you can know of off you are
                    f"tolerance_{metric_name}": self.metric_tolerance,
                    f"is_error_{metric_name}": lambda x: x[metric_name]
                    > self.metric_tolerance,
                }
            )
            .assign(group=self.by)
            .reset_index()
        )
        return df


class ErrorFrequency(ErrorMetric):
    """Check error frequency in XBRL calculations."""

    def metric(self: Self, gb: DataFrameGroupBy) -> pd.Series:
        """Calculate the frequency with which records are tagged as errors."""
        try:
            out = gb[gb.is_not_close].shape[0] / gb.shape[0]
        except ZeroDivisionError:
            # Will only occur if all reported values are NaN when calculated values
            # exist, or vice versa.
            logger.warning(
                "Calculated values have no corresponding reported values in this table."
            )
            out = np.nan
        return out


class RelativeErrorMagnitude(ErrorMetric):
    """Check relative magnitude of errors in XBRL calculations."""

    def metric(self: Self, gb: DataFrameGroupBy) -> pd.Series:
        """Calculate the mangnitude of the errors relative to total reported value."""
        try:
            return gb.abs_diff.abs().sum() / gb["reported_value"].abs().sum()
        except ZeroDivisionError:
            return np.nan


class AbsoluteErrorMagnitude(ErrorMetric):
    """Check absolute magnitude of errors in XBRL calculations.

    These numbers may vary wildly from table to table so no default values for the
    expected errors are provided here...
    """

    def metric(self: Self, gb: DataFrameGroupBy) -> pd.Series:
        """Calculate the absolute mangnitude of XBRL calculation errors."""
        return gb.abs_diff.abs().sum()


class NullCalculatedValueFrequency(ErrorMetric):
    """Check the frequency of null calculated values."""

    def apply_metric(self: Self, df: pd.DataFrame) -> pd.Series:
        """Only apply metric to rows that contain calculated values."""
        return (
            df[df.row_type_xbrl == "calculated_value"]
            .groupby(self.groupby_cols())
            .apply(self.metric)
        )

    def metric(self: Self, gb: DataFrameGroupBy) -> pd.Series:
        """Fraction of non-null reported values that have null corresponding calculated values."""
        non_null_reported = gb["reported_value"].notnull()
        null_calculated = gb["calculated_value"].isnull()
        try:
            return (non_null_reported & null_calculated).sum() / non_null_reported.sum()
        except ZeroDivisionError:
            return np.nan


class NullReportedValueFrequency(ErrorMetric):
    """Check the frequency of null reported values."""

    def metric(self: Self, gb: DataFrameGroupBy) -> pd.Series:
        """Frequency with which the reported values are Null."""
        return gb["reported_value"].isnull().sum() / gb.shape[0]


def add_corrections(
    calculated_df: pd.DataFrame,
    value_col: str,
    is_close_tolerance: IsCloseTolerance,
    table_name: str,
) -> pd.DataFrame:
    """Add corrections to discrepancies between reported & calculated values.

    To isolate the sources of error, and ensure that all totals add up as expected in
    later phases of the transformation, we add correction records to the dataframe
    which compensate for any difference between the calculated and reported values. The
    ``_correction`` factoids that are added here have already been added to the
    calculation components during the metadata processing.

    Args:
        calculated_df: DataFrame containing the data to correct. Must already have
            ``abs_diff`` column that was added by :func:`check_calculation_metrics`
        value_col: Label of the column whose values are being calculated.
        calculation_tolerance: Data structure containing various calculation tolerances.
        table_name: Name of the table whose data we are working with. For logging.
    """
    corrections = calculated_df[
        ~np.isclose(
            calculated_df["calculated_value"],
            calculated_df[value_col],
            rtol=is_close_tolerance.isclose_rtol,
            atol=is_close_tolerance.isclose_atol,
        )
        & (calculated_df["abs_diff"].notnull())
    ].copy()

    corrections[value_col] = (
        corrections[value_col].fillna(0.0) - corrections["calculated_value"]
    )
    corrections = corrections.assign(
        xbrl_factoid_corrected=lambda x: x["xbrl_factoid"],
        xbrl_factoid=lambda x: x["xbrl_factoid"] + "_correction",
        row_type_xbrl="correction",
        is_within_table_calc=False,
        record_id=pd.NA,
    )
    num_notnull_calcs = sum(calculated_df["abs_diff"].notnull())
    num_corrections = corrections.shape[0]
    num_records = calculated_df.shape[0]
    try:
        corrected_fraction = num_corrections / num_notnull_calcs
    except ZeroDivisionError:
        corrected_fraction = np.nan
    logger.info(
        f"{table_name}: Correcting {corrected_fraction:.2%} of all non-null reported "
        f"values ({num_corrections}/{num_notnull_calcs}) out of a total of "
        f"{num_records} original records."
    )

    return pd.concat([calculated_df, corrections], axis="index")


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
    unstack_balances_to_report_year_instant_xbrl: UnstackBalancesToReportYearInstantXbrl = UnstackBalancesToReportYearInstantXbrl()
    combine_axis_columns_xbrl: CombineAxisColumnsXbrl = CombineAxisColumnsXbrl()
    reconcile_table_calculations: ReconcileTableCalculations = (
        ReconcileTableCalculations()
    )

    @property
    def xbrl_factoid_name(self) -> str:
        """Access the column name of the ``xbrl_factoid``."""
        return self.merge_xbrl_metadata.on

    @property
    def rename_dicts_xbrl(self):
        """Compile all of the XBRL rename dictionaries into an ordered list."""
        return self.rename_columns_ferc1.rename_dicts_xbrl

    @property
    def wide_to_tidy_value_types(self) -> list[str]:
        """Compile a list of all of the ``value_types`` from ``wide_to_tidy``."""
        return self.wide_to_tidy.value_types

    @property
    def aligned_dbf_table_names(self) -> list[str]:
        """The list of DBF tables aligned by row number in this transform."""
        return self.align_row_numbers_dbf.dbf_table_names


################################################################################
# FERC 1 transform helper functions. Probably to be integrated into a class
# below as methods or moved to a different module once it's clear where they belong.
################################################################################
def get_ferc1_dbf_rows_to_map(ferc1_engine: sa.Engine) -> pd.DataFrame:
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


def update_dbf_to_xbrl_map(ferc1_engine: sa.Engine) -> pd.DataFrame:
    """Regenerate the FERC 1 DBF+XBRL glue while retaining existing mappings.

    Reads all rows that need to be mapped out of the ``f1_row_lit_tbl`` and appends
    columns containing any previously mapped values, returning the resulting dataframe.
    """
    idx_cols = ["sched_table_name", "row_number", "report_year"]
    all_rows = get_ferc1_dbf_rows_to_map(ferc1_engine).set_index(idx_cols)
    mapped_rows = (
        pd.read_csv(
            importlib.resources.files("pudl.package_data.ferc1") / "dbf_to_xbrl.csv"
        )
        .set_index(idx_cols)
        .drop(["row_literal"], axis="columns")
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
    row_map = pd.read_csv(
        importlib.resources.files("pudl.package_data.ferc1") / "dbf_to_xbrl.csv",
        usecols=[
            "sched_table_name",
            "report_year",
            "row_number",
            "row_type",
            "row_literal",
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
    df.loc[:, ["xbrl_factoid", "row_literal"]] = df.groupby(
        ["row_number", "sched_table_name"]
    )[["xbrl_factoid", "row_literal"]].transform("ffill")
    # Drop NA values produced in the broadcasting merge onto the exhaustive index.
    df = df.dropna(subset="xbrl_factoid")
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


def read_xbrl_calculation_fixes() -> pd.DataFrame:
    """Read in the table of calculation fixes."""
    source = importlib.resources.files("pudl.package_data.ferc1").joinpath(
        "xbrl_calculation_component_fixes.csv"
    )
    with importlib.resources.as_file(source) as file:
        calc_fixes = pd.read_csv(file)
    return calc_fixes


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
    xbrl_calculations: pd.DataFrame | None = None
    """Dataframe of calculation components.

    If ``None``, the calculations have not been instantiated. If the table has been
    instantiated but is an empty table, then there are no calculations for that table.
    """

    def __init__(
        self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]]
        | None = None,
        params: TableTransformParams | None = None,
        cache_dfs: bool = False,
        clear_cached_dfs: bool = True,
    ) -> None:
        """Augment inherited initializer to store XBRL metadata in the class."""
        super().__init__(
            params=params,
            cache_dfs=cache_dfs,
            clear_cached_dfs=clear_cached_dfs,
        )
        if xbrl_metadata_json:
            xbrl_metadata_converted = self.convert_xbrl_metadata_json_to_df(
                xbrl_metadata_json
            )
            self.xbrl_calculations = self.process_xbrl_metadata_calculations(
                xbrl_metadata_converted
            )
            self.xbrl_metadata = self.process_xbrl_metadata(
                xbrl_metadata_converted, self.xbrl_calculations
            )

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

        Checks calculations. Enforces dataframe schema. Checks for empty dataframes and
        null columns.
        """
        df = self.reconcile_table_calculations(df).pipe(self.enforce_schema)
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

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Normalize the XBRL JSON metadata, turning it into a dataframe.

        This process concatenates and deduplicates the metadata which is associated with
        the instant and duration tables, since the metadata is only combined with the
        data after the instant and duration (and DBF) tables have been merged. This
        happens in :meth:`Ferc1AbstractTableTransformer.merge_xbrl_metadata`.
        """
        logger.info(f"{self.table_id.value}: Processing XBRL metadata.")
        tbl_meta = (
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
                calculations=lambda x: x.calculations.apply(json.dumps),
            )
            .astype(
                {
                    "xbrl_factoid": pd.StringDtype(),
                    "balance": pd.StringDtype(),
                    "ferc_account": pd.StringDtype(),
                    "calculations": pd.StringDtype(),
                }
            )
            .assign(
                xbrl_factoid_original=lambda x: x.xbrl_factoid,
                xbrl_factoid=lambda x: self.rename_xbrl_factoid(x.xbrl_factoid),
            )
            .pipe(self.deduplicate_xbrl_factoid_xbrl_metadata)
        )
        return tbl_meta

    @cache_df(key="process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Process XBRL metadata after the calculations have been cleaned.

        Add ``row_type_xbrl`` and ``is_within_table_calc`` columns and create
        ``xbrl_factoid`` records for the calculation corrections.

        Args:
            xbrl_metadata_converted: Dataframe of relatively unprocessed metadata.
                Result of :meth:`convert_xbrl_metadata_json_to_df`.
            xbrl_calculations: Dataframe of calculation components. Result of
                :meth:`process_xbrl_metadata_calculations`.
        """
        # drop the calcs bc we never want to use them again. the xbrl_calculations are
        # now the main source of truth for the calcs. set index so we can easily
        # graph some calc info onto the metadata using an index of xbrl_factoid_parent.
        tbl_meta = xbrl_metadata_converted.drop(columns=["calculations"]).set_index(
            ["xbrl_factoid"]
        )
        # Flag metadata record types
        tbl_meta.loc[:, "row_type_xbrl"] = (
            (  # if there is nothing in the calc cols for a parent fact - its reported
                xbrl_calculations.groupby(["xbrl_factoid_parent"])[
                    ["table_name", "xbrl_factoid"]
                ].count()
                == 0
            )
            .all(axis="columns")
            .replace({True: "reported_value", False: "calculated_value"})
            .astype(pd.StringDtype())
        )
        tbl_meta.loc[:, "row_type_xbrl"] = tbl_meta.loc[:, "row_type_xbrl"].fillna(
            "reported_value"
        )
        # this bool column is created and used within the calculations. but its a
        # helpful thing in the metadata table as well.
        tbl_meta.loc[:, "is_within_table_calc"] = (
            xbrl_calculations.groupby(["xbrl_factoid_parent"])["is_within_table_calc"]
            .all()
            .astype(pd.BooleanDtype())
        )
        tbl_meta = tbl_meta.reset_index().pipe(self.add_metadata_corrections)
        return tbl_meta

    def deduplicate_xbrl_factoid_xbrl_metadata(
        self, tbl_meta: pd.DataFrame
    ) -> pd.DataFrame:
        """De-duplicate the xbrl_metadata based on ``xbrl_factoid``.

        Default is to do nothing besides check for duplicate values because almost all
        tables have no deduping. Deduplication needs to be applied before the
        :meth:`apply_xbrl_calculation_fixes` inside of :meth:`process_xbrl_metadata`.
        """
        if tbl_meta.duplicated(subset=["xbrl_factoid"]).any() & (
            self.params.merge_xbrl_metadata.on is not None
        ):
            raise AssertionError(
                f"Metadata for {self.table_id.value} has duplicative xbrl_factoid records."
            )
        return tbl_meta

    def raw_xbrl_factoid_to_pudl_name(
        self,
        col_name_xbrl: str,
    ) -> str:
        """Rename a column name from original XBRL name to the transformed PUDL name.

        There are several transform params that either explicitly or implicity rename
        columns:
        * :class:`RenameColumnsFerc1`
        * :class:`WideToTidySourceFerc1`
        * :class:`UnstackBalancesToReportYearInstantXbrl`
        * :class:`ConvertUnits`

        This method attempts to use the table params to translate a column name.

        Note: Instead of doing this for each individual column name, we could compile a
        rename dict for the whole table with a similar processand then apply it for each
        group of columns instead of running through this full process every time. If
        this took longer than...  ~5 ms on a single table w/ lots of calcs this would
        probably be worth it for simplicity.
        """
        col_name_new = col_name_xbrl
        for rename_stage in self.params.rename_dicts_xbrl:
            col_name_new = str(rename_stage.columns.get(col_name_new, col_name_new))

        for value_type in self.params.wide_to_tidy_value_types:
            if col_name_new.endswith(f"_{value_type}"):
                col_name_new = re.sub(f"_{value_type}$", "", col_name_new)

        if self.params.unstack_balances_to_report_year_instant_xbrl:
            # TODO: do something...? add starting_balance & ending_balance suffixes?
            if self.params.merge_xbrl_metadata.on:
                NotImplementedError(
                    "We haven't implemented a xbrl_factoid rename for the parameter "
                    "unstack_balances_to_report_year_instant_xbrl. Since you are trying"
                    "to merge the metadata on this table that has this treatment, a "
                    "xbrl_factoid rename will be required."
                )
            pass
        if self.params.convert_units:
            # TODO: use from_unit -> to_unit map. but none of the $$ tables have this rn.
            if self.params.merge_xbrl_metadata.on:
                NotImplementedError(
                    "We haven't implemented a xbrl_factoid rename for the parameter "
                    "convert_units. Since you are trying to merge the metadata on this "
                    "table that has this treatment, a xbrl_factoid rename will be "
                    "required."
                )
            pass
        return col_name_new

    def rename_xbrl_factoid(self, col: pd.Series) -> pd.Series:
        """Rename a series of raw to PUDL factoid names via :meth:`raw_xbrl_factoid_to_pudl_name`."""
        xbrl_factoid_name_map = {
            xbrl_factoid_name_og: self.raw_xbrl_factoid_to_pudl_name(
                xbrl_factoid_name_og
            )
            for xbrl_factoid_name_og in col
        }
        return col.map(xbrl_factoid_name_map)

    def rename_xbrl_factoid_other_tables(self, calc_comps):
        """Rename the factoids from calculation components from other tables.

        Note: It is probably possible to build an apply style function that takes a
        series of factoid names and a series of table names and returns a table-specific
        rename_xbrl_factoid.
        """
        calc_tables = calc_comps.table_name.dropna().unique()
        os_tables = [
            tbl
            for tbl in calc_tables
            if (tbl != self.table_id.value) & (tbl in FERC1_TFR_CLASSES)
        ]
        for tbl in os_tables:
            trns = FERC1_TFR_CLASSES[tbl]()
            calc_comps = calc_comps.assign(
                xbrl_factoid=lambda x: np.where(
                    x.table_name == tbl,
                    trns.rename_xbrl_factoid(x.xbrl_factoid),
                    x.xbrl_factoid,
                ),
            )
        return calc_comps

    @staticmethod
    def add_metadata_corrections(tbl_meta: pd.DataFrame) -> pd.DataFrame:
        """Create metadata records for the calculation correction factoids.

        Args:
            tbl_meta: processed metadata table which contains columns ``row_type_xbrl``.
        """
        correction_meta = tbl_meta[tbl_meta.row_type_xbrl == "calculated_value"].assign(
            is_within_table_calc=True,
            row_type_xbrl="correction",
            xbrl_factoid=lambda x: x.xbrl_factoid + "_correction",
        )
        tbl_meta = (
            pd.concat([tbl_meta, correction_meta])
            .reset_index(drop=True)
            .convert_dtypes()
        )
        return tbl_meta

    def add_calculation_corrections(
        self: Self, calc_components: pd.DataFrame
    ) -> pd.DataFrame:
        """Add correction components and parent-only factoids to calculation metadata.

        Args:
            tbl_meta: Partially transformed table metadata in dataframe form.

        Returns:
            An updated version of the table metadata containing calculation definitions
            that include a correction component.
        """
        # If we haven't provided calculation check parameters, then we can't identify
        # a appropriate correction factor.
        if self.params.reconcile_table_calculations.column_to_check is None:
            return calc_components

        # split the calcs from non-calcs/make corrections/append
        calcs = calc_components[calc_components.xbrl_factoid.notnull()]
        correction_components = (
            calcs[["table_name_parent", "xbrl_factoid_parent"]]
            .drop_duplicates()
            .assign(
                table_name=lambda t: t.table_name_parent,
                xbrl_factoid=lambda x: x.xbrl_factoid_parent + "_correction",
                weight=1,
            )
        )
        # for every calc component, also make the parent-only version
        correction_components.assign(
            table_name_parent=lambda t: t.table_name,
            xbrl_factoid_parent=lambda x: x.xbrl_factoid,
        ).drop(columns=["table_name", "xbrl_factoid"])
        return pd.concat([calc_components, correction_components])

    def get_xbrl_calculation_fixes(self: Self) -> pd.DataFrame:
        """Grab the XBRL calculation file."""
        calc_fixes = read_xbrl_calculation_fixes()
        # grab the fixes from this table only!
        calc_fixes = calc_fixes[calc_fixes.table_name_parent == self.table_id.value]
        return calc_fixes

    def apply_xbrl_calculation_fixes(
        self: Self, calc_components: pd.DataFrame, calc_fixes: pd.DataFrame
    ) -> pd.DataFrame:
        """Use the fixes we've compiled to update calculations in the XBRL metadata.

        Note: Temp fix. These updates should probably be moved into the table params
        and integrated into the calculations via TableCalcs.
        """
        calc_comp_idx = [
            "table_name_parent",
            "xbrl_factoid_parent",
            "table_name",
            "xbrl_factoid",
        ]
        if not (
            dupes := calc_fixes[calc_fixes.duplicated(subset=calc_comp_idx, keep=False)]
        ).empty:
            raise AssertionError(
                "Duplicates found in the calculation fixes where none were expected."
                f"{dupes}"
            )

        calc_fixes = calc_fixes.set_index(calc_comp_idx).sort_index()
        calc_components = calc_components.set_index(calc_comp_idx).sort_index()
        # find the fixes that need to be replaced. We id them
        # by finding the fixes that share indexes with the calc components
        # Note: we can't just dropna after adding the replacements instead
        # of while finding the replacements because we have included all
        # factoids in the calculation component table as parent factoids
        # even if there are no/null calculation components.
        replace_me = calc_fixes.loc[
            calc_fixes.index.intersection(calc_components.index)
        ].dropna(how="all")
        calc_components.loc[replace_me.index, list(replace_me.columns)] = replace_me

        # find the lines that only show up in the fixes that need to be added
        add_me = calc_fixes.loc[calc_fixes.index.difference(calc_components.index)]
        calc_components = pd.concat([calc_components, add_me])
        # sometimes we add fresh calculations to parent facts that originally didn't
        # have any calculation components. So if we are adding those parent facts
        # with child facts/calc components, we need to remove the non-calc records
        # so make fake little parent facts with null childern from all the add_mes
        null_calc_versions_of_add_mes = (
            add_me.reset_index()[["table_name_parent", "xbrl_factoid_parent"]]
            .assign(table_name=pd.NA, xbrl_factoid=pd.NA)
            .set_index(calc_comp_idx)
        )
        remove_the_non_cals_from_add_mes = calc_components.index.difference(
            null_calc_versions_of_add_mes.index.drop_duplicates()
        )
        calc_components = calc_components.loc[remove_the_non_cals_from_add_mes]

        # find the "null" fixes which correspond to records which need to be deleted.
        delete_me = calc_fixes[calc_fixes.isnull().all(axis=1)]
        calc_components = calc_components.loc[
            calc_components.index.difference(delete_me.index)
        ]
        len_fixes_applied = len(replace_me) + len(add_me) + len(delete_me)
        logger.debug(
            f"We've applied {len_fixes_applied} calculation fixes including "
            f"{len(replace_me)} replacements, {len(add_me)} additions and "
            f"{len(delete_me)} deletions."
        )
        if len(calc_fixes) != len_fixes_applied:
            raise AssertionError(
                f"We've applied {len_fixes_applied} calculation fixes while we started "
                f"with {len(calc_fixes)}. Length of applied and original fixes should "
                f"be the same.\n{replace_me=}\n{add_me=}\n{delete_me=}"
            )
        return calc_components.reset_index()

    def process_xbrl_metadata_calculations(
        self, xbrl_metadata_converted: pd.DataFrame
    ) -> pd.DataFrame:
        """Convert xbrl metadata calculations into a table of calculation components.

        This method extracts the calculations from the ``xbrl_metadata_converted``
        that are stored as json embedded within the ``calculations``column and convert
        those into calculation component records. The resulting table includes columns
        pertaining to both the calculation components and the parent factoid that the
        components pertain to. The parental columns had suffixes of ``_parent``.

        This method also adds fixes to the calculations via
        :meth:`apply_xbrl_calculation_fixes`, adds corrections records via
        :meth:`add_calculation_corrections` and adds the column
        ``is_within_table_calc``.

        Args:
            xbrl_metadata_converted: Dataframe of relatively unprocessed metadata.
                Result of :meth:`convert_xbrl_metadata_json_to_df`.
        """
        metadata = xbrl_metadata_converted.copy()
        metadata.calculations = metadata.calculations.apply(json.loads)
        # reset the index post calc explosion so we can merge on index later
        metadata = metadata.explode("calculations").reset_index(drop=True)
        if all(metadata.calculations.isnull()):
            calc_comps = pd.DataFrame(columns=["name", "source_tables"])
        else:
            calc_comps = pd.json_normalize(metadata.calculations)

        calc_comps = (
            calc_comps.explode("source_tables")
            .rename(
                columns={
                    "name": "xbrl_factoid",
                    "source_tables": "table_name",
                }
            )
            .merge(
                metadata.drop(columns=["calculations"]).rename(
                    columns={
                        "xbrl_factoid": "xbrl_factoid_parent",
                    }
                ),
                left_index=True,
                right_index=True,
                how="left",
            )
            .dropna(subset=["xbrl_factoid"])
            .reset_index(drop=True)
            .assign(
                table_name_parent=self.table_id.value,
                xbrl_factoid=lambda x: np.where(
                    x.table_name == self.table_id.value,
                    self.rename_xbrl_factoid(x.xbrl_factoid),
                    x.xbrl_factoid,
                ),
            )
            .pipe(self.rename_xbrl_factoid_other_tables)
            .pipe(
                self.apply_xbrl_calculation_fixes,
                calc_fixes=self.get_xbrl_calculation_fixes(),
            )
            .drop_duplicates(keep="first")
            .pipe(self.add_calculation_corrections)
        )

        # this is really a xbrl_factoid-level flag, but we need it while using this
        # calc components.
        calc_comps["is_within_table_calc"] = (
            # make a temp bool col to check if all the componets are intra table
            # should the non-calc guys get a null or a true here? rn its true bc fillna
            calc_comps.assign(
                intra_table_calc_comp_flag=lambda x: (
                    self.table_id.value == x.table_name.fillna(self.table_id.value)
                )
            )
            .groupby(["table_name_parent", "xbrl_factoid_parent"])[
                "intra_table_calc_comp_flag"
            ]
            .transform("all")
            .astype(pd.BooleanDtype())
        )
        # check for uniqueness only when we are reconciling the calculations
        # bc that implies we have cleaned the calcs and are intending to use them.
        if self.params.reconcile_table_calculations.column_to_check:
            calc_comp_idx = [
                "table_name_parent",
                "xbrl_factoid_parent",
                "table_name",
                "xbrl_factoid",
            ]
            if not (
                dupes := calc_comps[calc_comps.duplicated(subset=calc_comp_idx)]
            ).empty:
                raise AssertionError(
                    "Duplicates found in the calculation components where none were ."
                    f"expected {dupes}"
                )
        return calc_comps.convert_dtypes()

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
            if self.xbrl_metadata.empty:
                raise AssertionError(
                    "Metadata has not yet been generated. Must run process_xbrl_metadata"
                    "and assign xbrl_metadata before merging metadata."
                )
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
                f"{self.table_id.value}: After selection of only annual records,"
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

        multiple_params = [params] if isinstance(params, WideToTidy) else params
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

        return out_df.loc[out_df.report_year.isin(Ferc1Settings().xbrl_years)]

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

        dupe_ids = df.record_id[df.record_id.duplicated()].to_numpy()
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

    def reconcile_table_calculations(
        self: Self,
        df: pd.DataFrame,
        params: ReconcileTableCalculations | None = None,
    ):
        """Check how well a table's calculated values match reported values."""
        if params is None:
            params = self.params.reconcile_table_calculations
        if params.column_to_check:
            if self.xbrl_calculations is None:
                raise AssertionError(
                    "No calculations table has been built. Must run process_xbrl_metadata_calculations"
                )
            logger.info(
                f"{self.table_id.value}: Checking the XBRL metadata-based calculations."
            )
            df = reconcile_table_calculations(
                df=df,
                calculation_components=self.xbrl_calculations,
                xbrl_factoid_name=self.params.xbrl_factoid_name,
                xbrl_metadata=self.xbrl_metadata,
                table_name=self.table_id.value,
                params=params,
            )
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
            .pipe(self.to_numeric)
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
            .pipe(self.to_numeric)
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

    def to_numeric(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns containing numeric strings to numeric types."""
        numeric_cols = [
            "fuel_consumed_units",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_mmbtu",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col])

        return df

    def standardize_physical_fuel_units(self: Self, df: pd.DataFrame) -> pd.DataFrame:
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
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        We fill in some gaps in the metadata, e.g. for FERC accounts that have been
        split across multiple rows, or combined without being calculated. We also need
        to rename the XBRL metadata categories to conform to the same naming convention
        that we are using in the data itself (since FERC doesn't quite follow their own
        naming conventions...). We use the same rename dictionary, but as an argument to
        :meth:`pd.Series.replace` instead of :meth:`pd.DataFrame.rename`.
        """
        tbl_meta = (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="electric", plant_status="in_service")
        )

        # Set pseudo-account numbers for rows that split or combine FERC accounts, but
        # which are not calculated values.
        tbl_meta.loc[
            tbl_meta.xbrl_factoid == "electric_plant_purchased",
            ["ferc_account", "plant_status"],
        ] = ["102_purchased", pd.NA]
        tbl_meta.loc[
            tbl_meta.xbrl_factoid == "electric_plant_sold",
            ["ferc_account", "plant_status"],
        ] = ["102_sold", pd.NA]
        tbl_meta.loc[
            tbl_meta.xbrl_factoid
            == "electric_plant_in_service_and_completed_construction_not_classified_electric",
            "ferc_account",
        ] = "101_and_106"
        return tbl_meta

    def deduplicate_xbrl_factoid_xbrl_metadata(
        self, tbl_meta: pd.DataFrame
    ) -> pd.DataFrame:
        """De-duplicate the XBLR metadata.

        We deduplicate the metadata on the basis of the ``xbrl_factoid`` name.
        This table in particular has multiple ``wide_to_tidy`` ``value_types`` because
        there are multiple dollar columns embedded (it has both the standard start/end
        balances as well as modifcations like transfers/retirements). In the XBRL
        metadata, each xbrl_fact has its own set of metadata and possibly its own set of
        calculations. Which means that one ``xbrl_factoid`` for this table natively
        could have multiple calculations or other metadata.

        For merging, we need the metadata to have one field per ``xbrl_factoid``.
        Because we normally only use the start/end balance in calculations, when there
        are duplicate renamed ``xbrl_factoid`` s in our processed metadata, we are going
        to prefer the one that refers to the start/end balances. In an ideal world, we
        would be able to access this metadata based on both the ``xbrl_factoid`` and
        any column from ``value_types`` but that would require a larger change in
        architecture.
        """
        # remove duplication of xbrl_factoid
        same_calcs_mask = tbl_meta.duplicated(
            subset=["xbrl_factoid", "calculations"], keep=False
        )
        # if they key values are the same, select the records with values in ferc_account
        same_calcs_deduped = (
            tbl_meta[same_calcs_mask]
            .sort_values(["ferc_account"])  # sort brings the nulls to the bottom
            .drop_duplicates(subset=["xbrl_factoid"], keep="first")
        )
        # when the calcs are different, they are referring to the non-adjustments
        suffixes = ("_additions", "_retirements", "_adjustments", "_transfers")
        unique_calcs_deduped = tbl_meta[
            ~same_calcs_mask & (~tbl_meta.xbrl_factoid_original.str.endswith(suffixes))
        ]
        tbl_meta_cleaned = pd.concat([same_calcs_deduped, unique_calcs_deduped])
        assert set(tbl_meta_cleaned.xbrl_factoid.unique()) == set(
            tbl_meta.xbrl_factoid.unique()
        )
        assert ~tbl_meta_cleaned.duplicated(["xbrl_factoid"]).all()
        return tbl_meta_cleaned

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

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Drop targeted duplicates in the DBF data so we can use FERC respondent ID."""
        return super().process_dbf(raw_dbf).pipe(self.targeted_drop_duplicates_dbf)

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """The main table-specific transformations, affecting contents not structure.

        Annotates and alters data based on information from the XBRL taxonomy metadata.
        Also assigns utility type, plant status & function for use in table explosions.
        Make all electric_plant_sold balances positive.
        """
        df = super().transform_main(df).pipe(self.apply_sign_conventions)
        # Make all electric_plant_sold values positive
        # This could probably be a FERC transformer class function or in the
        # apply_sign_conventions function, but it doesn't seem like the best fit for
        # now.
        neg_values = (df["ferc_account_label"] == "electric_plant_sold") & (
            df["ending_balance"] < 0
        )
        df.loc[neg_values, "ending_balance"] = abs(df["ending_balance"])
        logger.info(
            f"{self.table_id.value}: Converted {len(df[neg_values])} negative values to positive."
        )
        # Assign plant status and utility type
        df = df.assign(utility_type="electric", plant_status="in_service")
        df.loc[
            df.ferc_account_label.isin(
                ["electric_plant_sold", "electric_plant_purchased"]
            )
        ].plant_status = pd.NA  # With two exceptions
        return df


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
        df = df.reset_index(drop=True)
        df["header_group"] = (df["row_type"] == "header").cumsum()
        df.loc[df["row_type"] != "note", "header"] = df.groupby(
            ["utility_id_ferc1", "report_year", "header_group"]
        ).header.ffill()

        # Create temporary columns for plant type and fuel type
        df["plant_type_from_header"] = df["header"]
        df["fuel_type_from_header"] = df["header"]
        df = df.drop(columns=["header", "header_group"])

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
        ].to_numpy()

        # Remove row_with_info so there is no duplicate information
        df = df[~row_with_info]

        return df


class TransmissionStatisticsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer for the :ref:`transmission_statistics_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.TRANSMISSION_STATISTICS_FERC1
    has_unique_record_ids: bool = False

    def transform_main(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Do some string-to-numeric ninja moves."""
        df["num_transmission_circuits"] = pd.to_numeric(df["num_transmission_circuits"])
        return super().transform_main(df)


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

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Perform default xbrl metadata processing plus adding 1 new xbrl_factoid.

        Note: we should probably parameterize this and add it into the standard
        :meth:`process_xbrl_metadata`.
        """
        tbl_meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
        facts_to_add = [
            {
                "xbrl_factoid": new_fact,
                "calculations": "[]",
                "balance": "credit",
                "ferc_account": pd.NA,
                "xbrl_factoid_original": new_fact,
                "is_within_table_calc": True,
                "row_type_xbrl": "reported_value",
            }
            for new_fact in ["megawatt_hours_purchased", "purchased_mwh"]
        ]
        new_facts = pd.DataFrame(facts_to_add).convert_dtypes()
        return pd.concat([tbl_meta, new_facts])


class ElectricEnergyDispositionsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`electric_energy_dispositions_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_ENERGY_DISPOSITIONS_FERC1
    has_unique_record_ids: bool = False


class UtilityPlantSummaryFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`utility_plant_summary_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.UTILITY_PLANT_SUMMARY_FERC1
    has_unique_record_ids: bool = False

    def process_xbrl(
        self: Self, raw_xbrl_instant: pd.DataFrame, raw_xbrl_duration: pd.DataFrame
    ) -> pd.DataFrame:
        """Remove the end-of-previous-year instant data."""
        all_current_year = raw_xbrl_instant[
            raw_xbrl_instant["date"].astype("datetime64[ns]").dt.year
            == raw_xbrl_instant["report_year"].astype("int64")
        ]
        return super().process_xbrl(all_current_year, raw_xbrl_duration)

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Do the default metadata processing plus add a new factoid.

        The new factoid cooresponds to the aggregated factoid in
        :meth:`aggregated_xbrl_factoids`.
        """
        tbl_meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
        # things that could be grabbed from a aggregated_xbrl_factoids param
        new_factoid_name = (
            "utility_plant_in_service_classified_and_property_under_capital_leases"
        )
        # point this new aggregated factiod to the PIS table's equivilant when the
        # subdimensions line up
        calc = [
            {
                "name": "electric_plant_in_service_and_completed_construction_not_classified_electric",
                "weight": 1.0,
                "source_tables": ["plant_in_service_ferc1"],
                "utility_type": "electric",
            }
        ]
        new_fact = pd.DataFrame(
            {
                "xbrl_factoid": [new_factoid_name],
                "calculations": [json.dumps(calc)],
                "balance": ["debit"],
                "ferc_account": [pd.NA],
                "xbrl_factoid_original": [new_factoid_name],
                "is_within_table_calc": [False],
                "row_type_xbrl": ["calculated_value"],
            }
        ).convert_dtypes()

        tbl_meta = pd.concat([tbl_meta, new_fact]).reset_index(drop=True)
        return tbl_meta

    def transform_main(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Default transforming, plus spot fixing and building aggregate xbrl_factoid."""
        # we want to aggregate the factoids first here bc merge_xbrl_metadata is done
        # at the end of super().transform_main
        df = (
            self.aggregated_xbrl_factoids(df)
            .pipe(super().transform_main)
            .pipe(self.spot_fix_bad_signs)
        )
        return df

    def aggregated_xbrl_factoids(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate xbrl_factoids records for linking to :ref:`plant_in_service_ferc1`.

        This table has two ``xbrl_factoid`` which can be linked via calcuations to one
        ``xbrl_factoid`` in the :ref:`plant_in_service_ferc1`. Doing this 2:1 linkage
        would be fine in theory. But the :ref:`plant_in_service_ferc1` is in most senses
        the table with the more details and of our desire to build tree-link
        relationships between factoids, we need to build a new factoid to link in a 1:1
        manner between this table and the :ref:`plant_in_service_ferc1`.

        We'll also add this factoid into the metadata via :meth:`process_xbrl_metadata`
        and add the linking calculation via :meth:`apply_xbrl_calculation_fixes`.
        """
        # these guys could be params
        factoids_to_agg = [
            "utility_plant_in_service_classified",
            "utility_plant_in_service_property_under_capital_leases",
        ]
        new_factoid_name = (
            "utility_plant_in_service_classified_and_property_under_capital_leases"
        )
        cols_to_agg = ["ending_balance"]
        # grab some key infor for the actual aggregation
        xbrl_factoid_name = self.params.xbrl_factoid_name
        pks = pudl.metadata.classes.Resource.from_id(
            self.table_id.value
        ).schema.primary_key
        pks_wo_factoid = [col for col in pks if col != xbrl_factoid_name]

        agg_mask = df[xbrl_factoid_name].isin(factoids_to_agg)
        agg_df = (
            df[agg_mask]
            .groupby(pks_wo_factoid, as_index=False, dropna=False)[cols_to_agg]
            .sum(min_count=1)
            .assign(**{xbrl_factoid_name: new_factoid_name})
        )
        # note: this results in the "loss" of non-pk columns like record_id - which
        # seems appropriate imo. still flag a warning
        missing_cols = [
            col for col in df.columns if col not in list(agg_df.columns) + ["record_id"]
        ]
        logger.warning(
            f"Post-aggregating a new xbrl_factoid, we are missing the following columns: {missing_cols}"
        )
        # squish em back together
        df = pd.concat([df, agg_df]).reset_index(drop=True)
        return df

    def spot_fix_bad_signs(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Spot fix depreciation_utility_plant_in_service records with bad signs."""
        primary_keys = [
            "report_year",
            "utility_id_ferc1",
            "utility_type",
            "utility_plant_asset_type",
        ]

        spot_fix_pks = [
            (
                2012,
                156,
                "total",
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
            ),
            (
                2012,
                156,
                "total",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
            ),
            (2012, 156, "total", "depreciation_utility_plant_in_service"),
            (
                2012,
                156,
                "electric",
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
            ),
            (
                2012,
                156,
                "electric",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
            ),
            (2012, 156, "electric", "depreciation_utility_plant_in_service"),
            (
                2013,
                170,
                "total",
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
            ),
            (
                2013,
                170,
                "total",
                "amortization_of_other_utility_plant_utility_plant_in_service",
            ),
            (2013, 170, "total", "amortization_of_plant_acquisition_adjustment"),
            (
                2013,
                170,
                "total",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
            ),
            (2013, 170, "total", "depreciation_utility_plant_in_service"),
            (
                2013,
                170,
                "electric",
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
            ),
            (
                2013,
                170,
                "electric",
                "amortization_of_other_utility_plant_utility_plant_in_service",
            ),
            (2013, 170, "electric", "amortization_of_plant_acquisition_adjustment"),
            (
                2013,
                170,
                "electric",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
            ),
            (2013, 170, "electric", "depreciation_utility_plant_in_service"),
            (
                2007,
                393,
                "electric",
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
            ),
            (
                2007,
                393,
                "electric",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
            ),
            (2007, 393, "electric", "depreciation_utility_plant_in_service"),
            (
                2007,
                393,
                "total",
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
            ),
            (
                2007,
                393,
                "total",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
            ),
            (2007, 393, "total", "depreciation_utility_plant_in_service"),
        ]

        spot_fix_pks += [
            (year, 211, utility_type, column_name)
            for year in [2006] + list(range(2009, 2021))
            for utility_type in ["electric", "total"]
            for column_name in [
                "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
                "amortization_of_other_utility_plant_utility_plant_in_service",
                "depreciation_amortization_and_depletion_utility_plant_in_service",
                "depreciation_utility_plant_in_service",
            ]
        ]

        # Par down spot fixes to account for fast tests where not all years are used
        df_years = df.report_year.unique().tolist()
        spot_fix_pks = [x for x in spot_fix_pks if x[0] in df_years]
        logger.info(f"{self.table_id.value}: Spotfixing {len(spot_fix_pks)} records.")

        if spot_fix_pks:
            # Create a df of the primary key of the records you want to fix
            df_keys = pd.DataFrame(spot_fix_pks, columns=primary_keys).set_index(
                primary_keys
            )
            df = df.set_index(primary_keys)
            # Flip the signs for the values in "ending balance" all records in the original
            # df that appear in the primary key df
            df.loc[df_keys.index, "ending_balance"] *= -1
            # All of these are flipping negative values to positive values,
            # so let's make sure that's what happens
            flipped_values = df.loc[df_keys.index]
            if (flipped_values["ending_balance"] < 0).any():
                raise AssertionError("None of these spot fixes should be negative")
            df = df.reset_index()

        return apply_pudl_dtypes(df, group="ferc1")


class BalanceSheetLiabilitiesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`balance_sheet_liabilities_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.BALANCE_SHEET_LIABILITIES
    has_unique_record_ids: bool = False

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, assign utility type.
        """
        return (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="total")
        )

    @cache_df(key="main")
    def transform_main(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Duplicate data that appears in multiple distinct calculations.

        There is a one case in which exactly the same data values are referenced in
        multiple calculations which can't be resolved by choosing one of the
        referenced values as the canonical location for that data. In order to preserve
        all of the calculation structure, we need to duplicate those records in the
        data, the metadata, and the calculation specifications.  Here we duplicate the
        data and associated it with newly defined facts, which we will also add to
        the metadata and calculations.
        """
        df = super().transform_main(df)
        facts_to_duplicate = [
            "long_term_portion_of_derivative_instrument_liabilities",
            "long_term_portion_of_derivative_instrument_liabilities_hedges",
        ]
        new_data = (
            df[df.liability_type.isin(facts_to_duplicate)]
            .copy()
            .assign(liability_type=lambda x: "less_" + x.liability_type)
        )

        return pd.concat([df, new_data]).assign(utility_type="total")

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Perform default xbrl metadata processing plus adding 2 new xbrl_factoids.

        We add two new factoids which are defined (by PUDL) only for the DBF data, and
        also duplicate and redefine several factoids which are referenced in multiple
        calculations and need to be distinguishable from each other.

        Note: we should probably parameterize this and add it into the standard
        :meth:`process_xbrl_metadata`.
        """
        tbl_meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
        facts_to_duplicate = [
            "long_term_portion_of_derivative_instrument_liabilities",
            "long_term_portion_of_derivative_instrument_liabilities_hedges",
        ]
        duplicated_facts = (
            tbl_meta[tbl_meta.xbrl_factoid.isin(facts_to_duplicate)]
            .copy()
            .assign(
                xbrl_factoid=lambda x: "less_" + x.xbrl_factoid,
                xbrl_factoid_original=lambda x: "less_" + x.xbrl_factoid_original,
                balance="credit",
            )
        )
        facts_to_add = [
            {
                "xbrl_factoid": new_fact,
                "calculations": "[]",
                "balance": "credit",
                "ferc_account": pd.NA,
                "xbrl_factoid_original": new_fact,
                "is_within_table_calc": True,
                "row_type_xbrl": "reported_value",
            }
            for new_fact in [
                "accumulated_deferred_income_taxes",
            ]
        ]

        new_facts = pd.DataFrame(facts_to_add).convert_dtypes()
        return pd.concat([tbl_meta, new_facts, duplicated_facts]).reset_index(drop=True)


class BalanceSheetAssetsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`balance_sheet_assets_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.BALANCE_SHEET_ASSETS_FERC1
    has_unique_record_ids: bool = False

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, assign utility type.
        """
        return (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="total")
        )

    @cache_df(key="main")
    def transform_main(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Duplicate data that appears in multiple distinct calculations.

        There is a one case in which exactly the same data values are referenced in
        multiple calculations which can't be resolved by choosing one of the
        referenced values as the canonical location for that data. In order to preserve
        all of the calculation structure, we need to duplicate those records in the
        data, the metadata, and the calculation specifications.  Here we duplicate the
        data and associated it with newly defined facts, which we will also add to
        the metadata and calculations.
        """
        df = super().transform_main(df).assign(utility_type="total")
        facts_to_duplicate = [
            "noncurrent_portion_of_allowances",
            "derivative_instrument_assets_long_term",
            "derivative_instrument_assets_hedges_long_term",
        ]
        new_data = (
            df[df.asset_type.isin(facts_to_duplicate)]
            .copy()
            .assign(asset_type=lambda x: "less_" + x.asset_type)
        )

        return pd.concat([df, new_data])

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Default xbrl metadata processing plus some error correction.

        We add two new factoids which are defined (by PUDL) only for the DBF data, and
        also duplicate and redefine several factoids which are referenced in multiple
        calculations and need to be distinguishable from each other.

        Note: we should probably parameterize this and add it into the standard
        :meth:`process_xbrl_metadata`.
        """
        tbl_meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)

        facts_to_duplicate = [
            "noncurrent_portion_of_allowances",
            "derivative_instrument_assets_long_term",
            "derivative_instrument_assets_hedges_long_term",
        ]
        duplicated_facts = (
            tbl_meta[tbl_meta.xbrl_factoid.isin(facts_to_duplicate)]
            .copy()
            .assign(
                xbrl_factoid=lambda x: "less_" + x.xbrl_factoid,
                xbrl_factoid_original=lambda x: "less_" + x.xbrl_factoid_original,
                balance="credit",
            )
        )
        facts_to_add = [
            {
                "xbrl_factoid": new_fact,
                "calculations": "[]",
                "balance": "credit",
                "ferc_account": pd.NA,
                "xbrl_factoid_original": new_fact,
                "is_within_table_calc": True,
                "row_type_xbrl": "reported_value",
            }
            for new_fact in [
                "special_funds_all",
                "nuclear_fuel",
                "preliminary_natural_gas_and_other_survey_and_investigation_charges",
            ]
        ]
        new_facts = pd.DataFrame(facts_to_add).convert_dtypes()
        return pd.concat([tbl_meta, new_facts, duplicated_facts]).assign(
            utility_type="total"
        )


class IncomeStatementFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for the :ref:`income_statement_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.INCOME_STATEMENT_FERC1
    has_unique_record_ids: bool = False

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Perform default xbrl metadata processing plus adding a new xbrl_factoid.

        Note: we should probably parameterize this and add it into the standard
        :meth:`process_xbrl_metadata`.
        """
        tbl_meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
        facts_to_add = {
            "xbrl_factoid": ["miscellaneous_deductions"],
            "calculations": ["[]"],
            "balance": ["debit"],
            "ferc_account": [pd.NA],
            "xbrl_factoid_original": ["miscellaneous_deductions"],
            "is_within_table_calc": [True],
            "row_type_xbrl": ["reported_value"],
        }
        new_facts = pd.DataFrame(facts_to_add).convert_dtypes()
        return pd.concat([tbl_meta, new_facts])

    def process_dbf(self: Self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
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

    def transform_main(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop duplicate records from f1_income_stmnt.

        Because net_utility_operating_income is reported on both page 1 and 2 of the
        form, it ends up introducing a bunch of duplicated records, so we need to drop
        one of them. Since the value is used in the calculations that are part of the
        second page, we'll drop it from the first page.
        """
        df = super().transform_main(df)
        df = df[
            ~(
                (df.record_id.str.startswith("f1_income_stmnt_"))
                & (df.income_type == "net_utility_operating_income")
            )
        ]
        return apply_pudl_dtypes(df, group="ferc1")


class RetainedEarningsFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`retained_earnings_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.RETAINED_EARNINGS_FERC1
    has_unique_record_ids: bool = False

    current_year_types: set[str] = {
        "unappropriated_undistributed_subsidiary_earnings",
        "unappropriated_retained_earnings",
    }
    previous_year_types: set[str] = {
        "unappropriated_undistributed_subsidiary_earnings_previous_year",
        "unappropriated_retained_earnings_previous_year",
    }

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, add FERC account values for a few known values.
        """
        meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
        meta.loc[
            meta.xbrl_factoid
            == "transfers_from_unappropriated_undistributed_subsidiary_earnings",
            "ferc_account",
        ] = "216.1"
        meta.loc[
            meta.xbrl_factoid
            == "appropriated_retained_earnings_including_reserve_amortization",
            "ferc_account",
        ] = "215_and_215.1"
        meta.loc[
            meta.xbrl_factoid == "retained_earnings",
            "ferc_account",
        ] = "215_and_215.1_and_216"
        meta.loc[
            meta.xbrl_factoid == "unappropriated_retained_earnings",
            "ferc_account",
        ] = "216"
        meta.loc[
            meta.xbrl_factoid == "equity_in_earnings_of_subsidiary_companies",
            "ferc_account",
        ] = "418.1"

        # NOTE: Needs to happen before `process_xbrl_metadata_calculations`
        facts_to_add = [
            {
                "xbrl_factoid": new_fact,
                "calculations": "[]",
                "balance": "credit",
                "ferc_account": pd.NA,
                "xbrl_factoid_original": new_fact,
                "is_within_table_calc": True,
                "row_type_xbrl": "reported_value",
            }
            for new_fact in [
                "unappropriated_retained_earnings_previous_year",
                "unappropriated_undistributed_subsidiary_earnings_previous_year",
            ]
        ]

        new_facts = pd.DataFrame(facts_to_add).convert_dtypes()
        return pd.concat([meta, new_facts])

    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Preform generic :meth:`process_dbf`, plus deal with duplicates.

        Along with the standard processing in
        :meth:`Ferc1AbstractTableTransformer.process_dbf`, this method runs:
        * :meth:`targeted_drop_duplicates_dbf`
        * :meth:`reconcile_double_year_earnings_types_dbf`
        """
        processed_dbf = (
            super()
            .process_dbf(raw_dbf)
            .pipe(self.targeted_drop_duplicates_dbf)
            .pipe(self.reconcile_double_year_earnings_types_dbf)
        )
        return processed_dbf

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, assign utility type.
        """
        return (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="total")
        )

    @cache_df("main")
    def transform_main(self, df):
        """Add `_previous_year` factoids after standard transform_main.

        Add `_previous_year` factoids for `unappropriated_retained_earnings` and
        `unappropriated_undistributed_subsidiary_earnings` after standard
        transform_main. This should only affect XBRL data, but we do it after merging to
        enable access to DBF data to fill this in as well.
        """
        df = super().transform_main(df).pipe(self.add_previous_year_factoid)
        return df.assign(utility_type="total")

    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check ``_previous_year`` factoids for consistency after the transformation is done."""
        return super().transform_end(df).pipe(self.check_double_year_earnings_types)

    def check_double_year_earnings_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check previous year/current year factoids for consistency.

        The terminology can be very confusing - here are the expectations:

        1. "inter year consistency": earlier year's "current starting/end
           balance" == later year's "previous starting/end balance"
        2. "intra year consistency": each year's "previous ending balance" ==
           "current starting balance"
        """
        current_year_facts = df.loc[df.earnings_type.isin(self.current_year_types)]
        previous_year_facts = df.loc[
            df.earnings_type.isin(self.previous_year_types)
        ].pipe(
            lambda df: df.assign(
                earnings_type=df.earnings_type.str.removesuffix("_previous_year")
            )
        )

        # inter year comparison requires us to match the earlier year's current facts
        # to the later year's previous facts, so we add 1 to the report year & merge.
        earlier_years = current_year_facts.assign(
            report_year=current_year_facts.report_year + 1
        )
        later_years = previous_year_facts
        idx = ["utility_id_ferc1", "report_year", "earnings_type"]
        inter_year_facts = earlier_years.merge(
            later_years,
            on=idx,
            suffixes=["_earlier", "_later"],
        ).dropna(
            subset=[
                "starting_balance_earlier",
                "starting_balance_later",
                "ending_balance_earlier",
                "ending_balance_later",
            ]
        )

        intra_year_facts = previous_year_facts.merge(
            current_year_facts, on=idx, suffixes=["_previous", "_current"]
        )

        assert_cols_areclose(
            df=inter_year_facts,
            a_cols=["starting_balance_earlier"],
            b_cols=["starting_balance_later"],
            mismatch_threshold=0.05,
            message="'Current starting balance' for year X-1 doesn't match "
            "'previous starting balance' for year X.",
        )

        assert_cols_areclose(
            df=inter_year_facts,
            a_cols=["ending_balance_earlier"],
            b_cols=["ending_balance_later"],
            mismatch_threshold=0.05,
            message="'Current ending balance' for year X-1 doesn't match "
            "'previous ending balance' for year X.",
        )

        assert_cols_areclose(
            df=intra_year_facts,
            a_cols=["ending_balance_previous"],
            b_cols=["starting_balance_current"],
            mismatch_threshold=0.05,
            message="'Previous year ending balance' should be the same as "
            "'current year starting balance' for all years!",
        )

        return df

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

    def reconcile_double_year_earnings_types_dbf(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Reconcile current and past year data reported in 1 report_year.

        The DBF table includes two different earnings types that have: "Begining of
        Period" and "End of Period" rows. But the table has both an amount column that
        corresponds to a balance and a starting balance column. For these two earnings
        types, this means that there is in effect two years of data in this table for
        each report year: a starting and ending balance for the pervious year and a
        starting and ending balance for the current year. The ending balance for the
        previous year should be the same as the starting balance for the current year.

        We need to keep both pieces of data in order to calculate `ending_balances`,
        so we want to check these assumptions, extract as much information from these
        two years of data, and keep both records for each of these two earnings
        types for each utility.

        Raises:
            AssertionError: There are a very small number of instances in which the
                ending balance from the previous year does not match the starting
                balance from the current year. The % of these non-matching instances
                should be less than 2% of the records with these date duplicative
                earnings types.
        """
        logger.info(f"{self.table_id.value}: Reconciling previous year's data.")
        # DBF has _current_year suffix while PUDL core version does not
        current_year_types = [
            "unappropriated_undistributed_subsidiary_earnings_current_year",
            "unappropriated_retained_earnings_current_year",
        ]
        previous_year_types = [
            "unappropriated_undistributed_subsidiary_earnings_previous_year",
            "unappropriated_retained_earnings_previous_year",
        ]
        # assign() copies, so no need to double copy when extracting this slice
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
            [df[~df.earnings_type.isin(current_year_types)], date_dupe_types]
        )

        # Since we've created an ending balance column, we should use the 'amount'
        # value to fill it across the table and drop the amount column.
        df.ending_balance = df.ending_balance.fillna(df.amount)
        df = df.drop("amount", axis=1)

        return df

    def add_previous_year_factoid(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create ``*_previous_year`` factoids for XBRL data.

        XBRL doesn't include the previous year's data, but DBF does - so we try to
        check that year X's ``*_current_year`` factoid has the same value as year X+1's
        ``*_previous_year`` factoid.

        To do this, we need to add some ``*_previous_year`` factoids to the XBRL data.
        """
        current_year_facts = df[df.earnings_type.isin(self.current_year_types)]
        previous_year_facts = df[df.earnings_type.isin(self.previous_year_types)]

        missing_years = set(current_year_facts.report_year.unique()) - set(
            previous_year_facts.report_year.unique()
        )

        to_copy_forward = current_year_facts[
            (current_year_facts.report_year + 1).isin(missing_years)
        ]

        idx = [
            "utility_id_ferc1",
            "earnings_type",
            "report_year",
        ]
        inferred_previous_year_facts = (
            to_copy_forward.assign(
                report_year=to_copy_forward.report_year + 1,
                new_earnings_type=to_copy_forward.earnings_type + "_previous_year",
            )
            .merge(current_year_facts[idx])
            .drop(columns=["earnings_type"])
            .rename(columns={"new_earnings_type": "earnings_type"})
            .assign(row_type_xbrl="reported_value")
        )
        return pd.concat([df, inferred_previous_year_facts])

    def deduplicate_xbrl_factoid_xbrl_metadata(self, tbl_meta) -> pd.DataFrame:
        """Deduplicate the xbrl_metadata based on the ``xbrl_factoid``.

        The metadata relating to dollar_value column *generally* had the same name as
        the renamed xbrl_factoid. we'll double check that we a) didn't remove too many
        factoid's by doing this AND that we have a fully deduped output below. In an
        ideal world, we would have multiple pieces of metadata information (like
        calucations and ferc account #'s), for every single :meth:`wide_to_tidy` value
        column.

        Note: This is **almost** the same as the method for
        :ref:`electric_operating_revenues_ferc1`. If we wanted to lean into this
        version of deduplication more generally this might be a fine way start to an
        abstraction, but ideally we wouldn't need to dedupe this at all and instead
        enable metadata for every value column from :meth:`wide_to_tidy`.
        """
        dupes_masks = tbl_meta.duplicated(subset=["xbrl_factoid"], keep=False)
        non_dupes = tbl_meta[~dupes_masks]
        dupes = tbl_meta[dupes_masks]

        deduped = dupes[dupes.xbrl_factoid == dupes.xbrl_factoid_original]
        tbl_meta_cleaned = pd.concat([non_dupes, deduped])
        assert ~tbl_meta_cleaned.duplicated(subset=["xbrl_factoid"]).all()

        missing = {
            factoid
            for factoid in tbl_meta.xbrl_factoid.unique()
            if factoid not in tbl_meta_cleaned.xbrl_factoid.unique()
        }
        if missing:
            raise AssertionError(
                "We expected to find no missing xbrl_factoid's after deduplication "
                f"but found {missing}"
            )
        return tbl_meta_cleaned


class DepreciationAmortizationSummaryFerc1TableTransformer(
    Ferc1AbstractTableTransformer
):
    """Transformer class for :ref:`depreciation_amortization_summary_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.DEPRECIATION_AMORTIZATION_SUMMARY_FERC1
    has_unique_record_ids: bool = False

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, add FERC account values for a few known values.
        """
        meta = (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="electric")
        )
        # logger.info(meta)
        meta.loc[
            meta.xbrl_factoid == "depreciation_expense",
            "ferc_account",
        ] = "403"
        meta.loc[
            meta.xbrl_factoid == "depreciation_expense_asset_retirement",
            "ferc_account",
        ] = "403.1"
        meta.loc[
            meta.xbrl_factoid == "amortization_limited_term_electric_plant",
            "ferc_account",
        ] = "404"
        meta.loc[
            meta.xbrl_factoid == "amortization_other_electric_plant",
            "ferc_account",
        ] = "405"
        return meta

    @cache_df("main")
    def transform_main(self, df):
        """After standard transform_main, assign utility type as electric."""
        df = super().transform_main(df).assign(utility_type="electric")
        return df


class ElectricPlantDepreciationChangesFerc1TableTransformer(
    Ferc1AbstractTableTransformer
):
    """Transformer class for :ref:`electric_plant_depreciation_changes_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_PLANT_DEPRECIATION_CHANGES_FERC1
    has_unique_record_ids: bool = False

    def convert_xbrl_metadata_json_to_df(
        self: Self, xbrl_metadata_json
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Warning: The calculations in this table are currently being corrected using
        reconcile_table_calculations(), but they still contain high rates of error.
        This function replaces the name of the single balance column reported in the
        XBRL Instant table with starting_balance / ending_balance. We pull those two
        values into their own separate labeled rows, each of which should get the
        metadata from the original column. We do this pre-processing before we
        call the main function in order for the calculation fixes and renaming to work
        as expected.
        """
        new_xbrl_metadata_json = xbrl_metadata_json
        # Get instant metadata
        instant = pd.json_normalize(new_xbrl_metadata_json["instant"])
        # Duplicate instant metadata, and add starting/ending suffix
        # should just be balance begining of year
        instant = pd.concat([instant] * 2).reset_index(drop=True)
        instant["name"] = instant["name"] + ["_starting_balance", "_ending_balance"]
        # Return to JSON format in order to continue processing
        new_xbrl_metadata_json["instant"] = json.loads(
            instant.to_json(orient="records")
        )
        self.xbrl_metadata_json = new_xbrl_metadata_json
        tbl_meta = super().convert_xbrl_metadata_json_to_df(new_xbrl_metadata_json)
        return tbl_meta

    @cache_df("dbf")
    def process_dbf(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Accumulated Depreciation table specific DBF cleaning operations.

        The XBRL reports a utility_type which is always electric in this table, but
        which may be necessary for differentiating between different values when this
        data is combined with other tables. The DBF data doesn't report this value so
        we are adding it here for consistency across the two data sources.

        Also rename the ``ending_balance_accounts`` to ``ending_balance``
        """
        df = super().process_dbf(raw_df).assign(utility_type="electric")
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

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Create a metadata table with the one factoid we've assigned to this table.

        Instead of adding facts to the metdata like a lot of the other table-specific
        :meth:`convert_xbrl_metadata_json_to_df`, this method creates a metadata table
        with one singular ``xbrl_factoid``. We assign that factoid to the table in
        :meth:`transform_main`.
        """
        single_table_fact = [
            {
                "xbrl_factoid": fact,
                "calculations": "[]",
                "balance": "credit",
                "ferc_account": pd.NA,
                "xbrl_factoid_original": fact,
                "is_within_table_calc": True,
                "row_type_xbrl": "reported_value",
            }
            for fact in ["accumulated_depreciation"]
        ]
        tbl_meta = pd.DataFrame(single_table_fact).convert_dtypes()
        return tbl_meta

    def raw_xbrl_factoid_to_pudl_name(
        self,
        col_name_xbrl: str,
    ) -> str:
        """Return the one fact name for this table.

        We've artificially assigned this table to have one ``xbrl_factoid`` during
        :meth:`transform_main`. Because this table only has one value for its
        ``xbrl_factoid`` column, all ``col_name_xbrl`` should be converted to
        "accumulated_depreciation".
        """
        return "accumulated_depreciation"

    @cache_df("dbf")
    def process_dbf(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Accumulated Depreciation table specific DBF cleaning operations.

        The XBRL reports a utility_type which is always electric in this table, but
        which may be necessary for differentiating between different values when this
        data is combined with other tables. The DBF data doesn't report this value so we
        are adding it here for consistency across the two data sources.
        """
        return super().process_dbf(raw_df).assign(utility_type="electric")

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

    @cache_df("main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ``depreciation_type`` then run default :meth:`transform_main`.

        We are adding ``depreciation_type`` as the ``xbrl_factoid`` column for this
        table with one value ("accumulated_depreciation") across the whole table. This
        table has multiple "dimension" columns such as ``utility_type`` and
        ``plant_function`` which differentiate what slice of a utility's assets each
        record pertains to. We added this new column as the ``xbrl_factoid`` of the
        table instead of using one of the dimensions of the table so that the table can
        conform to the same patern of treatment for these dimension columns.
        """
        df = df.assign(depreciation_type="accumulated_depreciation").pipe(
            super().transform_main
        )
        # convert this **one** utility's depreciation $$ from negative -> +
        # this was found through checking the inter-table calculations in the explosion
        # process. The one factoid in this table is linked with
        # depreciation_utility_plant_in_service in the utility_plant_summary_ferc1 table.
        # the values in both tables are almost always postive. Not always & there are
        # some logical reasons why depreciation can sometimes be negative. Nonetheless,
        # for this one utility, all of its values in utility_plant_summary_ferc1 are
        # postive while nearly all of the $s over here are negative. No other utility
        # has as many -$ which tells me this is a data entry error.
        # see https://github.com/catalyst-cooperative/pudl/issues/2703 for more details
        negative_util_mask = df.utility_id_ferc1 == 211
        df.loc[negative_util_mask, "ending_balance"] = abs(
            df.loc[negative_util_mask, "ending_balance"]
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

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Default XBRL metadata processing and add a DBF-only xblr factoid.

        Note: we should probably parameterize this and add it into the standard
        :meth:`process_xbrl_metadata`.
        """
        tbl_meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
        dbf_only_facts = [
            {
                "xbrl_factoid": dbf_only_fact,
                "calculations": "[]",
                "balance": "credit",
                "ferc_account": pd.NA,
                "xbrl_factoid_original": dbf_only_fact,
                "is_within_table_calc": True,
                "row_type_xbrl": "reported_value",
            }
            for dbf_only_fact in ["load_dispatching_transmission_expense"]
        ]
        dbf_only_facts = pd.DataFrame(dbf_only_facts).convert_dtypes()
        return pd.concat([tbl_meta, dbf_only_facts]).assign(utility_type="electric")

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, add utility type.
        """
        return (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="electric")
        )

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Process DBF but drop a bad row that is flagged by drop_duplicates."""
        return super().process_dbf(self.targeted_drop_duplicates_dbf(raw_dbf))

    @cache_df("main")
    def transform_main(self, df):
        """After standard transform_main, assign utility type as electric."""
        return super().transform_main(df).assign(utility_type="electric")


class ElectricOperatingRevenuesFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`electric_operating_revenues_ferc1` table."""

    table_id: TableIdFerc1 = TableIdFerc1.ELECTRIC_OPERATING_REVENUES_FERC1
    has_unique_record_ids: bool = False

    def deduplicate_xbrl_factoid_xbrl_metadata(
        self, tbl_meta: pd.DataFrame
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Employ the standard process for processing metadata. Then remove duplication on
        the basis of the ``xbrl_factoid``. This table used :meth:`wide_to_tidy` with three
        seperate value columns. Which results in one ``xbrl_factoid`` referencing three
        seperate data columns. This method grabs only one piece of metadata for each
        renamed ``xbrl_factoid``, preferring the calculated value or the factoid
        referencing the dollar columns.

        In an ideal world, we would have multiple pieces of metadata information (like
        calucations and ferc account #'s), for every single :meth:`wide_to_tidy` value
        column. We would probably want to employ that across the board - adding suffixes
        or something like that to stack the metadata in a similar fashion that we stack
        the data.
        """
        dupes_masks = tbl_meta.duplicated(subset=["xbrl_factoid"], keep=False)
        non_dupes = tbl_meta[~dupes_masks]
        dupes = tbl_meta[dupes_masks]
        # the metadata relating to dollar_value column *generally* had the same name as
        # the renamed xbrl_factoid. the outliers here are these two that have calcs for
        # the factoid we want to keep (we could also id them w/ their og factoid names
        # if that would be more straightforward)
        deduped = dupes[
            (dupes.xbrl_factoid == dupes.xbrl_factoid_original)
            | (
                dupes.xbrl_factoid.isin(["small_or_commercial", "large_or_industrial"])
                & (dupes.calculations != "[]")
            )
        ]
        tbl_meta_cleaned = pd.concat([non_dupes, deduped])
        assert ~tbl_meta_cleaned.duplicated(subset=["xbrl_factoid"]).all()

        # double check that we're getting only the guys we want
        missing = {
            factoid
            for factoid in tbl_meta.xbrl_factoid.unique()
            if factoid not in tbl_meta_cleaned.xbrl_factoid.unique()
        }
        if missing:
            raise AssertionError(
                "We expected to find no missing xbrl_factoid's after deduplication "
                f"but found {missing}"
            )
        return tbl_meta_cleaned

    @cache_df("process_xbrl_metadata")
    def process_xbrl_metadata(
        self: Self,
        xbrl_metadata_converted: pd.DataFrame,
        xbrl_calculations: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Beyond the standard :meth:`Ferc1AbstractTableTransformer.process_xbrl_metadata`
        processing, add utility type.
        """
        return (
            super()
            .process_xbrl_metadata(xbrl_metadata_converted, xbrl_calculations)
            .assign(utility_type="electric")
        )

    @cache_df("main")
    def transform_main(self, df):
        """Add duplicate removal after standard transform_main & assign utility type."""
        return (
            super()
            .transform_main(df)
            .pipe(self.targeted_drop_duplicates)
            .assign(utility_type="electric")
        )

    @cache_df("main")
    def targeted_drop_duplicates(self, df):
        """Drop one duplicate records from 2011, utility_id_ferc1 295."""
        dupe_mask = (
            (df.utility_id_ferc1 == 295)
            & (df.report_year == 2011)
            & ((df.dollar_value == 3.33e8) | (df.dollar_value == 3.333e9))
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

    def convert_xbrl_metadata_json_to_df(
        self: Self,
        xbrl_metadata_json: dict[Literal["instant", "duration"], list[dict[str, Any]]],
    ) -> pd.DataFrame:
        """Transform the metadata to reflect the transformed data.

        Replace the name of the balance column reported in the XBRL Instant table with
        starting_balance / ending_balance since we pull those two values into their own
        separate labeled rows, each of which should get the original metadata for the
        Instant column.
        """
        meta = super().convert_xbrl_metadata_json_to_df(xbrl_metadata_json)
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


FERC1_TFR_CLASSES: Mapping[str, type[Ferc1AbstractTableTransformer]] = {
    "fuel_ferc1": FuelFerc1TableTransformer,
    "plants_steam_ferc1": PlantsSteamFerc1TableTransformer,
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


def ferc1_transform_asset_factory(
    table_name: str,
    tfr_class: Ferc1AbstractTableTransformer,
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
        tfr_class: A transformer class cooresponding to the table_name.
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

    ins = {f"raw_dbf__{tn}": AssetIn(f"raw_ferc1_dbf__{tn}") for tn in dbf_tables}
    ins |= {
        f"raw_xbrl_instant__{tn}": AssetIn(f"raw_ferc1_xbrl__{tn}_instant")
        for tn in xbrl_tables
    }
    ins |= {
        f"raw_xbrl_duration__{tn}": AssetIn(f"raw_ferc1_xbrl__{tn}_duration")
        for tn in xbrl_tables
    }
    ins["clean_xbrl_metadata_json"] = AssetIn("clean_xbrl_metadata_json")

    table_id = TableIdFerc1(table_name)

    @asset(name=table_name, ins=ins, io_manager_key=io_manager_key)
    def ferc1_transform_asset(**kwargs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Transform a FERC Form 1 table.

        Args:
            raw_dbf: raw dbf table.
            raw_xbrl_instant: raw XBRL instant table.
            raw_xbrl_duration: raw XBRL duration table.
            clean_xbrl_metadata_json: XBRL metadata json for all tables.

        Returns:
            transformed FERC Form 1 table.
        """
        # TODO: split the key by __, then groupby, then concatenate
        clean_xbrl_metadata_json = kwargs["clean_xbrl_metadata_json"]
        if generic:
            transformer = tfr_class(
                xbrl_metadata_json=clean_xbrl_metadata_json[table_name],
                table_id=table_id,
            )
        else:
            transformer = tfr_class(
                xbrl_metadata_json=clean_xbrl_metadata_json[table_name]
            )

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
    assets = []
    for table_name, tfr_class in FERC1_TFR_CLASSES.items():
        # Bespoke exception. fuel must come before steam b/c fuel proportions are used to
        # aid in FERC plant ID assignment.
        if table_name != "plants_steam_ferc1":
            assets.append(ferc1_transform_asset_factory(table_name, tfr_class))
    return assets


ferc1_assets = create_ferc1_transform_assets()


@asset(io_manager_key="pudl_sqlite_io_manager")
def plants_steam_ferc1(
    clean_xbrl_metadata_json: dict[str, dict[str, list[dict[str, Any]]]],
    raw_ferc1_dbf__f1_steam: pd.DataFrame,
    raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_duration: pd.DataFrame,
    raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_instant: pd.DataFrame,
    fuel_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Create the clean plants_steam_ferc1 table.

    Args:
            clean_xbrl_metadata_json: XBRL metadata json for all tables.
            raw_ferc1_dbf__f1_steam: Raw f1_steam table.
            raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_duration: raw XBRL duration table.
            raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_instant: raw XBRL instant table.
            fuel_ferc1: Transformed fuel_ferc1 table.

    Returns:
        Clean plants_steam_ferc1 table.
    """
    df = PlantsSteamFerc1TableTransformer(
        xbrl_metadata_json=clean_xbrl_metadata_json["plants_steam_ferc1"]
    ).transform(
        raw_dbf=raw_ferc1_dbf__f1_steam,
        raw_xbrl_instant=raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_instant,
        raw_xbrl_duration=raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_duration,
        transformed_fuel=fuel_ferc1,
    )
    return convert_cols_dtypes(df, data_source="ferc1")


def other_dimensions(table_names: list[str]) -> list[str]:
    """Get a list of the other dimension columns across all of the transformers."""
    # grab all of the dimensions columns that we are currently verifying as a part of
    # reconcile_table_calculations
    other_dimensions = [
        FERC1_TFR_CLASSES[
            table_name
        ]().params.reconcile_table_calculations.subtotal_column
        for table_name in table_names
    ]
    # remove nulls and dedupe
    other_dimensions = [sub for sub in other_dimensions if sub]
    other_dimensions = list(set(other_dimensions))
    return other_dimensions


def table_to_xbrl_factoid_name() -> dict[str, str]:
    """Build a dictionary of table name (keys) to ``xbrl_factoid`` column name."""
    return {
        table_name: transformer().params.xbrl_factoid_name
        for (table_name, transformer) in FERC1_TFR_CLASSES.items()
    }


def table_to_column_to_check() -> dict[str, list[str]]:
    """Build a dictionary of table name (keys) to column_to_check from reconcile_table_calculations."""
    return {
        table_name: transformer().params.reconcile_table_calculations.column_to_check
        for (table_name, transformer) in FERC1_TFR_CLASSES.items()
        if transformer().params.reconcile_table_calculations.column_to_check
    }


@asset(
    ins={
        table_name: AssetIn(table_name)
        for table_name in FERC1_TFR_CLASSES
        if table_name != "plants_steam_ferc1"
    }
)
def table_dimensions_ferc1(**kwargs) -> pd.DataFrame:
    """Build a table of values of dimensions observed in the transformed data tables.

    Compile a dataframe indicating what distinct values are observed in the data for
    each dimension column in association with each unique combination of ``table_name``
    and ``xbrl_factoid``. E.g. for all factoids found in the
    :ref:`electric_plant_depreciation_functional_ferc1` table,
    the only value observed for ``utility_type`` is ``electric`` and the values observed
    for ``plant_status`` include: ``future``, ``in_service``, ``leased`` and ``total``.

    We need to include the ``xbrl_factoid`` column because these dimensions can differ
    based on the ``xbrl_factoid``. So we first rename all of the columns which
    contain the ``xbrl_factoid`` using :func:`table_to_xbrl_factoid_name` rename
    dictionary. Then we concatenate all of the tables together and drop duplicates so
    we have unique instances of observed ``table_name`` and ``xbrl_factoid`` and the
    other dimension columns found in :func:`other_dimensions`.
    """
    table_to_xbrl_factoid_name_dict = table_to_xbrl_factoid_name()
    tbls = {
        name: df.assign(table_name=name).rename(
            columns={table_to_xbrl_factoid_name_dict[name]: "xbrl_factoid"}
        )
        for (name, df) in kwargs.items()
    }
    dimensions = (
        pd.concat(tbls.values())[
            ["table_name", "xbrl_factoid"]
            + other_dimensions(table_names=list(FERC1_TFR_CLASSES))
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return dimensions


@asset(
    ins={
        "clean_xbrl_metadata_json": AssetIn("clean_xbrl_metadata_json"),
        "table_dimensions_ferc1": AssetIn("table_dimensions_ferc1"),
    },
    io_manager_key=None,  # Change to sqlite_io_manager...
)
def metadata_xbrl_ferc1(**kwargs) -> pd.DataFrame:
    """Build a table of all of the tables' XBRL metadata."""
    clean_xbrl_metadata_json = kwargs["clean_xbrl_metadata_json"]
    table_dimensions_ferc1 = kwargs["table_dimensions_ferc1"]
    tbl_metas = []
    for table_name, trans in FERC1_TFR_CLASSES.items():
        tbl_meta = (
            trans(xbrl_metadata_json=clean_xbrl_metadata_json[table_name])
            .xbrl_metadata[
                [
                    "xbrl_factoid",
                    "xbrl_factoid_original",
                    "is_within_table_calc",
                ]
            ]
            .assign(table_name=table_name)
        )
        tbl_metas.append(tbl_meta)
    dimensions = other_dimensions(table_names=list(FERC1_TFR_CLASSES))
    metadata_all = (
        pd.concat(tbl_metas)
        .reset_index(drop=True)
        .assign(**{dim: pd.NA for dim in dimensions})
        .pipe(
            make_calculation_dimensions_explicit,
            table_dimensions_ferc1=table_dimensions_ferc1,
            dimensions=dimensions,
        )
    )
    return metadata_all


@asset(
    ins={
        "clean_xbrl_metadata_json": AssetIn("clean_xbrl_metadata_json"),
        "table_dimensions_ferc1": AssetIn("table_dimensions_ferc1"),
        "metadata_xbrl_ferc1": AssetIn("metadata_xbrl_ferc1"),
    },
    io_manager_key=None,  # Change to sqlite_io_manager...
)
def calculation_components_xbrl_ferc1(**kwargs) -> pd.DataFrame:
    """Create calculation-component table from table-level metadata."""
    clean_xbrl_metadata_json = kwargs["clean_xbrl_metadata_json"]
    table_dimensions_ferc1 = kwargs["table_dimensions_ferc1"]
    metadata_xbrl_ferc1 = kwargs["metadata_xbrl_ferc1"]
    # compile all of the calc comp tables.
    calc_metas = []
    for table_name, transformer in FERC1_TFR_CLASSES.items():
        calc_meta = transformer(
            xbrl_metadata_json=clean_xbrl_metadata_json[table_name]
        ).xbrl_calculations
        calc_metas.append(calc_meta)
    # squish all of the calc comp tables then add in the implicit table dimensions
    dimensions = other_dimensions(table_names=list(FERC1_TFR_CLASSES))
    calc_components = (
        pd.concat(calc_metas)
        .astype({dim: pd.StringDtype() for dim in dimensions})
        .pipe(
            make_calculation_dimensions_explicit,
            table_dimensions_ferc1,
            dimensions=dimensions,
        )
        .pipe(
            assign_parent_dimensions,
            table_dimensions=table_dimensions_ferc1,
            dimensions=dimensions,
        )
        .pipe(
            infer_intra_factoid_totals,
            meta_w_dims=metadata_xbrl_ferc1,
            table_dimensions=table_dimensions_ferc1,
            dimensions=dimensions,
        )
    )

    child_cols = ["table_name", "xbrl_factoid"]
    calc_cols = child_cols + dimensions
    calc_and_parent_cols = calc_cols + [f"{col}_parent" for col in calc_cols]

    # Defensive testing on this table!
    assert calc_components[["table_name", "xbrl_factoid"]].notnull().all(axis=1).all()

    # Let's check that all calculated components that show up in our data are
    # getting calculated.
    def check_calcs_vs_table(
        calcs: pd.DataFrame,
        checked_table: pd.DataFrame,
        idx_calcs: list[str],
        idx_table: list[str],
        how: Literal["in", "not_in"],
    ) -> pd.DataFrame:
        if how == "in":
            idx = calcs.set_index(idx_calcs).index.intersection(
                checked_table.set_index(idx_table).index
            )
        elif how == "not_in":
            idx = calcs.set_index(idx_calcs).index.difference(
                checked_table.set_index(idx_table).index
            )
        calcs_vs_table = calcs.set_index(idx_calcs).loc[idx]
        return calcs_vs_table.reset_index()

    # which calculations are missing from the metadata table?
    missing_calcs = check_calcs_vs_table(
        calcs=calc_components[
            calc_components.table_name.isin(FERC1_TFR_CLASSES.keys())
        ],
        checked_table=metadata_xbrl_ferc1,
        idx_calcs=calc_cols,
        idx_table=calc_cols,
        how="not_in",
    )
    # ensure that none of the calculation components that are missing from the metadata
    # table are from any of the exploded tables.
    if not missing_calcs.empty:
        logger.warning(
            "Calculations found in calculation components table are missing from the "
            "metadata_xbrl_ferc1 table."
        )
        # which of these missing calculations actually show up in the transformed tables?
        # This handles dbf-only calculation components, whic are added to the
        # metadata_xbrl_ferc1 table as part of each table's transformations but aren't
        # observed (or therefore present in table_dimensions_ferc1) in the fast ETL or
        # in all subsets of years. We only want to flag calculation components as
        # missing when they're actually observed in the data.
        actually_missing_kids = check_calcs_vs_table(
            calcs=missing_calcs,
            checked_table=table_dimensions_ferc1,
            idx_calcs=child_cols,
            idx_table=child_cols,
            how="in",
        )
        logger.warning(
            f"{len(actually_missing_kids)} of {len(missing_calcs)} missing calculation components observed in transformed FERC1 data."
        )
        if not actually_missing_kids.empty:
            raise AssertionError(
                f"Found missing calculations from the exploded tables:\n{actually_missing_kids=}"
            )

    check_for_calc_components_duplicates(
        calc_components,
        table_names_known_dupes=["electricity_sales_by_rate_schedule_ferc1"],
        idx=calc_and_parent_cols,
    )

    # check for parent/child duplicates. again need to remove the
    # electricity_sales_by_rate_schedule_ferc1 table. Null hack bc comparing pandas
    # nulls
    self_refs_mask = calc_components[calc_and_parent_cols].fillna("NULL HACK").apply(
        lambda x: all(x[col] == x[f"{col}_parent"] for col in calc_cols), axis=1
    ) & (calc_components.table_name != "electricity_sales_by_rate_schedule_ferc1")
    if not (parent_child_dupes := calc_components.loc[self_refs_mask]).empty:
        raise AssertionError(
            f"Found {len(parent_child_dupes)} calcuations where the parent and child "
            f"columns are identical and expected 0.\n{parent_child_dupes=}"
        )

    if not (
        unexpected_totals := unexpected_total_components(
            calc_components.convert_dtypes(), dimensions
        )
    ).empty:
        raise AssertionError(f"Found unexpected total records: {unexpected_totals}")
    # Remove convert_dtypes() once we're writing to the DB using enforce_schema()
    return calc_components.convert_dtypes()


def unexpected_total_components(
    calc_comps: pd.DataFrame, dimensions: list[str]
) -> pd.DataFrame:
    """Find unexpected components in within-fact total calculations.

    This doesn't check anything about the calcs we get from the metadata, we
    are only looking at within-fact totals which we've added ourselves.

    Finds calculation relationships where:

    - child components that do not match with parent in non-total dimensions.

      - For example, if utility_type_parent is not "total", then utility_type
        must be the same as utility_type_parent.

    - child components, that share table_name/xbrl_factoid with their parent,
      that have "total" for any dimension - these should be represented by
      *their* child components

    Args:
        calc_comps: calculation component join table
        dimensions: list of dimensions we resolved "total" values for
    """
    parent_dimensions = [f"{dim}_parent" for dim in dimensions]
    totals_mask = (
        (calc_comps[parent_dimensions] == "total").any(axis="columns")
        & (calc_comps["table_name_parent"] == calc_comps["table_name"])
        & (calc_comps["xbrl_factoid_parent"] == calc_comps["xbrl_factoid"])
    )
    calcs_with_totals = calc_comps[totals_mask]

    unexpected_links = []
    for child_dim in dimensions:
        mismatched_non_total = (
            calcs_with_totals[f"{child_dim}_parent"] != calcs_with_totals[child_dim]
        ) & (calcs_with_totals[f"{child_dim}_parent"] != "total")
        children_with_totals = calcs_with_totals[child_dim] == "total"
        unexpected_links.append(
            calcs_with_totals[mismatched_non_total | children_with_totals][
                ["table_name_parent", "xbrl_factoid_parent"]
                + parent_dimensions
                + ["table_name", "xbrl_factoid"]
                + dimensions
            ]
        )
    return pd.concat(unexpected_links)


def check_for_calc_components_duplicates(
    calc_components: pd.DataFrame, table_names_known_dupes: list[str], idx: list[str]
) -> None:
    """Check for duplicates calculation records.

    We need to remove the electricity_sales_by_rate_schedule_ferc1 bc there are
    duplicate renamed factoids in that table (originally billed/unbilled).
    """
    calc_components_test = (
        calc_components[
            ~calc_components.table_name_parent.isin(table_names_known_dupes)
        ]
        .set_index(idx)
        .sort_index()
    )
    if not calc_components_test.index.is_unique:
        raise AssertionError(
            f"Found duplicates based on {idx=} when expected none.\n"
            f"{calc_components_test[calc_components_test.index.duplicated(keep=False)]}"
        )


def make_calculation_dimensions_explicit(
    calculation_components: pd.DataFrame,
    table_dimensions_ferc1: pd.DataFrame,
    dimensions: list[str],
    parent: bool = False,
) -> pd.DataFrame:
    """Fill in null dimensions w/ the values observed in :func:`table_dimensions_ferc1`.

    In the raw XBRL metadata's calculations, there is an implicit assumption that
    calculated values are aggregated within categorical columns called Axes or
    dimensions, in addition to being grouped by date, utility, table, and fact. The
    dimensions and their values don't need to be specified explicitly in the calculation
    components because the same calculation is assumed to apply in all cases.

    We have extended this calculation system to allow independent calculations to be
    specified for different values within a given dimension. For example, the
    :ref:`utility_plant_summary_ferc1` table contains records with a variety of
    different ``utility_type`` values (gas, electric, etc.). For many combinations of
    fact and ``utility_type``, no more detailed information about the soruce of the data
    is available, but for some, and only in the case of electric utilities, much more
    detail can be found in the :ref:`plant_in_service_ferc1` table. In order to use this
    additional information when it is available, we sometimes explicitly specify
    different calculations for different values of additional dimension columns.

    This function uses the observed associations between ``table_name``,
    ``xbrl_factoid`` and the other dimension columns compiled by
    :func:`table_dimensions_ferc1` to fill in missing (previously implied) dimension
    values in the calculation components table.

    This is often a broadcast merge because many tables contain many values within these
    dimension columns, so it is expected that new calculation component table will have
    many more records than the input calculation components table.

    Any dimension that was already specified in the calculation fixes will be left
    unchanged. If no value of a particular dimension has ever been observed in
    association with a given combination of ``table_name`` and ``xbrl_factoid`` it will
    remain null.

    Args:
        calculation_components: a table of calculation component records which have had
            some manual calculation fixes applied.
        table_dimensions_ferc1: table with all observed values of
            :func:`other_dimensions` for each ``table_name`` and ``xbrl_factoid``
        dimensions: list of dimension columns to check.
        parent: boolean to indicate whether or not the dimensions to be added are
            the parental dimensions or the child dimensions.
    """
    logger.info(f"Adding {dimensions=} into calculation component table.")
    calc_comps_w_dims = calculation_components.copy()
    on_cols = ["table_name", "xbrl_factoid"]
    if parent:
        table_dimensions_ferc1 = table_dimensions_ferc1.rename(
            columns={col: f"{col}_parent" for col in on_cols}
            | {dim: f"{dim}_parent" for dim in dimensions}
        )
        on_cols = [f"{col}_parent" for col in on_cols]
        dimensions = [f"{dim}_parent" for dim in dimensions]
    # for each dimension, use split/apply/combine. when there are no dims explict in
    # the calc components, merge in all of the dims.
    for dim_col in dimensions:
        # extract the unique observed instances of this one dimension column & add the
        # _calc suffix so we can merge onto the calculation components.
        observed_dim = (
            table_dimensions_ferc1[on_cols + [dim_col]]
            .drop_duplicates()
            .dropna(subset=dim_col)
        )  # bc there are dupes after we removed the other dim cols

        null_dim_mask = calc_comps_w_dims[dim_col].isnull()
        null_dim = calc_comps_w_dims[null_dim_mask].drop(columns=[dim_col])
        calc_comps_w_implied_dims = pd.merge(
            null_dim,
            observed_dim,
            on=on_cols,
            how="left",
        )
        calc_comps_w_explicit_dims = calc_comps_w_dims[~null_dim_mask]
        # astypes dealing w/ future warning regarding empty or all null dfs
        calc_comps_w_dims = pd.concat(
            [
                calc_comps_w_implied_dims.convert_dtypes(),
                calc_comps_w_explicit_dims.convert_dtypes(),
            ]
        )
    return calc_comps_w_dims


def assign_parent_dimensions(
    calc_components: pd.DataFrame, table_dimensions: pd.DataFrame, dimensions: list[str]
) -> pd.DataFrame:
    """Add dimensions to calculation parents.

    We now add in parent-dimension values for all of the original calculation component
    records using the observed dimensions.

    Args:
        calc_components: a table of calculation component records which have had some
            manual calculation fixes applied.
        table_dimensions: table with all observed values of :func:`other_dimensions` for
            each ``table_name`` and ``xbrl_factoid``.
        dimensions: list of dimension columns to check.
    """
    if calc_components.empty:
        return calc_components.assign(**{f"{dim}_parent": pd.NA for dim in dimensions})
    # desired: add parental dimension columns
    for dim in dimensions:
        # split the nulls and non-nulls. If the child dim is null, then we can run the
        # parent factoid through make_calculation_dimensions_explicit to get it's dims.
        # if a child fact has dims, we need to merge the dimensions using the dim of the
        # child and the dim of the parent bc we don't want to broadcast merge all parent
        # dims to all child dims. We are assuming here that if a child is has a dim
        null_dim_mask = calc_components[dim].isnull()
        calc_components_null = make_calculation_dimensions_explicit(
            calculation_components=calc_components[null_dim_mask].assign(
                **{f"{dim}_parent": pd.NA}
            ),
            table_dimensions_ferc1=table_dimensions,
            dimensions=[dim],
        )
        parent_dim_idx = ["table_name_parent", "xbrl_factoid_parent", f"{dim}_parent"]
        calc_components_non_null = calc_components[~null_dim_mask]
        table_dimensions_non_null = table_dimensions.rename(
            columns={col: f"{col}_parent" for col in table_dimensions}
        )[parent_dim_idx].drop_duplicates()
        calc_components_non_null = pd.merge(
            left=calc_components_non_null,
            right=table_dimensions_non_null,
            left_on=["table_name_parent", "xbrl_factoid_parent", dim],
            right_on=parent_dim_idx,
            how="left",
        )
        # astypes dealing w/ future warning regarding empty or all null dfs
        calc_components = pd.concat(
            [
                calc_components_null.astype(calc_components_non_null.dtypes),
                calc_components_non_null.astype(calc_components_null.dtypes),
            ]
        ).reset_index(drop=True)

    return calc_components


def infer_intra_factoid_totals(
    calc_components: pd.DataFrame,
    meta_w_dims: pd.DataFrame,
    table_dimensions: pd.DataFrame,
    dimensions: list[str],
) -> pd.DataFrame:
    """Define dimension total calculations.

    Some factoids are marked as a total along some dimension in the metadata,
    which means that they are the sum of all the non-total factoids along that
    dimension.

    We match the parent factoids from the metadata to child factoids from the
    table_dimensions. We treat "total" as a wildcard value.

    We exclude child factoids that are themselves totals, because that would
    result in a double-count.

    Here are a few examples:

    Imagine a factoid with the following dimensions & values:

    - utility types: "total", "gas", "electric";
    - plant status: "total", "in_service", "future"

    Then the following parents would match/not-match:

    - parent: "total", "in_service"

      - child: "gas", "in_service" WOULD match.
      - child: "electric", "in_service" WOULD match.
      - child: "electric", "future" WOULD NOT match.

    - parent: "total", "total"

      - child: "gas", "in_service" WOULD match.
      - child: "electric", "future" WOULD match.

    See the unit test in ferc1_test.py for more details.

    To be able to define these within-dimension calculations we also add dimension
    columns to all of the parent factoids in the table.

    Args:
        calc_components: a table of calculation component records which have had some
            manual calculation fixes applied. Passed through unmodified.
        meta_w_dims: metadata table with the dimensions.
        table_dimensions: table with all observed values of :func:`other_dimensions` for
            each ``table_name`` and ``xbrl_factoid``.
        dimensions: list of dimension columns to check.

    Returns:
        An table associating calculation components with the parents they will be
        aggregated into. The components and the parents are each identified by
        ``table_name``, ``xbrl_factoid``, and columns defining the additional dimensions
        (``utility_type``, ``plant_status``, ``plant_function``). The parent columns
        have a ``_parent`` suffix.
    """
    child_candidates = table_dimensions[
        ~(table_dimensions[dimensions] == "total").any(axis="columns")
    ]

    total_comps = []

    # check *every* combination of dimensions that could have any total values
    dim_combos = itertools.chain.from_iterable(
        itertools.combinations(dimensions, i + 1) for i in range(len(dimensions))
    )
    for _total_dims in dim_combos:
        total_dims = list(_total_dims)
        parents = meta_w_dims.dropna(subset=total_dims).loc[
            (meta_w_dims[total_dims] == "total").all(axis="columns")
        ]
        if parents.empty:
            continue

        # There's no wildcard merge in Pandas, so we just ignore whichever
        # columns have "total"
        non_total_cols = ["table_name", "xbrl_factoid"] + [
            d for d in dimensions if d not in total_dims
        ]
        total_comps.append(
            pd.merge(
                left=parents,
                right=child_candidates,
                on=non_total_cols,
                how="inner",
                suffixes=("_parent", ""),
            ).assign(
                is_within_table_calc=True,
                weight=1,
                table_name_parent=lambda x: x.table_name,
                xbrl_factoid_parent=lambda x: x.xbrl_factoid,
            )
        )

    child_node_pk = ["table_name", "xbrl_factoid"] + dimensions
    parent_node_pk = [f"{col}_parent" for col in child_node_pk]
    relationship_cols = ["is_within_table_calc", "weight"]
    all_expected_cols = parent_node_pk + child_node_pk + relationship_cols
    inferred_totals = (
        pd.concat(total_comps).reindex(columns=all_expected_cols).reset_index(drop=True)
    )

    # merge() will have dropped shared columns, so re-fill with child values:
    child_values = inferred_totals[["table_name", "xbrl_factoid"] + dimensions].rename(
        lambda dim: f"{dim}_parent", axis="columns"
    )
    inferred_totals = inferred_totals.fillna(child_values)
    calcs_with_totals = pd.concat(
        [
            calc_components.assign(is_total_to_subdimensions_calc=False),
            inferred_totals.assign(is_total_to_subdimensions_calc=True),
        ]
    )

    # verification + deduping below.

    check_for_calc_components_duplicates(
        calcs_with_totals,
        table_names_known_dupes=[
            "electricity_sales_by_rate_schedule_ferc1",
        ],
        idx=parent_node_pk + child_node_pk,
    )

    # only drop duplicates if the table_name is in known dupes list.
    calcs_with_totals = calcs_with_totals.drop_duplicates(
        parent_node_pk + child_node_pk, keep="first"
    )
    assert calcs_with_totals[calcs_with_totals.duplicated()].empty
    return calcs_with_totals


@asset(
    ins={
        table_name: AssetIn(table_name)
        # list of tables that have reconcile_table_calculations params
        # minus electric_plant_depreciation_changes_ferc1 bc that table is messy and
        # not actually in the explosion work
        for table_name in [
            "plant_in_service_ferc1",
            "utility_plant_summary_ferc1",
            "electric_operating_expenses_ferc1",
            "balance_sheet_liabilities_ferc1",
            "depreciation_amortization_summary_ferc1",
            "balance_sheet_assets_ferc1",
            "income_statement_ferc1",
            "electric_plant_depreciation_functional_ferc1",
            "retained_earnings_ferc1",
            "electric_operating_revenues_ferc1",
        ]
    }
    | {
        "calculation_components_xbrl_ferc1": AssetIn(
            "calculation_components_xbrl_ferc1"
        )
    },
)
def _core_ferc1__calculation_metric_checks(**kwargs):
    """Check calculation metrics for all transformed tables which have reconciled calcs."""
    calculation_components = kwargs["calculation_components_xbrl_ferc1"]
    transformed_ferc1_dfs = {
        name: df
        for (name, df) in kwargs.items()
        if name not in ["calculation_components_xbrl_ferc1"]
    }
    # standardize the two key columns we are going to use into generic names
    xbrl_factoid_name = table_to_xbrl_factoid_name()
    columns_to_check = table_to_column_to_check()
    tbls = [
        df.assign(table_name=name).rename(
            columns={
                xbrl_factoid_name[name]: "xbrl_factoid",
                columns_to_check[name]: "column_to_check",
            }
        )
        for (name, df) in transformed_ferc1_dfs.items()
    ]
    transformed_ferc1 = pd.concat(tbls)
    # restrict the calculation components to only the bits we want
    calculation_components = calculation_components[
        # remove total to subdimensions
        ~calculation_components.is_total_to_subdimensions_calc
        # only the intra table calcs
        & calculation_components.is_within_table_calc
        # remove corrections (bc they would clean up the calcs so the errors wouldn't show up)
        & ~calculation_components.xbrl_factoid.str.contains("_correction")
        # remove all of the tables that aren't in this check
        & calculation_components.table_name_parent.isin(transformed_ferc1_dfs.keys())
    ]

    calc_idx = ["xbrl_factoid", "table_name"] + other_dimensions(
        table_names=list(FERC1_TFR_CLASSES)
    )
    calculated_df = calculate_values_from_components(
        data=transformed_ferc1,
        calculation_components=calculation_components,
        calc_idx=calc_idx,
        value_col="column_to_check",
    )
    calculation_metrics = check_calculation_metrics_by_group(
        calculated_df=calculated_df,
        group_metric_checks=GroupMetricChecks(
            groups_to_check=[
                "ungrouped",
                "table_name",
                "xbrl_factoid",
                "utility_id_ferc1",
                "report_year",
            ]
        ),
    )

    errors = calculation_metrics[
        calculation_metrics.filter(like="is_error").any(axis=1)
    ]
    if len(errors) > 42:
        raise AssertionError(
            f"Found {len(errors)} from the results of check_calculation_metrics_by_group"
            f"with default group values when less than 41 was expected.\n{errors}"
        )
    return calculation_metrics
