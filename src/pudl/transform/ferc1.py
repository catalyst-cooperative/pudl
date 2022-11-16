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
from typing import Any

import numpy as np
import pandas as pd
import sqlalchemy as sa
from pandas.core.groupby import DataFrameGroupBy

import pudl
from pudl.analysis.classify_plants_ferc1 import (
    plants_steam_assign_plant_ids,
    plants_steam_validate_ids,
)
from pudl.extract.ferc1 import TABLE_NAME_MAP
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
class Ferc1Source(enum.Enum):
    """Enumeration of allowed FERC 1 raw data sources."""

    XBRL = "xbrl"
    DBF = "dbf"


@enum.unique
class Ferc1TableId(enum.Enum):
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


class Ferc1RenameColumns(TransformParams):
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

    dbf: RenameColumns = {}
    xbrl: RenameColumns = {}


class Ferc1TableTransformParams(TableTransformParams):
    """A model defining what TransformParams are allowed for FERC Form 1.

    This adds additional parameter models beyond the ones inherited from the
    :class:`pudl.transform.classes.AbstractTableTransformer` class.
    """

    class Config:
        """Only allow the known table transform params."""

        extra = "forbid"

    rename_columns_ferc1: Ferc1RenameColumns = Ferc1RenameColumns(
        dbf=RenameColumns(),
        xbrl=RenameColumns(),
    )
    rename_columns_instant_xbrl: RenameColumns = RenameColumns()
    rename_columns_duration_xbrl: RenameColumns = RenameColumns()


################################################################################
# FERC 1 specific Column, MultiColumn, and Table Transform Functions
# (empty for now, but we anticipate there will be some)
################################################################################


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


def update_ferc1_dbf_xbrl_glue(ferc1_engine: sa.engine.Engine) -> pd.DataFrame:
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


def get_ferc1_dbf_xbrl_glue(dbf_table_name: str) -> pd.DataFrame:
    """Read the DBF+XBRL glue and expand it to cover all DBF years."""
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
                "xbrl_column_stem",
            ],
        )
    # Select only the rows that pertain to dbf_table_name
    row_map = row_map.loc[
        row_map.sched_table_name == dbf_table_name,
        ["report_year", "row_number", "row_type", "xbrl_column_stem"],
    ]
    # Indicate which rows have unmappable headers in them, to differentiate them from
    # null values in the exhaustive index we create below.
    # We need the ROW_HEADER sentinel value so we can distinguish
    # between two different reasons that we might find NULL values in the
    # xbrl_column_stem field:
    # 1. It's NULL because it's between two valid mapped values (the NULL was created
    #    in our filling of the time series) and should thus be filled in, or
    # 2. It's NULL because it was a header row in the DBF data, which means it should
    #    NOT be filled in. Without the HEADER_ROW value, when a row number from year X
    #    becomes associated with a non-header row in year X+1
    #    the ffill will keep right on filling, associating all of the new header rows
    #    with the value of xbrl_column_stem that was associated with the old row number.
    row_map.loc[
        (row_map.row_type == "header") & (row_map.xbrl_column_stem.isna()),
        "xbrl_column_stem",
    ] == "HEADER_ROW"
    row_map = row_map.drop(["row_type"], axis="columns")

    # Create an index containing all possible index values:
    idx = pd.MultiIndex.from_product(
        [
            Ferc1Settings().dbf_years,
            row_map.row_number.unique(),
        ],
        names=["report_year", "row_number"],
    )

    # Concatenate the row map with the empty index, so we have blank spaces to fill:
    row_map = pd.concat(
        [
            pd.DataFrame(index=idx),
            row_map.set_index(["report_year", "row_number"]),
        ],
        axis="columns",
    ).reset_index()

    # Forward fill missing XBRL column names, until a new definition for the row
    # number is encountered:
    row_map["xbrl_column_stem"] = row_map.groupby(
        "row_number"
    ).xbrl_column_stem.transform("ffill")
    # Drop any rows that do not actually map between DBF rows and XBRL columns:
    row_map = row_map.replace({"xbrl_column_stem": {"HEADER_ROW": np.nan}}).dropna(
        subset=["xbrl_column_stem"]
    )
    return row_map


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

    table_id: Ferc1TableId
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

    xbrl_metadata_json: list[dict[Any]] = []
    """An array of JSON objects extracted from the FERC 1 XBRL taxonomy."""

    xbrl_metadata_normalized: pd.DataFrame = pd.DataFrame()
    """A semi-normalized dataframe containing table-specific XBRL metadata."""

    def __init__(
        self,
        params: TableTransformParams | None = None,
        cache_dfs: bool = False,
        clear_cached_dfs: bool = True,
        xbrl_metadata_json: list[dict[Any]] | None = None,
    ) -> None:
        """Augment inherited initializer to store XBRL metadata in the class."""
        super().__init__(
            params=params,
            cache_dfs=cache_dfs,
            clear_cached_dfs=clear_cached_dfs,
        )
        # Many tables don't require this input:
        if xbrl_metadata_json:
            self.xbrl_metadata_json = xbrl_metadata_json

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
            self.normalize_strings(df)
            .pipe(self.categorize_strings)
            .pipe(self.convert_units)
            .pipe(self.strip_non_numeric_values)
            .pipe(self.nullify_outliers)
            .pipe(self.drop_invalid_rows)
            .pipe(
                pudl.metadata.classes.Package.from_resource_ids()
                .get_resource(self.table_id.value)
                .encode
            )
        )
        return df

    @cache_df(key="end")
    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enforce the database schema and remove any cached dataframes."""
        return self.enforce_schema(df)

    def normalize_metadata_xbrl(
        self, xbrl_fact_names: list[str] | None
    ) -> pd.DataFrame:
        """Normalize XBRL metadata, select table-specific rows, and transform them.

        In order to select the relevant rows from the normalized metadata, this function
        needs to know which XBRL facts pertain to the table being transformed. These are
        the names of the data columns in the raw XBRL data. To avoid creating a separate
        dependency on the FERC 1 XBRL DB, we defer the assignment of this class
        attribute until :meth:`Ferc1AbstractTableTransformer.process_xbrl` is called,
        and read the column labels from the input dataframes.
        """
        # If the table has no XBRL metadata, return immediately:
        if not self.xbrl_metadata_json:
            return pd.DataFrame()

        normed_meta = (
            pd.json_normalize(self.xbrl_metadata_json)
            .rename(
                columns={
                    "name": "xbrl_fact_name",
                    "references.Account": "ferc_account",
                }
            )
            .loc[
                :,
                [
                    "xbrl_fact_name",
                    "balance",
                    "calculations",
                    "ferc_account",
                ],
            ]
        )
        # Use nullable strings, converting NaN to pd.NA
        normed_meta = normed_meta.astype(
            {
                "xbrl_fact_name": pd.StringDtype(),
                "balance": pd.StringDtype(),
                "ferc_account": pd.StringDtype(),
            }
        )
        if xbrl_fact_names:
            normed_meta = normed_meta.loc[
                normed_meta.xbrl_fact_name.isin(xbrl_fact_names)
            ]
        self.xbrl_metadata_normalized = normed_meta
        return normed_meta

    @cache_df(key="dbf")
    def align_row_numbers_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Align row-numbers across multiple years of DBF data.

        This is a no-op in the abstract base class, but for row-oriented DBF data where
        the same data shows up in different row numbers in different years, it needs to
        be implemented. Parameterization TBD with additional experience. See:
        https://github.com/catalyst-cooperative/pudl/issues/2012
        """
        return df

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """DBF-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing DBF data pre-concatenation.")
        return (
            raw_dbf.drop_duplicates()
            .pipe(self.drop_footnote_columns_dbf)
            # Note: in this rename_columns we have to pass in params, since we're using
            # the inherited method, with param specific to the child class.
            .pipe(self.rename_columns, params=self.params.rename_columns_ferc1.dbf)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.DBF)
            .pipe(self.align_row_numbers_dbf)
            .pipe(self.drop_unused_original_columns_dbf)
            .pipe(
                self.assign_utility_id_ferc1,
                source_ferc1=Ferc1Source.DBF,
            )
        )

    @cache_df(key="xbrl")
    def process_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """XBRL-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing XBRL data pre-concatenation.")
        logger.info(f"{self.table_id.value}: Normalizing XBRL taxonomy metadata.")
        self.xbrl_metadata_normalized = self.normalize_metadata_xbrl(
            xbrl_fact_names=get_data_cols_raw_xbrl(
                raw_xbrl_duration=raw_xbrl_duration,
                raw_xbrl_instant=raw_xbrl_instant,
            )
        )
        return (
            self.merge_instant_and_duration_tables_xbrl(
                raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.wide_to_tidy_xbrl)
            # Note: in this rename_columns we have to pass in params, since we're using
            # the inherited method, with param specific to the child class.
            .pipe(self.rename_columns, params=self.params.rename_columns_ferc1.xbrl)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.XBRL)
            .pipe(
                self.assign_utility_id_ferc1,
                source_ferc1=Ferc1Source.XBRL,
            )
        )

    @cache_df(key="xbrl")
    def wide_to_tidy_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reshape the XBRL data from wide to tidy format.

        This is a no-op in the abstract base class, but should be implemented for
        child classes which need to reshape the XBRL data for concatenation with DBF.
        Parameterization TBD based on additional experience. See:
        https://github.com/catalyst-cooperative/pudl/issues/2012
        """
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
            return duration
        elif duration.empty:
            logger.info(f"{self.table_id.value}: No XBRL duration table found.")
            return instant
        else:
            instant_merge_keys = ["entity_id", "report_year"] + instant_axes
            duration_merge_keys = ["entity_id", "report_year"] + duration_axes
            # See if there are any values in the instant table that don't show up in the
            # duration table.
            unique_instant_rows = instant.set_index(
                instant_merge_keys
            ).index.difference(duration.set_index(duration_merge_keys).index)
            if unique_instant_rows.empty:
                # Merge instant into duration.
                return pd.merge(
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
                return pd.concat(
                    [
                        instant.set_index(["report_year", "entity_id"] + instant_axes),
                        duration.set_index(
                            ["report_year", "entity_id"] + duration_axes
                        ),
                    ],
                    axis="columns",
                ).reset_index()

    @cache_df("process_instant_xbrl")
    def process_instant_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make instant and duration tables compatible.

        Column renaming is sometimes required because a few columns in the instant and
        duration tables do not have corresponding names that follow the naming
        conventions of ~95% of all the columns, which we rely on programmatically when
        reshaping and concatenating these tables together.
        """
        df = self.rename_columns(df, self.params.rename_columns_instant_xbrl)
        return df

    @cache_df("process_duration_xbrl")
    def process_duration_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make instant and duration tables compatible.

        Column renaming is sometimes required because a few columns in the instant and
        duration tables do not have corresponding names that follow the naming
        conventions of ~95% of all the columns, which we rely on programmatically when
        reshaping and concatenating these tables together.
        """
        df = self.rename_columns(df, self.params.rename_columns_duration_xbrl)
        return df

    @cache_df(key="dbf")
    def drop_footnote_columns_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop DBF footnote reference columns, which all end with _f."""
        logger.debug(f"{self.table_id.value}: Dropping DBF footnote columns.")
        return df.drop(columns=df.filter(regex=r".*_f$").columns)

    def source_table_id(self, source_ferc1: Ferc1Source) -> str:
        """Look up the ID of the raw data source table."""
        return TABLE_NAME_MAP[self.table_id.value][source_ferc1.value]

    def source_table_primary_key(self, source_ferc1: Ferc1Source) -> list[str]:
        """Look up the pre-renaming source table primary key columns."""
        if source_ferc1 == Ferc1Source.DBF:
            pk_cols = [
                "report_year",
                "report_prd",
                "respondent_id",
                "spplmnt_num",
                "row_number",
            ]
        else:
            assert source_ferc1 == Ferc1Source.XBRL  # nosec: B101
            cols = self.params.rename_columns_ferc1.xbrl.columns
            pk_cols = ["report_year", "entity_id"]
            # Sort to avoid dependence on the ordering of rename_columns.
            # Doing the sorting here because we have a particular ordering
            # hard coded for the DBF primary keys.
            pk_cols += sorted(col for col in cols if col.endswith("_axis"))
        return pk_cols

    def renamed_table_primary_key(self, source_ferc1: Ferc1Source) -> list[str]:
        """Look up the post-renaming primary key columns."""
        if source_ferc1 == Ferc1Source.DBF:
            cols = self.params.rename_columns_ferc1.dbf.columns
        else:
            assert source_ferc1 == Ferc1Source.XBRL  # nosec: B101
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
        self, df: pd.DataFrame, source_ferc1: Ferc1Source
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
            source_table_id=self.source_table_id(source_ferc1),
            record_id=lambda x: x.source_table_id.str.cat(
                x[pk_cols].astype(str), sep="_"
            ),
        ).drop(columns=["source_table_id"])
        df.record_id = enforce_snake_case(df.record_id)

        dupe_ids = df.record_id[df.record_id.duplicated()].values
        if dupe_ids.any() and self.has_unique_record_ids:
            logger.warning(
                f"{self.table_id.value}: Found {len(dupe_ids)} duplicate record_ids: \n"
                f"{dupe_ids}."
            )
        return df

    def assign_utility_id_ferc1(
        self, df: pd.DataFrame, source_ferc1: Ferc1Source
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

    table_id: Ferc1TableId = Ferc1TableId.FUEL_FERC1

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Table specific transforms for fuel_ferc1.

        Args:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.
        """
        return self.drop_invalid_rows(df).pipe(self.correct_units)

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
            # Note: in this rename_columns we have to pass in params, since we're using
            # the inherited method, with param specific to the child class.
            .pipe(self.rename_columns, params=self.params.rename_columns_ferc1.xbrl)
            .pipe(self.convert_units)
            .pipe(self.normalize_strings)
            .pipe(self.categorize_strings)
            .pipe(self.standardize_physical_fuel_units)
            .pipe(self.aggregate_duplicate_fuel_types_xbrl)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.XBRL)
            .pipe(
                self.assign_utility_id_ferc1,
                source_ferc1=Ferc1Source.XBRL,
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
            FuelFix("nuclear", "mmmbtu", "mwhth", (1.0 / 3.412142)),
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
        pk_cols = self.renamed_table_primary_key(source_ferc1=Ferc1Source.XBRL)
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

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_STEAM_FERC1

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

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_HYDRO_FERC1


class PlantsPumpedStorageFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`plants_pumped_storage_ferc1` table."""

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_PUMPED_STORAGE_FERC1


class PurchasedPowerTableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for :ref:`purchased_power_ferc1` table.

    This table has data about inter-utility power purchases into the PUDL DB. This
    includes how much electricty was purchased, how much it cost, and who it was
    purchased from. Unfortunately the field describing which other utility the power was
    being bought from is poorly standardized, making it difficult to correlate with
    other data. It will need to be categorized by hand or with some fuzzy matching
    eventually.
    """

    table_id: Ferc1TableId = Ferc1TableId.PURCHASED_POWER_FERC1


class PlantInServiceFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A transformer for the :ref:`plant_in_service_ferc1` table."""

    table_id: Ferc1TableId = Ferc1TableId.PLANT_IN_SERVICE_FERC1
    has_unique_record_ids: bool = False

    @cache_df(key="dbf")
    def align_row_numbers_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Align historical FERC1 DBF row numbers with XBRL account IDs."""
        return pd.merge(
            df,
            get_ferc1_dbf_xbrl_glue(self.source_table_id(Ferc1Source.DBF)),
            on=["report_year", "row_number"],
        ).rename(columns={"xbrl_column_stem": "ferc_account_label"})

    @cache_df("process_instant_xbrl")
    def process_instant_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing required to make the instant and duration tables compatible.

        Each year the plant account balances are reported twice, in two separate
        records: one for the end of the previous year, and one for the end of the
        current year, with appropriate dates for the two year ends. Here we are
        reshaping the table so that we instead have two columns: ``starting_balance``
        and ``ending_balance`` that both pertain to the current year, so that all of
        the records pertaining to a single ``report_year`` can be identified without
        dealing with the instant / duration distinction.
        """
        df = super().process_instant_xbrl(df)
        df["year"] = pd.to_datetime(df["date"]).dt.year
        df.loc[df.report_year == (df.year + 1), "balance_type"] = "starting_balance"
        df.loc[df.report_year == df.year, "balance_type"] = "ending_balance"
        if not df.balance_type.notna().all():
            raise ValueError(
                f"Unexpected years found in the {self.table_id.value} table: "
                f"{df.loc[df.balance_type.isna(), 'year'].unique()}"
            )
        df = (
            df.drop(["year", "date"], axis="columns")
            .set_index(["entity_id", "report_year", "balance_type"])
            .unstack("balance_type")
        )
        # This turns a multi-index into a single-level index with tuples of strings as
        # the keys, and then converts the tuples of strings into a single string by
        # joining their values with an underscore. This results in column labels like
        # boiler_plant_equipment_steam_production_starting_balance
        # Is there a better way?
        df.columns = ["_".join(items) for items in df.columns.to_flat_index()]
        return df.reset_index()

    @cache_df(key="xbrl")
    def wide_to_tidy_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
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
        hydraulic production plant -> all production plant -> all electric utility
        plant) though the categorical columns required for that aggregation are added
        later.
        """
        df = df.drop(["start_date", "end_date"], axis="columns")
        value_types = [
            "starting_balance",
            "additions",
            "retirements",
            "transfers",
            "adjustments",
            "ending_balance",
        ]
        suffixes = "|".join(value_types)
        pat = r"(^.*)_(" + suffixes + r"$)"
        df = df.set_index(["entity_id", "report_year"])
        new_cols = pd.MultiIndex.from_tuples(
            [(re.sub(pat, r"\1", col), re.sub(pat, r"\2", col)) for col in df.columns],
            names=["ferc_account_label", "value_type"],
        )
        df.columns = new_cols
        df = (
            df.stack(level="ferc_account_label", dropna=False)
            .loc[:, value_types]
            .reset_index()
        )
        return df

    def normalize_metadata_xbrl(
        self, xbrl_fact_names: list[str] | None
    ) -> pd.DataFrame:
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
        naming conventions...). We use the same rename dictionary, but as an argument
        to :meth:`pd.Series.replace` instead of :meth:`pd.DataFrame.rename`.
        """
        pis_meta = (
            super()
            .normalize_metadata_xbrl(xbrl_fact_names)
            .assign(
                ferc_account_label=lambda x: x.xbrl_fact_name.replace(
                    self.params.rename_columns_instant_xbrl.columns
                )
            )
        )

        # Remove metadata records that pertain to columns we have eliminated through
        # reshaping. The *_starting_balance and *_ending_balance columns come from the
        # instant table, but they have no suffix there -- they just show up as the
        # stem (e.g. land_and_land_rights_general_plant). So by removing any column
        # that has these four value type suffixes, we're left with only the stem
        # categories.
        value_types = ["additions", "retirements", "adjustments", "transfers"]
        pattern = ".*(" + "|".join(value_types) + ")$"
        pis_meta = pis_meta[~pis_meta["ferc_account_label"].str.match(pattern)]

        # Set pseudo-account numbers for rows that split or combine FERC accounts, but
        # which are not calculated values.
        pis_meta.loc[
            pis_meta.ferc_account_label == "electric_plant_purchased", "ferc_account"
        ] = "102_purchased"
        pis_meta.loc[
            pis_meta.ferc_account_label == "electric_plant_sold", "ferc_account"
        ] = "102_sold"
        pis_meta.loc[
            pis_meta.ferc_account_label
            == "electric_plant_in_service_and_completed_construction_not_classified_electric",
            "ferc_account",
        ] = "101_and_106"

        # Flag the metadata record types
        pis_meta.loc[pis_meta.calculations.astype(bool), "row_type_xbrl"] = "calculated"
        pis_meta.loc[
            ~pis_meta.calculations.astype(bool) & pis_meta.ferc_account.notna(),
            "row_type_xbrl",
        ] = "ferc_account"
        # Save the normalized metadata so it can be used by other methods.
        self.xbrl_metadata_normalized = pis_meta
        return pis_meta

    def merge_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Combine XBRL-derived metadata with the data it pertains to.

        While the metadata we're using to annotate the data comes from the more recent
        XBRL data, it applies generally to all the historical DBF data as well! This
        method reads the normalized metadata out of an attribute.
        """
        return pd.merge(
            df, self.xbrl_metadata_normalized, on="ferc_account_label", how="left"
        )

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

    @cache_df("main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Annotates and transforms the table based on XBRL taxonomy metadata."""
        return (
            super()
            .transform_main(df)
            .pipe(self.merge_metadata)
            .pipe(self.apply_sign_conventions)
        )


class PlantsSmallFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the :ref:`plants_small_ferc1` table."""

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_SMALL_FERC1

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
            self.normalize_strings(df)
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


def transform(
    ferc1_dbf_raw_dfs: dict[str, pd.DataFrame],
    ferc1_xbrl_raw_dfs: dict[str, dict[str, pd.DataFrame]],
    xbrl_metadata_json: list[dict[Any]],
    ferc1_settings: Ferc1Settings | None = None,
) -> dict[str, pd.DataFrame]:
    """Coordinate the transformation of all FERC Form 1 tables.

    Args:
        ferc1_dbf_raw_dfs: Dictionary mapping PUDL table names (keys) to raw DBF
            dataframes (values).
        ferc1_xbrl_raw_dfs: Nested dictionary containing both an instant and duration
            table for each input XBRL table. Some of these are empty.
        xbrl_metadata_json: FERC 1XBRL taxonomy metadata exported as an array of JSON
            objects.
        ferc1_settings: Validated FERC 1 ETL settings.

    Returns:
        A dictionary of transformed DataFrames.
    """
    if ferc1_settings is None:
        ferc1_settings = Ferc1Settings()

    ferc1_tfr_classes = {
        "fuel_ferc1": FuelFerc1TableTransformer,
        "plants_small_ferc1": PlantsSmallFerc1TableTransformer,
        "plants_hydro_ferc1": PlantsHydroFerc1TableTransformer,
        "plant_in_service_ferc1": PlantInServiceFerc1TableTransformer,
        "plants_pumped_storage_ferc1": PlantsPumpedStorageFerc1TableTransformer,
        "purchased_power_ferc1": PurchasedPowerTableTransformer,
    }
    # create an empty ditctionary to fill up through the transform fuctions
    ferc1_transformed_dfs = {}
    # for each ferc table,
    for table in ferc1_tfr_classes:
        if table in ferc1_settings.tables:
            logger.info(
                f"Transforming raw FERC Form 1 dataframe for loading into {table}"
            )

            ferc1_transformed_dfs[table] = ferc1_tfr_classes[table](
                xbrl_metadata_json=xbrl_metadata_json,
            ).transform(
                raw_dbf=ferc1_dbf_raw_dfs[table],
                raw_xbrl_instant=ferc1_xbrl_raw_dfs[table].get(
                    "instant", pd.DataFrame()
                ),
                raw_xbrl_duration=ferc1_xbrl_raw_dfs[table].get(
                    "duration", pd.DataFrame()
                ),
            )
    # Bespoke exception. fuel must come before steam b/c fuel proportions are used to
    # aid in FERC plant ID assignment.
    if "plants_steam_ferc1" in ferc1_settings.tables:
        ferc1_transformed_dfs["plants_steam_ferc1"] = PlantsSteamFerc1TableTransformer(
            xbrl_metadata_json=xbrl_metadata_json
        ).transform(
            raw_dbf=ferc1_dbf_raw_dfs["plants_steam_ferc1"],
            raw_xbrl_instant=ferc1_xbrl_raw_dfs["plants_steam_ferc1"].get(
                "instant", pd.DataFrame()
            ),
            raw_xbrl_duration=ferc1_xbrl_raw_dfs["plants_steam_ferc1"].get(
                "duration", pd.DataFrame()
            ),
            transformed_fuel=ferc1_transformed_dfs["fuel_ferc1"],
        )

    # convert types and return:
    return {
        name: convert_cols_dtypes(df, data_source="ferc1")
        for name, df in ferc1_transformed_dfs.items()
    }


if __name__ == "__main__":
    """Make the module runnable for iterative testing during development."""

    ferc1_settings = Ferc1Settings(
        years=[2020, 2021],
        # If you want to run it with all years:
        # years=Ferc1Settings().years,
        tables=[
            "fuel_ferc1",
            "plants_steam_ferc1",
            "plants_hydro_ferc1",
            "plant_in_service_ferc1",
            "plants_pumped_storage_ferc1",
            "purchased_power_ferc1",
            "plants_small_ferc1",
        ],
    )
    pudl_settings = pudl.workspace.setup.get_defaults()
    ferc1_dbf_raw_dfs = pudl.extract.ferc1.extract_dbf(
        ferc1_settings=ferc1_settings, pudl_settings=pudl_settings
    )
    ferc1_xbrl_raw_dfs = pudl.extract.ferc1.extract_xbrl(
        ferc1_settings=ferc1_settings, pudl_settings=pudl_settings
    )
    xbrl_metadata_json = pudl.extract.ferc1.extract_xbrl_metadata(pudl_settings)
    dfs = transform(
        ferc1_dbf_raw_dfs=ferc1_dbf_raw_dfs,
        ferc1_xbrl_raw_dfs=ferc1_xbrl_raw_dfs,
        xbrl_metadata_json=xbrl_metadata_json,
        ferc1_settings=ferc1_settings,
    )
