"""Routines for transforming FERC Form 1 data before loading into the PUDL DB.

This module provides a variety of functions that are used in cleaning up the FERC Form 1
data prior to loading into our database. This includes adopting standardized units and
column names, standardizing the formatting of some string values, and correcting data
entry errors which we can infer based on the existing data. It may also include removing
bad data, or replacing it with the appropriate NA values.
"""
import enum
import importlib.resources
import logging
import re
from collections import namedtuple

import numpy as np
import pandas as pd

import pudl
from pudl.analysis.classify_plants_ferc1 import (
    plants_steam_assign_plant_ids,
    plants_steam_validate_ids,
)
from pudl.extract.ferc1 import TABLE_NAME_MAP
from pudl.helpers import convert_cols_dtypes
from pudl.metadata.classes import DataSource
from pudl.metadata.dfs import FERC_DEPRECIATION_LINES
from pudl.settings import Ferc1Settings
from pudl.transform.classes import (
    AbstractTableTransformer,
    InvalidRows,
    RenameColumns,
    TableTransformParams,
    TransformParams,
    cache_df,
)

# This is only here to keep the module importable. Breaks legacy functions.
CONSTRUCTION_TYPE_CATEGORIES = {}

logger = pudl.logging.get_logger(__name__)


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
    """Enumeration of the allowable FERC 1 table IDs.

    Hard coding this seems bad. Somehow it should be either defined in the context of
    the Package, the Ferc1Settings, an etl_group, or DataSource. All of the table
    transformers associated with a given data source should have a table_id that's
    from that data source's subset of the database. Where should this really happen?

    Alternatively, the allowable values could be derived *from* the structure of the
    Package.
    """

    FUEL_FERC1 = "fuel_ferc1"
    PLANTS_STEAM_FERC1 = "plants_steam_ferc1"
    PLANTS_HYDRO_FERC1 = "plants_hydro_ferc1"
    PLANTS_SMALL_FERC1 = "plants_small_ferc1"
    PLANTS_PUMPED_STORAGE_FERC1 = "plants_pumped_storage_ferc1"
    PLANT_IN_SERVICE_FERC1 = "plant_in_service_ferc1"
    PURCHASED_POWER = "purchased_power_ferc1"


class Ferc1RenameColumns(TransformParams):
    """Dictionaries for renaming either XBRL or DBF derived FERC 1 columns.

    This is FERC 1 specific, because we need to store both DBF and XBRL rename
    dictionaires separately. (Is this true? Could we just include all of the column
    mappings from both the DBF and XBRL inputs in the same rename dictionary? Would that
    be simpler, or would there be issues that come up with name collisions where the
    source columns have the same name but map to different column namess / meanings in
    the PUDL DB?)

    Potential validations:

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
    """A model defining additional allowed FERC Form 1 specific TransformParams."""

    class Config:
        """Only allow the known table transform params."""

        extra = "forbid"

    rename_columns_ferc1: Ferc1RenameColumns = Ferc1RenameColumns(
        dbf=RenameColumns(),
        xbrl=RenameColumns(),
    )


################################################################################
# FERC 1 specific Column, MultiColumn, and Table Transform Functions
# (empty for now, but we anticipate there will be some)
################################################################################


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

    @cache_df(key="main")
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
    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enforce the database schema and remove any cached dataframes."""
        return self.enforce_schema(df)

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """DBF-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing DBF data pre-concatenation.")
        return (
            self.drop_footnote_columns_dbf(raw_dbf)
            # Note: in this rename_columns we have to pass in params, since we're using
            # the inherited method, with param specific to the child class.
            .pipe(self.rename_columns, params=self.params.rename_columns_ferc1.dbf)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.DBF)
            .pipe(self.drop_unused_original_columns_dbf)
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
            # Note: in this rename_columns we have to pass in params, since we're using
            # the inherited method, with param specific to the child class.
            .pipe(self.rename_columns, params=self.params.rename_columns_ferc1.xbrl)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.XBRL)
            .pipe(self.assign_utility_id_ferc1_xbrl)
        )

    @cache_df(key="xbrl")
    def merge_instant_and_duration_tables_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge the XBRL instant and duration tables into a single dataframe.

        FERC1 XBRL instant period signifies that it is true as of the reported date,
        while a duration fact pertains to the specified time period. The ``date`` column
        for an instant fact corresponds to the ``end_date`` column of a duration fact.

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

        if instant.empty:
            logger.debug(
                f"{self.table_id.value}: No XBRL instant table found, returning the "
                "duration table."
            )
            return duration
        if duration.empty:
            logger.debug(
                f"{self.table_id.value}: No XBRL duration table found, returning "
                "instant table."
            )
            return instant

        instant_axes = [
            col for col in raw_xbrl_instant.columns if col.endswith("_axis")
        ]
        duration_axes = [
            col for col in raw_xbrl_duration.columns if col.endswith("_axis")
        ]
        if set(instant_axes) != set(duration_axes):
            raise ValueError(
                f"{self.table_id.value}: Instant and Duration XBRL Axes do not match.\n"
                f"    instant: {instant_axes}\n"
                f"    duration: {duration_axes}"
            )

        return pd.merge(
            instant,
            duration,
            how="outer",
            left_on=["date", "entity_id", "report_year"] + instant_axes,
            right_on=["end_date", "entity_id", "report_year"] + duration_axes,
            validate="1:1",
        )

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
        identified by the combination of: report_year, report_prd, utility_id_ferc1,
        spplmnt_num, row_number.

        The FERC Form 1 XBRL tables do not have these supplement and row number
        columns, so we construct an id based on:
        report_year, entity_id, and the primary key columns of the XBRL table

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
        dupe_ids = df.record_id[df.record_id.duplicated()].values
        if dupe_ids.any():
            logger.warning(
                f"{self.table_id.value}: Found {len(dupe_ids)} duplicate record_ids. "
                f"{dupe_ids}."
            )
        return df

    @cache_df(key="xbrl")
    def assign_utility_id_ferc1_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assign utility_id_ferc1.

        This is a temporary solution until we have real ID mapping working for the XBRL
        entity IDs. See https://github.com/catalyst-cooperative/pudl/issue/1705

        Note that in some cases this will create collisions with the existing
        utility_id_ferc1 values.
        """
        logger.warning(f"{self.table_id.value}: USING DUMMY UTILITY_ID_FERC1 IN XBRL.")
        return df.assign(
            utility_id_ferc1=lambda x: x.entity_id.str.replace(r"^C", "", regex=True)
            .str.lstrip("0")
            .astype("Int64")
        )


class FuelFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the ``fuel_ferc1`` table.

    The ``fuel_ferc1`` table reports data about fuel consumed by large thermal power
    plants that report in the ``plants_steam_ferc1`` table.  Each record in the steam
    table is typically associated with several records in the fuel table, with each fuel
    record reporting data for a particular type of fuel consumed by that plant over the
    course of a year. The fuel table presents several challenges.

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

    Steps to take, in order:

    * Convert units in per-unit columns and rename the columns
    * Normalize freeform strings (fuel type and fuel units)
    * Categorize strings in fuel type and fuel unit columns
    * Standardize physical fuel units based on reported units (tons, mcf, bbl)
    * Remove fuel_units column
    * Convert heterogenous fuel price and heat content columns to their aspirational
      units.
    * Apply fuel unit corrections to fuel price and heat content columns based on
      observed clustering of values.
    """

    table_id: Ferc1TableId = Ferc1TableId.FUEL_FERC1

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Table specific transforms for fuel_ferc1.

        Params:
            df: Pre-processed, concatenated XBRL and DBF data.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.
        """
        return self.drop_invalid_rows(df).pipe(self.correct_units)

    @cache_df(key="dbf")
    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Start with inherited method and do some fuel-specific processing.

        Mostly this needs to do extra work because of the linkage between the fuel_ferc1
        and plants_steam_ferc1 tables, and because the fuel type column is both a big
        mess of freeform strings and part of the primary key.
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
        """Special pre-concat treatment of the fuel_ferc1 table.

        This is necessary because the fuel type is a messy freeform string column that
        needs to be cleaned up, and is also (insanely) a primary key column for the
        table, and required for merging the fuel_ferc1 and plants_steam_ferc1 tables.
        This means that we can't assign a record ID until the fuel types have been
        cleaned up. Additionally the string categorization results in a number of
        duplicate fuel records which need to be aggregated.
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
            .pipe(self.assign_utility_id_ferc1_xbrl)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.XBRL)
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
        represent plant totals.
        """
        return super().drop_invalid_rows(df, params).pipe(self.drop_total_rows)


class PlantsSteamFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for the plants_steam_ferc1 table."""

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_STEAM_FERC1

    @cache_df(key="main")
    def transform_main(
        self, df: pd.DataFrame, transformed_fuel: pd.DataFrame
    ) -> pd.DataFrame:
        """Perform table transformations for the plants_steam_ferc1 table."""
        fuel_categories = list(
            FuelFerc1TableTransformer()
            .params.categorize_strings["fuel_type_code_pudl"]
            .categories.keys()
        )
        plants_steam = (
            self.normalize_strings(df)
            .pipe(self.nullify_outliers)
            .pipe(self.categorize_strings)
            .pipe(self.convert_units)
            .pipe(self.drop_invalid_rows)
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


def transform(
    ferc1_dbf_raw_dfs,
    ferc1_xbrl_raw_dfs,
    ferc1_settings: Ferc1Settings | None = None,
):
    """Transforms FERC 1.

    Args:
        ferc1_dbf_raw_dfs (dict): Dictionary pudl table names (keys) and raw DBF
            dataframes (values).
        ferc1_xbrl_raw_dfs (dict): Dictionary pudl table names with `_instant`
            or `_duration` (keys) and raw XRBL dataframes (values).
        ferc1_settings: Validated ETL parameters required by
            this data source.

    Returns:
        dict: A dictionary of the transformed DataFrames.
    """
    if ferc1_settings is None:
        ferc1_settings = Ferc1Settings()

    ferc1_tfr_classes = {
        "fuel_ferc1": FuelFerc1TableTransformer,
        # "plants_small_ferc1": plants_small,
        # "plants_hydro_ferc1": plants_hydro,
        # "plants_pumped_storage_ferc1": plants_pumped_storage,
        # "plant_in_service_ferc1": plant_in_service,
        # "purchased_power_ferc1": purchased_power,
        # "accumulated_depreciation_ferc1": accumulated_depreciation,
    }
    # create an empty ditctionary to fill up through the transform fuctions
    ferc1_transformed_dfs = {}
    # for each ferc table,
    for table in ferc1_tfr_classes:
        if table in ferc1_settings.tables:
            logger.info(
                f"Transforming raw FERC Form 1 dataframe for loading into {table}"
            )

            ferc1_transformed_dfs[table] = ferc1_tfr_classes[table]().transform(
                raw_dbf=ferc1_dbf_raw_dfs[table],
                raw_xbrl_instant=ferc1_xbrl_raw_dfs[table].get(
                    "instant", pd.DataFrame()
                ),
                raw_xbrl_duration=ferc1_xbrl_raw_dfs[table].get(
                    "duration", pd.DataFrame()
                ),
            )
    # Bespoke exception. fuel must come before steam b/c fuel proportions are used to
    # aid in plant # ID assignment.
    if "plants_steam_ferc1" in ferc1_settings.tables:
        ferc1_transformed_dfs[
            "plants_steam_ferc1"
        ] = PlantsSteamFerc1TableTransformer().transform(
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
    """Make the module runnable for testing purposes."""

    logging.basicConfig(
        level=logging.DEBUG,
        format=r"%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s",
    )
    ferc1_settings = Ferc1Settings(
        years=[2020, 2021],
        tables=["fuel_ferc1", "plants_steam_ferc1"],
    )
    pudl_settings = pudl.workspace.setup.get_defaults()
    raw_dbf = pudl.extract.ferc1.extract_dbf(
        ferc1_settings=ferc1_settings, pudl_settings=pudl_settings
    )
    raw_xbrl = pudl.extract.ferc1.extract_xbrl(
        ferc1_settings=ferc1_settings, pudl_settings=pudl_settings
    )
    dfs = transform(
        ferc1_dbf_raw_dfs=raw_dbf,
        ferc1_xbrl_raw_dfs=raw_xbrl,
        ferc1_settings=ferc1_settings,
    )


##################################################################################
# OLD FERC TRANSFORM HELPER FUNCTIONS ############################################
##################################################################################
def unpack_table(ferc1_df, table_name, data_cols, data_rows):
    """Normalize a row-and-column based FERC Form 1 table.

    Pulls the named database table from the FERC Form 1 DB and uses the corresponding
    ferc1_row_map to unpack the row_number coded data.

    Args:
        ferc1_df (pandas.DataFrame): Raw FERC Form 1 DataFrame from the DB.
        table_name (str): Original name of the FERC Form 1 DB table.
        data_cols (list): List of strings corresponding to the original FERC Form 1
            database table column labels -- these are the columns of data that we are
            extracting (it can be a subset of the columns which are present in the
            original database).
        data_rows (list): List of row_names to extract, as defined in the FERC 1 row
            maps. Set to slice(None) if you want all rows.

    Returns:
        pandas.DataFrame
    """
    # Read in the corresponding row map:
    row_map = (
        pd.read_csv(
            importlib.resources.open_text(
                "pudl.package_data.ferc1.row_maps", f"{table_name}.csv"
            ),
            index_col=0,
            comment="#",
        )
        .copy()
        .transpose()
        .rename_axis(index="year_index", columns=None)
    )
    row_map.index = row_map.index.astype(int)

    # For each year, rename row numbers to variable names based on row_map.
    rename_dict = {}
    out_df = pd.DataFrame()
    for year in row_map.index:
        rename_dict = {v: k for k, v in dict(row_map.loc[year, :]).items()}
        _ = rename_dict.pop(-1, None)
        df = ferc1_df.loc[ferc1_df.report_year == year].copy()
        df.loc[:, "row_name"] = df.loc[:, "row_number"].replace(rename_dict)
        # The concatenate according to row_name
        out_df = pd.concat([out_df, df], axis="index")

    # Is this list of index columns universal? Or should they be an argument?
    idx_cols = ["respondent_id", "report_year", "report_prd", "spplmnt_num", "row_name"]
    logger.info(
        f"{len(out_df[out_df.duplicated(idx_cols)])/len(out_df):.4%} "
        f"of unpacked records were duplicates, and discarded."
    )
    # Index the dataframe based on the list of index_cols
    # Unstack the dataframe based on variable names
    out_df = (
        out_df.loc[:, idx_cols + data_cols]
        # These lost records should be minimal. If not, something's wrong.
        .drop_duplicates(subset=idx_cols)
        .set_index(idx_cols)
        .unstack("row_name")
        .loc[:, (slice(None), data_rows)]
    )
    return out_df


def cols_to_cats(df, cat_name, col_cats):
    """Turn top-level MultiIndex columns into a categorial column.

    In some cases FERC Form 1 data comes with many different types of related values
    interleaved in the same table -- e.g. current year and previous year income -- this
    can result in DataFrames that are hundreds of columns wide, which is unwieldy. This
    function takes those top level MultiIndex labels and turns them into categories in a
    single column, which can be used to select a particular type of report.

    Args:
        df (pandas.DataFrame): the dataframe to be simplified.
        cat_name (str): the label of the column to be created indicating what
            MultiIndex label the values came from.
        col_cats (dict): a dictionary with top level MultiIndex labels as keys,
            and the category to which they should be mapped as values.

    Returns:
        pandas.DataFrame: A re-shaped/re-labeled dataframe with one fewer levels of
        MultiIndex in the columns, and an additional column containing the assigned
        labels.
    """
    out_df = pd.DataFrame()
    for col, cat in col_cats.items():
        logger.info(f"Col: {col}, Cat: {cat}")
        tmp_df = df.loc[:, col].copy().dropna(how="all")
        tmp_df.loc[:, cat_name] = cat
        out_df = pd.concat([out_df, tmp_df])
    return out_df.reset_index()


def _clean_cols(df, table_name):
    """Adds a FERC record ID and drop FERC columns not to be loaded into PUDL.

    It is often useful to be able to tell exactly which record in the FERC Form 1
    database a given record within the PUDL database came from. Within each FERC Form 1
    table, each record is supposed to be uniquely identified by the combination of:
    report_year, report_prd, respondent_id, spplmnt_num, row_number.

    So this function takes a dataframe, checks to make sure it contains each of those
    columns and that none of them are NULL, and adds a new column to the dataframe
    containing a string of the format:

    {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}

    In some PUDL FERC Form 1 tables (e.g. plant_in_service_ferc1) a single row is
    re-organized into several new records in order to normalize the data and ensure it
    is stored in a "tidy" format. In such cases each of the resulting PUDL records will
    have the same ``record_id``.  Otherwise, the ``record_id`` is expected to be unique
    within each FERC Form 1 table. However there are a handful of cases in which this
    uniqueness constraint is violated due to data reporting issues in FERC Form 1.

    In addition to those primary key columns, there are some columns which are not
    meaningful or useful in the context of PUDL, but which show up in virtually every
    FERC table, and this function drops them if they are present. These columns include:
    row_prvlg, row_seq, item, record_number (a temporary column used in plants_small)
    and all the footnote columns, which end in "_f".

    TODO: remove in xbrl transition. migrated this functionality into
    ``assign_record_id()``. The last chunk of this function that removes the "_f"
    columns should be abandoned in favor of using the metadata to ensure the
    tables have all/only the correct columns.

    Args:
        df (pandas.DataFrame): The DataFrame in which the function looks for columns
            for the unique identification of FERC records, and ensures that those
            columns are not NULL.
        table_name (str): The name of the table that we are cleaning.

    Returns:
        pandas.DataFrame: The same DataFrame with a column appended containing a string
        of the format
        {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}

    Raises:
        AssertionError: If the table input contains NULL columns
    """
    # Make sure that *all* of these columns exist in the proffered table:
    for field in [
        "report_year",
        "report_prd",
        "respondent_id",
        "spplmnt_num",
        "row_number",
    ]:
        if field in df.columns:
            if df[field].isnull().any():
                raise AssertionError(
                    f"Null field {field} found in ferc1 table {table_name}."
                )

    # Create a unique inter-year FERC table record ID:
    df["record_id"] = (
        table_name
        + "_"
        + df.report_year.astype(str)
        + "_"
        + df.report_prd.astype(str)
        + "_"
        + df.respondent_id.astype(str)
        + "_"
        + df.spplmnt_num.astype(str)
    )
    # Because of the way we are re-organizing columns and rows to create well
    # normalized tables, there may or may not be a row number available.
    if "row_number" in df.columns:
        df["record_id"] = df["record_id"] + "_" + df.row_number.astype(str)

        # Check to make sure that the generated record_id is unique... since
        # that's kind of the whole point. There are couple of genuine bad
        # records here that are taken care of in the transform step, so just
        # print a warning.
        n_dupes = df.record_id.duplicated().values.sum()
        if n_dupes:
            dupe_ids = df.record_id[df.record_id.duplicated()].values
            logger.warning(
                f"{n_dupes} duplicate record_id values found "
                f"in pre-transform table {table_name}: {dupe_ids}."
            )
    # May want to replace this with always constraining the cols to the metadata cols
    # at the end of the transform step (or in rename_columns if we don't need any
    # temp columns)
    # Drop any _f columns... since we're not using the FERC Footnotes...
    # Drop columns and don't complain about it if they don't exist:
    no_f = [c for c in df.columns if not re.match(".*_f$", c)]
    df = (
        df.loc[:, no_f]
        .drop(
            [
                "spplmnt_num",
                "row_number",
                "row_prvlg",
                "row_seq",
                "report_prd",
                "item",
                "record_number",
            ],
            errors="ignore",
            axis="columns",
        )
        .rename(columns={"respondent_id": "utility_id_ferc1"})
    )
    return df


########################################################################################
# Old per-table transform functions
########################################################################################
def plants_small(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_small data for loading into PUDL Database.

    This FERC Form 1 table contains information about a large number of small plants,
    including many small hydroelectric and other renewable generation facilities.
    Unfortunately the data is not well standardized, and so the plants have been
    categorized manually, with the results of that categorization stored in an Excel
    spreadsheet. This function reads in the plant type data from the spreadsheet and
    merges it with the rest of the information from the FERC DB based on record number,
    FERC respondent ID, and report year. When possible the FERC license number for small
    hydro plants is also manually extracted from the data.

    This categorization will need to be renewed with each additional year of FERC data
    we pull in. As of v0.1 the small plants have been categorized for 2004-2015.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_small_df = ferc1_dbf_raw_dfs["plants_small_ferc1"]
    # Standardize plant_name_raw capitalization and remove leading/trailing
    # white space -- necesary b/c plant_name_raw is part of many foreign keys.
    ferc1_small_df = pudl.helpers.simplify_strings(
        ferc1_small_df, ["plant_name", "kind_of_fuel"]
    )

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df = pudl.helpers.oob_to_nan(
        ferc1_small_df,
        cols=["yr_constructed"],
        lb=1850,
        ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
    )

    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df["fuel_cost_per_mmbtu"] = ferc1_small_df["fuel_cost"] / 100.0
    ferc1_small_df.drop("fuel_cost", axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df["record_number"] = (
        46 * ferc1_small_df["spplmnt_num"] + ferc1_small_df["row_number"]
    )

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = importlib.resources.open_binary(
        "pudl.package_data.ferc1", "small_plants_2004-2016.xlsx"
    )
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(
        subset=[
            "plant_type",
        ],
        inplace=True,
    )
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[
        [
            "report_year",
            "respondent_id",
            "record_number",
            "plant_name_clean",
            "plant_type",
            "ferc_license",
        ]
    ]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(
        ferc1_small_df,
        small_types_df,
        how="left",
        on=["report_year", "respondent_id", "record_number"],
    )

    # Remove extraneous columns and add a record ID
    ferc1_small_df = _clean_cols(ferc1_small_df, "f1_gnrt_plant")

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name matches formatting of plant_name_raw
    ferc1_small_df = pudl.helpers.simplify_strings(ferc1_small_df, ["plant_name_clean"])

    # in order to create one complete column of plant names, we have to use the
    # cleaned plant names when available and the orignial plant names when the
    # cleaned version is not available, but the strings first need cleaning
    ferc1_small_df["plant_name_clean"] = ferc1_small_df["plant_name_clean"].fillna(
        value=""
    )
    ferc1_small_df["plant_name_clean"] = ferc1_small_df.apply(
        lambda row: row["plant_name"]
        if (row["plant_name_clean"] == "")
        else row["plant_name_clean"],
        axis=1,
    )

    # now we don't need the uncleaned version anymore
    # ferc1_small_df.drop(['plant_name'], axis=1, inplace=True)

    ferc1_small_df.rename(
        columns={
            # FERC 1 DB Name      PUDL DB Name
            "plant_name": "plant_name_ferc1",
            "ferc_license": "ferc_license_id",
            "yr_constructed": "construction_year",
            "capacity_rating": "capacity_mw",
            "net_demand": "peak_demand_mw",
            "net_generation": "net_generation_mwh",
            "plant_cost": "total_cost_of_plant",
            "plant_cost_mw": "capex_per_mw",
            "operation": "opex_operations",
            "expns_fuel": "opex_fuel",
            "expns_maint": "opex_maintenance",
            "kind_of_fuel": "fuel_type",
            "fuel_cost": "fuel_cost_per_mmbtu",
        },
        inplace=True,
    )

    ferc1_transformed_dfs["plants_small_ferc1"] = ferc1_small_df
    return ferc1_transformed_dfs


def plants_hydro(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_hydro data for loading into PUDL Database.

    Standardizes plant names (stripping whitespace and Using Title Case). Also converts
    into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_hydro_df = (
        _clean_cols(ferc1_dbf_raw_dfs["plants_hydro_ferc1"], "f1_hydro")
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ["plant_name"])
        .pipe(
            pudl.helpers.cleanstrings,
            ["plant_const"],
            [CONSTRUCTION_TYPE_CATEGORIES["categories"]],
            unmapped=pd.NA,
        )
        .assign(
            # Converting kWh to MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            # Converting cost per kW installed to cost per MW installed:
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            # Converting kWh to MWh
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0,
        )
        .pipe(
            pudl.helpers.oob_to_nan,
            cols=["yr_const", "yr_installed"],
            lb=1850,
            ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
        )
        .drop(columns=["net_generation", "cost_per_kw", "expns_kwh"])
        .rename(
            columns={
                # FERC1 DB          PUDL DB
                "plant_name": "plant_name_ferc1",
                "project_no": "project_num",
                "yr_const": "construction_year",
                "plant_kind": "plant_type",
                "plant_const": "construction_type",
                "yr_installed": "installation_year",
                "tot_capacity": "capacity_mw",
                "peak_demand": "peak_demand_mw",
                "plant_hours": "plant_hours_connected_while_generating",
                "favorable_cond": "net_capacity_favorable_conditions_mw",
                "adverse_cond": "net_capacity_adverse_conditions_mw",
                "avg_num_of_emp": "avg_num_employees",
                "cost_of_land": "capex_land",
                "cost_structure": "capex_structures",
                "cost_facilities": "capex_facilities",
                "cost_equipment": "capex_equipment",
                "cost_roads": "capex_roads",
                "cost_plant_total": "capex_total",
                "cost_per_mw": "capex_per_mw",
                "expns_operations": "opex_operations",
                "expns_water_pwr": "opex_water_for_power",
                "expns_hydraulic": "opex_hydraulic",
                "expns_electric": "opex_electric",
                "expns_generation": "opex_generation_misc",
                "expns_rents": "opex_rents",
                "expns_engineering": "opex_engineering",
                "expns_structures": "opex_structures",
                "expns_dams": "opex_dams",
                "expns_plant": "opex_plant",
                "expns_misc_plant": "opex_misc_plant",
                "expns_per_mwh": "opex_per_mwh",
                "expns_engnr": "opex_engineering",
                "expns_total": "opex_total",
                "asset_retire_cost": "asset_retirement_cost",
                "": "",
            }
        )
        .drop_duplicates(
            subset=[
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",
            ],
            keep=False,
        )
    )
    if ferc1_hydro_df["construction_type"].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC1 hydro clean, add "
            "string to CONSTRUCTION_TYPES"
        )
    ferc1_hydro_df = ferc1_hydro_df.replace({"construction_type": "unknown"}, pd.NA)
    ferc1_transformed_dfs["plants_hydro_ferc1"] = ferc1_hydro_df
    return ferc1_transformed_dfs


def plants_pumped_storage(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 pumped storage data for loading into PUDL.

    Standardizes plant names (stripping whitespace and Using Title Case). Also converts
    into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_pump_df = (
        _clean_cols(
            ferc1_dbf_raw_dfs["plants_pumped_storage_ferc1"], "f1_pumped_storage"
        )
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ["plant_name"])
        # Clean up the messy plant construction type column:
        .pipe(
            pudl.helpers.cleanstrings,
            ["plant_kind"],
            [CONSTRUCTION_TYPE_CATEGORIES["categories"]],
            unmapped=pd.NA,
        )
        .assign(
            # Converting from kW/kWh to MW/MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            energy_used_for_pumping_mwh=lambda x: x.energy_used / 1000.0,
            net_load_mwh=lambda x: x.net_load / 1000.0,
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0,
        )
        .pipe(
            pudl.helpers.oob_to_nan,
            cols=["yr_const", "yr_installed"],
            lb=1850,
            ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
        )
        .drop(
            columns=[
                "net_generation",
                "energy_used",
                "net_load",
                "cost_per_kw",
                "expns_kwh",
            ]
        )
        .rename(
            columns={
                # FERC1 DB          PUDL DB
                "plant_name": "plant_name_ferc1",
                "project_number": "project_num",
                "tot_capacity": "capacity_mw",
                "project_no": "project_num",
                "plant_kind": "construction_type",
                "peak_demand": "peak_demand_mw",
                "yr_const": "construction_year",
                "yr_installed": "installation_year",
                "plant_hours": "plant_hours_connected_while_generating",
                "plant_capability": "plant_capability_mw",
                "avg_num_of_emp": "avg_num_employees",
                "cost_wheels": "capex_wheels_turbines_generators",
                "cost_land": "capex_land",
                "cost_structures": "capex_structures",
                "cost_facilties": "capex_facilities",
                "cost_wheels_turbines_generators": "capex_wheels_turbines_generators",
                "cost_electric": "capex_equipment_electric",
                "cost_misc_eqpmnt": "capex_equipment_misc",
                "cost_roads": "capex_roads",
                "asset_retire_cost": "asset_retirement_cost",
                "cost_of_plant": "capex_total",
                "cost_per_mw": "capex_per_mw",
                "expns_operations": "opex_operations",
                "expns_water_pwr": "opex_water_for_power",
                "expns_pump_strg": "opex_pumped_storage",
                "expns_electric": "opex_electric",
                "expns_misc_power": "opex_generation_misc",
                "expns_rents": "opex_rents",
                "expns_engneering": "opex_engineering",
                "expns_structures": "opex_structures",
                "expns_dams": "opex_dams",
                "expns_plant": "opex_plant",
                "expns_misc_plnt": "opex_misc_plant",
                "expns_producton": "opex_production_before_pumping",
                "pumping_expenses": "opex_pumping",
                "tot_prdctn_exns": "opex_total",
                "expns_per_mwh": "opex_per_mwh",
            }
        )
        .drop_duplicates(
            subset=[
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",
            ],
            keep=False,
        )
    )
    if ferc1_pump_df["construction_type"].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC 1 pumped storage "
            "clean, add string to CONSTRUCTION_TYPES."
        )
    ferc1_pump_df = ferc1_pump_df.replace({"construction_type": "unknown"}, pd.NA)
    ferc1_transformed_dfs["plants_pumped_storage_ferc1"] = ferc1_pump_df
    return ferc1_transformed_dfs


def plant_in_service(ferc1_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 Plant in Service data for loading into PUDL.

    Re-organizes the original FERC Form 1 Plant in Service data by unpacking the rows as
    needed on a year by year basis, to organize them into columns. The "columns" in the
    original FERC Form 1 denote starting balancing, ending balance, additions,
    retirements, adjustments, and transfers -- these categories are turned into labels
    in a column called "amount_type". Because each row in the transformed table is
    composed of many individual records (rows) from the original table, row_number can't
    be part of the record_id, which means they are no longer unique. To infer exactly
    what record a given piece of data came from, the record_id and the row_map (found in
    the PUDL package_data directory) can be used.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    pis_df = (
        unpack_table(
            ferc1_df=ferc1_raw_dfs["plant_in_service_ferc1"],
            table_name="f1_plant_in_srvce",
            data_rows=slice(None),  # Gotta catch 'em all!
            data_cols=[
                "begin_yr_bal",
                "addition",
                "retirements",
                "adjustments",
                "transfers",
                "yr_end_bal",
            ],
        )
        .pipe(  # Convert top level of column index into a categorical column:
            cols_to_cats,
            cat_name="amount_type",
            col_cats={
                "begin_yr_bal": "starting_balance",
                "addition": "additions",
                "retirements": "retirements",
                "adjustments": "adjustments",
                "transfers": "transfers",
                "yr_end_bal": "ending_balance",
            },
        )
        .rename_axis(columns=None)
        .pipe(_clean_cols, "f1_plant_in_srvce")
        .set_index(["utility_id_ferc1", "report_year", "amount_type", "record_id"])
        .reset_index()
    )

    # Get rid of the columns corresponding to "header" rows in the FERC
    # form, which should *never* contain data... but in about 2 dozen cases,
    # they do. See this issue on Github for more information:
    # https://github.com/catalyst-cooperative/pudl/issues/471
    pis_df = pis_df.drop(columns=pis_df.filter(regex=".*_head$").columns)

    ferc1_transformed_dfs["plant_in_service_ferc1"] = pis_df
    return ferc1_transformed_dfs


def purchased_power(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 pumped storage data for loading into PUDL.

    This table has data about inter-utility power purchases into the PUDL DB. This
    includes how much electricty was purchased, how much it cost, and who it was
    purchased from. Unfortunately the field describing which other utility the power was
    being bought from is poorly standardized, making it difficult to correlate with
    other data. It will need to be categorized by hand or with some fuzzy matching
    eventually.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    # grab table from dictionary of dfs
    df = (
        _clean_cols(ferc1_dbf_raw_dfs["purchased_power_ferc1"], "f1_purchased_pwr")
        .rename(
            columns={
                "athrty_co_name": "seller_name",
                "sttstcl_clssfctn": "purchase_type_code",
                "rtsched_trffnbr": "tariff",
                "avgmth_bill_dmnd": "billing_demand_mw",
                "avgmth_ncp_dmnd": "non_coincident_peak_demand_mw",
                "avgmth_cp_dmnd": "coincident_peak_demand_mw",
                "mwh_purchased": "purchased_mwh",
                "mwh_recv": "received_mwh",
                "mwh_delvd": "delivered_mwh",
                "dmnd_charges": "demand_charges",
                "erg_charges": "energy_charges",
                "othr_charges": "other_charges",
                "settlement_tot": "total_settlement",
            }
        )
        .assign(  # Require these columns to numeric, or NaN
            billing_demand_mw=lambda x: pd.to_numeric(
                x.billing_demand_mw, errors="coerce"
            ),
            non_coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.non_coincident_peak_demand_mw, errors="coerce"
            ),
            coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.coincident_peak_demand_mw, errors="coerce"
            ),
        )
        .fillna(
            {  # Replace blanks w/ 0.0 in data columns.
                "purchased_mwh": 0.0,
                "received_mwh": 0.0,
                "delivered_mwh": 0.0,
                "demand_charges": 0.0,
                "energy_charges": 0.0,
                "other_charges": 0.0,
                "total_settlement": 0.0,
            }
        )
    )

    # Reencode the power purchase types:
    df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("purchased_power_ferc1")
        .encode(df)
    )

    # Drop records containing no useful data and also any completely duplicate
    # records -- there are 6 in 1998 for utility 238 for some reason...
    df = df.drop_duplicates().drop(
        df.loc[
            (
                (df.purchased_mwh == 0)
                & (df.received_mwh == 0)
                & (df.delivered_mwh == 0)
                & (df.demand_charges == 0)
                & (df.energy_charges == 0)
                & (df.other_charges == 0)
                & (df.total_settlement == 0)
            ),
            :,
        ].index
    )

    ferc1_transformed_dfs["purchased_power_ferc1"] = df

    return ferc1_transformed_dfs


def accumulated_depreciation(
    ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs
):
    """Transforms FERC Form 1 depreciation data for loading into PUDL.

    This information is organized by FERC account, with each line of the FERC Form 1
    having a different descriptive identifier like 'balance_end_of_year' or
    'transmission'.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    # grab table from dictionary of dfs
    ferc1_apd_df = ferc1_dbf_raw_dfs["accumulated_depreciation_ferc1"]

    ferc1_acct_apd = FERC_DEPRECIATION_LINES.drop(["ferc_account_description"], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd["row_number"] = ferc1_acct_apd["row_number"].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(
        ferc1_apd_df, ferc1_acct_apd, how="left", on="row_number"
    )
    ferc1_accumdepr_prvsn_df = _clean_cols(
        ferc1_accumdepr_prvsn_df, "f1_accumdepr_prvsn"
    )

    ferc1_accumdepr_prvsn_df.rename(
        columns={
            # FERC1 DB   PUDL DB
            "total_cde": "total"
        },
        inplace=True,
    )

    ferc1_transformed_dfs["accumulated_depreciation_ferc1"] = ferc1_accumdepr_prvsn_df

    return ferc1_transformed_dfs
