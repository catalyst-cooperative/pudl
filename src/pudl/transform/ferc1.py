"""Classes & functions to process FERC Form 1 data before loading into the PUDL DB.

Note that many of the classes/objects here inherit from/are instances of classes defined
in :mod:`pudl.transform.classes`. Their design and relationships to each other are
documented in that module.

See :mod:`pudl.transform.params.ferc1` for the values that parameterize many of these
transformations.
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
    enforce_snake_case,
)

# This is only here to keep the module importable. Removal Breaks legacy functions.
CONSTRUCTION_TYPE_CATEGORIES = {}

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
    PURCHASED_POWER = "purchased_power_ferc1"


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
        return (
            self.merge_instant_and_duration_tables_xbrl(
                raw_xbrl_instant, raw_xbrl_duration
            )
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
    def merge_instant_and_duration_tables_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge the XBRL instant and duration tables into a single dataframe.

        FERC1 XBRL instant period signifies that it is true as of the reported date,
        while a duration fact pertains to the specified time period. The ``date`` column
        for an instant fact corresponds to the ``end_date`` column of a duration fact.

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
        if dupe_ids.any():
            logger.warning(
                f"{self.table_id.value}: Found {len(dupe_ids)} duplicate record_ids. "
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


class PlantsHydroFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the ``plants_hydro_ferc1`` table."""

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_HYDRO_FERC1

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Table specific transforms for plants_hydro_ferc1.

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
            .pipe(self.nullify_outliers)
            .pipe(self.drop_invalid_rows)
        )
        df["project_num"] = self.strip_col_of_non_ints(df["project_num"])
        return df

    def strip_col_of_non_ints(self, col: pd.Series) -> pd.DataFrame:
        """Strip the column of any non-integer values."""
        return pd.to_numeric(
            col,
            errors="coerce",
        )


class PlantsSmallFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the ``plants_small_ferc1`` table."""

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
            .pipe(self.categorize_strings)
            .pipe(self.extract_ferc1_license)
            .pipe(self.label_row_type)
            .pipe(self.map_header_fuel_types)
            .pipe(self.map_plant_name_fuel_types)
            .pipe(self.associate_notes_with_values)
            # .pipe(self.drop_invalid_rows)
        )
        # Remove headers and note rows
        df = df[(df["row_type"] != "header") & (df["row_type"] != "note")].copy()

        return df

    def extract_ferc1_license(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract FERC license number from plant_name.

        Many FERC license numbers are embedded in the plant_name_ferc1 field, whether
        as a note or an actual plant name. Not all numbers in the plant_name_ferc1 field
        that are FERC licenses, however. Some are dates, dollar amounts, page numbers,
        or numbers of wind turbines. This function extracts valid FERC license numbers
        and puts them in a new column.

        Potential FERC license numbers are valid when:
        - Two or more integers were found.
        - The found integers were accompanied bykey phrases such as "license", "no.",
        "ferc", or "project".
        - The accompanying name does not contain phrases like "page", "pg", "$", "wind",
        "units".
        - The found integers don't fall don't fall within the range of a valid year,
        defined as: 1900-2050.
        - The plant record is categorized as a hydro or not categorized via the
        `plant_type` and `fuel_type` columns.

        This function also fills "other" fuel types with hydro for all these plants.
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
        exceptions = out_df.plant_name_ferc1.str.contains(
            r"tomahawk|otter rapids|wausau|alexander|hooksett|north umpqua", regex=True
        )
        year_vs_num = (out_df["license_id_ferc1"] > 1900) & (
            out_df["license_id_ferc1"] < 2050
        )
        not_hydro = ~out_df["plant_type"].isin(["hydro", np.nan, None]) | ~out_df[
            "fuel_type"
        ].isin(["hydro", "other"])
        # Replace all the non-license numbers with NA
        out_df.loc[
            (not_hydro & ~obvious_license)
            | not_license
            | (year_vs_num & ~obvious_license & ~exceptions),
            "license_id_ferc1",
        ] = np.nan
        # Fill fuel type with hydro
        out_df.loc[
            out_df["license_id_ferc1"].notna() & (out_df["fuel_type"] == "other"),
            "fuel_type",
        ] = "hydro"

        return out_df

    def label_row_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Label rows as headers, notes, or totals."""
        # Define header qualifications
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
        nonheader_strings = [
            "#",
            r"\*",
            "pg",
            "solargenix",
            "solargennix",
            r"\@",
            "rockton",
            "albany steam",
        ]
        exceptions = [
            "hydro plants: licensed proj. no.",
            "hydro license no.",
            "hydro: license no.",
            "hydro plants: licensed proj no.",
        ]
        not_header_if_cols_na = [
            "construction_year",
            "net_generation_mwh",
            "total_cost_of_plant",
            "capex_per_mw",
            "opex_total",
            "opex_fuel",
            "opex_maintenance",
            "fuel_cost_per_mmbtu",
        ]

        def label_header_rows(df: pd.DataFrame) -> pd.DataFrame:
            """Label header rows.

            This function labels rows it believes are possible headers based on whether
            they contain information in certain key columns. Of those possible headers,
            ones that contain a specific key word or phrase are dubbed headers. Leftover
            possible header rows are evaluated as possible note rows in the
            _label_notes_rows() function.
            """
            logger.info(f"{self.table_id.value}: Labeling header rows")

            # Label possible header rows (based on the nan cols specified above)
            df.loc[
                df.filter(not_header_if_cols_na).isna().all(1), "possible_header"
            ] = True

            # Label good header rows (based on whether they contain key strings)
            possible_header = df["possible_header"]
            good_header = df["plant_name_ferc1"].str.contains("|".join(header_strings))
            not_bad = ~df["plant_name_ferc1"].str.contains("|".join(nonheader_strings))

            df.loc[possible_header & good_header & not_bad, "row_type"] = "header"
            df.loc[df["plant_name_ferc1"].isin(exceptions), "row_type"] = "header"

            return df

        def label_total_rows(df: pd.DataFrame) -> pd.DataFrame:
            """Label total rows."""
            logger.info(f"{self.table_id.value}: Labeling total rows")

            df.loc[df["plant_name_ferc1"].str.contains("total"), "row_type"] = "total"

            # Now deal with some outliers:

            # There are a couple rows that contain the phrase "amounts are for" in the
            # plant name that contain total pertaining to total records above. This
            # section of code moves the total row values that are reported as notes to
            # correct row above and nulls the values in the notes row.
            num_cols = [
                x
                for x in df.select_dtypes(include=["float", "Int64"]).columns.tolist()
                if x not in ["utility_id_ferc1", "report_year", "license_id_ferc1"]
            ]
            bad_row = df[
                (df["plant_name_ferc1"].str.contains("amounts are for"))
                & (df["capacity_mw"] > 0)
            ]

            df.loc[bad_row.index, num_cols] = df.loc[bad_row.index - 1][num_cols].values
            df.loc[bad_row.index - 1, num_cols] = np.nan

            return df

        def label_notes_rows(df: pd.DataFrame) -> pd.DataFrame:
            """Remove clumps of consecutive rows flagged as possible headers.

            FERC has lots of note rows that are not headers but are also not useful for
            analysis. This function looks for rows flagged as possible headers (based on
            NA values) and checks to see if there are multiple in a row. A header row is
            (usually) defined as a row with NA values followed by rows without NA
            values, so when there are more than one clumped together they are likely
            either notes or not helpful. Sometimes note clumps will end with a
            meaningful header. This function also checks for this and will unclump any
            headers at the bottom of clumps. There is one exception to this case which
            is a header that is followed by a plant that had no values reported...
            Unfortunately I haven't built a work around, but hopefully there aren't very
            many of these. Currently, that header and plant will be categorized as
            clumps and removed.
            """
            logger.info(f"{self.table_id.value}: Labeling notes rows")

            util_groups = df.groupby(["utility_id_ferc1", "report_year"])

            def _find_header_clumps_sg(group, group_col):
                """Count groups of possible headers in a given utiltiy group.

                This function is used within the _label_note_rows() function. It takes a
                utility group and regroups it by rows where possible_header = True
                (i.e.: all values in the specified NAN_COLS are NA) vs. False. Rows
                where possible_header = True can be bad data, headers, or notes. The
                result is a DataFrame that contains one row per clump of similar
                adjecent possible_header values with columns val_col depicting the
                number of rows per possible_header clump.

                Ex: If you pass in a df with the possible_header values: True, False
                False, True, True, the header_groups output df will look like this:
                {'header':[True, False, True], 'val_col: [1, 2, 2]}.

                Args:
                    group (pandas.DataFrameGroupBy): A groupby object that you'd like
                        to condense by group_col
                    group_col (str): The name of the column you'd like to make sub
                        groups from.

                Returns:
                    pandas.DataFrame: A condensed version of that dataframe input
                        grouped by breaks in header row designation.
                """
                # Make groups based on consecutive sections where the group_col is alike.
                header_groups = group.groupby(
                    (group[f"{group_col}"].shift() != group[f"{group_col}"]).cumsum(),
                    as_index=False,
                )

                # Identify the first (and only) group_col value for each group and count
                # how many rows are in each group.
                header_groups_df = header_groups.agg(
                    header=(f"{group_col}", "first"),
                    val_count=(f"{group_col}", "count"),
                )

                return header_groups, header_groups_df

            def _label_notes_rows_group(util_year_group):
                """Find an label notes rows in a designated sub-group of the sg table.

                Utilities report to FERC on a yearly basis therefore it is on a utility
                and yearly basis by which we need to parse the data. Each year for each
                utility appears in the data like a copy of a pdf form. If you are
                looking for rows that are notes or headers, this context is extremely
                important. For example. Flagged headers that appear at the bottom of a
                given utility-year subgroup are notes rather than headers strictly due
                to their location in the group. For this reason, we must parse the notes
                from the header groups at the utility-year level rather than the dataset
                as a whole.

                Args:
                    util_year_group (pandas.DataFrame): A groupby object that contains
                        a single year and utility.
                """
                # Create mini groups that count pockets of true and false for each
                # utility and year. Basically what it does is create a df
                # where each row represents a clump of adjecent, equal values for a
                # given column. Ex: a column of True, True, True, False, True, False,
                # False, will appear as True, False, True, False with value counts for
                # each.
                group, header_count = _find_header_clumps_sg(
                    util_year_group, "possible_header"
                )

                # Used later to enable exceptions
                max_df_val = util_year_group.index.max()

                # Create a list of the index values that comprise each of the header
                # clumps. It's only considered a clump if it is greater than 1.
                idx_list = list(
                    header_count[
                        (header_count["header"]) & (header_count["val_count"] > 1)
                    ].index
                )

                # If the last row is not a clump (i.e. there is just one value) but it
                # is a header (i.e. has nan values) then also include it in the index
                # values to be flagged because it might be a one-liner note. And because
                # it is at the bottom there is no chance it can actually be a useful
                # header because there are no value rows below it.
                last_row = header_count.tail(1)
                if (last_row["header"].item()) & (last_row["val_count"].item() == 1):
                    idx_list = idx_list + list(last_row.index)
                # If there are any clumped/end headers:
                if idx_list:
                    for idx in idx_list:
                        # Check to see if last clump bit is not a header... sometimes
                        # you might find a clump of notes FOLLOWED by a useful header.
                        # This next bit will check the last row in each of the
                        # identified clumps and "unclump" it if it looks like a valid
                        # header. We only need to check clumps that fall in the middle
                        # because, as previously mentioned, the last row cannot contain
                        # any meaningful header information because there are no values
                        # below it.
                        idx_range = group.groups[idx + 1]
                        is_middle_clump = group.groups[idx + 1].max() < max_df_val
                        is_good_header = (
                            util_year_group.loc[
                                util_year_group.index.isin(group.groups[idx + 1])
                            ]
                            .tail(1)["plant_name_ferc1"]
                            .str.contains("|".join(header_strings))
                            .all()
                        )
                        # If the clump is in the middle and the last row looks like a
                        # header, then drop it from the idx range
                        if is_middle_clump & is_good_header:
                            idx_range = [x for x in idx_range if x != idx_range.max()]
                        # Label the clump as a note
                        util_year_group.loc[
                            util_year_group.index.isin(idx_range), "row_type"
                        ] = "note"

                return util_year_group

            return util_groups.apply(lambda x: _label_notes_rows_group(x))

        # Add some new helper columns
        df.insert(3, "possible_header", False)
        df.insert(3, "row_type", np.nan)

        # Label the row types
        df_labeled = (
            df.pipe(label_header_rows)
            .pipe(label_total_rows)
            .pipe(label_notes_rows)
            .drop(columns=["possible_header"])
        )

        return df_labeled

    def map_header_fuel_types(
        self, df: pd.DataFrame, show_unmapped_headers=False
    ) -> pd.DataFrame:
        """Apply the plant type indicated in the header row to the relevant rows.

        This function groups the data by utility, year, and header and forward fills the
        cleaned plant type based on that. As long as each utility year group that uses a
        header for one plant type also uses headers for other plant types this will
        work. I.e., if a utility's plant_name_ferc1 column looks like this: [STEAM,
        coal_plant1, coal_plant2, wind_turbine1], this algorythem will think that wind
        turbine is steam. Ideally (also usually) if they label one, they will label all.
        The ideal version is:

        [STEAM, coal_plant1, coal_plant2, WIND, wind_turbine]. Right now, this function
        puts the new header plant type into a column called plant_type_2. This is so we
        can compare against the current plant_type column for accuracy and validation
        purposes.
        """
        logger.info(f"{self.table_id.value}: Mapping header fuels to relevant rows")

        def expand_dict(dic):
            """Change format of header_labels.

            Right now, the header_labels dictionaries defined above follow this format:

            {clean_name_A: [ bad_name_A1, bad_name_A2, ...], clean_name_B: [bad_name_B1,
            bad_nameB2]}. This is convenient visually, but it makes more sense to format
            it like this: {bad_name_A1: clean_name_A, bad_name_A2: clean_name_A,
            bad_name_B1: clean_name_B, bad_name_B2: clean_name_B}. We could reformat the
            dictionaries, but for now I created this function to do it where necessariy
            in certain functions.
            """
            d = {}
            for k, lst in dic.items():
                for i in range(len(lst)):
                    d[lst[i]] = k
            return d

        header_labels = {  # Add these to the PLANT_CATEGORIES param ideally
            "hydroelectric": ["hydro", "hyrdo"],
            "internal combustion": ["internal", "interal", "international combustion"],
            "combustion turbine": ["combustion turbine"],
            "combined cycle": ["combined cycle"],
            "gas turbine": ["gas"],
            "petroleum liquids": ["oil", "diesel", "diesal"],
            "solar": ["solar", "photovoltaic"],
            "wind": ["wind"],
            "geothermal": ["geothermal"],
            "waste": ["waste", "landfill"],
            "steam": ["steam"],
            "nuclear": ["nuclear"],
            "fuel_cell": ["fuel cell"],
            "other": ["other"],
            "renewables": ["renewables"],
        }
        # Clean header names
        df["header_clean"] = np.nan
        d = expand_dict(header_labels)
        # Map cleaned header names onto df in a new column
        df.loc[df["row_type"] == "header", "header_clean"] = (
            df["plant_name_ferc1"]
            .str.extract(rf"({'|'.join(d.keys())})", expand=False)
            .map(d)
        )
        # Make groups based on utility, year, and header
        header_groups = df.groupby(
            [
                "utility_id_ferc1",
                "report_year",
                (df["row_type"] == "header").cumsum(),
            ]
        )
        # Forward fill based on headers
        df["plant_type_2"] = np.nan
        df.loc[
            df["row_type"] != "note", "plant_type_2"
        ] = header_groups.header_clean.ffill()

        # Document unmapped headers
        unmapped_headers = df[
            (df["row_type"] == "header") & (df["header_clean"].isna())
        ].plant_name_ferc1.value_counts()
        logger.info(f"Found unmapped headers: \n {unmapped_headers}")

        df = df.drop(columns=["header_clean"])

        return df

    def map_plant_name_fuel_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get plant type from plant name.

        If there is a plant type embedded in the plant name (that's not a header) then
        move that to the plant_type_2 column. Right now, this only works for hydro
        plants because the rest are complicated and have a slew of exceptions.
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
        """Use footnotes to map string and ferc license to value rows.

        There are many utilities that report a bunch of note rows at the bottom of their
        yearly entry. These note rows often pertain directly to specific plant rows
        above. Sometimes, the notes and their respective plant rows are connected by a
        footnote such as (a) or (1) etc. This function finds those footnotes, associates
        the "note" version with the regular value row, maps the note content from the
        note row into a new note column that's associated with the value row, also maps
        any ferc license extracted from this note column up to the value row it
        references.
        """
        logger.info(
            f"{self.table_id.value}: Mapping notes and ferc license from footnotes"
        )

        def associate_notes_with_values_group(group):
            """Map footnotes within a given utility year group.

            Because different utilities may use the same footnotes or the same utility
            could reuse footnotes each year, we must do the footnote association within
            utility-year groups.
            """
            regular_row = group["row_type"].isna()
            has_note = group["row_type"] == "note"
            # has_footnote = group.plant_name_ferc1.str.contains(footnote_pattern)

            # Shorten execution time by only looking at groups with discernable footnotes
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
        df["footnote"] = pd.NA
        # Create new footnote column
        df.loc[:, "footnote"] = df.plant_name_ferc1.str.extract(
            footnote_pattern, expand=False
        )
        # Group by year and utility and run footnote association
        groups = df.groupby(["report_year", "utility_id_ferc1"])
        sg_notes = groups.apply(lambda x: associate_notes_with_values_group(x))
        # Remove footnote column now that rows are associated
        sg_notes = sg_notes.drop(columns=["footnote"])

        return sg_notes


def transform(
    ferc1_dbf_raw_dfs: dict[str, pd.DataFrame],
    ferc1_xbrl_raw_dfs: dict[str, dict[str, pd.DataFrame]],
    ferc1_settings: Ferc1Settings | None = None,
) -> dict[str, pd.DataFrame]:
    """Coordinate the transformation of all FERC Form 1 tables.

    Args:
        ferc1_dbf_raw_dfs: Dictionary mapping PUDL table names (keys) to raw DBF
            dataframes (values).
        ferc1_xbrl_raw_dfs: Nested dictionary containing both an instant and duration
            table for each input XBRL table. Some of these are empty.
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
    # aid in FERC plant ID assignment.
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
    """Make the module runnable for testing purposes during development.

    This should probably be removed when we are done with the FERC 1 Transforms.
    """

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
