"""Module to perform data cleaning functions on EIA860 data tables."""

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, asset, asset_check

import pudl
from pudl.helpers import drop_records_with_null_in_column
from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import DataSource
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.transform.eia861 import clean_nerc

logger = pudl.logging_helpers.get_logger(__name__)


@asset
def _core_eia860__ownership(raw_eia860__ownership: pd.DataFrame) -> pd.DataFrame:
    """Pull and transform the ownership table.

    Transformations include:

    * Replace . values with NA.
    * Convert pre-2012 ownership percentages to proportions to match post-2012
      reporting.

    Args:
        raw_eia860__ownership: The raw ``raw_eia860__ownership`` dataframe.

    Returns:
        Cleaned ``_core_eia860__ownership`` dataframe ready for harvesting.
    """
    # Preliminary clean and get rid of unnecessary 'year' column
    own_df = (
        raw_eia860__ownership.copy()
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.convert_to_date)
        .drop(columns=["year"])
    )

    if min(own_df.report_date.dt.year) < min(
        DataSource.from_id("eia860").working_partitions["years"]
    ):
        raise ValueError(
            f"EIA 860 transform step is only known to work for "
            f"year {min(DataSource.from_id('eia860').working_partitions['years'])} and later, "
            f"but found data from year {min(own_df.report_date.dt.year)}."
        )

    # Prior to 2012, ownership was reported as a percentage, rather than
    # as a proportion, so we need to divide those values by 100.
    own_df.loc[own_df.report_date.dt.year < 2012, "fraction_owned"] = (
        own_df.loc[own_df.report_date.dt.year < 2012, "fraction_owned"] / 100
    )

    # This has to come before the fancy indexing below, otherwise the plant_id_eia
    # is still a float.
    own_df = apply_pudl_dtypes(own_df, group="eia")

    # A small number of generators are reported multiple times in the ownership
    # table due to the use of leading zeroes in their integer generator_id values
    # which are stored as strings (since some generators use strings). This
    # makes sure that we only keep a single copy of those duplicated records which
    # we've identified as falling into this category. We refrain from doing a wholesale
    # drop_duplicates() so that if duplicates are introduced by some other mechanism
    # we'll be notified.

    # The plant & generator ID values we know have duplicates to remove.
    known_dupes = (
        own_df.set_index(["plant_id_eia", "generator_id"])
        .sort_index()
        .loc[(56032, "1")]
    )
    # Index of own_df w/ duplicated records removed.
    without_known_dupes_idx = own_df.set_index(
        ["plant_id_eia", "generator_id"]
    ).index.difference(known_dupes.index)
    # own_df w/ duplicated records removed.
    without_known_dupes = (
        own_df.set_index(["plant_id_eia", "generator_id"])
        .loc[without_known_dupes_idx]
        .reset_index()
    )
    # Drop duplicates from the known dupes using the whole primary key
    own_pk = [
        "report_date",
        "plant_id_eia",
        "generator_id",
        "owner_utility_id_eia",
    ]
    deduped = known_dupes.reset_index().drop_duplicates(subset=own_pk)
    # Bring these two parts back together:
    own_df = pd.concat([without_known_dupes, deduped])
    # Check whether we have truly deduplicated the dataframe.
    remaining_dupes = own_df[own_df.duplicated(subset=own_pk, keep=False)]
    if not remaining_dupes.empty:
        raise ValueError(
            f"Duplicate ownership slices found in _core_eia860__ownership: {remaining_dupes}"
        )

    # Remove a couple of records known to have (literal) "nan" values in the
    # generator_id column, which is part of the table's natural primary key.
    # These "nan" strings get converted to true pd.NA values when the column
    # datatypes are applied, which violates the primary key constraints.
    # See https://github.com/catalyst-cooperative/pudl/issues/1207
    mask = (
        (
            own_df.report_date.isin(
                [
                    f"{year}-01-01"
                    for year in range(
                        2018, max(pudl.settings.Eia860Settings().years) + 1
                    )
                ]
            )
        )
        & (own_df.plant_id_eia == 62844)
        & (own_df.owner_utility_id_eia == 62745)
        & (own_df.generator_id == "nan")
    )
    own_df = own_df[~mask]

    if not (nulls := own_df[own_df.generator_id == ""]).empty:
        logger.warning(
            f"Found records with null IDs in _core_eia860__ownership: {nulls}"
        )
    # In 2010 there are several hundred utilities that appear to be incorrectly
    # reporting the owner_utility_id_eia value *also* in the utility_id_eia
    # column. This results in duplicate operator IDs associated with a given
    # generator in a particular year, which should never happen. We identify
    # these values and set them to NA so they don't mess up the harvested
    # relationships between plants and utilities:
    # See https://github.com/catalyst-cooperative/pudl/issues/1116
    duplicate_operators = (
        own_df.groupby(
            ["report_date", "plant_id_eia", "generator_id"]
        ).operator_utility_id_eia.transform(pd.Series.nunique)
    ) > 1
    own_df.loc[duplicate_operators, "operator_utility_id_eia"] = pd.NA

    # The above fix won't catch owner_utility_id_eia values in the
    # utility_id_eia (operator) column when there's only a single
    # owner-operator. But also, when there's a single owner-operator they shouldn't
    # even be reporting in this table. So we can also drop those utility_id_eia
    # values without losing any valuable information here. The utility_id_eia
    # column here is only useful for entity harvesting & resolution purposes
    # since the (report_date, plant_id_eia) tuple fully defines the operator id.
    # See https://github.com/catalyst-cooperative/pudl/issues/1116
    single_owner_operator = (
        own_df.operator_utility_id_eia == own_df.owner_utility_id_eia
    ) & (own_df.fraction_owned == 1.0)
    own_df.loc[single_owner_operator, "operator_utility_id_eia"] = pd.NA
    own_df = PUDL_PACKAGE.encode(own_df)
    # CN is an invalid political subdivision code used by a few respondents to indicate
    # that the owner is in Canada. At least we can recover the country:
    state_to_country = {
        x.subdivision_code: x.country_code for x in POLITICAL_SUBDIVISIONS.itertuples()
    } | {"CN": "CAN"}
    own_df["owner_country"] = own_df["owner_state"].map(state_to_country)
    own_df.loc[own_df.owner_state == "CN", "owner_state"] = pd.NA

    return own_df


@asset
def _core_eia860__generators(
    raw_eia860__generator_proposed: pd.DataFrame,
    raw_eia860__generator_existing: pd.DataFrame,
    raw_eia860__generator_retired: pd.DataFrame,
    raw_eia860__generator: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the generators table.

    There are three tabs that the generator records come from (proposed, existing,
    retired). Pre 2009, the existing and retired data are lumped together under a single
    generator file with one tab. We pull each tab into one dataframe and include an
    ``operational_status`` to indicate which tab the record came from. We use
    ``operational_status`` to parse the pre 2009 files as well.

    Transformations include:

    * Replace . values with NA.
    * Update ``operational_status_code`` to reflect plant status as either proposed,
      existing or retired.
    * Drop values with NA for plant and generator id.
    * Replace 0 values with NA where appropriate.
    * Convert Y/N/X values to boolean True/False.
    * Convert U/Unknown values to NA.
    * Map full spelling onto code values.
    * Create a fuel_type_code_pudl field that organizes fuel types into
      clean, distinguishable categories.

    Args:
        raw_eia860__generator_proposed: The raw ``raw_eia860__generator_proposed`` dataframe.
        raw_eia860__generator_existing: The raw ``raw_eia860__generator_existing`` dataframe.
        raw_eia860__generator_retired: The raw ``raw_eia860__generator_retired`` dataframe.
        raw_eia860__generator: The raw ``raw_eia860__generator`` dataframe.

    Returns:
        Cleaned ``_core_eia860__generators`` dataframe ready for harvesting.
    """
    # Groupby objects were creating chained assignment warning that is N/A
    pd.options.mode.chained_assignment = None

    # There are three sets of generator data reported in the EIA860 table,
    # planned, existing, and retired generators. We're going to concatenate
    # them all together into a single big table, with a column that indicates
    # which one of these tables the data came from, since they all have almost
    # exactly the same structure
    gp_df = raw_eia860__generator_proposed
    ge_df = raw_eia860__generator_existing
    gr_df = raw_eia860__generator_retired
    g_df = raw_eia860__generator
    # the retired tab of eia860 does not have a operational_status_code column.
    # we still want these gens to have a code (and subsequently a
    # operational_status). We could do this by fillna w/ the retirement_date, but
    # this way seems more straightforward.
    gr_df["operational_status_code"] = gr_df["operational_status_code"].fillna("RE")
    # Prep dicts for column based pd.replace:
    # A subset of the columns have zero values, where NA is appropriate:
    nulls_replace_cols = {
        col: {" ": np.nan, 0: np.nan}
        for col in [
            "planned_generator_retirement_month",
            "planned_generator_retirement_year",
            "planned_uprate_month",
            "planned_uprate_year",
            "other_modifications_month",
            "other_modifications_year",
            "planned_derate_month",
            "planned_derate_year",
            "planned_repower_month",
            "planned_repower_year",
            "planned_net_summer_capacity_derate_mw",
            "planned_net_summer_capacity_uprate_mw",
            "planned_net_winter_capacity_derate_mw",
            "planned_net_winter_capacity_uprate_mw",
            "planned_new_capacity_mw",
            "nameplate_power_factor",
            "minimum_load_mw",
            "winter_capacity_mw",
            "summer_capacity_mw",
        ]
    }
    boolean_columns_to_fix = [
        "duct_burners",
        "can_burn_multiple_fuels",
        "deliver_power_transgrid",
        "synchronized_transmission_grid",
        "solid_fuel_gasification",
        "pulverized_coal_tech",
        "fluidized_bed_tech",
        "subcritical_tech",
        "supercritical_tech",
        "ultrasupercritical_tech",
        "carbon_capture",
        "stoker_tech",
        "other_combustion_tech",
        "can_cofire_fuels",
        "can_switch_oil_gas",
        "bypass_heat_recovery",
        "associated_combined_heat_power",
        "planned_modifications",
        "other_planned_modifications",
        "uprate_derate_during_year",
        "previously_canceled",
        "owned_by_non_utility",
        "summer_capacity_estimate",
        "winter_capacity_estimate",
        "distributed_generation",
        "ferc_cogen_status",
        "ferc_small_power_producer",
        "ferc_exempt_wholesale_generator",
        "ferc_qualifying_facility",
    ]
    gens_df = (
        pd.concat([ge_df, gp_df, gr_df, g_df], sort=True)
        .pipe(pudl.helpers.fix_eia_na)
        .dropna(subset=["generator_id", "plant_id_eia"])
        .pipe(
            pudl.helpers.fix_boolean_columns,
            boolean_columns_to_fix=boolean_columns_to_fix,
        )
        .replace(to_replace=nulls_replace_cols)
        .pipe(pudl.helpers.month_year_to_date)
        .pipe(
            pudl.helpers.simplify_strings,
            columns=["rto_iso_lmp_node_id", "rto_iso_location_wholesale_reporting_id"],
        )
        .pipe(pudl.helpers.convert_to_date)
    )
    # This manual fix is required before encoding because there's not a unique mapping
    # PA -> PACW in Oregon
    gens_df.loc[
        (gens_df.state == "OR") & (gens_df.balancing_authority_code_eia == "PA"),
        "balancing_authority_code_eia",
    ] = "PACW"
    # PA -> PACE in Utah
    gens_df.loc[
        (gens_df.state == "UT") & (gens_df.balancing_authority_code_eia == "PA"),
        "balancing_authority_code_eia",
    ] = "PACE"
    gens_df = PUDL_PACKAGE.encode(gens_df)

    gens_df["fuel_type_code_pudl"] = gens_df.energy_source_code_1.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_energy_sources"]["df"],
            from_col="code",
            to_col="fuel_type_code_pudl",
            null_value=pd.NA,
        )
    )

    gens_df["operational_status"] = gens_df.operational_status_code.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_operational_status"]["df"],
            from_col="code",
            to_col="operational_status",
            null_value=pd.NA,
        )
    )

    return gens_df


@asset
def _core_eia860__generators_solar(
    raw_eia860__generator_solar_existing: pd.DataFrame,
    raw_eia860__generator_solar_retired: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the solar-specific generators table.

    Many of the same transforms to the core generators table are applied here.
    Most of the unique solar columns are booleans.

    Notes for possible future cleaning:

    * Both the ``tilt_angle`` and ``azimuth_angle`` columns have a small number of
      negative values (both under 40 records). This seems off, but not impossible?
    * A lot of the boolean columns in this table are mostly null. It is probably
      that a lot of the nulls should correspond to False's, but there is no sure way
      to know, so nulls seem more appropriate.

    """
    solar_existing = raw_eia860__generator_solar_existing
    solar_retired = raw_eia860__generator_solar_retired
    # every boolean column in the raw solar tables has a uses prefix
    boolean_columns_to_fix = list(solar_existing.filter(regex=r"^uses_"))
    if mismatched_bool_cols := set(boolean_columns_to_fix).difference(
        set(solar_retired.filter(regex=r"^uses_"))
    ):
        raise AssertionError(
            "We expect that the raw existing and retired assets to have the exact same "
            f"boolean columns with prefix of uses_ but we found {mismatched_bool_cols=}"
        )
    solar_df = (
        pd.concat([solar_existing, solar_retired], sort=True)
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.fix_boolean_columns, boolean_columns_to_fix)
        .pipe(pudl.helpers.month_year_to_date)
        .pipe(pudl.helpers.convert_to_date)
        .pipe(PUDL_PACKAGE.encode)
    )

    solar_df["operational_status"] = solar_df.operational_status_code.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_operational_status"]["df"],
            from_col="code",
            to_col="operational_status",
            null_value=pd.NA,
        )
    )
    return solar_df


@asset
def _core_eia860__generators_energy_storage(
    raw_eia860__generator_energy_storage_existing: pd.DataFrame,
    raw_eia860__generator_energy_storage_proposed: pd.DataFrame,
    raw_eia860__generator_energy_storage_retired: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the energy storage specific generators table."""
    storage_ex = raw_eia860__generator_energy_storage_existing
    storage_pr = raw_eia860__generator_energy_storage_proposed.pipe(
        drop_records_with_null_in_column,
        column="generator_id",
        num_of_expected_nulls=2,  # Plant ID 62844 in 2023-4
    )
    storage_re = raw_eia860__generator_energy_storage_retired

    # every boolean column in the raw storage tables has a served_ or stored_ prefix
    boolean_columns_to_fix = list(storage_ex.filter(regex=r"^served_|^stored_|^is_"))

    storage_df = (
        pd.concat([storage_ex, storage_pr, storage_re], sort=True)
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.month_year_to_date)
        .pipe(pudl.helpers.convert_to_date)
        .pipe(
            pudl.helpers.fix_boolean_columns,
            boolean_columns_to_fix=boolean_columns_to_fix,
        )
        .pipe(PUDL_PACKAGE.encode)
    )

    storage_df["operational_status"] = (
        storage_df.operational_status_code.str.upper().map(
            pudl.helpers.label_map(
                CODE_METADATA["core_eia__codes_operational_status"]["df"],
                from_col="code",
                to_col="operational_status",
                null_value=pd.NA,
            )
        )
    )

    # Capitalize the direct_support generator IDs
    # We harvest these values into our generator tables, but they aren't as
    # well-normalized as the directly reported generator IDs. The differences in
    # capitalization were marking a larger number of generators as non-existent
    # than are actually the case (e.g., a plant reporting both generator GEN1 and Gen1).
    # See PR#3699 and PR#4332. We manually fix a known list of these.

    # Any remaining 'fake' generator IDs get dropped in _out_eia__yearly_generators

    known_bad_caps = [
        "SunB",
        "EcheB",
        "MayB",
        "IssaP",
        "Matad",
        "WolfB",
        "IrisB",
        "TwinB",
    ]
    filter_condition = (storage_df.report_date == "2024-01-01") & (
        storage_df.generator_id_direct_support_1.isin(known_bad_caps)
    )

    storage_df.loc[filter_condition, "generator_id_direct_support_1"] = storage_df.loc[
        filter_condition, "generator_id_direct_support_1"
    ].str.upper()

    return storage_df


@asset
def _core_eia860__generators_wind(
    raw_eia860__generator_wind_existing: pd.DataFrame,
    raw_eia860__generator_wind_retired: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the wind-specific generators table.

    Many of the same transforms to the core generators table are applied here.

    Some notes for possible cleaning later:

    * technology_description: this field didn't exist in 2013. We could try to backfill.
      this is an annual scd so it'll get slurped up there and backfilling does happen
      in the output layer via :func:`pudl.output.eia.fill_generator_technology_description`
    * turbines_num: this field doesn't show up in this table for 2013 and 2014, but it does
      exist in the 2001-2012 generators tab. This is an annual generator scd.

    """
    wind_ex = raw_eia860__generator_wind_existing
    wind_re = raw_eia860__generator_wind_retired.pipe(
        drop_records_with_null_in_column,
        column="generator_id",
        num_of_expected_nulls=1,
    )

    wind_df = (
        pd.concat([wind_ex, wind_re], sort=True)
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.month_year_to_date)
        .pipe(pudl.helpers.convert_to_date)
        .pipe(
            pudl.helpers.simplify_strings,
            columns=["predominant_turbine_manufacturer"],
        )
        .convert_dtypes()  # converting here before the wind encoding bc int's are codes
        .pipe(PUDL_PACKAGE.encode)
    )

    wind_df["operational_status"] = wind_df.operational_status_code.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_operational_status"]["df"],
            from_col="code",
            to_col="operational_status",
            null_value=pd.NA,
        )
    )
    return wind_df


@asset
def _core_eia860__generators_multifuel(
    raw_eia860__multifuel_existing: pd.DataFrame,
    raw_eia860__multifuel_proposed: pd.DataFrame,
    raw_eia860__multifuel_retired: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the multifuel generators table."""
    multifuel_ex = raw_eia860__multifuel_existing
    multifuel_pr = raw_eia860__multifuel_proposed
    multifuel_re = raw_eia860__multifuel_retired

    boolean_columns_to_fix = [
        "has_air_permit_limits",
        "can_cofire_100_oil",
        "can_cofire_fuels",
        "has_factors_that_limit_switching",
        "can_burn_multiple_fuels",
        "has_other_factors_that_limit_switching",
        "has_storage_limits",
        "can_switch_oil_gas",
        "can_switch_when_operating",
    ]

    # A subset of the columns have zero values, where NA is appropriate:
    nulls_replace_cols = {
        col: {" ": np.nan, 0: np.nan}
        for col in [
            "winter_capacity_mw",
            "summer_capacity_mw",
        ]
    }

    multifuel_df = (
        pd.concat([multifuel_ex, multifuel_pr, multifuel_re], sort=True)
        .dropna(subset=["generator_id", "plant_id_eia"])
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(
            pudl.helpers.fix_boolean_columns,
            boolean_columns_to_fix=boolean_columns_to_fix,
        )
        .replace(to_replace=nulls_replace_cols)
        .pipe(pudl.helpers.month_year_to_date)
        .pipe(pudl.helpers.convert_to_date)
        .pipe(PUDL_PACKAGE.encode)
    )

    multifuel_df["fuel_type_code_pudl"] = (
        multifuel_df.energy_source_code_1.str.upper().map(
            pudl.helpers.label_map(
                CODE_METADATA["core_eia__codes_energy_sources"]["df"],
                from_col="code",
                to_col="fuel_type_code_pudl",
                null_value=pd.NA,
            )
        )
    )

    multifuel_df["operational_status"] = (
        multifuel_df.operational_status_code.str.upper().map(
            pudl.helpers.label_map(
                CODE_METADATA["core_eia__codes_operational_status"]["df"],
                from_col="code",
                to_col="operational_status",
                null_value=pd.NA,
            )
        )
    )

    # There are some pesky duplicate rows from the plant_id 56032 gen_id 1
    # The rows are almost identical, so this gets rid of the duplicates.
    # It only drops known duplicates from 56032 so we can spot any other
    # irregularities in the future.
    dupe_pk_rows_to_drop = multifuel_df[
        multifuel_df[["report_date", "plant_id_eia", "generator_id"]].duplicated()
        & (multifuel_df["plant_id_eia"] == 56032)
    ]
    multifuel_df = multifuel_df.drop(dupe_pk_rows_to_drop.index)
    return multifuel_df


@asset
def _core_eia860__plants(raw_eia860__plant: pd.DataFrame) -> pd.DataFrame:
    """Pull and transform the plants table.

    Much of the static plant information is reported repeatedly, and scattered across
    several different pages of EIA 923. The data frame which this function uses is
    assembled from those many different pages, and passed in via the same dictionary of
    dataframes that all the other ingest functions use for uniformity.

    Transformations include:

    * Replace . values with NA.
    * Homogenize spelling of county names.
    * Convert Y/N/X values to boolean True/False.

    Args:
        raw_eia860__plant: The raw ``raw_eia860__plant`` dataframe.

    Returns:
        Cleaned ``_core_eia860__plants`` dataframe ready for harvesting.
    """
    # Populating the '_core_eia860__plants' table
    p_df = (
        raw_eia860__plant.pipe(pudl.helpers.fix_eia_na)
        .astype({"zip_code": str})
        .drop("iso_rto", axis="columns")
    )

    # Spelling, punctuation, and capitalization of county names can vary from
    # year to year. We homogenize them here to facilitate correct value
    # harvesting.
    p_df["county"] = (
        p_df.county.str.replace(r"[^a-z,A-Z]+", " ", regex=True)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    # A subset of the columns have "X" values, where other columns_to_fix
    # have "N" values. Replacing these values with "N" will make for uniform
    # values that can be converted to Boolean True and False pairs.

    p_df.ash_impoundment_lined = p_df.ash_impoundment_lined.replace(
        to_replace="X", value="N"
    )
    p_df.natural_gas_storage = p_df.natural_gas_storage.replace(
        to_replace="X", value="N"
    )
    p_df.liquefied_natural_gas_storage = p_df.liquefied_natural_gas_storage.replace(
        to_replace="X", value="N"
    )

    boolean_columns_to_fix = [
        "ferc_cogen_status",
        "ferc_small_power_producer",
        "ferc_exempt_wholesale_generator",
        "ash_impoundment",
        "ash_impoundment_lined",
        "energy_storage",
        "natural_gas_storage",
        "liquefied_natural_gas_storage",
        "has_net_metering",
    ]

    for column in boolean_columns_to_fix:
        p_df[column] = (
            p_df[column]
            .fillna("NaN")
            .replace(to_replace=["Y", "N", "NaN"], value=[True, False, pd.NA])
        )

    p_df = pudl.helpers.convert_to_date(p_df).pipe(
        clean_nerc, idx_cols=["plant_id_eia", "report_date", "nerc_region"]
    )

    return p_df


@asset
def _core_eia860__boiler_generator_assn(
    raw_eia860__boiler_generator_assn: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the boilder generator association table.

    Transformations include:

    * Drop non-data rows with EIA notes.
    * Drop duplicate rows.

    Args:
        raw_eia860__boiler_generator_assn: Each entry in this dictionary of DataFrame
            objects corresponds to a page from the EIA860 form, as reported in the Excel
            spreadsheets they distribute.

    Returns:
        Cleaned ``_core_eia860__boiler_generator_assn`` dataframe ready for harvesting.
    """
    # Populating the 'core_eia860__scd_generators' table
    b_g_df = (
        pudl.helpers.convert_to_date(raw_eia860__boiler_generator_assn)
        .pipe(pudl.helpers.convert_cols_dtypes, data_source="eia")
        .drop_duplicates()
    )

    return b_g_df


@asset
def _core_eia860__utilities(raw_eia860__utility: pd.DataFrame) -> pd.DataFrame:
    """Pull and transform the utilities table.

    Transformations include:

    * Replace . values with NA.
    * Fix typos in state abbreviations, convert to uppercase.
    * Drop address_3 field (all NA).
    * Combine phone number columns into one field and set values that don't mimic real
      US phone numbers to NA.
    * Convert Y/N/X values to boolean True/False.
    * Map full spelling onto code values.

    Args:
        raw_eia860__utility: The raw ``raw_eia860__utility`` dataframe.

    Returns:
        Cleaned ``_core_eia860__utilities`` dataframe ready for harvesting.
    """
    # Populating the '_core_eia860__utilities' table
    u_df = raw_eia860__utility

    # Replace empty strings, whitespace, and '.' fields with real NA values
    u_df = pudl.helpers.fix_eia_na(u_df)
    u_df["state"] = u_df.state.str.upper()
    u_df["state"] = u_df.state.replace(
        {
            "QB": "QC",  # wrong abbreviation for Quebec
            "Y": "NY",  # Typo
        }
    )

    # Remove Address 3 column that is all NA
    u_df = u_df.drop(["address_3"], axis=1)

    # Combine phone number columns into one
    def _make_phone_number(col1, col2, col3):
        """Make and validate full phone number separated by dashes."""
        p_num = (
            col1.astype("string")
            + "-"
            + col2.astype("string")
            + "-"
            + col3.astype("string")
        )
        # Turn anything that doesn't match a US phone number format to NA
        # using noqa to get past flake8 test that give a false positive thinking
        # that the regex string is supposed to be an f-string and is missing
        # the it's designated prefix.
        return p_num.replace(regex=r"^(?!.*\d{3}-\d{3}-\d{4}).*$", value=pd.NA)

    u_df = u_df.assign(
        phone_number=_make_phone_number(
            u_df.phone_number_first, u_df.phone_number_mid, u_df.phone_number_last
        ),
        phone_number_2=_make_phone_number(
            u_df.phone_number_first_2, u_df.phone_number_mid_2, u_df.phone_number_last_2
        ),
    )

    boolean_columns_to_fix = [
        "plants_reported_owner",
        "plants_reported_operator",
        "plants_reported_asset_manager",
        "plants_reported_other_relationship",
    ]

    for column in boolean_columns_to_fix:
        u_df[column] = (
            u_df[column]
            .fillna("NaN")
            .replace(to_replace=["Y", "N", "NaN"], value=[True, False, pd.NA])
        )

    u_df = (
        u_df.astype({"utility_id_eia": "Int64"})
        .pipe(pudl.helpers.convert_to_date)
        .fillna({"entity_type": pd.NA})
    )

    return u_df


@asset
def _core_eia860__boilers(
    raw_eia860__emission_control_strategies: pd.DataFrame,
    raw_eia860__boiler_info: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the boilers table.

    Transformations include:

    * Replace . values with NA.
    * Convert Y/N/NA values to boolean True/False.
    * Combine month and year columns into date columns.
    * Add boiler manufacturer name column.
    * Convert pre-2012 efficiency percentages to proportions to match post-2012
      reporting.

    Args:
        raw_eia860__emission_control_strategies: DataFrame extracted from EIA forms
            earlier in the ETL process.
        raw_eia860__boiler_info: DataFrame extracted from EIA forms earlier in the ETL
            process.

    Returns:
        The transformed boilers table.
    """
    # Populating the 'core_eia860__scd_boilers' table
    b_df = raw_eia860__boiler_info
    ecs = raw_eia860__emission_control_strategies

    # Combine and replace empty strings, whitespace, and '.' fields with real NA values

    b_df = (
        pd.concat([b_df, ecs], sort=True)
        .dropna(subset=["boiler_id", "plant_id_eia"])
        .pipe(pudl.helpers.fix_eia_na)
    )

    # Defensive check: if any values in boiler_fuel_code_5 - boiler_fuel_code_8,
    # raise error.
    cols_to_check = [
        "boiler_fuel_code_5",
        "boiler_fuel_code_6",
        "boiler_fuel_code_7",
        "boiler_fuel_code_8",
    ]
    if b_df[cols_to_check].notnull().sum().sum() > 0:
        raise ValueError(
            "There are non-null values in boiler_fuel_code #5-8."
            "These are currently getting dropped from the final dataframe."
            "Please revise the table schema to include these columns."
        )

    # Replace 0's with NaN for certain columns.
    zero_columns_to_fix = [
        "firing_rate_using_coal_tons_per_hour",
        "firing_rate_using_gas_mcf_per_hour",
        "firing_rate_using_oil_bbls_per_hour",
        "firing_rate_using_other_fuels",
        "fly_ash_reinjection",
        "hrsg",
        "new_source_review",
        "turndown_ratio",
        "waste_heat_input_mmbtu_per_hour",
    ]

    for column in zero_columns_to_fix:
        b_df[column] = b_df[column].replace(to_replace=0, value=np.nan)

    # Fix unlikely year values for compliance year columns
    year_cols_to_fix = [
        "compliance_year_nox",
        "compliance_year_so2",
        "compliance_year_mercury",
        "compliance_year_particulate",
    ]

    for col in year_cols_to_fix:
        b_df.loc[b_df[col] < 1900, col] = pd.NA

    # Convert boolean columns from Y/N to True/False.
    boolean_columns_to_fix = [
        "hrsg",
        "fly_ash_reinjection",
        "new_source_review",
        "mercury_emission_control",
        "ACI",
        "BH",
        "DS",
        "EP",
        "FGD",
        "LIJ",
        "WS",
        "BS",
        "BP",
        "BR",
        "EC",
        "EH",
        "EK",
        "EW",
        "OT",
    ]

    for column in boolean_columns_to_fix:
        b_df[column] = (
            b_df[column]
            .fillna("NaN")
            .replace(
                to_replace=["Y", "N", "NaN", "0"], value=[True, False, pd.NA, pd.NA]
            )
        )

    # 2009-2012 data uses boolean columns for mercury equipment that
    # later are converted to strategy codes. Here we convert them manually.

    mercury_boolean_cols = [
        "ACI",
        "BH",
        "DS",
        "EP",
        "FGD",
        "LIJ",
        "WS",
        "BS",
        "BP",
        "BR",
        "EC",
        "EH",
        "EK",
        "EW",
        "OT",
    ]

    # Get list of True columns
    b_df["agg"] = b_df[mercury_boolean_cols].apply(
        lambda row: row[row.__eq__(True)].index.to_list(), axis=1
    )

    # Split list into columns
    b_df = pd.concat(
        [
            b_df.drop(columns="agg"),
            pd.DataFrame(b_df["agg"].tolist(), index=b_df.index)
            .add_prefix("mercury_strategy_")
            .fillna(pd.NA),
        ],
        axis=1,
    )

    # Add three new mercury_strategy columns
    (
        b_df["mercury_control_existing_strategy_4"],
        b_df["mercury_control_existing_strategy_5"],
        b_df["mercury_control_existing_strategy_6"],
    ) = [pd.NA, pd.NA, pd.NA]

    for col in (col for col in b_df.columns if "mercury_strategy_" in col):
        i = str(int(col[-1]) + 1)  # Get digit from column
        # Fill strategy codes using columns
        b_df[f"mercury_control_existing_strategy_{i}"] = b_df[
            f"mercury_control_existing_strategy_{i}"
        ].fillna(b_df[col])

    # Convert month and year columns to date.
    b_df = b_df.pipe(pudl.helpers.month_year_to_date).pipe(pudl.helpers.convert_to_date)

    # Add boiler manufacturer name to column
    b_df["boiler_manufacturer"] = b_df.boiler_manufacturer_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_environmental_equipment_manufacturers"][
                "df"
            ],
            from_col="code",
            to_col="description",
            null_value=pd.NA,
        )
    )

    b_df["nox_control_manufacturer"] = b_df.nox_control_manufacturer_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_environmental_equipment_manufacturers"][
                "df"
            ],
            from_col="code",
            to_col="description",
            null_value=pd.NA,
        )
    )

    # Prior to 2012, efficiency was reported as a percentage, rather than
    # as a proportion, so we need to divide those values by 100.
    b_df.loc[b_df.report_date.dt.year < 2012, "efficiency_100pct_load"] = (
        b_df.loc[b_df.report_date.dt.year < 2012, "efficiency_100pct_load"] / 100
    )
    b_df.loc[b_df.report_date.dt.year < 2012, "efficiency_50pct_load"] = (
        b_df.loc[b_df.report_date.dt.year < 2012, "efficiency_50pct_load"] / 100
    )

    return b_df


@asset
def _core_eia860__emissions_control_equipment(
    raw_eia860__emissions_control_equipment: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the emissions control equipment table."""
    # Replace empty strings, whitespace, and '.' fields with real NA values
    emce_df = pudl.helpers.fix_eia_na(raw_eia860__emissions_control_equipment)

    # Spot fix bad months
    emce_df["emission_control_operating_month"] = emce_df[
        "emission_control_operating_month"
    ].replace({"88": "8"})
    # Fill in values with a year not a month based on later years that do have a month
    # I thought about doing some sort of backfill here, but decided not to because
    # emission_control_id_pudl is not guaranteed to be consistent over time
    bad_month_1 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 10346)
        & (emce_df["nox_control_id_eia"] == "BOIL01")
    )
    bad_month_2 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 10202)
        & (emce_df["particulate_control_id_eia"].isin(["5PMDC", "5PPPT"]))
    )
    bad_month_3 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 3131)
        & (emce_df["emission_control_operating_year"] == "2005")
    )
    bad_month_4 = (emce_df["report_year"] == 2013) & (emce_df["plant_id_eia"] == 10405)
    bad_month_5 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 50661)
        & (emce_df["nox_control_id_eia"].isin(["ASNCR", "BSNCR"]))
    )
    bad_month_6 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 4054)
        & (emce_df["emission_control_operating_year"] == "2011")
    )
    bad_month_7 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 50544)
        & (emce_df["emission_control_operating_year"] == "1990")
    )
    bad_month_8 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 50189)
        & (emce_df["particulate_control_id_eia"].isin(["EGS1", "EGS2", "ESP1CB"]))
    )

    # Add this conditional in case we're doing the fast ETL with one year of data
    # (in which case the assertions will fail)
    if 2013 in emce_df.report_year.unique():
        assert len(emce_df[bad_month_1]) == 1
        emce_df.loc[bad_month_1, "emission_control_operating_month"] = 6
        assert len(emce_df[bad_month_2]) == 4
        emce_df.loc[bad_month_2, "emission_control_operating_month"] = 6
        assert len(emce_df[bad_month_3]) == 4
        emce_df.loc[bad_month_3, "emission_control_operating_month"] = 1
        assert len(emce_df[bad_month_4]) == 1
        emce_df.loc[bad_month_4, "emission_control_operating_month"] = 12
        assert len(emce_df[bad_month_5]) == 2
        emce_df.loc[bad_month_5, "emission_control_operating_month"] = 6
        assert len(emce_df[bad_month_6]) == 2
        emce_df.loc[bad_month_6, "emission_control_operating_month"] = 10
        assert len(emce_df[bad_month_7]) == 1
        emce_df.loc[bad_month_7, "emission_control_operating_month"] = 6
        assert len(emce_df[bad_month_8]) == 9
        emce_df.loc[bad_month_8, "emission_control_operating_month"] = 12

    # Convert month-year columns to a single date column
    emce_df = pudl.helpers.convert_to_date(
        df=emce_df,
        date_col="emission_control_operating_date",
        year_col="emission_control_operating_year",
        month_col="emission_control_operating_month",
    ).pipe(
        pudl.helpers.convert_to_date,
        date_col="emission_control_retirement_date",
        year_col="emission_control_retirement_year",
        month_col="emission_control_retirement_month",
    )

    # Convert acid gas control column to boolean
    emce_df = pudl.helpers.convert_col_to_bool(
        df=emce_df, col_name="acid_gas_control", true_values=["Y"], false_values=[]
    )
    # Add a emission_control_id_pudl as a primary key. This is not unique over years.
    # We could maybe try and do this, but not doing it now.
    emce_df["emission_control_id_pudl"] = (
        emce_df.groupby(["report_year", "plant_id_eia"]).cumcount() + 1
    )
    # Fix outlier value in emission_control_equipment_cost. We know this is an
    # outlier because it is the highest value reported in the dataset and
    # the other years from the same plant show that it likely contains three
    # extra zeros. We use the primary keys to spot fix the value.
    outlier_primary_keys = (
        (emce_df["report_year"] == 2017)
        & (emce_df["plant_id_eia"] == 57794)
        & (emce_df["emission_control_equipment_cost"] == 3200000)
    )

    if len(emce_df[outlier_primary_keys]) > 2:
        raise AssertionError("Only expecting two spot fixed values here")

    emce_df.loc[
        outlier_primary_keys,
        "emission_control_equipment_cost",
    ] = 3200

    # Convert thousands of dollars to dollars:
    emce_df.loc[:, "emission_control_equipment_cost"] = (
        1000.0 * emce_df["emission_control_equipment_cost"]
    )

    return emce_df


@asset
def _core_eia860__boiler_emissions_control_equipment_assn(
    raw_eia860__boiler_so2: pd.DataFrame,
    raw_eia860__boiler_mercury: pd.DataFrame,
    raw_eia860__boiler_nox: pd.DataFrame,
    raw_eia860__boiler_particulate: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the emissions control <> boiler ID link tables.

    Args:
        raw_eia860__boiler_so2: Raw EIA 860 boiler to SO2 emission control equipment
            association table.
        raw_eia860__boiler_mercury: Raw EIA 860 boiler to mercury emission control
            equipment association table.
        raw_eia860__boiler_nox: Raw EIA 860 boiler to nox emission control equipment
            association table.
        raw_eia860__boiler_particulate: Raw EIA 860 boiler to particulate emission
            control equipment association table.
        raw_eia860__boiler_cooling: Raw EIA 860 boiler to cooling equipment association
            table.
        raw_eia860__boiler_stack_flue: Raw EIA 860 boiler to stack flue equipment
            association table.

    Returns:
        A combination of all the emission control equipment association tables.
    """
    raw_tables = [
        raw_eia860__boiler_so2,
        raw_eia860__boiler_mercury,
        raw_eia860__boiler_nox,
        raw_eia860__boiler_particulate,
    ]

    dfs = []
    for table in raw_tables:
        # There are some utilities that report the same emissions control equipment.
        # Drop duplicate rows where the only difference is utility.
        table = table.drop_duplicates(
            subset=[
                x
                for x in table.columns
                if x not in ["utility_id_eia", "utility_name_eia"]
            ]
        )
        # Melt the table so that the control id column is in one row and the type of
        # control id (the pollutant type) is in another column. This makes it easier
        # combine all of the tables for different pollutants.
        value_col = [col for col in table if "control_id_eia" in col]
        id_cols = [col for col in table if "control_id_eia" not in col]
        # Each table should only have one column with control_id_eia in the name
        assert len(value_col) == 1
        table = pd.melt(
            table,
            value_vars=value_col,
            id_vars=id_cols,
            var_name="emission_control_id_type",
            value_name="emission_control_id_eia",
        )
        dfs.append(table)
    bece_df = pd.concat(dfs)

    # The report_year column must be report_date in order for the harvcesting process
    # to work on this table. It later gets converted back to report_year.
    bece_df = pudl.helpers.convert_to_date(
        df=bece_df, year_col="report_year", date_col="report_date"
    ).assign(
        # Remove the string _control_id_eia from the control_id_type column so it just
        # shows the prefix (i.e., the name of the pollutant: so2, nox, etc.)
        emission_control_id_type=lambda x: x.emission_control_id_type.str.replace(
            "_control_id_eia", ""
        )
    )
    # There are some records that don't have an emission control id that are not
    # helpful so we drop them.
    bece_df = bece_df.dropna(subset="emission_control_id_eia")

    return bece_df


@asset
def _core_eia860__boiler_cooling(
    raw_eia860__boiler_cooling: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the EIA 860 boiler to cooler ID table.

    Args:
        raw_eia860__boiler_cooling: Raw EIA 860 boiler to cooler ID association table.

    Returns:
        A cleaned and normalized version of the EIA boiler to cooler ID table.
    """
    # Replace empty strings, whitespace, and '.' fields with real NA values
    bc_assn = pudl.helpers.fix_eia_na(raw_eia860__boiler_cooling)
    # Replace the report year col with a report date col for the harvesting process
    bc_assn = pudl.helpers.convert_to_date(
        df=bc_assn, year_col="report_year", date_col="report_date"
    )
    # Drop rows with no cooling ID and just in case, drop duplicate
    bc_assn = bc_assn.dropna(subset="cooling_id_eia").drop_duplicates()

    return bc_assn


@asset
def _core_eia860__boiler_stack_flue(
    raw_eia860__boiler_stack_flue: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the EIA 860 boiler to stack flue ID table.

    Args:
        raw_eia860__boiler_stack_flue: Raw EIA 860 boiler to stack flue ID association
            table.

    Returns:
        A cleaned and normalized version of the EIA boiler to stack flue ID table.
    """
    # Replace empty strings, whitespace, and '.' fields with real NA values
    bsf_assn = pudl.helpers.fix_eia_na(raw_eia860__boiler_stack_flue)
    # Replace the report year col with a report date col for the harvesting process
    bsf_assn = pudl.helpers.convert_to_date(
        df=bsf_assn, year_col="report_year", date_col="report_date"
    )
    # Drop duplicates
    bsf_assn = bsf_assn.drop_duplicates()
    # Create a primary key column for stack flue IDs.
    # Prior to 2013, EIA reported a stack_id_eia and a flue_id_eia. Sometimes there
    # was a m:m relationship between these values. 2013 and later, EIA published a
    # stack_flue_id_eia column the represented either the stack or flue id.
    # In order to create a primary key with no NA values, we create a new
    # stack_flue_id_pudl column. We do this instead of backfilling stack_flue_id_pudl
    # because stack_flue_id_pudl would not be a unique identifier in older years due to
    # the m:m relationship between stack_id and flue_id. We also don't forward fill
    # the individual stack or flue id columns because we can't be sure whether a
    # stack_flue_id_eia value is the stack or flue id. And we don't want to
    # missrepresent complicated relationships between stacks and flues. Also there's
    # several instances where flue_id_eia is NA (hence the last fillna(x.stack_id_eia))
    bsf_assn = bsf_assn.assign(
        stack_flue_id_pudl=lambda x: (
            x.stack_flue_id_eia.fillna(
                x.stack_id_eia.astype("string") + "_" + x.flue_id_eia.astype("string")
            ).fillna(x.stack_id_eia)
        )
    )

    return bsf_assn


@asset(io_manager_key="pudl_io_manager")
def _core_eia860__cooling_equipment(
    raw_eia860__cooling_equipment: pd.DataFrame,
    _core_censuspep__yearly_geocodes: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 860 cooling equipment table.

    - spot clean year values before converting to dates
    - standardize water rate units to gallons per minute (2009-2013 used cubic
      feet per second)
    - convert kilodollars to normal dollars

    Note that the "power_requirement_mw" field is erroneously reported as
    "_kwh" in the raw data for 2009, 2010, and 2011, even though the values are
    in MW. This is corroborated by the values for these plants matching up with
    the stated MW values in later years. Additionally, the PDF form for those
    years indicates that the value should be in MW, and KWh isn't even a power
    measurement.

    In 2009, we have two incorrectly entered ``cooling_type`` values of ``HR``,
    for utility ID 14328, plant IDs 56532/56476, and cooling ID ACC1. This
    corresponds to the Colusa and Gateway generating stations run by PG&E. In
    all later years, these cooling facilities are marked as ``DC``, or "dry
    cooling"; however, ``HR`` looks like the codes for hybrid systems (the
    others are ``HRC``, ``HRF``, ``HRI``). As such we drop the ``HR`` code
    completely in ``pudl.metadata.codes``.
    """
    ce_df = raw_eia860__cooling_equipment

    # Generic cleaning
    ce_df = ce_df.pipe(pudl.helpers.fix_eia_na).pipe(
        pudl.helpers.add_fips_ids, _core_censuspep__yearly_geocodes
    )

    # Spot cleaning and date conversion
    ce_df.loc[
        ce_df["chlorine_equipment_operating_year"] == 971,
        "chlorine_equipment_operating_year",
    ] = 1971
    ce_df.loc[ce_df["pond_operating_year"] == 0, "pond_operating_year"] = np.nan
    ce_df.loc[ce_df["tower_operating_year"] == 974, "tower_operating_year"] = 1974
    ce_df = ce_df.pipe(pudl.helpers.month_year_to_date).pipe(
        pudl.helpers.convert_to_date
    )

    # There's one row which has an NA cooling_id_eia, which we mark as "PLANT"
    # to allow it to be in a DB primary key.
    ce_df.loc[
        (ce_df["plant_id_eia"] == 6285)
        & (ce_df["utility_id_eia"] == 7353)
        & (ce_df["report_date"] == "2016-01-01"),
        "cooling_id_eia",
    ] = "PLANT"

    # Convert cubic feet/second to gallons/minute
    cfs_in_gpm = 448.8311688
    ce_df = ce_df.fillna(
        {
            "tower_water_rate_100pct_gallons_per_minute": ce_df.tower_water_rate_100pct_cubic_feet_per_second
            * cfs_in_gpm,
            "intake_rate_100pct_gallons_per_minute": ce_df.intake_rate_100pct_cubic_feet_per_second
            * cfs_in_gpm,
        }
    ).drop(
        columns=[
            "tower_water_rate_100pct_cubic_feet_per_second",
            "intake_rate_100pct_cubic_feet_per_second",
        ]
    )

    # Convert thousands of dollars to dollars and remove suffix from column name
    ce_df.loc[:, ce_df.columns.str.endswith("_thousand_dollars")] *= 1000
    ce_df.columns = ce_df.columns.str.replace("_thousand_dollars", "")

    # Encoding is required here because this table is not yet getting harvested.
    return apply_pudl_dtypes(ce_df, group="eia", strict=True).pipe(PUDL_PACKAGE.encode)


@asset_check(asset=_core_eia860__cooling_equipment, blocking=True)
def cooling_equipment_null_cols(cooling_equipment):
    """The only completely null cols we expect are tower type 3 and 4.

    In fast-ETL, i.e. recent years, we also expect a few other columns to be
    null since they only show up in older data.
    """
    expected_null_cols = {"tower_type_3", "tower_type_4"}
    if cooling_equipment.report_date.min() > pd.Timestamp("2010-01-01T00:00:00"):
        expected_null_cols.update(
            {"plant_summer_capacity_mw", "water_source", "county", "cooling_type_4"}
        )
    pudl.validate.no_null_cols(
        cooling_equipment,
        cols=set(cooling_equipment.columns) - expected_null_cols,
    )
    col_is_null = cooling_equipment.isna().all()
    if not all(col_is_null[col] for col in expected_null_cols):
        return AssetCheckResult(
            passed=False, metadata={"col_is_null": col_is_null.to_json()}
        )
    return AssetCheckResult(passed=True)


@asset_check(asset=_core_eia860__cooling_equipment, blocking=True)
def cooling_equipment_continuity(cooling_equipment):
    """Check to see if columns vary as slowly as expected.

    2024-03-04: pond cost, tower cost, and tower cost all have one-off
    discontinuities that are worth investigating, but we're punting on that
    investigation since we're out of time.
    """
    return pudl.validate.group_mean_continuity_check(
        df=cooling_equipment,
        thresholds={
            "intake_rate_100pct_gallons_per_minute": 0.1,
            "outlet_distance_shore_feet": 0.1,
            "outlet_distance_surface_feet": 0.1,
            "pond_cost": 0.1,
            "pond_surface_area_acres": 0.1,
            "pond_volume_acre_feet": 0.2,
            "power_requirement_mw": 0.1,
            "plant_summer_capacity_mw": 0.1,
            "tower_cost": 0.1,
            "tower_water_rate_100pct_gallons_per_minute": 0.1,
        },
        groupby_col="report_date",
        n_outliers_allowed=1,
    )


@asset(io_manager_key="pudl_io_manager")
def _core_eia860__fgd_equipment(
    raw_eia860__fgd_equipment: pd.DataFrame,
    _core_censuspep__yearly_geocodes: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 860 FGD equipment table.

    Transformations include:
    - convert string booleans to boolean dtypes, and mixed strings/numbers to numbers
    - convert kilodollars to normal dollars
    - handle mixed reporting of percentages
    - spot fix a duplicated SO2 control ID
    - change an old water code to preserve detail of reporting over time
    - add manufacturer name based on the code reported

    """
    fgd_df = raw_eia860__fgd_equipment

    # Generic cleaning
    fgd_df = fgd_df.pipe(pudl.helpers.fix_eia_na).pipe(
        pudl.helpers.add_fips_ids, _core_censuspep__yearly_geocodes
    )

    # Spot cleaning and date conversion
    fgd_df = fgd_df.pipe(pudl.helpers.month_year_to_date).pipe(
        pudl.helpers.convert_to_date
    )

    # Handle mixed boolean types in control flag column
    for col in [
        "byproduct_recovery",
        "flue_gas_bypass_fgd",
        "sludge_pond",
        "sludge_pond_lined",
    ]:
        fgd_df = pudl.helpers.convert_col_to_bool(
            df=fgd_df,
            col_name=col,
            true_values=["Y", "y"],
            false_values=["N", "n"],
        )

    # Convert thousands of dollars to dollars and remove suffix from column name
    fgd_df.loc[:, fgd_df.columns.str.endswith("_thousand_dollars")] *= 1000
    fgd_df.columns = fgd_df.columns.str.replace("_thousand_dollars", "")

    # Deal with mixed 0-1 and 0-100 percentage reporting
    pct_cols = [
        "flue_gas_entering_fgd_pct_of_total",
        "so2_removal_efficiency_design",
        "specifications_of_coal_ash",
        "specifications_of_coal_sulfur",
    ]
    fgd_df = pudl.helpers.standardize_percentages_ratio(
        frac_df=fgd_df, mixed_cols=pct_cols, years_to_standardize=[2009, 2010, 2011]
    )

    # Fix duplicated SO2 control ID for plant 6016 in 2011
    # Every other year 2009-2012 "01" refers to 2009 operating FGD equipment, and "1"
    # refers to the 1976 operating FGD equipment. In 2011 the plant reports two "1"
    # plants, so we change the SO2 control ID to "01" for the 2009 unit to preserve
    # uniqueness.
    fgd_df.loc[
        (fgd_df.plant_id_eia == 6016)
        & (fgd_df.report_date == "2011-01-01")
        & (fgd_df.fgd_operating_date == "2009-03-01"),
        "so2_control_id_eia",
    ] = "01"

    # Add a manufacturer name from the code.
    fgd_df["fgd_manufacturer"] = fgd_df.fgd_manufacturer_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_environmental_equipment_manufacturers"][
                "df"
            ],
            from_col="code",
            to_col="description",
            null_value=pd.NA,
        )
    )

    # Check for uniqueness of index and handle special case duplicates.
    pkey = ["plant_id_eia", "so2_control_id_eia", "report_date"]
    fgd_df = pudl.helpers.dedupe_and_drop_nas(fgd_df, primary_key_cols=pkey)

    # After 2012, treated wastewater and water get lumped into what was previously
    # reported as just a water code. Let's rename this code to "W" to preserve detail in
    # earlier years.
    for sorbent_col in (col for col in fgd_df.columns if "sorbent_type" in col):
        fgd_df.loc[fgd_df[sorbent_col] == "WT", sorbent_col] = "W"

    # Handle some non-numeric values in the pond landfill requirements column
    fgd_df["pond_landfill_requirements_acre_foot_per_year"] = pd.to_numeric(
        fgd_df.pond_landfill_requirements_acre_foot_per_year, errors="coerce"
    )

    # Encoding required because this isn't fed into harvesting yet.
    return PUDL_PACKAGE.encode(fgd_df).pipe(apply_pudl_dtypes, strict=False)


@asset_check(asset=_core_eia860__fgd_equipment, blocking=True)
def fgd_equipment_null_check(fgd):
    """Check that columns other than expected columns aren't null."""
    fast_run_null_cols = {
        "county",
        "county_id_fips",
        "fgd_operational_status_code",
        "fgd_operating_date",
        "fgd_manufacturer",
        "fgd_manufacturer_code",
        "plant_summer_capacity_mw",
        "water_source",
        "so2_equipment_type_4",
    }
    if fgd.report_date.min() >= pd.Timestamp("2011-01-01T00:00:00"):
        expected_cols = set(fgd.columns) - fast_run_null_cols
    else:
        expected_cols = set(fgd.columns)
    pudl.validate.no_null_cols(fgd, cols=expected_cols)
    return AssetCheckResult(passed=True)


@asset_check(asset=_core_eia860__fgd_equipment, blocking=True)
def fgd_equipment_continuity(fgd):
    """Check to see if columns vary as slowly as expected."""
    return pudl.validate.group_mean_continuity_check(
        df=fgd,
        thresholds={
            "flue_gas_exit_rate_cubic_feet_per_minute": 0.1,
            "flue_gas_exit_temperature_fahrenheit": 0.1,
            "pond_landfill_requirements_acre_foot_per_year": 0.1,
            "so2_removal_efficiency_design": 0.1,
            "so2_emission_rate_lbs_per_hour": 0.1,
            "specifications_of_coal_ash": 0.1,
            "specifications_of_coal_sulfur": 7,  # TODO (2024-03-14): Investigate
            "plant_summer_capacity_mw": 0.1,
            "total_fgd_equipment_cost": 0.2,
        },
        groupby_col="report_date",
        n_outliers_allowed=1,
    )
