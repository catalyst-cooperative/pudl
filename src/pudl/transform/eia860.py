"""Module to perform data cleaning functions on EIA860 data tables."""

import numpy as np
import pandas as pd
from dagster import asset

import pudl
from pudl.metadata.classes import DataSource
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.transform.eia861 import clean_nerc

logger = pudl.logging_helpers.get_logger(__name__)


@asset
def clean_ownership_eia860(raw_ownership_eia860: pd.DataFrame) -> pd.DataFrame:
    """Pull and transform the ownership table.

    Transformations include:

    * Replace . values with NA.
    * Convert pre-2012 ownership percentages to proportions to match post-2012
      reporting.

    Args:
        raw_ownership_eia860: The raw ``ownership_eia860`` dataframe.

    Returns:
        Cleaned ``ownership_eia860`` dataframe ready for harvesting.
    """
    # Preiminary clean and get rid of unecessary 'year' column
    own_df = (
        raw_ownership_eia860.copy()
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
            f"Duplicate ownership slices found in ownership_eia860: {remaining_dupes}"
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
                    "2018-01-01",
                    "2019-01-01",
                    "2020-01-01",
                    "2021-01-01",
                ]
            )
        )
        & (own_df.plant_id_eia == 62844)
        & (own_df.owner_utility_id_eia == 62745)
        & (own_df.generator_id == "nan")
    )
    own_df = own_df[~mask]

    if not (nulls := own_df[own_df.generator_id == ""]).empty:
        logger.warning(f"Found records with null IDs in ownership_eia860: {nulls}")
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
        ).utility_id_eia.transform(pd.Series.nunique)
    ) > 1
    own_df.loc[duplicate_operators, "utility_id_eia"] = pd.NA

    # The above fix won't catch owner_utility_id_eia values in the
    # utility_id_eia (operator) column when there's only a single
    # owner-operator. But also, when there's a single owner-operator they souldn't
    # even be reporting in this table. So we can also drop those utility_id_eia
    # values without losing any valuable information here. The utility_id_eia
    # column here is only useful for entity harvesting & resolution purposes
    # since the (report_date, plant_id_eia) tuple fully defines the operator id.
    # See https://github.com/catalyst-cooperative/pudl/issues/1116
    single_owner_operator = (own_df.utility_id_eia == own_df.owner_utility_id_eia) & (
        own_df.fraction_owned == 1.0
    )
    own_df.loc[single_owner_operator, "utility_id_eia"] = pd.NA
    own_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("ownership_eia860")
        .encode(own_df)
    )
    # CN is an invalid political subdivision code used by a few respondents to indicate
    # that the owner is in Canada. At least we can recover the country:
    state_to_country = {
        x.subdivision_code: x.country_code for x in POLITICAL_SUBDIVISIONS.itertuples()
    } | {"CN": "CAN"}
    own_df["owner_country"] = own_df["owner_state"].map(state_to_country)
    own_df.loc[own_df.owner_state == "CN", "owner_state"] = pd.NA

    # Spot fix NA generator_id. Might want to change this once we have the official 2022
    # data not just early release.
    constraints = (own_df["plant_id_eia"] == 62844) & (
        own_df["report_date"].dt.year == 2022
    )
    if len(own_df[constraints]) > 1:
        raise AssertionError("Too many records getting spot fixed.")

    own_df.loc[constraints, "generator_id"] = "1"

    return own_df


@asset
def clean_generators_eia860(
    raw_generator_proposed_eia860: pd.DataFrame,
    raw_generator_existing_eia860: pd.DataFrame,
    raw_generator_retired_eia860: pd.DataFrame,
    raw_generator_eia860: pd.DataFrame,
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
        raw_generator_proposed_eia860: The raw ``raw_generator_proposed_eia860`` dataframe.
        raw_generator_existing_eia860: The raw ``raw_generator_existing_eia860`` dataframe.
        raw_generator_retired_eia860: The raw ``raw_generator_retired_eia860`` dataframe.
        raw_generator_eia860: The raw ``raw_generator_eia860`` dataframe.

    Returns:
        Cleaned ``generators_eia860`` dataframe ready for harvesting.
    """
    # Groupby objects were creating chained assignment warning that is N/A
    pd.options.mode.chained_assignment = None

    # There are three sets of generator data reported in the EIA860 table,
    # planned, existing, and retired generators. We're going to concatenate
    # them all together into a single big table, with a column that indicates
    # which one of these tables the data came from, since they all have almost
    # exactly the same structure
    gp_df = raw_generator_proposed_eia860
    ge_df = raw_generator_existing_eia860
    gr_df = raw_generator_retired_eia860
    g_df = raw_generator_eia860
    # the retired tab of eia860 does not have a operational_status_code column.
    # we still want these gens to have a code (and subsequently a
    # operational_status). We could do this by fillna w/ the retirement_date, but
    # this way seems more straightforward.
    gr_df["operational_status_code"] = gr_df["operational_status_code"].fillna("RE")

    gens_df = (
        pd.concat([ge_df, gp_df, gr_df, g_df], sort=True)
        .dropna(subset=["generator_id", "plant_id_eia"])
        .pipe(pudl.helpers.fix_eia_na)
    )

    # A subset of the columns have zero values, where NA is appropriate:
    columns_to_fix = [
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

    for column in columns_to_fix:
        gens_df[column] = gens_df[column].replace(to_replace=[" ", 0], value=np.nan)

    # A subset of the columns have "X" values, where other columns_to_fix
    # have "N" values. Replacing these values with "N" will make for uniform
    # values that can be converted to Boolean True and False pairs.
    gens_df.duct_burners = gens_df.duct_burners.replace(to_replace="X", value="N")
    gens_df.bypass_heat_recovery = gens_df.bypass_heat_recovery.replace(
        to_replace="X", value="N"
    )
    gens_df.syncronized_transmission_grid = gens_df.bypass_heat_recovery.replace(
        to_replace="X", value="N"
    )

    # A subset of the columns have "U" values, presumably for "Unknown," which
    # must be set to None in order to convert the columns to datatype Boolean.

    gens_df.multiple_fuels = gens_df.multiple_fuels.replace(to_replace="U", value=None)
    gens_df.switch_oil_gas = gens_df.switch_oil_gas.replace(to_replace="U", value=None)

    boolean_columns_to_fix = [
        "duct_burners",
        "multiple_fuels",
        "deliver_power_transgrid",
        "syncronized_transmission_grid",
        "solid_fuel_gasification",
        "pulverized_coal_tech",
        "fluidized_bed_tech",
        "subcritical_tech",
        "supercritical_tech",
        "ultrasupercritical_tech",
        "carbon_capture",
        "stoker_tech",
        "other_combustion_tech",
        "cofire_fuels",
        "switch_oil_gas",
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

    for column in boolean_columns_to_fix:
        gens_df[column] = (
            gens_df[column]
            .fillna("NaN")
            .replace(to_replace=["Y", "N", "NaN"], value=[True, False, pd.NA])
        )

    gens_df = (
        gens_df.pipe(pudl.helpers.month_year_to_date)
        .pipe(
            pudl.helpers.simplify_strings,
            columns=["rto_iso_lmp_node_id", "rto_iso_location_wholesale_reporting_id"],
        )
        .pipe(pudl.helpers.convert_to_date)
    )

    gens_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("generators_eia860")
        .encode(gens_df)
    )

    gens_df["fuel_type_code_pudl"] = gens_df.energy_source_code_1.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["energy_sources_eia"]["df"],
            from_col="code",
            to_col="fuel_type_code_pudl",
            null_value=pd.NA,
        )
    )

    gens_df["operational_status"] = gens_df.operational_status_code.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["operational_status_eia"]["df"],
            from_col="code",
            to_col="operational_status",
            null_value=pd.NA,
        )
    )

    return gens_df


@asset
def clean_plants_eia860(raw_plant_eia860: pd.DataFrame) -> pd.DataFrame:
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
        raw_plant_eia860: The raw ``raw_plant_eia860`` dataframe.

    Returns:
        Cleaned ``plants_eia860`` dataframe ready for harvesting.
    """
    # Populating the 'plants_eia860' table
    p_df = (
        raw_plant_eia860.pipe(pudl.helpers.fix_eia_na)
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
        "net_metering",
    ]

    for column in boolean_columns_to_fix:
        p_df[column] = (
            p_df[column]
            .fillna("NaN")
            .replace(to_replace=["Y", "N", "NaN"], value=[True, False, pd.NA])
        )

    p_df = pudl.helpers.convert_to_date(p_df)

    p_df = clean_nerc(p_df, idx_cols=["plant_id_eia", "report_date", "nerc_region"])

    p_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("plants_eia860")
        .encode(p_df)
    )

    return p_df


@asset
def clean_boiler_generator_assn_eia860(
    raw_boiler_generator_assn_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the boilder generator association table.

    Transformations include:

    * Drop non-data rows with EIA notes.
    * Drop duplicate rows.

    Args:
        raw_boiler_generator_assn_eia860 (df): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA860 form, as reported in the Excel
            spreadsheets they distribute.

    Returns:
        Cleaned ``boiler_generator_assn_eia860`` dataframe ready for harvesting.
    """
    # Populating the 'generators_eia860' table
    b_g_df = raw_boiler_generator_assn_eia860

    b_g_df = pudl.helpers.convert_to_date(b_g_df)
    b_g_df = pudl.helpers.convert_cols_dtypes(df=b_g_df, data_source="eia")
    b_g_df = b_g_df.drop_duplicates()

    b_g_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("boiler_generator_assn_eia860")
        .encode(b_g_df)
    )

    return b_g_df


@asset
def clean_utilities_eia860(raw_utility_eia860: pd.DataFrame) -> pd.DataFrame:
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
        raw_utility_eia860: The raw ``raw_utility_eia860`` dataframe.

    Returns:
        Cleaned ``utilities_eia860`` dataframe ready for harvesting.
    """
    # Populating the 'utilities_eia860' table
    u_df = raw_utility_eia860

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
        """Make and validate full phone number seperated by dashes."""
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
        return p_num.replace(
            regex=r"^(?!.*\d{3}-\d{3}-\d{4}).*$", value=pd.NA
        )  # noqa: FS003

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
def clean_boilers_eia860(
    raw_emission_control_strategies_eia860, raw_boiler_info_eia860
):
    """Pull and transform the boilers table.

    Transformations include:

    * Replace . values with NA.
    * Convert Y/N/NA values to boolean True/False.
    * Combine month and year columns into date columns.
    * Add boiler manufacturer name column.
    * Convert pre-2012 efficiency percentages to proportions to match post-2012
      reporting.

    Args:
        raw_emission_control_strategies_eia860 (pandas.DataFrame):
            DataFrame extracted from EIA forms earlier in the ETL process.
        raw_boiler_info_eia860 (pandas.DataFrame):
            DataFrame extracted from EIA forms earlier in the ETL process.

    Returns:
        pandas.DataFrame: the transformed boilers table
    """
    # Populating the 'boilers_eia860' table
    b_df = raw_boiler_info_eia860
    ecs = raw_emission_control_strategies_eia860

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
            CODE_METADATA["environmental_equipment_manufacturers_eia"]["df"],
            from_col="code",
            to_col="description",
            null_value=pd.NA,
        )
    )

    b_df["nox_control_manufacturer"] = b_df.nox_control_manufacturer_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["environmental_equipment_manufacturers_eia"]["df"],
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

    b_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("boilers_eia860")
        .encode(b_df)
    )

    return b_df


@asset
def clean_emissions_control_equipment_eia860(
    raw_emissions_control_equipment_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the emissions control equipment table."""
    # Replace empty strings, whitespace, and '.' fields with real NA values
    emce_df = pudl.helpers.fix_eia_na(raw_emissions_control_equipment_eia860)

    # Spot fix bad months
    emce_df["operating_month"] = emce_df["operating_month"].replace({"88": "8"})
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
        & (emce_df["operating_year"] == "2005")
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
        & (emce_df["operating_year"] == "2011")
    )
    bad_month_7 = (
        (emce_df["report_year"] == 2013)
        & (emce_df["plant_id_eia"] == 50544)
        & (emce_df["operating_year"] == "1990")
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
        emce_df.loc[bad_month_1, "operating_month"] = 6
        assert len(emce_df[bad_month_2]) == 4
        emce_df.loc[bad_month_2, "operating_month"] = 6
        assert len(emce_df[bad_month_3]) == 4
        emce_df.loc[bad_month_3, "operating_month"] = 1
        assert len(emce_df[bad_month_4]) == 1
        emce_df.loc[bad_month_4, "operating_month"] = 12
        assert len(emce_df[bad_month_5]) == 2
        emce_df.loc[bad_month_5, "operating_month"] = 6
        assert len(emce_df[bad_month_6]) == 2
        emce_df.loc[bad_month_6, "operating_month"] = 10
        assert len(emce_df[bad_month_7]) == 1
        emce_df.loc[bad_month_7, "operating_month"] = 6
        assert len(emce_df[bad_month_8]) == 9
        emce_df.loc[bad_month_8, "operating_month"] = 12

    # Convert month-year columns to a single date column
    emce_df = pudl.helpers.convert_to_date(
        df=emce_df,
        date_col="emission_control_operating_date",
        year_col="operating_year",
        month_col="operating_month",
    ).pipe(
        pudl.helpers.convert_to_date,
        date_col="emission_control_retirement_date",
        year_col="retirement_year",
        month_col="retirement_month",
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

    emce_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("emissions_control_equipment_eia860")
        .encode(emce_df)
    )

    return emce_df


@asset
def clean_boiler_emissions_control_equipment_assn_eia860(
    raw_boiler_so2_eia860: pd.DataFrame,
    raw_boiler_mercury_eia860: pd.DataFrame,
    raw_boiler_nox_eia860: pd.DataFrame,
    raw_boiler_particulate_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the emissions control <> boiler ID link tables.

    Args:
        raw_boiler_so2_eia860: Raw EIA 860 boiler to SO2 emission control equipment
            association table.
        raw_boiler_mercury_eia860: Raw EIA 860 boiler to mercury emission control
            equipment association table.
        raw_boiler_nox_eia860: Raw EIA 860 boiler to nox emission control equipment
            association table.
        raw_boiler_particulate_eia860: Raw EIA 860 boiler to particulate emission
            control equipment association table.
        raw_boiler_cooling_eia860: Raw EIA 860 boiler to cooling equipment association
            table.
        raw_boiler_stack_flue_eia860: Raw EIA 860 boiler to stack flue equipment
            association table.

    Returns:
        pd.DataFrame: A combination of all the emission control equipment association
            tables.
    """
    raw_tables = [
        raw_boiler_so2_eia860,
        raw_boiler_mercury_eia860,
        raw_boiler_nox_eia860,
        raw_boiler_particulate_eia860,
    ]

    bece_df = pd.DataFrame({})

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
        bece_df = bece_df.append(table)

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
def clean_boiler_cooling_assn_eia860(
    raw_boiler_cooling_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the EIA 860 boiler to cooler ID table.

    Args:
        raw_boiler_cooling_eia860: Raw EIA 860 boiler to cooler ID association table.

    Returns:
        pd.DataFrame: A cleaned and normalized version of the EIA boiler to cooler ID
            table.
    """
    # Replace empty strings, whitespace, and '.' fields with real NA values
    bc_assn = pudl.helpers.fix_eia_na(raw_boiler_cooling_eia860)
    # Replace the report year col with a report date col for the harvesting process
    bc_assn = pudl.helpers.convert_to_date(
        df=bc_assn, year_col="report_year", date_col="report_date"
    )
    # Drop rows with no cooling ID and just in case, drop duplicate
    bc_assn = bc_assn.dropna(subset="cooling_id_eia").drop_duplicates()

    return bc_assn


@asset
def clean_boiler_stack_flue_assn_eia860(
    raw_boiler_stack_flue_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the EIA 860 boiler to stack flue ID table.

    Args:
        raw_boiler_stack_flue_eia860: Raw EIA 860 boiler to stack flue ID association
            table.

    Returns:
        pd.DataFrame: A cleaned and normalized version of the EIA boiler to stack flue
            ID table.
    """
    # Replace empty strings, whitespace, and '.' fields with real NA values
    bsf_assn = pudl.helpers.fix_eia_na(raw_boiler_stack_flue_eia860)
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
    # several instances where flue_id_eia is NA (hense the last fillna(x.stack_id_eia))
    bsf_assn = bsf_assn.assign(
        stack_flue_id_pudl=lambda x: (
            x.stack_flue_id_eia.fillna(
                x.stack_id_eia.astype("string") + "_" + x.flue_id_eia.astype("string")
            ).fillna(x.stack_id_eia)
        )
    )

    return bsf_assn
