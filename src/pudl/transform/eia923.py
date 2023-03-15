"""Module to perform data cleaning functions on EIA923 data tables."""

import numpy as np
import pandas as pd

import pudl
from pudl.metadata.codes import CODE_METADATA
from pudl.settings import Eia923Settings
from pudl.transform.classes import InvalidRows, drop_invalid_rows

logger = pudl.logging_helpers.get_logger(__name__)

COALMINE_COUNTRY_CODES: dict[str, str] = {
    "AU": "AUS",  # Australia
    "CL": "COL",  # Colombia
    "CN": "CAN",  # Canada
    "IS": "IDN",  # Indonesia
    "PL": "POL",  # Poland
    "RS": "RUS",  # Russia
    "UK": "GBR",  # United Kingdom of Great Britain
    "VZ": "VEN",  # Venezuela
    "OT": "other_country",
    "IM": "unknown",
}
"""A mapping of EIA foreign coal mine country codes to 3-letter ISO-3166-1 codes.

The EIA-923 lists the US state of origin for coal deliveries using standard
2-letter US state abbreviations. However, foreign countries are also included
as "states" in this category and because some of them have 2-letter abbreviation
collisions with US states, their coding is non-standard.

Instead of using the provided non-standard codes, we convert to the ISO-3166-1
three letter country codes: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
"""

###############################################################################
###############################################################################
# HELPER FUNCTIONS
###############################################################################
###############################################################################


def _get_plant_nuclear_unit_id_map(nuc_fuel: pd.DataFrame) -> dict[int, str]:
    """Get a plant_id -> nuclear_unit_id mapping for all plants with one nuclear unit.

    Args:
        nuc_fuel: dataframe of nuclear unit fuels.

    Returns:
        plant_to_nuc_id: one to one mapping of plant_id_eia to nuclear_unit_id.
    """
    nuc_fuel = nuc_fuel[nuc_fuel.nuclear_unit_id.notna()].copy()

    # Find the plants with one nuclear unit
    plant_nuc_unit_counts = (
        nuc_fuel.groupby("plant_id_eia").nuclear_unit_id.nunique().copy()
    )

    plant_id_with_one_unit = plant_nuc_unit_counts[plant_nuc_unit_counts.eq(1)].index

    # get the nuclear_unit_id for the plants with one prime mover
    plant_to_nuc_id = (
        nuc_fuel.groupby("plant_id_eia")
        .nuclear_unit_id.unique()
        .loc[plant_id_with_one_unit]
    )

    plant_to_nuc_id = plant_to_nuc_id.explode()

    # check there is one nuclear unit per plant.
    assert (
        plant_to_nuc_id.index.is_unique
    ), "Found multiple nuclear units in plant_to_nuc_id mapping."
    # Check there are no missing nuclear unit ids.
    assert (
        ~plant_to_nuc_id.isna()
    ).all(), "Found missing nuclear_unit_ids in plant_to_nuc_id mappings."

    plant_to_nuc_id = plant_to_nuc_id.astype("string")

    return dict(plant_to_nuc_id)


def _backfill_nuclear_unit_id(nuc_fuel: pd.DataFrame) -> pd.DataFrame:
    """Backfill 2001 and 2002 nuclear_unit_id for plants with one nuclear unit.

    2001 and 2002 generation_fuel_eia923 records do not include nuclear_unit_id which is
    required for the primary key of nuclear_unit_fuel_eia923. We backfill this field for
    plants with one nuclear unit. nuclear_unit_id is filled with 'UNK' if the
    nuclear_unit_id can't be recovered.

    Params:
        nuc_fuel: nuclear fuels dataframe.

    Returns:
        nuc_fuel: nuclear fuels dataframe with backfilled nuclear_unit_id field.
    """
    plant_to_nuc_id_map = _get_plant_nuclear_unit_id_map(nuc_fuel)

    missing_nuclear_unit_id = nuc_fuel.nuclear_unit_id.isna()

    unit_id_fill = nuc_fuel.loc[missing_nuclear_unit_id, "plant_id_eia"].map(
        plant_to_nuc_id_map
    )
    # If we aren't able to impute nuclear_unit_id, fill them UNK.
    unit_id_fill = unit_id_fill.fillna("UNK")

    nuc_fuel.loc[missing_nuclear_unit_id, "nuclear_unit_id"] = unit_id_fill

    missing_nuclear_unit_id = nuc_fuel.nuclear_unit_id.isna()
    nuc_fuel.loc[missing_nuclear_unit_id, "nuclear_unit_id"] = "UNK"

    return nuc_fuel


def _get_plant_prime_mover_map(gen_fuel: pd.DataFrame) -> dict[int, str]:
    """Get a plant_id -> prime_mover_code mapping for all plants with one prime mover.

    Args:
        gen_fuel: dataframe of generation fuels.

    Returns:
        fuel_type_map: one to one mapping of plant_id_eia to prime_mover_codes.
    """
    # Remove fuels that don't have a prime mover.
    gen_fuel = gen_fuel[~gen_fuel.prime_mover_code.isna()].copy()

    # find plants with one prime mover
    plant_prime_movers_counts = (
        gen_fuel.groupby("plant_id_eia").prime_mover_code.nunique().copy()
    )
    plant_ids_with_one_pm = plant_prime_movers_counts[
        plant_prime_movers_counts.eq(1)
    ].index

    # get the prime mover codes for the plants with one prime mover
    plant_to_prime_mover = (
        gen_fuel.groupby("plant_id_eia")
        .prime_mover_code.unique()
        .loc[plant_ids_with_one_pm]
    )

    plant_to_prime_mover = plant_to_prime_mover.explode()

    # check there is one prime mover per plant.
    assert (
        plant_to_prime_mover.index.is_unique
    ), "Found multiple plants in plant_to_prime_mover mapping."
    # Check there are no missing prime mover codes.
    assert (
        plant_to_prime_mover.notnull()
    ).all(), "Found missing prime_mover_codes in plant_to_prime_mover mappings."

    return dict(plant_to_prime_mover)


def _backfill_prime_mover_code(gen_fuel: pd.DataFrame) -> pd.DataFrame:
    """Backfill 2001 and 2002 prime_mover_code for plants with one prime mover.

    2001 and 2002 generation_fuel_eia923 records do not include prime_mover_code
    which is required for the primary key. We backfill this field for plants
    with one prime mover. prime_mover_code is set to 'UNK' if future plants
    have multiple prime movers.

    Args:
        gen_fuel: generation fuels dataframe.

    Returns:
        gen_fuel: generation fuels dataframe with backfilled prime_mover_code field.
    """
    plant_to_prime_mover_map = _get_plant_prime_mover_map(gen_fuel)

    missing_prime_movers = gen_fuel.prime_mover_code.isna()
    gen_fuel.loc[missing_prime_movers, "prime_mover_code"] = (
        gen_fuel.loc[missing_prime_movers, "plant_id_eia"]
        .map(plant_to_prime_mover_map)
        .astype("string")
    )

    # Assign prime mover codes for hydro fuels
    hydro_map = {"HPS": "PS", "HYC": "HY"}
    missing_hydro = (
        gen_fuel.energy_source_code.eq("WAT") & gen_fuel.prime_mover_code.isna()
    )
    gen_fuel.loc[missing_hydro, "prime_mover_code"] = gen_fuel.loc[
        missing_hydro, "fuel_type_code_aer"
    ].map(hydro_map)

    # Assign the rest to UNK
    missing_prime_movers = gen_fuel.prime_mover_code.isna()
    gen_fuel.loc[missing_prime_movers, "prime_mover_code"] = "UNK"

    assert (
        gen_fuel.prime_mover_code.notna().all()
    ), "generation_fuel_923.prime_mover_code has missing values after backfill."
    return gen_fuel


def _get_most_frequent_energy_source_map(gen_fuel: pd.DataFrame) -> dict[str, str]:
    """Get the a mapping of the most common energy_source for each fuel_type_code_aer.

    Args:
        gen_fuel: generation_fuel dataframe.

    Returns:
        energy_source_map: mapping of fuel_type_code_aer to energy_source_code.
    """
    energy_source_counts = gen_fuel.groupby(
        ["fuel_type_code_aer", "energy_source_code"]
    ).plant_id_eia.count()
    energy_source_counts = energy_source_counts.reset_index().sort_values(
        by="plant_id_eia", ascending=False
    )

    energy_source_map = (
        energy_source_counts.groupby(["fuel_type_code_aer"])
        .first()
        .reset_index()[["fuel_type_code_aer", "energy_source_code"]]
    )
    return dict(energy_source_map.values)


def _clean_gen_fuel_energy_sources(gen_fuel: pd.DataFrame) -> pd.DataFrame:
    """Clean the generator_fuel_eia923.energy_source_code field specifically.

    Transformations include:

    * Remap MSW to biogenic and non biogenic fuel types.
    * Fill missing energy_source_code using most common code for each AER fuel codes.

    Args:
        gen_fuel: generation fuels dataframe.

    Returns:
        gen_fuel: generation fuels dataframe with cleaned energy_source_code field.
    """
    # replace whitespace and empty strings with NA values.
    gen_fuel["energy_source_code"] = gen_fuel.energy_source_code.replace(
        to_replace=r"^\s*$", value=pd.NA, regex=True
    )

    # Remap MSW: Prior to 2006, MSW contained biogenic and non biogenic fuel types.
    # Starting in 2006 MSW got split into MSB and MSN. However, the AER fuel type
    # codes always differentiated between biogenic and non-biogenic waste, so we can
    # impose the more recent categorization on the older EIA fuel types.
    msw_fuels = gen_fuel.energy_source_code.eq("MSW")
    gen_fuel.loc[msw_fuels, "energy_source_code"] = gen_fuel.loc[
        msw_fuels, "fuel_type_code_aer"
    ].map(
        {
            "OTH": "MSN",  # non-biogenic municipal waste
            "MLG": "MSB",  # biogenic municipal waste
        }
    )

    # Make sure we replaced all MSWs
    assert gen_fuel.energy_source_code.ne("MSW").all()

    # Fill in any missing fuel_types with the most common fuel type of each
    # fuel_type_code_aer.
    missing_energy_source = gen_fuel.energy_source_code.isna()
    frequent_energy_source_map = _get_most_frequent_energy_source_map(gen_fuel)

    gen_fuel.loc[missing_energy_source, "energy_source_code"] = gen_fuel.loc[
        missing_energy_source, "fuel_type_code_aer"
    ].map(frequent_energy_source_map)
    if gen_fuel.energy_source_code.isna().any():
        raise AssertionError("Missing data in generator_fuel_eia923.energy_source_code")

    return gen_fuel


def _aggregate_generation_fuel_duplicates(
    gen_fuel: pd.DataFrame,
    nuclear: bool = False,
) -> pd.DataFrame:
    """Aggregate remaining duplicate generation fuels.

    There are a handful of plants (< 100) whose prime_mover_code can't be imputed
    or duplicates exist in the raw table. We resolve these be aggregate the variable
    fields.

    Args:
        gen_fuel: generation fuels dataframe.
        nuclear: adds nuclear_unit_id to list of natural key fields.

    Returns:
        gen_fuel: generation fuels dataframe without duplicates in natural key fields.
    """
    natural_key_fields = [
        "report_date",
        "plant_id_eia",
        "energy_source_code",
        "prime_mover_code",
    ]
    if nuclear:
        natural_key_fields += ["nuclear_unit_id"]

    is_duplicate = gen_fuel.duplicated(subset=natural_key_fields, keep=False)

    duplicates = gen_fuel[is_duplicate].copy()
    fuel_type_code_aer_is_unique = (
        duplicates.groupby(natural_key_fields).fuel_type_code_aer.nunique().eq(1).all()
    )
    if not fuel_type_code_aer_is_unique:
        raise AssertionError("Duplicate fuels have different fuel_type_code_aer.")
    data_maturity_is_unique = (
        duplicates.groupby(natural_key_fields).data_maturity.nunique().eq(1).all()
    )
    if not data_maturity_is_unique:
        raise AssertionError("Duplicate fuels have different data_maturity.")

    agg_fields = {
        "fuel_consumed_units": "sum",
        "fuel_consumed_for_electricity_units": "sum",
        "fuel_consumed_mmbtu": "sum",
        "fuel_consumed_for_electricity_mmbtu": "sum",
        "net_generation_mwh": "sum",
        # We can safely select the first values here because we know they are unique
        # within each group of duplicates. We check explicitly for fuel_type_code_aer
        # and data_maturity above, and fuel_type_code_pudl maps to fuel_type_code_aer
        # such that if fuel_type_code_aer is unique, fuel_type_code_pudl must also
        # be unique.
        "fuel_type_code_aer": "first",
        "fuel_type_code_pudl": "first",
        "data_maturity": "first",
    }

    resolved_dupes = (
        duplicates.groupby(natural_key_fields).agg(agg_fields).reset_index()
    )
    # Recalculate fuel_mmbtu_per_unit after aggregation.
    resolved_dupes["fuel_mmbtu_per_unit"] = (
        resolved_dupes["fuel_consumed_mmbtu"] / resolved_dupes["fuel_consumed_units"]
    )
    # In a few cases heat content is reported without any fuel consumed units.
    # In these cases we want NA values, not infinite values:
    resolved_dupes["fuel_mmbtu_per_unit"] = resolved_dupes[
        "fuel_mmbtu_per_unit"
    ].replace([np.inf, -np.inf], np.nan)

    # Add the resolved records back to generation_fuel dataframe.
    gen_df = gen_fuel[~is_duplicate].copy()
    gen_df = pd.concat([gen_df, resolved_dupes])

    if gen_df[natural_key_fields].isnull().any().any():
        raise AssertionError(
            "There are missing values in "
            f"generation_fuel{'_nuclear' if nuclear else ''}_eia923 "
            "natural key fields."
        )

    if gen_df.duplicated(subset=natural_key_fields).any():
        raise AssertionError("Duplicate generation fuels have not been resolved.")

    if gen_fuel.fuel_type_code_pudl.isnull().any():
        raise AssertionError(
            "Null fuel_type_code_pudl values found after aggregating duplicates."
        )

    return gen_df


def _yearly_to_monthly_records(df: pd.DataFrame) -> pd.DataFrame:
    """Converts an EIA 923 record of 12 months of data into 12 monthly records.

    Much of the data reported in EIA 923 is monthly, but all 12 months worth of data is
    reported in a single record, with one field for each of the 12 months.  This
    function converts these annualized composite records into a set of 12 monthly
    records containing the same information, by parsing the field names for months, and
    adding a month field.  Non - time series data is retained in the same format.

    Args:
        df: A pandas DataFrame containing the annual data to be
            converted into monthly records.

    Returns:
        A dataframe containing the same data as was passed in via df,
        but with monthly records as rows instead of as columns.
    """
    month_dict = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    multi_idx = df.columns.str.rsplit("_", n=1, expand=True).set_names(
        [None, "report_month"]
    )
    ends_with_month_filter = multi_idx.get_level_values("report_month").isin(
        set(month_dict.keys())
    )
    if not ends_with_month_filter.any():
        return df
    index_cols = df.columns[~ends_with_month_filter]
    # performance note: this was good enough for eia923 data size.
    # Using .set_index() is simple but inefficient due to unecessary index creation.
    # Performance may be improved by separating into two dataframes,
    # .stack()ing the monthly data, then joining back together on the original index.
    df = df.set_index(list(index_cols), append=True)
    # convert month names to numbers (january -> 1)
    col_df = multi_idx[ends_with_month_filter].to_frame(index=False)
    col_df.loc[:, "report_month"] = col_df.loc[:, "report_month"].map(month_dict)
    month_idx = pd.MultiIndex.from_frame(col_df).set_names([None, "report_month"])
    # reshape
    df.columns = month_idx
    df = df.stack()
    # restore original index and columns - reset index except level 0
    df = df.reset_index(level=list(range(1, df.index.nlevels)))
    return df


def _coalmine_cleanup(cmi_df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the coalmine_eia923 table.

    This function does most of the coalmine_eia923 table transformation. It is separate
    from the coalmine() transform function because of the peculiar way that we are
    normalizing the fuel_receipts_costs_eia923() table.

    All of the coalmine information is originally coming from the EIA
    fuel_receipts_costs spreadsheet, but it really belongs in its own table. We strip it
    out of FRC, and create that separate table, but then we need to refer to that table
    through a foreign key. To do so, we actually merge the entire contents of the
    coalmine table into FRC, including the surrogate key, and then drop the data fields.

    For this to work, we need to have exactly the same coalmine data fields in both the
    new coalmine table, and the FRC table. To ensure that's true, we isolate the
    transformations here in this function, and apply them to the coalmine columns in
    both the FRC table and the coalmine table.

    Args:
        cmi_df: Coal mine information table (e.g. mine name, county, state)

    Returns:
        A cleaned DataFrame containing coalmine information.
    """
    # Because we need to pull the mine_id_msha field into the FRC table,
    # but we don't know what that ID is going to be until we've populated
    # this table... we're going to functionally end up using the data in
    # the coalmine info table as a "key."  Whatever set of things we
    # drop duplicates on will be the defacto key.  Whatever massaging we do
    # of the values here (case, removing whitespace, punctuation, etc.) will
    # affect the total number of "unique" mines that we end up having in the
    # table... and we probably want to minimize it (without creating
    # collisions).  We will need to do exactly the same transofrmations in the
    # FRC ingest function before merging these values in, or they won't match
    # up.
    cmi_df = (
        cmi_df.assign(
            # replace 2-letter country codes w/ ISO 3 letter as appropriate:
            state=lambda x: x.state.replace(COALMINE_COUNTRY_CODES),
            # remove all internal non-alphanumeric characters:
            mine_name=lambda x: x.mine_name.replace("[^a-zA-Z0-9 -]", "", regex=True),
            # Homogenize the data type that we're finding inside the
            # county_id_fips field (ugh, Excel sheets!).  Mostly these are
            # integers or NA values, but for imported coal, there are both
            # 'IMP' and 'IM' string values.
            county_id_fips=lambda x: pudl.helpers.zero_pad_numeric_string(
                x.county_id_fips,
                n_digits=3,
            ),
        )
        # No leading or trailing whitespace:
        .pipe(pudl.helpers.simplify_strings, columns=["mine_name"]).pipe(
            pudl.helpers.add_fips_ids, county_col=None
        )
    )
    # join state and partial county FIPS into five digit county FIPS
    cmi_df["county_id_fips"] = cmi_df["state_id_fips"] + cmi_df["county_id_fips"]
    cmi_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("coalmine_eia923")
        .encode(cmi_df)
    )
    return cmi_df


###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################


def plants(eia923_dfs, eia923_transformed_dfs):
    """Transforms the plants_eia923 table.

    Much of the static plant information is reported repeatedly, and scattered across
    several different pages of EIA 923. The data frame that this function uses is
    assembled from those many different pages, and passed in via the same dictionary of
    dataframes that all the other ingest functions use for uniformity.

    Transformations include:

    * Map full spelling onto code values.
    * Convert Y/N columns to booleans.
    * Remove excess white space around values.
    * Drop duplicate rows.

    Args:
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this dictionary of
            DataFrame objects corresponds to a page from the EIA 923 form, as reported
            in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA923 form (keys) correspond to normalized DataFrames of values from that
        page (values).
    """
    plant_info_df = eia923_dfs["plant_frame"].copy()

    # There are other fields being compiled in the plant_info_df from all of
    # the various EIA923 spreadsheet pages. Do we want to add them to the
    # database model too? E.g. capacity_mw, operator_name, etc.
    plant_info_df = plant_info_df[
        [
            "plant_id_eia",
            "combined_heat_power",
            "plant_state",
            "sector_id_eia",
            "naics_code",
            "reporting_frequency_code",
            "census_region",
            "nerc_region",
            "capacity_mw",
            "report_year",
        ]
    ]
    # Since this is a plain Yes/No variable -- just make it a real sa.Boolean.
    plant_info_df.combined_heat_power.replace({"N": False, "Y": True}, inplace=True)

    # Get rid of excessive whitespace introduced to break long lines (ugh)
    plant_info_df.census_region = plant_info_df.census_region.str.replace(" ", "")
    plant_info_df.drop_duplicates(subset="plant_id_eia")

    plant_info_df["plant_id_eia"] = plant_info_df["plant_id_eia"].astype(int)

    eia923_transformed_dfs["plants_eia923"] = plant_info_df

    return eia923_transformed_dfs


def gen_fuel_nuclear(gen_fuel_nuke: pd.DataFrame) -> pd.DataFrame:
    """Transforms the generation_fuel_nuclear_eia923 table.

    Transformations include:

    * Backfill nuclear_unit_ids for 2001 and 2002.
    * Set all prime_mover_codes to 'ST'.
    * Aggregate remaining duplicate units.

    Args:
        gen_fuel_nuke: dataframe of nuclear unit fuels.

    Returns:
        Transformed nuclear generation fuel table.
    """
    gen_fuel_nuke["nuclear_unit_id"] = (
        gen_fuel_nuke["nuclear_unit_id"].astype("Int64").astype("string")
    )

    gen_fuel_nuke = _backfill_nuclear_unit_id(gen_fuel_nuke)

    # All nuclear plants have steam turbines.
    gen_fuel_nuke.loc[:, "prime_mover_code"] = gen_fuel_nuke["prime_mover_code"].fillna(
        "ST"
    )

    # Aggregate remaining duplicates.
    gen_fuel_nuke = _aggregate_generation_fuel_duplicates(gen_fuel_nuke, nuclear=True)

    return gen_fuel_nuke


def generation_fuel(eia923_dfs, eia923_transformed_dfs):
    """Transforms the generation_fuel_eia923 table.

    Transformations include:

    * Remove fields implicated elsewhere.
    * Replace . values with NA.
    * Remove rows with utility ids 99999.
    * Create a fuel_type_code_pudl field that organizes fuel types into
      clean, distinguishable categories.
    * Combine year and month columns into a single date column.
    * Clean and impute fuel_type field.
    * Backfill missing prime_mover_codes
    * Create a separate generation_fuel_nuclear table.
    * Aggregate records with duplicate natural keys.

    Args:
        eia923_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA923 form, as reported in the Excel
            spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA923 form (keys) correspond to normalized DataFrames of values from that
        page (values).
    """
    # This needs to be a copy of what we're passed in so we can edit it.
    gen_fuel = eia923_dfs["generation_fuel"].copy()

    # Drop fields we're not inserting into the generation_fuel_eia923 table.
    cols_to_drop = [
        "combined_heat_power",
        "plant_name_eia",
        "operator_name",
        "operator_id",
        "plant_state",
        "census_region",
        "nerc_region",
        "naics_code",
        "fuel_unit",
        "total_fuel_consumption_quantity",
        "electric_fuel_consumption_quantity",
        "total_fuel_consumption_mmbtu",
        "elec_fuel_consumption_mmbtu",
        "net_generation_megawatthours",
        "early_release",
    ]
    gen_fuel.drop(cols_to_drop, axis=1, inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    gen_fuel = _yearly_to_monthly_records(gen_fuel)
    # Replace the EIA923 NA value ('.') with a real NA value.
    gen_fuel = pudl.helpers.fix_eia_na(gen_fuel)
    # Remove "State fuel-level increment" records... which don't pertain to
    # any particular plant (they have plant_id_eia == operator_id == 99999)
    gen_fuel = gen_fuel[gen_fuel.plant_id_eia != 99999]

    # conservative manual correction for bad prime mover codes
    gen_fuel["prime_mover_code"] = (
        # one plant in 2004. Pre-2004, it was '',
        # post-2004, it was broken into combined cycle parts
        gen_fuel["prime_mover_code"].replace({"CC": ""})
        # Empty strings and whitespace that should be NA.
        .replace(to_replace=r"^\s*$", value=pd.NA, regex=True)
    )

    gen_fuel = _clean_gen_fuel_energy_sources(gen_fuel)

    gen_fuel = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("generation_fuel_eia923")
        .encode(gen_fuel)
    )

    gen_fuel["fuel_type_code_pudl"] = gen_fuel.energy_source_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["energy_sources_eia"]["df"],
            from_col="code",
            to_col="fuel_type_code_pudl",
            null_value=pd.NA,
        )
    )

    # Drop records missing all variable fields.
    variable_fields = [
        "fuel_consumed_units",
        "fuel_consumed_for_electricity_units",
        "fuel_mmbtu_per_unit",
        "fuel_consumed_mmbtu",
        "fuel_consumed_for_electricity_mmbtu",
        "net_generation_mwh",
    ]
    gen_fuel = gen_fuel.dropna(subset=variable_fields, how="all")

    # Convert Year/Month columns into a single Date column...
    gen_fuel = pudl.helpers.convert_to_date(gen_fuel)

    # Create separate nuclear unit fuel table
    nukes = gen_fuel[
        gen_fuel.nuclear_unit_id.notna() | gen_fuel.energy_source_code.eq("NUC")
    ].copy()

    eia923_transformed_dfs["generation_fuel_nuclear_eia923"] = gen_fuel_nuclear(nukes)

    gen_fuel = gen_fuel[
        gen_fuel.nuclear_unit_id.isna() & gen_fuel.energy_source_code.ne("NUC")
    ].copy()
    gen_fuel = gen_fuel.drop(columns=["nuclear_unit_id"])

    # Backfill 2001, 2002 prime_mover_codes.
    gen_fuel = _backfill_prime_mover_code(gen_fuel)

    # Aggregate any remaining duplicates.
    gen_fuel = _aggregate_generation_fuel_duplicates(gen_fuel)

    eia923_transformed_dfs["generation_fuel_eia923"] = gen_fuel

    return eia923_transformed_dfs


def _map_prime_mover_sets(prime_mover_set: np.ndarray) -> str:
    """Map unique prime mover combinations to a single prime mover code.

    In 2001-2019 data, the .value_counts() of the combinations is:
    (CA, CT)        750
    (ST, CA)        101
    (ST)             60
    (CA)             17
    (CS, ST, CT)      2
    Args:
        prime_mover_set (np.ndarray): unique combinations of prime_mover_code

    Returns:
        str: single prime mover code
    """
    if len(prime_mover_set) == 1:  # single valued
        return prime_mover_set[0]
    elif "CA" in prime_mover_set:
        return "CA"  # arbitrary choice
    elif "CS" in prime_mover_set:
        return "CS"
    else:
        raise ValueError(
            "Dataset contains new kinds of duplicate boiler_fuel rows. "
            f"Prime movers are {prime_mover_set}"
        )


def boiler_fuel(eia923_dfs, eia923_transformed_dfs):
    """Transforms the boiler_fuel_eia923 table.

    Transformations include:

    * Remove fields implicated elsewhere.
    * Drop values with plant and boiler id values of NA.
    * Replace . values with NA.
    * Create a fuel_type_code_pudl field that organizes fuel types into clean,
      distinguishable categories.
    * Combine year and month columns into a single date column.

    Args:
        eia923_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA923 form, as reported in the Excel
            spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).
    """
    bf_df = eia923_dfs["boiler_fuel"].copy()

    # Need to stop dropping fields that contain harvestable entity attributes.
    # See https://github.com/catalyst-cooperative/pudl/issues/509
    cols_to_drop = [
        "combined_heat_power",
        "plant_name_eia",
        "operator_name",
        "operator_id",
        "plant_state",
        "census_region",
        "nerc_region",
        "naics_code",
        "fuel_unit",
        "total_fuel_consumption_quantity",
        "balancing_authority_code_eia",
        "early_release",
        "reporting_frequency_code",
        "data_maturity",
    ]
    bf_df.drop(cols_to_drop, axis=1, inplace=True)

    bf_df.dropna(subset=["boiler_id", "plant_id_eia"], inplace=True)

    bf_df = _yearly_to_monthly_records(bf_df)
    # Replace the EIA923 NA value ('.') with a real NA value.
    bf_df = pudl.helpers.fix_eia_na(bf_df)
    # Convert Year/Month columns into a single Date column...
    bf_df = pudl.helpers.convert_to_date(bf_df)

    bf_df = remove_duplicate_pks_boiler_fuel_eia923(bf_df)

    bf_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("boiler_fuel_eia923")
        .encode(bf_df)
    )

    # Add a simplified PUDL fuel type
    bf_df["fuel_type_code_pudl"] = bf_df.energy_source_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["energy_sources_eia"]["df"],
            from_col="code",
            to_col="fuel_type_code_pudl",
            null_value=pd.NA,
        )
    )

    eia923_transformed_dfs["boiler_fuel_eia923"] = bf_df

    return eia923_transformed_dfs


def remove_duplicate_pks_boiler_fuel_eia923(bf: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate on primary keys for :ref:`boiler_fuel_eia923`.

    There are a relatively small number of records ~5% from the boiler fuel table that
    have duplicate records based on what we believe is this table's primary keys.
    Fortunately, all of these duplicates have at least one records w/ only zeros and or
    nulls. So this method drops only the records which have duplicate pks and only have
    zeros or nulls in the non-primary key columns.

    Note: There are 4 boilers in 2021 that are being dropped entirely during this
    cleaning. They have BOTH duplicate pks and only have zeros or nulls in the
    non-primary key columns. We could choose to preserve all instances of the pks even
    after :func:`drop_invalid_rows` or only dropping one when there are two. We chose to
    leave this be because it was minor and these boilers show up in other years.
    See `comment <https://github.com/catalyst-cooperative/pudl/pull/2362#issuecomment-1470012538>`_
    for more details.
    """
    pk = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("boiler_fuel_eia923")
        .schema.primary_key
    )

    # Drop nulls
    required_valid_cols = [
        "ash_content_pct",
        "fuel_consumed_units",
        "fuel_mmbtu_per_unit",
        "sulfur_content_pct",
    ]
    # make a mask to split bf into records w/ & w/o pk dupes
    pk_dupe_mask = bf.duplicated(pk, keep=False)

    params_pk_dupes = InvalidRows(
        invalid_values=[pd.NA, np.nan, 0], required_valid_cols=required_valid_cols
    )
    bf_no_null_pks_dupes = drop_invalid_rows(
        df=bf[pk_dupe_mask], params=params_pk_dupes
    )

    if not (
        pk_dupes := bf_no_null_pks_dupes[
            bf_no_null_pks_dupes.duplicated(pk, keep=False)
        ]
    ).empty:
        raise AssertionError(
            f"There are ({len(pk_dupes)}) boiler_fuel_eia923 records with "
            "duplicate primary keys after cleaning - expected 0."
        )
    return pd.concat([bf[~pk_dupe_mask], bf_no_null_pks_dupes])


def generation(eia923_dfs, eia923_transformed_dfs):
    """Transforms the generation_eia923 table.

    Transformations include:

    * Drop rows with NA for generator id.
    * Remove fields implicated elsewhere.
    * Replace . values with NA.
    * Drop generator-date row duplicates (all have no data).

    Args:
        eia923_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA923 form, as reported in the Excel
            spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA923 form (keys) correspond to normalized DataFrames of values from that
        page (values).
    """
    gen_df = (
        eia923_dfs["generator"]
        .dropna(subset=["generator_id"])
        .drop(
            [
                "combined_heat_power",
                "plant_name_eia",
                "operator_name",
                "operator_id",
                "plant_state",
                "census_region",
                "nerc_region",
                "naics_code",
                "net_generation_mwh_year_to_date",
                "early_release",
            ],
            axis="columns",
        )
        .pipe(_yearly_to_monthly_records)
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.convert_to_date)
    )
    # There are a few records that contain (literal) "nan"s in the generator_id
    # field.  We are doing a targeted drop here instead of a full drop because
    # We don't want to drop a bunch of data points if new nans are introduced
    # into the data. See issue #1208 for targeted drop reasoning.
    drop_plant_ids = [54587]
    missing_data_strings = ["nan"]
    row_drop_mask = gen_df.plant_id_eia.isin(drop_plant_ids) & gen_df.generator_id.isin(
        missing_data_strings
    )
    gen_df = gen_df[~row_drop_mask]

    # There are a few hundred (out of a few hundred thousand) records which
    # have duplicate records for a given generator/date combo. However, in all
    # cases one of them has no data (net_generation_mwh) associated with it,
    # so it's pretty clear which one to drop.
    unique_subset = ["report_date", "plant_id_eia", "generator_id"]
    dupes = gen_df[gen_df.duplicated(subset=unique_subset, keep=False)]
    gen_df = gen_df.drop(dupes.net_generation_mwh.isna().index)

    gen_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("generation_eia923")
        .encode(gen_df)
    )

    eia923_transformed_dfs["generation_eia923"] = gen_df

    return eia923_transformed_dfs


def coalmine(eia923_dfs, eia923_transformed_dfs):
    """Transforms the coalmine_eia923 table.

    Transformations include:

    * Remove fields implicated elsewhere.
    * Drop duplicates with MSHA ID.

    Args:
        eia923_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA923 form, as reported in the Excel
            spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA923 form (keys) correspond to normalized DataFrames of values from that
        page (values).
    """
    # These are the columns that we want to keep from FRC for the
    # coal mine info table.
    coalmine_cols = [
        "mine_name",
        "mine_type_code",
        "state",
        "county_id_fips",
        "mine_id_msha",
        "data_maturity",
    ]

    # Make a copy so we don't alter the FRC data frame... which we'll need
    # to use again for populating the FRC table (see below)
    cmi_df = eia923_dfs["fuel_receipts_costs"].copy()
    # Keep only the columns listed above:
    cmi_df = _coalmine_cleanup(cmi_df)

    cmi_df = cmi_df[coalmine_cols]

    # If we actually *have* an MSHA ID for a mine, then we have a totally
    # unique identifier for that mine, and we can safely drop duplicates and
    # keep just one copy of that mine, no matter how different all the other
    # fields associated with the mine info are... Here we split out all the
    # coalmine records that have an MSHA ID, remove them from the CMI
    # data frame, drop duplicates, and then bring the unique mine records
    # back into the overall CMI dataframe...
    cmi_with_msha = cmi_df[cmi_df["mine_id_msha"] > 0]
    cmi_with_msha = cmi_with_msha.drop_duplicates(
        subset=[
            "mine_id_msha",
        ]
    )
    cmi_df.drop(cmi_df[cmi_df["mine_id_msha"] > 0].index)
    cmi_df = pd.concat([cmi_df, cmi_with_msha])

    cmi_df = cmi_df.drop_duplicates(subset=coalmine_cols)

    # drop null values if they occur in vital fields....
    cmi_df.dropna(subset=["mine_name", "state"], inplace=True)

    # we need an mine id to associate this coalmine table with the frc
    # table. In order to do that, we need to create a clean index, like
    # an autoincremeted id column in a db, which will later be used as a
    # primary key in the coalmine table and a forigen key in the frc table

    # first we reset the index to get a clean index
    cmi_df = cmi_df.reset_index()
    # then we get rid of the old index
    cmi_df = cmi_df.drop(labels=["index"], axis=1)
    # then name the index id
    cmi_df.index.name = "mine_id_pudl"
    # then make the id index a column for simpler transferability
    cmi_df = cmi_df.reset_index()

    cmi_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("coalmine_eia923")
        .encode(cmi_df)
    )

    eia923_transformed_dfs["coalmine_eia923"] = cmi_df

    return eia923_transformed_dfs


def fuel_receipts_costs(eia923_dfs, eia923_transformed_dfs):
    """Transforms the fuel_receipts_costs_eia923 dataframe.

    Transformations include:

    * Remove fields implicated elsewhere.
    * Replace . values with NA.
    * Standardize codes values.
    * Fix dates.
    * Replace invalid mercury content values with NA.

    Fuel cost is reported in cents per mmbtu. Converts cents to dollars.

    Args:
        eia923_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA923 form, as reported in the Excel
            spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA923 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA923 form (keys) correspond to normalized DataFrames of values from that
        page (values).
    """
    frc_df = eia923_dfs["fuel_receipts_costs"].copy()

    # Drop fields we're not inserting into the fuel_receipts_costs_eia923
    # table.
    cols_to_drop = [
        "plant_name_eia",
        "plant_state",
        "operator_name",
        "operator_id",
        "mine_id_msha",
        "mine_type_code",
        "state",
        "county_id_fips",
        "state_id_fips",
        "mine_name",
        "regulated",
        "early_release",
    ]

    cmi_df = (
        eia923_transformed_dfs["coalmine_eia923"].copy()
        # In order for the merge to work, we need to get the county_id_fips
        # field back into ready-to-dump form... so it matches the types of the
        # county_id_fips field that we are going to be merging on in the
        # frc_df.
        # rename(columns={'id': 'mine_id_pudl'})
    )

    # This type/naming cleanup function is separated out so that we can be
    # sure it is applied exactly the same both when the coalmine_eia923 table
    # is populated, and here (since we need them to be identical for the
    # following merge)
    frc_df = (
        frc_df.pipe(_coalmine_cleanup)
        .merge(
            cmi_df,
            how="left",
            on=[
                "mine_name",
                "state",
                "mine_id_msha",
                "mine_type_code",
                "county_id_fips",
                "data_maturity",
            ],
        )
        .drop(cols_to_drop, axis=1)
        # Replace the EIA923 NA value ('.') with a real NA value.
        .pipe(pudl.helpers.fix_eia_na)
        # These come in ALL CAPS from EIA...
        .pipe(pudl.helpers.simplify_strings, columns=["supplier_name"])
        .pipe(
            pudl.helpers.fix_int_na,
            columns=[
                "contract_expiration_date",
            ],
        )
        .assign(
            fuel_cost_per_mmbtu=lambda x: x.fuel_cost_per_mmbtu / 100,
            fuel_group_code=lambda x: (
                x.fuel_group_code.str.lower().str.replace(" ", "_")
            ),
            contract_expiration_month=lambda x: x.contract_expiration_date.apply(
                lambda y: y[:-2] if y != "" else y
            ),
        )
        .assign(
            # These assignments are separate b/c they exp_month is altered 2x
            contract_expiration_month=lambda x: x.contract_expiration_month.apply(
                lambda y: y if y != "" and int(y) <= 12 else ""
            ),
            contract_expiration_year=lambda x: x.contract_expiration_date.apply(
                lambda y: "20" + y[-2:] if y != "" else y
            ),
        )
        # Now that we will create our own real date field, so chuck this one.
        .drop("contract_expiration_date", axis=1)
        .pipe(
            pudl.helpers.convert_to_date,
            date_col="contract_expiration_date",
            year_col="contract_expiration_year",
            month_col="contract_expiration_month",
        )
        .pipe(pudl.helpers.convert_to_date)
        .pipe(
            pudl.helpers.cleanstrings,
            ["natural_gas_transport_code", "natural_gas_delivery_contract_type_code"],
            [
                {"firm": ["F"], "interruptible": ["I"]},
                {"firm": ["F"], "interruptible": ["I"]},
            ],
            unmapped=pd.NA,
        )
    )
    frc_df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("fuel_receipts_costs_eia923")
        .encode(frc_df)
    )
    frc_df["fuel_type_code_pudl"] = frc_df.energy_source_code.map(
        pudl.helpers.label_map(
            CODE_METADATA["energy_sources_eia"]["df"],
            from_col="code",
            to_col="fuel_type_code_pudl",
            null_value=pd.NA,
        )
    )

    # Remove known to be invalid mercury content values. Almost all of these
    # occur in the 2012 data. Real values should be <0.25ppm.
    bad_hg_idx = frc_df.mercury_content_ppm >= 7.0
    frc_df.loc[bad_hg_idx, "mercury_content_ppm"] = np.nan

    eia923_transformed_dfs["fuel_receipts_costs_eia923"] = frc_df

    return eia923_transformed_dfs


def transform(eia923_raw_dfs, eia923_settings: Eia923Settings = Eia923Settings()):
    """Transforms all the EIA 923 tables.

    Args:
        eia923_raw_dfs (dict): a dictionary of tab names (keys) and DataFrames
            (values). Generated from `pudl.extract.eia923.extract()`.
        settings: Object containing validated settings
            relevant to EIA 923. Contains the tables and years to be loaded
            into PUDL.

    Returns:
        dict: A dictionary of DataFrame with table names as keys and
        :class:`pandas.DataFrame` objects as values, where the contents of the
        DataFrames correspond to cleaned and normalized PUDL database tables, ready for
        loading.
    """
    eia923_transform_functions = {
        "generation_fuel_eia923": generation_fuel,
        "boiler_fuel_eia923": boiler_fuel,
        "generation_eia923": generation,
        "coalmine_eia923": coalmine,
        "fuel_receipts_costs_eia923": fuel_receipts_costs,
    }
    eia923_transformed_dfs = {}

    if not eia923_raw_dfs:
        logger.info("No raw EIA 923 DataFrames found. " "Not transforming EIA 923.")
        return eia923_transformed_dfs

    for table in eia923_transform_functions:
        if table in eia923_settings.tables:
            logger.info(
                f"Transforming raw EIA 923 DataFrames for {table} "
                f"concatenated across all years."
            )
            eia923_transform_functions[table](eia923_raw_dfs, eia923_transformed_dfs)
        else:
            logger.info(f"Not transforming {table}")
    return eia923_transformed_dfs
