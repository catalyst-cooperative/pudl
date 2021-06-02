"""Module to perform data cleaning functions on EIA860 data tables."""

import logging

import numpy as np
import pandas as pd

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


BOOL_MAP = {
    "Y": True,
    "X": False,
    "N": False,
    "true": True,
    "false": False,
    "True": True,
    "False": False,
    "U": pd.NA,
    "NaN": pd.NA,
    "nan": pd.NA,
    np.nan: pd.NA,
}

OWNERSHIP_PLANT_GEN_ID_DUPES = [
    (56032, "1"),
]
"""tuple: EIA Plant IDs which have duplicate generators within the ownership table due
to the removal of leading zeroes from the generator IDs."""


def ownership(eia860_dfs, eia860_transformed_dfs):
    """
    Pull and transform the ownership table.

    Transformations include:

    * Replace . values with NA.
    * Convert pre-2012 ownership percentages to proportions to match post-2012
      reporting.

    Args:
        eia860_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA860 form, as reported in the Excel
            spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA860 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in which
        pages from EIA860 form (keys) correspond to normalized DataFrames of values
        from that page (values).

    """
    # Preiminary clean and get rid of unecessary 'year' column
    own_df = (
        eia860_dfs['ownership'].copy()
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.convert_to_date)
        .drop(columns=['year'])
    )

    if (min(own_df.report_date.dt.year)
            < min(pc.working_partitions['eia860']['years'])):
        raise ValueError(
            f"EIA 860 transform step is only known to work for "
            f"year {min(pc.working_partitions['eia860']['years'])} and later, "
            f"but found data from year {min(own_df.report_date.dt.year)}."
        )

    # Prior to 2012, ownership was reported as a percentage, rather than
    # as a proportion, so we need to divide those values by 100.
    own_df.loc[own_df.report_date.dt.year < 2012, 'fraction_owned'] = \
        own_df.loc[own_df.report_date.dt.year < 2012, 'fraction_owned'] / 100

    # This has to come before the fancy indexing below, otherwise the plant_id_eia
    # is still a float.
    own_df = own_df.astype({
        "owner_utility_id_eia": pd.Int64Dtype(),
        "utility_id_eia": pd.Int64Dtype(),
        "plant_id_eia": pd.Int64Dtype(),
        "owner_state": pd.StringDtype()
    })

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
        .loc[OWNERSHIP_PLANT_GEN_ID_DUPES]
    )
    # Index of own_df w/ duplicated records removed.
    without_known_dupes_idx = (
        own_df.set_index(["plant_id_eia", "generator_id"])
        .index.difference(known_dupes.index)
    )
    # own_df w/ duplicated records removed.
    without_known_dupes = (
        own_df.set_index(["plant_id_eia", "generator_id"])
        .loc[without_known_dupes_idx]
        .reset_index()
    )
    # Drop duplicates from the known dupes using the whole primary key
    own_pk = [
        'report_date',
        'plant_id_eia',
        'generator_id',
        'owner_utility_id_eia',
    ]
    deduped = known_dupes.reset_index().drop_duplicates(subset=own_pk)
    # Bring these two parts back together:
    own_df = pd.concat([without_known_dupes, deduped])
    # Check whether we have truly deduplicated the dataframe.
    remaining_dupes = own_df[own_df.duplicated(subset=own_pk, keep=False)]
    if len(remaining_dupes) > 0:
        raise ValueError(
            "Duplicate ownership slices found in ownership_eia860:"
            f"{remaining_dupes}"
        )

    eia860_transformed_dfs['ownership_eia860'] = own_df

    return eia860_transformed_dfs


def generators(eia860_dfs, eia860_transformed_dfs):
    """
    Pull and transform the generators table.

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
        eia860_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the EIA860 form,
            as reported in the Excel spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA860 form (keys) correspond to a normalized DataFrame of
            values from that page (values).

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA860 form (keys) correspond to normalized DataFrames of values from that
        page (values).

    """
    # Groupby objects were creating chained assignment warning that is N/A
    pd.options.mode.chained_assignment = None

    # There are three sets of generator data reported in the EIA860 table,
    # planned, existing, and retired generators. We're going to concatenate
    # them all together into a single big table, with a column that indicates
    # which one of these tables the data came from, since they all have almost
    # exactly the same structure
    gp_df = eia860_dfs['generator_proposed'].copy()
    ge_df = eia860_dfs['generator_existing'].copy()
    gr_df = eia860_dfs['generator_retired'].copy()
    g_df = eia860_dfs['generator'].copy()
    gp_df['operational_status'] = 'proposed'
    ge_df['operational_status'] = 'existing'
    gr_df['operational_status'] = 'retired'
    g_df['operational_status'] = (
        g_df['operational_status_code']
        .map({  # could move this dict to constants
            'OP': 'existing',
            'SB': 'existing',
            'OA': 'existing',
            'OS': 'existing',
            'RE': 'retired'
        })
    )

    gens_df = (
        pd.concat([ge_df, gp_df, gr_df, g_df], sort=True)
        .dropna(subset=['generator_id', 'plant_id_eia'])
        .pipe(pudl.helpers.fix_eia_na)
    )

    # A subset of the columns have zero values, where NA is appropriate:
    columns_to_fix = [
        'minimum_load_mw',
        'nameplate_power_factor',
        'other_modifications_month',
        'other_modifications_year',
        'planned_derate_month',
        'planned_derate_year',
        'planned_net_summer_capacity_derate_mw',
        'planned_net_summer_capacity_uprate_mw',
        'planned_net_winter_capacity_derate_mw',
        'planned_net_winter_capacity_uprate_mw',
        'planned_new_capacity_mw',
        'planned_repower_month',
        'planned_repower_year',
        'planned_retirement_month',
        'planned_retirement_year',
        'planned_uprate_month',
        'planned_uprate_year',
        'summer_capacity_mw',
        'winter_capacity_mw',
    ]

    for column in columns_to_fix:
        gens_df[column] = gens_df[column].replace(
            to_replace=[" ", 0], value=np.nan)

    boolean_columns_to_fix = [
        'associated_combined_heat_power',
        'bypass_heat_recovery',
        'carbon_capture',
        'cofire_fuels',
        'deliver_power_transgrid',
        'distributed_generation',
        'duct_burners',
        'ferc_cogen_status',
        'ferc_exempt_wholesale_generator',
        'ferc_small_power_producer',
        'fluidized_bed_tech',
        'multiple_fuels',
        'other_combustion_tech',
        'other_planned_modifications',
        'owned_by_non_utility',
        'planned_modifications',
        'previously_canceled',
        'pulverized_coal_tech',
        'solid_fuel_gasification',
        'stoker_tech',
        'summer_capacity_estimate',
        'subcritical_tech',
        'supercritical_tech',
        'switch_oil_gas',
        'syncronized_transmission_grid',
        'ultrasupercritical_tech',
        'uprate_derate_during_year',
        'winter_capacity_estimate',
    ]

    for column in boolean_columns_to_fix:
        gens_df[column] = gens_df[column].map(BOOL_MAP)

    # A subset of the pre-2009 columns refer to transportation methods with a
    # series of codes. This writes them out in their entirety.

    transport_columns_to_fix = [
        'energy_source_1_transport_1',
        'energy_source_1_transport_2',
        'energy_source_1_transport_3',
        'energy_source_2_transport_1',
        'energy_source_2_transport_2',
        'energy_source_2_transport_3',
    ]

    for column in transport_columns_to_fix:
        gens_df[column] = (
            gens_df[column]
            .astype('string')
            .map(pc.TRANSIT_TYPE_DICT)
        )

    gens_df = (
        gens_df.
        pipe(pudl.helpers.month_year_to_date).
        assign(fuel_type_code_pudl=lambda x: pudl.helpers.cleanstrings_series(
            x['energy_source_code_1'], pc.fuel_type_eia860_simple_map)).
        pipe(pudl.helpers.simplify_strings,
             columns=['rto_iso_lmp_node_id',
                      'rto_iso_location_wholesale_reporting_id']).
        pipe(pudl.helpers.convert_to_date)
    )

    eia860_transformed_dfs['generators_eia860'] = gens_df

    return eia860_transformed_dfs


def plants(eia860_dfs, eia860_transformed_dfs):
    """
    Pull and transform the plants table.

    Much of the static plant information is reported repeatedly, and scattered across
    several different pages of EIA 923. The data frame which this function uses is
    assembled from those many different pages, and passed in via the same dictionary of
    dataframes that all the other ingest functions use for uniformity.

    Transformations include:

    * Replace . values with NA.
    * Homogenize spelling of county names.
    * Convert Y/N/X values to boolean True/False.

    Args:
        eia860_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA860 form, as reported in the Excel
            spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA860 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA860 form (keys) correspond to normalized DataFrames of values from that
        page (values).

    """
    # Populating the 'plants_eia860' table
    p_df = (
        eia860_dfs['plant'].copy()
        .pipe(pudl.helpers.fix_eia_na)
        .astype({"zip_code": str})
        .drop("iso_rto", axis="columns")
    )

    # Spelling, punctuation, and capitalization of county names can vary from
    # year to year. We homogenize them here to facilitate correct value
    # harvesting.
    p_df['county'] = (
        p_df.county.
        str.replace(r'[^a-z,A-Z]+', ' ', regex=True).
        str.strip().
        str.lower().
        str.replace(r'\s+', ' ', regex=True).
        str.title()
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
        p_df[column] = p_df[column].map(BOOL_MAP)

    # Ensure plant & operator IDs are integers.
    p_df = pudl.helpers.convert_to_date(p_df)

    eia860_transformed_dfs['plants_eia860'] = p_df

    return eia860_transformed_dfs


def boiler_generator_assn(eia860_dfs, eia860_transformed_dfs):
    """
    Pull and transform the boilder generator association table.

    Transformations include:

    * Drop non-data rows with EIA notes.
    * Drop duplicate rows.

    Args:
        eia860_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA860 form, as reported in the Excel
            spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA860 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA860 form (keys) correspond to normalized DataFrames of values from that
        page (values).

    """
    # Populating the 'generators_eia860' table
    b_g_df = eia860_dfs['boiler_generator_assn'].copy()
    b_g_cols = ['report_year',
                'utility_id_eia',
                'plant_id_eia',
                'boiler_id',
                'generator_id']

    b_g_df = b_g_df[b_g_cols]

    # There are some bad (non-data) lines in some of the boiler generator
    # data files (notes from EIA) which are messing up the import. Need to
    # identify and drop them early on.
    b_g_df['utility_id_eia'] = b_g_df['utility_id_eia'].astype(str)
    b_g_df = b_g_df[b_g_df.utility_id_eia.str.isnumeric()]

    b_g_df['plant_id_eia'] = b_g_df['plant_id_eia'].astype(int)

    # We need to cast the generator_id column as type str because sometimes
    # it is heterogeneous int/str which make drop_duplicates fail.
    b_g_df['generator_id'] = b_g_df['generator_id'].astype(str)
    b_g_df['boiler_id'] = b_g_df['boiler_id'].astype(str)

    # This drop_duplicates isn't removing all duplicates
    b_g_df = b_g_df.drop_duplicates().dropna()

    b_g_df = pudl.helpers.convert_to_date(b_g_df)

    eia860_transformed_dfs['boiler_generator_assn_eia860'] = b_g_df

    return eia860_transformed_dfs


def utilities(eia860_dfs, eia860_transformed_dfs):
    """
    Pull and transform the utilities table.

    Transformations include:

    * Replace . values with NA.
    * Fix typos in state abbreviations, convert to uppercase.
    * Drop address_3 field (all NA).
    * Combine phone number columns into one field and set values that don't mimic real
      US phone numbers to NA.
    * Convert Y/N/X values to boolean True/False.
    * Map full spelling onto code values.

    Args:
        eia860_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the EIA860 form,
            as reported in the Excel spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in which pages
            from EIA860 form (keys) correspond to normalized DataFrames of values from
            that page (values).

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in which pages
        from EIA860 form (keys) correspond to normalized DataFrames of values from that
        page (values).

    """
    # Populating the 'utilities_eia860' table
    u_df = eia860_dfs['utility'].copy()

    # Replace empty strings, whitespace, and '.' fields with real NA values
    u_df = pudl.helpers.fix_eia_na(u_df)
    u_df['state'] = u_df.state.str.upper()
    u_df['state'] = u_df.state.replace({
        'QB': 'QC',  # wrong abbreviation for Quebec
        'Y': 'NY',  # Typo
    })

    # Remove Address 3 column that is all NA
    u_df = u_df.drop(['address_3'], axis=1)

    # Combine phone number columns into one
    def _make_phone_number(col1, col2, col3):
        """Make and validate full phone number seperated by dashes."""
        p_num = (
            col1.astype('string')
            + '-' + col2.astype('string')
            + '-' + col3.astype('string')
        )
        # Turn anything that doesn't match a US phone number format to NA
        # using noqa to get past flake8 test that give a false positive thinking
        # that the regex string is supposed to be an f-string and is missing
        # the it's designated prefix.
        return p_num.replace(regex=r'^(?!.*\d{3}-\d{3}-\d{4}).*$', value=pd.NA)  # noqa: FS003

    u_df = (
        u_df.assign(
            phone_number_1=_make_phone_number(
                u_df.phone_number_first_1,
                u_df.phone_number_mid_1,
                u_df.phone_number_last_1),
            phone_number_2=_make_phone_number(
                u_df.phone_number_first_2,
                u_df.phone_number_mid_2,
                u_df.phone_number_last_2))
    )

    boolean_columns_to_fix = [
        'plants_reported_owner',
        'plants_reported_operator',
        'plants_reported_asset_manager',
        'plants_reported_other_relationship'
    ]

    for column in boolean_columns_to_fix:
        u_df[column] = u_df[column].map(BOOL_MAP)

    u_df = (
        u_df.astype({
            "utility_id_eia": int
        })
        .assign(
            entity_type=lambda x: x.entity_type.map(pc.ENTITY_TYPE_DICT)
        )
        .pipe(pudl.helpers.convert_to_date)
    )

    eia860_transformed_dfs['utilities_eia860'] = u_df

    return eia860_transformed_dfs


def transform(eia860_raw_dfs, eia860_tables=pc.pudl_tables["eia860"]):
    """
    Transform EIA 860 DataFrames.

    Args:
        eia860_raw_dfs (dict): a dictionary of tab names (keys) and DataFrames
            (values). This can be generated by pudl.
        eia860_tables (tuple): A tuple containing the names of the EIA 860 tables that
            can be pulled into PUDL.

    Returns:
        dict: A dictionary of DataFrame objects in which pages from EIA860 form (keys)
        corresponds to a normalized DataFrame of values from that page (values).

    """
    # these are the tables that we have transform functions for...
    eia860_transform_functions = {
        'ownership_eia860': ownership,
        'generators_eia860': generators,
        'plants_eia860': plants,
        'boiler_generator_assn_eia860': boiler_generator_assn,
        'utilities_eia860': utilities}
    eia860_transformed_dfs = {}

    if not eia860_raw_dfs:
        logger.info("No raw EIA 860 dataframes found. "
                    "Not transforming EIA 860.")
        return eia860_transformed_dfs
    # for each of the tables, run the respective transform funtction
    for table in eia860_transform_functions:
        if table in eia860_tables:
            logger.info("Transforming raw EIA 860 DataFrames for %s "
                        "concatenated across all years.", table)
            eia860_transform_functions[table](eia860_raw_dfs,
                                              eia860_transformed_dfs)

    return eia860_transformed_dfs
