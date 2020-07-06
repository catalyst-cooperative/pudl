"""Routines specific to cleaning up EIA Form 923 data."""
import logging

import numpy as np
import pandas as pd

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)
###############################################################################
###############################################################################
# HELPER FUNCTIONS
###############################################################################
###############################################################################


def _yearly_to_monthly_records(df, md):
    """Converts an EIA 923 record of 12 months of data into 12 monthly records.

    Much of the data reported in EIA 923 is monthly, but all 12 months worth of
    data is reported in a single record, with one field for each of the 12
    months.  This function converts these annualized composite records into a
    set of 12 monthly records containing the same information, by parsing the
    field names for months, and adding a month field.  Non - time series data
    is retained in the same format.

    Args:
        df (pandas.DataFrame): A pandas DataFrame containing the annual
            data to be converted into monthly records.
        md (dict): a dictionary with the integers 1-12 as keys, and the
            patterns used to match field names for each of the months as
            values. These patterns are also used to rename the columns in
            the dataframe which is returned, so they need to match the entire
            portion of the column name that is month specific.

    Returns:
        pandas.DataFrame: A dataframe containing the same data as was passed in
        via df, but with monthly records instead of annual records.

    """
    yearly = df.copy()
    all_years = pd.DataFrame()

    for y in yearly.report_year.unique():
        this_year = yearly[yearly.report_year == y].copy()
        monthly = pd.DataFrame()
        for m in md:
            # Grab just the columns for the month we're working on.
            this_month = this_year.filter(regex=md[m]).copy()
            # Drop this month's data from the yearly data frame.
            this_year.drop(this_month.columns, axis=1, inplace=True)
            # Rename this month's columns to get rid of the month reference.
            this_month.columns = this_month.columns.str.replace(md[m], '')
            # Add a numerical month column corresponding to this month.
            this_month['report_month'] = m
            # Add this month's data to the monthly DataFrame we're building.
            monthly = pd.concat([monthly, this_month], sort=True)

        # Merge the monthly data we've built up with the remaining fields in
        # the data frame we started with -- all of which should be independent
        # of the month, and apply across all 12 of the monthly records created
        # from each of the # initial annual records.
        this_year = this_year.merge(monthly, left_index=True, right_index=True)
        # Add this new year's worth of data to the big dataframe we'll return
        all_years = pd.concat([all_years, this_year], sort=True)

    return all_years


def _coalmine_cleanup(cmi_df):
    """Cleans up the coalmine_eia923 table.

    This function does most of the coalmine_eia923 table transformation. It is
    separate from the coalmine() transform function because of the peculiar
    way that we are normalizing the fuel_receipts_costs_eia923() table.

    All of the coalmine information is originally coming from the EIA
    fuel_receipts_costs spreadsheet, but it really belongs in its own table.
    We strip it out of FRC, and create that separate table, but then we need
    to refer to that table through a foreign key. To do so, we actually merge
    the entire contents of the coalmine table into FRC, including the surrogate
    key, and then drop the data fields.

    For this to work, we need to have exactly the same coalmine data fields in
    both the new coalmine table, and the FRC table. To ensure that's true, we
    isolate the transformations here in this function, and apply them to the
    coalmine columns in both the FRC table and the coalmine table.

    Args:
        cmi_df (pandas.DataFrame): A DataFrame to be cleaned, containing
            coalmine information (e.g. name, county, state)

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing coalmine information.

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
            # Map mine type codes, which have changed over the years, to a few
            # canonical values:
            mine_type_code=lambda x: x.mine_type_code.replace(
                {'[pP]': 'P', 'U/S': 'US', 'S/U': 'SU', 'Su': 'S'},
                regex=True),
            # replace 2-letter country codes w/ ISO 3 letter as appropriate:
            state=lambda x: x.state.replace(pc.coalmine_country_eia923),
            # remove all internal non-alphanumeric characters:
            mine_name=lambda x: x.mine_name.replace(
                '[^a-zA-Z0-9 -]', '', regex=True),
            # Homogenize the data type that we're finding inside the
            # county_id_fips field (ugh, Excel sheets!).  Mostly these are
            # integers or NA values, but for imported coal, there are both
            # 'IMP' and 'IM' string values.
            county_id_fips=lambda x: x.county_id_fips.replace(
                '[a-zA-Z]+', value=np.nan, regex=True
            )
        )
        # No leading or trailing whitespace:
        .pipe(pudl.helpers.strip_lower, columns=["mine_name"])
        .astype({"county_id_fips": float})
        .astype({"county_id_fips": pd.Int64Dtype()})
        .fillna({"mine_type_code": pd.NA})
        .astype({"mine_type_code": pd.StringDtype()})
    )
    return cmi_df

###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################


def plants(eia923_dfs, eia923_transformed_dfs):
    """Transforms the plants_eia923 table.

    Much of the static plant information is reported repeatedly, and scattered
    across several different pages of EIA 923. The data frame that this
    function uses is assembled from those many different pages, and passed in
    via the same dictionary of dataframes that all the other ingest functions
    use for uniformity.

    Args:
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the EIA
            923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA923 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA923 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    plant_info_df = eia923_dfs['plant_frame'].copy()

    # There are other fields being compiled in the plant_info_df from all of
    # the various EIA923 spreadsheet pages. Do we want to add them to the
    # database model too? E.g. capacity_mw, operator_name, etc.
    plant_info_df = plant_info_df[['plant_id_eia',
                                   'combined_heat_power',
                                   'plant_state',
                                   'eia_sector',
                                   'naics_code',
                                   'reporting_frequency',
                                   'census_region',
                                   'nerc_region',
                                   'capacity_mw',
                                   'report_year']]

    plant_info_df['reporting_frequency'] = plant_info_df.reporting_frequency.replace({'M': 'monthly',
                                                                                      'A': 'annual'})
    # Since this is a plain Yes/No variable -- just make it a real sa.Boolean.
    plant_info_df.combined_heat_power.replace({'N': False, 'Y': True},
                                              inplace=True)

    # Get rid of excessive whitespace introduced to break long lines (ugh)
    plant_info_df.census_region = plant_info_df.census_region.str.replace(
        ' ', '')
    plant_info_df.drop_duplicates(subset='plant_id_eia')

    plant_info_df['plant_id_eia'] = plant_info_df['plant_id_eia'].astype(int)

    eia923_transformed_dfs['plants_eia923'] = plant_info_df

    return eia923_transformed_dfs


def generation_fuel(eia923_dfs, eia923_transformed_dfs):
    """Transforms the generation_fuel_eia923 table.

    Args:
        eia923_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA923 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA923 form (keys) correspond to normalized
        DataFrames of values from that page (values).

    """
    # This needs to be a copy of what we're passed in so we can edit it.
    gf_df = eia923_dfs['generation_fuel'].copy()

    # Drop fields we're not inserting into the generation_fuel_eia923 table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name_eia',
                    'operator_name',
                    'operator_id',
                    'plant_state',
                    'census_region',
                    'nerc_region',
                    'naics_code',
                    'eia_sector',
                    'sector_name',
                    'fuel_unit',
                    'total_fuel_consumption_quantity',
                    'electric_fuel_consumption_quantity',
                    'total_fuel_consumption_mmbtu',
                    'elec_fuel_consumption_mmbtu',
                    'net_generation_megawatthours']
    gf_df.drop(cols_to_drop, axis=1, inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    gf_df = _yearly_to_monthly_records(gf_df, pc.month_dict_eia923)
    # Replace the EIA923 NA value ('.') with a real NA value.
    gf_df = pudl.helpers.fix_eia_na(gf_df)
    # Remove "State fuel-level increment" records... which don't pertain to
    # any particular plant (they have plant_id_eia == operator_id == 99999)
    gf_df = gf_df[gf_df.plant_id_eia != 99999]

    gf_df['fuel_type_code_pudl'] = pudl.helpers.cleanstrings_series(gf_df.fuel_type,
                                                                    pc.fuel_type_eia923_gen_fuel_simple_map)

    # Convert Year/Month columns into a single Date column...
    gf_df = pudl.helpers.convert_to_date(gf_df)

    eia923_transformed_dfs['generation_fuel_eia923'] = gf_df

    return eia923_transformed_dfs


def boiler_fuel(eia923_dfs, eia923_transformed_dfs):
    """Transforms the boiler_fuel_eia923 table.

    Args:
        eia923_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA923 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA923 form (keys) correspond to normalized
        DataFrames of values from that page (values).

    """
    bf_df = eia923_dfs['boiler_fuel'].copy()

    # Drop fields we're not inserting into the boiler_fuel_eia923 table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name_eia',
                    'operator_name',
                    'operator_id',
                    'plant_state',
                    'census_region',
                    'nerc_region',
                    'naics_code',
                    'eia_sector',
                    'sector_name',
                    'fuel_unit',
                    'total_fuel_consumption_quantity']
    bf_df.drop(cols_to_drop, axis=1, inplace=True)

    bf_df.dropna(subset=['boiler_id', 'plant_id_eia'], inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    bf_df = _yearly_to_monthly_records(
        bf_df, pc.month_dict_eia923)
    bf_df['fuel_type_code_pudl'] = pudl.helpers.cleanstrings_series(
        bf_df.fuel_type_code,
        pc.fuel_type_eia923_boiler_fuel_simple_map)
    # Replace the EIA923 NA value ('.') with a real NA value.
    bf_df = pudl.helpers.fix_eia_na(bf_df)

    # Convert Year/Month columns into a single Date column...
    bf_df = pudl.helpers.convert_to_date(bf_df)

    eia923_transformed_dfs['boiler_fuel_eia923'] = bf_df

    return eia923_transformed_dfs


def generation(eia923_dfs, eia923_transformed_dfs):
    """Transforms the generation_eia923 table.

    Args:
        eia923_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA923 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA923 form (keys) correspond to normalized
        DataFrames of values from that page (values).

    """
    gen_df = (
        eia923_dfs['generator']
        .dropna(subset=['generator_id'])
        .drop(['combined_heat_power',
               'plant_name_eia',
               'operator_name',
               'operator_id',
               'plant_state',
               'census_region',
               'nerc_region',
               'naics_code',
               'eia_sector',
               'sector_name',
               'net_generation_mwh_year_to_date'],
              axis="columns")
        .pipe(_yearly_to_monthly_records, pc.month_dict_eia923)
        .pipe(pudl.helpers.fix_eia_na)
        .pipe(pudl.helpers.convert_to_date)
    )
    # There are a few hundred (out of a few hundred thousand) records which
    # have duplicate records for a given generator/date combo. However, in all
    # cases one of them has no data (net_generation_mwh) associated with it,
    # so it's pretty clear which one to drop.
    unique_subset = ["report_date", "plant_id_eia", "generator_id"]
    dupes = gen_df[gen_df.duplicated(subset=unique_subset, keep=False)]
    gen_df = gen_df.drop(dupes.net_generation_mwh.isna().index)

    eia923_transformed_dfs['generation_eia923'] = gen_df

    return eia923_transformed_dfs


def coalmine(eia923_dfs, eia923_transformed_dfs):
    """Transforms the coalmine_eia923 table.

    Args:
        eia923_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA923 form, as reported in the
            Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA923 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA923 form (keys) correspond to normalized
        DataFrames of values from that page (values).

    """
    # These are the columns that we want to keep from FRC for the
    # coal mine info table.
    coalmine_cols = ['mine_name',
                     'mine_type_code',
                     'state',
                     'county_id_fips',
                     'mine_id_msha']

    # Make a copy so we don't alter the FRC data frame... which we'll need
    # to use again for populating the FRC table (see below)
    cmi_df = eia923_dfs['fuel_receipts_costs'].copy()
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
    cmi_with_msha = cmi_df[cmi_df['mine_id_msha'] > 0]
    cmi_with_msha = cmi_with_msha.drop_duplicates(subset=['mine_id_msha', ])
    cmi_df.drop(cmi_df[cmi_df['mine_id_msha'] > 0].index)
    cmi_df.append(cmi_with_msha)

    cmi_df = cmi_df.drop_duplicates(subset=['mine_name',
                                            'state',
                                            'mine_id_msha',
                                            'mine_type_code',
                                            'county_id_fips'])

    # drop null values if they occur in vital fields....
    cmi_df.dropna(subset=['mine_name', 'state'], inplace=True)

    # we need an mine id to associate this coalmine table with the frc
    # table. In order to do that, we need to create a clean index, like
    # an autoincremeted id column in a db, which will later be used as a
    # primary key in the coalmine table and a forigen key in the frc table

    # first we reset the index to get a clean index
    cmi_df = cmi_df.reset_index()
    # then we get rid of the old index
    cmi_df = cmi_df.drop(labels=['index'], axis=1)
    # then name the index id
    cmi_df.index.name = 'mine_id_pudl'
    # then make the id index a column for simpler transferability
    cmi_df = cmi_df.reset_index()

    eia923_transformed_dfs['coalmine_eia923'] = cmi_df

    return eia923_transformed_dfs


def fuel_receipts_costs(eia923_dfs, eia923_transformed_dfs):
    """Transforms the fuel_receipts_costs_eia923 dataframe.

    Fuel cost is reported in cents per mmbtu. Converts cents to dollars.

    Args:
        eia923_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA923 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia923_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA923 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    frc_df = eia923_dfs['fuel_receipts_costs'].copy()

    # Drop fields we're not inserting into the fuel_receipts_costs_eia923
    # table.
    cols_to_drop = ['plant_name_eia',
                    'plant_state',
                    'operator_name',
                    'operator_id',
                    'mine_id_msha',
                    'mine_type_code',
                    'state',
                    'county_id_fips',
                    'mine_name',
                    'regulated',
                    'reporting_frequency']

    cmi_df = (
        eia923_transformed_dfs['coalmine_eia923'].copy()
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
        frc_df.pipe(_coalmine_cleanup).
        merge(cmi_df, how='left',
              on=['mine_name', 'state', 'mine_id_msha',
                  'mine_type_code', 'county_id_fips']).
        drop(cols_to_drop, axis=1).
        # Replace the EIA923 NA value ('.') with a real NA value.
        pipe(pudl.helpers.fix_eia_na).
        # These come in ALL CAPS from EIA...
        pipe(pudl.helpers.strip_lower, columns=['supplier_name']).
        pipe(pudl.helpers.fix_int_na, columns=['contract_expiration_date', ]).
        assign(
            # Standardize case on transportaion codes -- all upper case!
            primary_transportation_mode_code=lambda x: x.primary_transportation_mode_code.str.upper(),
            secondary_transportation_mode_code=lambda x: x.secondary_transportation_mode_code.str.upper(),
            fuel_cost_per_mmbtu=lambda x: x.fuel_cost_per_mmbtu / 100,
            fuel_group_code=lambda x: x.fuel_group_code.str.lower().str.replace(' ', '_'),
            fuel_type_code_pudl=lambda x: pudl.helpers.cleanstrings_series(
                x.energy_source_code, pc.energy_source_eia_simple_map),
            fuel_group_code_simple=lambda x: pudl.helpers.cleanstrings_series(
                x.fuel_group_code, pc.fuel_group_eia923_simple_map),
            contract_expiration_month=lambda x: x.contract_expiration_date.apply(
                lambda y: y[:-2] if y != '' else y)).
        assign(
            # These assignments are separate b/c they exp_month is altered 2x
            contract_expiration_month=lambda x: x.contract_expiration_month.apply(
                lambda y: y if y != '' and int(y) <= 12 else ''),
            contract_expiration_year=lambda x: x.contract_expiration_date.apply(
                lambda y: '20' + y[-2:] if y != '' else y)).
        # Now that we will create our own real date field, so chuck this one.
        drop('contract_expiration_date', axis=1).
        pipe(pudl.helpers.convert_to_date,
             date_col='contract_expiration_date',
             year_col='contract_expiration_year',
             month_col='contract_expiration_month').
        pipe(pudl.helpers.convert_to_date).
        pipe(pudl.helpers.cleanstrings,
             ['natural_gas_transport_code',
              'natural_gas_delivery_contract_type_code'],
             [{'firm': ['F'], 'interruptible': ['I']},
              {'firm': ['F'], 'interruptible': ['I']}],
             unmapped='')
    )

    # Remove known to be invalid mercury content values. Almost all of these
    # occur in the 2012 data. Real values should be <0.25ppm.
    bad_hg_idx = frc_df.mercury_content_ppm >= 7.0
    frc_df.loc[bad_hg_idx, "mercury_content_ppm"] = np.nan

    eia923_transformed_dfs['fuel_receipts_costs_eia923'] = frc_df

    return eia923_transformed_dfs


def transform(eia923_raw_dfs, eia923_tables=pc.eia923_pudl_tables):
    """Transforms all the EIA 923 tables.

    Args:
        eia923_raw_dfs (dict): a dictionary of tab names (keys) and DataFrames
            (values). Generated from `pudl.extract.eia923.extract()`.
        eia923_tables (tuple): A tuple containing the EIA923 tables that can be
            pulled into PUDL.

    Returns:
        dict: A dictionary of DataFrame with table names as keys and
        :class:`pandas.DataFrame` objects as values, where the contents of the
        DataFrames correspond to cleaned and normalized PUDL database tables,
        ready for loading.

    """
    eia923_transform_functions = {
        'generation_fuel_eia923': generation_fuel,
        'boiler_fuel_eia923': boiler_fuel,
        'generation_eia923': generation,
        'coalmine_eia923': coalmine,
        'fuel_receipts_costs_eia923': fuel_receipts_costs
    }
    eia923_transformed_dfs = {}

    if not eia923_raw_dfs:
        logger.info("No raw EIA 923 DataFrames found. "
                    "Not transforming EIA 923.")
        return eia923_transformed_dfs

    for table in eia923_transform_functions.keys():
        if table in eia923_tables:
            logger.info(
                f"Transforming raw EIA 923 DataFrames for {table} "
                f"concatenated across all years.")
            eia923_transform_functions[table](eia923_raw_dfs,
                                              eia923_transformed_dfs)
        else:
            logger.info(f'Not transforming {table}')
    return eia923_transformed_dfs
