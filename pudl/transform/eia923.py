
"""Routines specific to cleaning up EIA Form 923 data."""

import pandas as pd
import numpy as np
import pudl
import pudl.constants as pc

###############################################################################
###############################################################################
# HELPER FUNCTIONS
###############################################################################
###############################################################################


def _yearly_to_monthly_records(df, md):
    """
    Convert an EIA 923 record with 12 months of data into 12 monthly records.

    Much of the data reported in EIA 923 is monthly, but all 12 months worth of
    data is reported in a single record, with one field for each of the 12
    months.  This function converts these annualized composite records into a
    set of 12 monthly records containing the same information, by parsing the
    field names for months, and adding a month field.  Non - time series data
    is retained in the same format.

    Args:
        df(pandas.DataFrame): A pandas DataFrame containing the annual
            data to be converted into monthly records.
        md(dict): a dictionary with the integers 1-12 as keys, and the
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
    """
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
    """
    cmi_df = cmi_df.copy()
    # Map mine type codes, which have changed over the years, to a few
    # canonical values:
    cmi_df['mine_type_code'].replace(
        {'[pP]': 'P', 'U/S': 'US', 'S/U': 'SU', 'Su': 'S'},
        inplace=True, regex=True)
    cmi_df['state'] = cmi_df.state.replace(pc.coalmine_country_eia923)

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

    # Transform coalmine names to a canonical form to reduce duplicates:
    # No leading or trailing whitespace:
    cmi_df['mine_name'] = pudl.helpers.strip_lower(cmi_df,
                                                   columns=['mine_name'])
    # remove all internal non-alphanumeric characters:
    cmi_df['mine_name'] = \
        cmi_df['mine_name'].replace('[^a-zA-Z0-9 -]', '', regex=True)

    # Homogenize the data type that we're finding inside the county_id_fips
    # field (ugh, Excel sheets!).  Mostly these are integers or NA values,
    # but for imported coal, there are both 'IMP' and 'IM' string values.
    # This should change it all to strings that are compatible with the
    # Integer type within postgresql.
    cmi_df['county_id_fips'].replace('[a-zA-Z]+',
                                     value=np.nan,
                                     regex=True,
                                     inplace=True)
    cmi_df['county_id_fips'] = cmi_df['county_id_fips'].astype(float)
    return cmi_df

###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################


def plants(eia923_dfs, eia923_transformed_dfs):
    """
    Clean the plants_eia923 table.

    Much of the static plant information is reported repeatedly, and scattered
    across several different pages of EIA 923. The data frame which this
    function uses is assembled from those many different pages, and passed in
    via the same dictionary of dataframes that all the other ingest functions
    use for uniformity.

    Args:
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the EIA
            923 form, as reported in the Excel spreadsheets they distribute.

        eia923_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
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

    plant_info_df['reporting_frequency'] = \
        plant_info_df.reporting_frequency.replace({'M': 'monthly',
                                                   'A': 'annual'})
    # Since this is a plain Yes/No variable -- just make it a real sa.Boolean.
    plant_info_df.combined_heat_power.replace({'N': False, 'Y': True},
                                              inplace=True)

    # Get rid of excessive whitespace introduced to break long lines (ugh)
    plant_info_df.census_region = \
        plant_info_df.census_region.str.replace(' ', '')
    plant_info_df.drop_duplicates(subset='plant_id_eia')

    plant_info_df['plant_id_eia'] = plant_info_df['plant_id_eia'].astype(int)

    eia923_transformed_dfs['plants_eia923'] = plant_info_df

    return eia923_transformed_dfs


def generation_fuel(eia923_dfs, eia923_transformed_dfs):
    """
    Transform the generation_fuel_eia923 table.

    Args:
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # This needs to be a copy of what we're passed in so we can edit it.
    gf_df = eia923_dfs['generation_fuel'].copy()

    # Drop fields we're not inserting into the generation_fuel_eia923 table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name',
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

    gf_df['fuel_type_code_pudl'] = \
        pudl.helpers.cleanstrings(
            gf_df.fuel_type,
            pc.fuel_type_eia923_gen_fuel_simple_map)

    # Convert Year/Month columns into a single Date column...
    gf_df = pudl.helpers.convert_to_date(gf_df)

    eia923_transformed_dfs['generation_fuel_eia923'] = gf_df

    return eia923_transformed_dfs


# def boilers(eia923_dfs, eia923_transformed_dfs):
#    """
#    Transform the boiler_eia923 table.
#
#    Args:
#        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
#            dictionary of DataFrame objects corresponds to a page from the
#            EIA923 form, as reported in the Excel spreadsheets they distribute.
#        eia923_transformed_dfs (dictionary of DataFrames)
#
#    Returns: transformed dataframe.
#    """
#    boilers_df = eia923_dfs['boiler_fuel'].copy()
#    # Populate 'boilers_eia923' table
#    boiler_cols = ['plant_id_eia',
#                   'boiler_id',
#                   'prime_mover_code']
#    boilers_df = boilers_df[boiler_cols]
#
#    # drop null values from foreign key fields
#    boilers_df.dropna(subset=['boiler_id', 'plant_id_eia'], inplace=True)
#
#    # We need to cast the boiler_id column as type str because sometimes
#    # it is heterogeneous int/str which make drop_duplicates fail.
#    boilers_df['boiler_id'] = boilers_df['boiler_id'].astype(str)
#    boilers_df = boilers_df.drop_duplicates(
#        subset=['plant_id_eia', 'boiler_id'])
#
#    eia923_transformed_dfs['boilers_eia923'] = boilers_df
#
#    return eia923_transformed_dfs


def boiler_fuel(eia923_dfs, eia923_transformed_dfs):
    """
    Transform the boiler_fuel_eia923 table.

    Args:
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    bf_df = eia923_dfs['boiler_fuel'].copy()

    # Drop fields we're not inserting into the boiler_fuel_eia923 table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name',
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
    bf_df['fuel_type_code_pudl'] = \
        pudl.helpers.cleanstrings(
            bf_df.fuel_type_code,
            pc.fuel_type_eia923_boiler_fuel_simple_map)
    # Replace the EIA923 NA value ('.') with a real NA value.
    bf_df = pudl.helpers.fix_eia_na(bf_df)

    # Convert Year/Month columns into a single Date column...
    bf_df = pudl.helpers.convert_to_date(bf_df)

    eia923_transformed_dfs['boiler_fuel_eia923'] = bf_df

    return eia923_transformed_dfs


def generation(eia923_dfs, eia923_transformed_dfs):
    """
    Transform the generation_eia923 table.

    Args:
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # This needs to be a copy of what we're passed in so we can edit it.
    generation_df = eia923_dfs['generator'].copy()

    # Drop fields we're not inserting into the generation_eia923_fuel_eia923
    # table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name',
                    'operator_name',
                    'operator_id',
                    'plant_state',
                    'census_region',
                    'nerc_region',
                    'naics_code',
                    'eia_sector',
                    'sector_name',
                    'net_generation_mwh_year_to_date']

    generation_df.dropna(subset=['generator_id'], inplace=True)

    generation_df.drop(cols_to_drop, axis=1, inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    generation_df = _yearly_to_monthly_records(
        generation_df, pc.month_dict_eia923)
    # Replace the EIA923 NA value ('.') with a real NA value.
    generation_df = pudl.helpers.fix_eia_na(generation_df)

    # Convert Year/Month columns into a single Date column...
    generation_df = pudl.helpers.convert_to_date(generation_df)

    eia923_transformed_dfs['generation_eia923'] = generation_df

    return eia923_transformed_dfs


# def generators(eia923_dfs, eia923_transformed_dfs):
#    """
#    Transform the generators_eia923 table.
#
#    Args:
#        eia860_dfs (dictionary of pandas.DataFrame): Each entry in this
#            dictionary of DataFrame objects corresponds to a page from the
#            EIA860 form, as reported in the Excel spreadsheets they distribute.
#        eia860_transformed_dfs (dictionary of DataFrames)
#
#    Returns: transformed dataframe.
#    """
#    # Populating the 'generators_eia923' table
#    generators_df = eia923_dfs['generator'].copy()
#    generator_cols = ['plant_id_eia',
#                      'generator_id',
#                      'prime_mover_code']
#    generators_df = generators_df[generator_cols]
#
    # drop null values from foreign key fields
#    generators_df.dropna(subset=['generator_id', 'plant_id_eia'], inplace=True)

    # We need to cast the generator_id column as type str because sometimes
    # it is heterogeneous int/str which make drop_duplicates fail.
#    generators_df['generator_id'] = generators_df['generator_id'].astype(str)
#    generators_df = generators_df.drop_duplicates(
#        subset=['plant_id_eia', 'generator_id'])

#    eia923_transformed_dfs['generators_eia923'] = generators_df

#    return eia923_transformed_dfs


def coalmine(eia923_dfs, eia923_transformed_dfs):
    """
    Transform the coalmine_eia923 table.

    Args:
    -----
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
            eia923_transformed_dfs (dictionary of DataFrames)

    Returns:
    --------
        Transformed dataframe.

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
    cmi_with_msha = \
        cmi_with_msha.drop_duplicates(subset=['mine_id_msha', ])
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
    cmi_df.index.name = 'id'
    # then make the id index a column for simpler transferability
    cmi_df = cmi_df.reset_index()

    eia923_transformed_dfs['coalmine_eia923'] = cmi_df

    return eia923_transformed_dfs


def fuel_reciepts_costs(eia923_dfs, eia923_transformed_dfs):
    """
    Transform the fuel_receipts_costs_eia923.

    Fuel cost is reported in cents per mmbtu. Convert cents to dollars.

    Args:
    -----
        eia923_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA923 form, as reported in the Excel spreadsheets they distribute.
        eia923_transformed_dfs (dictionary of DataFrames)

    Returns:
    --------
        Transformed dataframe.

    """
    frc_df = eia923_dfs['fuel_receipts_costs'].copy()

    # Drop fields we're not inserting into the fuel_receipts_costs_eia923
    # table.
    cols_to_drop = ['plant_name',
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

    cmi_df = eia923_transformed_dfs['coalmine_eia923'].copy()

    # In order for the merge to work, we need to get the county_id_fips field
    # back into ready-to-dump form... so it matches the types of the
    # county_id_fips field that we are going to be merging on in the frc_df.
    cmi_df = cmi_df.rename(columns={'id': 'mine_id_pudl'})

    # This type/naming cleanup function is separated out so that we can be
    # sure it is applied exactly the same both when the coalmine_eia923 table
    # is populated, and here (since we need them to be identical for the
    # following merge)
    frc_df = _coalmine_cleanup(frc_df)
    frc_df = frc_df.merge(cmi_df, how='left',
                          on=['mine_name',
                              'state',
                              'mine_id_msha',
                              'mine_type_code',
                              'county_id_fips'])

    frc_df.drop(cols_to_drop, axis=1, inplace=True)

    # Replace the EIA923 NA value ('.') with a real NA value.
    frc_df = pudl.helpers.fix_eia_na(frc_df)

    # These come in ALL CAPS from EIA...
    frc_df['supplier_name'] = pudl.helpers.strip_lower(
        frc_df, columns=['supplier_name'])

    # Standardize case on transportaion codes -- all upper case!
    frc_df['primary_transportation_mode_code'] = \
        frc_df['primary_transportation_mode_code'].str.upper()
    frc_df['secondary_transportation_mode_code'] = \
        frc_df['secondary_transportation_mode_code'].str.upper()

    frc_df = pudl.helpers.fix_int_na(frc_df,
                                     columns=['contract_expiration_date', ])
    # Convert the 3-4 digit (MYY|MMYY) date of contract expiration to
    # two fields MM and YYYY for easier analysis later.
    frc_df['contract_expiration_month'] = \
        frc_df.contract_expiration_date. \
        apply(lambda x: x[:-2] if x != '' else x)
    # This gets rid of some bad data that's not in (MYY|MMYY) format.
    frc_df['contract_expiration_month'] = \
        frc_df.contract_expiration_month. \
        apply(lambda x: x if x != '' and int(x) <= 12 else '')

    frc_df['contract_expiration_year'] = frc_df.contract_expiration_date. \
        apply(lambda x: '20' + x[-2:] if x != '' else x)

    frc_df = frc_df.drop('contract_expiration_date', axis=1)
    frc_df = pudl.helpers.convert_to_date(
        frc_df,
        date_col='contract_expiration_date',
        year_col='contract_expiration_year',
        month_col='contract_expiration_month'
    )

    frc_df = pudl.helpers.convert_to_date(frc_df)

    frc_df['fuel_cost_per_mmbtu'] = frc_df['fuel_cost_per_mmbtu'] / 100

    frc_df['fuel_group_code'] = (frc_df.fuel_group_code.
                                 str.lower().
                                 str.replace(' ', '_'))

    frc_df['fuel_type_code_pudl'] = \
        pudl.helpers.cleanstrings(frc_df.energy_source_code,
                                  pc.energy_source_eia_simple_map)
    frc_df['fuel_group_code_simple'] = \
        pudl.helpers.cleanstrings(frc_df.fuel_group_code,
                                  pc.fuel_group_eia923_simple_map)

    frc_df['natural_gas_transport_code'] = pudl.helpers.cleanstrings(
        frc_df.natural_gas_transport_code,
        {'firm': ['F'], 'interruptible': ['I']}
    )
    frc_df['natural_gas_delivery_contract_type_code'] = \
        pudl.helpers.cleanstrings(
            frc_df.natural_gas_delivery_contract_type_code,
            {'firm': ['F'], 'interruptible': ['I']}
    )
    eia923_transformed_dfs['fuel_receipts_costs_eia923'] = frc_df

    return eia923_transformed_dfs


def transform(eia923_raw_dfs,
              eia923_tables=pc.eia923_pudl_tables,
              verbose=True):
    """Transform all EIA 923 tables."""
    eia923_transform_functions = {
        # ****'generation_fuel_eia923': generation_fuel,
        # 'boilers_eia923': boilers,
        'boiler_fuel_eia923': boiler_fuel,
        'generation_eia923': generation,
        # 'generators_eia923': generators,
        'coalmine_eia923': coalmine,
        'fuel_receipts_costs_eia923': fuel_reciepts_costs
    }
    eia923_transformed_dfs = {}

    if not eia923_raw_dfs:
        if verbose:
            print('Not transforming EIA 923.')
        return eia923_transformed_dfs

    if verbose:
        print("Transforming tables from EIA 923:")
    for table in eia923_transform_functions:
        if table in eia923_tables:
            if verbose:
                print("    {}...".format(table))
            eia923_transform_functions[table](eia923_raw_dfs,
                                              eia923_transformed_dfs)
    return eia923_transformed_dfs
