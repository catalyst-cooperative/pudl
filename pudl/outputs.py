"""
A library of useful tabular outputs compiled from multiple data sources.

Many of our potential users are comfortable using spreadsheets, not databases,
so we are creating a collection of tabular outputs that containt the most
useful core information from the PUDL DB, including additional keys and human
readable names for the objects (utilities, plants, generators) being described
in the table.

These tabular outputs can be joined with each other using those keys, and used
as a data source within Microsoft Excel, Access, R Studio, or other data
analysis packages that folks may be familiar with.  They aren't meant to
completely replicate all the data and relationships contained within the full
PUDL database, but should serve as a generally usable set of data products
that contain some calculated values, and allow a variety of interesting
questions to be addressed (e.g. about the marginal cost of electricity on a
generator by generatory basis).

Over time this library will hopefully grow and acccumulate more data and more
post-processing, post-analysis outputs as well.
"""

# Useful high-level external modules.
import sqlalchemy as sa
import pandas as pd

# Need the models so we can grab table structures.
from pudl import models

# Shorthand for easier table referecnes:
pt = models.PUDLBase.metadata.tables


###############################################################################
###############################################################################
#   Output Helper Functions
###############################################################################
###############################################################################
def organize_cols(df, cols):
    """
    Organize columns into key ID & name fields & alphabetical data columns.

    For readability, it's nice to group a few key columns at the beginning
    of the dataframe (report_year or report_data, plant_id, etc.) and then
    put all the rest of the data columns in alphabetical order.

    Args:
        df: The DataFrame to be re-organized.
        cols: The columns to put first, in their desired output ordering.
    """
    key_cols = df[cols].copy()
    data_cols = df.drop(cols, axis=1)
    data_cols = df[df.columns.sort_values()]
    out_df = pd.merge(key_cols, data_cols)
    return(out_df)


def add_id_cols(df):
    """
    Merge in additional commonly useful columns, based on dataframe contents.

    We constantly merge in additional Utility, Plant, and Generator columns
    from elsewhere in the database for output. This function automates that
    process, based on the columns which are present in the input dataframe.

    Within EIA at least, a generator_id + plant_id + report_year implies a
    given plant, and a plant_id + a report_year implies an operator_id. So
    when those lower level ids are included in the table, we can
    automatically merge in the associated lower level entities.

    Having all of these IDs and names integrated into each of these tables
    allows them to be easily joined with other tables so additional columns
    can be pulled in if needed.

    Utilities:
        - if 'operator_id' pull in:
            - 'operator_name' (from utilities_eia)
            - 'util_id_pudl' (from utilities_eia)
        - if 'respondent_id' pull in:
            - 'respondent_name'

    Plants:
        - if 'plant_id' pull in:
            - 'plant_name' (from plants_eia)
            - 'plant_id_pudl' (from plants_eia)
        - if 'plant_id' and 'report_year' or 'report_date' also pull in:
            - 'operator_id' (from plants_eia860)
            - 'operator_name' (from utilities_eia)
            - 'util_id_pudl' (from utilities_eia)

    Generators:
        - if 'generator_id' and 'plant_id' pull in:
            - 'plant_name' (from generators_eia860)
            - 'plant_id_pudl' (from plants_eia)
        - if 'plant_id' and ''
    """
    pass


###############################################################################
###############################################################################
#   Cross datasource output (e.g. EIA923 + EIA860, PUDL specific IDs)
###############################################################################
###############################################################################
def plants_utils_eia(pudl_engine):
    """
    Create a dataframe of plant and utility IDs and names from EIA.

    Returns a pandas dataframe with the following columns:
    - year (in which data was reported)
    - plant_name (from EIA860)
    - plant_id (from EIA860)
    - plant_id_pudl
    - operator_id (from EIA860)
    - operator_name (frome EIA860)
    - util_id_pudl

    EIA 860 data has only been integrated back to 2011, so this information
    isn't available any further back.
    """
    # Contains the one-to-one mapping of EIA plants to their operators, but
    # we only have the 860 data integrated for 2011 forward right now.
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([
        plants_eia860_tbl.c.report_year,
        plants_eia860_tbl.c.plant_id,
        plants_eia860_tbl.c.plant_name,
        plants_eia860_tbl.c.operator_id,
    ])
    plants_eia860 = pd.read_sql(plants_eia860_select, pudl_engine)

    utils_eia860_tbl = pt['utilities_eia860']
    utils_eia860_select = sa.sql.select([
        utils_eia860_tbl.c.report_year,
        utils_eia860_tbl.c.operator_id,
        utils_eia860_tbl.c.operator_name,
    ])
    utils_eia860 = pd.read_sql(utils_eia860_select, pudl_engine)

    # Pull the canonical EIA860 operator name into the output DataFrame:
    out_df = pd.merge(plants_eia860, utils_eia860,
                      how='left', on=['report_year', 'operator_id', ])

    # Get the PUDL Utility ID
    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.operator_id,
        utils_eia_tbl.c.util_id_pudl,
    ])
    utils_eia = pd.read_sql(utils_eia_select,  pudl_engine)
    out_df = pd.merge(out_df, utils_eia, how='left', on='operator_id')

    # Get the PUDL Plant ID
    plants_eia_tbl = pt['plants_eia']
    plants_eia_select = sa.sql.select([
        plants_eia_tbl.c.plant_id,
        plants_eia_tbl.c.plant_id_pudl,
    ])
    plants_eia = pd.read_sql(plants_eia_select, pudl_engine)
    out_df = pd.merge(out_df, plants_eia, how='left', on='plant_id')

    out_df = out_df.dropna()
    out_df.plant_id_pudl = out_df.plant_id_pudl.astype(int)
    out_df.util_id_pudl = out_df.util_id_pudl.astype(int)

    return(out_df)


def plants_utils_ferc1(pudl_engine):
    """Build a dataframe of useful FERC Plant & Utility information."""
    utils_ferc_tbl = pt['utilities_ferc']
    utils_ferc_select = sa.sql.select([utils_ferc_tbl, ])
    utils_ferc = pd.read_sql(utils_ferc_select, pudl_engine)

    plants_ferc_tbl = pt['plants_ferc']
    plants_ferc_select = sa.sql.select([plants_ferc_tbl, ])
    plants_ferc = pd.read_sql(plants_ferc_select, pudl_engine)

    out_df = pd.merge(plants_ferc, utils_ferc, on='respondent_id')
    return(out_df)


###############################################################################
###############################################################################
#   EIA 860 Outputs
###############################################################################
###############################################################################
def utilities_eia860(pudl_engine):
    """Pull all fields from the EIA860 Utilities table."""
    utils_eia860_tbl = pt['utilities_eia860']
    utils_eia860_select = sa.sql.select([utils_eia860_tbl])
    utils_eia860_df = pd.read_sql(utils_eia860_select, pudl_engine)

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.operator_id,
        utils_eia_tbl.c.util_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select,  pudl_engine)

    out_df = pd.merge(utils_eia860_df, utils_eia_df,
                      how='left', on=['operator_id', ])

    out_df = out_df.drop(['id'], axis=1)
    first_cols = [
        'report_year',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ]

    out_df = organize_cols(out_df, first_cols)
    return(out_df)


def plants_eia860(pudl_engine):
    """Pull all fields from the EIA860 Plants table."""
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([plants_eia860_tbl])
    plants_eia860_df = pd.read_sql(plants_eia860_select, pudl_engine)

    plants_eia_tbl = pt['plants_eia']
    plants_eia_select = sa.sql.select([
        plants_eia_tbl.c.plant_id,
        plants_eia_tbl.c.plant_id_pudl,
    ])
    plants_eia_df = pd.read_sql(plants_eia_select,  pudl_engine)

    out_df = pd.merge(plants_eia860_df, plants_eia_df,
                      how='left', on=['plant_id', ])

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.operator_id,
        utils_eia_tbl.c.util_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select,  pudl_engine)

    out_df = pd.merge(out_df, utils_eia_df,
                      how='left', on=['operator_id', ])

    out_df = out_df.drop(['id'], axis=1)
    first_cols = [
        'report_year',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
    ]

    out_df = organize_cols(out_df, first_cols)
    return(out_df)


def generators_eia860(pudl_engine):
    """
    Pull all fields reported in the generators_eia860 table.

    Merge in other useful fields including the latitude & longitude of the
    plant that the generators are part of, canonical plant & operator names and
    the PUDL IDs of the plant and operator, for merging with other PUDL data
    sources.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        A pandas dataframe.
    """
    # Almost all the info we need will come from here.
    gens_eia860_tbl = pt['generators_eia860']
    gens_eia860_select = sa.sql.select([gens_eia860_tbl, ])
    gens_eia860 = pd.read_sql(gens_eia860_select, pudl_engine)

    # Canonical sources for these fields are elsewhere. We will merge them in.
    gens_eia860 = gens_eia860.drop(['operator_id',
                                    'operator_name',
                                    'plant_name'], axis=1)

    # To get the Lat/Lon coordinates, and plant/utility ID mapping:
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([
        plants_eia860_tbl.c.report_year,
        plants_eia860_tbl.c.plant_id,
        plants_eia860_tbl.c.operator_id,
        plants_eia860_tbl.c.latitude,
        plants_eia860_tbl.c.longitude,
    ])
    plants_eia860 = pd.read_sql(plants_eia860_select, pudl_engine)

    out_df = pd.merge(gens_eia860, plants_eia860,
                      how='left', on=['report_year', 'plant_id'])

    # For the PUDL Utility & Plant IDs, as well as utility & plant names:
    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([utils_eia_tbl, ])
    utils_eia = pd.read_sql(utils_eia_select,  pudl_engine)
    out_df = pd.merge(out_df, utils_eia, on='operator_id')

    plants_eia_tbl = pt['plants_eia']
    plants_eia_select = sa.sql.select([plants_eia_tbl, ])
    plants_eia = pd.read_sql(plants_eia_select, pudl_engine)
    out_df = pd.merge(out_df, plants_eia, how='left', on='plant_id')

    # Drop a few extraneous fields...
    cols_to_drop = ['id', ]
    out_df = out_df.drop(cols_to_drop, axis=1)

    first_cols = [
        'report_year',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    return(out_df)


def ownership_eia860(pudl_engine):
    """
    Pull a useful set of fields related to ownership_eia860 table.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        out_df: a pandas dataframe.
    """
    o_eia860_tbl = pt['ownership_eia860']
    o_eia860_select = sa.sql.select([o_eia860_tbl, ])
    o_df = pd.read_sql(o_eia860_select, pudl_engine)

    pu_eia = plants_utils_eia(pudl_engine)
    pu_eia = pu_eia[['plant_id', 'plant_id_pudl', 'util_id_pudl',
                     'report_year']]

    out_df = pd.merge(o_df, pu_eia, how='left', on=['report_year', 'plant_id'])

    out_df = out_df.drop(['id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'generator_id',
        'ownership_id',
    ])

    first_cols = [
        'report_year',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
        'ownership_id',
        'owner_name',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    return(out_df)


###############################################################################
###############################################################################
#   EIA 923 Outputs
###############################################################################
###############################################################################
def generation_fuel_eia923(pudl_engine):
    """
    Pull a useful set of fields related to generation_fuel_eia923 table.

    This function grabs fields from the generation_fuel_eia923 PUDL DB table
    and joins them to utility names, and plant and utility PUDL IDs for easier
    readability, and integration with other PUDL tables.

    TODO: Check whether this includes all of the useful gf_eia923 fields...
    TODO: What EIA860 fields should we pull in on a plant-by-plant basis?

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        gf_df: a pandas dataframe.
    """
    gf_eia923_tbl = pt['generation_fuel_eia923']
    gf_eia923_select = sa.sql.select([gf_eia923_tbl, ])
    gf_df = pd.read_sql(gf_eia923_select, pudl_engine)

    pu_eia = plants_utils_eia(pudl_engine)

    # Need a temporary year column to merge with EIA860 data which is annual.
    gf_df['report_year'] = pd.to_datetime(gf_df['report_date']).dt.year
    out_df = pd.merge(gf_df, pu_eia,
                      how='left', on=['plant_id', 'report_year'])
    out_df = out_df.drop(['report_year', 'id'], axis=1)
    out_df = out_df.dropna(subset=[
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ])

    first_cols = ['report_date',
                  'plant_id',
                  'plant_id_pudl',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name', ]

    out_df = organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id'] = out_df.plant_id.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return(out_df)


def fuel_receipts_costs_eia923(pudl_engine):
    """
    Pull a useful fields related to fuel_receipts_costs_eia923 table.

    This function grabs fields from the fuel_receipts_costs_eia923 PUDL DB
    table and joins them to utility names, and plant and utility PUDL IDs for
    easier readability, and integration with other PUDL tables.

    It also calculates some additional columns which we commonly want, that are
    derived from the basic data. These include the total fuel cost per delivery
    and the total fuel heat content per delivery.

    For now, the fuel_group column is also being re-categorized to use our
    ad-hoc standard categories: ['coal', 'oil', 'gas'] rather than ['Coal',
    'Petroleum', 'Natural Gas']. Really need to do this re-organization
    upstream on ingest.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        A pandas dataframe.
    """
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt['fuel_receipts_costs_eia923']
    frc_select = sa.sql.select([frc_tbl, ])
    frc_df = pd.read_sql(frc_select, pudl_engine)

    # Need a year column to merge with EIA860 data which is annual.
    frc_df['report_year'] = pd.to_datetime(frc_df['report_date']).dt.year

    # Need to re-integrate the MSHA coalmine info:
    cmi_tbl = pt['coalmine_info_eia923']
    cmi_select = sa.sql.select([cmi_tbl, ])
    cmi_df = pd.read_sql(cmi_select, pudl_engine)

    out_df = pd.merge(frc_df, cmi_df,
                      how='left',
                      left_on='coalmine_id',
                      right_on='id')
    pu_eia = plants_utils_eia(pudl_engine)
    out_df = pd.merge(out_df, pu_eia,
                      how='left', on=['plant_id', 'report_year'])

    # Sadly b/c we're depending on 860 for Operator/Plant mapping,
    # we only get 2011 and later
    out_df = out_df.dropna(subset=['operator_id', 'operator_name'])
    cols_to_drop = ['fuel_receipt_id',
                    'coalmine_id',
                    'id',
                    'report_year']
    out_df = out_df.drop(cols_to_drop, axis=1)

    # Calculate a few totals that are commonly needed:
    out_df['total_heat_content_mmbtu'] = \
        out_df['average_heat_content'] * out_df['fuel_quantity']
    out_df['total_fuel_cost'] = \
        out_df['total_heat_content_mmbtu'] * out_df['fuel_cost_per_mmbtu']

    # There are a couple of bad rows with no specified fuel.
    out_df = out_df.dropna(subset=['fuel_group'])
    # Add a simplified fuel category (this should really happen at ingest)
    out_df['fuel_pudl'] = out_df.fuel_group.replace(
        to_replace=['Petroleum', 'Natural Gas', 'Other Gas', 'Coal',
                    'Petroleum Coke'],
        value=['oil', 'gas', 'gas', 'coal', 'petcoke'])

    first_cols = ['report_date',
                  'plant_id',
                  'plant_id_pudl',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name', ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id'] = out_df.plant_id.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return(out_df)


def boiler_fuel_eia923(pudl_engine):
    """
    Pull a useful set of fields related to boiler_fuel_eia923 table.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        out_df: a pandas dataframe.
    """
    bf_eia923_tbl = pt['boiler_fuel_eia923']
    bf_eia923_select = sa.sql.select([bf_eia923_tbl, ])
    bf_df = pd.read_sql(bf_eia923_select, pudl_engine)

    pu_eia = plants_utils_eia(pudl_engine)

    # Need a temporary year column to merge with EIA860 data which is annual.
    bf_df['report_year'] = pd.to_datetime(bf_df['report_date']).dt.year

    out_df = pd.merge(bf_df, pu_eia, how='left', on=['plant_id',
                                                     'report_year'])

    # It's often useful to know total heat content consumed by each boiler:
    out_df['total_heat_content_mmbtu'] = out_df['fuel_qty_consumed'] * \
        out_df['fuel_mmbtu_per_unit']

    out_df = out_df.drop(['report_year', 'id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'boiler_id',
    ])

    first_cols = [
        'report_date',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'boiler_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return(out_df)


def generation_eia923(pudl_engine):
    """
    Pull a useful set of fields related to generation_eia923 table.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        out_df: a pandas dataframe.
    """
    g_eia923_tbl = pt['generation_eia923']
    g_eia923_select = sa.sql.select([g_eia923_tbl, ])
    g_df = pd.read_sql(g_eia923_select, pudl_engine)

    pu_eia = plants_utils_eia(pudl_engine)

    # Need a temporary year column to merge with EIA860 data which is annual.
    g_df['report_year'] = pd.to_datetime(g_df['report_date']).dt.year

    out_df = pd.merge(g_df, pu_eia, how='left', on=['plant_id', 'report_year'])
    out_df = out_df.drop(['report_year', 'id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'generator_id',
    ])

    first_cols = [
        'report_date',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return(out_df)


###############################################################################
###############################################################################
#   FERC Form 1 Outputs
###############################################################################
###############################################################################
def plants_steam_ferc1(pudl_engine):
    """
    Select and join some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        steam_df: a pandas dataframe.
    """
    steam_ferc1_tbl = pt['plants_steam_ferc1']
    steam_ferc1_select = sa.sql.select([steam_ferc1_tbl, ])
    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    pu_ferc = plants_utils_ferc1(pudl_engine)

    out_df = pd.merge(steam_df, pu_ferc, on=['respondent_id', 'plant_name'])

    first_cols = [
        'report_year',
        'respondent_id',
        'util_id_pudl',
        'respondent_name',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = organize_cols(out_df, first_cols)

    return(out_df)


def fuel_ferc1(pudl_engine):
    """
    Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant,
    allowing integration with other PUDL tables.

    Also calculates the total heat content consumed for each fuel, and the
    total cost for each fuel. Total cost is calculated in two different ways,
    on the basis of fuel units consumed (e.g. tons of coal, mcf of gas) and
    on the basis of heat content consumed. In theory these should give the
    same value for total cost, but this is not always the case.

    TODO: Check whether this includes all of the fuel_ferc1 fields...

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        fuel_df: a pandas dataframe.
    """
    fuel_ferc1_tbl = pt['fuel_ferc1']
    fuel_ferc1_select = sa.sql.select([fuel_ferc1_tbl, ])
    fuel_df = pd.read_sql(fuel_ferc1_select, pudl_engine)

    # We have two different ways of assessing the total cost of fuel given cost
    # per unit delivered and cost per mmbtu. They *should* be the same, but we
    # know they aren't always. Calculate both so we can compare both.
    fuel_df['fuel_consumed_total_mmbtu'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_avg_mmbtu_per_unit']
    fuel_df['fuel_consumed_total_cost_mmbtu'] = \
        fuel_df['fuel_cost_per_mmbtu'] * fuel_df['fuel_consumed_total_mmbtu']
    fuel_df['fuel_consumed_total_cost_unit'] = \
        fuel_df['fuel_cost_per_unit_burned'] * fuel_df['fuel_qty_burned']

    pu_ferc = plants_utils_ferc1(pudl_engine)

    out_df = pd.merge(fuel_df, pu_ferc, on=['respondent_id', 'plant_name'])
    out_df = out_df.drop('id', axis=1)

    first_cols = [
        'report_year',
        'respondent_id',
        'util_id_pudl',
        'respondent_name',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = organize_cols(out_df, first_cols)

    return(out_df)
