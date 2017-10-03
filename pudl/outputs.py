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


def plants_utils_eia_df(pudl_engine):
    """Create a dataframe of plant and utility IDs and names from EIA.

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


def gf_eia923_df(pudl_engine):
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

    pu_eia = plants_utils_eia_df(pudl_engine)

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

    out_df = out_df[[
        'report_date',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'nuclear_unit_id',
        'fuel_type',
        'aer_fuel_type',
        'aer_fuel_category',
        'prime_mover',
        'fuel_consumed_total',
        'fuel_consumed_for_electricity',
        'fuel_mmbtu_per_unit',
        'fuel_consumed_total_mmbtu',
        'fuel_consumed_for_electricity_mmbtu',
        'net_generation_mwh',
    ]]

    # Clean up the types of a few columns...
    out_df['plant_id'] = out_df.plant_id.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return(out_df)


def frc_eia923_df(pudl_engine):
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
    pu_eia = plants_utils_eia_df(pudl_engine)
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

    # Clean up the types of a few columns...
    out_df['plant_id'] = out_df.plant_id.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    # Re-arrange the columns for easier readability:
    out_df = out_df[[
        'report_date',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'contract_type',
        'contract_expiration_date',
        'energy_source',
        'fuel_group',
        'fuel_pudl',
        'supplier',
        'fuel_quantity',
        'average_heat_content',
        'average_sulfur_content',
        'average_ash_content',
        'average_mercury_content',
        'fuel_cost_per_mmbtu',
        'primary_transportation_mode',
        'secondary_transportation_mode',
        'natural_gas_transport',
        'coalmine_msha_id',
        'coalmine_name',
        'coalmine_type',
        'coalmine_state',
        'coalmine_county',
        'total_heat_content_mmbtu',
        'total_fuel_cost',
    ]]

    return(out_df)


def bf_eia923_df(pudl_engine):
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

    pu_eia = plants_utils_eia_df(pudl_engine)

    # Need a temporary year column to merge with EIA860 data which is annual.
    bf_df['report_year'] = pd.to_datetime(bf_df['report_date']).dt.year

    out_df = pd.merge(bf_df, pu_eia, how='left', on=['plant_id',
                                                     'report_year'])
    out_df = out_df.drop(['report_year', 'id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ])

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return(out_df)


def g_eia923_df(pudl_engine):
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

    pu_eia = plants_utils_eia_df(pudl_engine)

    # Need a temporary year column to merge with EIA860 data which is annual.
    g_df['report_year'] = pd.to_datetime(g_df['report_date']).dt.year

    out_df = pd.merge(g_df, pu_eia, how='left', on=['plant_id', 'report_year'])
    out_df = out_df.drop(['report_year', 'id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ])

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return(out_df)


def o_eia860_df(pudl_engine):
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

    pu_eia = plants_utils_eia_df(pudl_engine)
    pu_eia = pu_eia[['plant_id', 'plant_id_pudl', 'util_id_pudl',
                     'report_year']]

    out_df = pd.merge(o_df, pu_eia, how='left', on=['report_year', 'plant_id'])

    out_df = out_df.drop(['id'], axis=1)

    return(out_df)


def gens_eia860_df(pudl_engine):
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

    # Re-arrange the dataframe to be more readable:
    out_df = out_df[[
        'report_year',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'plant_id',
        'plant_id_pudl',
        'plant_name',
        'generator_id',
        'state',
        'county',
        'latitude',
        'longitude',
        'prime_mover',
        'unit_code',
        'status',
        'ownership',
        'duct_burners',
        'nameplate_capacity_mw',
        'summer_capacity_mw',
        'winter_capacity_mw',
        'operating_date',
        'energy_source_1',
        'energy_source_2',
        'energy_source_3',
        'energy_source_4',
        'energy_source_5',
        'energy_source_6',
        'multiple_fuels',
        'deliver_power_transgrid',
        'syncronized_transmission_grid',
        'turbines',
        'cogenerator',
        'sector_name',
        'sector',
        'topping_bottoming',
        'planned_modifications',
        'planned_net_summer_capacity_uprate',
        'planned_net_winter_capacity_uprate',
        'planned_uprate_date',
        'planned_net_summer_capacity_derate',
        'planned_net_winter_capacity_derate',
        'planned_derate_date',
        'planned_new_prime_mover',
        'planned_energy_source_1',
        'planned_repower_date',
        'other_planned_modifications',
        'other_modifications_date',
        'planned_retirement_date',
        'solid_fuel_gasification',
        'pulverized_coal_tech',
        'fluidized_bed_tech',
        'subcritical_tech',
        'supercritical_tech',
        'ultrasupercritical_tech',
        'carbon_capture',
        'startup_source_1',
        'startup_source_2',
        'startup_source_3',
        'startup_source_4',
        'technology',
        'turbines_inverters_hydrokinetics',
        'time_cold_shutdown_full_load',
        'stoker_tech',
        'other_combustion_tech',
        'planned_new_nameplate_capacity_mw',
        'cofire_fuels',
        'switch_oil_gas',
        'heat_bypass_recovery',
        'rto_iso_lmp_node',
        'rto_iso_location_wholesale_reporting',
        'nameplate_power_factor',
        'minimum_load_mw',
        'uprate_derate_during_year',
        'uprate_derate_completed_date',
        'associated_combined_heat_power',
        'original_planned_operating_date',
        'current_planned_operating_date',
        'summer_estimated_capability',
        'winter_estimated_capability',
        'operating_switch',
        'previously_canceled',
        'retirement_date',
    ]]

    return(out_df)


def plants_utils_ferc_df(pudl_engine):
    """Build a dataframe of useful FERC Plant & Utility information."""
    utils_ferc_tbl = pt['utilities_ferc']
    utils_ferc_select = sa.sql.select([utils_ferc_tbl, ])
    utils_ferc = pd.read_sql(utils_ferc_select, pudl_engine)

    plants_ferc_tbl = pt['plants_ferc']
    plants_ferc_select = sa.sql.select([plants_ferc_tbl, ])
    plants_ferc = pd.read_sql(plants_ferc_select, pudl_engine)

    out_df = pd.merge(plants_ferc, utils_ferc, on='respondent_id')
    return(out_df)


def plants_steam_ferc1_df(pudl_engine):
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

    pu_ferc = plants_utils_ferc_df(pudl_engine)

    out_df = pd.merge(steam_df, pu_ferc, on=['respondent_id', 'plant_name'])
    out_df = out_df[[
        'report_year',
        'respondent_id',
        'respondent_name',
        'util_id_pudl',
        'plant_name',
        'plant_id_pudl',
        'plant_kind',
        'type_const',
        'year_constructed',
        'year_installed',
        'total_capacity_mw',
        'peak_demand_mw',
        'plant_hours',
        'plant_capability_mw',
        'water_limited_mw',
        'not_water_limited_mw',
        'avg_num_employees',
        'net_generation_mwh',
        'cost_land',
        'cost_structure',
        'cost_equipment',
        'cost_of_plant_total',
        'cost_per_mw',
        'expns_operations',
        'expns_fuel',
        'expns_coolants',
        'expns_steam',
        'expns_steam_other',
        'expns_transfer',
        'expns_electric',
        'expns_misc_power',
        'expns_rents',
        'expns_allowances',
        'expns_engineering',
        'expns_structures',
        'expns_boiler',
        'expns_plants',
        'expns_misc_steam',
        'expns_production_total',
        'expns_per_mwh',
        'asset_retire_cost',
    ]]
    return(out_df)


def fuel_ferc1_df(pudl_engine):
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

    pu_ferc = plants_utils_ferc_df(pudl_engine)

    out_df = pd.merge(fuel_df, pu_ferc, on=['respondent_id', 'plant_name'])
    out_df = out_df.drop('id', axis=1)

    out_df = out_df[[
        'report_year',
        'respondent_id',
        'respondent_name',
        'util_id_pudl',
        'plant_name',
        'plant_id_pudl',
        'fuel',
        'fuel_unit',
        'fuel_qty_burned',
        'fuel_avg_mmbtu_per_unit',
        'fuel_cost_per_unit_burned',
        'fuel_cost_per_unit_delivered',
        'fuel_cost_per_mmbtu',
        'fuel_cost_per_mwh',
        'fuel_mmbtu_per_mwh',
        'fuel_consumed_total_mmbtu',
        'fuel_cost_per_mmbtu',
        'fuel_cost_per_unit_burned',
    ]]

    return(out_df)
