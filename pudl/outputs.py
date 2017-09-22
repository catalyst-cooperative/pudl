"""A library of useful tabular outputs compiled from multiple data sources."""

# Useful high-level external modules.
import sqlalchemy as sa
import pandas as pd

# Our own code...
from pudl import pudl, ferc1, eia923, settings, constants
from pudl import models, models_ferc1, models_eia923
from pudl import clean_eia923, clean_ferc1, clean_pudl

##############################################################################
##############################################################################
# A collection of tabular compilations whose core information comes from a
# single table in the PUDL database, with minimal calculations taking place
# to generate them, but additional IDs, names, and plant or utility linked
# information joined in, allowing extensive filtering to be done downstream.
#
# naming convention: tablename_returntype
#
# EIA 923 table abbreviations:
# - gf = generation_fuel
# - bf = boiler_fuel
# - frc = fuel_receipts_costs
# - g = generation
##############################################################################
##############################################################################


def plants_steam_ferc1_df(pudl_engine):
    """
    Select and join some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    TODO: Check whether this includes all of the steam_ferc1 fields...

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        steam_df: a pandas dataframe.
    """
    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    steam_ferc1_select = sa.sql.select([
        pt['plants_steam_ferc1'].c.report_year,
        pt['utilities_ferc'].c.respondent_id,
        pt['utilities_ferc'].c.util_id_pudl,
        pt['utilities_ferc'].c.respondent_name,
        pt['plants_ferc'].c.plant_id_pudl,
        pt['plants_steam_ferc1'].c.plant_name,
        pt['plants_steam_ferc1'].c.total_capacity_mw,
        pt['plants_steam_ferc1'].c.year_constructed,
        pt['plants_steam_ferc1'].c.year_installed,
        pt['plants_steam_ferc1'].c.peak_demand_mw,
        pt['plants_steam_ferc1'].c.water_limited_mw,
        pt['plants_steam_ferc1'].c.not_water_limited_mw,
        pt['plants_steam_ferc1'].c.plant_hours,
        pt['plants_steam_ferc1'].c.net_generation_mwh,
        pt['plants_steam_ferc1'].c.expns_operations,
        pt['plants_steam_ferc1'].c.expns_fuel,
        pt['plants_steam_ferc1'].c.expns_coolants,
        pt['plants_steam_ferc1'].c.expns_steam,
        pt['plants_steam_ferc1'].c.expns_steam_other,
        pt['plants_steam_ferc1'].c.expns_transfer,
        pt['plants_steam_ferc1'].c.expns_electric,
        pt['plants_steam_ferc1'].c.expns_misc_power,
        pt['plants_steam_ferc1'].c.expns_rents,
        pt['plants_steam_ferc1'].c.expns_allowances,
        pt['plants_steam_ferc1'].c.expns_engineering,
        pt['plants_steam_ferc1'].c.expns_structures,
        pt['plants_steam_ferc1'].c.expns_boiler,
        pt['plants_steam_ferc1'].c.expns_plants,
        pt['plants_steam_ferc1'].c.expns_misc_steam,
        pt['plants_steam_ferc1'].c.expns_production_total,
        pt['plants_steam_ferc1'].c.expns_per_mwh]).\
        where(sa.sql.and_(
            pt['utilities_ferc'].c.respondent_id ==
            pt['plants_steam_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.respondent_id ==
            pt['plants_steam_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.plant_name ==
            pt['plants_steam_ferc1'].c.plant_name,
        ))

    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    return(steam_df)


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
    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    # Build a SELECT statement that gives us information from several different
    # tables that are relevant to FERC Fuel.
    fuel_ferc1_select = sa.sql.select([
        pt['fuel_ferc1'].c.report_year,
        pt['utilities_ferc'].c.respondent_id,
        pt['utilities_ferc'].c.respondent_name,
        pt['utilities_ferc'].c.util_id_pudl,
        pt['plants_ferc'].c.plant_id_pudl,
        pt['fuel_ferc1'].c.plant_name,
        pt['fuel_ferc1'].c.fuel,
        pt['fuel_ferc1'].c.fuel_qty_burned,
        pt['fuel_ferc1'].c.fuel_avg_mmbtu_per_unit,
        pt['fuel_ferc1'].c.fuel_cost_per_unit_burned,
        pt['fuel_ferc1'].c.fuel_cost_per_unit_delivered,
        pt['fuel_ferc1'].c.fuel_cost_per_mmbtu,
        pt['fuel_ferc1'].c.fuel_cost_per_mwh,
        pt['fuel_ferc1'].c.fuel_mmbtu_per_mwh]).\
        where(sa.sql.and_(
            pt['utilities_ferc'].c.respondent_id ==
            pt['fuel_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.respondent_id ==
            pt['fuel_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.plant_name ==
            pt['fuel_ferc1'].c.plant_name))

    # Pull the data from the DB into a DataFrame
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

    return(fuel_df)


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
    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    gf_eia923_select = sa.sql.select([
        pt['utilities_eia'].c.operator_id,
        pt['utilities_eia'].c.operator_name,
        pt['plants_eia'].c.plant_id_pudl,
        pt['plants_eia'].c.plant_name,
        pt['generation_fuel_eia923'].c.plant_id,
        pt['generation_fuel_eia923'].c.report_date,
        pt['generation_fuel_eia923'].c.aer_fuel_category,
        pt['generation_fuel_eia923'].c.fuel_consumed_total_mmbtu,
        pt['generation_fuel_eia923'].c.net_generation_mwh]).\
        where(sa.sql.and_(
            pt['plants_eia'].c.plant_id ==
            pt['generation_fuel_eia923'].c.plant_id,
            pt['util_plant_assn'].c.plant_id ==
            pt['plants_eia'].c.plant_id_pudl,
            pt['util_plant_assn'].c.utility_id ==
            pt['utilities_eia'].c.util_id_pudl
        ))

    return(pd.read_sql(gf_eia923_select, pudl_engine))


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
    frc_df = pd.read_sql(
        '''SELECT * FROM fuel_receipts_costs_eia923''', pudl_engine)
    # Need a year column to merge with EIA860 data which is annual.
    frc_df['year'] = pd.to_datetime(frc_df['report_date']).dt.year

    # Need to re-integrate the MSHA coalmine info:
    cmi_df = pd.read_sql('''SELECT * FROM coalmine_info_eia923''', pudl_engine)

    # Contains the one-to-one mapping of EIA plants to their operators, but
    # we only have the 860 data integrated for 2011 forward right now.
    plants_eia860 = pd.read_sql(
        '''SELECT * FROM plants_eia860''', pudl_engine)
    plants_eia860 = plants_eia860[['year', 'plant_id', 'operator_id']]

    # For the PUDL Utility & Plant IDs, as well as utility & plant names:
    utils_eia = pd.read_sql('''SELECT * FROM utilities_eia''', pudl_engine)
    plants_eia = pd.read_sql('''SELECT * FROM plants_eia''', pudl_engine)

    out_df = pd.merge(frc_df, cmi_df,
                      how='left',
                      left_on='coalmine_id',
                      right_on='id')
    out_df = pd.merge(out_df, plants_eia860,
                      how='left',
                      on=['plant_id', 'year'])
    out_df = pd.merge(out_df, utils_eia, how='left', on='operator_id')
    out_df = pd.merge(out_df, plants_eia, how='left', on='plant_id')

    # Sadly b/c we're depending on 860 for Operator/Plant mapping,
    # we only get 2011 and later
    out_df = out_df.dropna(subset=['operator_id', 'operator_name'])
    cols_to_drop = ['fuel_receipt_id',
                    'coalmine_id',
                    'id',
                    'year']
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


def generators_eia860_df(pudl_engine):
    """
    Pull all fields reported in the generators_eia860 table. Merge in other
    useful fields including the latitude & longitude of the plant that the
    generators are part of, canonical plant & operator names and the PUDL
    IDs of the plant and operator, for merging with other PUDL data sources.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
    Returns:
        A pandas dataframe.
    """
    # Almost all the info we need will come from here.
    gens_eia860 = pd.read_sql(
        '''SELECT * FROM generators_eia860''', pudl_engine)
    # Canonical sources for these fields are elsewhere.
    gens_eia860 = gens_eia860.drop(['operator_id',
                                    'operator_name',
                                    'plant_name'], axis=1)

    # To get the Lat/Lon coordinates, and plant/utility ID mapping:
    plants_eia860 = pd.read_sql('''
        SELECT year, plant_id, operator_id, latitude, longitude
        FROM plants_eia860''', pudl_engine)
    out_df = pd.merge(gens_eia860, plants_eia860,
                      how='left', on=['year', 'plant_id'])

    # Get the utility names and PUDL utility IDs
    utils_eia = pd.read_sql('''SELECT * FROM utilities_eia''', pudl_engine)
    out_df = pd.merge(out_df, utils_eia, on='operator_id')

    # Get the plant names and PUDL plant IDs
    plants_eia = pd.read_sql('''SELECT * FROM plants_eia''', pudl_engine)
    out_df = pd.merge(out_df, plants_eia, how='left', on='plant_id')

    # Drop a few extraneous fields...
    cols_to_drop = ['id', ]
    out_df = out_df.drop(cols_to_drop, axis=1)

    # Re-arrange the dataframe to be more readable:
    out_df = out_df[[
        'year',
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
        'operating_month',
        'operating_year',
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
        'planned_uprate_month',
        'planned_uprate_year',
        'planned_net_summer_capacity_derate',
        'planned_net_winter_capacity_derate',
        'planned_derate_month',
        'planned_derate_year',
        'planned_new_prime_mover',
        'planned_energy_source_1',
        'planned_repower_month',
        'planned_repower_year',
        'other_planned_modifications',
        'other_modifications_month',
        'other_modifications_year',
        'planned_retirement_month',
        'planned_retirement_year',
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
        'month_uprate_derate_completed',
        'year_uprate_derate_completed',
        'associated_combined_heat_power',
        'effective_month',
        'effective_year',
        'current_month',
        'current_year',
        'summer_estimated_capability',
        'winter_estimated_capability',
        'operating_switch',
        'previously_canceled',
        'retirement_month',
        'retirement_year',
    ]]

    return(out_df)
