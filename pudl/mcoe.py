"""A module with functions to aid generating MCOE."""

# Useful high-level external modules.
import numpy as np
import pandas as pd
import sqlalchemy as sa
import matplotlib.pyplot as plt
import itertools
import random

# Our own code...
from pudl import pudl, ferc1, eia923, settings, constants
from pudl import models, models_ferc1, models_eia923
from pudl import clean_eia923, clean_ferc1, clean_pudl
from pudl import outputs, analysis

# Data Pulls


def generation_pull_eia923(pudl_engine):
    # Convert the generation_eia923 table into a dataframe
    g9 = analysis.simple_select('generation_eia923', pudl_engine)

    # Get yearly net generation by plant_id, year and generator_id
    g9_summed = analysis.yearly_sum_eia(g9, 'net_generation_mwh')
    g9_summed.reset_index(inplace=True)

    return(g9_summed)


def generators_pull_eia860(pudl_engine):
    # Convert the generators_eia860 table into a dataframe
    g8 = analysis.simple_select('generators_eia860', pudl_engine)

    # create a generator table with mostly just the energy source
    # for use in manipulating values by energy source
    g8_es = g8[['plant_id_eia', 'plant_id_pudl', 'plant_name', 'operator_name',
                'state', 'generator_id', 'energy_source_1',
                'nameplate_capacity_mw', 'report_year']]
    g8_es = g8_es.rename(columns={'energy_source_1': 'energy_source'})
    g8_es.drop_duplicates(
        ['plant_id_eia', 'generator_id', 'report_year'], inplace=True)

    # create a consolidated energy source column
    # copy the energy course column and replace the content with consolidated
    # values
    g8_es['energy_source_cons'] = g8_es.energy_source.copy()
    # use the energy source map in constants to replace each energy source with
    for energy_source_cons in constants.energy_source_map.keys():
        for energy_source in constants.energy_source_map[energy_source_cons]:
            g8_es.loc[g8_es.energy_source == energy_source,
                      'energy_source_cons'] = energy_source_cons

    # Create a count of the types of energy sources
    g8_es_count = g8_es[['plant_id_eia', 'energy_source_cons', 'report_year']
                        ].drop_duplicates().groupby(['plant_id_eia',
                                                     'report_year']).count()
    g8_es_count.reset_index(inplace=True)
    g8_es_count = g8_es_count.rename(
        columns={'energy_source_cons': 'energy_source_count'})
    g8_es = g8_es.merge(g8_es_count, how='left', on=[
                        'plant_id_eia', 'report_year'])

    # Cheating to duplicate 2011 EIA860 energy srouce for 2010 and 2009:
    g8_es_2010 = g8_es.loc[g8_es['report_year'] == 2011].copy()
    g8_es_2010.report_year.replace([2011], [2010], inplace=True)
    g8_es_2009 = g8_es.loc[g8_es['report_year'] == 2011].copy()
    g8_es_2009.report_year.replace([2011], [2009], inplace=True)
    g8_es_2016 = g8_es.loc[g8_es['report_year'] == 2015].copy()
    g8_es_2016.report_year.replace([2015], [2016], inplace=True)
    # Append 2009 and 2010
    g8_es = g8_es.append([g8_es_2009, g8_es_2010, g8_es_2016])

    return(g8_es)


def fuel_reciept_costs_pull_eia923(pudl_engine):
    # Convert the fuel_receipts_costs_eia923 table into a dataframe
    frc9 = analysis.simple_select('fuel_receipts_costs_eia923', pudl_engine)
    frc9['fuel_cost'] = (frc9['fuel_quantity'] *
                         frc9['average_heat_content'] *
                         frc9['fuel_cost_per_mmbtu'])
    frc9['mmbtu'] = (frc9['fuel_quantity'] * frc9['average_heat_content'])

    frc9['energy_source_cons'] = frc9.energy_source.copy()

    for energy_source_cons in constants.energy_source_map.keys():
        for energy_source in constants.energy_source_map[energy_source_cons]:
            frc9.loc[frc9.energy_source == energy_source,
                     'energy_source_cons'] = energy_source_cons

    # Get yearly fuel cost by plant_id, year and energy_source
    frc9_summed = analysis.yearly_sum_eia(frc9, 'fuel_cost', columns=[
                                          'plant_id_eia', 'plant_id_pudl',
                                          'report_year', 'energy_source_cons'])
    frc9_summed = frc9_summed.reset_index()
    frc9_mmbtu_summed = analysis.yearly_sum_eia(
        frc9, 'mmbtu', columns=['plant_id_eia', 'report_year',
                                'energy_source_cons'])
    frc9_mmbtu_summed = frc9_mmbtu_summed.reset_index()
    frc9_summed = frc9_mmbtu_summed.merge(frc9_summed)
    frc9_summed['fuel_cost_per_mmbtu_average'] = (
        frc9_summed.fuel_cost / frc9_summed.mmbtu)

    # Get yearly fuel cost by plant_id and year
    # For use in calculating fuel cost for plants with one main energy soure
    frc9_summed_plant = analysis.yearly_sum_eia(
        frc9, 'fuel_cost', columns=['plant_id_eia', 'report_year'])
    frc9_summed_plant = frc9_summed_plant.reset_index()
    frc9_mmbtu_summed_plant = analysis.yearly_sum_eia(
        frc9, 'mmbtu', columns=['plant_id_eia', 'report_year'])
    frc9_mmbtu_summed_plant = frc9_mmbtu_summed_plant.reset_index()
    frc9_summed_plant = frc9_mmbtu_summed_plant.merge(frc9_summed_plant)
    frc9_summed_plant['fuel_cost_per_mmbtu_average'] = (
        frc9_summed_plant.fuel_cost / frc9_summed_plant.mmbtu)

    return(frc9_summed, frc9_summed_plant)


def boiler_generator_pull_eia860(pudl_engine):
    # Convert the boiler_generator_assn_eia860 table into a dataframe
    bga8 = analysis.simple_select('boiler_generator_assn_eia860', pudl_engine)
    bga8.drop(['id', 'operator_id'], axis=1, inplace=True)
    bga8.drop_duplicates(['plant_id_eia', 'boiler_id',
                          'generator_id'], inplace=True)

    return(bga8)


def boiler_fuel_pull_eia923(pudl_engine):
    # Convert the boiler_fuel_eia923 table into a dataframe
    bf9 = analysis.simple_select('boiler_fuel_eia923', pudl_engine)
    bf9['fuel_consumed_mmbtu'] = bf9['fuel_qty_consumed'] * \
        bf9['fuel_mmbtu_per_unit']
    # Get yearly fuel consumed by plant_id, year and boiler_id
    bf9_summed = analysis.yearly_sum_eia(bf9, 'fuel_consumed_mmbtu', columns=[
                                         'plant_id_eia', 'plant_id_pudl',
                                         'report_year', 'boiler_id'])
    bf9_summed.reset_index(inplace=True)
    # Get yearly fuel consumed by plant_id, year and boiler_id
    bf9_plant_summed = analysis.yearly_sum_eia(
        bf9, 'fuel_consumed_mmbtu', columns=['plant_id_eia', 'plant_id_pudl',
                                             'report_year'])
    bf9_plant_summed.reset_index(inplace=True)

    return(bf9_summed, bf9_plant_summed)

# MCOE Components


def heat_rate(bga8, g9_summed, bf9_summed,
              bf9_plant_summed, pudl_engine, id_col='plant_id_eia'):
    """
    Generate hate rates for all EIA generators.
    """
    # This section pulls the unassociated generators
    gens = gens_with_bga(bga8, g9_summed)
    # Get a list of generators from plants with unassociated plants
    # gens_unassn_plants = gens[gens['plant_assn'] == False
    gens_unassn_plants = gens[gens['complete_assn'] == False]

    # Sum the yearly net generation for these plants
    gup_gb = gens_unassn_plants.groupby(by=[id_col, 'report_year'])
    gens_unassn_plants_summed = gup_gb.agg({'net_generation_mwh': np.sum})
    gens_unassn_plants_summed.reset_index(inplace=True)

    # Pull in mmbtu
    unassn_plants = gens_unassn_plants_summed.merge(
        bf9_plant_summed, on=[id_col, 'report_year'])
    # calculate heat rate by plant
    unassn_plants['heat_rate_mmbtu_mwh'] = \
        unassn_plants['fuel_consumed_mmbtu'] / \
        unassn_plants['net_generation_mwh']

    # Merge these plant level heat heat rates with the unassociated generators
    # Assign heat rates to generators across the plants with unassociated
    # generators
    heat_rate_unassn = gens_unassn_plants.merge(unassn_plants[[
                                                'plant_id_eia', 'plant_id_pudl',
                                                'report_year',
                                                'heat_rate_mmbtu_mwh']],
                                                on=['plant_id_eia', 'plant_id_pudl',
                                                    'report_year'],
                                                how='left')
    heat_rate_unassn.drop(
        ['boiler_id', 'boiler_generator_assn'], axis=1, inplace=True)

    # This section generates heat rate from the generators of
    # the plants that have any generators that are included in
    # the boiler generator association table (860)
    generation_w_boilers = g9_summed.merge(
        bga8, how='left', on=['plant_id_eia', 'plant_id_pudl', 'generator_id'])

    # get net generation per boiler
    gb1 = generation_w_boilers.groupby(
        by=[id_col, 'report_year', 'boiler_id'])
    generation_w_boilers_summed = gb1.agg({'net_generation_mwh': np.sum})
    generation_w_boilers_summed.reset_index(inplace=True)
    generation_w_boilers_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler'},
        inplace=True)

    # get the generation per boiler/generator combo
    gb2 = generation_w_boilers.groupby(
        by=[id_col, 'report_year', 'boiler_id', 'generator_id'])
    generation_w_bg_summed = gb2.agg({'net_generation_mwh': np.sum})
    generation_w_bg_summed.reset_index(inplace=True)
    generation_w_bg_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler_gen'},
        inplace=True)

    # squish them together
    generation_w_boilers_summed = \
        generation_w_boilers_summed.merge(generation_w_bg_summed,
                                          how='left',
                                          on=[id_col,
                                              'report_year',
                                              'boiler_id'])

    bg = bf9_summed.merge(bga8, how='left', on=[
                          'plant_id_eia', 'plant_id_pudl', 'boiler_id'])
    bg = bg.merge(generation_w_boilers_summed, how='left', on=[
                  id_col, 'report_year', 'boiler_id', 'generator_id'])

    # Use the proportion of the generation of each generator to allot mmBTU
    bg['proportion_of_gen_by_boil_gen'] = \
        bg['net_generation_mwh_boiler_gen'] / bg['net_generation_mwh_boiler']
    bg['fuel_consumed_mmbtu_per_gen'] = \
        bg['proportion_of_gen_by_boil_gen'] * bg['fuel_consumed_mmbtu']

    # Get yearly fuel_consumed_mmbtu by plant_id, year and generator_id
    bg_gb = bg.groupby(by=[id_col,
                           'report_year',
                           'generator_id'])
    bg_summed = bg_gb.agg({'fuel_consumed_mmbtu_per_gen': np.sum})
    bg_summed.reset_index(inplace=True)

    # Calculate heat rate
    heat_rate = bg_summed.merge(g9_summed, how='left', on=[
                                id_col, 'report_year', 'generator_id'])
    heat_rate['heat_rate_mmbtu_mwh'] = \
        heat_rate['fuel_consumed_mmbtu_per_gen'] / \
        heat_rate['net_generation_mwh']

    # Importing the plant association tag to filter out the
    # generators that are a part of plants that aren't in the bga table
    heat_rate = heat_rate.merge(gens[['plant_id_eia', 'plant_id_pudl',
                                      'report_year',
                                      'generator_id',
                                      'complete_assn',
                                      'plant_assn']],
                                on=['plant_id_eia', 'plant_id_pudl',
                                    'report_year',
                                    'generator_id'])
    heat_rate_assn = heat_rate[heat_rate['complete_assn'] == True]

    # Append heat rates for associated and unassociated
    heat_rate_all = heat_rate_assn.append(heat_rate_unassn)
    heat_rate_all.sort_values(
        by=[id_col, 'report_year', 'generator_id'], inplace=True)

    if 'plant_id_pudl_x' in heat_rate_all.columns:
        heat_rate_all.drop('plant_id_pudl_x', axis=1, inplace=True)
    if 'plant_id_pudl_y' in heat_rate_all.columns:
        heat_rate_all.drop('plant_id_pudl_y', axis=1, inplace=True)

    return(heat_rate_all)


def fuel_cost(g8_es, g9_summed, frc9_summed, frc9_summed_plant, heat_rate):
    one_fuel_plants = g8_es[g8_es['energy_source_count'] == 1]
    multi_fuel_plants = g8_es[g8_es['energy_source_count'] > 1]

    # one fuel plants
    net_gen_one_fuel = g9_summed.merge(one_fuel_plants, how='left', on=[
        'plant_id_eia', 'generator_id', 'report_year'])
    net_gen_one_fuel.dropna(inplace=True)

    # Merge this net_gen table with frc9_summed_plant to have
    # fuel_cost_per_mmbtu_total associated with generators
    fuel_cost_per_mmbtu_one_fuel = net_gen_one_fuel.merge(frc9_summed_plant,
                                                          how='left',
                                                          on=['plant_id_eia',
                                                              'report_year'])

    fuel_cost_one_fuel = fuel_cost_per_mmbtu_one_fuel.\
        merge(heat_rate[['plant_id_eia',
                         'report_year',
                         'generator_id',
                         'net_generation_mwh',
                         'heat_rate_mmbtu_mwh']],
              on=['plant_id_eia',
                  'report_year',
                  'generator_id',
                  'net_generation_mwh'])

    # Calculate fuel cost per mwh using average fuel cost given year, plant,
    # fuel type; divide by generator-specific heat rate
    fuel_cost_one_fuel['fuel_cost_per_mwh'] = \
        (fuel_cost_one_fuel['fuel_cost_per_mmbtu_average']
         * fuel_cost_one_fuel['heat_rate_mmbtu_mwh'])

    # mutli fuel plants
    net_gen_multi_fuel = g9_summed.merge(multi_fuel_plants, how='left', on=[
        'plant_id_eia', 'generator_id', 'report_year'])
    net_gen_multi_fuel.dropna(inplace=True)

    # Merge this net_gen table with frc9_summed to have
    # fuel_cost_per_mmbtu_total associated with generators
    fuel_cost_per_mmbtu_multi_fuel = net_gen_multi_fuel.\
        merge(frc9_summed,
              how='left',
              on=['plant_id_eia',
                  'report_year',
                  'energy_source_cons'])

    fuel_cost_multi_fuel = fuel_cost_per_mmbtu_multi_fuel.\
        merge(heat_rate[['plant_id_eia',
                         'report_year',
                         'generator_id',
                         'net_generation_mwh',
                         'heat_rate_mmbtu_mwh']],
              on=['plant_id_eia',
                  'report_year',
                  'generator_id',
                  'net_generation_mwh'])

    # Calculate fuel cost per mwh using average fuel cost given year, plant,
    # fuel type; divide by generator-specific heat rate
    fuel_cost_multi_fuel['fuel_cost_per_mwh'] = \
        (fuel_cost_multi_fuel['fuel_cost_per_mmbtu_average'] *
         fuel_cost_multi_fuel['heat_rate_mmbtu_mwh'])

    # Squish them together!
    fuel_cost = fuel_cost_one_fuel.append(fuel_cost_multi_fuel)

    return(fuel_cost)
