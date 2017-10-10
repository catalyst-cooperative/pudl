"""A module with functions to aid generating MCOE."""

from pudl import constants, analysis

# Data Pulls


def generation_pull_eia923(pudl_engine):
    # Convert the generation_eia923 table into a dataframe
    g9 = analysis.simple_select_with_pudl_plant_id(
        'generation_eia923', pudl_engine)

    # Get yearly net generation by plant_id, year and generator_id
    g9_summed = analysis.yearly_sum_eia(g9, 'net_generation_mwh')
    g9_summed.reset_index(inplace=True)

    return(g9_summed)


def generators_pull_eia860(pudl_engine):
    # Convert the generators_eia860 table into a dataframe
    g8 = analysis.simple_select_with_pudl_plant_id(
        'generators_eia860', pudl_engine)

    # create a generator table with mostly just the energy source
    # for use in manipulating values by energy source
    g8_es = g8[['plant_id_eia', 'generator_id',
                'energy_source_1', 'report_year']]
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


def fuel_reciepts_costs_pull_eia923(pudl_engine):
    # Convert the fuel_receipts_costs_eia923 table into a dataframe
    frc9 = analysis.simple_select_with_pudl_plant_id(
        'fuel_receipts_costs_eia923', pudl_engine)
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
                                          'plant_id_eia', 'report_year',
                                          'energy_source_cons'])
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
    bga8 = analysis.simple_select_with_pudl_plant_id(
        'boiler_generator_assn_eia860', pudl_engine)
    bga8.drop(['id', 'operator_id'], axis=1, inplace=True)
    bga8.drop_duplicates(['plant_id_eia', 'boiler_id',
                          'generator_id'], inplace=True)

    return(bga8)


def boiler_fuel_pull_eia923(pudl_engine):
    # Convert the boiler_fuel_eia923 table into a dataframe
    bf9 = analysis.simple_select_with_pudl_plant_id(
        'boiler_fuel_eia923', pudl_engine)
    bf9['fuel_consumed_mmbtu'] = bf9['fuel_qty_consumed'] * \
        bf9['fuel_mmbtu_per_unit']
    # Get yearly fuel consumed by plant_id, year and boiler_id
    bf9_summed = analysis.yearly_sum_eia(bf9, 'fuel_consumed_mmbtu', columns=[
                                         'plant_id_eia', 'report_year',
                                         'boiler_id'])
    bf9_summed.reset_index(inplace=True)
    # Get yearly fuel consumed by plant_id, year and boiler_id
    bf9_plant_summed = analysis.yearly_sum_eia(
        bf9, 'fuel_consumed_mmbtu', columns=['plant_id_eia', 'report_year'])
    bf9_plant_summed.reset_index(inplace=True)

    return(bf9_summed, bf9_plant_summed)

# MCOE Components


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
