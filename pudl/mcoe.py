"""A module with functions to aid generating MCOE."""

from pudl import constants, analysis

# Data Pulls


def generation_pull_eia923(pudl_engine):
    """
    Pull an annualized version of the EIA 923 generation table.

    Adds plant_pudl_id to the dataframe, and sums up net_generation_mwh per
    EIA plant and per year. Also renames plant_id column to plant_id_eia.
    """
    # Convert the generation_eia923 table into a dataframe
    g9 = analysis.simple_select_with_pudl_plant_id(
        'generation_eia923', pudl_engine)

    # Get yearly net generation by plant_id, year and generator_id
    g9_summed = analysis.yearly_sum_eia(g9, 'net_generation_mwh')
    g9_summed.reset_index(inplace=True)

    return(g9_summed)


def generators_pull_eia860(pudl_engine):
    """
    Compile a table of EIA generators by year and primary energy source.

    Pulls a few columns from the EIA 860 generators table, including:
     - report_year
     - plant_id (renamed to plant_id_eia)
     - generator_id
     - energy_source_1 (renamed to energy_source)

    Consolidates the codes in energy_source to just coal, oil, and gas as
    we've been using those three broad categories across many datasets, and
    adds that category in a new column called energy_source_cons.

    Because we only have EIA 860 data for 2011-2015, and we have EIA 923 data
    for 2009-2016, we're currently copying the 2011 EIA 860 information about
    generator energy_source into 2009 and 2010, and the 2015 data into 2016.
    These will need to be fixed as we integrate additional EIA860 data, but
    shouldn't be too wrong for now -- those things don't change much.

    Comments from Zane:
     - The replacement of many energy_source codes with just a few should be
       doable using the cleanstrings() function that we developed first in
       support of the FERC data, rather than needing to do a double-loop.
     - This simplified fuel taxonomy is going to get used in a lot of places.
       Should probably be moved far upstream in the data cleaning -- always
       adding our own simple column(s), and leaving the original codes intact.
     - Rather than hard-coding the 2011, 2010, and 2016 years here, we could
       automatically detect which years we DO have for EIA 923, that we DON'T
       have for EIA 860, and automatically extend the fields forward and
       backward using the earliest and latest years in EIA 860.  Then we
       wouldn't have to worry about updating this function when new data is
       integrated into the system.
     - In the process of counting how many energy_source_cons values there are
       for each plant, is it appropriate to be doing it on a year-by-year
       basis? For the plant-years that change status over time, are we
       sure that we're doing all of the subsequent analysis on the basis of
       plant-year, rather than plant_id alone?
    """
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
    """
    Compile dataframes of annual EIA 923 fuel receipts and costs for MCOE.

    Additional columns are calculated, containing the total MMBTU and total
    cost for each fuel delivery. The energy_source associated with the fuel
    delivery is consolidated into gas, oil, or coal, codes which we've been
    using across the datasets.

    Two separate data frames are returned, one is grouped by plant_id_eia,
    report_year, and consolidated energy source, the other is the same,
    except it's not grouped by energy_source, and has an additional column
    that contains the average cost per mmbtu for the listed plants. This is
    for use with the single-fuel plants, in calculating their fuel_cost per
    generator.

    Notes from Zane:
    - Should functionalize consolidation of energy_source, since it's being
      done in more than one place.
    - Addition of plant_id_pudl and the mmbtu and fuel cost calculations are
      also already being done in the outputs module. We should just do those
      calculations in one place.
    - Should be able to simultaneously annualize multiple data columns at the
      same time, as they're all being grouped by report_year and plant_id_eia.
    - What's the difference between frc9_summed and frc9_summed_plant? Seems
      like the frc9_summed is grouped by plant, year, and energy source, while
      frc9_summed_plant is just year and plant?
    - Handing back two dataframes feels a little awkward, and the additional
      step of further grouping and calculating cost per mmbtu across the entire
      plant feels like it might be done over in the fuel_cost function
      instead, so it's clear what two different things are being done, side by
      side, in the single vs. multi-fuel plant cases.
    - Also slightly dangerous to calculate the fuel cost per mmbtu column for
      all of the plants, when I don't think we want folks to use those numbers
      for the multi-fuel plants (since it'd be a mix of e.g. coal and gas costs
      per mmbtu).
    """
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
    """
    Pull the boiler generator associations from EIA 860.

    Adds plant_id_pudl and drops operator_id and id (an internal automatically
    incrementing surrogate key), and keeps only unique combinations of plant,
    boiler, and generator -- without preserving any changes over time.

    Comments from Zane:
     - Under what circumstances were we finding duplicate records that need to
       be dropped? Shouldn't the plant/boiler/generator records be unique?
     - Is this a table that also contains years? Is that the duplication that's
       being eliminated?  If so, does it ever result in weird duplications of
       a mapping? I.e. is there ever a case where in some years a given boiler
       generator mapping applied, and in others a different one applied, but
       both of those mappings appear in the dataframe which this function
       returns, since they're not identical... but they would be year specific.
    """
    # Convert the boiler_generator_assn_eia860 table into a dataframe
    bga8 = analysis.simple_select_with_pudl_plant_id(
        'boiler_generator_assn_eia860', pudl_engine)
    bga8.drop(['id', 'operator_id'], axis=1, inplace=True)
    bga8.drop_duplicates(['plant_id_eia', 'boiler_id',
                          'generator_id'], inplace=True)

    return(bga8)


def boiler_fuel_pull_eia923(pudl_engine):
    """
    Pull annualized boiler fuel consumption data from EIA 923 for MCOE.

    Calculates total heat content of all fuel consumed by each boiler, and then
    sums that on an annual basis by plant and boiler ID. Also creates a second
    similar dataframe that is not broken out by boiler_id, and instead only
    sums the fuel consumption by plant and year.

    Comments from Zane:
     - Guessing that the 2 dataframe thing here is the same as for FRC above,
       with the plant-only number being destined for use in the single energy
       source fuel cost calculation. Same comment as above... that that
       additional layer of grouping and summing seems like it might be more
       clearly done over in the functions doing different things on the basis
       of those different situations.
     - I've added the total heat content consumed per boiler per record to the
       outputs.boiler_fuel_eia923() calculations.
    """
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
    # Get yearly fuel consumed by plant_id and year.
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
