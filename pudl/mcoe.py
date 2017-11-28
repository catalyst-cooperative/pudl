"""A module with functions to aid generating MCOE."""

from pudl import analysis, clean_pudl, outputs, pudl
from pudl import constants as pc
import numpy as np
import pandas as pd

# General issues:
# - Need to deal with both EIA & PUDL plant IDs.

# - boiler_generator_pull_eia860:
#   - We need the more complex and exhaustive construction of the BGA that
#     Christina is working on.  Needs to vary over time, and integrate more
#     inter-year information.


def fuel_receipts_costs_pull_eia923(pudl_engine, years=[2014, 2015, 2016]):
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

    - Handing back two dataframes feels a little awkward, and the additional
      step of further grouping and calculating cost per mmbtu across the entire
      plant feels like it might be done over in the fuel_cost function
      instead, so it's clear what two different things are being done, side by
      side, in the single vs. multi-fuel plant cases.
        I don't think I agree with this, but we'll see.

    - Also slightly dangerous to calculate the fuel cost per mmbtu column for
      all of the plants, when I don't think we want folks to use those numbers
      for the multi-fuel plants (since it'd be a mix of e.g. coal and gas costs
      per mmbtu).
        Maybe you need to explain this. I don't see this going anywhere expect
        for into the fuel cost calcs.
    """
    # Convert the fuel_receipts_costs_eia923 table into a dataframe
    frc9 = analysis.simple_select('fuel_receipts_costs_eia923', pudl_engine)

    frc9['fuel_cost'] = (frc9['fuel_quantity'] *
                         frc9['average_heat_content'] *
                         frc9['fuel_cost_per_mmbtu'])

    frc9['mmbtu'] = (frc9['fuel_quantity'] * frc9['average_heat_content'])

    # Get yearly fuel cost by plant_id, year and energy_source
    frc9_summed = analysis.yearly_sum_eia(frc9, 'fuel_cost', columns=[
                                          'plant_id_eia', 'plant_id_pudl',
                                          'report_year',
                                          'energy_source_simple'])
    frc9_summed = frc9_summed.reset_index()

    frc9_mmbtu_summed = analysis.yearly_sum_eia(
        frc9, 'mmbtu', columns=['plant_id_eia', 'report_year',
                                'energy_source_simple'])
    frc9_mmbtu_summed = frc9_mmbtu_summed.reset_index()
    frc9_summed = frc9_mmbtu_summed.merge(frc9_summed)
    frc9_summed['fuel_cost_per_mmbtu_es'] = (
        frc9_summed.fuel_cost / frc9_summed.mmbtu)
    frc9_summed.rename(columns={'mmbtu': 'mmbtu_es',
                                'fuel_cost': 'fuel_cost_es'}, inplace=True)

    # Get yearly fuel cost by plant_id and year
    # For use in calculating fuel cost for plants with one main energy soure
    frc9_summed_plant = analysis.yearly_sum_eia(
        frc9, 'fuel_cost', columns=['plant_id_eia', 'report_year'])
    frc9_summed_plant = frc9_summed_plant.reset_index()
    frc9_mmbtu_summed_plant = analysis.yearly_sum_eia(
        frc9, 'mmbtu', columns=['plant_id_eia', 'report_year'])
    frc9_mmbtu_summed_plant = frc9_mmbtu_summed_plant.reset_index()
    frc9_summed_plant = frc9_mmbtu_summed_plant.merge(frc9_summed_plant)
    frc9_summed_plant['fuel_cost_per_mmbtu_plant'] = (
        frc9_summed_plant.fuel_cost / frc9_summed_plant.mmbtu)
    frc9_summed_plant.rename(columns={'mmbtu': 'mmbtu_plant',
                                      'fuel_cost': 'fuel_cost_plant'},
                             inplace=True)

    frc9_summed = frc9_summed[frc9_summed.report_year.isin(years)]
    frc9_summed_plant = \
        frc9_summed_plant[frc9_summed_plant.report_year.isin(years)]

    return(frc9_summed, frc9_summed_plant)


def boiler_generator_pull_eia860(testing=False):
    """
    Pull the boiler generator associations from EIA 860.

    Adds plant_id_pudl and drops operator_id and id (an internal automatically
    incrementing surrogate key), and keeps only unique combinations of plant,
    boiler, and generator -- without preserving any changes over time.

    This function will be replaced with Christina's new BGA compilation.
    """
    pudl_engine = pudl.db_connect_pudl(testing=testing)
    # Convert the boiler_generator_assn_eia860 table into a dataframe
    bga8 = analysis.simple_select('boiler_generator_assn_eia860', pudl_engine)
    bga8.drop(['id', 'operator_id'], axis=1, inplace=True)
    bga8.drop_duplicates(['plant_id_eia', 'boiler_id',
                          'generator_id'], inplace=True)
    return(bga8)


def gens_with_bga(bga_eia860, gen_eia923, id_col='plant_id_eia'):
    """
    Label EIA generators by whether they've ever been part of complete plants.

    Issues/Comments:
      - Dropping duplicates from bga_eia860 means we lose many boiler
        generator associations -- any time there's more than one boiler
        associated w/ a given generator we only retain one of them.
      - How is it that we're actually retaining all of the reported MWh?
        - Oh, the MWh is associated w/ generators, not the boilers
      - Seems like there are two incompatible things going on here. We need
        to know how much generation is associated with each generator, and
        we need to retain all of the boiler-generator relationships so we
        can tell which generators are mapped to which boilers later on.
    """
    # All generators from the Boiler Generator Association table (860)
    bga8 = bga_eia860[['plant_id_eia', 'plant_id_pudl',
                       'generator_id', 'boiler_id']]

    # All generators from the generation_eia923 table, by year.
    gens9 = gen_eia923[['report_date', 'plant_id_eia',
                        'plant_id_pudl', 'generator_id']].drop_duplicates()

    # Merge in the boiler associations across all the different years of
    # generator - plant associations.
    gens = pd.merge(gens9, bga8, how='left',
                    on=['plant_id_eia', 'plant_id_pudl', 'generator_id'])
    # Set a boolean flag on each record indicating whether the plant-generator
    # pairing has a boiler associated with it.
    gens['boiler_generator_assn'] = \
        np.where(gens.boiler_id.isnull(), False, True)

    # Find all the generator records that were ever missing a boiler:
    unassociated_generators = gens[~gens['boiler_generator_assn']]
    # Create a list of plants with unassociated generators, by year.
    unassociated_plants = unassociated_generators.\
        drop_duplicates(subset=[id_col, 'report_date']).\
        drop(['generator_id', 'boiler_id', 'boiler_generator_assn'], axis=1)
    # Tag those plant-years as being unassociated
    unassociated_plants['plant_assn'] = False

    # Merge the plant association flag back in to the generators
    gens = pd.merge(gens, unassociated_plants, how='left',
                    on=['plant_id_eia', 'plant_id_pudl', 'report_date'])
    # Tag the rest of the generators as being part of a plant association...
    # This may or may not be true. Need to filter out partially associated
    # plants in the next step.
    gens['plant_assn'] = gens.plant_assn.fillna(value=True)

    # Using the associtated plants, extract the generator/boiler combos
    # that represent complete plants at any time to preserve
    # associations (i.e. if a coal plant had its boilers and generators
    # fully associated in the bga table in 2011 and then adds a
    # combined cycle plant the coal boiler/gen combo will be saved).

    # Remove the report_date:
    gens_complete = gens.drop('report_date', axis=1)
    # Select only those generators tagged as being part of a complete plant:
    gens_complete = gens_complete[gens_complete['plant_assn']]

    gens_complete = gens_complete.drop_duplicates(subset=['plant_id_eia',
                                                          'plant_id_pudl',
                                                          'generator_id',
                                                          'boiler_id'])
    gens_complete['complete_assn'] = True

    gens = gens.merge(gens_complete[['plant_id_eia', 'plant_id_pudl',
                                     'generator_id', 'boiler_id',
                                     'complete_assn']],
                      how='left',
                      on=['plant_id_eia', 'plant_id_pudl', 'generator_id',
                          'boiler_id'])
    gens['complete_assn'] = gens.complete_assn.fillna(value=False)

    return(gens)


def heat_rate(bga_eia860, gen_eia923, bf_eia923,
              plant_id='plant_id_eia', min_heat_rate=5.5):
    """
    Calculate heat rates (mmBTU/MWh) within separable generation units.

    We use three different methods to calculate the heat rate for three types
    of boiler/generator arrangements:
     - Generator level heat rates if we know the boiler-generator associations.
       (these are overwhelmingly coal plants and their steam turbines)
     - Plant level average heat rates if we don't know the boiler-generator
       associations. (these are overwhelmingly combined cycle gas plants)
     - Plant level average heat rates if we get a heat rate which is too low
       to be real, based on a given boiler-generator association.

    The resulting heat rates are returned on a per-generator basis, with a
    column entitled heatrate_calc indicating which type of heat rate was
    calculated.
    """
    assert plant_id in ['plant_id_eia', 'plant_id_pudl']
    if(plant_id == 'plant_id_eia'):
        other_plant_id = 'plant_id_pudl'
    else:
        other_plant_id = 'plant_id_eia'

    generation_w_boilers = pd.merge(gen_eia923, bga_eia860, how='left',
                                    on=['plant_id_eia', 'plant_id_pudl',
                                        'generator_id'])

    # Calculate net generation from all generators associated with each boiler
    gb1 = generation_w_boilers.groupby(
        by=[plant_id, 'report_date', 'boiler_id'])
    gen_by_boiler = gb1.net_generation_mwh.sum().to_frame().reset_index()
    gen_by_boiler.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler'},
        inplace=True)

    # Calculate net generation per unique boiler generator combo
    gb2 = generation_w_boilers.groupby(
        by=[plant_id, 'report_date', 'boiler_id', 'generator_id'])
    gen_by_bg = gb2.net_generation_mwh.sum().to_frame().reset_index()
    gen_by_bg.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler_gen'},
        inplace=True)

    # squish them together
    gen_by_bg_and_boiler = pd.merge(gen_by_boiler, gen_by_bg,
                                    on=[plant_id, 'report_date', 'boiler_id'],
                                    how='left')

    # Bring in boiler fuel consumption and boiler generator associations
    bg = pd.merge(bf_eia923, bga_eia860, how='left',
                  on=['plant_id_eia', 'plant_id_pudl', 'boiler_id'])
    # Merge boiler fuel consumption in with our per-boiler and boiler
    # generator combo net generation calculations
    bg = pd.merge(bg, gen_by_bg_and_boiler, how='left',
                  on=[plant_id, 'report_date', 'boiler_id', 'generator_id'])

    # Use the proportion of the generation of each generator to allot mmBTU
    bg['proportion_of_gen_by_boil_gen'] = \
        bg['net_generation_mwh_boiler_gen'] / bg['net_generation_mwh_boiler']
    bg['fuel_consumed_mmbtu_generator'] = \
        bg['proportion_of_gen_by_boil_gen'] * bg['total_heat_content_mmbtu']

    # Generators with no generation and no associated fuel consumption result
    # in some 0/0 = NaN values, which propagate when summed. For our purposes
    # they should be set to zero, since those generators are contributing
    # nothing to either the fuel consumed or the proportion of net generation.
    bg['proportion_of_gen_by_boil_gen'] = \
        bg.proportion_of_gen_by_boil_gen.fillna(0)
    bg['fuel_consumed_mmbtu_generator'] = \
        bg.fuel_consumed_mmbtu_generator.fillna(0)

    # Get total heat consumed per time period by each generator.
    # Before this, the bg dataframe has mulitple records for each generator
    # when there are multiple boiler associated with each generators. This step
    # squishes the boiler level data into generators to be compared to the
    # generator level net generation.
    bg_gb = bg.groupby(by=[plant_id, 'report_date', 'generator_id'])
    bg = bg_gb.fuel_consumed_mmbtu_generator.sum().to_frame().reset_index()

    # Now that we have the fuel consumed per generator, bring the net
    # generation per generator back in:
    hr = pd.merge(bg, gen_eia923, how='left',
                  on=[plant_id, 'report_date', 'generator_id'])

    # Finally, calculate heat rate
    hr['heat_rate_mmbtu_mwh'] = \
        hr['fuel_consumed_mmbtu_generator'] / \
        hr['net_generation_mwh']

    # Importing the plant association tag to filter out the
    # generators that are a part of plants that aren't in the bga table
    gens = gens_with_bga(bga_eia860, gen_eia923)
    # This is a per-generator table now -- so we don't want the boiler_id
    # And we only want the ones with complete associations.
    gens_assn = gens[gens['complete_assn']].drop('boiler_id', axis=1)
    hr = pd.merge(hr, gens_assn, on=['plant_id_eia', 'plant_id_pudl',
                                     'report_date', 'generator_id'])

    # Only keep the generators with reasonable heat rates
    hr = hr[hr.heat_rate_mmbtu_mwh >= min_heat_rate]

    # Sort it a bit and clean up some types
    first_cols = [
        'report_date',
        'operator_id',
        'operator_name',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'generator_id'
    ]
    hr = outputs.organize_cols(hr, first_cols)
    hr['util_id_pudl'] = hr.util_id_pudl.astype(int)
    hr['operator_id'] = hr.operator_id.astype(int)
    hr = hr.sort_values(by=['operator_id', plant_id,
                            'generator_id', 'report_date'])
    return(hr)


def fuel_cost(freq=None, testing=False, start_date=None, end_date=None):
    """
    Calculate fuel costs per MWh on a per generator basis for MCOE.

    Fuel costs are reported on a per-plant basis, but we want to estimate them
    at the generator level. This is complicated by the fact that some plants
    have several different types of generators, using different fuels. We have
    fuel costs broken out by type of fuel (coal, oil, gas), and we know which
    generators use which fuel based on their energy_source code and reported
    prime_mover. Coal plants use a little bit of natural gas or diesel to get
    started, but based on our analysis of the "pure" coal plants, this amounts
    to only a fraction of a percent of their overal fuel consumption on a
    heat content basis, so we're ignoring it for now.

    For plants whose generators all rely on the same fuel source, we simply
    attribute the fuel costs proportional to the fuel heat content consumption
    associated with each generator.

    For plants with more than one type of generator energy source, we need to
    split out the fuel costs according to fuel type -- so the gas fuel costs
    are associated with generators that have energy_source gas, and the coal
    fuel costs are associated with the generators that have energy_source coal.

    Zane Comments:
     - For net_gen_one_fuel, rather than doing a left join and then dropping
       NA values, why not just do an inner join?
        Sure.
     - Same question as above for fuel_cost_multi_fuel merge.
    """
    one_fuel_plants = g8_es[g8_es['energy_source_count'] == 1]
    multi_fuel_plants = g8_es[g8_es['energy_source_count'] > 1]

    # one fuel plants
    net_gen_one_fuel = g9_summed.merge(one_fuel_plants, how='left', on=[
        'plant_id_eia', 'plant_id_pudl', 'generator_id', 'report_year'])
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
                         'heat_rate_mmbtu_mwh']],
              on=['plant_id_eia',
                  'report_year',
                  'generator_id'])

    # Calculate fuel cost per mwh using average fuel cost given year, plant,
    # fuel type; divide by generator-specific heat rate
    fuel_cost_one_fuel['fuel_cost_per_mwh'] = \
        (fuel_cost_one_fuel['fuel_cost_per_mmbtu_plant']
         * fuel_cost_one_fuel['heat_rate_mmbtu_mwh'])

    # multi fuel plants
    net_gen_multi_fuel = g9_summed.merge(multi_fuel_plants, how='left', on=[
        'plant_id_eia', 'plant_id_pudl', 'generator_id', 'report_year'])
    net_gen_multi_fuel.dropna(inplace=True)

    # Merge this net_gen table with frc9_summed to have
    # fuel_cost_per_mmbtu_total associated with energy source
    # in this case, we are using energy source as a more granular sub plant
    # lumping because we don't have generator ids in the frc table.
    fuel_cost_per_mmbtu_multi_fuel = net_gen_multi_fuel.\
        merge(frc9_summed,
              how='left',
              on=['plant_id_eia',
                  'plant_id_pudl',
                  'report_year',
                  'energy_source_simple'])

    fuel_cost_multi_fuel = fuel_cost_per_mmbtu_multi_fuel.\
        merge(heat_rate[['plant_id_eia',
                         'report_year',
                         'generator_id',
                         'heat_rate_mmbtu_mwh']],
              on=['plant_id_eia',
                  'report_year',
                  'generator_id'])

    # Calculate fuel cost per mwh using average fuel cost given year, plant,
    # fuel type; divide by generator-specific heat rate
    fuel_cost_multi_fuel['fuel_cost_per_mwh'] = \
        (fuel_cost_multi_fuel['fuel_cost_per_mmbtu_es'] *
         fuel_cost_multi_fuel['heat_rate_mmbtu_mwh'])

    # Squish them together!
    fuel_cost = fuel_cost_one_fuel.append(fuel_cost_multi_fuel)

    fuel_cost = \
        fuel_cost.sort_values(['report_year', 'plant_id_eia', 'generator_id'])

    return(fuel_cost)


def mcoe(freq='AS', testing=False, plant_id='plant_id_eia',
         start_date=None, end_date=None,
         min_heat_rate=5.5, output=None,):
    """
    Compile marginal cost of electricity (MCOE) at the generator level.

    Use data from EIA 923, EIA 860, and (eventually) FERC Form 1 to estimate
    the MCOE of individual generating units. By default, this is done at
    annual resolution, since the FERC Form 1 data is annual.  Perform the
    calculation for time periods between start_date and end_date. If those
    dates aren't given, then perform the calculation across all of the years
    for which the EIA 923 data is available.

    Args:
        freq: String indicating time resolution on which to calculate MCOE.
        start_date: beginning of the date range to calculate MCOE within.
        end_date: end of the date range to calculate MCOE within.
        output: path to output CSV to. No output if None.
        plant_id: Which plant ID to aggregate on? PUDL or EIA?
        min_heat_rate: lowest plausible heat rate, in mmBTU/MWh.

    Returns:
        mcoe: a dataframe organized by date and generator, with lots of juicy
            information about MCOE.

    Issues:
        - Start and end dates outside of the EIA860 valid range don't seem to
          result in additional EIA860 data being synthesized and returned.
        - Merge annual data with other time resolutions.
    """
    # If we haven't been given start & end dates, use the full extent of
    # the EIA923 data:
    if start_date is None:
        start_date = \
            pd.to_datetime('{}-01-01'.format(min(pc.working_years['eia923'])))
    else:
        # Make sure it's a date... and not a string.
        start_date = pd.to_datetime(start_date)
    if end_date is None:
        end_date = \
            pd.to_datetime('{}-12-31'.format(max(pc.working_years['eia923'])))
    else:
        # Make sure it's a date... and not a string.
        end_date = pd.to_datetime(end_date)

    # Select the required data from the database:
    # formerly g8_es
    gens_eia860 = outputs.generators_eia860(testing=testing,
                                            start_date=start_date,
                                            end_date=end_date)
    gens_eia860 = gens_eia860.rename(columns={'plant_id': 'plant_id_eia'})

    # formerly bga8
    bga_eia860 = boiler_generator_pull_eia860(testing=testing)
    bga_eia860 = bga_eia860.rename(columns={'plant_id': 'plant_id_eia'})

    # formerly g9_summed
    gen_eia923 = outputs.generation_eia923(freq=freq, testing=testing,
                                           start_date=start_date,
                                           end_date=end_date)
    gen_eia923 = gen_eia923.rename(columns={'plant_id': 'plant_id_eia'})

    # formerly bf9_summed & bf9_plant_summed
    bf_eia923 = outputs.boiler_fuel_eia923(freq=freq, testing=testing,
                                           start_date=start_date,
                                           end_date=end_date)
    bf_eia923 = bf_eia923.rename(columns={'plant_id': 'plant_id_eia'})

    # Calculate heat rates by generator
    hr_df = heat_rate(bga_eia860=bga_eia860,
                      gen_eia923=gen_eia923,
                      bf_eia923=bf_eia923,
                      plant_id=plant_id,
                      min_heat_rate=min_heat_rate)

    hr_df = analysis.merge_on_date_year(
        hr_df,
        gens_eia860[['report_date', 'plant_id_eia', 'plant_id_pudl',
                     'generator_id', 'energy_source_simple']],
        how='inner', on=['plant_id_eia', 'plant_id_pudl', 'generator_id'])

    # formerly frc9_summed & frc9_summed_plant
    # frc_eia923 = outputs.fuel_receipts_costs_eia923(freq=freq, testing=testing,
    #                                                 start_date=start_date,
    #                                                 end_date=end_date)

    # Calculate fuel costs by generator
    # Calculate capacity factors by generator
    # Compile the above into a single dataframe for output/return.

    return(hr_df)
