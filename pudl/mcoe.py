"""A module with functions to aid generating MCOE."""

from pudl import constants, analysis
import numpy as np
import pandas as pd
# Data Pulls


def generation_pull_eia923(pudl_engine):
    """
    Pull an annualized version of the EIA 923 generation table.

    Adds plant_pudl_id to the dataframe, and sums up net_generation_mwh per
    EIA plant and per year. Also renames plant_id column to plant_id_eia.
    """
    # Convert the generation_eia923 table into a dataframe
    g9 = analysis.simple_select('generation_eia923', pudl_engine)

    # Get yearly net generation by plant_id, year and generator_id
    g9_summed = analysis.yearly_sum_eia(g9, 'net_generation_mwh',
                                        columns=['report_year', 'plant_id_eia',
                                                 'plant_id_pudl',
                                                 'generator_id'])
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
     - Rather than hard-coding the 2011, 2010, and 2016 years here, we could
       automatically detect which years we DO have for EIA 923, that we DON'T
       have for EIA 860, and automatically extend the fields forward and
       backward using the earliest and latest years in EIA 860.  Then we
       wouldn't have to worry about updating this function when new data is
       integrated into the system.
        Do it, but with a flag or assert statement if more than x years.
    """
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

    # Now we are going to compile the generators by generator (not by energy
    # source) in order to use the capacity of each generator
    g8 = g8[['plant_id_eia', 'plant_id_pudl', 'plant_name', 'operator_name',
             'state', 'generator_id', 'nameplate_capacity_mw',
             'summer_capacity_mw', 'winter_capacity_mw', 'report_year']]

    # # Cheating to duplicate 2015 EIA860 capacity factor data for 2016:
    g8_2016 = g8.loc[g8['report_year'] == 2015]
    g8_2016.report_year.replace([2015], [2016], inplace=True)
    g8 = g8_2016.append(g8)

    return(g8, g8_es)


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
        We should do this in the cleaning functions, through a function that is
        used on multiple tables.
    - Addition of plant_id_pudl and the mmbtu and fuel cost calculations are
      also already being done in the outputs module. We should just do those
      calculations in one place.
    - Should be able to simultaneously annualize multiple data columns at the
      same time, as they're all being grouped by report_year and plant_id_eia.
        TODO.
    - What's the difference between frc9_summed and frc9_summed_plant? Seems
      like the frc9_summed is grouped by plant, year, and energy source, while
      frc9_summed_plant is just year and plant?
        Yup.
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
        It's every year.
     - Is this a table that also contains years? Is that the duplication that's
       being eliminated?  If so, does it ever result in weird duplications of
       a mapping? I.e. is there ever a case where in some years a given boiler
       generator mapping applied, and in others a different one applied, but
       both of those mappings appear in the dataframe which this function
       returns, since they're not identical... but they would be year specific.
    """
    # Convert the boiler_generator_assn_eia860 table into a dataframe
    bga8 = analysis.simple_select('boiler_generator_assn_eia860', pudl_engine)
    bga8.drop(['id', 'operator_id'], axis=1, inplace=True)
    bga8.drop_duplicates(['plant_id_eia', 'boiler_id',
                          'generator_id'], inplace=True)
    # TODO: drop the year
    return(bga8)


def boiler_fuel_pull_eia923(pudl_engine):
    """
    Pull annualized boiler fuel consumption data from EIA 923 for MCOE.

    Note: 'fuel_consumed_mmbtu' renmaed to 'fuel_consumed_mmbtu_plant'

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
    bf9 = analysis.simple_select('boiler_fuel_eia923', pudl_engine)
    bf9['fuel_consumed_mmbtu_plant'] = bf9['fuel_qty_consumed'] * \
        bf9['fuel_mmbtu_per_unit']
    # Get yearly fuel consumed by plant_id, year and boiler_id
    bf9_summed = analysis.yearly_sum_eia(bf9, 'fuel_consumed_mmbtu_plant',
                                         columns=['plant_id_eia',
                                                  'plant_id_pudl',
                                                  'report_year',
                                                  'boiler_id'])
    bf9_summed.reset_index(inplace=True)
    # Get yearly fuel consumed by plant_id and year.
    bf9_plant_summed = analysis.yearly_sum_eia(
        bf9, 'fuel_consumed_mmbtu_plant', columns=['plant_id_eia',
                                                   'plant_id_pudl',
                                                   'report_year'])
    bf9_plant_summed.reset_index(inplace=True)

    return(bf9_summed, bf9_plant_summed)


def gens_with_bga(bga8, g9_summed, id_col='plant_id_eia'):
    """
    Label EIA generators with boiler generator association.

    Because there are missing generators in the bga table, without lumping all
    of the heat input and generation from these plants together, the heat rates
    were off. The vast majority of missing generators from the bga table seem
    to be the gas tubrine from combined cycle plants. This was generating heat
    rates for the steam generators alone, therefore much too low.

    Zane's attempt at describing what's going on in the function:
    - We're passed in a dataframe of all the boiler-generator associations, as
      listed in the EIA860, and a dataframe of all the plant-generator
      associations as listed in the EIA923 FRC table.
    - For the 860 boiler-generator association, we keep a list of all the
      unique plant-generator pairings.
    - For the 923 plant-generator pairings, we keep a list of the ones that are
      unique in terms of plant, generator and year.
    - These dataframes are then merged, bringing the boiler-generator
      associations into the per-year plant-generator associations.
    - At this point, many of the plant-generator associations have a boiler_id
      listed as well, but some of them don't. This may vary by year, as new
      generators are sometimes added, and old ones removed.
    - Any plants that have any generators without an associated boiler are
      tagged as "unassociated_plants" -- these plants have to have all of their
      input heat and all of their output generation pooled together for the
      purpose of calculating heat rate, since we can't individually identify
      the fuel inputs associated with all of the electricity generation.
    - However, any generators which were ever associated with a plant that was
      fully associated should still have valid and isolatable boiler
      associations, so we find all of those associations and preserve them,
      leaving only the mysterious never fully associated generators to be
      pooled with the remaining boilers.
    - At the end of all this, we have a dataframe with the following columns:
      - report_year (non-null)
      - plant_id_eia (non-null)
      - generator_id (non-null)
      - boiler_id (may be null -- every boiler has a generator, but not every
                   generator has a boiler...)
      - boiler_generator_assn:
      - plant_assn:
      - complete_assn: True if the plant/generator/boiler combination listed
        in the record is known to have been part of a completely associated
        plant at some point in time, False otherwise.

    Comments from Zane:
    - I feel like there must be a more concise way to do this, but I think I
      understand what's being done.
    """
    # All generators from the Boiler Generator Association table (860)
    gens8 = bga8.drop_duplicates(
        subset=['plant_id_eia', 'plant_id_pudl', 'generator_id'])
    # All generators from the generation table (923)/
    gens9 = g9_summed.drop_duplicates(
        subset=['plant_id_eia', 'plant_id_pudl', 'generator_id',
                'report_year'])

    # See which generators are missing from the bga table
    gens = gens9.merge(
        gens8, on=['plant_id_eia', 'plant_id_pudl', 'generator_id'],
        how="left")
    gens['boiler_generator_assn'] = np.where(gens.boiler_id.isnull(),
                                             False, True)

    # Create a list of plants that include any generators that are not in the
    # bga table
    unassociated_plants = gens[~gens['boiler_generator_assn']].\
        drop_duplicates(subset=[id_col, 'report_year']).\
        drop(['generator_id', 'net_generation_mwh',
              'boiler_id', 'boiler_generator_assn'], axis=1)
    unassociated_plants['plant_assn'] = False

    # Using these unassociated_plants, lable all the generators that
    # are a part of plants that have generators that are not included
    # in the bga table
    gens = gens.merge(unassociated_plants, on=[
                      'plant_id_eia', 'plant_id_pudl', 'report_year'],
                      how='left')
    gens['plant_assn'] = gens.plant_assn.fillna(value=True)

    # Using the associtated plants, extract the generator/boiler combos
    # that represent complete plants at any time to preserve
    # associations (i.e. if a coal plant had its boilers and generators
    # fully associated in the bga table in 2011 and then adds a
    # combined cycle plant the coal boiler/gen combo will be saved).
    gens_complete = gens[['plant_id_eia', 'plant_id_pudl', 'generator_id',
                          'boiler_id', 'boiler_generator_assn', 'plant_assn']]
    gens_complete = \
        gens_complete[gens_complete['plant_assn']].\
        drop_duplicates(
            subset=['plant_id_eia', 'plant_id_pudl', 'generator_id',
                    'boiler_id'])
    gens_complete['complete_assn'] = True
    gens = gens.merge(gens_complete[['plant_id_eia', 'plant_id_pudl',
                                     'generator_id',
                                     'boiler_id',
                                     'complete_assn']],
                      how='left',
                      on=['plant_id_eia', 'plant_id_pudl', 'generator_id',
                          'boiler_id'])
    gens['complete_assn'] = gens.complete_assn.fillna(value=False)

    return(gens)


def heat_rate_old(bga8, g9_summed, bf9_summed,
                  bf9_plant_summed, pudl_engine,
                  id_col='plant_id_eia', min_heat_rate=5.5):
    """
    Calculate heat rates for all EIA generators.

    Note: 'fuel_consumed_mmbtu_per_gen' renamed to
          'fuel_consumed_mmbtu_generator'

    Zane attempt to understand/describe what's going on here:
    - Create a dataframe of (year, plant, generator, boiler) records for which
      the boilers and generators have never been fully associated with each
      other.  All of the heat input and all of the electricity output will be
      mixed together in a pot to get a heat-rate for those generators.
    - For those unassociated boiler/generator plants, bring in the heat content
      consumed by the boilers from bf9_plant_summed go ahead and calculate
      this averaged heat_content -- it's the best we can do.
    - For the boilers and generators that are associated, we then calculate
      both the total net generation attributable to each boiler, across all of
      the generators that it is associated with, as well as the net generation
      attributable to each individual boiler-generator pairing (right?).
    - Then we calculate the proportion of the overall generation attributable
      to heat from a given boiler, that each of its associated generators is
      responsible for.
    - We use that per-generator proportion of net generation to attribute a
      proportion of the per-boiler total heat consumed to that generator.
    - For each year and generator, we then sum up all of the fuel heat content
      that was consumed in generating electricity by that generator.
    - Then we merge in g9_summed to get access to the net_generation on a per
      generator basis, and calculate the heat rate for each generator-year.
    - We filter the resulting dataframe based on whether the plant, generator,
      year combination is known to be completely associated. Heat rates for
      any incompletely associated plants are invalid, and are eliminated.
    - Now the plant-level heat rates that we calculated earlier are appended,
      hopefully replacing the invalid heat rates that we just dropped.
    - Then we eliminate any records with unrealistically low heat rates i.e.
      those less than 5 mmbtu/MWh, and return!

    Zane Comments:
    - I note that ~10% of the records in the generation_w_boilers dataframe
      have no boiler_id. Is that expected? Given the subsequent groupby() and
      merge() calls that depend on boiler_id, my guess is it isn't expected,
      and may mess up the calculation of annual net generation sums. This
      seems to suggest that some of the relationships in bga8 aren't complete,
      which I guess isn't surprising -- but is it valid to be using those
      incomplete relationships here, or should we be limiting the calculation
      of heat rates per boiler and per boiler-generator pairing to those which
      are part of complete boiler-generator associations? Would that be:
      gens_assn_plants = gens[gens['complete_assn']]

    - I'm not sure I understand how the merges using plant_id_pudl are going
      to impact the overall calculation -- if we're sure that all dataframes
      we're playing with have plant_id_pudl in them, then it should be okay,
      or if we're sure that plant_id_pudl is in the left frame on all the
      left merges, we should be okay, but in poking around at the the
      intermediate dataframes, it does look like there are some which do not
      have a plant_id_pudl in them.

    - When we bring in the heat consumed for an unassociated plant from the
      bf9_plant_summed dataframe, we're just merging on plant and year. Won't
      this be *all* of the heat consumed by the plant for that year, as opposed
      to just the heat consumed by the subset of the plant's boilers that
      weren't fully associated, in the case of plants that contain generators
      which were for some years fully associated, and for other years not
      fully associated? Whereas the net_generation_mwh that is being summed
      is what was attributed only to those generators that weren't ever part
      of a fully associated plant?
        TODO: We need to also lump by association and strip away the associated
        boilers.

    - Have we done any kind of checking to see if we're losing any records
      when we drop the unassociated plant invalid heat rates... and then
      append the plant-level heat rates that were calculated earlier?
    - What fraction of the overall calculated heat rates are we chucking
      because theyre <= 5 mmbtu/MWh?
    - How is it that we've gone through all of the heat_rate calculation w/o
      considering the energy_source or fuel types at all? Seems like we must be
      combining all the fuel consumed by a given boiler regardless of the fuel
      type. But I guess heat is heat is heat, and we're attributing cost based
      on the primary fuel for each generator.
        We could add an additional refinement. For the combo coal and combined
        cycle natural gas plants for which we have no years of boiler generator
        association for the coal plant (which is a rare instance), we could use
        the energy source to associate coal heat inputs with coal generators
        and gas heat inputs to gas generators.
    """
    # Create a dataframe that indicates which plant, boiler, generator,
    # combinations have *ever* been part of a complete association, and thus
    # can have their heat input & electricity output isolated at the sub-plant
    # level.
    gens = gens_with_bga(bga8, g9_summed)

    # Subset of the above dataframe containing those plant, boiler, generator
    # combinations which have *never* been fully associated -- plant level
    # heat rates are the best we can do for these at the moment:
    gens_unassn_plants = gens[~gens['complete_assn']]

    # Sum the yearly net generation for these plants
    gup_gb = gens_unassn_plants.groupby(by=[id_col, 'report_year'])
    gens_unassn_plants_summed = \
        gup_gb.net_generation_mwh.sum().to_frame().reset_index()

    # Pull in the total annual per plant MMBTU consumed:
    unassn_plants = gens_unassn_plants_summed.merge(
        bf9_plant_summed, on=[id_col, 'report_year'])
    # calculate the average heat rate at the plant level -- best we can do:
    unassn_plants['heat_rate_mmbtu_mwh'] = \
        unassn_plants['fuel_consumed_mmbtu_plant'] / \
        unassn_plants['net_generation_mwh']

    # Merge these plant level heat heat rates with the unassociated generators
    # Assign heat rates to generators across the plants with unassociated
    # generators.  Somehow, in this step, we go from haing about 1% of all
    # the heat_rate_mmbtu_mwh values be NaN, to having more like 10% of them
    # be NaN... but it's not entirely clear why. There must be a bunch of
    # plant-year combos that exist in gens_unassn_plants that deon't show up
    # in unassn_plants.
    heat_rate_unassn = gens_unassn_plants.merge(
        unassn_plants[[id_col, 'report_year', 'heat_rate_mmbtu_mwh']],
        on=[id_col, 'report_year'])
    heat_rate_unassn.drop(
        ['boiler_id', 'boiler_generator_assn'], axis=1, inplace=True)

    ######################################################################
    # Start in on the ASSOCIATED generators here.
    ######################################################################
    # This section generates heat rate from the generators of
    # the plants that have any generators that are included in
    # the boiler generator association table (860)
    generation_w_boilers = g9_summed.merge(
        bga8, how='left', on=['plant_id_eia', 'plant_id_pudl', 'generator_id'])

    # get net generation per boiler
    gb1 = generation_w_boilers.groupby(
        by=[id_col, 'report_year', 'boiler_id'])
    generation_w_boilers_summed = \
        gb1.net_generation_mwh.sum().to_frame().reset_index()
    generation_w_boilers_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler'},
        inplace=True)

    # get the generation per boiler/generator combo
    gb2 = generation_w_boilers.groupby(
        by=[id_col, 'report_year', 'boiler_id', 'generator_id'])
    generation_w_bg_summed = \
        gb2.net_generation_mwh.sum().to_frame().reset_index()
    generation_w_bg_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler_gen'},
        inplace=True)

    # squish them together
    generation_w_boilers_summed = \
        generation_w_boilers_summed.merge(
            generation_w_bg_summed, on=[id_col, 'report_year', 'boiler_id'],
            how='left')

    bg = bf9_summed.merge(bga8, how='left',
                          on=['plant_id_eia', 'plant_id_pudl', 'boiler_id'])
    bg = bg.merge(generation_w_boilers_summed, how='left',
                  on=[id_col, 'report_year', 'boiler_id', 'generator_id'])

    # Use the proportion of the generation of each generator to allot mmBTU
    bg['proportion_of_gen_by_boil_gen'] = \
        bg['net_generation_mwh_boiler_gen'] / bg['net_generation_mwh_boiler']
    bg['fuel_consumed_mmbtu_generator'] = \
        bg['proportion_of_gen_by_boil_gen'] * bg['fuel_consumed_mmbtu_plant']

    # Get yearly fuel_consumed_mmbtu_plant by plant_id, year and generator_id
    # before this, the bg dataframe has mulitple records for each generators
    # when there are multiple boiler associated with each generators. This step
    # squishes the boiler level data into generators to be compared to the
    # generator level net generation.
    bg_gb = bg.groupby(by=[id_col, 'report_year', 'generator_id'])
    bg_summed = \
        bg_gb.fuel_consumed_mmbtu_generator.sum().to_frame().reset_index()

    # Calculate heat rate
    heat_rate = bg_summed.merge(g9_summed, how='left',
                                on=[id_col, 'report_year', 'generator_id'])
    heat_rate['heat_rate_mmbtu_mwh'] = \
        heat_rate['fuel_consumed_mmbtu_generator'] / \
        heat_rate['net_generation_mwh']

    # Importing the plant association tag to filter out the
    # generators that are a part of plants that aren't in the bga table
    heat_rate = heat_rate.merge(gens[[id_col,
                                      'report_year',
                                      'generator_id',
                                      'complete_assn',
                                      'plant_assn']],
                                on=[id_col,
                                    'report_year',
                                    'generator_id'])
    heat_rate_assn = heat_rate[heat_rate['complete_assn']]

    ######################################################################
    # Concatenate UNASSOCIATED & ASSOCIATED generators, filter by min_heat_rate
    ######################################################################
    # Append heat rates for associated and unassociated
    heat_rate_all = heat_rate_assn.append(heat_rate_unassn)
    heat_rate_all.sort_values(
        by=[id_col, 'report_year', 'generator_id'], inplace=True)

    # Now, let's chuck the incorrect (lower than 5 mmBTU/MWh)
    heat_rate_all = heat_rate_all[
        heat_rate_all['heat_rate_mmbtu_mwh'] >= min_heat_rate]

    # Because there were null values in the plant_id_pudl field before merging
    # the unassn and assn plants, we needed to reassing the type.
    heat_rate_all.plant_id_pudl = heat_rate_all.plant_id_pudl.astype(int)

    return(heat_rate_all)


def heat_rate(g8_es, bga8, g9_summed, bf9_summed,
              bf9_plant_summed, pudl_engine,
              plant_id='plant_id_eia', min_heat_rate=5.5):
    """An alternative heat rate calculation."""
    # Calculate heat rates for unassociated plants:
    hru = heat_rate_unassn(bga8, g9_summed, bf9_summed,
                           bf9_plant_summed, pudl_engine,
                           plant_id=plant_id)

    # Calculate heat rates for associated plants:
    hra = heat_rate_assn(bga8, g9_summed, bf9_summed,
                         bf9_plant_summed, pudl_engine,
                         plant_id=plant_id)

    # Dissociate all the plants, and calculate heat rates at the plant level:
    hrd = heat_rate_unassn(bga8, g9_summed, bf9_summed,
                           bf9_plant_summed, pudl_engine,
                           plant_id=plant_id, dissociate=True)

    # Identify which plants (either associated or unassociated) are
    # apparently "contaminated" by virtue of having obviously wrong
    # heat rates. This means their boiler/generator mappings or data
    # have something wonky going on:
    bad_plants_assoc = \
        hra[hra.heat_rate_mmbtu_mwh < min_heat_rate][plant_id].unique()
    bad_plants_unassoc = \
        hru[hru.heat_rate_mmbtu_mwh < min_heat_rate][plant_id].unique()
    bad_plants = np.append(bad_plants_assoc, bad_plants_unassoc)

    # Use that master list of "bad plants" to sort both the associated
    # and unassociated plants into usable and unusable data:
    hra_bad = hra[hra[plant_id].isin(bad_plants)]
    hra_good = hra[~hra[plant_id].isin(bad_plants)]
    hru_bad = hru[hru[plant_id].isin(bad_plants)]
    hru_good = hru[~hru[plant_id].isin(bad_plants)]

    # There are several reasons that a plant might have ended up
    # in the "bad" plants -- i.e. those with any generator level
    # heat rates below the minimum plausible heat rate value.
    # Sometimes this is because there's truly missing data, like
    # fuel that was delivered and burned, but which doesn't show up
    # in the boiler fuel table -- there's no getting around that.
    # Other times, it's because the boiler/generator mapping was
    # messed up somehow. By looking at the plant level average heat
    # rates, we can still get some useful information out of the plant
    # if all of the fuel & generation were reported.
    one_fuel_plants = g8_es[g8_es['energy_source_count'] == 1][plant_id]
    hrd_leftover = hrd[(hrd[plant_id].isin(bad_plants))]

    # If there's more than one fuel... then we can't effectively screen
    # based on heat rate -- zero + high can average out to "reasonable"
    bad_hrd_mask = ((~hrd_leftover[plant_id].isin(one_fuel_plants)) |
                    (hrd_leftover.heat_rate_mmbtu_mwh < min_heat_rate))
    hrd_bad = hrd_leftover[bad_hrd_mask]
    hrd_fix = hrd_leftover[~bad_hrd_mask]

    # Tag the records so we know what kind they are
    hra_good['heatrate_calc'] = 'associated_good'
    hru_good['heatrate_calc'] = 'unassociated_good'
    hrd_fix['heatrate_calc'] = 'dissociated_fix'

    # Compile the usable heatrates into one dataframe
    hr_good = pd.concat([hra_good, hru_good, hrd_fix])
    hr_good.sort_values(
        by=[plant_id, 'report_year', 'generator_id'], inplace=True)

    # That this cleanup is required seems to suggest something isn't okay.
    hr_good.plant_id_pudl = hr_good.plant_id_pudl.astype(int)
    return(hr_good)


def heat_rate_assn(bga8, g9_summed, bf9_summed,
                   bf9_plant_summed, pudl_engine, plant_id='plant_id_eia'):
    """Calculate the per generator heat rate for associated generators."""
    assert plant_id in ['plant_id_eia', 'plant_id_pudl']
    if(plant_id == 'plant_id_eia'):
        other_plant_id = 'plant_id_pudl'
    else:
        other_plant_id = 'plant_id_eia'

    gens = gens_with_bga(bga8, g9_summed)
    gens_assn = gens[gens['complete_assn']]
    ga_gb = gens_assn.groupby([plant_id, 'report_year'])

    generation_w_boilers = g9_summed.merge(
        bga8, how='left', on=['plant_id_eia', 'plant_id_pudl', 'generator_id'])

    # get net generation per boiler
    gb1 = generation_w_boilers.groupby(
        by=[plant_id, 'report_year', 'boiler_id'])
    generation_w_boilers_summed = \
        gb1.net_generation_mwh.sum().to_frame().reset_index()
    generation_w_boilers_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler'},
        inplace=True)

    # get the generation per boiler/generator combo
    gb2 = generation_w_boilers.groupby(
        by=[plant_id, 'report_year', 'boiler_id', 'generator_id'])
    generation_w_bg_summed = \
        gb2.net_generation_mwh.sum().to_frame().reset_index()
    generation_w_bg_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler_gen'},
        inplace=True)

    # squish them together
    generation_w_boilers_summed = \
        generation_w_boilers_summed.merge(
            generation_w_bg_summed, on=[plant_id, 'report_year', 'boiler_id'],
            how='left')

    bg = bf9_summed.merge(bga8, how='left',
                          on=['plant_id_eia', 'plant_id_pudl', 'boiler_id'])
    bg = bg.merge(generation_w_boilers_summed, how='left',
                  on=[plant_id, 'report_year', 'boiler_id', 'generator_id'])

    # Use the proportion of the generation of each generator to allot mmBTU
    bg['proportion_of_gen_by_boil_gen'] = \
        bg['net_generation_mwh_boiler_gen'] / bg['net_generation_mwh_boiler']
    bg['fuel_consumed_mmbtu_generator'] = \
        bg['proportion_of_gen_by_boil_gen'] * bg['fuel_consumed_mmbtu_plant']

    # Generators with no generation and no associated fuel consumption result
    # in some 0/0 = NaN values, which propagate when summed. For our purposes
    # they should be set to zero, since those generators are contributing
    # nothing to either the fuel consumed or the proportion of net generation.
    bg['proportion_of_gen_by_boil_gen'] = \
        bg.proportion_of_gen_by_boil_gen.fillna(0)
    bg['fuel_consumed_mmbtu_generator'] = \
        bg.fuel_consumed_mmbtu_generator.fillna(0)

    # Get yearly fuel_consumed_mmbtu_plant by plant_id, year and generator_id
    # before this, the bg dataframe has mulitple records for each generators
    # when there are multiple boiler associated with each generators. This step
    # squishes the boiler level data into generators to be compared to the
    # generator level net generation.
    bg_gb = bg.groupby(by=[plant_id, 'report_year', 'generator_id'])
    bg_summed = \
        bg_gb.fuel_consumed_mmbtu_generator.sum().to_frame().reset_index()

    # Calculate heat rate
    hr = bg_summed.merge(g9_summed, how='left',
                         on=[plant_id, 'report_year', 'generator_id'])
    hr['heat_rate_mmbtu_mwh'] = \
        hr['fuel_consumed_mmbtu_generator'] / \
        hr['net_generation_mwh']

    # Importing the plant association tag to filter out the
    # generators that are a part of plants that aren't in the bga table
    hr = hr.merge(gens[[plant_id,
                        'report_year',
                        'generator_id',
                        'complete_assn',
                        'plant_assn']],
                  on=[plant_id,
                      'report_year',
                      'generator_id'])
    hra = hr[hr['complete_assn']]

    return(hra)


def heat_rate_unassn(bga8, g9_summed, bf9_summed,
                     bf9_plant_summed, pudl_engine,
                     plant_id='plant_id_eia', dissociate=False):
    """Calculate the whole plant heat rate for unassociated generators."""
    assert plant_id in ['plant_id_eia', 'plant_id_pudl']
    if(plant_id == 'plant_id_eia'):
        other_plant_id = 'plant_id_pudl'
    else:
        other_plant_id = 'plant_id_eia'

    # This dataframe contains information about the generators and whether
    # they have boiler associations that can be used to connect fuel consumed
    # with electricity generated at anything below the plant level:
    gens = gens_with_bga(bga8, g9_summed)

    if dissociate:
        gens['complete_assn'] = False

    # There are two basic calculation cases which we need to address, based on
    # the association status of each plant-generator-boiler set:
    # - Boilers and generators that have EVER been associated with each other
    #   as part of a complete plant will remain associated with each other for
    #   heat_rate calculation purposes, even in other years.
    # - In the case of associated boilers and generators, we have both the
    #   (plant, generator) ID pairs needed to select generation and the
    #   (plant, boiler) ID pairs needed to select heat input, so it's
    #   relatively straightforward to calculate the heat rates.
    gens_assn = gens[gens['complete_assn']]
    ga_gb = gens_assn.groupby([plant_id, 'report_year'])

    # - Generators that have NEVER been part of a complete plant association
    #   get pooled with all the boilers that are part of the same plant that
    #   have ALSO never been part of a complete plant association. In many
    #   cases, this will be all the generators and boilers for a given plant.
    gens_unassn = gens[~gens['complete_assn']]
    gu_gb = gens_unassn.groupby([plant_id, 'report_year'])

    # - In this case, the gens DataFrame contains the list of unassociated
    #   generators, but doesn't give us the information we need to be able to
    #   select unassociated boilers, so we know how much heat input to
    #   attribute to the generation from those generators... we have to do
    #   some work to get it.
    # - Get the FULL list of all the (plant, boiler) pairs.
    # - Get the list of (plant, boiler) pairs that were in the completely
    #   associated plant list.
    # - Select any (plant, boiler) pairs that are found in the FULL list, but
    #   which were NOT found in the completely associated list.
    # - bf9 is our best "full" list of boilers.
    # - Selecting on multiple columns simultaneously sadly does not seem to
    #   be a built-in thing, so we're synthesizing a disposable index field
    #   that is a concatenation of the plant and boiler IDs here:
    bf9_summed['stupid_id'] = \
        bf9_summed[plant_id].astype(str) + bf9_summed['boiler_id'].astype(str)
    gens_assn['stupid_id'] = \
        gens_assn[plant_id].astype(str) + gens_assn['boiler_id'].astype(str)

    # Get all of records from bf9_summed that do NOT have their (plant, boiler)
    # combination represetned in the list of completely associated plants.
    boils_unassn = bf9_summed[~bf9_summed.stupid_id.isin(gens_assn.stupid_id)]
    bu_gb = boils_unassn.groupby([plant_id, 'report_year'])

    # This is the yearly per plant net generation attributable to generators
    # that have never been fully associated:
    gu_plant_yr = gu_gb.net_generation_mwh.sum().to_frame().reset_index()
    # This is the yearly per plant fuel heat content consumed from boilers
    # that have never been fully associated:
    bu_plant_yr = \
        bu_gb.fuel_consumed_mmbtu_plant.sum().to_frame().reset_index()

    # Bring together the unassociated generators and boilers by year and plant
    # id so we can calculate the overall heat rate for the unassociated
    # portions of these plants (which will be the whole plant in many cases).
    plants_unassn_yr = pd.merge(gu_plant_yr, bu_plant_yr,
                                on=[plant_id, 'report_year'])

    plants_unassn_yr['heat_rate_mmbtu_mwh'] = \
        plants_unassn_yr.fuel_consumed_mmbtu_plant / \
        plants_unassn_yr.net_generation_mwh

    hru = pd.merge(
        gens_unassn,
        plants_unassn_yr[[plant_id, 'report_year', 'heat_rate_mmbtu_mwh']],
        on=[plant_id, 'report_year']
    )
    return(hru)


def fuel_cost(g8_es, g9_summed, frc9_summed, frc9_summed_plant, heat_rate):
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
     - I think creating the column for energy_source_count in this function
       would make more sense in terms of readability than doing it in the table
       pull function -- it's really specific to what's going on here, and kinda
       comes out of nowhere.
        Agree, let's move it.
     - For net_gen_one_fuel, rather than doing a left join and then dropping
       NA values, why not just do an inner join?
        Sure.
     - In creation of fuel_cost_one_fuel why is net_generation_mwh one of the
       fields that's being merged on? That's a data field... normally this is
       a no-no. Especially for floating point numbers, which you can't easily
       guarantee will be exactly the same, even when they should be
       conceptually. Because binary.
        TODO: drop net gen from one of the dataframes b4 merging w/ out net gen
     - Same question as above for fuel_cost_multi_fuel merge.
     - In creation of fuel_cost_per_mmbtu_multi_fuel we're merging on the
       energy_source_cons, but not including generator_id (because FRC doesn't
       include generator_id). This is magical step, right, where we take the
       energy source of the various generators, and the energy source
       associated with the fuel records, and use it to map per-plant deliveries
       of fuel to the individual generators.
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
                         'net_generation_mwh',
                         'heat_rate_mmbtu_mwh']],
              on=['plant_id_eia',
                  'report_year',
                  'generator_id',
                  'net_generation_mwh'])

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
                  'energy_source_cons'])

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

    return(fuel_cost)
