"""A module with functions to aid generating MCOE."""

from pudl import analysis, clean_pudl, outputs, pudl
from pudl import constants as pc
import numpy as np
import pandas as pd


def gens_with_bga(bga_eia860, gen_eia923):
    """
    Label EIA generators based on which type of associations they have.

    Given the boiler generator associations, and the generation records,
    label each generator according to which kinds of associations it has
    had. Three boolean values are set for each generator, for each time
    period:
        - boiler_generator_assn: True if the generator has any boilers
          associated with it, False otherwise.
        - plant_assn: True if all the generators associated with a given
          plant_id have a boiler associated with them, False otherwise.
        - complete_assn: True if the generator has *ever* been part of a
          plant in which all generators had a boiler associated with them,
          False if otherwise.

    Returns:
        A dataframe containing plant_id_eia, generator_id, boiler_id, and
        the three boolean columns mentioned above.
    """
    # All generators from the Boiler Generator Association table (860)
    bga8 = bga_eia860[['report_date', 'plant_id_eia',
                       'generator_id', 'boiler_id']]

    # All generators from the generation_eia923 table, by year.
    gens9 = gen_eia923[['report_date', 'plant_id_eia',
                        'generator_id']].drop_duplicates()

    # Merge in the boiler associations across all the different years of
    # generator - plant associations.
    gens = analysis.merge_on_date_year(gens9, bga8, how='left',
                                       on=['plant_id_eia', 'generator_id'])
    # Set a boolean flag on each record indicating whether the plant-generator
    # pairing has a boiler associated with it.
    gens['boiler_generator_assn'] = \
        np.where(gens.boiler_id.isnull(), False, True)

    # Find all the generator records that were ever missing a boiler:
    unassociated_generators = gens[~gens['boiler_generator_assn']]
    # Create a list of plants with unassociated generators, by year.
    unassociated_plants = unassociated_generators.\
        drop_duplicates(subset=['plant_id_eia', 'report_date']).\
        drop(['generator_id', 'boiler_id', 'boiler_generator_assn'], axis=1)
    # Tag those plant-years as being unassociated
    unassociated_plants['plant_assn'] = False

    # Merge the plant association flag back in to the generators
    gens = pd.merge(gens, unassociated_plants, how='left',
                    on=['plant_id_eia', 'report_date'])
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
                                                          'generator_id',
                                                          'boiler_id'])
    gens_complete['complete_assn'] = True

    gens = gens.merge(gens_complete[['plant_id_eia', 'generator_id',
                                     'boiler_id', 'complete_assn']],
                      how='left',
                      on=['plant_id_eia', 'generator_id', 'boiler_id'])
    gens['complete_assn'] = gens.complete_assn.fillna(value=False)

    return(gens)


def boiler_generator_association(bga_eia860, gens_eia860,
                                 gen_eia923, bf_eia923,
                                 start_date=None, end_date=None,
                                 testing=False):
    """
    Temporary function to create more complete boiler generator associations.

    This is a temporary function until it can be pulled into a datatable. This
    function pulls in all of the generators and all of the boilers, uses them
    to create a relatively complete association list. First, the original bga
    table is used, then the remaining unmatched generators are matched to the
    boilers with the same string (in the same plant and year), then the unit
    codes are used to connect all generators and boilers within each given
    unit. Each of the incomplete or inaccurate records are tagged in columns.

    Notes:
     - unit_code is coming out as a mix of None and NaN values. Should pick
       a single type for the column and stick to it (or enforce on output).
    """
    pudl_engine = pudl.db_connect_pudl(testing=testing)
    # compile and scrub all the parts
    bga_eia860.drop_duplicates(['plant_id_eia', 'boiler_id',
                                'generator_id', 'report_date'], inplace=True)
    bga_eia860.drop(['id', 'operator_id'], axis=1, inplace=True)

    gen_eia923 = gen_eia923.set_index(pd.DatetimeIndex(gen_eia923.report_date))
    gen_eia923_gb = gen_eia923.groupby(
        [pd.Grouper(freq='AS'), 'plant_id_eia', 'generator_id'])
    gen_eia923 = gen_eia923_gb['net_generation_mwh'].sum().reset_index()
    gen_eia923['missing_from_923'] = False

    # The generator records that are missing from 860 but appear in 923
    # I created issue no. 128 to deal with this at a later date
    merged = pd.merge(gens_eia860, gen_eia923,
                      on=['plant_id_eia', 'report_date', 'generator_id'],
                      indicator=True, how='outer')
    missing = merged[merged['_merge'] == 'right_only']

    # compile all of the generators
    gens = pd.merge(gen_eia923, gens_eia860,
                    on=['plant_id_eia', 'report_date', 'generator_id'],
                    how='outer')

    gens = gens[['plant_id_eia',
                 'report_date',
                 'generator_id',
                 'unit_code',
                 'net_generation_mwh',
                 'missing_from_923']].drop_duplicates()

    # create the beginning of a bga compilation w/ the generators as the
    # background
    bga_compiled_1 = pd.merge(gens, bga_eia860,
                              on=['plant_id_eia', 'generator_id',
                                  'report_date'],
                              how='outer')

    # Side note: there are only 6 generators that appear in bga8 that don't
    # apear in gens9 or gens8 (must uncomment-out the og_tag creation above)
    # bga_compiled_1[bga_compiled_1['og_tag'].isnull()]

    bf_eia923 = bf_eia923.set_index(pd.DatetimeIndex(bf_eia923.report_date))
    bf_eia923_gb = bf_eia923.groupby(
        [pd.Grouper(freq='AS'), 'plant_id_eia', 'boiler_id'])
    bf_eia923 = bf_eia923_gb['total_heat_content_mmbtu'].sum().reset_index()

    bf_eia923.drop_duplicates(
        subset=['plant_id_eia', 'report_date', 'boiler_id'], inplace=True)

    # Create a set of bga's that are linked, directly from bga8
    bga_assn = bga_compiled_1[bga_compiled_1['boiler_id'].notnull()].copy()
    bga_assn['bga_source'] = 'eia860_org'

    # Create a set of bga's that were not linked directly through bga8
    bga_unassn = bga_compiled_1[bga_compiled_1['boiler_id'].isnull()].copy()
    bga_unassn = bga_unassn.drop(['boiler_id'], axis=1)

    # Create a list of boilers that were not in bga8
    bf9_bga = bf_eia923.merge(bga_compiled_1,
                              on=['plant_id_eia', 'boiler_id', 'report_date'],
                              how='outer',
                              indicator=True)
    bf9_not_in_bga = bf9_bga[bf9_bga['_merge'] == 'left_only']
    bf9_not_in_bga = bf9_not_in_bga.drop(['_merge'], axis=1)

    # Match the unassociated generators with unassociated boilers
    # This method is assuming that some the strings of the generators and the
    # boilers are the same
    bga_unassn = bga_unassn.merge(bf9_not_in_bga[['plant_id_eia',
                                                  'boiler_id',
                                                  'report_date']],
                                  how='left',
                                  left_on=['report_date',
                                           'plant_id_eia',
                                           'generator_id'],
                                  right_on=['report_date',
                                            'plant_id_eia',
                                            'boiler_id'])
    bga_unassn.sort_values(['report_date', 'plant_id_eia'], inplace=True)
    bga_unassn['bga_source'] = None
    bga_unassn.loc[bga_unassn.boiler_id.notnull(),
                   'bga_source'] = 'string_assn'

    bga_compiled_2 = bga_assn.append(bga_unassn)
    bga_compiled_2.sort_values(['plant_id_eia', 'report_date'], inplace=True)
    bga_compiled_2['missing_from_923'].fillna(value=True, inplace=True)

    # Connect the gens and boilers in units
    bga_compiled_units = bga_compiled_2.loc[
        bga_compiled_2['unit_code'].notnull()]
    bga_gen_units = bga_compiled_units.drop(['boiler_id'], axis=1)
    bga_boil_units = bga_compiled_units[['plant_id_eia',
                                         'report_date',
                                         'boiler_id',
                                         'unit_code']].copy()
    bga_boil_units.dropna(subset=['boiler_id'], inplace=True)

    # merge the units with the boilers
    bga_unit_compilation = bga_gen_units.merge(bga_boil_units,
                                               how='outer',
                                               on=['plant_id_eia',
                                                   'report_date',
                                                   'unit_code'],
                                               indicator=True)
    # label the bga_source
    bga_unit_compilation. \
        loc[bga_unit_compilation['bga_source'].isnull(),
            'bga_source'] = 'unit_connection'
    bga_unit_compilation.drop(['_merge'], axis=1, inplace=True)
    bga_non_units = bga_compiled_2[bga_compiled_2['unit_code'].isnull()]

    # combine the unit compilation and the non units
    bga_compiled_3 = bga_non_units.append(bga_unit_compilation)

    # resort the records and the columns
    bga_compiled_3.sort_values(['plant_id_eia', 'report_date'], inplace=True)
    bga_compiled_3 = bga_compiled_3[['plant_id_eia',
                                     'report_date',
                                     'generator_id',
                                     'boiler_id',
                                     'unit_code',
                                     'bga_source',
                                     'net_generation_mwh',
                                     'missing_from_923']]

    # label plants that have 'bad' generator records (generators that have MWhs
    # in gens9 but don't have connected boilers) create a df with just the bad
    # plants by searching for the 'bad' generators
    bad_plants = bga_compiled_3[(bga_compiled_3['boiler_id'].isnull()) &
                                (bga_compiled_3['net_generation_mwh'] > 0)].\
        drop_duplicates(subset=['plant_id_eia', 'report_date'])
    bad_plants = bad_plants[['plant_id_eia', 'report_date']]

    # merge the 'bad' plants back into the larger frame
    bga_compiled_3 = bga_compiled_3.merge(bad_plants,
                                          how='outer',
                                          on=['plant_id_eia', 'report_date'],
                                          indicator=True)

    # use the indicator to create labels
    bga_compiled_3['plant_w_bad_generator'] = \
        np.where(bga_compiled_3._merge == 'both', True, False)
    # Note: At least one gen has reported MWh in 923, but could not be
    # programmatically mapped to a boiler

    # we don't need this one anymore
    bga_compiled_3 = bga_compiled_3.drop(['_merge'], axis=1)

    # create a label for generators that are unmapped but in 923
    bga_compiled_3['unmapped_but_in_923'] = \
        np.where((bga_compiled_3.boiler_id.isnull()) &
                 ~bga_compiled_3.missing_from_923 &
                 (bga_compiled_3.net_generation_mwh == 0),
                 True,
                 False)

    # create a label for generators that are unmapped
    bga_compiled_3['unmapped'] = np.where(bga_compiled_3.boiler_id.isnull(),
                                          True,
                                          False)
    bga_compiled_3 = bga_compiled_3.drop('net_generation_mwh', axis=1)
    bga_compiled_3.loc[bga_compiled_3.unit_code.isnull(), 'unit_code'] = None
    return(bga_compiled_3)


def heat_rate(bga, gen_eia923, bf_eia923, min_heat_rate=5.5):
    """Calculate heat rates (mmBTU/MWh) within separable generation units."""
    generation_w_boilers = \
        analysis.merge_on_date_year(gen_eia923, bga, how='left',
                                    on=['plant_id_eia', 'generator_id'])

    # Calculate net generation from all generators associated with each boiler
    gb1 = generation_w_boilers.groupby(
        by=['plant_id_eia', 'report_date', 'boiler_id'])
    gen_by_boiler = gb1.net_generation_mwh.sum().to_frame().reset_index()
    gen_by_boiler.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler'},
        inplace=True)

    # Calculate net generation per unique boiler generator combo
    gb2 = generation_w_boilers.groupby(
        by=['plant_id_eia', 'report_date', 'boiler_id', 'generator_id'])
    gen_by_bg = gb2.net_generation_mwh.sum().to_frame().reset_index()
    gen_by_bg.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler_gen'},
        inplace=True)

    # squish them together
    gen_by_bg_and_boiler = \
        pd.merge(gen_by_boiler, gen_by_bg,
                 on=['plant_id_eia', 'report_date', 'boiler_id'], how='left')
    # Bring in boiler fuel consumption and boiler generator associations
    bg = analysis.merge_on_date_year(bf_eia923, bga, how='left',
                                     on=['plant_id_eia', 'boiler_id'])
    # Merge boiler fuel consumption in with our per-boiler and boiler
    # generator combo net generation calculations
    bg = pd.merge(bg, gen_by_bg_and_boiler, how='left',
                  on=['plant_id_eia', 'report_date',
                      'boiler_id', 'generator_id'])

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
    bg_gb = bg.groupby(by=['plant_id_eia', 'report_date', 'generator_id'])
    bg = bg_gb.fuel_consumed_mmbtu_generator.sum().to_frame().reset_index()
    # Now that we have the fuel consumed per generator, bring the net
    # generation per generator back in:
    hr = pd.merge(bg, gen_eia923, how='left',
                  on=['plant_id_eia', 'report_date', 'generator_id'])

    # Finally, calculate heat rate
    hr['heat_rate_mmbtu_mwh'] = \
        hr['fuel_consumed_mmbtu_generator'] / \
        hr['net_generation_mwh']

    # Importing the plant association tag to filter out the
    # generators that are a part of plants that aren't in the bga table
    gens = gens_with_bga(bga, gen_eia923)
    # This is a per-generator table now -- so we don't want the boiler_id
    # And we only want the ones with complete associations.
    gens_assn = gens[gens['complete_assn']].drop('boiler_id', axis=1)
    hr = pd.merge(hr, gens_assn,
                  on=['plant_id_eia', 'report_date', 'generator_id'])

    # Only keep the generators with reasonable heat rates
    hr = hr[hr.heat_rate_mmbtu_mwh >= min_heat_rate]

    # Sort it a bit and clean up some types
    first_cols = [
        'report_date',
        'operator_id',
        'operator_name',
        'plant_id_eia',
        'plant_name',
        'generator_id'
    ]
    hr = outputs.organize_cols(hr, first_cols)
    hr['util_id_pudl'] = hr.util_id_pudl.astype(int)
    hr['operator_id'] = hr.operator_id.astype(int)
    hr = hr.sort_values(by=['operator_id', 'plant_id_eia',
                            'generator_id', 'report_date'])
    return(hr)


def fuel_cost(hr, frc_eia923, gen_eia923):
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
    """
    # Split up the plants on the basis of how many different primary energy
    # sources the component generators have:
    gen_w_ft = pd.merge(gen_eia923,
                        hr[['plant_id_eia', 'report_date', 'generator_id',
                            'fuel_type_pudl', 'fuel_type_count',
                            'heat_rate_mmbtu_mwh']],
                        how='inner',
                        on=['plant_id_eia', 'report_date', 'generator_id'])

    one_fuel = gen_w_ft[gen_w_ft.fuel_type_count == 1]
    multi_fuel = gen_w_ft[gen_w_ft.fuel_type_count > 1]

    # Bring the single fuel cost & generation information together for just
    # the one fuel plants:
    one_fuel = pd.merge(one_fuel, frc_eia923[['plant_id_eia', 'report_date',
                                              'fuel_cost_per_mmbtu',
                                              'fuel_type_pudl',
                                              'total_fuel_cost',
                                              'total_heat_content_mmbtu']],
                        how='left', on=['plant_id_eia', 'report_date'])
    # We need to retain the different energy_source information from the
    # generators (primary for the generator) and the fuel receipts (which is
    # per-delivery), and in the one_fuel case, there will only be a single
    # generator getting all of the fuels:
    one_fuel.rename(columns={'fuel_type_pudl_x': 'ftp_gen',
                             'fuel_type_pudl_y': 'ftp_frc'},
                    inplace=True)

    # Do the same thing for the multi fuel plants, but also merge based on
    # the different fuel types within the plant, so that we keep that info
    # as separate records:
    multi_fuel = pd.merge(multi_fuel,
                          frc_eia923[['plant_id_eia', 'report_date',
                                      'fuel_cost_per_mmbtu',
                                      'fuel_type_pudl']],
                          how='left', on=['plant_id_eia', 'report_date',
                                          'fuel_type_pudl'])

    # At this point, within each plant, we should have one record per
    # combination of generator & fuel type, which includes the heat rate of
    # each generator, as well as *plant* level fuel cost per unit heat input
    # for *each* fuel, which we can combine to figure out the fuel cost per
    # unit net electricity generation on a generator basis.

    # We have to do these calculations separately for the single and multi-fuel
    # plants because in the case of the one fuel plants we need to sum up all
    # of the fuel costs -- including both primary and secondary fuel
    # consumption -- whereas in the multi-fuel plants we are going to look at
    # fuel costs on a per-fuel basis (this is very close to being correct,
    # since secondary fuels are typically a fraction of a percent of the
    # plant's overall costs).

    one_fuel_gb = one_fuel.groupby(by=['report_date', 'plant_id_eia'])
    one_fuel_agg = one_fuel_gb.agg({
        'total_fuel_cost': np.sum,
        'total_heat_content_mmbtu': np.sum
    })
    one_fuel_agg['fuel_cost_per_mmbtu'] = \
        one_fuel_agg['total_fuel_cost'] / \
        one_fuel_agg['total_heat_content_mmbtu']
    one_fuel_agg = one_fuel_agg.reset_index()
    one_fuel = pd.merge(one_fuel[['plant_id_eia', 'report_date',
                                  'generator_id', 'heat_rate_mmbtu_mwh']],
                        one_fuel_agg[['plant_id_eia', 'report_date',
                                      'fuel_cost_per_mmbtu']],
                        on=['plant_id_eia', 'report_date'])
    one_fuel = one_fuel.drop_duplicates(
        subset=['plant_id_eia', 'report_date', 'generator_id'])

    multi_fuel = multi_fuel[['plant_id_eia', 'report_date', 'generator_id',
                             'fuel_cost_per_mmbtu', 'heat_rate_mmbtu_mwh']]

    fuel_cost = one_fuel.append(multi_fuel)
    fuel_cost['fuel_cost_per_mwh'] = \
        fuel_cost['fuel_cost_per_mmbtu'] * fuel_cost['heat_rate_mmbtu_mwh']
    fuel_cost = \
        fuel_cost.sort_values(['report_date', 'plant_id_eia', 'generator_id'])

    out_df = gen_w_ft.drop('heat_rate_mmbtu_mwh', axis=1)
    out_df = pd.merge(out_df.drop_duplicates(), fuel_cost,
                      on=['report_date', 'plant_id_eia', 'generator_id'])

    return(out_df)


def capacity_factor(gens_eia860, gen_eia923, min_cap_fact=0, max_cap_fact=1.5):
    """
    Calculate the capacity factor for each generator.

    Capacity Factor is calculated by using the net generation from eia923 and
    the nameplate capacity from eia860. The net gen and capacity are pulled
    into one dataframe, then the dates from that dataframe are pulled out to
    determine the hours in each period based on the frequency. The number of
    hours is used in calculating the capacity factor. Then the 'bad' records
    are dropped.
    """
    # infer the natural frequency of our input dataset:
    freq = pd.infer_freq(
        pd.DatetimeIndex(gen_eia923.report_date.unique()).sort_values()
    )
    # Only include columns to be used
    gens_eia860 = gens_eia860[['plant_id_eia',
                               'report_date',
                               'generator_id',
                               'nameplate_capacity_mw']]
    gen_eia923 = gen_eia923[['plant_id_eia',
                             'report_date',
                             'generator_id',
                             'net_generation_mwh']]

    # merge the generation and capacity to calculate capacity factor
    capacity_factor = analysis.merge_on_date_year(gen_eia923,
                                                  gens_eia860,
                                                  on=['plant_id_eia',
                                                      'generator_id'])

    # get a unique set of dates to generate the number of hours
    dates = capacity_factor['report_date'].drop_duplicates()
    dates_to_hours = pd.DataFrame(
        data={'report_date': dates,
              'hours': dates.apply(lambda d: (pd.date_range(d, periods=2,
                                                            freq=freq)[1] -
                                              pd.date_range(d, periods=2,
                                                            freq=freq)[0]) /
                                   pd.Timedelta(hours=1))})

    # merge in the hours for the calculation
    capacity_factor = capacity_factor.merge(dates_to_hours, on=['report_date'])

    # actually calculate capacity factor wooo!
    capacity_factor['capacity_factor'] = \
        capacity_factor['net_generation_mwh'] / \
        (capacity_factor['nameplate_capacity_mw'] * capacity_factor['hours'])

    # Replace unrealistic capacity factors with NaN
    capacity_factor.loc[capacity_factor['capacity_factor']
                        < min_cap_fact, 'capacity_factor'] = np.nan
    capacity_factor.loc[capacity_factor['capacity_factor']
                        >= max_cap_fact, 'capacity_factor'] = np.nan

    # drop the hours column, cause we don't need it anymore
    capacity_factor.drop(['hours'], axis=1, inplace=True)

    return(capacity_factor)


def mcoe(freq='AS', testing=False,
         start_date=None, end_date=None,
         min_heat_rate=5.5, min_cap_fact=0.0, max_cap_fact=1.5,
         output=None, debug=False):
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
    # Generation:
    gen_eia923 = outputs.generation_eia923(freq=freq, testing=testing,
                                           start_date=start_date,
                                           end_date=end_date)
    # Boiler Fuel Consumption:
    bf_eia923 = outputs.boiler_fuel_eia923(freq=freq, testing=testing,
                                           start_date=start_date,
                                           end_date=end_date)
    # Grab information about the individual generators:
    gens_eia860 = outputs.generators_eia860(testing=testing,
                                            start_date=start_date,
                                            end_date=end_date)
    # The proto-BGA:
    bga_eia860 = outputs.boiler_generator_assn_eia860(testing=testing,
                                                      start_date=start_date,
                                                      end_date=end_date)
    # The Boiler - Generator Associations:
    bga = boiler_generator_association(bga_eia860, gens_eia860,
                                       gen_eia923, bf_eia923,
                                       start_date=start_date,
                                       end_date=end_date,
                                       testing=testing)

    # Remove all associations tagged as bad for one reason or another
    bga_good = bga[~bga.missing_from_923 &
                   ~bga.plant_w_bad_generator &
                   ~bga.unmapped_but_in_923 &
                   ~bga.unmapped]

    bga_good = bga_good.drop(['missing_from_923',
                              'plant_w_bad_generator',
                              'unmapped_but_in_923',
                              'unmapped'], axis=1)
    bga_good = bga_good.drop_duplicates(subset=['report_date', 'plant_id_eia',
                                                'boiler_id', 'generator_id'])
    # Now, calculate the heat_rates on a per-generator basis:
    hr = heat_rate(bga_good, gen_eia923, bf_eia923,
                   min_heat_rate=min_heat_rate)

    # Merge just the primary energy source information into heat rate, since
    # we will need it to calculate the per-fuel costs, based on plant level
    # fuel receipts:
    hr = analysis.merge_on_date_year(
        hr,
        gens_eia860[['report_date', 'plant_id_eia', 'generator_id',
                     'fuel_type_pudl', 'fuel_type_count']],
        how='inner', on=['plant_id_eia', 'generator_id'])

    frc_eia923 = outputs.fuel_receipts_costs_eia923(freq=freq, testing=testing,
                                                    start_date=start_date,
                                                    end_date=end_date)
    # Calculate fuel costs by generator
    fc = fuel_cost(hr, frc_eia923, gen_eia923)
    # Calculate capacity factors by generator
    cf = capacity_factor(gens_eia860, gen_eia923,
                         min_cap_fact=min_cap_fact,
                         max_cap_fact=max_cap_fact)
    # Compile the above into a single dataframe for output/return.
    mcoe_out = pd.merge(fc,
                        cf[['report_date', 'plant_id_eia',
                            'generator_id', 'capacity_factor']],
                        on=['report_date', 'plant_id_eia', 'generator_id'],
                        how='left')

    return(hr, cf, fc, mcoe_out)
