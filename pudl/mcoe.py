"""A module with functions to aid generating MCOE."""

from pudl import analysis, clean_pudl, outputs, init
from pudl import constants as pc
import numpy as np
import pandas as pd
import networkx as nx


def boiler_generator_association(pudl_out, debug=False):
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
    # compile and scrub all the parts
    bga_eia860 = pudl_out.bga_eia860().drop_duplicates(['plant_id_eia',
                                                        'boiler_id',
                                                        'generator_id',
                                                        'report_date'])
    bga_eia860 = bga_eia860.drop(['id', 'operator_id'], axis=1)

    gen_eia923 = pudl_out.gen_eia923()
    gen_eia923 = gen_eia923.set_index(pd.DatetimeIndex(gen_eia923.report_date))
    gen_eia923_gb = gen_eia923.groupby(
        [pd.Grouper(freq='AS'), 'plant_id_eia', 'generator_id'])
    gen_eia923 = gen_eia923_gb['net_generation_mwh'].sum().reset_index()
    gen_eia923['missing_from_923'] = False

    # The generator records that are missing from 860 but appear in 923
    # I created issue no. 128 to deal with this at a later date
    merged = pd.merge(pudl_out.gens_eia860(), gen_eia923,
                      on=['plant_id_eia', 'report_date', 'generator_id'],
                      indicator=True, how='outer')
    missing = merged[merged['_merge'] == 'right_only']

    # compile all of the generators
    gens = pd.merge(gen_eia923, pudl_out.gens_eia860(),
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

    bf_eia923 = pudl_out.bf_eia923()
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
    bga_out = bga_compiled_3.drop('net_generation_mwh', axis=1)
    bga_out.loc[bga_out.unit_code.isnull(), 'unit_code'] = None

    bga_for_nx = bga_out[['plant_id_eia', 'report_date', 'generator_id',
                          'boiler_id', 'unit_code']]
    # If there's no boiler... there's no boiler-generator association
    bga_for_nx = bga_for_nx.dropna(subset=['boiler_id']).drop_duplicates()

    # Need boiler & generator specific ID strings, or they look like
    # the same node to NX
    bga_for_nx['generators'] = 'p' + bga_for_nx.plant_id_eia.astype(str) + \
                               '_g' + bga_for_nx.generator_id.astype(str)
    bga_for_nx['boilers'] = 'p' + bga_for_nx.plant_id_eia.astype(str) + \
                            '_b' + bga_for_nx.boiler_id.astype(str)

    # dataframe to accumulate the unit_ids in
    bga_w_units = pd.DataFrame()
    # We want to start our unit_id counter anew for each plant:
    for pid in bga_for_nx.plant_id_eia.unique():
        bga_byplant = bga_for_nx[bga_for_nx.plant_id_eia == pid].copy()

        # Create a graph from the dataframe of boilers and generators. It's a
        # multi-graph, meaning the same nodes can be connected by more than one
        # edge -- this allows us to preserve multiple years worth of boiler
        # generator association information for later inspection if need be:
        bga_graph = nx.from_pandas_edgelist(bga_byplant,
                                            source='generators',
                                            target='boilers',
                                            edge_attr=True,
                                            create_using=nx.MultiGraph())

        # Each connected sub-graph is a generation unit:
        gen_units = list(nx.connected_component_subgraphs(bga_graph))

        # Assign a unit_id to each subgraph, and extract edges into a dataframe
        for unit_id, unit in zip(range(len(gen_units)), gen_units):
            # All the boiler-generator association graphs should be bi-partite,
            # meaning generators only connect to boilers, and boilers only
            # connect to generators.
            assert nx.algorithms.bipartite.is_bipartite(unit), \
                """Non-bipartite generation unit graph found.
    plant_id_eia={}, unit_id_pudl={}.""".format(pid, unit_id)
            nx.set_edge_attributes(
                unit, name='unit_id_pudl', values=unit_id + 1)
            new_unit_df = nx.to_pandas_edgelist(unit)
            bga_w_units = bga_w_units.append(new_unit_df)

    bga_w_units = bga_w_units.sort_values(['plant_id_eia', 'unit_id_pudl',
                                           'generator_id', 'boiler_id'])
    bga_w_units = bga_w_units.drop(['source', 'target'], axis=1)

    # Check whether the PUDL unit_id values we've inferred conflict with
    # the unit_code values that were reported to EIA. Are there any PUDL
    # unit_id values that have more than 1 EIA unit_code within them?
    bga_unit_code_counts = \
        bga_w_units.groupby(['plant_id_eia', 'unit_id_pudl'])['unit_code'].\
        nunique().to_frame().reset_index()
    bga_unit_code_counts = bga_unit_code_counts.rename(
        columns={'unit_code': 'unit_code_count'})
    bga_unit_code_counts = pd.merge(bga_w_units, bga_unit_code_counts,
                                    on=['plant_id_eia', 'unit_id_pudl'])
    too_many_codes = \
        bga_unit_code_counts[bga_unit_code_counts.unit_code_count > 1]
    too_many_codes = \
        too_many_codes[~too_many_codes.unit_code.isnull()].\
        groupby(['plant_id_eia', 'unit_id_pudl'])['unit_code'].unique()
    print('WARNING: multiple EIA unit codes found in these PUDL units:')
    print(too_many_codes)
    bga_w_units = bga_w_units.drop('unit_code', axis=1)

    # These assertions test that all boilers and generators ended up in the
    # same unit_id across all the years of reporting:
    assert (bga_w_units.groupby(
        ['plant_id_eia', 'generator_id'])['unit_id_pudl'].nunique() == 1).all()
    assert (bga_w_units.groupby(
        ['plant_id_eia', 'boiler_id'])['unit_id_pudl'].nunique() == 1).all()
    bga_w_units = bga_w_units.drop('report_date', axis=1)
    bga_w_units = bga_w_units[['plant_id_eia', 'unit_id_pudl',
                               'generator_id', 'boiler_id']].drop_duplicates()
    bga_out = pd.merge(bga_out, bga_w_units, how='left',
                       on=['plant_id_eia', 'generator_id', 'boiler_id'])
    bga_out['unit_id_pudl'] = \
        bga_out['unit_id_pudl'].fillna(value=0).astype(int)

    if not debug:
        bga_out = bga_out[~bga_out.missing_from_923 &
                          ~bga_out.plant_w_bad_generator &
                          ~bga_out.unmapped_but_in_923 &
                          ~bga_out.unmapped]

        bga_out = bga_out.drop(['missing_from_923',
                                'plant_w_bad_generator',
                                'unmapped_but_in_923',
                                'unmapped'], axis=1)
        bga_out = bga_out.drop_duplicates(subset=['report_date',
                                                  'plant_id_eia',
                                                  'boiler_id',
                                                  'generator_id'])
    return(bga_out)


def heat_rate_by_unit(pudl_out):
    """Calculate heat rates (mmBTU/MWh) within separable generation units.

    Assumes a "good" Boiler Generator Association (bga) i.e. one that only
    contains boilers and generators which have been completely associated at
    some point in the past.

    The BGA dataframe needs to have the following columns:
     - report_date (annual)
     - plant_id_eia
     - unit_id_pudl
     - generator_id
     - boiler_id

    The unit_id is associated with generation records based on report_date,
    plant_id_eia, and generator_id. Analogously, the unit_id is associtated
    with boiler fuel consumption records based on report_date, plant_id_eia,
    and boiler_id.

    Then the total net generation and fuel consumption per unit per time period
    are calculated, allowing the calculation of a per unit heat rate. That
    per unit heat rate is returned in a dataframe containing:
     - report_date
     - plant_id_eia
     - unit_id_pudl
     - net_generation_mwh
     - total_heat_content_mmbtu
     - heat_rate_mmbtu_mwh
    """
    # Create a dataframe containing only the unit-generator mappings:
    bga_gens = pudl_out.bga()[['report_date',
                               'plant_id_eia',
                               'generator_id',
                               'unit_id_pudl']].drop_duplicates()
    # Merge those unit ids into the generation data:
    gen_w_unit = analysis.merge_on_date_year(
        pudl_out.gen_eia923(), bga_gens, on=['plant_id_eia', 'generator_id'])
    # Sum up the net generation per unit for each time period:
    gen_gb = gen_w_unit.groupby(['report_date',
                                 'plant_id_eia',
                                 'unit_id_pudl'])
    gen_by_unit = gen_gb['net_generation_mwh'].sum().reset_index()

    # Create a dataframe containingonly the unit-boiler mappings:
    bga_boils = pudl_out.bga()[['report_date', 'plant_id_eia',
                                'boiler_id', 'unit_id_pudl']].drop_duplicates()
    # Merge those unit ids into the boiler fule consumption data:
    bf_w_unit = analysis.merge_on_date_year(
        pudl_out.bf_eia923(), bga_boils, on=['plant_id_eia', 'boiler_id'])
    # Sum up all the fuel consumption per unit for each time period:
    bf_gb = bf_w_unit.groupby(['report_date',
                               'plant_id_eia',
                               'unit_id_pudl'])
    bf_by_unit = bf_gb['total_heat_content_mmbtu'].sum().reset_index()

    # Merge together the per-unit generation and fuel consumption data so we
    # can calculate a per-unit heat rate:
    hr_by_unit = pd.merge(gen_by_unit, bf_by_unit,
                          on=['report_date', 'plant_id_eia', 'unit_id_pudl'],
                          validate='one_to_one')
    hr_by_unit['heat_rate_mmbtu_mwh'] = \
        hr_by_unit.total_heat_content_mmbtu / hr_by_unit.net_generation_mwh

    return(hr_by_unit)


def heat_rate_by_gen(pudl_out):
    """Convert by-unit heat rate to by-generator, adding fuel type & count."""
    gens_simple = pudl_out.gens_eia860()[['report_date', 'plant_id_eia',
                                          'generator_id', 'fuel_type_pudl']]
    bga_gens = pudl_out.bga()[['report_date',
                               'plant_id_eia',
                               'unit_id_pudl',
                               'generator_id']].drop_duplicates()
    gens_simple = pd.merge(gens_simple, bga_gens,
                           on=['report_date', 'plant_id_eia', 'generator_id'],
                           validate='one_to_one')
    # Associate those heat rates with individual generators. This also means
    # losing the net generation and fuel consumption information for now.
    hr_by_gen = analysis.merge_on_date_year(
        pudl_out.heat_rate_by_unit()[['report_date', 'plant_id_eia',
                                      'unit_id_pudl', 'heat_rate_mmbtu_mwh']],
        bga_gens, on=['plant_id_eia', 'unit_id_pudl']
    )
    hr_by_gen = hr_by_gen.drop('unit_id_pudl', axis=1)
    # Now bring information about generator fuel type & count
    hr_by_gen = analysis.merge_on_date_year(
        hr_by_gen,
        pudl_out.gens_eia860()[['report_date', 'plant_id_eia', 'generator_id',
                                'fuel_type_pudl', 'fuel_type_count']],
        on=['plant_id_eia', 'generator_id']
    )
    return(hr_by_gen)


def fuel_cost(pudl_out):
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
    gen_w_ft = pd.merge(pudl_out.gen_eia923(),
                        pudl_out.heat_rate_by_gen()[['plant_id_eia',
                                                     'report_date',
                                                     'generator_id',
                                                     'fuel_type_pudl',
                                                     'fuel_type_count',
                                                     'heat_rate_mmbtu_mwh']],
                        how='inner',
                        on=['plant_id_eia', 'report_date', 'generator_id'])

    one_fuel = gen_w_ft[gen_w_ft.fuel_type_count == 1]
    multi_fuel = gen_w_ft[gen_w_ft.fuel_type_count > 1]

    # Bring the single fuel cost & generation information together for just
    # the one fuel plants:
    one_fuel = pd.merge(one_fuel,
                        pudl_out.frc_eia923()[['plant_id_eia',
                                               'report_date',
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
                          pudl_out.frc_eia923()[['plant_id_eia',
                                                 'report_date',
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


def capacity_factor(pudl_out, min_cap_fact=0, max_cap_fact=1.5):
    """
    Calculate the capacity factor for each generator.

    Capacity Factor is calculated by using the net generation from eia923 and
    the nameplate capacity from eia860. The net gen and capacity are pulled
    into one dataframe, then the dates from that dataframe are pulled out to
    determine the hours in each period based on the frequency. The number of
    hours is used in calculating the capacity factor. Then records with
    capacity factors outside the range specified by min_cap_fact and
    max_cap_fact are dropped.
    """
    # Only include columns to be used
    gens_eia860 = pudl_out.gens_eia860()[['plant_id_eia',
                                          'report_date',
                                          'generator_id',
                                          'nameplate_capacity_mw']]
    gen_eia923 = pudl_out.gen_eia923()[['plant_id_eia',
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
              'hours': dates.apply(
                  lambda d: (
                      pd.date_range(d, periods=2, freq=pudl_out.freq)[1] -
                      pd.date_range(d, periods=2, freq=pudl_out.freq)[0]) /
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


def mcoe(pudl_out,
         min_heat_rate=5.5, min_fuel_cost_per_mwh=0.0,
         min_cap_fact=0.0, max_cap_fact=1.5):
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
    # Compile the above into a single dataframe for output/return.
    mcoe_out = pd.merge(pudl_out.fuel_cost(),
                        pudl_out.capacity_factor()[['report_date',
                                                    'plant_id_eia',
                                                    'generator_id',
                                                    'capacity_factor']],
                        on=['report_date', 'plant_id_eia', 'generator_id'],
                        how='left')

    # Bring the PUDL Unit IDs into the output dataframe.
    mcoe_out = analysis.merge_on_date_year(
        mcoe_out,
        pudl_out.bga()[['report_date', 'plant_id_eia',
                        'unit_id_pudl', 'generator_id']].drop_duplicates(),
        how='left',
        on=['plant_id_eia', 'generator_id'])

    mcoe_out['total_mmbtu'] = \
        mcoe_out.net_generation_mwh * mcoe_out.heat_rate_mmbtu_mwh
    mcoe_out['total_fuel_cost'] = \
        mcoe_out.total_mmbtu * mcoe_out.fuel_cost_per_mmbtu

    simplified_gens_eia860 = pudl_out.gens_eia860().drop([
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'fuel_type_count',
        'fuel_type_pudl'
    ], axis=1)
    mcoe_out = analysis.merge_on_date_year(mcoe_out, simplified_gens_eia860,
                                           on=['plant_id_eia',
                                               'generator_id'])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'unit_id_pudl',
                  'generator_id',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name']
    mcoe_out = outputs.organize_cols(mcoe_out, first_cols)
    mcoe_out = mcoe_out.sort_values(
        ['plant_id_eia', 'unit_id_pudl', 'generator_id', 'report_date']
    )

    # Filter the output based on the range of validity supplied by the user:
    if min_heat_rate is not None:
        mcoe_out = mcoe_out[mcoe_out.heat_rate_mmbtu_mwh >= min_heat_rate]
    if min_fuel_cost_per_mwh is not None:
        mcoe_out = mcoe_out[mcoe_out.fuel_cost_per_mwh > min_fuel_cost_per_mwh]
    if min_cap_fact is not None:
        mcoe_out = mcoe_out[mcoe_out.capacity_factor >= min_cap_fact]
    if max_cap_fact is not None:
        mcoe_out = mcoe_out[mcoe_out.capacity_factor <= max_cap_fact]

    return(mcoe_out)
