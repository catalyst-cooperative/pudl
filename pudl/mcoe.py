"""A module with functions to aid generating MCOE."""

from pudl import analysis, outputs, init
from pudl import constants as pc
import numpy as np
import pandas as pd
import networkx as nx


def boiler_generator_association(pudl_out, debug=False, verbose=False):
    """
    Temporary function to create more complete boiler generator associations.

    Creates a unique unit_id_pudl for each collection of boilers and generators
    within a plant that have ever been associated with each other, based on
    the boiler generator associations reported in EIA860. Unfortunately, this
    information is not complete for years before 2014, as the gas turbine
    portion of combined cycle power plants in those earlier years were not
    reporting their fuel consumption, or existence as part of the plants.

    For years 2014 and on, EIA860 contains a unit_code value, allowing the
    combined cycle plant compoents to be associated with each other. For many
    plants not listed in the reported boiler generator associations, it is
    nonetheless possible to associate boilers and generators on a one-to-one
    basis, as they use identical strings to describe the units.

    In the end, between the reported BGA table, the string matching, and the
    unit_code values, it's possible to create a nearly complete mapping of the
    generation units, at least for 2014 and later.

    Ultimately, this work needs to be ported into the ingest/load step for
    the PUDL DB, creating a valuable 'glue' table allowing the boilers and
    generators to be associated by unit_id easily.

    For now, it is done in the context of the MCOE calculation, within the
    pudl_out PudlOutput object that is passed in as the only positional
    argument.

    Args:
        pudl_out: a PudlOutput object, which can obtain the dataframes needed
            to infer the boiler generator associations.
        debug (bool): If True, include columns in the returned dataframe
            indicating by what method the individual boiler generator
            associations were inferred.

    Returns:
        bga_out (DataFrame): A dataframe containing plant_id_eia, generator_id,
            boiler_id, and unit_id_pudl
    """
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    assert pudl_out.freq is not None,\
        "pudl_out must inclue a frequency for mcoe"

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
    bf_eia923 = bf_eia923_gb.agg({
        'total_heat_content_mmbtu': analysis.sum_na,
    }).reset_index()

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


def heat_rate_by_unit(pudl_out, verbose=False):
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
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    assert pudl_out.freq is not None,\
        "pudl_out must inclue a frequency for mcoe"

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
    gen_by_unit = gen_gb.agg({'net_generation_mwh': analysis.sum_na})
    gen_by_unit = gen_by_unit.reset_index()

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
    bf_by_unit = bf_gb.agg({'total_heat_content_mmbtu': analysis.sum_na})
    bf_by_unit = bf_by_unit.reset_index()

    # Merge together the per-unit generation and fuel consumption data so we
    # can calculate a per-unit heat rate:
    hr_by_unit = pd.merge(gen_by_unit, bf_by_unit,
                          on=['report_date', 'plant_id_eia', 'unit_id_pudl'],
                          validate='one_to_one')
    hr_by_unit['heat_rate_mmbtu_mwh'] = \
        hr_by_unit.total_heat_content_mmbtu / hr_by_unit.net_generation_mwh

    return(hr_by_unit)


def heat_rate_by_gen(pudl_out, verbose=False):
    """Convert by-unit heat rate to by-generator, adding fuel type & count."""
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    assert pudl_out.freq is not None,\
        "pudl_out must inclue a frequency for mcoe"

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


def fuel_cost(pudl_out, verbose=False):
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
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    assert pudl_out.freq is not None,\
        "pudl_out must inclue a frequency for mcoe"

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
        'total_fuel_cost': analysis.sum_na,
        'total_heat_content_mmbtu': analysis.sum_na
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


def capacity_factor(pudl_out, min_cap_fact=0, max_cap_fact=1.5, verbose=False):
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
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    assert pudl_out.freq is not None,\
        "pudl_out must inclue a frequency for mcoe"

    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    assert pudl_out.freq is not None,\
        "pudl_out must inclue a frequency for mcoe"
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
         min_cap_fact=0.0, max_cap_fact=1.5, verbose=False):
    """
    Compile marginal cost of electricity (MCOE) at the generator level.

    Use data from EIA 923, EIA 860, and (eventually) FERC Form 1 to estimate
    the MCOE of individual generating units. The calculation is performed at
    the time resolution, and for the period indicated by the pudl_out object.
    that is passed in.

    Args:
        pudl_out: a PudlOutput object, specifying the time resolution and
            date range for which the calculations should be performed.
        min_heat_rate: lowest plausible heat rate, in mmBTU/MWh. Any MCOE
            records with lower heat rates are presumed to be invalid, and are
            discarded before returning.
        min_cap_fact, max_cap_fact: minimum & maximum generator capacity
            factor. Generator records with a lower capacity factor will be
            filtered out before returning. This allows the user to exclude
            generators that aren't being used enough to have valid.
        min_fuel_cost_per_mwh: minimum fuel cost on a per MWh basis that is
            required for a generator record to be considered valid. For some
            reason there are now a large number of $0 fuel cost records, which
            previously would have been NaN.

    Returns:
        mcoe_out: a dataframe organized by date and generator, with lots of
        juicy information about the generators -- including fuel cost on a per
        MWh and MMBTU basis, heat rates, and neg generation.
    """
    # Bring together the fuel cost and capacity factor dataframes, which
    # also include heat rate information.
    mcoe_out = pd.merge(pudl_out.fuel_cost(verbose=verbose),
                        pudl_out.capacity_factor(verbose=verbose)[
                            ['report_date', 'plant_id_eia',
                             'generator_id', 'capacity_factor']],
                        on=['report_date', 'plant_id_eia', 'generator_id'],
                        how='left')

    # Bring the PUDL Unit IDs into the output dataframe so we can see how
    # the generators are really grouped.
    mcoe_out = analysis.merge_on_date_year(
        mcoe_out,
        pudl_out.bga(verbose=verbose)[['report_date',
                                       'plant_id_eia',
                                       'unit_id_pudl',
                                       'generator_id']].drop_duplicates(),
        how='left',
        on=['plant_id_eia', 'generator_id'])

    # Instead of getting the total MMBTU through this multiplication... we
    # could also calculate the total fuel consumed on a per-unit basis, from
    # the boiler_fuel table, and then determine what proportion should be
    # distributed to each generator based on its heat-rate and net generation.
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
