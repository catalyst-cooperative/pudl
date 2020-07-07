"""A module with functions to aid generating MCOE."""
import pandas as pd

import pudl


def heat_rate_by_unit(pudl_out):
    """
    Calculate heat rates (mmBTU/MWh) within separable generation units.

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
    if pudl_out.freq is None:
        raise ValueError(
            "pudl_out must include a frequency for heat rate calculation")

    # Create a dataframe containing only the unit-generator mappings:
    bga_gens = pudl_out.bga()[['report_date',
                               'plant_id_eia',
                               'generator_id',
                               'unit_id_pudl']].drop_duplicates()
    # Merge those unit ids into the generation data:
    gen_w_unit = pudl.helpers.merge_on_date_year(
        pudl_out.gen_eia923(), bga_gens, on=['plant_id_eia', 'generator_id'])
    # Sum up the net generation per unit for each time period:
    gen_gb = gen_w_unit.groupby(['report_date',
                                 'plant_id_eia',
                                 'unit_id_pudl'])
    gen_by_unit = gen_gb.agg({'net_generation_mwh': pudl.helpers.sum_na})
    gen_by_unit = gen_by_unit.reset_index()

    # Create a dataframe containingonly the unit-boiler mappings:
    bga_boils = pudl_out.bga()[['report_date', 'plant_id_eia',
                                'boiler_id', 'unit_id_pudl']].drop_duplicates()
    # Merge those unit ids into the boiler fule consumption data:
    bf_w_unit = pudl.helpers.merge_on_date_year(
        pudl_out.bf_eia923(), bga_boils, on=['plant_id_eia', 'boiler_id'])
    # Sum up all the fuel consumption per unit for each time period:
    bf_gb = bf_w_unit.groupby(['report_date',
                               'plant_id_eia',
                               'unit_id_pudl'])
    bf_by_unit = bf_gb.agg({'total_heat_content_mmbtu': pudl.helpers.sum_na})
    bf_by_unit = bf_by_unit.reset_index()

    # Merge together the per-unit generation and fuel consumption data so we
    # can calculate a per-unit heat rate:
    hr_by_unit = pd.merge(gen_by_unit, bf_by_unit,
                          on=['report_date', 'plant_id_eia', 'unit_id_pudl'],
                          validate='one_to_one')
    hr_by_unit['heat_rate_mmbtu_mwh'] = \
        hr_by_unit.total_heat_content_mmbtu / hr_by_unit.net_generation_mwh

    return hr_by_unit


def heat_rate_by_gen(pudl_out):
    """Convert by-unit heat rate to by-generator, adding fuel type & count."""
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    if pudl_out.freq is None:
        raise ValueError(
            "pudl_out must include a frequency for heat rate calculation")

    bga_gens = pudl_out.bga()[['report_date',
                               'plant_id_eia',
                               'unit_id_pudl',
                               'generator_id']].drop_duplicates()
    # Associate those heat rates with individual generators. This also means
    # losing the net generation and fuel consumption information for now.
    hr_by_gen = pudl.helpers.merge_on_date_year(
        pudl_out.hr_by_unit()[['report_date', 'plant_id_eia',
                               'unit_id_pudl', 'heat_rate_mmbtu_mwh']],
        bga_gens, on=['plant_id_eia', 'unit_id_pudl']
    )
    hr_by_gen = hr_by_gen.drop('unit_id_pudl', axis=1)
    # Now bring information about generator fuel type & count
    hr_by_gen = pudl.helpers.merge_on_date_year(
        hr_by_gen,
        pudl_out.gens_eia860()[['report_date', 'plant_id_eia', 'generator_id',
                                'fuel_type_code_pudl', 'fuel_type_count']],
        on=['plant_id_eia', 'generator_id']
    )
    return hr_by_gen


def fuel_cost(pudl_out):
    """
    Calculate fuel costs per MWh on a per generator basis for MCOE.

    Fuel costs are reported on a per-plant basis, but we want to estimate them
    at the generator level. This is complicated by the fact that some plants
    have several different types of generators, using different fuels. We have
    fuel costs broken out by type of fuel (coal, oil, gas), and we know which
    generators use which fuel based on their energy_source_code and reported
    prime_mover. Coal plants use a little bit of natural gas or diesel to get
    started, but based on our analysis of the "pure" coal plants, this amounts
    to only a fraction of a percent of their overal fuel consumption on a
    heat content basis, so we're ignoring it for now.

    For plants whose generators all rely on the same fuel source, we simply
    attribute the fuel costs proportional to the fuel heat content consumption
    associated with each generator.

    For plants with more than one type of generator energy source, we need to
    split out the fuel costs according to fuel type -- so the gas fuel costs
    are associated with generators that have energy_source_code gas, and the
    coal fuel costs are associated with the generators that have
    energy_source_code coal.

    """
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    if pudl_out.freq is None:
        raise ValueError(
            "pudl_out must include a frequency for fuel cost calculation")

    # Split up the plants on the basis of how many different primary energy
    # sources the component generators have:
    hr_by_gen = pudl_out.hr_by_gen()[['plant_id_eia',
                                      'report_date',
                                      'generator_id',
                                      'heat_rate_mmbtu_mwh']]
    gens = pudl_out.gens_eia860()[['plant_id_eia',
                                   'report_date',
                                   'plant_name_eia',
                                   'plant_id_pudl',
                                   'generator_id',
                                   'utility_id_eia',
                                   'utility_name_eia',
                                   'utility_id_pudl',
                                   'fuel_type_count',
                                   'fuel_type_code_pudl']]

    gen_w_ft = pudl.helpers.merge_on_date_year(
        hr_by_gen, gens,
        on=['plant_id_eia', 'generator_id'],
        how='inner')

    one_fuel = gen_w_ft[gen_w_ft.fuel_type_count == 1]
    multi_fuel = gen_w_ft[gen_w_ft.fuel_type_count > 1]

    # Bring the single fuel cost & generation information together for just
    # the one fuel plants:
    one_fuel = pd.merge(one_fuel,
                        pudl_out.frc_eia923()[['plant_id_eia',
                                               'report_date',
                                               'fuel_cost_per_mmbtu',
                                               'fuel_type_code_pudl',
                                               'total_fuel_cost',
                                               'total_heat_content_mmbtu',
                                               'fuel_cost_from_eiaapi',
                                               ]],
                        how='left', on=['plant_id_eia', 'report_date'])
    # We need to retain the different energy_source_code information from the
    # generators (primary for the generator) and the fuel receipts (which is
    # per-delivery), and in the one_fuel case, there will only be a single
    # generator getting all of the fuels:
    one_fuel.rename(columns={'fuel_type_code_pudl_x': 'ftp_gen',
                             'fuel_type_code_pudl_y': 'ftp_frc'},
                    inplace=True)

    # Do the same thing for the multi fuel plants, but also merge based on
    # the different fuel types within the plant, so that we keep that info
    # as separate records:
    multi_fuel = pd.merge(multi_fuel,
                          pudl_out.frc_eia923()[['plant_id_eia',
                                                 'report_date',
                                                 'fuel_cost_per_mmbtu',
                                                 'fuel_type_code_pudl',
                                                 'fuel_cost_from_eiaapi', ]],
                          how='left', on=['plant_id_eia', 'report_date',
                                          'fuel_type_code_pudl'])

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
        'total_fuel_cost': pudl.helpers.sum_na,
        'total_heat_content_mmbtu': pudl.helpers.sum_na,
        'fuel_cost_from_eiaapi': 'any',
    })
    one_fuel_agg['fuel_cost_per_mmbtu'] = \
        one_fuel_agg['total_fuel_cost'] / \
        one_fuel_agg['total_heat_content_mmbtu']
    one_fuel_agg = one_fuel_agg.reset_index()
    one_fuel = pd.merge(
        one_fuel[['plant_id_eia', 'report_date', 'generator_id',
                  'heat_rate_mmbtu_mwh', 'fuel_cost_from_eiaapi']],
        one_fuel_agg[['plant_id_eia', 'report_date', 'fuel_cost_per_mmbtu']],
        on=['plant_id_eia', 'report_date'])
    one_fuel = one_fuel.drop_duplicates(
        subset=['plant_id_eia', 'report_date', 'generator_id'])

    multi_fuel = multi_fuel[['plant_id_eia', 'report_date', 'generator_id',
                             'fuel_cost_per_mmbtu', 'heat_rate_mmbtu_mwh',
                             'fuel_cost_from_eiaapi', ]]

    fuel_cost = one_fuel.append(multi_fuel, sort=True)
    fuel_cost['fuel_cost_per_mwh'] = \
        fuel_cost['fuel_cost_per_mmbtu'] * fuel_cost['heat_rate_mmbtu_mwh']
    fuel_cost = \
        fuel_cost.sort_values(['report_date', 'plant_id_eia', 'generator_id'])

    out_df = gen_w_ft.drop('heat_rate_mmbtu_mwh', axis=1)
    out_df = pd.merge(out_df.drop_duplicates(), fuel_cost,
                      on=['report_date', 'plant_id_eia', 'generator_id'])

    return out_df


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
    # pudl_out must have a freq, otherwise capacity factor will fail and merges
    # between tables with different frequencies will fail
    if pudl_out.freq is None:
        raise ValueError(
            "pudl_out must include a frequency for capacity factor calculation"
        )

    # Only include columns to be used
    gens_eia860 = pudl_out.gens_eia860()[['plant_id_eia',
                                          'report_date',
                                          'generator_id',
                                          'capacity_mw']]
    gen_eia923 = pudl_out.gen_eia923()[['plant_id_eia',
                                        'report_date',
                                        'generator_id',
                                        'net_generation_mwh']]

    # merge the generation and capacity to calculate capacity factor
    capacity_factor = pudl.helpers.merge_on_date_year(gen_eia923,
                                                      gens_eia860,
                                                      on=['plant_id_eia',
                                                          'generator_id'],
                                                      how='inner')

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
        (capacity_factor['capacity_mw'] * capacity_factor['hours'])

    # Replace unrealistic capacity factors with NaN
    capacity_factor = pudl.helpers.oob_to_nan(
        capacity_factor, ['capacity_factor'], lb=min_cap_fact, ub=max_cap_fact)

    # drop the hours column, cause we don't need it anymore
    capacity_factor.drop(['hours'], axis=1, inplace=True)

    return capacity_factor


def mcoe(pudl_out,
         min_heat_rate=5.5, min_fuel_cost_per_mwh=0.0,
         min_cap_fact=0.0, max_cap_fact=1.5):
    """
    Compile marginal cost of electricity (MCOE) at the generator level.

    Use data from EIA 923, EIA 860, and (eventually) FERC Form 1 to estimate
    the MCOE of individual generating units. The calculation is performed at
    the time resolution, and for the period indicated by the pudl_out object.
    that is passed in.

    Args:
        pudl_out: a PudlTabl object, specifying the time resolution and
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
        pandas.DataFrame: a dataframe organized by date and generator,
        with lots of juicy information about the generators -- including fuel
        cost on a per MWh and MMBTU basis, heat rates, and net generation.

    """
    # because lots of these input dfs include same info columns, this generates
    # drop columnss for fuel_cost. This avoids needing to hard code columns.
    merge_cols = ['plant_id_eia', 'generator_id', 'report_date']
    drop_cols = [x for x in pudl_out.gens_eia860().columns
                 if x in pudl_out.fuel_cost().columns and x not in merge_cols]
    # start with the generators table so we have all of the generators
    mcoe_out = pudl.helpers.merge_on_date_year(
        pudl_out.fuel_cost().drop(drop_cols, axis=1),
        pudl_out.gens_eia860(),
        on=[x for x in merge_cols if x != 'report_date'],
        how='inner',
    )
    # Bring together the fuel cost and capacity factor dataframes, which
    # also include heat rate information.
    mcoe_out = pd.merge(
        mcoe_out,
        pudl_out.capacity_factor(min_cap_fact=min_cap_fact,
                                 max_cap_fact=max_cap_fact)[
            ['report_date', 'plant_id_eia',
             'generator_id', 'capacity_factor', 'net_generation_mwh']],
        on=['report_date', 'plant_id_eia', 'generator_id'],
        how='outer')

    # Bring the PUDL Unit IDs into the output dataframe so we can see how
    # the generators are really grouped.
    mcoe_out = pudl.helpers.merge_on_date_year(
        mcoe_out,
        pudl_out.bga()[['report_date',
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

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'unit_id_pudl',
                  'generator_id',
                  'plant_name_eia',
                  'utility_id_eia',
                  'utility_id_pudl',
                  'utility_name_eia']
    mcoe_out = pudl.helpers.organize_cols(mcoe_out, first_cols)
    mcoe_out = mcoe_out.sort_values(
        ['plant_id_eia', 'unit_id_pudl', 'generator_id', 'report_date']
    )

    # Filter the output based on the range of validity supplied by the user:
    mcoe_out = pudl.helpers.oob_to_nan(mcoe_out, ['heat_rate_mmbtu_mwh'],
                                       lb=min_heat_rate, ub=None)
    mcoe_out = pudl.helpers.oob_to_nan(mcoe_out, ['fuel_cost_per_mwh'],
                                       lb=min_fuel_cost_per_mwh, ub=None)
    mcoe_out = pudl.helpers.oob_to_nan(mcoe_out, ['capacity_factor'],
                                       lb=min_cap_fact, ub=max_cap_fact)
    return mcoe_out
