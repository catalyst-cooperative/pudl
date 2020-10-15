"""hello."""

import logging

# Useful high-level external modules.
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

IDX_GENS = ['plant_id_eia', 'generator_id', 'report_date']
"""Id columns for generators."""

IDX_PM_FUEL = ['plant_id_eia', 'prime_mover_code',
               'fuel_type', 'report_date']
"""Id columns for plant, prime mover & fuel type records."""


def allocate_gen_fuel_by_gen(pudl_out):
    """
    Allocate gen fuel data columns to generators.

    The generation_fuel_eia923 table includes net generation and fuel
    consumption data at the plant/fuel type/prime mover level. The most
    granular level of plants that PUDL typically uses is at the plant/generator
    level. This method converts the generation_fuel_eia923 table to the level
    of plant/generators.

    Args:
        pudl_out

    Returns:
        pandas.DataFrame
    """
    gen_pm_fuel = allocate_gen_fuel_by_gen_pm_fuel(pudl_out)
    gen = agg_by_generator(gen_pm_fuel)
    _test_gen_fuel_allocation(pudl_out, gen)
    return gen


def allocate_gen_fuel_by_gen_pm_fuel(pudl_out):
    """
    Proportionally allocate net gen from gen_fuel table to generators.

    Two main steps here:
     * associated gen_fuel data w/ generators
     * allocate gen_fuel data proportionally

    Args:
        pudl_out

    Returns:
        pandas.DataFrame
    """
    gens_asst = associate_gen_tables(pudl_out)
    # get the total values for the merge group
    gen_pm_fuel = (
        pd.merge(
            gens_asst,
            gens_asst.groupby(by=IDX_PM_FUEL)
            [['net_generation_mwh_gen', 'capacity_mw']]
            .sum(min_count=1)
            .add_suffix('_pm_fuel_total')
            .reset_index(),
            on=IDX_PM_FUEL,
        )
        .pipe(_associate_unconnected_records)
    )

    # do the allocating-ing!
    gen_pm_fuel = (
        gen_pm_fuel.assign(
            # we could condense these remaining cols into one... but let's keep
            # it for debuging for now
            gen_ratio_net_gen=lambda x: x.net_generation_mwh_gen / \
            x.net_generation_mwh_gen_pm_fuel_total,
            gen_ratio_cap=lambda x: x.capacity_mw / x.capacity_mw_pm_fuel_total,
            gen_ratio=lambda x:
                np.where(
                    x.gen_ratio_net_gen.notnull() | x.gen_ratio_net_gen != 0,
                    x.gen_ratio_net_gen, x.gen_ratio_cap),
            net_generation_mwh=lambda x: x.net_generation_mwh_gf * x.gen_ratio,
            fuel_consumed_mmbtu_gf=lambda x: x.fuel_consumed_mmbtu,
            fuel_consumed_mmbtu=lambda x: x.fuel_consumed_mmbtu * x.gen_ratio
        )
    )

    gen_pm_fuel = (
        gen_pm_fuel.astype(
            {"plant_id_eia": "Int64",
             "net_generation_mwh": "float"})
        .pipe(_test_generator_output, pudl_out)
    )
    return gen_pm_fuel


def agg_by_generator(gen_pm_fuel):
    """
    Aggreate the allocated gen fuel data to the generator level.

    Args:
        gen_pm_fuel (pandas.DataFrame): result of
            `allocate_gen_fuel_by_gen_pm_fuel()`
    """
    data_cols = ['net_generation_mwh', 'fuel_consumed_mmbtu']
    gen = (gen_pm_fuel.groupby(by=IDX_GENS)
           [data_cols].sum(min_count=1).reset_index())
    return gen


def _stack_generators(pudl_out, idx_stack, cols_to_stack,
                      cat_col='energy_source_code_num',
                      stacked_col='fuel_type'):
    """
    Stack the generator table with a set of columns.

    Args:
        pudl_out
        idx_stack (iterable): list of columns. index to stack based on
        cols_to_stack (iterable): list of columns to stack
        cat_col (string): name of category column which will end up having the
            column names of cols_to_stack
        stacked_col (string): name of column which will end up with the stacked
            data from cols_to_stack

    Returns:
        pandas.DataFrame: a dataframe with these columns: idx_stack, cat_col,
        stacked_col
    """
    gens = pudl_out.gens_eia860()
    gens_stack_prep = (
        pd.DataFrame(gens.set_index(idx_stack)[cols_to_stack].stack(level=0))
        .reset_index()
        .rename(columns={'level_3': cat_col, 0: stacked_col})
    )
    # merge the stacked df back onto the gens table
    # we first drop the cols_to_stack so we don't duplicate data
    gens_stack = pd.merge(
        gens.drop(columns=cols_to_stack),
        gens_stack_prep,
        how='outer'
    )
    return gens_stack


def associate_gen_tables(pudl_out):
    """
    Assocaite the three tables needed to assign net gen to generators.

    Args:
        pudl_out
    """
    esc = [
        'energy_source_code_1', 'energy_source_code_2', 'energy_source_code_3',
        'energy_source_code_4', 'energy_source_code_5', 'energy_source_code_6'
    ]

    stack_gens = _stack_generators(
        pudl_out, idx_stack=IDX_GENS, cols_to_stack=esc,
        cat_col='energy_source_code_num', stacked_col='fuel_type')

    # because lots of these input dfs include same info columns, this generates
    # drop columnss for fuel_cost. This avoids needing to hard code columns.
    drop_cols_gens = [x for x in stack_gens.columns
                      if x in pudl_out.gen_eia923().columns
                      and x not in IDX_GENS]
    gens_asst = (
        pd.merge(
            stack_gens,
            pudl_out.gen_eia923().drop(columns=drop_cols_gens),
            on=IDX_GENS,
            how='outer')
        .merge(
            pudl_out.gf_eia923().groupby(by=IDX_PM_FUEL)
            .sum(min_count=1).reset_index(),
            on=IDX_PM_FUEL,
            suffixes=('_gen', '_gf'),
            how='outer',
        )
    )
    return gens_asst


def _associate_unconnected_records(eia_generators_merged):
    """
    Associate unassocaited gen_fuel table records on idx_pm.

    Args:
        eia_generators_merged (pandas.DataFrame)
    """
    idx_pm = ['plant_id_eia', 'prime_mover_code',
              'energy_source_code_num', 'report_date', ]
    # we're going to only associate these unconnected fuel records w/
    # the primary fuel so we don't have to deal w/ double counting
    connected_mask = eia_generators_merged.generator_id.notnull()
    eia_generators_connected = (
        eia_generators_merged[connected_mask]
    )
    eia_generators_unconnected = (
        eia_generators_merged[~connected_mask]
        .dropna(axis='columns', how='all')
        .rename(columns={'fuel_type': 'fuel_type_unconnected'})
        .assign(energy_source_code_num='fuel_type')
        .groupby(by=idx_pm).sum(min_count=1)
        .reset_index()
    )

    eia_generators = (
        pd.merge(
            eia_generators_connected,
            eia_generators_unconnected,
            on=idx_pm,
            suffixes=('', '_unconnected'),
            how='outer'
        )
        .assign(
            net_generation_mwh_gf=lambda x: np.where(
                x.net_generation_mwh_gf.notnull()
                | x.net_generation_mwh_gf_unconnected.notnull(),
                x.net_generation_mwh_gf.fillna(0)
                + x.net_generation_mwh_gf_unconnected.fillna(0),
                pd.NA
            ),
            fuel_consumed_mmbtu=lambda x: x.fuel_consumed_mmbtu.fillna(
                0) + x.fuel_consumed_mmbtu_unconnected.fillna(0)
        )
    )
    return eia_generators


def _test_generator_output(eia_generators, pudl_out):
    # this is just for testing/debugging
    eia_generators = (
        pd.merge(
            eia_generators,
            eia_generators.groupby(by=IDX_PM_FUEL)
            [['net_generation_mwh']]
            .sum(min_count=1).add_suffix('_test').reset_index(),
            on=IDX_PM_FUEL,
            how='outer'
        )
        .assign(net_generation_mwh_diff=lambda x:
                x.net_generation_mwh_gf
                - x.net_generation_mwh_test)
    )
    no_cap_gen = eia_generators[
        (eia_generators.capacity_mw.isnull())
        & (eia_generators.net_generation_mwh_gen.isnull())
    ]
    if len(no_cap_gen) > 15:
        logger.info(
            f'Warning: {len(no_cap_gen)} records have no capacity or net gen')
    gen_fuel = pudl_out.gf_eia923()
    gen = pudl_out.gen_eia923()
    # remove the junk/corrective plants
    fuel_net_gen = gen_fuel[gen_fuel.plant_id_eia !=
                            '99999'].net_generation_mwh.sum()
    fuel_consumed = gen_fuel[gen_fuel.plant_id_eia !=
                             '99999'].fuel_consumed_mmbtu.sum()
    logger.info(
        "gen v fuel table net gen diff:      "
        f"{(gen.net_generation_mwh.sum())/fuel_net_gen:.1%}")
    logger.info(
        "new v fuel table net gen diff:      "
        f"{(eia_generators.net_generation_mwh.sum())/fuel_net_gen:.1%}")
    logger.info(
        "new v fuel table fuel (mmbtu) diff: "
        f"{(eia_generators.fuel_consumed_mmbtu.sum())/fuel_consumed:.1%}")
    return eia_generators


def _test_gen_fuel_allocation(pudl_out, gen_allocated, ratio=.05):
    gens_test = (
        pd.merge(
            gen_allocated,
            pudl_out.gen_eia923(),
            on=IDX_GENS,
            suffixes=('_new', '_og')
        )
        .assign(
            net_generation_new_v_og=lambda x:
                x.net_generation_mwh_new / x.net_generation_mwh_og)
    )

    os_ration = gens_test[
        (~gens_test.net_generation_new_v_og.between((1 - ratio), (1 + ratio)))
        & (gens_test.net_generation_new_v_og.notnull())
    ]
    os_ratio = len(os_ration) / len(gens_test)
    logger.info(
        f"{os_ratio:.2%} of generator records are more that {ratio:.0%} off from the net generation table")
    if ratio == 0.5 and os_ratio > .11:
        raise AssertionError(
            f"Too many generator records that have allocated net gen more than {ratio:.0%}"
        )
