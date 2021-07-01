"""
Allocated data from generation_fuel_eia923 table to generator level.

Net generation and fuel consumption is reported in two seperate tables in EIA
923: in the generation_eia923 and generation_fuel_eia923 tables. While the
generation_fuel_eia923 table is more complete (the generation_eia923 table
includes only ~55% of the reported MWhs), the generation_eia923 table is more
granular (it is reported at the generator level).

This module allocates net generation and fuel consumption from the
generation_fuel_eia923 table to the generator level. The main function here is
``allocate_gen_fuel_by_gen()``.

The methodology we are employing here to allocate the net generation from the
generation_fuel_eia923 table is not the only option and includes many
assumptions. Firstly, this methodology assumes the generation_fuel_eia923
table is the ground truth for net generation - as opposed to the
generation_eia923 table. We are making this assumption because we know that the
generation_fuel_eia923 table is necessarily more complete - there are many
full plants or generators in plants that do not report to the generation_eia923
table at all.

The next important note is the way in which we associated the data reported in
the generation_fuel_eia923 table with generators. The generation_fuel_eia923
table is reported at the level of prime_mover_code/fuel_type (See
``IDX_PM_FUEL``). Generators have prime_mover_codes, fuel_types (in
energy_source_code_*s) and report_dates. This methology does not distinguish
between primary and secondary fuel_types for generators - it associates
portions of net generatoion to each prime_mover_code/fuel_type.

The last high-level point about this methodology surrounds the allocation
method. In order to allocate portions of the net generation, we calculate as
allocation ratio, which is based on the net generation from the
generation_eia923 table when available and the capacity_mw from the
generators_eia860 table. Some plants have a portion of their generators that
report to generation_eia923. For those plants, we assign an allocation ratio in
three steps: first we generate an allocation ratio based on capacity_mw for each
group of generators (generators the do report in generation_eia923 and those
that do not). Then we generate an allocation ratio based on the net
generation reported in generation_eia923. Then we multiply both allocation
ratios together to scale down the net generation based ratio based on the
capacity of the generators reporting in generation_eia923.

This methodology has several potentail flaws and drawbacks. Because there is no
indicator of what portion of the energy_source_codes (ie. fule_type), we
associate the net generation equally amoung them. In effect, if a plant had
multiple generators with the same prime_mover_code but opposite primary and
secondary fuels (eg. gen 1 has a primary fuel of 'NG' and secondard fuel of
'DFO', while gen 2 has a primary fuel of 'DFO' and a secondary fuel of 'NG'),
the methodology associates the generation_fuel_eia923 records similarly across
these two generators. Nonetheless, the allocated net generation will still be
porporational to each generators generation_eia923 net generation or capacity.

This methodology also has an effect of smoothing differences of generators with
the same prime_mover_code and fuel_type. In effect, two similar generators will
appear to have similar capacity factors, especially if they reported no data to
the generation_eia923 table.

Another methodology that could be worth employing is use the generation_eia923
table when available and allocate the remaining net generation in a similar
methodology as we have currently employed by using each generators' capacity as
an allocator. For the ~.2% of records which report more net generation in the
generation_eia923 table, we would have to augment that methodology.
"""

import logging
import warnings

# Useful high-level external modules.
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

IDX_GENS = ['plant_id_eia', 'generator_id', 'report_date']
"""Id columns for generators."""

IDX_PM_FUEL = ['plant_id_eia', 'prime_mover_code',
               'fuel_type', 'report_date']
"""Id columns for plant, prime mover & fuel type records."""

IDX_FUEL = ['report_date', 'plant_id_eia', 'fuel_type']

DATA_COLS = ['net_generation_mwh', 'fuel_consumed_mmbtu']
"""Data columns from generation_fuel_eia923 that are being allocated."""


def allocate_gen_fuel_by_gen(pudl_out):
    """
    Allocate gen fuel data columns to generators.

    The generation_fuel_eia923 table includes net generation and fuel
    consumption data at the plant/fuel type/prime mover level. The most
    granular level of plants that PUDL typically uses is at the plant/generator
    level. This method converts the generation_fuel_eia923 table to the level
    of plant/generators.

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
            the tables for EIA and FERC Form 1 analysis.

    Returns:
        pandas.DataFrame: table with columns ``IDX_GENS`` and ``DATA_COLS``.
        The ``DATA_COLS`` will be scaled to the level of the ``IDX_GENS``.

    """
    gen_pm_fuel = allocate_gen_fuel_by_gen_pm_fuel(pudl_out)
    gen = agg_by_generator(gen_pm_fuel, pudl_out)
    _test_gen_fuel_allocation(pudl_out, gen)
    return gen


def allocate_gen_fuel_by_gen_pm_fuel(pudl_out):
    """
    Proportionally allocate net gen from gen_fuel table to generators.

    Two main steps here:
     * associated gen_fuel data w/ generators
     * allocate gen_fuel data proportionally

     The assocation process happens via `associate_gen_tables()`.

     The allocation process entails generating a ratio for each record within a
     ``IDX_PM_FUEL`` group. We have two options for generating this ratio: the
     net generation in the generation_eia923 table and the capacity from the
     generators_eia860 table. We calculate both these ratios, then used the
     net generation based ratio if available to allocation a portion of the
     associated data fields.

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
            the tables for EIA and FERC Form 1 analysis.

    Returns:
        pandas.DataFrame
    """
    gens_asst = (associate_gen_tables(pudl_out)
                 .pipe(_associate_unconnected_records)
                 .pipe(_associate_fuel_type_only, pudl_out))

    gen_pm_fuel = make_allocation_ratio(gens_asst).pipe(_test_gen_ratio)

    # do the allocating-ing!
    gen_pm_fuel = (
        gen_pm_fuel.assign(
            # we could x.net_generation_mwh_gen.fillna here if we wanted to
            # take the net gen
            net_generation_mwh=lambda x: x.net_generation_mwh_gf * x.gen_ratio,
            # let's preserve the gf version of fuel consumption (it didn't show
            # up in the tables we pulled together in associate_gen_tables()).
            fuel_consumed_mmbtu_gf=lambda x: x.fuel_consumed_mmbtu,
            fuel_consumed_mmbtu=lambda x: x.fuel_consumed_mmbtu * x.gen_ratio
        )
    )

    gen_pm_fuel = (
        gen_pm_fuel.astype(
            {"plant_id_eia": "Int64",
             "net_generation_mwh": "float"})
        .pipe(_test_gen_pm_fuel_output, pudl_out)
    )
    return gen_pm_fuel


def agg_by_generator(gen_pm_fuel, pudl_out):
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
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
            the tables for EIA and FERC Form 1 analysis.
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
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
            the tables for EIA and FERC Form 1 analysis.
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
                      if x in pudl_out.gen_original_eia923().columns
                      and x not in IDX_GENS]
    gens_asst = (
        pd.merge(
            stack_gens,
            pudl_out.gen_original_eia923().drop(columns=drop_cols_gens),
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

    gens_asst = (
        pd.merge(
            gens_asst,
            gens_asst.groupby(by=IDX_FUEL)
            [['capacity_mw', 'net_generation_mwh_gen']].sum(min_count=1)
            .add_suffix('_fuel_total')
            .reset_index(),
            on=IDX_FUEL,
        )
    )
    return gens_asst


def _associate_unconnected_records(eia_generators_merged):
    """
    Associate unassocaited gen_fuel table records on idx_pm.

    There are a subset of generation_fuel_eia923 records which do not
    merge onto the stacked generator table on ``IDX_PM_FUEL``. These records
    generally don't match with the set of prime movers and fuel types in the
    stacked generator table. In this method, we associated those straggler,
    unconnected records by merging these records with the stacked generators on
    the prime mover only.

    Args:
        eia_generators_merged (pandas.DataFrame)
    """
    # we're assocaiting on the plant/pm level... but we only want to associated
    # these unassocaited records w/ the primary fuel type from _stack_generators
    # so we're going to merge on energy_source_code_num and
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
        .assign(energy_source_code_num='energy_source_code_1')
        .groupby(by=idx_pm).sum(min_count=1)
        .reset_index()
    )

    eia_generators = (
        pd.merge(
            eia_generators_connected,
            eia_generators_unconnected[
                idx_pm + ['net_generation_mwh_gf', 'fuel_consumed_mmbtu']],
            on=idx_pm,
            suffixes=('', '_unconnected'),
            how='left'
        )
        .assign(
            # we want the main and the unconnected net get to be added together
            # but sometimes there is no main net get and sometimes there is no
            # unconnected net gen
            net_generation_mwh_gf=lambda x: np.where(
                x.net_generation_mwh_gf.notnull()
                | x.net_generation_mwh_gf_unconnected.notnull(),
                x.net_generation_mwh_gf.fillna(0)
                + x.net_generation_mwh_gf_unconnected.fillna(0),
                np.nan
            ),
            fuel_consumed_mmbtu=lambda x: np.where(
                x.fuel_consumed_mmbtu.notnull()
                | x.fuel_consumed_mmbtu_unconnected.notnull(),
                x.fuel_consumed_mmbtu.fillna(0)
                + x.fuel_consumed_mmbtu_unconnected.fillna(0),
                np.nan
            ),
        )
    )
    return eia_generators


def _associate_fuel_type_only(gens_asst, pudl_out):
    """
    Assocaite the records w/o prime movers with fuel cost.

    The 2001 and 2002 generation fuel table does not include any prime mover
    codes. Because of this, we need to associated these records via their fuel
    types.

    Note: 2001 and 2002 eia years are not currently integrated into PUDL.
    """
    gf_grouped = (
        pudl_out.gf_eia923()
        .groupby(by=IDX_PM_FUEL, dropna=False)
        .sum(min_count=1).reset_index()
    )
    gf_missing_pm = (
        gf_grouped[gf_grouped[IDX_PM_FUEL].isnull().any(axis=1)]
        .drop(columns=['prime_mover_code'])
        .set_index(IDX_FUEL).add_suffix("_fuel").reset_index()
    )

    gens_asst = pd.merge(
        gens_asst,
        gf_missing_pm,
        how='outer',
        on=IDX_FUEL,
        indicator=True
    )

    gens_asst = _associate_fuel_type_only_wo_matching_fuel_type(
        gens_asst, gf_grouped)

    if gf_missing_pm.empty:
        logger.info(
            "No records found with fuel-only records. This is expected.")
    else:
        logger.info(
            f"{len(gf_missing_pm)/len(gens_asst):.02%} records w/o prime movers now"
            f" associated for: {gf_missing_pm.report_date.dt.year.unique()}")
    return gens_asst


def _associate_fuel_type_only_wo_matching_fuel_type(gens_asst, gf_grouped):
    """
    Associated the missing-pm records that don't have matching fuel types.

    There are some generation fuel table records which don't associated with
    any of the energy_source_code's reported in for the generators. For these
    records, we need to take a step back and associate these records with the
    full plant.
    """
    idx_plant = ['plant_id_eia', 'report_date']
    gens_asst = pd.merge(
        gens_asst,
        gens_asst.groupby(by=idx_plant, dropna=False)[['capacity_mw']]
        .sum(min_count=1).add_suffix('_plant').reset_index(),
        on=idx_plant,
        how='left'
    )

    gens_asst_w_unassociated = (
        pd.merge(
            gens_asst[
                (gens_asst._merge != 'right_only')
                | (gens_asst._merge.isnull())
            ],
            (gens_asst[gens_asst._merge == 'right_only']
             .groupby(idx_plant)
             [['net_generation_mwh_fuel', 'fuel_consumed_mmbtu_fuel']]
             .sum(min_count=1)),
            on=idx_plant,
            how='left',
            suffixes=('', '_unconnected')
        )
        .assign(
            net_generation_mwh_gf=lambda x:
                x.net_generation_mwh_gf.fillna(
                    x.net_generation_mwh_fuel
                    + x.net_generation_mwh_fuel_unconnected.fillna(0)
                ),
            fuel_consumed_mmbtu=lambda x:
                x.fuel_consumed_mmbtu.fillna(
                    x.fuel_consumed_mmbtu_fuel
                    + x.fuel_consumed_mmbtu_fuel_unconnected.fillna(0)
                ),
        )
    )
    return gens_asst_w_unassociated


def make_allocation_ratio(gens_asst):
    """Generate a ratio to use to allocate net generation by."""
    # generate a flag if the generator exists in
    # the generator table (this will be used later on)
    # for generating ratios to use to allocate
    gens_asst = gens_asst.assign(
        exists_in_gen=lambda x: np.where(
            x.net_generation_mwh_gen.notnull(),
            True, False)
    )

    gens_gb = gens_asst.groupby(by=IDX_PM_FUEL)
    # get the total values for the merge group
    # we would use on groupby here with agg but it is much slower
    # so we're gb-ing twice w/ a merge
    # gens_gb.agg({'net_generation_mwh_gen': lambda x: x.sum(min_count=1),
    #              'capacity_mw': lambda x: x.sum(min_count=1),
    #              'exists_in_gen': 'all'},)
    gen_pm_fuel = (
        pd.merge(
            gens_asst,
            gens_gb
            [['net_generation_mwh_gen', 'capacity_mw']]
            .sum(min_count=1)
            .add_suffix('_pm_fuel_total')
            .reset_index(),
            on=IDX_PM_FUEL,
        )
        .merge(
            gens_gb[['exists_in_gen']].all().reset_index(),
            on=IDX_PM_FUEL,
            suffixes=('', '_pm_fuel_total')
        )
    )
    gen_pm_fuel_ratio = (
        pd.merge(
            gen_pm_fuel,
            gen_pm_fuel.groupby(by=IDX_PM_FUEL + ['exists_in_gen'])
            [['capacity_mw']]
            .sum(min_count=1)
            .add_suffix('_exist_in_gen_group')
            .reset_index(),
            on=IDX_PM_FUEL + ['exists_in_gen'],
        )
    )

    gen_pm_fuel_ratio = (
        gen_pm_fuel_ratio.assign(
            # we have two options for generating a ratio for allocating
            # we'll first try to allocated based on net generation from the gen
            # table, but we need to scale that based on capacity of the
            # generators the report in net gen table
            # and if that isn't there we'll allocate based on capacity
            gen_ratio_exist_in_gen_group=lambda x:
                x.capacity_mw_exist_in_gen_group / x.capacity_mw_pm_fuel_total,
            gen_ratio_net_gen=lambda x:
                x.net_generation_mwh_gen /
                x.net_generation_mwh_gen_pm_fuel_total,
            gen_ratio_net_gen_scaled_by_cap=lambda x:
                x.gen_ratio_net_gen * x.gen_ratio_exist_in_gen_group,
            gen_ratio_cap=lambda x: x.capacity_mw / x.capacity_mw_pm_fuel_total,
            # ratio for the records with a missing prime mover that are
            # assocaited at the plant fuel level
            gen_ratio_net_gen_fuel=lambda x:
                x.net_generation_mwh_gf
                / x.net_generation_mwh_gen_fuel_total,
            gen_ratio_cap_fuel=lambda x:
                x.capacity_mw / x.capacity_mw_fuel_total,
            gen_ratio_fuel=lambda x:
                np.where(x.gen_ratio_net_gen_fuel.notnull()
                         | x.gen_ratio_net_gen_fuel != 0,
                         x.gen_ratio_net_gen_fuel, x.gen_ratio_cap_fuel),
            # final ratio
            gen_ratio=lambda x:
                np.where(
                    x.net_generation_mwh_fuel.notnull(),
                    x.gen_ratio_fuel,
                    np.where(
                        (x.gen_ratio_net_gen_scaled_by_cap.notnull()
                         | x.gen_ratio_net_gen_scaled_by_cap != 0),
                        x.gen_ratio_net_gen_scaled_by_cap, x.gen_ratio_cap)),)
    )
    return gen_pm_fuel_ratio


def _test_gen_ratio(gen_pm_fuel):
    # test! Check if each of the IDX_PM_FUEL groups gen_ratio's add up to 1
    ratio_test_pm_fuel = (
        gen_pm_fuel.groupby(IDX_PM_FUEL)
        [['gen_ratio']].sum(min_count=1)
        .reset_index()
    )

    ratio_test_fuel = (
        gen_pm_fuel.groupby(IDX_FUEL)
        [['gen_ratio', 'net_generation_mwh_fuel']].sum(min_count=1)
        .reset_index()
    )

    ratio_test = (
        pd.merge(
            ratio_test_pm_fuel, ratio_test_fuel,
            on=IDX_FUEL,
            suffixes=("", "_fuel")
        )
        .assign(
            gen_ratio=lambda x: np.where(
                x.net_generation_mwh_fuel.isnull(),
                x.gen_ratio, x.gen_ratio_fuel
            )
        )
    )

    ratio_test_bad = ratio_test[
        ~np.isclose(ratio_test.gen_ratio, 1)
        & ratio_test.gen_ratio.notnull()
    ]
    if not ratio_test_bad.empty:
        raise AssertionError(
            f"Ooopsies. You got {len(ratio_test_bad)} records where the "
            "'gen_ratio' column isn't adding up to 1 for each 'IDX_PM_FUEL' "
            "group. Check 'make_allocation_ratio()'"
        )
    return gen_pm_fuel


def _test_gen_pm_fuel_output(gen_pm_fuel, pudl_out):
    # this is just for testing/debugging
    def calc_net_gen_diff(gen_pm_fuel, idx):
        gen_pm_fuel_test = (
            pd.merge(
                gen_pm_fuel,
                gen_pm_fuel.groupby(by=idx)
                [['net_generation_mwh']]
                .sum(min_count=1).add_suffix('_test').reset_index(),
                on=idx,
                how='outer'
            )
            .assign(net_generation_mwh_diff=lambda x:
                    x.net_generation_mwh_gf
                    - x.net_generation_mwh_test)
        )
        return gen_pm_fuel_test
    # make different totals and calc differences for two different indexs
    gen_pm_fuel_test = calc_net_gen_diff(gen_pm_fuel, idx=IDX_PM_FUEL)
    gen_fuel_test = calc_net_gen_diff(gen_pm_fuel, idx=IDX_FUEL)

    gen_pm_fuel_test = gen_pm_fuel_test.assign(
        net_generation_mwh_test=lambda x: x.net_generation_mwh_test.fillna(
            gen_fuel_test.net_generation_mwh_test),
        net_generation_mwh_diff=lambda x: x.net_generation_mwh_diff.fillna(
            gen_fuel_test.net_generation_mwh_diff),
    )

    bad_diff = gen_pm_fuel_test[
        (~np.isclose(gen_pm_fuel_test.net_generation_mwh_diff, 0))
        & (gen_pm_fuel_test.net_generation_mwh_diff.notnull())]
    logger.info(
        f"{len(bad_diff)/len(gen_pm_fuel):.03%} of records have are partially "
        "off from their 'IDX_PM_FUEL' group")
    no_cap_gen = gen_pm_fuel_test[
        (gen_pm_fuel_test.capacity_mw.isnull())
        & (gen_pm_fuel_test.net_generation_mwh_gen.isnull())
    ]
    if len(no_cap_gen) > 15:
        logger.info(
            f'Warning: {len(no_cap_gen)} records have no capacity or net gen')
    gen_fuel = pudl_out.gf_eia923()
    gen = pudl_out.gen_original_eia923()
    # remove the junk/corrective plants
    fuel_net_gen = gen_fuel[
        gen_fuel.plant_id_eia != '99999'].net_generation_mwh.sum()
    fuel_consumed = gen_fuel[
        gen_fuel.plant_id_eia != '99999'].fuel_consumed_mmbtu.sum()
    logger.info(
        "gen v fuel table net gen diff:      "
        f"{(gen.net_generation_mwh.sum())/fuel_net_gen:.1%}")
    logger.info(
        "new v fuel table net gen diff:      "
        f"{(gen_pm_fuel_test.net_generation_mwh.sum())/fuel_net_gen:.1%}")
    logger.info(
        "new v fuel table fuel (mmbtu) diff: "
        f"{(gen_pm_fuel_test.fuel_consumed_mmbtu.sum())/fuel_consumed:.1%}")
    return gen_pm_fuel_test


def _test_gen_fuel_allocation(pudl_out, gen_allocated, ratio=.05):
    gens_test = (
        pd.merge(
            gen_allocated,
            pudl_out.gen_original_eia923(),
            on=IDX_GENS,
            suffixes=('_new', '_og')
        )
        .assign(
            net_generation_new_v_og=lambda x:
                x.net_generation_mwh_new / x.net_generation_mwh_og)
    )

    os_ratios = gens_test[
        (~gens_test.net_generation_new_v_og.between((1 - ratio), (1 + ratio)))
        & (gens_test.net_generation_new_v_og.notnull())
    ]
    os_ratio = len(os_ratios) / len(gens_test)
    logger.info(
        f"{os_ratio:.2%} of generator records are more that {ratio:.0%} off from the net generation table")
    if ratio == 0.05 and os_ratio > .15:
        warnings.warn(
            f"Many generator records that have allocated net gen more than {ratio:.0%}"
        )
