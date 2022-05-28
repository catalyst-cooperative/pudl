"""
Allocate data from generation_fuel_eia923 table to generator level.

Net electricity generation and fuel consumption are reported in mutiple ways in the EIA
923. The generation_fuel_eia923 table reports both generation and fuel consumption, and
breaks them down by plant, prime mover, and energy source. In parallel, the generation_eia923
table reports generation by generator, and the boiler_fuel_eia923 table reports fuel
consumption by boiler.

The generation_fuel_eia923 table is more complete, but the generation_eia923 +
boiler_fuel_eia923 tables are more granular. The generation_eia923 table includes only
~55% of the total MWhs reported in the generation_fuel_eia923 table.

This module estimates the net electricity generation and fuel consumption attributable
to individual generators based on the more expansive reporting of the data in the
generation_fuel_eia923 table. The main coordinating functions here are
:func:`allocate_gen_fuel_by_generator_energy_source` and
:func:`aggregate_gen_fuel_by_generator`.

The algorithm we're using assumes:

* The generation_eia923 table is the authoritative source of information about
  how much generation is attributable to an individual generator, if it reports
  in that table.
* The generation_fuel_eia923 table is the authoritative source of information
  about how much generation and fuel consumption is attributable to an entire
  plant.
* The generators_eia860 table provides an exhaustive list of all generators
  whose generation is being reported in the generation_fuel_eia923 table.

We allocate the net generation reported in the generation_fuel_eia923 table on the basis
of plant, prime mover, and fuel type among the generators in each plant that have
matching fuel types. Generation is allocated proportional to reported generation if it's
available, and proportional to each generator's capacity if generation is not available.

In more detail: within each month of data, we split the plants into three groups:

* Plants where ALL generators report in the more granular generation_eia923
  table.
* Plants where NONE of the generators report in the generation_eia923 table.
* Plants where only SOME of the generators report in the generation_eia923
  table.

In plant-months where ALL generators report more granular generation, the total net
generation reported in the generation_fuel_eia923 table is allocated in proportion to
the generation each generator reported in the generation_eia923 table. We do this
instead of using net_generation_mwh from generation_eia923 because there are some small
discrepancies between the total amounts of generation reported in these two tables.

In plant-months where NONE of the generators report more granular generation, we create a
generator record for each associated fuel type. Those records are merged with the
generation_fuel_eia923 table on plant, prime mover code, and fuel type. Each group of
plant, prime mover, and fuel will have some amount of reported net generation associated
with it, and one or more generators. The net generation is allocated among the
generators within the group in proportion to their capacity. Then the allocated net
generation is summed up by generator.

In the hybrid case, where only SOME of of a plant's generators report the more granular
generation data, we use a combination of the two allocation methods described above.
First, the total generation reported across a plant in the generation_fuel_eia923 table
is allocated between the two categories of generators (those that report fine-grained
generation, and those that don't) in direct proportion to the fraction of the plant's
generation which is reported in the generation_eia923 table, relative to the total
generation reported in the generation_fuel_eia923 table.

Note that this methology does not distinguish between primary and secondary
energy_sources for generators. It associates portions of net generation to each
generators in the same plant do not report detailed generation, have the same
prime_mover_code, and use the same fuels, but have very different capacity factors in
reality, this methodology will allocate generation such that they end up with very
similar capacity factors. We imagine this is an uncommon scenario.

This methodology has several potential flaws and drawbacks. Because there is no
indicator of what portion of the energy_source_codes, we associate the net generation
equally among them. In effect, if a plant had multiple generators with the same
prime_mover_code but opposite primary and secondary fuels (eg. gen 1 has a primary fuel
of 'NG' and secondary fuel of 'DFO', while gen 2 has a primary fuel of 'DFO' and a
secondary fuel of 'NG'), the methodology associates the generation_fuel_eia923 records
similarly across these two generators. However, the allocated net generation will still
be porporational to each generator's net generation (if it's reported) or capacity (if
generation is not reported).

"""

import logging
import warnings
from typing import List

# Useful high-level external modules.
import numpy as np
import pandas as pd

import pudl.helpers
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)

IDX_GENS = ["report_date", "plant_id_eia", "generator_id"]
"""Id columns for generators."""

IDX_PM_ESC = ["report_date", "plant_id_eia", "energy_source_code", "prime_mover_code"]
"""Id columns for plant, prime mover & fuel type records."""

IDX_ESC = ["report_date", "plant_id_eia", "energy_source_code"]

IDX_U_ESC = ["report_date", "plant_id_eia", "energy_source_code", "unit_id_pudl"]

"""Data columns from generation_fuel_eia923 that are being allocated."""


# Two top-level functions (allocate & aggregate)
def allocate_gen_fuel_by_generator_energy_source(pudl_out, drop_interim_cols=True):
    """
    Proportionally allocate net gen from gen_fuel table to the generator/energy_source_code level.

    Three main steps here:
     * grab the three input tables from `pudl_out` with only the needed columns
     * associate `generation_fuel_eia923` table data w/ generators
     * allocate `generation_fuel_eia923` table data proportionally

     The association process happens via `associate_generator_tables()`.

     The allocation process (via `allocate_net_gen_by_gen_esc()`) entails
     generating a fraction for each record within a ``IDX_PM_ESC`` group. We
     have two data points for generating this ratio: the net generation in the
     generation_eia923 table and the capacity from the generators_eia860 table.
     The end result is a `frac` column which is unique for each
     generator/prime_mover/fuel record and is used to allocate the associated
     net generation from the `generation_fuel_eia923` table.

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create the
            tables for EIA and FERC Form 1 analysis.
        drop_interim_cols (boolean): True/False flag for dropping interim
            columns which are used to generate the `net_generation_mwh` column
            (they are mostly the `frac` column and  net generataion reported in
            the original generation_eia923 and generation_fuel_eia923 tables)
            that are useful for debugging. Default is False, which will drop
            the columns.
    """
    # extract all of the tables from pudl_out early in the process and select
    # only the columns we need. this is for speed and clarity.
    gf = (
        pudl_out.gf_eia923()
        .loc[
            :,
            IDX_PM_ESC
            + [
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
        ]
        .pipe(manually_fix_energy_source_codes)
    )
    bf = (
        pudl_out.bf_eia923()
        .merge(
            pd.read_sql("boilers_entity_eia", pudl_out.pudl_engine),
            how="left",
            on=["plant_id_eia", "boiler_id"],
        )
        .loc[:, IDX_PM_ESC + ["fuel_consumed_mmbtu"]]
    )
    gen = (
        pudl_out.gen_original_eia923().loc[:, IDX_GENS + ["net_generation_mwh"]]
        # removes 4 records with NaN generator_id as of pudl v0.5
        .dropna(subset=IDX_GENS)
    )
    gens = pudl_out.gens_eia860().loc[
        :,
        IDX_GENS
        + [
            "prime_mover_code",
            "unit_id_pudl",
            "capacity_mw",
            "fuel_type_count",
            "operational_status",
            "retirement_date",
        ]
        + list(pudl_out.gens_eia860().filter(like="energy_source_code"))
        + list(pudl_out.gens_eia860().filter(like="startup_source_code")),
    ]
    # add any startup energy source codes to the list of energy source codes
    # fix MSW codes
    gens = adjust_energy_source_codes(gens, gf, bf)
    # fix prime mover codes in gens so that they match the codes in the gf table
    missing_pm = gens[gens["prime_mover_code"].isna()]
    if not missing_pm.empty:
        warnings.warn(
            f"{len(missing_pm)} generators are missing prime mover codes in gens_eia860. "
            "This will result in incorrect allocation."
        )
    # duplicate each entry 12 times to create an entry for each month of the year
    gens = create_monthly_gens_records(gens)

    # the gen table is missing some generator ids. Let's fill this using the gens table, leaving a missing value for net generation
    gen = gen.merge(
        gens[["plant_id_eia", "generator_id", "report_date"]],
        how="outer",
        on=["plant_id_eia", "generator_id", "report_date"],
    )

    # do the association!
    gen_assoc = associate_generator_tables(gf=gf, gen=gen, gens=gens, bf=bf)

    # Generate a fraction to use to allocate net generation by.
    # These two methods create a column called `frac`, which will be a fraction
    # to allocate net generation from the gf table for each `IDX_PM_ESC` group
    gen_pm_fuel = prep_alloction_fraction(gen_assoc)

    # Net gen allocation
    net_gen_alloc = allocate_net_gen_by_gen_esc(gen_pm_fuel).pipe(
        _test_gen_pm_fuel_output, gf=gf, gen=gen
    )

    _test_gen_fuel_allocation(gen, net_gen_alloc)

    # drop all of the columns we needed to get to the `frac` column
    if drop_interim_cols:
        net_gen_alloc = net_gen_alloc.loc[
            :,
            IDX_ESC
            + [
                "generator_id",
                "energy_source_code_num",
                "net_generation_mwh",
            ],
        ]

    # fuel allocation
    fuel_alloc = allocate_fuel_by_gen_esc(gen_pm_fuel)
    if drop_interim_cols:
        fuel_alloc = fuel_alloc.loc[
            :,
            IDX_ESC
            + [
                "generator_id",
                "energy_source_code_num",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
        ]

    # squish net gen and fuel allocation together
    net_gen_fuel_alloc = pd.merge(
        net_gen_alloc,
        fuel_alloc,
        on=IDX_ESC + ["generator_id", "energy_source_code_num"],
        how="outer",
        validate="1:1",
    ).sort_values(IDX_ESC + ["generator_id", "energy_source_code_num"])
    return net_gen_fuel_alloc


def aggregate_gen_fuel_by_generator(
    pudl_out,
    net_gen_fuel_alloc: pd.DataFrame,
    sum_cols,
) -> pd.DataFrame:
    """
    Aggregate gen fuel data columns to generators.

    The generation_fuel_eia923 table includes net generation and fuel
    consumption data at the plant/fuel type/prime mover level. The most
    granular level of plants that PUDL typically uses is at the plant/generator
    level. This function takes the plant/energy source code/prime mover level
    allocation, aggregates it to the generator level and then denormalizes it to
    make it more structurally in-line with the original generation_eia923 table
    (see :func:`pudl.output.eia923.denorm_generation_eia923`).

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create the tables for EIA and FERC Form 1
            analysis.
        net_gen_fuel_alloc: table of allocated generation at the generator/prime mover
            /fuel type. Result of :func:`allocate_gen_fuel_by_generator_energy_source`
        sum_cols: Data columns from that are being aggregated via a
            ``pandas.groupby.sum()`` in agg_by_generator

    Returns:
        table with columns :py:const:`IDX_GENS` and net generation and fuel
        consumption scaled to the level of the :py:const:`IDX_GENS`.

    """
    # aggregate the gen/pm/fuel records back to generator records
    gen_allocated = agg_by_generator(
        net_gen_fuel_alloc=net_gen_fuel_alloc, sum_cols=sum_cols
    )
    # make the output mirror the gen_original_eia923()

    gen_allocated = pudl.output.eia923.denorm_generation_eia923(
        g_df=gen_allocated,
        pudl_engine=pudl_out.pudl_engine,
        start_date=pudl_out.start_date,
        end_date=pudl_out.end_date,
    )
    return gen_allocated


def scale_allocated_net_gen_by_ownership(
    gen_pm_fuel: pd.DataFrame, gens: pd.DataFrame, own_eia860: pd.DataFrame
) -> pd.DataFrame:
    """
    Scale the allocated net generation at the generator/energy_source_code level by ownership.

    It can be helpful to have a table of net generation and fuel consumption
    at the generator/fuel-type level (i.e. the result of :func:`allocate_gen_fuel_by_generator_energy_source`)
    to be associated and scaled with all of the owners of those generators.
    This allows the aggregation of fuel use to the utility level.

    Scaling generators with their owners' ownership fraction is currently
    possible via :class:`pudl.analysis.plant_parts_eia.MakeMegaGenTbl`. This
    function uses the allocated net generation at the generator/fuel-type level,
    merges that with a generators table to ensure all necessary columns are
    available, and then feeds that table into the EIA Plant-parts' :meth:`scale_by_ownership`.

    Args:
        gen_pm_fuel: able of allocated generation at the generator/prime mover
            /fuel type. Result of :func:`allocate_gen_fuel_by_generator_energy_source`
        gens: `generators_eia860` table with cols: :const:``IDX_GENS``, `capacity_mw`
            and `utility_id_eia`
        own_eia860: `ownership_eia860` table.
    """
    gen_pm_fuel_own = pudl.analysis.plant_parts_eia.MakeMegaGenTbl().scale_by_ownership(
        gens_mega=pd.merge(
            gen_pm_fuel,
            gens[IDX_GENS + ["utility_id_eia", "capacity_mw"]],
            on=IDX_GENS,
            validate="m:1",
            how="left",
        ),
        own_eia860=own_eia860,
        scale_cols=[
            "net_generation_mwh",
            "fuel_consumed_mmbtu",
            "fuel_consumed_for_electricity_mmbtu",
            "capacity_mw",
        ],
        validate="m:m",  # m:m because there are multiple generators in gen_pm_fuel
    )
    return gen_pm_fuel_own


# Internal functions for allocate_gen_fuel_by_generator_energy_source


def agg_by_generator(
    net_gen_fuel_alloc: pd.DataFrame,
    by_cols: List[str] = IDX_GENS,
    sum_cols: List[str] = [
        "net_generation_mwh",
        "fuel_consumed_mmbtu",
        "fuel_consumed_for_electricity_mmbtu",
    ],
) -> pd.DataFrame:
    """
    Aggreate the allocated gen fuel data to the generator level.

    Args:
        net_gen_fuel_alloc: result of :func:`allocate_gen_fuel_by_generator_energy_source()`
        by_cols: list of columns to use as ``pandas.groupby`` arg ``by``
        sum_cols: Data columns from that are being aggregated via a
            ``pandas.groupby.sum()``
    """
    gen = (
        net_gen_fuel_alloc.groupby(by=IDX_GENS)[sum_cols]
        .sum(min_count=1)
        .reset_index()
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return gen


def stack_generators(
    gens, cat_col="energy_source_code_num", stacked_col="energy_source_code"
):
    """
    Stack the generator table with a set of columns.

    Args:
        gens (pandas.DataFrame): generators_eia860 table with cols: ``IDX_GENS``
            and all of the `energy_source_code` columns
        cat_col (string): name of category column which will end up having the
            column names of cols_to_stack
        stacked_col (string): name of column which will end up with the stacked
            data from cols_to_stack

    Returns:
        pandas.DataFrame: a dataframe with these columns: idx_stack, cat_col,
        stacked_col

    """
    esc = list(gens.filter(like="energy_source_code"))
    gens_stack_prep = (
        pd.DataFrame(gens.set_index(IDX_GENS)[esc].stack(level=0))
        .reset_index()
        .rename(columns={"level_3": cat_col, 0: stacked_col})
        .pipe(apply_pudl_dtypes, "eia")
    )

    # merge the stacked df back onto the gens table
    # we first drop the cols_to_stack so we don't duplicate data
    gens_stack = pd.merge(
        gens.drop(columns=esc), gens_stack_prep, on=IDX_GENS, how="outer"
    )
    return gens_stack


def associate_generator_tables(gf, gen, gens, bf):
    """
    Associate the three tables needed to assign net gen to generators.

    Args:
        gf (pandas.DataFrame): generator_fuel_eia923 table with columns:
            ``IDX_PM_ESC`` and `net_generation_mwh` and `fuel_consumed_mmbtu`.
        gen (pandas.DataFrame): generation_eia923 table with columns:
            ``IDX_GENS`` and `net_generation_mwh`.
        gens (pandas.DataFrame): generators_eia860 table with cols: ``IDX_GENS``
            and all of the `energy_source_code` columns
        bf (pandas.DataFrame): boiler_fuel_eia923 table

    TODO: Convert these groupby/merges into transforms.
    """
    stack_gens = stack_generators(
        gens, cat_col="energy_source_code_num", stacked_col="energy_source_code"
    )

    bf_summed = (
        bf.groupby(by=IDX_PM_ESC, dropna=False)
        .sum(min_count=1)
        .add_suffix("_bf_tbl")
        .reset_index()
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
        .replace(
            0, 0.001
        )  # replace zeros with small number to avoid div by zero errors when calculating allocation fraction
    )
    gf_pm_fuel_summed = (
        gf.groupby(by=IDX_PM_ESC)
        .sum(min_count=1)[
            [
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
                "net_generation_mwh",
            ]
        ]
        .add_suffix("_gf_tbl")
        .reset_index()
        .replace(
            0, 0.001
        )  # replace zeros with small number to avoid div by zero errors when calculating allocation fraction
    )
    gf_fuel_summed = (
        gf.groupby(by=IDX_ESC)
        .sum(min_count=1)[
            ["fuel_consumed_mmbtu", "fuel_consumed_for_electricity_mmbtu"]
        ]
        .add_suffix("_gf_tbl_fuel")
        .reset_index()
    )

    gen_assoc = (
        pd.merge(stack_gens, gen, on=IDX_GENS, how="outer")
        .rename(columns={"net_generation_mwh": "net_generation_mwh_g_tbl"})
        .merge(gf_pm_fuel_summed, on=IDX_PM_ESC, how="left", validate="m:1")
        .pipe(remove_inactive_generators)
        .merge(bf_summed, on=IDX_PM_ESC, how="left", validate="m:1")
        .merge(
            gf_fuel_summed,
            on=IDX_ESC,
            how="left",
            validate="m:1",
        )
    )
    # TODO: Check if I need to do this replacement at the base net generation data
    gen_assoc["net_generation_mwh_g_tbl"] = gen_assoc[
        "net_generation_mwh_g_tbl"
    ].replace(
        0, 0.001
    )  # replace zeros with small number to avoid div by zero errors when calculating allocation fraction
    # calculate the total capacity in every fuel group
    gen_assoc = (
        pd.merge(
            gen_assoc,
            gen_assoc.groupby(by=IDX_ESC)[["capacity_mw", "net_generation_mwh_g_tbl"]]
            .sum(min_count=1)
            .add_suffix("_fuel")
            .reset_index(),
            on=IDX_ESC,
        )
        .pipe(apply_pudl_dtypes, "eia")
        .pipe(_associate_unconnected_records)
    )
    return gen_assoc


def remove_inactive_generators(gen_assoc):
    """
    Remove the retired generators.

    We don't want to associate net generation to generators that are retired
    (or proposed! or any other `operational_status` besides `existing`).

    We do want to keep the generators that retire mid-year and have generator
    specific data from the generation_eia923 table. Removing the generators
    that retire mid-report year and don't report to the generation_eia923 table
    is not exactly a great assumption. For now, we are removing them. We should
    employ a strategy that allocates only a portion of the generation to them
    based on their operational months (or by doing the allocation on a monthly
    basis).

    Args:
        gen_assoc (pandas.DataFrame): table of generators with stacked fuel
            types and broadcasted net generation data from the
            generation_eia923 and generation_fuel_eia923 tables. Output of
            `associate_generator_tables()`.
    """
    existing = gen_assoc.loc[(gen_assoc.operational_status == "existing")]
    # keep the gens that retired mid-report-year that have generator
    # specific data
    retiring = gen_assoc.loc[
        (gen_assoc.operational_status == "retired")
        & (
            (gen_assoc.report_date <= gen_assoc.retirement_date)
            | (gen_assoc.net_generation_mwh_g_tbl.notnull())
        )
    ]
    new = gen_assoc.loc[
        (gen_assoc.operational_status == "proposed")
        & (gen_assoc.net_generation_mwh_g_tbl.notnull())
    ]

    gen_assoc_removed = pd.concat([existing, retiring, new])
    return gen_assoc_removed


def _associate_unconnected_records(eia_generators_merged):
    """
    Associate unassociated gen_fuel table records on idx_pm.

    There are a subset of generation_fuel_eia923 records which do not
    merge onto the stacked generator table on ``IDX_PM_ESC``. These records
    generally don't match with the set of prime movers and fuel types in the
    stacked generator table. In this method, we associate those straggler,
    unconnected records by merging these records with the stacked generators on
    the prime mover only.

    Args:
        eia_generators_merged (pandas.DataFrame)

    """
    # we're associating on the plant/pm level... but we only want to associated
    # these unassocaited records w/ the primary fuel type from stack_generators
    # so we're going to merge on energy_source_code_num and
    idx_pm = [
        "plant_id_eia",
        "prime_mover_code",
        "energy_source_code_num",
        "report_date",
    ]
    # we're going to only associate these unconnected fuel records w/
    # the primary fuel so we don't have to deal w/ double counting
    connected_mask = eia_generators_merged.generator_id.notnull()
    eia_generators_connected = eia_generators_merged[connected_mask]
    eia_generators_unconnected = (
        eia_generators_merged[~connected_mask]
        .rename(columns={"energy_source_code": "energy_source_unconnected"})
        .assign(energy_source_code_num="energy_source_code_1")
        .groupby(by=idx_pm)
        .sum(min_count=1)
        .reset_index()
    )
    eia_generators = (
        pd.merge(
            eia_generators_connected,
            eia_generators_unconnected[
                idx_pm
                + [
                    "net_generation_mwh_gf_tbl",
                    "fuel_consumed_mmbtu_gf_tbl",
                    "fuel_consumed_for_electricity_mmbtu_gf_tbl",
                ]
            ],
            on=idx_pm,
            suffixes=("", "_unconnected"),
            how="left",
        )
        .assign(
            # we want the main and the unconnected net gen to be added together
            # but sometimes there is no main net gen and sometimes there is no
            # unconnected net gen
            net_generation_mwh_gf_tbl=lambda x: np.where(
                x.net_generation_mwh_gf_tbl.notnull()
                | x.net_generation_mwh_gf_tbl_unconnected.notnull(),
                x.net_generation_mwh_gf_tbl.fillna(0)
                + x.net_generation_mwh_gf_tbl_unconnected.fillna(0),
                np.nan,
            ),
            fuel_consumed_mmbtu_gf_tbl=lambda x: np.where(
                x.fuel_consumed_mmbtu_gf_tbl.notnull()
                | x.fuel_consumed_mmbtu_gf_tbl_unconnected.notnull(),
                x.fuel_consumed_mmbtu_gf_tbl.fillna(0)
                + x.fuel_consumed_mmbtu_gf_tbl_unconnected.fillna(0),
                np.nan,
            ),
            fuel_consumed_for_electricity_mmbtu_gf_tbl=lambda x: np.where(
                x.fuel_consumed_for_electricity_mmbtu_gf_tbl.notnull()
                | x.fuel_consumed_for_electricity_mmbtu_gf_tbl_unconnected.notnull(),
                x.fuel_consumed_for_electricity_mmbtu_gf_tbl.fillna(0)
                + x.fuel_consumed_for_electricity_mmbtu_gf_tbl_unconnected.fillna(0),
                np.nan,
            ),
        )  # we no longer need these _unconnected columns
        .drop(
            columns=[
                "net_generation_mwh_gf_tbl_unconnected",
                "fuel_consumed_mmbtu_gf_tbl_unconnected",
                "fuel_consumed_for_electricity_mmbtu_gf_tbl_unconnected",
            ]
        )
    )
    return eia_generators


def prep_alloction_fraction(gen_assoc):
    """
    Make flags and aggregations to prepare for the `calc_allocation_ratios()`.

    In `calc_allocation_ratios()`, we will break the generators out into four
    types - see `calc_allocation_ratios()` docs for details. This function adds
    flags for splitting the generators. It also adds

    """
    # flag whether the generator exists in the
    # generation table (this will be used later on)
    # for calculating ratios to use to allocate net generation
    gen_assoc = gen_assoc.assign(
        in_g_tbl=lambda x: np.where(x.net_generation_mwh_g_tbl.notnull(), True, False),
        in_bf_tbl=lambda x: np.where(
            x.fuel_consumed_mmbtu_bf_tbl.notnull(), True, False
        ),
    )

    gens_gb = gen_assoc.groupby(by=IDX_PM_ESC, dropna=False)
    # get the total values for the merge group
    # we would use on groupby here with agg but it is much slower
    # so we're gb-ing twice w/ a merge
    # gens_gb.agg({'net_generation_mwh_g_tbl': lambda x: x.sum(min_count=1),
    #              'capacity_mw': lambda x: x.sum(min_count=1),
    #              'in_g_tbl': 'all'},)
    gen_pm_fuel = (
        gen_assoc.merge(  # flag if all generators exist in the generators_eia860 tbl
            gens_gb[["in_g_tbl"]].all().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_all"),
        )
        .merge(  # flag if some generators exist in the generators_eia860 tbl
            gens_gb[["in_g_tbl"]].any().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_any"),
        )
        .merge(  # flag if all generators exist in the boiler fuel tbl
            gens_gb[["in_bf_tbl"]].all().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_all"),
        )
        .merge(  # flag if some generators exist in the boiler fuel tbl
            gens_gb[["in_bf_tbl"]].any().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_any"),
        )
        # Net generation and capacity are both proxies that can be used
        # to allocate the generation which only shows up in generation_fuel.
        # fuel consumption from the bf table can be used as a proxy to allocate
        # fuel consumption that only shows up in generation_fuel
        # Sum them up across the whole plant-prime-fuel group so we can tell
        # what fraction of the total capacity each generator is.
        .merge(
            (
                gens_gb[
                    [
                        "net_generation_mwh_g_tbl",
                        "fuel_consumed_mmbtu_bf_tbl",
                        "capacity_mw",
                    ]
                ]
                .sum(min_count=1)
                .add_suffix("_pm_fuel")
                .reset_index()
            ),
            on=IDX_PM_ESC,
        )
        .assign(
            # fill in the missing generation with zeros (this will help ensure
            # the calculations to run the fractions in `calc_allocation_ratios`
            # can be consistent)
            # do the same with missing fuel consumption
            net_generation_mwh_g_tbl=lambda x: x.net_generation_mwh_g_tbl.fillna(0),
            fuel_consumed_mmbtu_bf_tbl=lambda x: x.fuel_consumed_mmbtu_bf_tbl.fillna(0),
        )
    )
    # fuel consumed summed by prime mover and fuel from each table
    # for f_col in ['fuel_consumed_mmbtu_gf_tbl', 'fuel_consumed_mmbtu_bf_tbl']:
    # gen_pm_fuel[f'{f_col}_pm'] = (
    #     gen_pm_fuel.groupby(IDX_PM, dropna=False)
    #     [[f'{f_col}']].transform(sum, min_count=1)
    # )
    # gen_pm_fuel[f'{f_col}_fuel'] = (
    #     gen_pm_fuel.groupby(IDX_U_ESC, dropna=False)
    #     [[f'{f_col}']].transform(sum, min_count=1)
    # )
    # Add a column that indicates how much capacity comes from generators that
    # report in the generation table, and how much comes only from generators
    # that show up in the generation_fuel table.
    gen_pm_fuel = pd.merge(
        gen_pm_fuel,
        gen_pm_fuel.groupby(by=IDX_PM_ESC + ["in_g_tbl"], dropna=False)[["capacity_mw"]]
        .sum(min_count=1)
        .add_suffix("_in_g_tbl_group")
        .reset_index(),
        on=IDX_PM_ESC + ["in_g_tbl"],
    )
    gen_pm_fuel["capacity_mw_fuel_in_bf_tbl_group"] = gen_pm_fuel.groupby(
        IDX_ESC + ["in_bf_tbl", "unit_id_pudl"], dropna=False
    )[["capacity_mw"]].transform(sum, min_count=1)

    return gen_pm_fuel


def allocate_net_gen_by_gen_esc(gen_pm_fuel):
    """
    Allocate net generation to generators/energy_source_code via three methods.

    There are three main types of generators:
      * "all gen": generators of plants which fully report to the
        generators_eia860 table.
      * "some gen": generators of plants which partially report to the
        generators_eia860 table.
      * "gf only": generators of plants which do not report at all to the
        generators_eia860 table.

    Each different type of generator needs to be treated slightly differently,
    but all will end up with a ``frac`` column that can be used to allocate
    the ``net_generation_mwh_gf_tbl``.

    Args:
        gen_pm_fuel (pandas.DataFrame): output of :func:``prep_alloction_fraction()``.

    """
    # break out the table into these four different generator types.
    all_gen = gen_pm_fuel.loc[gen_pm_fuel.in_g_tbl_all]
    some_gen = gen_pm_fuel.loc[gen_pm_fuel.in_g_tbl_any & ~gen_pm_fuel.in_g_tbl_all]
    gf_only = gen_pm_fuel.loc[~gen_pm_fuel.in_g_tbl_any]

    logger.info(
        "Ratio calc types: \n"
        f"   All gens w/in generation table:  {len(all_gen)}#, {all_gen.capacity_mw.sum():.2} MW\n"
        f"   Some gens w/in generation table: {len(some_gen)}#, {some_gen.capacity_mw.sum():.2} MW\n"
        f"   No gens w/in generation table:   {len(gf_only)}#, {gf_only.capacity_mw.sum():.2} MW"
    )
    if len(gen_pm_fuel) != len(all_gen) + len(some_gen) + len(gf_only):
        raise AssertionError(
            "Error in splitting the gens between records showing up fully, "
            "partially, or not at all in the generation table."
        )

    # In the case where we have all of teh generation from the generation
    # table, we still allocate, because the generation reported in these two
    # tables don't always match perfectly
    all_gen = all_gen.assign(
        frac_net_gen=lambda x: x.net_generation_mwh_g_tbl
        / x.net_generation_mwh_g_tbl_pm_fuel,
        frac=lambda x: x.frac_net_gen,
    )
    # _ = _test_frac(all_gen)

    # a brief explaination of the equations below
    # input definitions:
    #   ng == net generation from the generation table (by generator)
    #   ngf == net generation from the generation fuel table (summed by PM/Fuel)
    #   ngt == total net generation from the generation table (summed by PM/Fuel)
    #
    # y = ngt / ngf (fraction of generation reporting in the generation table)
    # z = ng * ngt (fraction of generation from generation table by generator)
    # g = y * z  (fraction of generation reporting in generation table by generator - frac_gen)

    some_gen = some_gen.assign(
        # fraction of the generation that should go to the generators that
        # report in the generation table
        frac_from_g_tbl=lambda x: x.net_generation_mwh_g_tbl_pm_fuel
        / x.net_generation_mwh_gf_tbl,
        # for records within these mix groups that do have net gen in the
        # generation table..
        frac_net_gen=lambda x: x.net_generation_mwh_g_tbl
        / x.net_generation_mwh_g_tbl_pm_fuel,  # generator based net gen from gen table
        frac_gen=lambda x: x.frac_net_gen * x.frac_from_g_tbl,
        # fraction of generation that does not show up in the generation table
        frac_missing_from_g_tbl=lambda x: 1 - x.frac_from_g_tbl,
        capacity_mw_missing_from_g_tbl=lambda x: np.where(x.in_g_tbl, 0, x.capacity_mw),
        frac_cap=lambda x: x.frac_missing_from_g_tbl
        * (x.capacity_mw_missing_from_g_tbl / x.capacity_mw_in_g_tbl_group),
        # the real deal
        # this could aslo be `x.frac_gen + x.frac_cap` because the frac_gen
        # should be 0 for any generator that does not have net gen in the g_tbl
        # and frac_cap should be 0 for any generator that has net gen in the
        # g_tbl.
        frac=lambda x: np.where(x.in_g_tbl, x.frac_gen, x.frac_cap),
    )
    # _ = _test_frac(some_gen)

    # Calculate what fraction of the total capacity is associated with each of
    # the generators in the grouping.
    gf_only = gf_only.assign(
        frac_cap=lambda x: x.capacity_mw / x.capacity_mw_pm_fuel,
        frac=lambda x: x.frac_cap,
    )
    # _ = _test_frac(gf_only)

    # squish all of these methods back together.
    net_gen_alloc = pd.concat([all_gen, some_gen, gf_only])
    # null out the inf's
    net_gen_alloc.loc[abs(net_gen_alloc.frac) == np.inf] = np.NaN
    _ = _test_frac(net_gen_alloc)

    # do the allocating-ing!
    net_gen_alloc = (
        net_gen_alloc.assign(
            # we could x.net_generation_mwh_g_tbl.fillna here if we wanted to
            # take the net gen
            net_generation_mwh=lambda x: x.net_generation_mwh_gf_tbl
            * x.frac,
        )
        .pipe(apply_pudl_dtypes, group="eia")
        .dropna(how="all")
    )

    return net_gen_alloc


def allocate_fuel_by_gen_esc(gen_pm_fuel):
    """
    Allocate fuel_consumption to generators/energy_source_code via three methods.

    There are three main types of generators:
      * "all gen": generators of plants which fully report to the
        boiler_fuel_eia923 table.
      * "some gen": generators of plants which partially report to the
        boiler_fuel_eia923 table.
      * "gf only": generators of plants which do not report at all to the
        boiler_fuel_eia923 table.

    Each different type of generator needs to be treated slightly differently,
    but all will end up with a ``frac`` column that can be used to allocate
    the ``fuel_consumed_mmbtu_gf_tbl``.

    Args:
        gen_pm_fuel (pandas.DataFrame): output of :func:``prep_alloction_fraction()``.

    """
    # break out the table into these four different generator types.
    all_gen = gen_pm_fuel.loc[gen_pm_fuel.in_bf_tbl_all]
    some_gen = gen_pm_fuel.loc[gen_pm_fuel.in_bf_tbl_any & ~gen_pm_fuel.in_bf_tbl_all]
    bf_only = gen_pm_fuel.loc[~gen_pm_fuel.in_bf_tbl_any]

    logger.info(
        "Ratio calc types: \n"
        f"   All gens w/in generation table:  {len(all_gen)}#, {all_gen.capacity_mw.sum():.2} MW\n"
        f"   Some gens w/in generation table: {len(some_gen)}#, {some_gen.capacity_mw.sum():.2} MW\n"
        f"   No gens w/in generation table:   {len(bf_only)}#, {bf_only.capacity_mw.sum():.2} MW"
    )
    if len(gen_pm_fuel) != len(all_gen) + len(some_gen) + len(bf_only):
        raise AssertionError(
            "Error in splitting the gens between records showing up fully, "
            "partially, or not at all in the generation table."
        )

    # In the case where we have all of teh fuel from the bf
    # table, we still allocate, because the fuel reported in these two
    # tables don't always match perfectly
    all_gen = all_gen.assign(
        frac_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl
        / x.fuel_consumed_mmbtu_bf_tbl_pm_fuel,
        frac=lambda x: x.frac_fuel,
    )
    # _ = _test_frac(all_gen)

    # a brief explaination of the equations below
    # input definitions:
    #   ng == net generation from the generation table (by generator)
    #   ngf == net generation from the generation fuel table (summed by PM/Fuel)
    #   ngt == total net generation from the generation table (summed by PM/Fuel)
    #
    # y = ngt / ngf (fraction of generation reporting in the generation table)
    # z = ng * ngt (fraction of generation from generation table by generator)
    # g = y * z  (fraction of generation reporting in generation table by generator - frac_bf)

    some_gen = some_gen.assign(
        # fraction of the generation that should go to the generators that
        # report in the generation table
        frac_from_bf_tbl=lambda x: x.fuel_consumed_mmbtu_bf_tbl_pm_fuel
        / x.fuel_consumed_mmbtu_gf_tbl,
        # for records within these mix groups that do have net gen in the
        # generation table..
        frac_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl
        / x.fuel_consumed_mmbtu_bf_tbl_pm_fuel,  # generator based net gen from gen table
        frac_bf=lambda x: x.frac_fuel * x.frac_from_bf_tbl,
        # fraction of generation that does not show up in the generation table
        frac_missing_from_bf_tbl=lambda x: 1 - x.frac_from_bf_tbl,
        capacity_mw_missing_from_bf_tbl=lambda x: np.where(
            x.in_bf_tbl, 0, x.capacity_mw
        ),
        frac_cap=lambda x: x.frac_missing_from_bf_tbl
        * (x.capacity_mw_missing_from_bf_tbl / x.capacity_mw_fuel_in_bf_tbl_group),
        # the real deal
        # this could aslo be `x.frac_bf + x.frac_cap` because the frac_bf
        # should be 0 for any generator that does not have net gen in the g_tbl
        # and frac_cap should be 0 for any generator that has net gen in the
        # g_tbl.
        frac=lambda x: np.where(x.in_bf_tbl, x.frac_bf, x.frac_cap),
    )
    # _ = _test_frac(some_gen)

    # Calculate what fraction of the total capacity is associated with each of
    # the generators in the grouping.
    bf_only = bf_only.assign(
        frac_cap=lambda x: x.capacity_mw / x.capacity_mw_pm_fuel,
        frac=lambda x: x.frac_cap,
    )
    # _ = _test_frac(bf_only)

    # squish all of these methods back together.
    fuel_alloc = pd.concat([all_gen, some_gen, bf_only])
    # null out the inf's
    fuel_alloc.loc[abs(fuel_alloc.frac) == np.inf] = np.NaN
    _ = _test_frac(fuel_alloc)

    # do the allocating-ing!
    fuel_alloc = (
        fuel_alloc.assign(
            # we could x.fuel_consumed_mmbtu_bf_tbl.fillna here if we wanted to
            # take the net gen
            fuel_consumed_mmbtu=lambda x: x.fuel_consumed_mmbtu_gf_tbl * x.frac,
            fuel_consumed_for_electricity_mmbtu=lambda x: x.fuel_consumed_for_electricity_mmbtu_gf_tbl
            * x.frac,
        )
        .pipe(apply_pudl_dtypes, group="eia")
        .dropna(how="all")
    )

    return fuel_alloc


def _test_frac(gen_pm_fuel):
    # test! Check if each of the IDX_PM_ESC groups frac's add up to 1
    frac_test = (
        gen_pm_fuel.groupby(IDX_PM_ESC)[["frac", "net_generation_mwh_g_tbl"]]
        .sum(min_count=1)
        .reset_index()
    )

    frac_test_bad = frac_test[~np.isclose(frac_test.frac, 1) & frac_test.frac.notnull()]
    if not frac_test_bad.empty:
        # raise AssertionError(
        warnings.warn(
            f"Ooopsies. You got {len(frac_test_bad)} records where the "
            "'frac' column isn't adding up to 1 for each 'IDX_PM_ESC' "
            "group. Check 'make_allocation_frac()'"
        )
    return frac_test_bad


def _test_gen_pm_fuel_output(gen_pm_fuel, gf, gen):
    # this is just for testing/debugging
    def calc_net_gen_diff(gen_pm_fuel, idx):
        gen_pm_fuel_test = pd.merge(
            gen_pm_fuel,
            gen_pm_fuel.groupby(by=idx)[["net_generation_mwh"]]
            .sum(min_count=1)
            .add_suffix("_test")
            .reset_index(),
            on=idx,
            how="outer",
        ).assign(
            net_generation_mwh_diff=lambda x: x.net_generation_mwh_gf_tbl
            - x.net_generation_mwh_test
        )
        return gen_pm_fuel_test

    # make different totals and calc differences for two different indexs
    gen_pm_fuel_test = calc_net_gen_diff(gen_pm_fuel, idx=IDX_PM_ESC)
    gen_fuel_test = calc_net_gen_diff(gen_pm_fuel, idx=IDX_ESC)

    gen_pm_fuel_test = gen_pm_fuel_test.assign(
        net_generation_mwh_test=lambda x: x.net_generation_mwh_test.fillna(
            gen_fuel_test.net_generation_mwh_test
        ),
        net_generation_mwh_diff=lambda x: x.net_generation_mwh_diff.fillna(
            gen_fuel_test.net_generation_mwh_diff
        ),
    )

    bad_diff = gen_pm_fuel_test[
        (~np.isclose(gen_pm_fuel_test.net_generation_mwh_diff, 0))
        & (gen_pm_fuel_test.net_generation_mwh_diff.notnull())
    ]
    logger.info(
        f"{len(bad_diff)/len(gen_pm_fuel):.03%} of records have are partially "
        "off from their 'IDX_PM_ESC' group"
    )
    no_cap_gen = gen_pm_fuel_test[
        (gen_pm_fuel_test.capacity_mw.isnull())
        & (gen_pm_fuel_test.net_generation_mwh_g_tbl.isnull())
    ]
    if len(no_cap_gen) > 15:
        logger.info(f"Warning: {len(no_cap_gen)} records have no capacity or net gen")
    # remove the junk/corrective plants
    fuel_net_gen = gf[gf.plant_id_eia != "99999"].net_generation_mwh.sum()
    # fuel_consumed = gf[gf.plant_id_eia != "99999"].fuel_consumed_mmbtu.sum()
    logger.info(
        "gen v fuel table net gen diff:      "
        f"{(gen.net_generation_mwh.sum())/fuel_net_gen:.1%}"
    )
    logger.info(
        "new v fuel table net gen diff:      "
        f"{(gen_pm_fuel_test.net_generation_mwh.sum())/fuel_net_gen:.1%}"
    )
    # logger.info(
    #     "new v fuel table fuel (mmbtu) diff: "
    #     f"{(gen_pm_fuel_test.fuel_consumed_mmbtu.sum())/fuel_consumed:.1%}"
    # )

    gen_pm_fuel_test = gen_pm_fuel_test.drop(
        columns=["net_generation_mwh_test", "net_generation_mwh_diff"]
    )
    return gen_pm_fuel_test


def _test_gen_fuel_allocation(gen, gen_pm_fuel, ratio=0.05):
    gens_test = pd.merge(
        agg_by_generator(gen_pm_fuel, sum_cols=["net_generation_mwh"]),
        gen,
        on=IDX_GENS,
        suffixes=("_new", "_og"),
    ).assign(
        net_generation_new_v_og=lambda x: x.net_generation_mwh_new
        / x.net_generation_mwh_og
    )

    os_ratios = gens_test[
        (~gens_test.net_generation_new_v_og.between((1 - ratio), (1 + ratio)))
        & (gens_test.net_generation_new_v_og.notnull())
    ]
    os_ratio = len(os_ratios) / len(gens_test)
    logger.info(
        f"{os_ratio:.2%} of generator records are more that {ratio:.0%} off from the net generation table"
    )
    if ratio == 0.05 and os_ratio > 0.15:
        warnings.warn(
            f"Many generator records that have allocated net gen more than {ratio:.0%}"
        )


###########################
# Fuel Allocation Functions
###########################


def remove_bf_nulls(bf: pd.DataFrame):
    """
    Remove nulls in the unit_id_pudl and nulls or 0's in fuel_consumed_mmbtu.

    We need to drop some nulls and zero's here. drop the fuel 0's/nulls bc
    there will be nothing to allocate to/go off. drop the null units bc there
    is ~5% of bf records w/o units and the association happend on the units.

    Returns:
        a copy of ``bf``
    """
    if len(bf[bf.unit_id_pudl.isnull()]) / len(bf) > 0.06:
        raise AssertionError("There are more than ")
    bf = (
        bf[(bf.fuel_consumed_mmbtu != 0) | bf.fuel_consumed_mmbtu.isnull()]
        .dropna(subset=["unit_id_pudl"])
        .copy()
    )
    return bf


def test_frac_cap_in_bf(in_bf_tbl, debug=False):
    """
    Test the frac_cap column for records w/ BF data.

    Raise:
        AssertionError: if `frac_cap` does not sum to 1 within
            each plant/fuel group (via `IDX_ESC`).
    """
    # frac_cap for each fuel group should sum to 1
    in_bf_tbl["frac_cap_test"] = in_bf_tbl.groupby(
        IDX_ESC + ["unit_id_pudl"], dropna=False
    )[["frac_cap"]].transform(sum, min_count=1)

    frac_cap_test = in_bf_tbl[~np.isclose(in_bf_tbl.frac_cap_test, 1)]
    if not frac_cap_test.empty:
        message = (
            "Mayday! Mayday! The `frac_cap` test has failed. We have "
            f"{len(frac_cap_test)} records who's `frac_cap` isn't summing to 1"
            " in each plant/fuel group. Check creation of "
            "`capacity_mw_fuel_in_bf_tbl_group` column in "
            "`prep_alloction_fraction()` or assignment of `frac_calc in "
            "`allocate_fuel_for_in_bf_gens()`"
        )
        if debug:
            warnings.warn(message)
        else:
            raise AssertionError(message)
    else:
        logger.info("You've passed the frac_cap test for the `in_bf_tbl` records")
    if not debug:
        in_bf_tbl = in_bf_tbl.drop(columns=["frac_cap_test"])


def create_monthly_gens_records(gens):
    """Creates a duplicate record for each month of the year in the gens file."""
    # If we want to allocate net generation at the monthly level, we need to ensure that the gens file has monthly records
    # to do this, we can duplicate the records in gens 11 times for each month, so that there is a record for each month of the year

    # create a copy of gens to hold the monthly data
    gens_month = gens.copy()

    month = 2
    while month <= 12:
        # add one month to the copied data each iteration
        gens_month["report_date"] = gens_month["report_date"] + pd.DateOffset(months=1)
        # concat this data to the gens file
        gens = pd.concat([gens, gens_month], axis=0)
        month += 1

    return gens


def manually_fix_energy_source_codes(gf):
    """Patch: reassigns fuel codes in the gf table that don't match the fuel code in the gens table."""
    # plant 10204 should be waste heat instead of other
    gf.loc[
        (gf["plant_id_eia"] == 10204) & (gf["energy_source_code"] == "OTH"),
        "energy_source_code",
    ] = "WH"

    return gf


def manually_fix_prime_movers(gens):
    """
    Patch: Ensures prime movers assigned in 2020 match.

    The prime mover identified in the generators_entity table does not always match the
    prime mover identified in the generators file for a specific year
    This is currently a temporary patch until this issue can be more broadly resolved
    See https://github.com/catalyst-cooperative/pudl/issues/1585
    """
    # fix cogeneration issues
    plant_ids = [2465, 50150, 54268, 54410, 54262]
    for plant_id in plant_ids:
        gens.loc[
            (gens["plant_id_eia"] == plant_id) & (gens["prime_mover_code"] == "CT"),
            "prime_mover_code",
        ] = "GT"
        gens.loc[
            (gens["plant_id_eia"] == plant_id) & (gens["prime_mover_code"] == "CA"),
            "prime_mover_code",
        ] = "ST"

    return gens


def adjust_energy_source_codes(gens, gf, bf):
    """
    Adds startup fuels to the list of energy source codes and adjusts MSW codes.

    Adds the energy source code of any startup fuels to the energy source
    columns so that any fuel burned in startup can be allocated to the generator

    Adjust the MSW codes in gens to match those used in gf and bf.

    In recent years, EIA-923 started splitting out the ``MSW`` (municipal_solid_waste)
    into its consitituent components``MSB`` (municipal_solid_waste_biogenic) and
    ``MSN`` (municipal_solid_nonbiogenic). However, the EIA-860 Generators table still
    only uses the ``MSW`` code.

    This function identifies which MSW codes are used in teh gf and bf tables and creates records
    to match these.
    """
    # drop the planned energy source code column
    gens = gens.drop(columns=["planned_energy_source_code_1"])

    # create a column of all unique fuels in the order in which they appear (ESC 1-6, startup fuel 1-6)
    # this column will have each fuel code separated by a comma
    gens["unique_esc"] = [
        ",".join((fuel for fuel in list(dict.fromkeys(fuels)) if pd.notnull(fuel)))
        for fuels in gens.filter(like="source_code").values
    ]

    # Adjust any energy source codes related to municipal solid waste
    # get a list of all of the MSW-related codes used in gf and bf
    msw_codes_in_gf = set(
        list(
            gf.loc[
                gf["energy_source_code"].isin(["MSW", "MSB", "MSN"]),
                "energy_source_code",
            ].unique()
        )
    )
    msw_codes_in_bf = set(
        list(
            bf.loc[
                bf["energy_source_code"].isin(["MSW", "MSB", "MSN"]),
                "energy_source_code",
            ].unique()
        )
    )
    msw_codes_used = list(msw_codes_in_gf | msw_codes_in_bf)
    # join these codes into a string that will be used to replace the MSW code
    replacement_codes = ",".join(msw_codes_used)
    # replace any MSW codes with the codes used in bf and gf
    gens.loc[gens["unique_esc"].str.contains("MSW"), "unique_esc"] = gens.loc[
        gens["unique_esc"].str.contains("MSW"), "unique_esc"
    ].str.replace("MSW", replacement_codes)

    # we need to create numbered energy source code columns for each fuel
    # first, we need to identify the maximum number of fuel codes that exist for a single generator
    max_num_esc = (gens.unique_esc.str.count(",") + 1).max()

    # create a list of numbered fuel code columns
    esc_columns_to_add = []
    for n in range(1, max_num_esc + 1):
        esc_columns_to_add.append(f"energy_source_code_{n}")

    # drop all of the existing energy source code columns
    gens = gens.drop(columns=list(gens.filter(like="source_code").columns))

    # create new numbered energy source code columns by expanding the list of unique fuels
    gens[esc_columns_to_add] = gens["unique_esc"].str.split(",", expand=True)

    # drop the intermediate column
    gens = gens.drop(columns=["unique_esc"])

    return gens
