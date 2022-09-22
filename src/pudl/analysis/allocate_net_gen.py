"""Allocate data from generation_fuel_eia923 table to generator level.

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
* The boiler_fuel_eia923 table is the authoritative source of information about
  how much fuel consumption is attributable to an individual boiler, if it reports
  in that table.
* The generation_fuel_eia923 table is the authoritative source of information
  about how much generation and fuel consumption is attributable to an entire
  plant.
* The generators_eia860 table provides an exhaustive list of all generators
  whose generation is being reported in the generation_fuel_eia923 table.

We allocate the net generation/fuel consumption reported in the
generation_fuel_eia923/boiler_fuel_eia923 table on the basis of plant, prime mover,
and fuel type among the generators in each plant that have matching fuel types.
Generation/fuel consumption is allocated proportional to reported generation/fuel consumption
if it's available, and proportional to each generator's capacity if
generation/fuel consumption is not available.

In more detail: within each month of data, we split the plants into three groups:

* Plants where ALL generators report in the more granular generation_eia923 /
  boiler_fuel_eia923 table.
* Plants where NONE of the generators report in the generation_eia923 /
  boiler_fuel_eia923 table.
* Plants where only SOME of the generators report in the generation_eia923 /
  boiler_fuel_eia923 table.

In plant-months where ALL generators report more granular generation or fuel consumption,
the total net generation or fuel consumption reported in the generation_fuel_eia923 table
is allocated in proportion to the generation each generator reported in the
generation_eia923 table or fuel consumption reported in the boiler_fuel_eia923 table. We
do this instead of using net_generation_mwh from generation_eia923  or using
fuel_consumed_mmbtu from boiler_fuel_eia923 because there are some small
discrepancies between the total amounts of generation/fuel reported in these two tables.

In plant-months where NONE of the generators report more granular generation or fuel consumption,
we create a generator record for each associated fuel type. Those records are merged with the
generation_fuel_eia923 table on plant, prime mover code, and fuel type. Each group of
plant, prime mover, and fuel will have some amount of reported net generation associated
with it, and one or more generators. The net generation is allocated among the
generators within the group in proportion to their capacity. Then the allocated net
generation or fuel consumption is summed up by generator.

In the hybrid case, where only SOME of of a plant's generators report the more granular
generation or fuel data, we use a combination of the two allocation methods described above.
First, the total generation or fuel consumption reported across a plant in the
generation_fuel_eia923 table is allocated between the two categories of generators
(those that report fine-grained generation/fuel data, and those that don't) in direct
proportion to the fraction of the plant's generation which is reported in the
generation_eia923 table (or the fraction of the plant's fuel consumption which is reported
in the boiler_fuel_eia923 table), relative to the total generation or fuel reported in the
generation_fuel_eia923 table.

Because fuel consumption in the boiler_fuel_eia923 table is reported per boiler_id,
we must first map this data to generators using the boiler_generator_assn_eia860 table.
For boilers that have a 1:m or m:m relationship with generators, we allocate the reported
fuel to each associated generator based on the nameplate capacity of each generator.
So if boiler "1" was associated with generator A (25 MW) and generator B (75 MW), 25%
of the fuel consumption would be allocated to generator A and 75% would be allocated to
generator B.

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

# Useful high-level external modules.
import numpy as np
import pandas as pd

import pudl.helpers
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)

IDX_GENS = ["report_date", "plant_id_eia", "generator_id"]
"""Id columns for generators."""

IDX_GENS_PM_ESC = [
    "report_date",
    "plant_id_eia",
    "generator_id",
    "prime_mover_code",
    "energy_source_code",
]

IDX_PM_ESC = ["report_date", "plant_id_eia", "energy_source_code", "prime_mover_code"]
"""Id columns for plant, prime mover & fuel type records."""

IDX_B_PM_ESC = [
    "report_date",
    "plant_id_eia",
    "boiler_id",
    "energy_source_code",
    "prime_mover_code",
]
"""Id columns for plant, boiler, prime mover & fuel type records."""

IDX_ESC = ["report_date", "plant_id_eia", "energy_source_code"]

IDX_U_ESC = ["report_date", "plant_id_eia", "energy_source_code", "unit_id_pudl"]

"""Data columns from generation_fuel_eia923 that are being allocated."""


# Two top-level functions (allocate & aggregate)
def allocate_gen_fuel_by_generator_energy_source(pudl_out, drop_interim_cols=True):
    """Allocate net gen from gen_fuel table to the generator/energy_source_code level.

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
        .loc[:, IDX_B_PM_ESC + ["fuel_consumed_mmbtu"]]
    ).pipe(
        distribute_annually_reported_data_to_months,
        key_columns=["plant_id_eia", "boiler_id", "energy_source_code"],
        data_column_name="fuel_consumed_mmbtu",
    )
    # load boiler generator associations
    bga = pudl_out.bga_eia860().loc[
        :,
        [
            "plant_id_eia",
            "boiler_id",
            "generator_id",
            "report_date",
        ],
    ]
    # allocate the boiler fuel data to generators
    bf = allocate_bf_data_to_gens(bf, gens, bga)

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
        logger.info(
            missing_pm[
                [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "prime_mover_code",
                    "unit_id_pudl",
                    "operational_status",
                    "energy_source_code_1",
                ]
            ]
        )
    # duplicate each entry in the gens table 12 times to create an entry for each month of the year
    if pudl_out.freq == "MS":
        gens = pudl.helpers.expand_timeseries(
            df=gens, key_cols=["plant_id_eia", "generator_id"], freq="MS"
        )

    gen = (
        pudl_out.gen_original_eia923().loc[:, IDX_GENS + ["net_generation_mwh"]]
        # removes 4 records with NaN generator_id as of pudl v0.5
        .dropna(subset=IDX_GENS)
    ).pipe(
        distribute_annually_reported_data_to_months,
        key_columns=["plant_id_eia", "generator_id"],
        data_column_name="net_generation_mwh",
    )

    # the gen table is missing some generator ids. Let's fill this using the gens table, leaving a missing value for net generation
    gen = gen.merge(
        gens[["plant_id_eia", "generator_id", "report_date"]],
        how="outer",
        on=["plant_id_eia", "generator_id", "report_date"],
    )

    # do the association!
    gen_assoc = associate_generator_tables(
        gf=gf, gen=gen, gens=gens, bf=bf, pudl_out=pudl_out
    )

    # Generate a fraction to use to allocate net generation and fuel consumption by.
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
            IDX_PM_ESC
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
            IDX_PM_ESC
            + [
                "generator_id",
                "energy_source_code_num",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
        ]

    # ensure that the allocated data has unique merge keys
    net_gen_alloc = group_duplicate_keys(net_gen_alloc)
    fuel_alloc = group_duplicate_keys(fuel_alloc)

    # squish net gen and fuel allocation together
    net_gen_fuel_alloc = pd.merge(
        net_gen_alloc,
        fuel_alloc,
        on=IDX_PM_ESC + ["generator_id", "energy_source_code_num"],
        how="outer",
        validate="1:1",
    ).sort_values(IDX_PM_ESC + ["generator_id", "energy_source_code_num"])
    return net_gen_fuel_alloc


def group_duplicate_keys(df):
    """Catches duplicate keys in the allocated data and groups them together.

    Merging `net_gen_alloc` and `fuel_alloc` together requires unique keys in each df.
    Sometimes the allocation process creates duplicate keys. This function identifies
    when this happens, and aggregates the data on these keys to remove the duplicates.
    """
    # identify any duplicate records
    duplicate_keys = df[
        df.duplicated(subset=(IDX_PM_ESC + ["generator_id", "energy_source_code_num"]))
    ]
    # if there are duplicate records, print a warning and fix the issue
    if len(duplicate_keys) > 0:
        warnings.warn(
            "Duplicate keys exist in the allocated data."
            "These will be grouped together, but check the source "
            "of this issue."
        )
        logger.warning(
            duplicate_keys[IDX_PM_ESC + ["generator_id", "energy_source_code_num"]]
        )
        df = (
            df.groupby(IDX_PM_ESC + ["generator_id", "energy_source_code_num"])
            .sum()
            .reset_index()
        )
    return df


def aggregate_gen_fuel_by_generator(
    pudl_out,
    net_gen_fuel_alloc: pd.DataFrame,
    sum_cols: list[str] = [
        "net_generation_mwh",
        "fuel_consumed_mmbtu",
        "fuel_consumed_for_electricity_mmbtu",
    ],
) -> pd.DataFrame:
    """Aggregate gen fuel data columns to generators.

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
    """Scale allocated net gen at the generator/energy_source_code level by ownership.

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
    by_cols: list[str] = IDX_GENS,
    sum_cols: list[str] = [
        "net_generation_mwh",
        "fuel_consumed_mmbtu",
        "fuel_consumed_for_electricity_mmbtu",
    ],
) -> pd.DataFrame:
    """Aggreate the allocated gen fuel data to the generator level.

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
    """Stack the generator table with a set of columns.

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


def associate_generator_tables(gf, gen, gens, bf, pudl_out):
    """Associate the three tables needed to assign net gen to generators.

    Args:
        gf (pandas.DataFrame): generator_fuel_eia923 table with columns:
            ``IDX_PM_ESC`` and `net_generation_mwh` and `fuel_consumed_mmbtu`.
        gen (pandas.DataFrame): generation_eia923 table with columns:
            ``IDX_GENS`` and `net_generation_mwh`.
        gens (pandas.DataFrame): generators_eia860 table with cols: ``IDX_GENS``
            and all of the `energy_source_code` columns
        bf (pandas.DataFrame): boiler_fuel_eia923 table
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create the
            tables for EIA and FERC Form 1 analysis.

    TODO: Convert these groupby/merges into transforms.
    """
    stack_gens = stack_generators(
        gens, cat_col="energy_source_code_num", stacked_col="energy_source_code"
    )

    bf_summed = (
        bf.groupby(by=IDX_GENS_PM_ESC, dropna=False)
        .sum(min_count=1)
        .add_suffix("_bf_tbl")
        .reset_index()
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
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
    )
    # TODO: remove "_gf_tbl_fuel" - it does not seem to be used
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
        .pipe(remove_inactive_generators, pudl_out=pudl_out)
        .merge(bf_summed, on=IDX_GENS_PM_ESC, how="left", validate="m:1")
        .merge(
            gf_fuel_summed,
            on=IDX_ESC,
            how="left",
            validate="m:1",
        )
    )

    # replace zeros with small number to avoid div by zero errors when calculating allocation fraction
    data_columns = [
        "net_generation_mwh_g_tbl",
        "fuel_consumed_mmbtu_gf_tbl",
        "fuel_consumed_for_electricity_mmbtu_gf_tbl",
        "net_generation_mwh_gf_tbl",
        "fuel_consumed_mmbtu_bf_tbl",
        "fuel_consumed_mmbtu_gf_tbl_fuel",
        "fuel_consumed_for_electricity_mmbtu_gf_tbl_fuel",
    ]
    gen_assoc[data_columns] = gen_assoc[data_columns].replace(0, 0.001)

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


def remove_inactive_generators(gen_assoc, pudl_out):
    """Remove the retired generators.

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
        pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create the
            tables for EIA and FERC Form 1 analysis.
    """
    existing = gen_assoc.loc[(gen_assoc.operational_status == "existing")]
    # keep the gens that retired mid-report-year that have generator
    # specific data
    retiring_generators = gen_assoc.loc[
        (gen_assoc.operational_status == "retired")
        & (
            (gen_assoc.report_date <= gen_assoc.retirement_date)
            | (gen_assoc.net_generation_mwh_g_tbl.notnull())
        )
    ]

    # Get a list of all of the plants with a retired generator and non-null/non-zero gf generation data reported after the retirement date
    retired_with_gf = list(
        gen_assoc.loc[
            (gen_assoc.operational_status == "retired")
            & (gen_assoc.report_date > gen_assoc.retirement_date)
            & (gen_assoc.net_generation_mwh_gf_tbl.notnull())
            & (gen_assoc.net_generation_mwh_g_tbl.isnull())
            & (gen_assoc.net_generation_mwh_gf_tbl != 0),
            "plant_id_eia",
        ].unique()
    )

    # create a table for all of these plants that identifies all of the unique operational statuses
    plants_with_retired_generators = gen_assoc.loc[
        gen_assoc["plant_id_eia"].isin(retired_with_gf),
        ["plant_id_eia", "operational_status", "retirement_date"],
    ].drop_duplicates()

    # remove plants that have operational statuses other than retired
    plants_with_nonretired_generators = list(
        plants_with_retired_generators.loc[
            (plants_with_retired_generators["operational_status"] != "retired"),
            "plant_id_eia",
        ].unique()
    )
    plants_with_retired_generators = plants_with_retired_generators[
        ~plants_with_retired_generators["plant_id_eia"].isin(
            plants_with_nonretired_generators
        )
    ]

    # only keep the plants where all retirement dates are before the current year
    plants_retiring_after_start_date = list(
        plants_with_retired_generators.loc[
            plants_with_retired_generators["retirement_date"] >= pudl_out.start_date,
            "plant_id_eia",
        ].unique()
    )
    entirely_retired_plants = plants_with_retired_generators[
        ~plants_with_retired_generators["plant_id_eia"].isin(
            plants_retiring_after_start_date
        )
    ]

    entirely_retired_plants = list(entirely_retired_plants["plant_id_eia"].unique())

    retired_plants = gen_assoc[gen_assoc["plant_id_eia"].isin(entirely_retired_plants)]

    # sometimes a plant will report generation data before its proposed operating date
    # we want to keep any data that is reported for proposed generators
    proposed_generators = gen_assoc.loc[
        (gen_assoc.operational_status == "proposed")
        & (gen_assoc.net_generation_mwh_g_tbl.notnull())
    ]

    # when we do not have generator-specific generation for a proposed generator, we can also
    # look at whether there is generation reported from the gf table. However, if a proposed
    # generator is part of an existing plant, it is possible that this gf generation belongs
    # to one of the other existing generators. Thus, we want to identify those proposed generators
    # where the entire plant is proposed (since the gf-reported generation could only come from
    # one of the new generators).

    # Get a list of all of the plants that have a proposed generator with non-null and non-zero gf generation
    proposed_with_gf = list(
        gen_assoc.loc[
            (gen_assoc.operational_status == "proposed")
            & (gen_assoc.net_generation_mwh_gf_tbl.notnull())
            & (gen_assoc.net_generation_mwh_gf_tbl != 0),
            "plant_id_eia",
        ].unique()
    )

    # create a table for all of these plants that identifies all of the unique operational statuses
    plants_with_proposed_generators = gen_assoc.loc[
        gen_assoc["plant_id_eia"].isin(proposed_with_gf),
        ["plant_id_eia", "operational_status"],
    ].drop_duplicates()

    # filter this list to those plant ids where the only operational status is "proposed"
    # i.e. where the entire plant is new
    entirely_new_plants = plants_with_proposed_generators[
        (~plants_with_proposed_generators.duplicated(subset="plant_id_eia", keep=False))
        & (plants_with_proposed_generators["operational_status"] == "proposed")
    ]
    # convert this table into a list of these plant ids
    entirely_new_plants = list(entirely_new_plants["plant_id_eia"].unique())

    # keep data for these proposed plants in months where there is reported data
    proposed_plants = gen_assoc[
        gen_assoc["plant_id_eia"].isin(entirely_new_plants)
        & gen_assoc["net_generation_mwh_gf_tbl"].notnull()
    ]

    gen_assoc_removed = pd.concat(
        [
            existing,
            retiring_generators,
            retired_plants,
            proposed_generators,
            proposed_plants,
        ]
    )

    return gen_assoc_removed


def _associate_unconnected_records(eia_generators_merged: pd.DataFrame):
    """Associate unassociated gen_fuel table records on idx_pm.

    There are a subset of generation_fuel_eia923 records which do not
    merge onto the stacked generator table on ``IDX_PM_ESC``. These records
    generally don't match with the set of prime movers and fuel types in the
    stacked generator table. In this method, we associate those straggler,
    unconnected records by merging these records with the stacked generators on
    the prime mover only.

    Args:
        eia_generators_merged:

    """  # noqa: D417
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
        .sum(min_count=1, numeric_only=True)
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
    """Make flags and aggregations to prepare for the `allocate_net_gen_by_gen_esc()` and `allocate_fuel_by_gen_esc() functions`.

    In `allocate_net_gen_by_gen_esc()`, we will break the generators out into four
    types - see `allocate_net_gen_by_gen_esc()` docs for details. This function adds
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

    gens_gb_pm_esc = gen_assoc.groupby(by=IDX_PM_ESC, dropna=False)
    gens_gb_u_esc = gen_assoc.groupby(by=IDX_U_ESC, dropna=False)
    # get the total values for the merge group
    # we would use on groupby here with agg but it is much slower
    # so we're gb-ing twice w/ a merge
    # gens_gb.agg({'net_generation_mwh_g_tbl': lambda x: x.sum(min_count=1),
    #              'capacity_mw': lambda x: x.sum(min_count=1),
    #              'in_g_tbl': 'all'},)
    gen_pm_fuel = (
        gen_assoc.merge(  # flag if all generators exist in the generators_eia860 tbl
            gens_gb_pm_esc[["in_g_tbl"]].all().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_all"),
        )
        .merge(  # flag if some generators exist in the generators_eia860 tbl
            gens_gb_pm_esc[["in_g_tbl"]].any().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_any"),
        )
        .merge(  # flag if all generators exist in the boiler fuel tbl
            gens_gb_pm_esc[["in_bf_tbl"]].all().reset_index(),
            on=IDX_PM_ESC,
            suffixes=("", "_all"),
        )
        .merge(  # flag if some generators exist in the boiler fuel tbl
            gens_gb_pm_esc[["in_bf_tbl"]].any().reset_index(),
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
                gens_gb_pm_esc[
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
        .merge(
            (
                gens_gb_u_esc[
                    [
                        "fuel_consumed_mmbtu_bf_tbl",
                        "capacity_mw",
                    ]
                ]
                .sum(min_count=1)
                .add_suffix("_unit_fuel")
                .reset_index()
            ),
            on=IDX_U_ESC,
        )
        .assign(
            # fill in the missing generation with small numbers (this will help ensure
            # the calculations to run the fractions in `allocate_net_gen_by_gen_esc`
            # and `allocate_fuel_by_gen_esc` can be consistent)
            # do the same with missing fuel consumption
            net_generation_mwh_g_tbl=lambda x: x.net_generation_mwh_g_tbl.fillna(0.001),
            fuel_consumed_mmbtu_bf_tbl=lambda x: x.fuel_consumed_mmbtu_bf_tbl.fillna(
                0.001
            ),
            net_generation_mwh_g_tbl_pm_fuel=lambda x: x.net_generation_mwh_g_tbl_pm_fuel.fillna(
                0.001
            ),
            fuel_consumed_mmbtu_bf_tbl_pm_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl_pm_fuel.fillna(
                0.001
            ),
            fuel_consumed_mmbtu_bf_tbl_unit_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl_unit_fuel.fillna(
                0.001
            ),
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
        IDX_PM_ESC + ["in_bf_tbl"], dropna=False
    )[["capacity_mw"]].transform(sum, min_count=1)

    return gen_pm_fuel


def allocate_net_gen_by_gen_esc(gen_pm_fuel):
    """Allocate net generation to generators/energy_source_code via three methods.

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
    _ = _test_frac(net_gen_alloc)

    # replace the placeholder 0.001 values with zero before allocating
    # since some of these may have been aggregated, well, flag any values less than 0.01
    net_gen_alloc.loc[
        net_gen_alloc["net_generation_mwh_gf_tbl"] < 0.01, "net_generation_mwh_gf_tbl"
    ] = 0

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
    """Allocate fuel_consumption to generators/energy_source_code via three methods.

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

    # In the case where we have all of the fuel from the bf
    # table, we still allocate, because the fuel reported in these two
    # tables don't always match perfectly
    all_gen = all_gen.assign(
        frac_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl
        / x.fuel_consumed_mmbtu_bf_tbl_pm_fuel,
        frac=lambda x: x.frac_fuel,
    )
    # _ = _test_frac(all_gen)

    some_gen = some_gen.assign(
        # fraction of the fuel consumption that should go to the generators that
        # report in the boiler fuel table
        frac_from_bf_tbl=lambda x: x.fuel_consumed_mmbtu_bf_tbl_pm_fuel
        / x.fuel_consumed_mmbtu_gf_tbl,
        # for records within these mix groups that do have fuel consumption in the
        # bf table..
        frac_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl
        / x.fuel_consumed_mmbtu_bf_tbl_pm_fuel,  # generator based fuel from bf table
        frac_bf=lambda x: x.frac_fuel * x.frac_from_bf_tbl,
        # fraction of fuel that does not show up in the bf table
        # set minimum fraction to zero so we don't get negative fuel
        frac_missing_from_bf_tbl=lambda x: np.where(
            (x.frac_from_bf_tbl < 1), (1 - x.frac_from_bf_tbl), 0
        ),
        capacity_mw_missing_from_bf_tbl=lambda x: np.where(
            x.in_bf_tbl, 0, x.capacity_mw
        ),
        frac_cap=lambda x: x.frac_missing_from_bf_tbl
        * (x.capacity_mw_missing_from_bf_tbl / x.capacity_mw_fuel_in_bf_tbl_group),
        # the real deal
        # this could aslo be `x.frac_bf + x.frac_cap` because the frac_bf
        # should be 0 for any generator that does not have fuel in the bf_tbl
        # and frac_cap should be 0 for any generator that has fuel in the
        # bf_tbl.
        frac=lambda x: np.where(x.in_bf_tbl, x.frac_bf, x.frac_cap),
    )
    # _ = _test_frac(some_gen)

    # Calculate what fraction of the total capacity is associated with each of
    # the generators in the grouping.
    bf_only = bf_only.assign(
        frac_cap=lambda x: x.capacity_mw / x.capacity_mw_unit_fuel,
        frac=lambda x: x.frac_cap,
    )
    # _ = _test_frac(bf_only)

    # squish all of these methods back together.
    fuel_alloc = pd.concat([all_gen, some_gen, bf_only])
    # _ = _test_frac(fuel_alloc)

    # replace the placeholder 0.001 values with zero before allocating
    # since some of these may have been aggregated, well, flag any values less than 0.01
    fuel_alloc.loc[
        fuel_alloc["fuel_consumed_mmbtu_gf_tbl"] < 0.01, "fuel_consumed_mmbtu_gf_tbl"
    ] = 0
    fuel_alloc.loc[
        fuel_alloc["fuel_consumed_for_electricity_mmbtu_gf_tbl"] < 0.01,
        "fuel_consumed_for_electricity_mmbtu_gf_tbl",
    ] = 0

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


def distribute_annually_reported_data_to_months(df, key_columns, data_column_name):
    """Allocates annually-reported data from the gen or bf table to each month.

    Certain plants only report data to the generator table and boiler fuel table
    on an annual basis. In this case, their annual total is reported as a single
    value in December, and the other 11 months are reported as missing values. The
    raw EIA-923 table includes a column for 'Respondent Frequency' but this column
    is not currently included in pudl. This function first identifies which plants
    are annual respondents based on the pattern of how their data is reported. It
    then distributes this annually-reported value evenly across all months in the
    year.

    Args:
        df: a pandas dataframe, either loaded from pudl_out.gen_original_eia923() or pudl_out.bf_eia923()
        key_columns: a list of the primary key column names, either ["plant_id_eia","boiler_id","energy_source_code"] or ["plant_id_eia","generator_id"]
        data_column_name: the name of the data column to allocate, either "net_generation_mwh" or "fuel_consumed_mmbtu" depending on the df specified
    Returns:
        df with the annually reported values allocated to each month
    """
    # get a count of the number of missing values in a year
    annual_reporters = df.copy()
    annual_reporters["missing_data"] = annual_reporters[data_column_name].isna()
    annual_reporters = (
        annual_reporters.groupby(key_columns).sum()["missing_data"].reset_index()
    )
    # only keep plant-units where there is a single non-missing value in the year
    annual_reporters = annual_reporters[annual_reporters["missing_data"] == 11].drop(
        columns="missing_data"
    )
    # merge in the data for december - if the single non-missing value was in december, then this was likely an annual reporter
    annual_reporters = annual_reporters.merge(
        df[df["report_date"].dt.month == 12], how="left", on=key_columns, validate="1:1"
    )
    annual_reporters = annual_reporters[~annual_reporters[data_column_name].isna()]

    # distribute this data out to each of the monthly values
    df = df.merge(
        annual_reporters[key_columns + [data_column_name]],
        how="left",
        on=key_columns,
        suffixes=(None, "_annual_total"),
    )
    df.loc[:, data_column_name] = df.loc[:, data_column_name].fillna(
        df.loc[:, f"{data_column_name}_annual_total"] / 12
    )
    df = df.drop(columns=[f"{data_column_name}_annual_total"])

    return df


def manually_fix_energy_source_codes(gf):
    """Patch: reassigns fuel codes in the gf table that don't match the fuel code in the gens table."""
    # plant 10204 should be waste heat instead of other
    gf.loc[
        (gf["plant_id_eia"] == 10204) & (gf["energy_source_code"] == "OTH"),
        "energy_source_code",
    ] = "WH"

    return gf


def adjust_energy_source_codes(gens, gf, bf):
    """Adds startup fuels to the list of energy source codes and adjusts MSW codes.

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
        ",".join(fuel for fuel in list(dict.fromkeys(fuels)) if pd.notnull(fuel))
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


def allocate_bf_data_to_gens(bf, gens, bga):
    """Allocates bf data to the generator level.

    Distributes boiler-level data from boiler_fuel_eia923 to
    the generator level based on the boiler-generator association
    table and the nameplate capacity of the connected generators.
    """
    # merge generator capacity information into the BGA
    bga = bga.merge(
        gens[["plant_id_eia", "generator_id", "capacity_mw", "report_date"]],
        how="left",
        on=["plant_id_eia", "generator_id", "report_date"],
        validate="m:1",
    )
    # calculate an allocation fraction based on the capacity of each generator with which
    # a boiler is connected
    bga["cap_frac"] = bga[["capacity_mw"]] / bga.groupby(
        ["plant_id_eia", "boiler_id", "report_date"]
    )[["capacity_mw"]].transform("sum")

    # drop records from bf where there is missing fuel data
    bf = bf.dropna(subset="fuel_consumed_mmbtu")

    # merge in the generator id and the capacity fraction
    bf = pudl.helpers.date_merge(
        left=bf,
        right=bga,
        left_date_col="report_date",
        right_date_col="report_date",
        new_date_col="report_date",
        on=["plant_id_eia", "boiler_id"],
        date_on=["year"],
        how="left",
        report_at_start=True,
    )

    # distribute the boiler-level data to each generator based on the capacity fraciton
    bf["fuel_consumed_mmbtu"] = bf["fuel_consumed_mmbtu"] * bf["cap_frac"]

    # group the data by generator-PM-fuel, dropping records where there is no boiler-generator association
    bf = (
        bf.groupby(
            [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "energy_source_code",
                "prime_mover_code",
            ]
        )
        .sum()
        .reset_index()
    )

    # remove intermediate columns
    bf = bf.drop(columns=["capacity_mw", "cap_frac"])

    return bf
