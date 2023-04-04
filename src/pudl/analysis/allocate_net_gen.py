"""Allocate data from :ref:`generation_fuel_eia923` table to generator level.

The algorithm we're using assumes the following about the reported data:

* The :ref:`generation_fuel_eia923` table is the authoritative source of information
  about how much generation and fuel consumption is attributable to an entire
  plant. This table has the most complete data coverage, but it is not the most granular
  data reported. It's primary keys are :py:const:`IDX_PM_ESC`.
* The :ref:`generation_eia923` table contains the most granular net generation data. It
  is reported at the generator level with primary keys :py:const:`IDX_GENS`. This table
  includes only ~39% of the total MWhs reported in the :ref:`generation_fuel_eia923`
  table.
* The :ref:`boiler_fuel_eia923` table contains the most granular fuel consumption data.
  It is reported at the boiler/prime mover/energy source level with primary keys
  :py:const:`IDX_B_PM_ESC`. This table includes only ~38% of the total MMBTUs reported
  in the :ref:`generation_fuel_eia923` table.
* The :ref:`generators_eia860` table provides an exhaustive list of all generators
  whose generation is being reported in the :ref:`generation_fuel_eia923` table - with
  primary keys :py:const:`IDX_GENS`.

This module allocates the total net electricity generation and fuel consumption
reported in the :ref:`generation_fuel_eia923` table to individual generators, based
on more granular data reported in the :ref:`generation_eia923` and
:ref:`boiler_fuel_eia923` tables, as well as capacity (MW) found in the
:ref:`generators_eia860` table. It uses other generator attributes from the
:ref:`generators_eia860` table to associate the data found in the
:ref:`generation_fuel_eia923` with generators. It also uses as the associations between
boilers and generators found in the :ref:`boiler_generator_assn_eia860` table to
aggregate data :ref:`boiler_fuel_eia923` tables. The main coordinating functions hereare
:func:`allocate_gen_fuel_by_generator_energy_source` and
:func:`aggregate_gen_fuel_by_generator`.

Some definitions:

* **Data columns** refers to the net generation and fuel consumption - the specific
  columns are defined in :py:const:`DATA_COLUMNS`.
* **Granular tables** refers to :ref:`generation_eia923` and
  :ref:`boiler_fuel_eia923`, which report granular data but do not have complete
  coverage.

There are six main stages of the allocation process in this module:

#. **Read inputs**: Read denormalized net generation and fuel consumption data from the
   PUDL DB and standardize data reporting frequency. (See :func:`extract_input_tables`
   and :func:`standardize_input_frequency`).
#. **Associate inputs**: Merge data columns from the input tables described above on the
   basis of their shared primary key columns, producing an output with primary key
   :py:const:`IDX_GENS_PM_ESC`. This broadcasts many data values across multiple rows
   for use in the allocation process below (see :func:`associate_generator_tables`).
#. **Flag associated inputs**: For each record in the associated inputs, add boolean
   flags that separately indicate whether the generation and fuel consumption in that
   record are directly reported in the granular tables. This lets us choose an
   appropriate data allocation method based on how complete the granular data coverage
   is for a given value of :py:const:`IDX_PM_ESC`, which is the original primary key of
   the :ref:`generation_fuel_eia923` table. (See :func:`prep_allocation_fraction`).
#. **Allocate**: Allocate the net generation and fuel consumption reported in the less
   granular :ref:`generation_fuel_eia923` table to the :py:const:`IDX_GENS_PM_ESC`
   level. More details on the allocation process are below (see
   :func:`allocate_net_gen_by_gen_esc` and :func:`allocate_fuel_by_gen_esc`).
#. **Sanity check allocation**: Verify that the total allocated net generation and fuel
   consumption within each plant is equal to the total of the originally reported values
   within some tolerance (see :func:`test_original_gf_vs_the_allocated_by_gens_gf`).
   Warn if assumptions about the data and the outputs aren't met (see
   :func:`warn_if_missing_pms`, :func:`_test_frac`, :func:`test_gen_fuel_allocation` and
   :func:`_test_gen_pm_fuel_output`)
#. **Aggregate outputs**: Aggregate the allocated net generation and fuel consumption to
   the generator level, going from having primary keys of :py:const:`IDX_GENS_PM_ESC` to
   :py:const:`IDX_GENS` (see :func:`aggregate_gen_fuel_by_generator`).

**High-level description about the allocaiton step**:

We allocate the data columns reported in the :ref:`generation_fuel_eia923` table on the
basis of plant, prime mover, and energy source among the generators in each plant that
have matching energy sources.

We group the associated data columns by :py:const:`IDX_PM_ESC` and categorize
each resulting group of generators based on whether  **ALL**, **SOME**, or **NONE** of
them reported data in the granular tables. This is done for both the net generation and
fuel consumption since the same generator may have reported differently in its
respective granular table. This is done for both the net generation and fuel consumption
since the same generator may have reported differently in its respective granular table.

In more detail, within each reporting period, we split the plants into three groups:

* The **ALL** Coverage Records: where ALL generators report in the granular tables.
* The **NONE** Coverage Records: where NONE of the generators report in the granular
  tables.
* The **SOME** Coverage Records: where only SOME of the generators report in the granular
  tables.

In the **ALL** generators case, the data columns reported in the
:ref:`generation_fuel_eia923` table are allocated in proportion to data reported in the
granular data tables. We do this instead of directly using the data columns from the
granular tables because there are discrepancies between the generation_fuel_eia923 table
and the granular tables and we are assuming the totals reported in the
generation_fuel_eia923 table are authoritative.

In the **NONE** generators case, the data columns reported in the
:ref:`generation_fuel_eia923` table are allocated in proportion to the each generator's
capacity.

In the **SOME** generators case, we use a combination of the two allocation methods
described above. First, the data columns reported in the :ref:`generation_fuel_eia923`
table are allocated between the two categories of generators: those that report granular
data, and those that don't. The fraction allocated to each of those categories is based
on how much of the total is reported in the granular tables. If T is the total reported,
and X is the quantity reported in the granular tables, then the allocation is X/T to the
generators reporting granular data, and (T-X)/T to the generators not reporting granular
data. Within each of those categories the allocation then follows the ALL or NONE
allocation methods described above.

**Known Drawbacks of this methodology**:

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

from typing import Literal

# Useful high-level external modules.
import numpy as np
import pandas as pd

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)

IDX_GENS = ["report_date", "plant_id_eia", "generator_id"]
"""Primary key columns for generator records."""

IDX_GENS_PM_ESC = [
    "report_date",
    "plant_id_eia",
    "generator_id",
    "prime_mover_code",
    "energy_source_code",
]
"""Primary key columns for plant, generator, prime mover & energy source records."""

IDX_PM_ESC = ["report_date", "plant_id_eia", "energy_source_code", "prime_mover_code"]
"""Primary key columns for plant, prime mover & energy source records."""

IDX_B_PM_ESC = [
    "report_date",
    "plant_id_eia",
    "boiler_id",
    "energy_source_code",
    "prime_mover_code",
]
"""Primary key columns for plant, boiler, prime mover & energy source records."""

IDX_ESC = ["report_date", "plant_id_eia", "energy_source_code"]
"""Primary key columns for plant & energy source records."""

IDX_UNIT_ESC = ["report_date", "plant_id_eia", "energy_source_code", "unit_id_pudl"]
"""Primary key columns for plant, energy source & unit records."""

DATA_COLUMNS = [
    "net_generation_mwh",
    "fuel_consumed_mmbtu",
    "fuel_consumed_for_electricity_mmbtu",
]
"""Data columns from :ref:`generation_fuel_eia923` that are being allocated."""

MISSING_SENTINEL = 0.00001
"""A sentinel value for dealing with null or zero values.

#. Zero's in the relevant data columns get filled in with the sentinel value in
   :func:`associate_generator_tables`. At this stage all of the zeros from the original
   data that are now associated with generators, prime mover codes and energy source
   codes.
#. All of the nulls in the relevant data columns are filled with the sentinel value in
   :func:`prep_alloction_fraction`. (Could this also be done in
   :func:`associate_generator_tables`?)
#. After the allocation of net generation (within :func:`allocate_net_gen_by_gen_esc`
   and :func:`allocate_fuel_by_gen_esc` via :func:`remove_aggregated_sentinel_value`),
   convert all of the aggregated values that are between 0 and twenty times this
   sentinel value back to zero's. This is meant to find all instances of aggregated
   sentinel values. We avoid any negative values because there are instances of
   negative orignal values - especially negative net generation.
"""


# Two top-level functions (allocate & aggregate)
def allocate_gen_fuel_by_generator_energy_source(
    pudl_out, drop_interim_cols: bool = True
):
    """Allocate net gen from gen_fuel table to the generator/energy_source_code level.

    Three main steps here:
     * grab the three input tables from ``pudl_out`` with only the needed columns
     * associate ``generation_fuel_eia923`` table data w/ generators
     * allocate ``generation_fuel_eia923`` table data proportionally

     The association process happens via :func:`associate_generator_tables`.

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
        drop_interim_cols: True/False flag for dropping interim columns which are used
            to generate the ``net_generation_mwh`` column (they are mostly the
            ``frac`` column and  net generataion reported in the original
            ``generation_eia923`` and ``generation_fuel_eia923`` tables) that are
            useful for debugging. Default is False, which will drop the columns.
    """
    gf, bf, bga, gens, gen = extract_input_tables(pudl_out)
    bf, gens_at_freq, gen = standardize_input_frequency(
        bf, gens, gen, freq=pudl_out.freq
    )

    # add any startup energy source codes to the list of energy source codes
    # fix MSW codes
    gens_at_freq = adjust_energy_source_codes(gens_at_freq, gf, bf)
    # do the association!
    gen_assoc = associate_generator_tables(
        gens=gens_at_freq, gf=gf, gen=gen, bf=bf, bga=bga
    )

    # Generate a fraction to use to allocate net generation and fuel consumption by.
    # These two methods create a column called `frac`, which will be a fraction
    # to allocate net generation from the gf table for each `IDX_PM_ESC` group
    gen_pm_fuel = prep_alloction_fraction(gen_assoc)

    # Net gen allocation
    net_gen_alloc = allocate_net_gen_by_gen_esc(gen_pm_fuel).pipe(
        _test_gen_pm_fuel_output, gf=gf, gen=gen
    )
    test_gen_fuel_allocation(gen, net_gen_alloc)

    # fuel allocation
    fuel_alloc = allocate_fuel_by_gen_esc(gen_pm_fuel)

    # ensure that the allocated data has unique merge keys
    net_gen_alloc_agg = group_duplicate_keys(net_gen_alloc)
    fuel_alloc_agg = group_duplicate_keys(fuel_alloc)

    # squish net gen and fuel allocation together
    net_gen_fuel_alloc = pd.merge(
        net_gen_alloc_agg,
        fuel_alloc_agg,
        on=IDX_GENS_PM_ESC + ["energy_source_code_num"],
        how="outer",
        validate="1:1",
        suffixes=("_net_gen_alloc", "_fuel_alloc"),
    ).sort_values(IDX_GENS_PM_ESC)
    _ = test_original_gf_vs_the_allocated_by_gens_gf(
        gf=gf, gf_allocated=net_gen_fuel_alloc
    )
    if drop_interim_cols:
        net_gen_fuel_alloc = net_gen_fuel_alloc.loc[
            :,
            IDX_GENS_PM_ESC + ["energy_source_code_num"] + DATA_COLUMNS,
        ]
    return net_gen_fuel_alloc


def aggregate_gen_fuel_by_generator(
    pudl_out: "pudl.output.pudltabl.PudlTabl",
    net_gen_fuel_alloc: pd.DataFrame,
    sum_cols: list[str] = DATA_COLUMNS,
) -> pd.DataFrame:
    """Aggregate gen fuel data columns to generators.

    The generation_fuel_eia923 table includes net generation and fuel
    consumption data at the plant/energy source/prime mover level. The most
    granular level of plants that PUDL typically uses is at the plant/generator
    level. This function takes the plant/energy source code/prime mover level
    allocation, aggregates it to the generator level and then denormalizes it to
    make it more structurally in-line with the original generation_eia923 table
    (see :func:`pudl.output.eia923.denorm_generation_eia923`).

    Args:
        pudl_out: An object used to create the tables for EIA and FERC Form 1
            analysis.
        net_gen_fuel_alloc: table of allocated generation at the generator/prime mover/
            energy source. Result of :func:`allocate_gen_fuel_by_generator_energy_source`
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


def extract_input_tables(pudl_out: "pudl.output.pudltabl.PudlTabl"):
    """Extract the input tables from the pudl_out object.

    Extract all of the tables from pudl_out early in the process and select
    only the columns we need.

    Args:
        pudl_out: instantiated pudl output object.
    """
    gf = (
        pudl_out.gf_eia923()
        .loc[
            :,
            IDX_PM_ESC + DATA_COLUMNS,
        ]
        .pipe(manually_fix_energy_source_codes)
    )
    bf = pudl_out.bf_eia923().loc[:, IDX_B_PM_ESC + ["fuel_consumed_mmbtu"]]
    # load boiler generator associations
    bga = pudl_out.bga_eia860().loc[
        :,
        ["plant_id_eia", "boiler_id", "generator_id", "report_date"],
    ]
    gens = pudl_out.gens_eia860().loc[
        :,
        IDX_GENS
        + [
            "prime_mover_code",
            "unit_id_pudl",
            "capacity_mw",
            "fuel_type_count",
            "operational_status",
            "generator_retirement_date",
        ]
        + list(pudl_out.gens_eia860().filter(like="energy_source_code"))
        + list(pudl_out.gens_eia860().filter(like="startup_source_code")),
    ]
    warn_if_missing_pms(gens)

    gen = (
        pudl_out.gen_original_eia923().loc[:, IDX_GENS + ["net_generation_mwh"]]
        # removes 4 records with NaN generator_id as of pudl v0.5
        .dropna(subset=IDX_GENS)
    )
    granular_fuel_ratio = bf.fuel_consumed_mmbtu.sum() / gf.fuel_consumed_mmbtu.sum()
    granular_net_gen_ratio = gen.net_generation_mwh.sum() / gf.net_generation_mwh.sum()
    logger.info(
        f"The granular data tables contain {granular_fuel_ratio:.1%} of the fuel and "
        f"{granular_net_gen_ratio:.1%} of net generation in the higher-coverage "
        "generation_fuel_eia923 table."
    )
    return gf, bf, bga, gens, gen


def standardize_input_frequency(
    bf: pd.DataFrame, gens: pd.DataFrame, gen: pd.DataFrame, freq: Literal["MS", "MS"]
):
    """Standardize the frequency of the input tables.

    Employ :func:`distribute_annually_reported_data_to_months_if_annual` on the boiler
    fuel and generation table. Employ :func:`pudl.helpers.expand_timeseries` on the
    generators table. Also use the expanded generators table to ensure the generation
    table has all of the generators present.

    Args:
        bf: :ref:`boiler_fuel_eia923` table
        gens: :ref:`generators_eia860` table
        gen: :ref:`generation_eia923` table
        freq: the frequency code from the ``pudl_out`` object used to generate the above
            tables.
    """
    bf = distribute_annually_reported_data_to_months_if_annual(
        df=bf,
        key_columns=[
            "plant_id_eia",
            "boiler_id",
            "energy_source_code",
            "prime_mover_code",
            "report_date",
        ],
        data_column_name="fuel_consumed_mmbtu",
        freq=freq,
    )

    # duplicate each entry in the gens table 12 times to create an entry for each month of the year
    if freq == "MS":
        gens_at_freq = pudl.helpers.expand_timeseries(
            df=gens, key_cols=["plant_id_eia", "generator_id"], freq="MS"
        )
    else:
        gens_at_freq = gens

    gen = (
        distribute_annually_reported_data_to_months_if_annual(
            df=gen,
            key_columns=["plant_id_eia", "generator_id", "report_date"],
            data_column_name="net_generation_mwh",
            freq=freq,
        )
        # the gen table is missing many generator ids. Let's fill this using the gens table
        # leaving a missing value for net generation
        .merge(
            gens_at_freq[["plant_id_eia", "generator_id", "report_date"]],
            how="outer",
            on=["plant_id_eia", "generator_id", "report_date"],
        )
    )
    return bf, gens_at_freq, gen


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
        gen_pm_fuel: able of allocated generation at the generator/prime mover/energy
            source. Result of :func:`allocate_gen_fuel_by_generator_energy_source`
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
        scale_cols=DATA_COLUMNS + ["capacity_mw"],
        validate="m:m",  # m:m because there are multiple generators in gen_pm_fuel
    )
    return gen_pm_fuel_own


# Internal functions for allocate_gen_fuel_by_generator_energy_source


def agg_by_generator(
    net_gen_fuel_alloc: pd.DataFrame,
    by_cols: list[str] = IDX_GENS,
    sum_cols: list[str] = DATA_COLUMNS,
) -> pd.DataFrame:
    """Aggreate the allocated gen fuel data to the generator level.

    Args:
        net_gen_fuel_alloc: result of :func:`allocate_gen_fuel_by_generator_energy_source()`
        by_cols: list of columns to use as ``pandas.groupby`` arg ``by``
        sum_cols: Data columns from that are being aggregated via a
            ``pandas.groupby.sum()``
    """
    gen = (
        net_gen_fuel_alloc.groupby(by=by_cols)[sum_cols]
        .sum(min_count=1)
        .reset_index()
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return gen


def stack_generators(
    gens: pd.DataFrame,
    cat_col: str = "energy_source_code_num",
    stacked_col: str = "energy_source_code",
):
    """Stack the generator table with a set of columns.

    Args:
        gens: generators_eia860 table with cols: :py:const:`IDX_GENS` and all of the
            ``energy_source_code`` columns
        cat_col: name of category column which will end up having the column names of
            ``cols_to_stack``
        stacked_col: name of column which will end up with the stacked data from
            ``cols_to_stack``

    Returns:
        pandas.DataFrame: a dataframe with these columns: idx_stack, cat_col,
        stacked_col
    """
    # get a list of all energy_source_code, planned_energy_source_code, and startup_source_code columns
    esc = list(gens.filter(regex="source_code"))

    gens_stack_prep = (
        pd.DataFrame(gens.set_index(IDX_GENS)[esc].stack(level=0))
        .reset_index()
        .rename(columns={"level_3": cat_col, 0: stacked_col})
        .pipe(apply_pudl_dtypes, "eia")
    )
    # arrange energy source codes by number and type (start with energy_source_code, then planned_, then startup_)
    gens_stack_prep = gens_stack_prep.sort_values(
        by=(IDX_GENS + [cat_col]), ascending=True
    )
    # drop overlapping energy_source_code's from startup and planned codes
    gens_stack_prep = gens_stack_prep.drop_duplicates(
        subset=IDX_GENS + [stacked_col], keep="first"
    )
    # replace the energy_source_code_num. Existing energy source codes should be arranged in ascending order
    gens_stack_prep["energy_source_code_num"] = "energy_source_code_" + (
        gens_stack_prep.groupby(IDX_GENS).cumcount() + 1
    ).astype(str)

    # merge the stacked df back onto the gens table
    # we first drop the cols_to_stack so we don't duplicate data
    gens_stack = pd.merge(
        gens.drop(columns=esc), gens_stack_prep, on=IDX_GENS, how="outer"
    )
    return gens_stack


def associate_generator_tables(
    gens: pd.DataFrame,
    gf: pd.DataFrame,
    gen: pd.DataFrame,
    bf: pd.DataFrame,
    bga: pd.DataFrame,
) -> pd.DataFrame:
    """Associate the three tables needed to assign net gen and fuel to generators.

    The :ref:`generation_fuel_eia923` table's data is reported at the
    :py:const:`IDX_PM_ESC` granularity. Each generator in the :ref:`generators_eia860`
    has one ``prime_mover_code``, but potentially several ``energy_source_code``s that
    are reported in several columns. We need to reshape the generators table such that
    each generator has a separate record corresponding to each of its reported
    energy_source_codes, so it can be merged with the :ref:`generation_fuel_eia923`
    table. We do this using :func:``stack_generators`` employing
    :func:`pd.DataFrame.stack`.

    The stacked generators table has a primary key of
    ``["plant_id_eia", "generator_id", "report_date", "energy_source_code"]``. The table
    also includes the ``prime_mover_code`` column to enable merges with other tables,
    the ``capacity_mw`` column which we use to determine the allocation when there is no
    data in the granular data tables, and the ``operational_status`` column which we use
    to remove inactive plants from the association and allocation process.

    The remaining data tables are all less granular than this stacked generators table
    and have varying primary keys. We add suffixes to the data columns in these data
    tables to identify the source table before broadcast merging these data columns into
    the stacked generators. This broadcasted data will be used later in the allocation
    process.

    This function also removes inactive generators so that we don't associate any net
    generation or fuel to those generators. See :func:`remove_inactive_generators` for
    more details.

    There are some records in the data tables that have either ``prime_mover_code`` s  or
    ``energy_source_code`` s that do no appear in the :ref:`generators_eia860` table.
    We employ :func:`_allocate_unassociated_records` to make sure those records are
    associated.

    Args:
        gens: :ref:`generators_eia860` table with cols: :py:const:`IDX_GENS` and all of
            the ``energy_source_code`` columns and expanded to the frequency of
            ``pudl_out``
        gf: :ref:`generation_fuel_eia923` table with columns: :py:const:`IDX_PM_ESC` and
            ``net_generation_mwh`` and ``fuel_consumed_mmbtu``.
        gen: :ref:`generation_eia923` table with columns: :py:const:`IDX_GENS` and
            ``net_generation_mwh``.
        bf: :ref:`boiler_fuel_eia923` table with columns: :py:const:`IDX_B_PM_ESC` and
            fuel consumption columns.
        bga: :ref:`boiler_generator_assn_eia860` table.

    Returns:
        table of generators with stacked energy sources and broadcasted net generation
        and fuel data from the :ref:`generation_eia923` and :ref:`generation_fuel_eia923`
        tables. There are many duplicate values in this output which will later be used
        in the allocation process in :func:`allocate_net_gen_by_gen_esc` and
        :func:`allocate_fuel_by_gen_esc`.
    """
    stack_gens = stack_generators(
        gens, cat_col="energy_source_code_num", stacked_col="energy_source_code"
    )
    # allocate the boiler fuel data to generators
    bf_by_gens = allocate_bf_data_to_gens(bf, gens, bga)
    bf_by_gens = (
        bf_by_gens.set_index(IDX_GENS_PM_ESC).add_suffix("_bf_tbl").reset_index()
    )
    gf = gf.set_index(IDX_PM_ESC)[DATA_COLUMNS].add_suffix("_gf_tbl").reset_index()

    gen_assoc = (
        pd.merge(
            stack_gens,
            gen.rename(columns={"net_generation_mwh": "net_generation_mwh_g_tbl"}),
            on=IDX_GENS,
            how="outer",
        )
        .merge(
            gf,
            on=IDX_PM_ESC,
            how="outer",
            validate="m:1",
            indicator=True,  # used in _allocate_unassociated_records to find unassocited
        )
        .pipe(remove_inactive_generators)
        .pipe(
            _allocate_unassociated_records,
            idx_cols=IDX_PM_ESC,
            col_w_unexpected_codes="prime_mover_code",
            data_columns=[f"{col}_gf_tbl" for col in DATA_COLUMNS],
        )
        .drop(columns=["_merge"])  # drop do we can do this again in the bf_summed merge
        .merge(
            bf_by_gens, on=IDX_GENS_PM_ESC, how="outer", validate="m:1", indicator=True
        )
        .pipe(
            _allocate_unassociated_records,
            idx_cols=IDX_GENS_PM_ESC,
            col_w_unexpected_codes="energy_source_code",
            data_columns=["fuel_consumed_mmbtu_bf_tbl"],
        )
        .drop(columns=["_merge"])
    )

    # replace zeros with small number to avoid div by zero errors when calculating allocation fraction
    data_columns = [
        # "net_generation_mwh_g_tbl",
        "fuel_consumed_mmbtu_gf_tbl",
        "fuel_consumed_for_electricity_mmbtu_gf_tbl",
        "net_generation_mwh_gf_tbl",
        "fuel_consumed_mmbtu_bf_tbl",
    ]
    gen_assoc[data_columns] = gen_assoc[data_columns].replace(0, MISSING_SENTINEL)

    # calculate the total capacity in every fuel group
    gen_assoc = pd.merge(
        gen_assoc,
        gen_assoc.groupby(by=IDX_ESC)[["capacity_mw", "net_generation_mwh_g_tbl"]]
        .sum(min_count=1)
        .add_suffix("_fuel")
        .reset_index(),
        on=IDX_ESC,
        how="outer",
    ).pipe(apply_pudl_dtypes, "eia")
    return gen_assoc


def remove_inactive_generators(gen_assoc: pd.DataFrame) -> pd.DataFrame:
    """Remove the retired generators.

    We don't want to associate and later allocate net generation or fuel to generators
    that are retired (or proposed! or any other ``operational_status`` besides
    ``existing``). However, we do want to keep the generators that report operational
    statuses other than ``existing`` but which report non-zero data despite being
    ``retired`` or ``proposed``. This includes several categories of generators/plants:

        * ``retiring_generators``: generators that retire mid-year
        * ``retired_plants``: entire plants that supposedly retired prior to
          the current year but which report data. If a plant has a mix of gens
          which are existing and retired, they are not included in this category.
        * ``proposed_generators``: generators that become operational mid-year,
          or which are marked as ``proposed`` but start reporting non-zero data
        * ``proposed_plants``: entire plants that have a ``proposed`` status but
          which start reporting data. If a plant has a mix of gens which are
          existing and proposed, they are not included in this category.

    When we do not have generator-specific generation for a proposed/retired
    generator that is not coming online/retiring mid-year, we can also look
    at whether there is generation reported for this generator in the gf table.
    However, if a proposed/retired generator is part of an existing plant, it
    is possible that the reported generation from the gf table belongs to one
    of the other existing generators. Thus, we want to only keep proposed/retired
    generators where the entire plant is proposed/retired (in which case the gf-
    reported generation could only come from one of the new/retired generators).

    We also want to keep unassociated plants that have no ``generator_id`` which will
    be associated via :func:`_allocate_unassociated_records`.

    Args:
        gen_assoc: table of generators with stacked energy sources and broadcasted net
            generation data from the generation_eia923 and generation_fuel_eia923
            tables. Output of :func:`associate_generator_tables`.
    """
    existing = gen_assoc.loc[(gen_assoc.operational_status == "existing")]

    retiring_generators = identify_retiring_generators(gen_assoc)

    retired_plants = identify_retired_plants(gen_assoc)

    proposed_generators = identify_generators_coming_online(gen_assoc)

    proposed_plants = identify_proposed_plants(gen_assoc)

    unassociated_plants = gen_assoc[gen_assoc.generator_id.isnull()]

    gen_assoc_removed = pd.concat(
        [
            existing,
            retiring_generators,
            retired_plants,
            proposed_generators,
            proposed_plants,
            unassociated_plants,
        ]
    )

    return gen_assoc_removed


def identify_retiring_generators(gen_assoc):
    """Identify any generators that retire mid-year.

    These are generators with a retirement date after the earliest report_date or which
    report generator-specific generation data in the g table after their retirement
    date.
    """
    retiring_generators = gen_assoc.loc[
        (gen_assoc.operational_status == "retired")
        & (
            (gen_assoc.report_date <= gen_assoc.generator_retirement_date)
            | (gen_assoc.net_generation_mwh_g_tbl.notnull())
        )
    ]

    return retiring_generators


def identify_retired_plants(gen_assoc):
    """Identify entire plants that have previously retired but are reporting data."""
    # get a subset of the data that represents all plants that have completely retired before the start date
    # Get a list of all of the plants with at least one retired generator and reports non-zero generation data
    # after the generator retirement date
    retired_generators_with_reported_gf = list(
        gen_assoc.loc[
            (gen_assoc.operational_status == "retired")
            & (gen_assoc.report_date > gen_assoc.generator_retirement_date)
            & (gen_assoc.net_generation_mwh_gf_tbl.notnull())
            & (gen_assoc.net_generation_mwh_g_tbl.isnull())
            & (gen_assoc.net_generation_mwh_gf_tbl != 0),
            "plant_id_eia",
        ].unique()
    )

    # create a table for all of these plants that identifies all of the unique operational statuses
    plants_with_any_retired_generators = gen_assoc.loc[
        gen_assoc["plant_id_eia"].isin(retired_generators_with_reported_gf),
        ["plant_id_eia", "operational_status", "generator_retirement_date"],
    ].drop_duplicates()

    # remove plants that have operational statuses other than retired
    plants_with_both_retired_and_and_existing_generators = list(
        plants_with_any_retired_generators.loc[
            (plants_with_any_retired_generators["operational_status"] != "retired"),
            "plant_id_eia",
        ].unique()
    )
    plants_with_only_retired_generators = plants_with_any_retired_generators[
        ~plants_with_any_retired_generators["plant_id_eia"].isin(
            plants_with_both_retired_and_and_existing_generators
        )
    ]

    # only keep the plants where all retirement dates are before the current year
    plants_retiring_after_start_date = list(
        plants_with_only_retired_generators.loc[
            plants_with_only_retired_generators["generator_retirement_date"]
            >= min(gen_assoc.report_date),
            "plant_id_eia",
        ].unique()
    )
    entirely_retired_plants = list(
        plants_with_only_retired_generators.loc[
            ~plants_with_only_retired_generators["plant_id_eia"].isin(
                plants_retiring_after_start_date
            ),
            "plant_id_eia",
        ].unique()
    )

    retired_plants = gen_assoc[gen_assoc["plant_id_eia"].isin(entirely_retired_plants)]

    return retired_plants


def identify_generators_coming_online(gen_assoc):
    """Identify generators that are coming online mid-year.

    These are defined as generators that have a proposed status but which report
    generator-specific generation data in the g table
    """
    # sometimes a plant will report generation data before its proposed operating date
    # we want to keep any data that is reported for proposed generators
    proposed_generators = gen_assoc.loc[
        (gen_assoc.operational_status == "proposed")
        & (gen_assoc.net_generation_mwh_g_tbl.notnull())
    ]
    return proposed_generators


def identify_proposed_plants(gen_assoc):
    """Identify entirely new plants that are proposed but are already reporting data."""
    # Get a list of all of the plants that have a proposed generator with non-null and non-zero gf generation
    proposed_generators_with_reported_bf = list(
        gen_assoc.loc[
            (gen_assoc.operational_status == "proposed")
            & (gen_assoc.net_generation_mwh_gf_tbl.notnull())
            & (gen_assoc.net_generation_mwh_gf_tbl != 0),
            "plant_id_eia",
        ].unique()
    )

    # create a table for all of these plants that identifies all of the unique operational statuses
    plants_with_any_proposed_generators = gen_assoc.loc[
        gen_assoc["plant_id_eia"].isin(proposed_generators_with_reported_bf),
        ["plant_id_eia", "operational_status"],
    ].drop_duplicates()

    # filter this list to those plant ids where the only operational status is "proposed"
    # i.e. where the entire plant is new
    entirely_new_plants = list(
        plants_with_any_proposed_generators.loc[
            (
                ~plants_with_any_proposed_generators.duplicated(
                    subset="plant_id_eia", keep=False
                )
            )
            & (plants_with_any_proposed_generators["operational_status"] == "proposed"),
            "plant_id_eia",
        ].unique()
    )

    # keep data for these proposed plants in months where there is reported data
    proposed_plants = gen_assoc[
        gen_assoc["plant_id_eia"].isin(entirely_new_plants)
        & gen_assoc["net_generation_mwh_gf_tbl"].notnull()
    ]

    return proposed_plants


def _allocate_unassociated_records(
    gen_assoc: pd.DataFrame,
    idx_cols: list[str],
    col_w_unexpected_codes: Literal["energy_source_code", "prime_mover_code"],
    data_columns: list[str],
) -> pd.DataFrame:
    """Associate unassociated gen_fuel table records on idx_cols.

    There are a subset of ``generation_fuel_eia923`` or ``boiler_fuel_eia923`` records
    which do not merge onto the stacked generator table on ``IDX_PM_ESC`` or
    ``IDX_GENS_PM_ESC`` respecitively. These records generally don't match with
    the set of prime movers and energy sources in the stacked generator table. In this
    method, we associate those straggler, unassociated records by merging these records
    with the stacked generators witouth the un-matching data column.

    Args:
        gen_assoc: generators associated with data.
        idx_cols: ID columns (includes ``col_w_unexpected_codes``)
        col_w_unexpected_codes: name of the column which has codes in it that were not
            found in the generators table.
        data_columns: the data columns to associate and allocate.
    """
    # we're associating these unassociated records but we only want to associate
    # them w/ the primary energy source from stack_generators so we're going to assign
    # the energy_source_code_num as the primary source on the unassociated data and
    # merge on that column
    idx_minus_one = [col for col in idx_cols if col != col_w_unexpected_codes] + [
        "energy_source_code_num"
    ]
    # we're going to only associate these unassociated fuel records w/
    # the primary fuel so we don't have to deal w/ double counting
    # connected_mask = gen_assoc[unassociated_null_id_col].notnull()
    connected_mask = gen_assoc._merge != "right_only"
    eia_generators_connected = gen_assoc.loc[connected_mask].assign(
        capacity_mw_minus_one=lambda x: x.groupby(
            idx_minus_one + ["energy_source_code_num"]
        )["capacity_mw"].transform(sum),
        frac_cap_minus_one=lambda x: x.capacity_mw / x.capacity_mw_minus_one,
    )

    eia_generators_unassociated = (
        gen_assoc[~connected_mask]
        .assign(energy_source_code_num="energy_source_code_1")
        .groupby(by=idx_minus_one)
        .sum(min_count=1, numeric_only=True)
        .reset_index()
    )
    logger.info(
        f"Associating and allocating {len(eia_generators_unassociated)} "
        f"({len(eia_generators_unassociated)/len(gen_assoc):.1%}) records with "
        f"unexpected {col_w_unexpected_codes}."
    )

    def _allocate_unassociated_data_col(df: pd.DataFrame, col: str) -> pd.Series:
        """Helper function to allocate an unassociated data column.

        We want the main and the unassociated data to be added together but sometimes
        there is no data from the gf table and sometimes there is no unassociated data.
        The unassociated data column also needs to be allocated across the various
        gen/PM code combos in the plant that it is being merged with.
        """
        df.loc[df[col].notnull() | df[f"{col}_unassociated"].notnull(), col] = df[
            col
        ].fillna(0) + (
            df[f"{col}_unassociated"].fillna(0) * df.frac_cap_minus_one.fillna(0)
        )
        return df[col]

    eia_generators = pd.merge(
        eia_generators_connected,
        eia_generators_unassociated[idx_minus_one + data_columns],
        on=idx_minus_one,
        suffixes=("", "_unassociated"),
        how="left",
        validate="m:1",
    )
    eia_generators = eia_generators.assign(
        **{
            col: _allocate_unassociated_data_col(eia_generators, col=col)
            for col in data_columns
        }
    )  # .drop(
    #     columns=[f"{col}_unassociated" for col in data_columns]
    #     + [
    #         "capacity_mw_minus_one",
    #         "frac_cap_minus_one",
    #     ]
    # )
    return eia_generators


def prep_alloction_fraction(gen_assoc: pd.DataFrame) -> pd.DataFrame:
    """Prepare the associated generators for allocation.

    Make flags and aggregations to prepare for the :func:`allocate_net_gen_by_gen_esc`
    and :func:`allocate_fuel_by_gen_esc` functions.

    In :func:`allocate_net_gen_by_gen_esc`, we will break the generators out into four
    types - see :func:`allocate_net_gen_by_gen_esc` docs for details. This function adds
    flags for splitting the generators.

    Args:
        gen_assoc: a table of generators that have associated w/ energy sources, prime
            movers and boilers - result of :func:`associate_generator_tables`
    """
    # flag whether the generator exists in the generation table (this will be used
    # later on) for calculating ratios to use to allocate net generation
    # if there is more net gen reported to the gen table than the gf table, we assume
    # this is an "all gen" table (see allocate_net_gen_by_gen_esc). If this isn't done
    # the records that don't report in the gen table will end up getting negative net
    # generation
    gen_assoc = gen_assoc.assign(
        more_mwh_in_g_than_gf_tbl=lambda x: (
            x.net_generation_mwh_g_tbl >= x.net_generation_mwh_gf_tbl
        ),
        in_g_tbl=lambda x: np.where(
            x.net_generation_mwh_g_tbl.notnull(),  # | x.more_mwh_in_g_than_gf_tbl,
            True,
            False,
        ),
        in_bf_tbl=lambda x: np.where(
            x.fuel_consumed_mmbtu_bf_tbl.notnull(), True, False
        ),
    )

    gens_gb_pm_esc = gen_assoc.groupby(by=IDX_PM_ESC, dropna=False)
    gens_gb_u_esc = gen_assoc.groupby(by=IDX_UNIT_ESC, dropna=False)
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
            gens_gb_pm_esc[["in_g_tbl", "more_mwh_in_g_than_gf_tbl"]]
            .any()
            .reset_index(),
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
            on=IDX_UNIT_ESC,
        )
        .assign(
            # fill in the missing generation with small numbers (this will help ensure
            # the calculations to run the fractions in `allocate_net_gen_by_gen_esc`
            # and `allocate_fuel_by_gen_esc` can be consistent)
            # do the same with missing fuel consumption
            fuel_consumed_mmbtu_bf_tbl=lambda x: x.fuel_consumed_mmbtu_bf_tbl.fillna(
                MISSING_SENTINEL
            ),
            net_generation_mwh_g_tbl_pm_fuel=lambda x: x.net_generation_mwh_g_tbl_pm_fuel.fillna(
                MISSING_SENTINEL
            ),
            fuel_consumed_mmbtu_bf_tbl_pm_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl_pm_fuel.fillna(
                MISSING_SENTINEL
            ),
            fuel_consumed_mmbtu_bf_tbl_unit_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl_unit_fuel.fillna(
                MISSING_SENTINEL
            ),
        )
    )
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


def allocate_net_gen_by_gen_esc(gen_pm_fuel: pd.DataFrame) -> pd.DataFrame:
    """Allocate net generation to generators/energy_source_code via three methods.

    There are three main types of generators:
      * "all gen": generators of plants which fully report to the ``generation_eia923``
        table. This includes records that report more MWh to the ``generation_eia923``
        table than to the ``generation_fuel_eia923`` table (if we did not include these
        records, the ).
      * "some gen": generators of plants which partially report to the
        ``generation_eia923`` table.
      * "gf only": generators of plants which do not report at all to the
        ``generation_eia923`` table.

    Each different type of generator needs to be treated slightly differently,
    but all will end up with a ``frac`` column that can be used to allocate
    the ``net_generation_mwh_gf_tbl``.

    Args:
        gen_pm_fuel: output of :func:``prep_alloction_fraction()``.
    """
    # break out the table into these four different generator types and assign a category
    all_mask = gen_pm_fuel.in_g_tbl_all | gen_pm_fuel.more_mwh_in_g_than_gf_tbl_any
    all_gen = gen_pm_fuel.loc[all_mask].assign(net_gen_alloc_cat="all_gen")
    some_gen = gen_pm_fuel.loc[gen_pm_fuel.in_g_tbl_any & ~all_mask].assign(
        net_gen_alloc_cat="some_gen"
    )
    gf_only = gen_pm_fuel.loc[~gen_pm_fuel.in_g_tbl_any].assign(
        net_gen_alloc_cat="gf_only"
    )

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

    # replace the placeholder missing values with zero before allocating
    # since some of these may have been aggregated, well, flag any values less than 10
    # times the sentinel but more than 0 bc there are negative net generation values
    net_gen_alloc["net_generation_mwh_gf_tbl"] = remove_aggregated_sentinel_value(
        net_gen_alloc["net_generation_mwh_gf_tbl"]
    )
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


def allocate_fuel_by_gen_esc(gen_pm_fuel: pd.DataFrame) -> pd.DataFrame:
    """Allocate fuel_consumption to generators/energy_source_code via three methods.

    There are three main types of generators:

      * "all bf": generators of plants which fully report to the
        boiler_fuel_eia923 table.
      * "some bf": generators of plants which partially report to the
        boiler_fuel_eia923 table.
      * "gf only": generators of plants which do not report at all to the
        boiler_fuel_eia923 table.

    Each different type of generator needs to be treated slightly differently,
    but all will end up with a ``frac`` column that can be used to allocate
    the ``fuel_consumed_mmbtu_gf_tbl``.

    Args:
        gen_pm_fuel: output of :func:`prep_alloction_fraction`.
    """
    # break out the table into these four different generator types.
    all_bf = gen_pm_fuel.loc[gen_pm_fuel.in_bf_tbl_all]
    some_bf = gen_pm_fuel.loc[gen_pm_fuel.in_bf_tbl_any & ~gen_pm_fuel.in_bf_tbl_all]
    gf_only = gen_pm_fuel.loc[~gen_pm_fuel.in_bf_tbl_any]

    logger.info(
        "Ratio calc types: \n"
        f"   All gens w/in boiler fuel table:  {len(all_bf)}#, {all_bf.capacity_mw.sum():.2} MW\n"
        f"   Some gens w/in boiler fuel table: {len(some_bf)}#, {some_bf.capacity_mw.sum():.2} MW\n"
        f"   No gens w/in boiler fuel table:   {len(gf_only)}#, {gf_only.capacity_mw.sum():.2} MW"
    )
    if len(gen_pm_fuel) != len(all_bf) + len(some_bf) + len(gf_only):
        raise AssertionError(
            "Error in splitting the gens between records showing up fully, "
            "partially, or not at all in the boiler fuel table."
        )

    # In the case where we have all of the fuel from the bf
    # table, we still allocate, because the fuel reported in these two
    # tables don't always match perfectly
    all_bf = all_bf.assign(
        frac_fuel=lambda x: x.fuel_consumed_mmbtu_bf_tbl
        / x.fuel_consumed_mmbtu_bf_tbl_pm_fuel,
        frac=lambda x: x.frac_fuel,
    )
    # _ = _test_frac(all_bf)

    some_bf = some_bf.assign(
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
    # _ = _test_frac(some_bf)

    # Calculate what fraction of the total capacity is associated with each of
    # the generators in the grouping.
    gf_only = gf_only.assign(
        frac_cap=lambda x: x.capacity_mw / x.capacity_mw_unit_fuel,
        frac=lambda x: x.frac_cap,
    )
    # _ = _test_frac(gf_only)

    # squish all of these methods back together.
    fuel_alloc = pd.concat([all_bf, some_bf, gf_only])
    # _ = _test_frac(fuel_alloc)

    # replace the placeholder missing values with zero before allocating
    # since some of these may have been aggregated, well, flag any values less than the
    # sentinel missing value
    fuel_alloc = fuel_alloc.assign(
        fuel_consumed_mmbtu_gf_tbl=remove_aggregated_sentinel_value(
            fuel_alloc["fuel_consumed_mmbtu_gf_tbl"]
        ),
        fuel_consumed_for_electricity_mmbtu_gf_tbl=remove_aggregated_sentinel_value(
            fuel_alloc["fuel_consumed_for_electricity_mmbtu_gf_tbl"]
        ),
    )
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


def remove_aggregated_sentinel_value(col: pd.Series, scalar: float = 20.0) -> pd.Series:
    """Replace the post-aggregation sentinel values in a column with zero."""
    return np.where(col.between(0, scalar * MISSING_SENTINEL), 0, col)


def group_duplicate_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Catches duplicate keys in the allocated data and groups them together.

    Merging ``net_gen_alloc`` and ``fuel_alloc`` together requires unique keys in each
    df. Sometimes the allocation process creates duplicate keys. This function
    identifies when this happens, and aggregates the data on these keys to remove the
    duplicates.
    """
    # identify any duplicate records
    duplicate_keys = df[
        df.duplicated(subset=(IDX_GENS_PM_ESC + ["energy_source_code_num"]))
    ]
    # if there are duplicate records, print a warning and fix the issue
    if len(duplicate_keys) > 0:
        logger.warning(
            "Duplicate keys exist in the allocated data."
            "These will be grouped together, but check the source "
            "of this issue."
        )
        logger.warning(duplicate_keys[IDX_GENS_PM_ESC + ["energy_source_code_num"]])

        df = (
            df.groupby(IDX_GENS_PM_ESC + ["energy_source_code_num"])
            .sum(min_count=1)
            .reset_index()
        )
    return df


###########################
# Fuel Allocation Functions
###########################
def distribute_annually_reported_data_to_months_if_annual(
    df: pd.DataFrame,
    key_columns: list[str],
    data_column_name: str,
    freq: Literal["AS", "MS"],
) -> pd.DataFrame:
    """Allocates annually-reported data from the gen or bf table to each month.

    Certain plants only report data to the generator table and boiler fuel table
    on an annual basis. In these cases, their annual total is reported as a single
    value in January or December, and the other 11 months are reported as missing
    values. This function first identifies which plants are annual respondents by
    identifying plants that have 11 months of missing data, with the one month of
    existing data being in January or December. This is an assumption based on seeing
    that over 40% of the plants that have 11 months of missing data report their one
    month of data in January and December (this ratio of reporting is checked and will
    raise a warning if it becomes untrue). It then distributes this annually-reported
    value evenly across all months in the year. Because we know some of the plants are
    reporting in only one month that is not January or December, the assumption about
    January and December only reporting is almost certainly resulting in some non-annual
    data being allocated across all months, but on average the data will be more
    accruate.

    Note: We should be able to use the ``reporting_frequency_code`` column for the
    identification of annually reported data. This currently does not work because we
    assumed this was a plant-level annual attribute (and is thus stored in the
    ``plants_eia860`` table). See Issue #1933.

    Args:
        df: a pandas dataframe, either loaded from pudl_out.gen_original_eia923() or
            pudl_out.bf_eia923()
        key_columns: a list of the primary key column names, either
            ``["plant_id_eia","boiler_id","energy_source_code"]`` or
            ``["plant_id_eia","generator_id"]``
        data_column_name: the name of the data column to allocate, either
            "net_generation_mwh" or "fuel_consumed_mmbtu" depending on the df specified
        freq: frequency of input df. Must be either ``AS`` or ``MS``.

    Returns:
        df with the annually reported values allocated to each month
    """
    if freq == "MS":

        def assign_plant_year(df):
            return df.assign(
                plant_year=lambda x: x.report_date.dt.year.astype(str)
                + "_"
                + x.plant_id_eia.astype(str)
            )

        reporters = df.copy().pipe(assign_plant_year)
        # get a count of the number of missing values in a year
        key_columns_annual = ["plant_year"] + [
            col for col in key_columns if col != "report_date"
        ]
        reporters["missing_data"] = (
            reporters.assign(
                missing_data=lambda x: x[data_column_name].isnull()
                | np.isclose(reporters[data_column_name], 0)
            )
            .groupby(key_columns_annual, dropna=False)[["missing_data"]]
            .transform(sum)
        )

        # seperate annual and monthly reporters
        once_a_year_reporters = reporters[
            (
                reporters[data_column_name].notnull()
                & ~np.isclose(reporters[data_column_name], 0)
            )
            & (reporters.missing_data == 11)
        ]
        annual_reporters = once_a_year_reporters[
            once_a_year_reporters.report_date.dt.month.isin([1, 12])
        ].set_index(["plant_year"])

        # check if the plurality of the once_a_year_reporters are in Jan or Dec
        perc_of_annual = len(annual_reporters) / len(once_a_year_reporters)
        if perc_of_annual < 0.40:
            logger.warning(
                f"Less than 40% ({perc_of_annual:.0%}) of the once-a-year reporters "
                "are in January or December. Examine assumption about annual reporters."
            )

        reporters = reporters.set_index(["plant_year"])
        monthly_reporters = reporters.loc[
            reporters.index.difference(annual_reporters.index)
        ]

        logger.info(
            f"Distributing {len(annual_reporters)/len(reporters):.1%} annually reported"
            " records to months."
        )
        # first convert the december month to january bc expand_timeseries expands from
        # the start date and we want january on.
        annual_reporters_expanded = (
            annual_reporters.assign(
                report_date=lambda x: pd.to_datetime(
                    {
                        "year": x.report_date.dt.year,
                        "month": 1,
                        "day": 1,
                    }
                )
            )
            .pipe(
                pudl.helpers.expand_timeseries,
                key_cols=[col for col in key_columns if col != "report_date"],
                date_col="report_date",
                fill_through_freq="year",
            )
            .assign(**{data_column_name: lambda x: x[data_column_name] / 12})
            .pipe(assign_plant_year)
            .set_index(["plant_year"])
        )
        # sometimes a plant oscillates btwn annual and monthly reporting. when it does
        # expand_timeseries will generate monthly records for years that were not
        # included annual_reporters bc expand_timeseries expands from the most recent
        # to the last date... so we remove any plant/year combo that didn't show up
        # before the expansion
        annual_reporters_expanded = annual_reporters_expanded.loc[
            annual_reporters_expanded.index.intersection(annual_reporters.index)
        ]

        df_out = (
            pd.concat([monthly_reporters, annual_reporters_expanded])
            .reset_index(drop=True)
            .drop(columns=["missing_data"])
        )
    elif freq == "AS":
        df_out = df
    else:
        raise AssertionError(f"Frequency must be either `AS` or `MS`. Got {freq}")
    return df_out


def manually_fix_energy_source_codes(gf: pd.DataFrame) -> pd.DataFrame:
    """Patch: reassigns fuel codes in the gf table that don't match the fuel code in the gens table."""
    # plant 10204 should be waste heat instead of other
    gf.loc[
        (gf["plant_id_eia"] == 10204) & (gf["energy_source_code"] == "OTH"),
        "energy_source_code",
    ] = "WH"

    return gf


def adjust_energy_source_codes(
    gens: pd.DataFrame, gf: pd.DataFrame, bf_by_gens: pd.DataFrame
) -> pd.DataFrame:
    """Adjusts MSW codes.

    Adjust the MSW codes in gens to match those used in gf and bf.

    In recent years, EIA-923 started splitting out the ``MSW`` (municipal_solid_waste)
    into its consitituent components ``MSB`` (municipal_solid_waste_biogenic) and
    ``MSN`` (municipal_solid_nonbiogenic). However, the EIA-860 Generators table still
    only uses the ``MSW`` code.

    This function identifies which MSW codes are used in the gf and bf tables and
    creates records to match these.
    """
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
            bf_by_gens.loc[
                bf_by_gens["energy_source_code"].isin(["MSW", "MSB", "MSN"]),
                "energy_source_code",
            ].unique()
        )
    )
    msw_codes_used = list(msw_codes_in_gf | msw_codes_in_bf)
    # join these codes into a string that will be used to replace the MSW code
    replacement_codes = ",".join(msw_codes_used)

    # if MSN and MSB codes are used, replace the existing MSW value
    if replacement_codes != "MSW":
        # for each type of energy source column, we want to expand any "MSW" values
        for esc_type in ["energy_", "planned_energy_", "startup_"]:
            # create a column of all unique fuels in the order in which they appear (ESC 1-6, startup fuel 1-6)
            # this column will have each fuel code separated by a comma
            gens["unique_esc"] = [
                ",".join(
                    fuel for fuel in list(dict.fromkeys(fuels)) if pd.notnull(fuel)
                )
                for fuels in gens.loc[
                    :,
                    [
                        col
                        for col in gens.columns
                        if col.startswith(f"{esc_type}source_code")
                    ],
                ].values
            ]

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
                esc_columns_to_add.append(f"{esc_type}source_code_{n}")

            # drop all of the existing energy source code columns
            gens = gens.drop(
                columns=[
                    col
                    for col in gens.columns
                    if col.startswith(f"{esc_type}source_code")
                ]
            )

            # create new numbered energy source code columns by expanding the list of unique fuels
            gens[esc_columns_to_add] = gens["unique_esc"].str.split(",", expand=True)

            # creating columns from this list sometimes replaces NaN values with "" or None
            gens[esc_columns_to_add] = gens[esc_columns_to_add].replace("", np.NaN)
            gens[esc_columns_to_add] = gens[esc_columns_to_add].fillna(value=np.NaN)

            # drop the intermediate column
            gens = gens.drop(columns=["unique_esc"])

    return gens


def allocate_bf_data_to_gens(
    bf: pd.DataFrame, gens: pd.DataFrame, bga: pd.DataFrame
) -> pd.DataFrame:
    """Allocates boiler fuel data to the generator level.

    Distributes boiler-level data from boiler_fuel_eia923 to the generator level based
    on the boiler-generator association table and the nameplate capacity of the
    connected generators.

    Because fuel consumption in the boiler_fuel_eia923 table is reported per boiler_id,
    we must first map this data to generators using the boiler_generator_assn_eia860
    table. For boilers that have a 1:m or m:m relationship with generators, we allocate
    the reported fuel to each associated generator based on the nameplate capacity of
    each generator. So if boiler "1" was associated with generator A (25 MW) and
    generator B (75 MW), 25% of the fuel consumption would be allocated to generator A
    and 75% would be allocated to generator B.
    """
    # merge generator capacity information into the BGA
    bga_w_gen = bga.merge(
        gens[
            [
                "plant_id_eia",
                "generator_id",
                "capacity_mw",
                "report_date",
                "prime_mover_code",
            ]
        ],
        how="left",
        on=["plant_id_eia", "generator_id", "report_date"],
        validate="m:1",
    )
    # calculate an allocation fraction based on the capacity of each generator with which
    # a boiler is connected
    bga_w_gen["cap_frac"] = bga_w_gen[["capacity_mw"]] / bga_w_gen.groupby(
        ["plant_id_eia", "boiler_id", "report_date"]
    )[["capacity_mw"]].transform("sum")

    # drop records from bf where there is missing fuel data
    bf = bf.dropna(subset="fuel_consumed_mmbtu")
    # merge in the generator id and the capacity fraction
    bf_assoc_gen = pudl.helpers.date_merge(
        left=bf,
        right=bga_w_gen,
        left_date_col="report_date",
        right_date_col="report_date",
        new_date_col="report_date",
        on=["plant_id_eia", "boiler_id", "prime_mover_code"],
        date_on=["year"],
        how="left",
        report_at_start=True,
    )
    # distribute the boiler-level data to each generator based on the capacity fraciton
    bf_assoc_gen["fuel_consumed_mmbtu"] = (
        bf_assoc_gen["fuel_consumed_mmbtu"] * bf_assoc_gen["cap_frac"]
    )

    # group the data by generator-PM-fuel, dropping records where there is no boiler-generator association
    bf_by_gen = (
        bf_assoc_gen.groupby(
            [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "energy_source_code",
                "prime_mover_code",
            ]
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    # remove intermediate columns
    bf_by_gen = bf_by_gen.drop(columns=["capacity_mw", "cap_frac"])

    return bf_by_gen


##################
# Tests of Outputs
##################
def warn_if_missing_pms(gens):
    """Log warning if there are too many null ``prime_mover_code`` s.

    Warn if prime mover codes in gens do not match the codes in the gf table this is
    something that should probably be fixed in the input data see
    https://github.com/catalyst-cooperative/pudl/issues/1585 set a threshold and ignore
    2001 bc most errors are 2001 errors.
    """
    missing_pm = gens[
        gens["prime_mover_code"].isna() & (gens.report_date.dt.year != 2001)
    ]
    if len(missing_pm) > 35:
        logger.warning(
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


def _test_frac(gen_pm_fuel):
    """Check if each of the IDX_PM_ESC groups frac's add up to 1."""
    frac_test = (
        gen_pm_fuel.groupby(IDX_PM_ESC)[["frac", "net_generation_mwh_g_tbl"]]
        .sum(min_count=1)
        .reset_index()
    )

    frac_test_bad = frac_test[~np.isclose(frac_test.frac, 1) & frac_test.frac.notnull()]
    if not frac_test_bad.empty:
        # raise AssertionError(
        logger.warning(
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
        logger.warning(f"{len(no_cap_gen)} records have no capacity or net gen")
    # remove the junk/corrective plants
    fuel_net_gen = gf[gf.plant_id_eia != "99999"].net_generation_mwh.sum()
    logger.info(
        "gen v fuel table net gen diff:      "
        f"{(gen.net_generation_mwh.sum())/fuel_net_gen:.1%}"
    )
    logger.info(
        "new v fuel table net gen diff:      "
        f"{(gen_pm_fuel_test.net_generation_mwh.sum())/fuel_net_gen:.1%}"
    )

    gen_pm_fuel_test = gen_pm_fuel_test.drop(
        columns=["net_generation_mwh_test", "net_generation_mwh_diff"]
    )
    return gen_pm_fuel_test


def test_gen_fuel_allocation(gen, net_gen_alloc, ratio=0.05):
    """Does the allocated MWh differ from the granular :ref:`generation_eia923`?

    Args:
        gen: the ``generation_eia923`` table.
        net_gen_alloc: the allocated net generation at the :py:const:`IDX_PM_ESC` level
        ratio: the tolerance
    """
    net_gen_alloc[net_gen_alloc.more_mwh_in_g_than_gf_tbl]
    gens_test = pd.merge(
        agg_by_generator(net_gen_alloc, sum_cols=["net_generation_mwh"]),
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
        logger.warning(
            f"Many generator records that have allocated net gen more than {ratio:.0%}"
        )


def test_original_gf_vs_the_allocated_by_gens_gf(
    gf: pd.DataFrame,
    gf_allocated: pd.DataFrame,
    data_columns: list[str] = DATA_COLUMNS,
    by: list[str] = ["year", "plant_id_eia"],
    acceptance_threshold: float = 0.07,
) -> pd.DataFrame:
    """Test whether the allocated data and original data sum up to similar values.

    Raises:
        AssertionError: If the number of plant/years that are off by more than 5% is
            not within acceptable level of tolerance.
        AssertionError: If the difference between the allocated and original data for
            any plant/year is off by more than x10 or x-5.
    """
    gf_test = pd.merge(
        gf.assign(year=lambda x: x.report_date.dt.year).groupby(by)[data_columns].sum(),
        gf_allocated.assign(year=lambda x: x.report_date.dt.year)
        .groupby(by)[data_columns]
        .sum(),
        right_index=True,
        left_index=True,
        suffixes=("_og", "_allocated"),
        how="outer",
    )
    # calculate the difference between the allocated and the original data
    gf_test = gf_test.assign(
        **{
            f"{col}_diff": gf_test[f"{col}_allocated"] / gf_test[f"{col}_og"]
            for col in data_columns
        }
    )
    # remove the inf diffs for net gen if the allocated value if small. many seem to be
    # a result of the MISSING_SENTINEL filling in.
    if gf_test[
        np.isinf(gf_test.net_generation_mwh_diff)
        & ~(gf_test.net_generation_mwh_allocated < 1)
    ].empty:
        gf_test = gf_test[~np.isinf(gf_test.net_generation_mwh_diff)]

    for data_col in data_columns:
        col_test = gf_test[
            (
                (gf_test[f"{data_col}_diff"] > 1.05)
                | (gf_test[f"{data_col}_diff"] < 0.95)
            )
            & (gf_test[f"{data_col}_diff"].notnull())
        ]
        off_by_5_perc = len(col_test) / len(gf_test)
        logger.info(
            f"{data_col}: {off_by_5_perc:.1%} of allocated plant/year's are off by more"
            " than 5%"
        )
        if off_by_5_perc > acceptance_threshold:
            raise AssertionError(
                f"{len(col_test)} of {len(gf_test)} plants' ({off_by_5_perc:.1%}) allocated {data_col} are off"
                " the original data by more than 5%. Expected < {acceptance_threshold:.1%}."
            )
        max_diff = round(gf_test[f"{data_col}_diff"].max(), 2)
        min_diff = round(gf_test[f"{data_col}_diff"].min(), 2)
        logger.info(
            f"{data_col}: Min and max differnce are x{min_diff} and x{max_diff}"
        )
        if max_diff > 10 or min_diff < -5:
            raise AssertionError(
                f"ahhhHHhh. {data_col} has some plant-year aggregations that that "
                "allocated data that is off from the original generation_fuel_eia923 "
                "data by more than an accepted range of tolerance. \n"
                f"  Min difference: {min_diff}\n"
                f"  Max difference: {max_diff}"
            )
    return gf_test
