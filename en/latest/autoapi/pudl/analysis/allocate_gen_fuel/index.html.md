# pudl.analysis.allocate_gen_fuel

Allocate data from [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table to generator level.

The algorithm we’re using assumes the following about the reported data:

* The [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table is the authoritative source of
  information about how much generation and fuel consumption is attributable to an
  entire plant. This table has the most complete data coverage, but it is not the most
  granular data reported. It’s primary keys are [`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC).
* The [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) table contains the most granular net
  generation data. It is reported at the generator level with primary keys
  [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS). This table includes only ~39% of the total MWhs reported in the
  [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table.
* The [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) table contains the most granular fuel
  consumption data.  It is reported at the boiler/prime mover/energy source level with
  primary keys [`IDX_B_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_B_PM_ESC). This table includes only ~38% of the total
  MMBTUs reported in the [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table.
* The [core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) table provides an exhaustive list of all
  generators whose generation is being reported in the
  [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table - with primary keys
  [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS).

This module allocates the total net electricity generation and fuel consumption reported
in the [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table to individual generators, based
on more granular data reported in the [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) and
[core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) tables, as well as capacity (MW) found in the
[core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) table. It uses other generator attributes from the
[core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) table to associate the data found in the
[core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) with generators. It also uses as the
associations between boilers and generators found in the
[core_eia860_\_assn_boiler_generator](../../../../data_dictionaries/pudl_db.html.md#core-eia860-assn-boiler-generator) table to aggregate data
[core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) tables. The main coordinating functions hereare
[`allocate_gen_fuel_by_generator_energy_source()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source) and
`aggregate_gen_fuel_by_generator()`.

Some definitions:

* **Data columns** refers to the net generation and fuel consumption - the specific
  columns are defined in [`DATA_COLUMNS`](#pudl.analysis.allocate_gen_fuel.DATA_COLUMNS).
* **Granular tables** refers to [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) and
  [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel), which report granular data but do not have
  complete coverage.

There are six main stages of the allocation process in this module:

1. **Read inputs**: Read denormalized net generation and fuel consumption data from the
   PUDL DB and standardize data reporting frequency. (See [`select_input_data()`](#pudl.analysis.allocate_gen_fuel.select_input_data)
   and [`standardize_input_frequency()`](#pudl.analysis.allocate_gen_fuel.standardize_input_frequency)).
2. **Associate inputs**: Merge data columns from the input tables described above on the
   basis of their shared primary key columns, producing an output with primary key
   [`IDX_GENS_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_GENS_PM_ESC). This broadcasts many data values across multiple rows
   for use in the allocation process below (see [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables)).
3. **Flag associated inputs**: For each record in the associated inputs, add boolean
   flags that separately indicate whether the generation and fuel consumption in that
   record are directly reported in the granular tables. This lets us choose an
   appropriate data allocation method based on how complete the granular data coverage
   is for a given value of [`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC), which is the original primary key of
   the [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table. (See
   [`prep_allocation_fraction()`](#pudl.analysis.allocate_gen_fuel.prep_allocation_fraction)).
4. **Allocate**: Allocate the net generation and fuel consumption reported in the less
   granular [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table to the
   [`IDX_GENS_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_GENS_PM_ESC) level. More details on the allocation process are below
   (see [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc) and [`allocate_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_fuel_by_gen_esc)).
5. **Sanity check allocation**: Warn if assumptions about the data and the outputs
   aren’t met (see [`_warn_if_missing_pms()`](#pudl.analysis.allocate_gen_fuel._warn_if_missing_pms), [`_test_frac()`](#pudl.analysis.allocate_gen_fuel._test_frac) and
   [`test_gen_fuel_allocation()`](#pudl.analysis.allocate_gen_fuel.test_gen_fuel_allocation)). Verifying that the total allocated net generation
   and fuel consumption within each plant equals the originally reported values within
   tolerance is handled by the `validate_eia923__generation_fuel_allocation` dbt
   models.
6. **Aggregate outputs**: Aggregate the allocated net generation and fuel consumption to
   the generator level, going from having primary keys of [`IDX_GENS_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_GENS_PM_ESC) to
   [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS) (see `aggregate_gen_fuel_by_generator()`).

**High-level description about the allocation step**:

We allocate the data columns reported in the [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel)
table on the basis of plant, prime mover, and energy source among the generators in each
plant that have matching energy sources.

We group the associated data columns by [`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC) and categorize
each resulting group of generators based on whether  **ALL**, **SOME**, or **NONE** of
them reported data in the granular tables. This is done for both the net generation and
fuel consumption since the same generator may have reported differently in its
respective granular table. This is done for both the net generation and fuel consumption
since the same generator may have reported differently in its respective granular table.

In more detail, within each reporting period, we split the plants into three groups:

* The **ALL** Coverage Records: where ALL generators report in the granular tables.
* The **NONE** Coverage Records: where NONE of the generators report in the granular
  tables.
* The **SOME** Coverage Records: where only SOME of the generators report in the
  granular tables.

In the **ALL** generators case, the data columns reported in the
[core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table are allocated in proportion to data
reported in the granular data tables. We do this instead of directly using the data
columns from the granular tables because there are discrepancies between the
core_eia923_\_monthly_generation_fuel table and the granular tables and we are assuming
the totals reported in the core_eia923_\_monthly_generation_fuel table are authoritative.

In the **NONE** generators case, the data columns reported in the
[core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table are allocated in proportion to the
each generator’s capacity.

In the **SOME** generators case, we use a combination of the two allocation methods
described above. First, the data columns reported in the
[core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table are allocated between the two
categories of generators: those that report granular data, and those that don’t. The
fraction allocated to each of those categories is based on how much of the total is
reported in the granular tables. If T is the total reported, and X is the quantity
reported in the granular tables, then the allocation is X/T to the generators reporting
granular data, and (T-X)/T to the generators not reporting granular data. Within each of
those categories the allocation then follows the ALL or NONE allocation methods
described above.

**Known Drawbacks of this methodology**:

Note that this methodology does not distinguish between primary and secondary
energy_sources for generators. It associates portions of net generation to each
generators in the same plant do not report detailed generation, have the same
prime_mover_code, and use the same fuels, but have very different capacity factors in
reality, this methodology will allocate generation such that they end up with very
similar capacity factors. We imagine this is an uncommon scenario.

This methodology has several potential flaws and drawbacks. Because there is no
indicator of what portion of the energy_source_codes, we associate the net generation
equally among them. In effect, if a plant had multiple generators with the same
prime_mover_code but opposite primary and secondary fuels (eg. gen 1 has a primary fuel
of ‘NG’ and secondary fuel of ‘DFO’, while gen 2 has a primary fuel of ‘DFO’ and a
secondary fuel of ‘NG’), the methodology associates the
core_eia923_\_monthly_generation_fuel records similarly across these two generators.
However, the allocated net generation will still be porporational to each generator’s
net generation (if it’s reported) or capacity (if generation is not reported).

## Attributes

| [`logger`](#pudl.analysis.allocate_gen_fuel.logger)                   |                                                                                                                                                                      |
|---------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`AllocationFrequency`](#pudl.analysis.allocate_gen_fuel.AllocationFrequency)      | The two frequencies at which generation & fuel data can be allocated.                                                                                                |
| [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS)                 | Primary key columns for generator records.                                                                                                                           |
| [`IDX_GENS_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_GENS_PM_ESC)          | Primary key columns for plant, generator, prime mover & energy source records.                                                                                       |
| [`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC)               | Primary key columns for plant, prime mover & energy source records.                                                                                                  |
| [`IDX_B_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_B_PM_ESC)             | Primary key columns for plant, boiler, prime mover & energy source records.                                                                                          |
| [`IDX_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_ESC)                  | Primary key columns for plant & energy source records.                                                                                                               |
| [`IDX_UNIT_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_UNIT_ESC)             | Primary key columns for plant, energy source & unit records.                                                                                                         |
| [`DATA_COLUMNS`](#pudl.analysis.allocate_gen_fuel.DATA_COLUMNS)             | Data columns from [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) that are being allocated. |
| [`MISSING_SENTINEL`](#pudl.analysis.allocate_gen_fuel.MISSING_SENTINEL)         | A sentinel value for dealing with null or zero values.                                                                                                               |
| [`ALLOCATION_FREQUENCIES`](#pudl.analysis.allocate_gen_fuel.ALLOCATION_FREQUENCIES)   |                                                                                                                                                                      |
| [`allocate_gen_fuel_assets`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_assets) |                                                                                                                                                                      |
| [`_TRANSITION_DATE_COL`](#pudl.analysis.allocate_gen_fuel._TRANSITION_DATE_COL)     | The column recording a generator's actual transition date, keyed by `operational_status`.                                                                            |

## Functions

| [`allocate_gen_fuel_asset_factory`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_asset_factory)(...)                       | Build yearly and monthly net generation & fuel consumption allocation assets.                                                                                      |
|-------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`allocate_gen_fuel_by_generator_energy_source`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source)(...)          | Allocate net gen from gen_fuel table to the generator/energy_source_code level.                                                                                    |
| [`select_input_data`](#pudl.analysis.allocate_gen_fuel.select_input_data)(→ tuple[pandas.DataFrame, ...)           | Select only the subset of input data needed for the allocation.                                                                                                    |
| [`standardize_input_frequency`](#pudl.analysis.allocate_gen_fuel.standardize_input_frequency)(→ tuple[pandas.DataFrame, ...) | Standardize the frequency of the input tables.                                                                                                                     |
| [`scale_allocated_net_gen_fuel_by_ownership`](#pudl.analysis.allocate_gen_fuel.scale_allocated_net_gen_fuel_by_ownership)(...)             | Scale allocated net gen at the generator/energy_source_code level by ownership.                                                                                    |
| [`agg_by_generator`](#pudl.analysis.allocate_gen_fuel.agg_by_generator)(→ pandas.DataFrame)                       | Aggregate the allocated gen fuel data to the generator level.                                                                                                      |
| [`stack_generators`](#pudl.analysis.allocate_gen_fuel.stack_generators)(→ pandas.DataFrame)                       | Stack the generator table with a set of columns.                                                                                                                   |
| [`associate_generator_tables`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables)(→ pandas.DataFrame)             | Associate the three tables needed to assign net gen and fuel to generators.                                                                                        |
| [`_label_gf_unique_to_gen`](#pudl.analysis.allocate_gen_fuel._label_gf_unique_to_gen)(→ pandas.DataFrame)                | Flag rows whose plant/date/PM/ESC combo has only one generator.                                                                                                    |
| [`remove_inactive_generators`](#pudl.analysis.allocate_gen_fuel.remove_inactive_generators)(→ pandas.DataFrame)             | Remove the retired generators.                                                                                                                                     |
| [`_identify_transitioning_generators`](#pudl.analysis.allocate_gen_fuel._identify_transitioning_generators)(→ pandas.DataFrame)     | Identify generators whose annual status is empirically inaccurate.                                                                                                 |
| [`identify_retiring_generators`](#pudl.analysis.allocate_gen_fuel.identify_retiring_generators)(→ pandas.DataFrame)           | Identify any generators whose annual "retired" label doesn't match other reporting.                                                                                |
| [`identify_newly_operating_generators`](#pudl.analysis.allocate_gen_fuel.identify_newly_operating_generators)(→ pandas.DataFrame)    | Identify any generators whose annual "proposed" label doesn't match other reporting.                                                                               |
| [`_transition_date_in_report_year`](#pudl.analysis.allocate_gen_fuel._transition_date_in_report_year)(→ pandas.Series)           | Make a boolean series indicating if a generator's transition falls in `report_year`.                                                                               |
| [`_identify_entirely_transitioned_groups`](#pudl.analysis.allocate_gen_fuel._identify_entirely_transitioned_groups)(→ pandas.DataFrame) | Identify anomalously reporting PM/ESC groups with uniform `operational_status`.                                                                                    |
| [`identify_retired_groups`](#pudl.analysis.allocate_gen_fuel.identify_retired_groups)(→ pandas.DataFrame)                | Identify entire PM/ESC groups that have previously retired but are reporting data.                                                                                 |
| [`identify_proposed_groups`](#pudl.analysis.allocate_gen_fuel.identify_proposed_groups)(→ pandas.DataFrame)               | Identify entirely new PM/ESC groups that are proposed but already reporting data.                                                                                  |
| [`_allocate_unassociated_pm_records`](#pudl.analysis.allocate_gen_fuel._allocate_unassociated_pm_records)(→ pandas.DataFrame)      | Associate unassociated [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) table records on idx_cols. |
| [`prep_allocation_fraction`](#pudl.analysis.allocate_gen_fuel.prep_allocation_fraction)(→ pandas.DataFrame)               | Prepare the associated generators for allocation.                                                                                                                  |
| [`allocate_gen_fuel_by_gen_esc`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc)(→ pandas.DataFrame)           | Allocate net generation to generators/energy_source_code via three methods.                                                                                        |
| [`allocate_fuel_by_gen_esc`](#pudl.analysis.allocate_gen_fuel.allocate_fuel_by_gen_esc)(→ pandas.DataFrame)               | Allocate fuel_consumption to generators/energy_source_code via three methods.                                                                                      |
| [`remove_aggregated_sentinel_value`](#pudl.analysis.allocate_gen_fuel.remove_aggregated_sentinel_value)(→ pandas.Series)          | Replace the post-aggregation sentinel values in a column with zero.                                                                                                |
| [`group_duplicate_keys`](#pudl.analysis.allocate_gen_fuel.group_duplicate_keys)(→ pandas.DataFrame)                   | Catches duplicate keys in the allocated data and groups them together.                                                                                             |
| [`distribute_annually_reported_data_to_months_if_annual`](#pudl.analysis.allocate_gen_fuel.distribute_annually_reported_data_to_months_if_annual)(...) | Allocates annually-reported data from the gen or bf table to each month.                                                                                           |
| [`manually_fix_energy_source_codes`](#pudl.analysis.allocate_gen_fuel.manually_fix_energy_source_codes)(→ pandas.DataFrame)       | Reassign fuel codes that differ between gen-fuel and gens tables.                                                                                                  |
| [`adjust_msw_energy_source_codes`](#pudl.analysis.allocate_gen_fuel.adjust_msw_energy_source_codes)(→ pandas.DataFrame)         | Adjusts MSW codes.                                                                                                                                                 |
| [`add_missing_energy_source_codes_to_gens`](#pudl.analysis.allocate_gen_fuel.add_missing_energy_source_codes_to_gens)(gens_at_freq, ...) | Add energy_source_codes to gens that were found only in the gf or bf tables.                                                                                       |
| [`identify_missing_gf_escs_in_gens`](#pudl.analysis.allocate_gen_fuel.identify_missing_gf_escs_in_gens)(gens_at_freq, gf, bf)     | Identify energy_source_codes that exist in gf or bf but not gens.                                                                                                  |
| [`allocate_bf_data_to_gens`](#pudl.analysis.allocate_gen_fuel.allocate_bf_data_to_gens)(→ pandas.DataFrame)               | Allocates boiler fuel data to the generator level.                                                                                                                 |
| [`_warn_if_missing_pms`](#pudl.analysis.allocate_gen_fuel._warn_if_missing_pms)(→ None)                               | Log warning if there are too many null `prime_mover_code` s.                                                                                                       |
| [`_test_frac`](#pudl.analysis.allocate_gen_fuel._test_frac)(→ pandas.DataFrame)                             | Check if each of the IDX_PM_ESC groups frac's add up to 1.                                                                                                         |
| [`test_gen_fuel_allocation`](#pudl.analysis.allocate_gen_fuel.test_gen_fuel_allocation)(→ None)                           | Does the allocated MWh differ from the granular [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation)?    |

## Module Contents

### pudl.analysis.allocate_gen_fuel.logger

### pudl.analysis.allocate_gen_fuel.AllocationFrequency

The two frequencies at which generation & fuel data can be allocated.

### pudl.analysis.allocate_gen_fuel.IDX_GENS *= ['report_date', 'plant_id_eia', 'generator_id']*

Primary key columns for generator records.

### pudl.analysis.allocate_gen_fuel.IDX_GENS_PM_ESC *= ['report_date', 'plant_id_eia', 'generator_id', 'prime_mover_code', 'energy_source_code']*

Primary key columns for plant, generator, prime mover & energy source records.

### pudl.analysis.allocate_gen_fuel.IDX_PM_ESC *= ['report_date', 'plant_id_eia', 'energy_source_code', 'prime_mover_code']*

Primary key columns for plant, prime mover & energy source records.

### pudl.analysis.allocate_gen_fuel.IDX_B_PM_ESC *= ['report_date', 'plant_id_eia', 'boiler_id', 'energy_source_code', 'prime_mover_code']*

Primary key columns for plant, boiler, prime mover & energy source records.

### pudl.analysis.allocate_gen_fuel.IDX_ESC *= ['report_date', 'plant_id_eia', 'energy_source_code']*

Primary key columns for plant & energy source records.

### pudl.analysis.allocate_gen_fuel.IDX_UNIT_ESC *= ['report_date', 'plant_id_eia', 'energy_source_code', 'unit_id_pudl']*

Primary key columns for plant, energy source & unit records.

### pudl.analysis.allocate_gen_fuel.DATA_COLUMNS *= ['net_generation_mwh', 'fuel_consumed_mmbtu', 'fuel_consumed_for_electricity_mmbtu']*

Data columns from [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) that are being allocated.

### pudl.analysis.allocate_gen_fuel.MISSING_SENTINEL *= 1e-05*

A sentinel value for dealing with null or zero values.

1. Zeroes in the relevant data columns get filled in with the sentinel value in
   [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables). At this stage all of the zeros from the original
   data that are now associated with generators, prime mover codes and energy source
   codes.
2. All of the nulls in the relevant data columns are filled with the sentinel value in
   [`prep_allocation_fraction()`](#pudl.analysis.allocate_gen_fuel.prep_allocation_fraction). (Could this also be done in
   [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables)?)
3. After the allocation of net generation (within [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc)
   and [`allocate_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_fuel_by_gen_esc) via [`remove_aggregated_sentinel_value()`](#pudl.analysis.allocate_gen_fuel.remove_aggregated_sentinel_value)),
   convert all of the aggregated values that are between 0 and twenty times this
   sentinel value back to zero’s. This is meant to find all instances of aggregated
   sentinel values. We avoid any negative values because there are instances of
   negative original values - especially negative net generation.

### pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_asset_factory(freq: [AllocationFrequency](#pudl.analysis.allocate_gen_fuel.AllocationFrequency), io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)]

Build yearly and monthly net generation & fuel consumption allocation assets.

### pudl.analysis.allocate_gen_fuel.ALLOCATION_FREQUENCIES *: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[AllocationFrequency](#pudl.analysis.allocate_gen_fuel.AllocationFrequency), ...]* *= ('YS', 'MS')*

### pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_assets

### pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source(gf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gen: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bga: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), freq: [AllocationFrequency](#pudl.analysis.allocate_gen_fuel.AllocationFrequency), debug: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Allocate net gen from gen_fuel table to the generator/energy_source_code level.

There are two main steps here:

* associate `core_eia923__monthly_generation_fuel` table data w/ generators
* allocate `core_eia923__monthly_generation_fuel` table data proportionally

The association process happens via [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).

The allocation process (via [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc)) entails
generating a fraction for each record within a `IDX_PM_ESC` group. We
have two data points for generating this ratio: the net generation in the
core_eia923_\_monthly_generation table and the capacity from the core_eia860_\_scd_generators table.
The end result is a `frac` column which is unique for each combination of
generator, prime_mover, and fuel and is used to allocate the associated
net generation from the [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table.

* **Parameters:**
  * **gf** – Temporally aggregated [out_eia923_\_generation_fuel_combined](../../../../data_dictionaries/pudl_db.html.md#out-eia923-generation-fuel-combined) dataframe.
  * **bf** – Temporally aggregated [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) dataframe.
  * **gen** – Temporally aggregated [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) dataframe.
  * **bga** – [core_eia860_\_assn_boiler_generator](../../../../data_dictionaries/pudl_db.html.md#core-eia860-assn-boiler-generator) dataframe.
  * **gens** – [core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) dataframe.
  * **freq** – Frequency at which the tables are aggregated temporally.
  * **debug** – If True, return additional debugging information.

### pudl.analysis.allocate_gen_fuel.select_input_data(gf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gen: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bga: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Select only the subset of input data needed for the allocation.

This includes both selecting only a subset of columns from most input tables, and
restricting the dates to those which are available in all inputs. Otherwise we end
up with a bunch of NA values since the generators table has up to a year of more
recent data from the EIA-860M.

### pudl.analysis.allocate_gen_fuel.standardize_input_frequency(bf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gen: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), freq: [AllocationFrequency](#pudl.analysis.allocate_gen_fuel.AllocationFrequency)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Standardize the frequency of the input tables.

Employ [`distribute_annually_reported_data_to_months_if_annual()`](#pudl.analysis.allocate_gen_fuel.distribute_annually_reported_data_to_months_if_annual) on the boiler
fuel and generation table. Employ [`pudl.helpers.expand_timeseries()`](../../helpers/index.html.md#pudl.helpers.expand_timeseries) on the
generators table. Also use the expanded generators table to ensure the generation
table has all of the generators present.

* **Parameters:**
  * **bf** – [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) table
  * **gens** – [core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) table
  * **gen** – [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) table
  * **freq** – the (time) frequency at which the tables will be aggregated.

### pudl.analysis.allocate_gen_fuel.scale_allocated_net_gen_fuel_by_ownership(net_gen_fuel_alloc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), own_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Scale allocated net gen at the generator/energy_source_code level by ownership.

It can be helpful to have a table of net generation and fuel consumption at the
generator/fuel-type level (i.e. the result of
[`allocate_gen_fuel_by_generator_energy_source()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source)) to be associated and scaled
with all of the owners of those generators.  This allows the aggregation of fuel use
to the utility level.

This function uses the allocated net generation at the generator/fuel-type level,
merges that with a generators table to ensure all necessary columns are available,
and then feeds that table into the helper function `scale_by_ownership()`
to scale generators by their owners’ ownership fraction.

* **Parameters:**
  * **net_gen_fuel_alloc** – table of allocated generation and fuel consumption
    at the generator, prime mover, and energy source.
    From [`allocate_gen_fuel_by_generator_energy_source()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source)
  * **gens** – `core_eia860__scd_generators` table with cols: :const:`IDX_GENS`,
    `capacity_mw` and `utility_id_eia`
  * **own_eia860** – `core_eia860__scd_ownership` table.

### pudl.analysis.allocate_gen_fuel.agg_by_generator(net_gen_fuel_alloc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), by_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = IDX_GENS, sum_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = DATA_COLUMNS) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Aggregate the allocated gen fuel data to the generator level.

* **Parameters:**
  * **net_gen_fuel_alloc** – result of [`allocate_gen_fuel_by_generator_energy_source()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source)
  * **by_cols** – list of columns to use as `pandas.groupby` arg `by`
  * **sum_cols** – Data columns from that are being aggregated via a
    `pandas.groupby.sum()`

### pudl.analysis.allocate_gen_fuel.stack_generators(gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cat_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'energy_source_code_num', stacked_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'energy_source_code') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Stack the generator table with a set of columns.

* **Parameters:**
  * **gens** – core_eia860_\_scd_generators table with cols: [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS) and all of the
    `energy_source_code` columns
  * **cat_col** – name of category column which will end up having the column names of
    `cols_to_stack`
  * **stacked_col** – name of column which will end up with the stacked data from
    `cols_to_stack`
* **Returns:**
  a dataframe with these columns: idx_stack, cat_col,
  stacked_col
* **Return type:**
  [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.analysis.allocate_gen_fuel.associate_generator_tables(gens_at_freq: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gen: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bga: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Associate the three tables needed to assign net gen and fuel to generators.

The [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table’s data is reported at the
[`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC) granularity. Each generator in the [core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators)
has one `prime_mover_code`, but potentially several `energy_source_code``s that
are reported in several columns. We need to reshape the generators table such that
each generator has a separate record corresponding to each of its reported
energy_source_codes, so it can be merged with the :ref:`core_eia923__monthly_generation_fuel`
table. We do this using :func:``stack_generators` employing
`pd.DataFrame.stack()`.

The stacked generators table has a primary key of
`["plant_id_eia", "generator_id", "report_date", "energy_source_code"]`. The table
also includes the `prime_mover_code` column to enable merges with other tables,
the `capacity_mw` column which we use to determine the allocation when there is no
data in the granular data tables, and the `operational_status` column which we use
to remove inactive plants from the association and allocation process.

The remaining data tables are all less granular than this stacked generators table
and have varying primary keys. We add suffixes to the data columns in these data
tables to identify the source table before broadcast merging these data columns into
the stacked generators. This broadcasted data will be used later in the allocation
process.

This function also removes inactive generators so that we don’t associate any net
generation or fuel to those generators. See [`remove_inactive_generators()`](#pudl.analysis.allocate_gen_fuel.remove_inactive_generators) for
more details.

There are some records in the data tables that have either `prime_mover_code` s  or
`energy_source_code` s that do no appear in the [core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) table.
We employ `_allocate_unassociated_bf_records()` to make sure those records are
associated.

* **Parameters:**
  * **gens** – [core_eia860_\_scd_generators](../../../../data_dictionaries/pudl_db.html.md#core-eia860-scd-generators) table with cols: [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS) and all of
    the `energy_source_code` columns and expanded to the same frequency.
  * **gf** – [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) table with columns: [`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC) and
    `net_generation_mwh` and `fuel_consumed_mmbtu`.
  * **gen** – [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) table with columns: [`IDX_GENS`](#pudl.analysis.allocate_gen_fuel.IDX_GENS) and
    `net_generation_mwh`.
  * **bf** – [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) table with columns: [`IDX_B_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_B_PM_ESC) and
    fuel consumption columns.
  * **bga** – [core_eia860_\_assn_boiler_generator](../../../../data_dictionaries/pudl_db.html.md#core-eia860-assn-boiler-generator) table.
* **Returns:**
  table of generators with stacked energy sources and broadcasted net generation
  and fuel data from the [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation) and [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel)
  tables. There are many duplicate values in this output which will later be used
  in the allocation process in [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc) and
  [`allocate_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_fuel_by_gen_esc).

### pudl.analysis.allocate_gen_fuel.\_label_gf_unique_to_gen(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Flag rows whose plant/date/PM/ESC combo has only one generator.

A gf-table ([core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel)) record is reported at
plant/prime-mover/energy-source granularity, not per generator, so a nonzero
value can’t always be attributed to a specific generator. When a given
plant/report_date/prime_mover_code/energy_source_code combination is only ever
reported by a single generator, though, any gf-table data for that combination
can only belong to that one generator.

* **Parameters:**
  **gen_assoc** – table of generators with stacked energy sources and broadcasted
  net generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
* **Returns:**
  `gen_assoc` with a new boolean `gf_unique_to_gen` column, True for rows
  whose plant/report_date/prime_mover_code/energy_source_code combination is
  reported by exactly one generator.

### pudl.analysis.allocate_gen_fuel.remove_inactive_generators(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Remove the retired generators.

We don’t want to associate and later allocate net generation or fuel to generators
that are retired (or proposed! or any other `operational_status` besides
`existing`). However, we do want to keep the generators that report operational
statuses other than `existing` but which report non-zero data despite being
`retired` or `proposed`. This includes several categories of generators/plants,
which come in mirror-image pairs – retiring/retired vs. newly operating/proposed –
built from shared logic ([`_identify_transitioning_generators()`](#pudl.analysis.allocate_gen_fuel._identify_transitioning_generators) and
[`_identify_entirely_transitioned_groups()`](#pudl.analysis.allocate_gen_fuel._identify_entirely_transitioned_groups)):

* `retiring_generators`: generators that retire mid-year, or report data on or
  after their retirement date despite being labeled “retired” for the whole year.
* `retired_pm_esc_groups`: entire prime_mover/energy_source_code groups that
  supposedly retired prior to the current year but which report data. A different
  group at the same plant with a different (or mixed, or mid-year-transitioning)
  status doesn’t disqualify this one, since gf-table data is reported at PM/ESC
  granularity and so can never be ambiguous across groups. If a group has a mix of
  gens which are existing and retired, they are not included in this category.
* `newly_operating_generators`: generators that become operational mid-year, or
  report data before their operating date despite being labeled “proposed” for the
  whole year, or which start reporting non-zero data despite having no known
  operating date yet.
* `proposed_pm_esc_groups`: entire prime_mover/energy_source_code groups that
  have a `proposed` status but which start reporting data before their operating
  date, scoped per PM/ESC group for the same reason as `retired_pm_esc_groups`
  above. If a group has a mix of gens which are existing and proposed, they are not
  included in this category.

When we do not have generator-specific generation for a proposed/retired generator
that is not newly operating/retiring mid-year, we can also look at whether there is
generation reported for this generator in the gf table. However, if a
proposed/retired generator shares its prime_mover/energy_source_code group with an
existing generator, it is possible that the reported generation from the gf table
belongs to that other generator instead. Thus, we want to only keep proposed/retired
generators where the entire PM/ESC group is proposed/retired (in which case the gf-
reported generation could only come from one of the new/retired generators). If the
reported gf data is for a prime_mover / energy_source_code combo that is unique to
the retiring/newly-operating generator, we can identify it at the generator level.

We also want to keep unassociated plants that have no `generator_id` which will
be associated via `_allocate_unassociated_records()`.

* **Parameters:**
  **gen_assoc** – table of generators with stacked energy sources and broadcasted net
  generation data from the core_eia923_\_monthly_generation and
  core_eia923_\_monthly_generation_fuel tables. Output of
  [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
* **Returns:**
  `gen_assoc` filtered down to existing generators, unassociated plants, and
  the retiring/retired/newly-operating/proposed categories described above.

### pudl.analysis.allocate_gen_fuel.\_TRANSITION_DATE_COL *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Literal['retired', 'proposed'], [str](https://docs.python.org/3/library/stdtypes.html#str)]*

The column recording a generator’s actual transition date, keyed by `operational_status`.

### pudl.analysis.allocate_gen_fuel.\_identify_transitioning_generators(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), operational_status: Literal['retired', 'proposed']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify generators whose annual status is empirically inaccurate.

Shared by [`identify_retiring_generators()`](#pudl.analysis.allocate_gen_fuel.identify_retiring_generators) (retiring, keyed on
`generator_retirement_date`) and [`identify_newly_operating_generators()`](#pudl.analysis.allocate_gen_fuel.identify_newly_operating_generators)
(newly operating, keyed on `generator_operating_date`), so the two mirror-image
cases can’t silently drift out of sync with each other.

A generator qualifies for a given `report_year` if ANY of the following hold:

1. the transition-date column shows the annual `operational_status` label is
   stale for at least one month of the year – e.g. a generator labeled “retired”
   for the whole year whose `report_date <= generator_retirement_date` (it
   hadn’t actually retired yet as of that month), or a generator labeled
   “proposed” whose `report_date >= generator_operating_date` (it had already
   started operating as of that month). This check is purely date-based, with no
   bound on *how* stale the label is – a generator whose recorded transition date
   is decades in the past, but whose annual status was simply never updated to
   match, qualifies here just as readily as one that genuinely transitioned
   mid-year, OR
2. it reports generator-specific generation data in the g table, OR
3. it has non-zero generation or fuel reported in the gf table for a PM/ESC combo
   that is unique to that generator at the plant.

Once a generator qualifies for a report_year, every month of that generator’s
data in that report_year is kept, since the annual status label can’t be trusted
to isolate exactly which months are the anomalous ones.

* **Parameters:**
  * **gen_assoc** – table of generators with stacked energy sources and broadcasted
    net generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
  * **operational_status** – the `operational_status` value identifying candidate
    generators (`"retired"` or `"proposed"`). Determines both the
    transition-date column ([`_TRANSITION_DATE_COL`](#pudl.analysis.allocate_gen_fuel._TRANSITION_DATE_COL)) and the comparison
    used to detect condition A above ([`operator.le()`](https://docs.python.org/3/library/operator.html#operator.le) for `"retired"`,
    [`operator.ge()`](https://docs.python.org/3/library/operator.html#operator.ge) for `"proposed"`).
* **Returns:**
  The subset of `gen_assoc` rows belonging to qualifying generators, with
  every month of each qualifying generator’s data retained for the
  report_years it qualified in.

### pudl.analysis.allocate_gen_fuel.identify_retiring_generators(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify any generators whose annual “retired” label doesn’t match other reporting.

Thin wrapper around [`_identify_transitioning_generators()`](#pudl.analysis.allocate_gen_fuel._identify_transitioning_generators) for the
`"retired"` direction (keyed on `generator_retirement_date`). See that
function for the qualifying conditions, the date comparison used, and the
month-retention behavior.

* **Parameters:**
  **gen_assoc** – table of generators with stacked energy sources and broadcasted
  net generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
* **Returns:**
  The subset of `gen_assoc` rows belonging to retiring generators.

### pudl.analysis.allocate_gen_fuel.identify_newly_operating_generators(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify any generators whose annual “proposed” label doesn’t match other reporting.

Thin wrapper around [`_identify_transitioning_generators()`](#pudl.analysis.allocate_gen_fuel._identify_transitioning_generators) for the
`"proposed"` direction (keyed on `generator_operating_date`). See that
function for the qualifying conditions, the date comparison used, and the
month-retention behavior.

* **Parameters:**
  **gen_assoc** – table of generators with stacked energy sources and broadcasted
  net generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
* **Returns:**
  The subset of `gen_assoc` rows belonging to newly operating generators.

### pudl.analysis.allocate_gen_fuel.\_transition_date_in_report_year(operational_status: Literal['retired', 'proposed'], transition_date: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), report_year: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Make a boolean series indicating if a generator’s transition falls in `report_year`.

For `"retired"`, true if the retirement date falls on or after the start of
`report_year`. For `"proposed"`, true if the operating date falls on or
before the end of `report_year`.

* **Parameters:**
  * **operational_status** – the `operational_status` value identifying the kind of
    transition (`"retired"` or `"proposed"`). Determines whether
    `transition_date` is compared against the start or the end of
    `report_year`.
  * **transition_date** – the date the transition actually happened
    (`generator_retirement_date` for `"retired"`, or
    `generator_operating_date` for `"proposed"`).
  * **report_year** – the calendar year each row’s `transition_date` is being
    checked against.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – if `operational_status` is not `"retired"` or `"proposed"`.
* **Returns:**
  A boolean series, True for rows whose `transition_date` falls within
  `report_year`.

### pudl.analysis.allocate_gen_fuel.\_identify_entirely_transitioned_groups(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), operational_status: Literal['retired', 'proposed']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify anomalously reporting PM/ESC groups with uniform `operational_status`.

Used by [`identify_retired_groups()`](#pudl.analysis.allocate_gen_fuel.identify_retired_groups) and [`identify_proposed_groups()`](#pudl.analysis.allocate_gen_fuel.identify_proposed_groups) so the
two mirror-image cases are guaranteed to use the same process.

The “entirely operational_status” check is applied by `(plant_id_eia,
prime_mover_code, energy_source_code, report_year)` – a PM/ESC *group*-year
because that’s the granularity at which the generation fuel table is reported.
Scoping to the whole plant instead would let an unrelated PM/ESC group’s transition
block a stable group’s rescue, or a mixed-status generator in a different group
would block it via the “mixed status” check below (see
`test_remove_inactive_generators_cross_group_transition_does_not_lose_data`).

A PM/ESC-group-year is included only if:

* it has at least one row whose `operational_status` column matches the given
  `operational_status` argument, whose `report_date` is anomalous relative to
  the transition-date column (i.e. a retired plant reporting *after* its retirement
  date, or a proposed plant reporting *before* its operating date), whose generation
  fuel table generation is reported (notnull, nonzero), and whose generation table
  generation is *not* reported (since unambiguous generator-level reporting is
  handled by [`_identify_transitioning_generators()`](#pudl.analysis.allocate_gen_fuel._identify_transitioning_generators));
* every generator reported for that PM/ESC-group-year shares `operational_status`
  (no mixed status);
* none of those generators’ transition-date column falls within the report_year
  (mid-year transitions are handled by [`_identify_transitioning_generators()`](#pudl.analysis.allocate_gen_fuel._identify_transitioning_generators)).

The final output is filtered to months with non-null generation fuel table
generation, since there’s nothing to allocate in months where nothing was reported.

* **Parameters:**
  * **gen_assoc** – table of generators with stacked energy sources and broadcasted net
    generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
  * **operational_status** – the `operational_status` value identifying candidate
    PM/ESC-group-years (`"retired"` or `"proposed"`). Determines the name of
    the transition-date column ([`_TRANSITION_DATE_COL`](#pudl.analysis.allocate_gen_fuel._TRANSITION_DATE_COL)), the comparison
    used to detect an anomalous report ([`operator.gt()`](https://docs.python.org/3/library/operator.html#operator.gt) for `"retired"`,
    [`operator.lt()`](https://docs.python.org/3/library/operator.html#operator.lt) for `"proposed"`), and the within-report-year
    transition check ([`_transition_date_in_report_year()`](#pudl.analysis.allocate_gen_fuel._transition_date_in_report_year)).
* **Returns:**
  The subset of `gen_assoc` rows belonging to entirely-`operational_status`
  PM/ESC-group-years that reported anomalous gf-table generation, filtered to
  months with non-null gf-table generation.

### pudl.analysis.allocate_gen_fuel.identify_retired_groups(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify entire PM/ESC groups that have previously retired but are reporting data.

See [`_identify_entirely_transitioned_groups()`](#pudl.analysis.allocate_gen_fuel._identify_entirely_transitioned_groups) for the shared logic.

* **Parameters:**
  **gen_assoc** – table of generators with stacked energy sources and broadcasted
  net generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
* **Returns:**
  The subset of `gen_assoc` rows belonging to entirely-retired PM/ESC-
  group-years that reported anomalous gf-table generation, filtered to
  months with non-null gf-table generation.

### pudl.analysis.allocate_gen_fuel.identify_proposed_groups(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify entirely new PM/ESC groups that are proposed but already reporting data.

See [`_identify_entirely_transitioned_groups()`](#pudl.analysis.allocate_gen_fuel._identify_entirely_transitioned_groups) for the shared logic.

* **Parameters:**
  **gen_assoc** – table of generators with stacked energy sources and broadcasted
  net generation data. Output of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables).
* **Returns:**
  The subset of `gen_assoc` rows belonging to entirely-proposed PM/ESC-
  group-years that reported anomalous gf-table generation, filtered to
  months with non-null gf-table generation.

### pudl.analysis.allocate_gen_fuel.\_allocate_unassociated_pm_records(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], col_w_unexpected_codes: Literal['energy_source_code', 'prime_mover_code'], data_columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Associate unassociated [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) table records on idx_cols.

There are a subset of [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-boiler-fuel) and
[core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation-fuel) records which do not merge onto the
stacked generator table on `IDX_GENS_PM_ESC` or `ID_PM_ESC` respectively. These
records generally don’t match with the set of prime movers and energy sources in the
stacked generator table. In this method, we associate those straggler, unassociated
records by merging these records with the stacked generators without the unmatched
data column.

* **Parameters:**
  * **gen_assoc** – generators associated with data.
  * **idx_cols** – ID columns (includes `col_w_unexpected_codes`)
  * **col_w_unexpected_codes** – name of the column which has codes in it that were not
    found in the generators table.
  * **data_columns** – the data columns to associate and allocate.
* **Returns:**
  `gen_assoc` with the unassociated records’ `data_columns` merged onto and
  allocated across the matching generators, weighted by each generator’s share of
  `capacity_mw` within `idx_cols` minus `col_w_unexpected_codes`. If there
  are no unassociated records, `gen_assoc` is returned unchanged.

### pudl.analysis.allocate_gen_fuel.prep_allocation_fraction(gen_assoc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Prepare the associated generators for allocation.

Make flags and aggregations to prepare for the [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc)
and [`allocate_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_fuel_by_gen_esc) functions.

In [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc), we will break the generators out into four
types - see [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc) docs for details. This function adds
flags for splitting the generators.

* **Parameters:**
  **gen_assoc** – a table of generators that have associated w/ energy sources, prime
  movers and boilers - result of [`associate_generator_tables()`](#pudl.analysis.allocate_gen_fuel.associate_generator_tables)

### pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc(gen_pm_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Allocate net generation to generators/energy_source_code via three methods.

There are three main types of generators:
: * “all gen”: generators of plants which fully report to the `core_eia923__monthly_generation`
    table. This includes records that report more MWh to the `core_eia923__monthly_generation`
    table than to the `core_eia923__monthly_generation_fuel` table (if we did not include these
    records, the ).
  * “some gen”: generators of plants which partially report to the
    `core_eia923__monthly_generation` table.
  * “gf only”: generators of plants which do not report at all to the
    `core_eia923__monthly_generation` table.

Each different type of generator needs to be treated slightly differently,
but all will end up with a `frac` column that can be used to allocate
the `net_generation_mwh_gf_tbl`.

* **Parameters:**
  **gen_pm_fuel** – output of :func:`prep_allocation_fraction()`.

### pudl.analysis.allocate_gen_fuel.allocate_fuel_by_gen_esc(gen_pm_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Allocate fuel_consumption to generators/energy_source_code via three methods.

There are three main types of generators:

> * “all bf”: generators of plants which fully report to the
>   core_eia923_\_monthly_boiler_fuel table.
> * “some bf”: generators of plants which partially report to the
>   core_eia923_\_monthly_boiler_fuel table.
> * “gf only”: generators of plants which do not report at all to the
>   core_eia923_\_monthly_boiler_fuel table.

Each different type of generator needs to be treated slightly differently,
but all will end up with a `frac` column that can be used to allocate
the `fuel_consumed_mmbtu_gf_tbl`.

* **Parameters:**
  **gen_pm_fuel** – output of [`prep_allocation_fraction()`](#pudl.analysis.allocate_gen_fuel.prep_allocation_fraction).

### pudl.analysis.allocate_gen_fuel.remove_aggregated_sentinel_value(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), scalar: [float](https://docs.python.org/3/library/functions.html#float) = 20.0) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Replace the post-aggregation sentinel values in a column with zero.

### pudl.analysis.allocate_gen_fuel.group_duplicate_keys(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Catches duplicate keys in the allocated data and groups them together.

Merging `net_gen_alloc` and `fuel_alloc` together requires unique keys in each
df. Sometimes the allocation process creates duplicate keys. This function
identifies when this happens, and aggregates the data on these keys to remove the
duplicates.

### pudl.analysis.allocate_gen_fuel.distribute_annually_reported_data_to_months_if_annual(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), key_columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], data_column_name: [str](https://docs.python.org/3/library/stdtypes.html#str), freq: [AllocationFrequency](#pudl.analysis.allocate_gen_fuel.AllocationFrequency)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Allocates annually-reported data from the gen or bf table to each month.

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
accurate.

Note: We should be able to use the `reporting_frequency_code` column for the
identification of annually reported data. This currently does not work because we
assumed this was a plant-level annual attribute (and is thus stored in the
`core_eia860__scd_plants` table). See Issue #1933.

* **Parameters:**
  * **df** – A dataframe of either generation or boiler-fuel data, loaded from
    [out_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#out-eia923-monthly-generation) or
    [out_eia923_\_yearly_generation](../../../../data_dictionaries/pudl_db.html.md#out-eia923-yearly-generation) and
    [out_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#out-eia923-monthly-boiler-fuel) or
    [out_eia923_\_yearly_boiler_fuel](../../../../data_dictionaries/pudl_db.html.md#out-eia923-yearly-boiler-fuel) or respectively.
  * **key_columns** – a list of the primary key column names, either
    `["plant_id_eia","boiler_id","energy_source_code"]` or
    `["plant_id_eia","generator_id"]`
  * **data_column_name** – the name of the data column to allocate, either
    “net_generation_mwh” or “fuel_consumed_mmbtu” depending on the df specified
  * **freq** – frequency of input df. Must be either `YS` or `MS`.
* **Returns:**
  Dataframe with the annually reported generation or fuel consumption values
  allocated to each month.

### pudl.analysis.allocate_gen_fuel.manually_fix_energy_source_codes(gf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Reassign fuel codes that differ between gen-fuel and gens tables.

### pudl.analysis.allocate_gen_fuel.adjust_msw_energy_source_codes(gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bf_by_gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Adjusts MSW codes.

Adjust the MSW codes in gens to match those used in gf and bf.

In recent years, EIA-923 started splitting out the `MSW` (municipal_solid_waste)
into its consitituent components `MSB` (municipal_solid_waste_biogenic) and
`MSN` (municipal_solid_nonbiogenic). However, the EIA-860 Generators table still
only uses the `MSW` code.

This function identifies which MSW codes are used in the gf and bf tables and
creates records to match these.

### pudl.analysis.allocate_gen_fuel.add_missing_energy_source_codes_to_gens(gens_at_freq, gf, bf)

Add energy_source_codes to gens that were found only in the gf or bf tables.

In some cases, non-zero fuel consumption and net generation is reported in the
EIA-923 generation and fuel table that is associated with an energy_source_code that
is not associated with that plant-prime mover in the gens table, which would cause
these data to get dropped when these two tables are merged. To fix this, for each
plant-pm, this function identifies such esc, and adds them to the gens_at_freq
table as new energy_source_code columns.

### pudl.analysis.allocate_gen_fuel.identify_missing_gf_escs_in_gens(gens_at_freq, gf, bf)

Identify energy_source_codes that exist in gf or bf but not gens.

### pudl.analysis.allocate_gen_fuel.allocate_bf_data_to_gens(bf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bga: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Allocates boiler fuel data to the generator level.

Distributes boiler-level data from core_eia923_\_monthly_boiler_fuel to the generator level based
on the boiler-generator association table and the nameplate capacity of the
connected generators.

Because fuel consumption in the core_eia923_\_monthly_boiler_fuel table is reported per boiler_id,
we must first map this data to generators using the core_eia860_\_assn_boiler_generator
table. For boilers that have a 1:m or m: m relationship with generators, we allocate
the reported fuel to each associated generator based on the nameplate capacity of
each generator. So if boiler “1” was associated with generator A (25 MW) and generator
B (75 MW), 25% of the fuel consumption would be allocated to generator A and 75% would
be allocated to generator B.

### pudl.analysis.allocate_gen_fuel.\_warn_if_missing_pms(gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [None](https://docs.python.org/3/library/constants.html#None)

Log warning if there are too many null `prime_mover_code` s.

Warn if prime mover codes in gens do not match the codes in the gf table this is
something that should probably be fixed in the input data see
[https://github.com/catalyst-cooperative/pudl/issues/1585](https://github.com/catalyst-cooperative/pudl/issues/1585) set a threshold and ignore
2001 bc most errors are 2001 errors.

This is an input data quality check that can’t be migrated to dbt.

### pudl.analysis.allocate_gen_fuel.\_test_frac(gen_pm_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Check if each of the IDX_PM_ESC groups frac’s add up to 1.

This is used to test data expectations in an intermediate step of the allocation
process, and so can’t be migrated to dbt.

* **Parameters:**
  **gen_pm_fuel** – table of generators with an allocation `frac` column, grouped
  by `IDX_PM_ESC`. Output of [`allocate_gen_fuel_by_gen_esc()`](#pudl.analysis.allocate_gen_fuel.allocate_gen_fuel_by_gen_esc).
* **Returns:**
  The subset of `IDX_PM_ESC` groups whose `frac` values don’t sum to 1
  (empty if none are bad). Any bad groups are also logged as a warning.

### pudl.analysis.allocate_gen_fuel.test_gen_fuel_allocation(gen: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), net_gen_alloc: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), ratio: [float](https://docs.python.org/3/library/functions.html#float) = 0.05) → [None](https://docs.python.org/3/library/constants.html#None)

Does the allocated MWh differ from the granular [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.html.md#core-eia923-monthly-generation)?

This test should be migrated to dbt, since it compares data from two finished
outputs.

* **Parameters:**
  * **gen** – the `core_eia923__monthly_generation` table.
  * **net_gen_alloc** – the allocated net generation at the [`IDX_PM_ESC`](#pudl.analysis.allocate_gen_fuel.IDX_PM_ESC) level
  * **ratio** – the tolerance
