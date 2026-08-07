# pudl.transform.nrelatb

Transform NREL ATB data into well normalized, cleaned tables.

## Attributes

| [`logger`](#pudl.transform.nrelatb.logger)   |                                                        |
|----------------------------------------------|--------------------------------------------------------|
| [`IDX_ALL`](#pudl.transform.nrelatb.IDX_ALL) | Expected primary key columns for the raw nrelatb data. |

## Classes

| [`TableNormalizer`](#pudl.transform.nrelatb.TableNormalizer)   | Info needed to convert a selection of the raw NREL table into a normalized table.                                                                         |
|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`Normalizer`](#pudl.transform.nrelatb.Normalizer)             | Class that defines how to normalize all of the NREL tables that get the [`transform_normalize()`](#pudl.transform.nrelatb.transform_normalize) treatment. |
| [`TableUnstacker`](#pudl.transform.nrelatb.TableUnstacker)     | Info needed to unstack a portion of the NREL ATB table.                                                                                                   |
| [`Unstacker`](#pudl.transform.nrelatb.Unstacker)               | Class that defines how to unstack the raw ATB table into all of the tidy core tables.                                                                     |

## Functions

| [`transform_normalize`](#pudl.transform.nrelatb.transform_normalize)(nrelatb, normalizer)                                                               | Normalize a subset of the NREL ATB data into a small table.                                       |
|---------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| [`transform_unstack`](#pudl.transform.nrelatb.transform_unstack)(→ pandas.DataFrame)                                                                    | Generic unstacking function to convert ATB data from a skinny to wider format.                    |
| [`_core_nrelatb__transform_start`](#pudl.transform.nrelatb._core_nrelatb__transform_start)(raw_nrelatb_\_data)                                          | Transform raw NREL ATB data into semi-clean but still very skinny table.                          |
| [`core_nrelatb__yearly_projected_financial_cases`](#pudl.transform.nrelatb.core_nrelatb__yearly_projected_financial_cases)(...)                         | Transform the data defining the assumptions for the ATB financial cases.                          |
| [`core_nrelatb__yearly_projected_financial_cases_by_scenario`](#pudl.transform.nrelatb.core_nrelatb__yearly_projected_financial_cases_by_scenario)(...) | Transform the data defining the assumptions for the ATB financial cases which vary by scenario.   |
| [`broadcast_fixed_charge_rate_across_tech_detail`](#pudl.transform.nrelatb.broadcast_fixed_charge_rate_across_tech_detail)(...)                         | For older years, broadcast the `fixed_charge_rate` parameter across the technical detail columns. |
| [`broadcast_asterisk_cost_recovery_period_years`](#pudl.transform.nrelatb.broadcast_asterisk_cost_recovery_period_years)(...)                           | Broadcast the asterisk (wildcard) `cost_recovery_period_years`.                                   |
| [`_broadcast_core_metric_parameters`](#pudl.transform.nrelatb._broadcast_core_metric_parameters)(→ pandas.DataFrame)                                    | Broadcast a section of a table and fillna with the broadcasted values.                            |
| [`core_nrelatb__yearly_projected_cost_performance`](#pudl.transform.nrelatb.core_nrelatb__yearly_projected_cost_performance)(...)                       | Transform the yearly NREL ATB cost and performance projections.                                   |
| [`_core_nrelatb__yearly_units`](#pudl.transform.nrelatb._core_nrelatb__yearly_units)(→ pandas.DataFrame)                                                | Transform a table of units by `core_metric_parameter`.                                            |
| [`core_nrelatb__yearly_technology_status`](#pudl.transform.nrelatb.core_nrelatb__yearly_technology_status)(→ pandas.DataFrame)                          | Transform a small table of statuses of different technology types.                                |
| [`null_cols_cost_performance`](#pudl.transform.nrelatb.null_cols_cost_performance)(df)                                                                  | Check for the prevalence of nulls in the core_nrelatb_\_yearly_projected_cost_performance.        |
| [`check_technology_specific_parameters`](#pudl.transform.nrelatb.check_technology_specific_parameters)(df)                                              | Some parameters in the cost performance table only pertain to some technologies.                  |

## Module Contents

### pudl.transform.nrelatb.logger

### pudl.transform.nrelatb.IDX_ALL *= ['report_year', 'model_case_nrelatb', 'model_tax_credit_case_nrelatb', 'projection_year',...*

Expected primary key columns for the raw nrelatb data.

The normalized core tables we are trying to build will have primary keys which

are a subset of these columns.

### *class* pudl.transform.nrelatb.TableNormalizer(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Info needed to convert a selection of the raw NREL table into a normalized table.

#### idx *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Primary key columns of normalized subset table.

#### columns *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Columns reported in raw NREL table that are unique to the [`idx`](#pudl.transform.nrelatb.TableNormalizer.idx).

### pudl.transform.nrelatb.transform_normalize(nrelatb: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), normalizer: [TableNormalizer](#pudl.transform.nrelatb.TableNormalizer))

Normalize a subset of the NREL ATB data into a small table.

Given a [`TableNormalizer`](#pudl.transform.nrelatb.TableNormalizer) with a set of primary keys (`idx`) and a
list of columns (`columns`), build a table with just those primary key and
data columns from the larger ATB semi-processed table (output of
[`_core_nrelatb__transform_start()`](#pudl.transform.nrelatb._core_nrelatb__transform_start)). Ensure that the output table is
unique based on the primary keys.

### *class* pudl.transform.nrelatb.Normalizer(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Class that defines how to normalize all of the NREL tables that get the [`transform_normalize()`](#pudl.transform.nrelatb.transform_normalize) treatment.

There are several columns in the raw ATB data that are not a part of the primary keys
for the tables that get the [`transform_unstack()`](#pudl.transform.nrelatb.transform_unstack) treatment. This class helps us
build these smaller tables which have a smaller subset of primary key columns.

#### units *: [TableNormalizer](#pudl.transform.nrelatb.TableNormalizer)*

#### technology_status *: [TableNormalizer](#pudl.transform.nrelatb.TableNormalizer)*

### *class* pudl.transform.nrelatb.TableUnstacker(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Info needed to unstack a portion of the NREL ATB table.

This class defines a portion of the raw ATB table to get the [`transform_unstack()`](#pudl.transform.nrelatb.transform_unstack)
treatment. The set of tables which get this treatment are defined in [`Unstacker`](#pudl.transform.nrelatb.Unstacker).

#### idx *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### core_metric_parameters *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Values from the `core_metric_parameter` column to be included in this unstack.

#### *classmethod* idx_are_same_or_subset_of_idx_all(idx: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)])

Are the [`idx`](#pudl.transform.nrelatb.TableUnstacker.idx) columns either the same as or a subset of [`IDX_ALL`](#pudl.transform.nrelatb.IDX_ALL)?

#### *property* idx_unstacked *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Primary key columns after the table is unstacked.

All of the columns in [`idx`](#pudl.transform.nrelatb.TableUnstacker.idx) except core_metric_parameter.

### pudl.transform.nrelatb.transform_unstack(nrelatb: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), table_unstacker: [TableUnstacker](#pudl.transform.nrelatb.TableUnstacker)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Generic unstacking function to convert ATB data from a skinny to wider format.

This function applies `pandas.unstack()` to a subset of values
for `core_metric_parameter` (via [`TableUnstacker.core_metric_parameters`](#pudl.transform.nrelatb.TableUnstacker.core_metric_parameters))
with different primary keys (via [`TableUnstacker.idx`](#pudl.transform.nrelatb.TableUnstacker.idx)). If the set of given
`core_metric_parameters` result in non-unique values for the primary keys,
`pandas.unstack()` will raise an error.

### *class* pudl.transform.nrelatb.Unstacker(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Class that defines how to unstack the raw ATB table into all of the tidy core tables.

The ATB data is reported in a very skinny format that enables the raw data to have the
same schema over time. The `core_metric_parameter` column contains a string which
indicates what type of data is being reported in the `value` column.

We want the strings in `core_metric_parameter` to end up as column names in the
tables, so that each column represents a unique type of data. In the end, there will
be one column containing values from the `value` column for each unique
`core_metric_parameter`. A quirk with ATB is that different `core_metric_parameter`
have different set of primary keys. Subsets of the `core_metric_parameter` have
unique values across the data given specific primary keys.

The convention for ATB data is to use an asterisk in the key columns as a wildcard.
Generally when an asterisk is in one the `IDX_ALL` columns, the corresponding
`core_metric_parameter` should be associated with a table without that column
as one of its `idx` - thus in effect dropping these asterisks from the data.
Once these tables are in their core tidy format, they can be merged back together
using the primary keys.

This class defines all of the tables in the ATB data that get the
[`transform_unstack()`](#pudl.transform.nrelatb.transform_unstack) treatment.

#### rate_table *: [TableUnstacker](#pudl.transform.nrelatb.TableUnstacker)*

#### scenario_table *: [TableUnstacker](#pudl.transform.nrelatb.TableUnstacker)*

#### tech_detail_table *: [TableUnstacker](#pudl.transform.nrelatb.TableUnstacker)*

#### *property* core_metric_parameters_all *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Compilation of all of the parameter values from each of the tables.

Also check if there are no duplicate core_metric_parameters. We expect all of the parameters
across the [`TableUnstacker`](#pudl.transform.nrelatb.TableUnstacker) to be unique.

### pudl.transform.nrelatb.\_core_nrelatb_\_transform_start(raw_nrelatb_\_data)

Transform raw NREL ATB data into semi-clean but still very skinny table.

### pudl.transform.nrelatb.core_nrelatb_\_yearly_projected_financial_cases(\_core_nrelatb_\_transform_start) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the data defining the assumptions for the ATB financial cases.

Right now, this just unstacks the table.

### pudl.transform.nrelatb.core_nrelatb_\_yearly_projected_financial_cases_by_scenario(\_core_nrelatb_\_transform_start) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the data defining the assumptions for the ATB financial cases which vary by scenario.

Right now, this unstacks the table and applies [`broadcast_fixed_charge_rate_across_tech_detail()`](#pudl.transform.nrelatb.broadcast_fixed_charge_rate_across_tech_detail).

### pudl.transform.nrelatb.broadcast_fixed_charge_rate_across_tech_detail(nrelatb_unstacked: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_broadcast: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

For older years, broadcast the `fixed_charge_rate` parameter across the technical detail columns.

We want the table schema to be consistent for all years of ATB data. Mostly the parameters
have the same primary keys across all of the years. But the `fixed_charge_rate` parameter is the
only exception. For the older years (pre-2023), the FCR parameter is not variable
based on tech detail so we are going to broadcast the pre-2023 `fixed_charge_rate` values across the
tech details that exist in the data.

### pudl.transform.nrelatb.broadcast_asterisk_cost_recovery_period_years(nrelatb_unstacked, unstack_scenario: [TableUnstacker](#pudl.transform.nrelatb.TableUnstacker))

Broadcast the asterisk (wildcard) `cost_recovery_period_years`.

Most of the records in this unstacked table have values in the
`cost_recovery_period_years` column, but before broadcasting, about 15% of the
table has an `*` in this column. This is a part of the tables primary key and
because we know `*` in a primary key effectively means wildcard, we want to
broadcast the records with an asterisk across the rest of the data. Unfortunately,
there are still ~5% of records w/ `*` that don’t have associated records in the
rest of the data (the are left_only records in [`_broadcast_core_metric_parameters()`](#pudl.transform.nrelatb._broadcast_core_metric_parameters))
so they end up with nulls in the `cost_recovery_period_years` column.

Probably we could treat `cost_recovery_period_years` as a categorical column and/or
figure out ways to fill in these nulls with the right set of merge keys.

### pudl.transform.nrelatb.\_broadcast_core_metric_parameters(nrelatb_unstacked: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), mask_broadcast: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), core_metric_parameters: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], idx_broadcast: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Broadcast a section of a table and fillna with the broadcasted values.

* **Parameters:**
  * **nrelatb_unstacked** – the unstacked ATB table which has values to broadcast.
  * **mask_broadcast** – a series with the same index as nrelatb_unstacked and boolean
    values where the True’s are the records that you want to broadcast.
  * **core_metric_parameters** – the list of core_metric_parameter columns in
    nrelatb_unstacked which you want to extract values from the broadcasted
    records.
  * **idx_broadcast** – the columns to merge `on`.

### pudl.transform.nrelatb.core_nrelatb_\_yearly_projected_cost_performance(\_core_nrelatb_\_transform_start) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the yearly NREL ATB cost and performance projections.

Right now, this just unstacks the table.

### pudl.transform.nrelatb.\_core_nrelatb_\_yearly_units(\_core_nrelatb_\_transform_start: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform a table of units by `core_metric_parameter`.

This asset is created mostly to ensure that the input units do not
vary within one `core_metric_parameter`. If they do vary, we will
need to standardize the units of that parameter.

### pudl.transform.nrelatb.core_nrelatb_\_yearly_technology_status(\_core_nrelatb_\_transform_start: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform a small table of statuses of different technology types.

### pudl.transform.nrelatb.null_cols_cost_performance(df)

Check for the prevalence of nulls in the core_nrelatb_\_yearly_projected_cost_performance.

### pudl.transform.nrelatb.check_technology_specific_parameters(df)

Some parameters in the cost performance table only pertain to some technologies.
