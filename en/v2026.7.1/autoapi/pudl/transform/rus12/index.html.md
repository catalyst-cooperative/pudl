# pudl.transform.rus12

Transform the RUS12 tables.

## Attributes

| [`logger`](#pudl.transform.rus12.logger)                           |    |
|--------------------------------------------------------------------|----|
| [`_CORE_RUS12_TABLES`](#pudl.transform.rus12._CORE_RUS12_TABLES)   |    |
| [`finished_rus_assets`](#pudl.transform.rus12.finished_rus_assets) |    |

## Functions

| [`_core_rus12__yearly_meeting_and_board`](#pudl.transform.rus12._core_rus12__yearly_meeting_and_board)(...)                                           | Transform the core_rus12_\_yearly_meeting_and_board table.              |
|-------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`_core_rus12__yearly_balance_sheet_assets`](#pudl.transform.rus12._core_rus12__yearly_balance_sheet_assets)(...)                                     | Transform the core_rus12_\_yearly_balance_sheet_assets table.           |
| [`_core_rus12__yearly_balance_sheet_liabilities`](#pudl.transform.rus12._core_rus12__yearly_balance_sheet_liabilities)(...)                           | Transform the core_rus12_\_yearly_balance_sheet_liabilities table.      |
| [`_core_rus12__scd_borrowers`](#pudl.transform.rus12._core_rus12__scd_borrowers)(raw_rus12_\_borrowers)                                               | Transform the core_rus12_\_scd_borrowers table.                         |
| [`_core_rus12__yearly_external_financial_risk_ratio`](#pudl.transform.rus12._core_rus12__yearly_external_financial_risk_ratio)(...)                   | Transform the raw_rus12_\_external_financial_risk_ratio table.          |
| [`_core_rus12__yearly_investments`](#pudl.transform.rus12._core_rus12__yearly_investments)(→ pandas.DataFrame)                                        | Transform the investments table.                                        |
| [`_core_rus12__yearly_renewable_plants`](#pudl.transform.rus12._core_rus12__yearly_renewable_plants)(...)                                             | Transform the core_rus12_\_yearly_renewable_plants table.               |
| [`_core_rus12__yearly_long_term_debt`](#pudl.transform.rus12._core_rus12__yearly_long_term_debt)(...)                                                 | Transform the core_rus12_\_yearly_long_term_debt table.                 |
| [`_core_rus12__yearly_lines_stations_labor_materials_cost`](#pudl.transform.rus12._core_rus12__yearly_lines_stations_labor_materials_cost)(...)       | Transform the raw_rus12_\_lines_and_stations_labor_materials table.     |
| [`_core_rus12__yearly_loans`](#pudl.transform.rus12._core_rus12__yearly_loans)(raw_rus12_\_loans, ...)                                                | Transform the raw_rus12_\_loans and raw_rus12_\_loan_guarantees tables. |
| [`_core_rus12__yearly_plant_labor`](#pudl.transform.rus12._core_rus12__yearly_plant_labor)(raw_rus12_\_plant_labor)                                   | Transform the raw_rus12_\_plant_labor table.                            |
| [`_core_rus12__yearly_sources_and_distribution_by_plant_type`](#pudl.transform.rus12._core_rus12__yearly_sources_and_distribution_by_plant_type)(...) | Transform the raw_rus12_\_sources_and_distribution table.               |
| [`_core_rus12__yearly_sources_and_distribution`](#pudl.transform.rus12._core_rus12__yearly_sources_and_distribution)(...)                             | Transform the raw_rus12_\_sources_and_distribution table.               |
| [`_core_rus12__yearly_statement_of_operations`](#pudl.transform.rus12._core_rus12__yearly_statement_of_operations)(...)                               | Transform the raw_rus12_\_statement_of_operations table.                |
| [`_core_rus12__yearly_plant_costs`](#pudl.transform.rus12._core_rus12__yearly_plant_costs)(...)                                                       | Transform the plant cost tables.                                        |
| [`drop_bad_ownership_plant`](#pudl.transform.rus12.drop_bad_ownership_plant)(df)                                                                      | Drop 1 plant record with unexpected ownership label and duplicate data. |
| [`fix_string_unit_id_rus`](#pudl.transform.rus12.fix_string_unit_id_rus)(df)                                                                          | Fix unit_id_rus's bad string IDs.                                       |
| [`_core_rus12__yearly_plant_operations`](#pudl.transform.rus12._core_rus12__yearly_plant_operations)(...)                                             | Transform the plant operations tables.                                  |
| [`_core_rus12__monthly_demand_and_energy_at_delivery_points`](#pudl.transform.rus12._core_rus12__monthly_demand_and_energy_at_delivery_points)(...)   | Transform the raw_rus12_\_demand_and_energy_at_delivery_points table.   |
| [`_core_rus12__monthly_demand_and_energy_at_power_sources`](#pudl.transform.rus12._core_rus12__monthly_demand_and_energy_at_power_sources)(...)       | Transform the raw_rus12_\_demand_and_energy_at_power_sources table.     |
| [`_core_rus12__yearly_plant_factors_and_maximum_demand`](#pudl.transform.rus12._core_rus12__yearly_plant_factors_and_maximum_demand)(...)             | Transform the raw_rus12_\_plant_factors_and_maximum_demand table.       |
| [`_core_rus12__yearly_utility_plant_changes`](#pudl.transform.rus12._core_rus12__yearly_utility_plant_changes)(...)                                   | Transform the utility plant changes table.                              |
| [`_core_rus12__yearly_non_utility_plant_changes`](#pudl.transform.rus12._core_rus12__yearly_non_utility_plant_changes)(...)                           | Transform the non-utility plant changes table.                          |
| [`_core_rus12__yearly_depreciation_changes`](#pudl.transform.rus12._core_rus12__yearly_depreciation_changes)(...)                                     | Transform the accumulated depreciation changes table.                   |
| [`_core_rus12__yearly_depreciation_misc`](#pudl.transform.rus12._core_rus12__yearly_depreciation_misc)(→ pandas.DataFrame)                            | Transform the miscellaneous depreciation ending balance table.          |
| [`core_rus12__entity_borrowers`](#pudl.transform.rus12.core_rus12__entity_borrowers)(context, \*\*clean_dfs)                                          | Harvesting IDs & consistent static attributes for RUS12 entity.         |

## Module Contents

### pudl.transform.rus12.logger

### pudl.transform.rus12.\_core_rus12_\_yearly_meeting_and_board(raw_rus12_\_meeting_and_board)

Transform the core_rus12_\_yearly_meeting_and_board table.

### pudl.transform.rus12.\_core_rus12_\_yearly_balance_sheet_assets(raw_rus12_\_balance_sheet)

Transform the core_rus12_\_yearly_balance_sheet_assets table.

### pudl.transform.rus12.\_core_rus12_\_yearly_balance_sheet_liabilities(raw_rus12_\_balance_sheet)

Transform the core_rus12_\_yearly_balance_sheet_liabilities table.

### pudl.transform.rus12.\_core_rus12_\_scd_borrowers(raw_rus12_\_borrowers)

Transform the core_rus12_\_scd_borrowers table.

### pudl.transform.rus12.\_core_rus12_\_yearly_external_financial_risk_ratio(raw_rus12_\_external_financial_risk_ratio: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus12_\_external_financial_risk_ratio table.

### pudl.transform.rus12.\_core_rus12_\_yearly_investments(raw_rus12_\_investments: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the investments table.

### pudl.transform.rus12.\_core_rus12_\_yearly_renewable_plants(raw_rus12_\_renewable_plants)

Transform the core_rus12_\_yearly_renewable_plants table.

### pudl.transform.rus12.\_core_rus12_\_yearly_long_term_debt(raw_rus12_\_long_term_debt)

Transform the core_rus12_\_yearly_long_term_debt table.

### pudl.transform.rus12.\_core_rus12_\_yearly_lines_stations_labor_materials_cost(raw_rus12_\_lines_and_stations_labor_materials)

Transform the raw_rus12_\_lines_and_stations_labor_materials table.

### pudl.transform.rus12.\_core_rus12_\_yearly_loans(raw_rus12_\_loans, raw_rus12_\_loan_guarantees)

Transform the raw_rus12_\_loans and raw_rus12_\_loan_guarantees tables.

### pudl.transform.rus12.\_core_rus12_\_yearly_plant_labor(raw_rus12_\_plant_labor)

Transform the raw_rus12_\_plant_labor table.

### pudl.transform.rus12.\_core_rus12_\_yearly_sources_and_distribution_by_plant_type(raw_rus12_\_sources_and_distribution)

Transform the raw_rus12_\_sources_and_distribution table.

This function pivots the cost, capacity, net-energy, and plant num data by plant type
from the Sources and Distribution table. The rest of the table contents are in
core_rus12_\_yearly_sources_and_distribution.

### pudl.transform.rus12.\_core_rus12_\_yearly_sources_and_distribution(raw_rus12_\_sources_and_distribution)

Transform the raw_rus12_\_sources_and_distribution table.

This function process all columns from the Sources and Distribution table
that are not plant type specific. The plant type specific columns are processed
in core_rus12_\_yearly_sources_and_distribution_by_plant_type.

The multi_index_stack function intentionally drops a few columns that don’t
show up in other tables. This include:
- `total_energy_losses_pct` (calculable with other cols, dropped because pct value
was an outlier column and not easily stacked with other columns).
- `total_plant_num` (calculable with the sources_and_distribution_by_plant_type table).
- `total_capacity_kw` (calculable with the sources_and_distribution_by_plant_type table).

This function keeps the `total_plant_cost` and `total_plant_mwh` columns even though
they are also calculable with the other table, because they are components of other
totals included in this table.

### pudl.transform.rus12.\_core_rus12_\_yearly_statement_of_operations(raw_rus12_\_statement_of_operations)

Transform the raw_rus12_\_statement_of_operations table.

This function drops a number of columns that contain per_kwh values that are
entirely NA through all years. It then reshapes the table by stacking expense
types by the name of the total column for which they are calculation components.

### pudl.transform.rus12.\_core_rus12_\_yearly_plant_costs(raw_rus12_\_combined_cycle_plant_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_hydro_plant_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_internal_combustion_plant_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_nuclear_plant_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_steam_plant_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transform the plant cost tables.

This transform takes all of the plant production cost tables, processes
them similarly and combines them into one plant cost table.

### pudl.transform.rus12.drop_bad_ownership_plant(df)

Drop 1 plant record with unexpected ownership label and duplicate data.

There is a Wisdom steam plant record that is labeled to be both fully owned by
borrower and partly owned for one year. Which is an unexpected combo based on the
\_OR_PowerSupply Plant File Documentation.rst documentation file in the rus12
archive. Luckily this plant has exactly the same records as the other Wisdom steam
plant that year with more expected ownership labels.

So we check if the two plant records for that year have the same data, then
drop the one badly labeled ownership record.

### pudl.transform.rus12.fix_string_unit_id_rus(df)

Fix unit_id_rus’s bad string IDs.

There are two instances of unit_id_rus’s that have string values in them.
Based on pre-cleaned data, we were able to clearly identify that we can use
just the numeric values in these bad strings. This enables us to have an integer
type for this unit_id_rus column.

### pudl.transform.rus12.\_core_rus12_\_yearly_plant_operations(raw_rus12_\_combined_cycle_plant_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_hydro_plant_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_internal_combustion_plant_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_nuclear_plant_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus12_\_steam_plant_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transform the plant operations tables.

This transform takes all of the plant operations tables, processes
them similarly and combines them into one plant table. Which is then
split out into two tables: by borrower and by plant. The details of
which record should end up in which output table are documented in
these tables’ resource metadata.

### pudl.transform.rus12.\_core_rus12_\_monthly_demand_and_energy_at_delivery_points(raw_rus12_\_demand_and_energy_at_delivery_points) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus12_\_demand_and_energy_at_delivery_points table.

### pudl.transform.rus12.\_core_rus12_\_monthly_demand_and_energy_at_power_sources(raw_rus12_\_demand_and_energy_at_power_sources) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus12_\_demand_and_energy_at_power_sources table.

### pudl.transform.rus12.\_core_rus12_\_yearly_plant_factors_and_maximum_demand(raw_rus12_\_plant_factors_and_maximum_demand) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus12_\_plant_factors_and_maximum_demand table.

### pudl.transform.rus12.\_core_rus12_\_yearly_utility_plant_changes(raw_rus12_\_utility_plant_changes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transform the utility plant changes table.

### pudl.transform.rus12.\_core_rus12_\_yearly_non_utility_plant_changes(raw_rus12_\_non_utility_plant)

Transform the non-utility plant changes table.

### pudl.transform.rus12.\_core_rus12_\_yearly_depreciation_changes(raw_rus12_\_depreciation: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the accumulated depreciation changes table.

### pudl.transform.rus12.\_core_rus12_\_yearly_depreciation_misc(raw_rus12_\_depreciation: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the miscellaneous depreciation ending balance table.

### pudl.transform.rus12.\_CORE_RUS12_TABLES

### pudl.transform.rus12.core_rus12_\_entity_borrowers(context, \*\*clean_dfs)

Harvesting IDs & consistent static attributes for RUS12 entity.

### pudl.transform.rus12.finished_rus_assets
