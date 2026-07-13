# pudl.transform.rus7

Transform the RUS7 tables.

## Attributes

| [`logger`](#pudl.transform.rus7.logger)                           |    |
|-------------------------------------------------------------------|----|
| [`_CORE_RUS7_TABLES`](#pudl.transform.rus7._CORE_RUS7_TABLES)     |    |
| [`finished_rus_assets`](#pudl.transform.rus7.finished_rus_assets) |    |

## Functions

| [`_core_rus7__yearly_meeting_and_board`](#pudl.transform.rus7._core_rus7__yearly_meeting_and_board)(...)                                         | Transform the meeting and board (aka governance) table.               |
|--------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`_core_rus7__yearly_balance_sheet_assets`](#pudl.transform.rus7._core_rus7__yearly_balance_sheet_assets)(...)                                   | Transform the balance sheet assets table.                             |
| [`_core_rus7__yearly_balance_sheet_liabilities`](#pudl.transform.rus7._core_rus7__yearly_balance_sheet_liabilities)(...)                         | Transform the balance sheet liabilities table.                        |
| [`_core_rus7__scd_borrowers`](#pudl.transform.rus7._core_rus7__scd_borrowers)(raw_rus7_\_borrowers)                                              | Transform the borrowers table.                                        |
| [`_core_rus7__yearly_employee_statistics`](#pudl.transform.rus7._core_rus7__yearly_employee_statistics)(...)                                     | Transform the employee statistics table.                              |
| [`_core_rus7__yearly_energy_efficiency`](#pudl.transform.rus7._core_rus7__yearly_energy_efficiency)(...)                                         | Transform the energy efficiency table.                                |
| [`_core_rus7__power_requirements`](#pudl.transform.rus7._core_rus7__power_requirements)(...)                                                     | Early transform an internal power_requirements table.                 |
| [`_core_rus7__yearly_power_requirements_electric_sales`](#pudl.transform.rus7._core_rus7__yearly_power_requirements_electric_sales)(...)         | Transform the power requirements of electric sales table.             |
| [`_core_rus7__yearly_power_requirements_electric_customers`](#pudl.transform.rus7._core_rus7__yearly_power_requirements_electric_customers)(...) | Transform the power requirements of electric customers table.         |
| [`_core_rus7__yearly_power_requirements`](#pudl.transform.rus7._core_rus7__yearly_power_requirements)(→ pandas.DataFrame)                        | Transform the power requirements table.                               |
| [`_core_rus7__yearly_investments`](#pudl.transform.rus7._core_rus7__yearly_investments)(→ pandas.DataFrame)                                      | Transform the investments table.                                      |
| [`_core_rus7__yearly_long_term_debt`](#pudl.transform.rus7._core_rus7__yearly_long_term_debt)(→ pandas.DataFrame)                                | Transform the core_rus7_\_yearly_investments table.                   |
| [`_core_rus7__yearly_patronage_capital`](#pudl.transform.rus7._core_rus7__yearly_patronage_capital)(→ pandas.DataFrame)                          | Transform the patronage capital table.                                |
| [`_core_rus7__yearly_statement_of_operations`](#pudl.transform.rus7._core_rus7__yearly_statement_of_operations)(...)                             | Transform the statement of operations table.                          |
| [`_core_rus7__consumer_debt`](#pudl.transform.rus7._core_rus7__consumer_debt)(raw_rus7_\_owed_by_customers)                                      | Transform the owed by consumer table.                                 |
| [`_core_rus7__yearly_service_interruptions`](#pudl.transform.rus7._core_rus7__yearly_service_interruptions)(...)                                 | Transform the service_interruptions table.                            |
| [`_core_rus7__transmission_and_distribution`](#pudl.transform.rus7._core_rus7__transmission_and_distribution)(...)                               | Transform the transmission_and_distribution table.                    |
| [`_core_rus7__yearly_long_term_leases`](#pudl.transform.rus7._core_rus7__yearly_long_term_leases)(→ pandas.DataFrame)                            | Transform the long term leases table.                                 |
| [`_core_rus7__yearly_loans`](#pudl.transform.rus7._core_rus7__yearly_loans)(→ pandas.DataFrame)                                                  | Transform the raw_rus7_\_loans and raw_rus7_\_loan_guarantees tables. |
| [`_core_rus7__yearly_external_financial_risk_ratio`](#pudl.transform.rus7._core_rus7__yearly_external_financial_risk_ratio)(...)                 | Transform the raw_rus7_\_external_financial_risk_ratio table.         |
| [`_core_rus7__yearly_energy_purchased`](#pudl.transform.rus7._core_rus7__yearly_energy_purchased)(→ pandas.DataFrame)                            | Transform the raw_rus7_\_energy_purchased table.                      |
| [`_core_rus7__yearly_materials_and_supplies`](#pudl.transform.rus7._core_rus7__yearly_materials_and_supplies)(...)                               | Transform the materials and supplies table.                           |
| [`_core_rus7__yearly_utility_plant_changes`](#pudl.transform.rus7._core_rus7__yearly_utility_plant_changes)(...)                                 | Transform the utility plant changes table.                            |
| [`core_rus7__entity_borrowers`](#pudl.transform.rus7.core_rus7__entity_borrowers)(context, \*\*clean_dfs)                                        | Harvesting IDs & consistent static attributes for RUS7 entity.        |

## Module Contents

### pudl.transform.rus7.logger

### pudl.transform.rus7.\_core_rus7_\_yearly_meeting_and_board(raw_rus7_\_meeting_and_board)

Transform the meeting and board (aka governance) table.

### pudl.transform.rus7.\_core_rus7_\_yearly_balance_sheet_assets(raw_rus7_\_balance_sheet)

Transform the balance sheet assets table.

### pudl.transform.rus7.\_core_rus7_\_yearly_balance_sheet_liabilities(raw_rus7_\_balance_sheet)

Transform the balance sheet liabilities table.

### pudl.transform.rus7.\_core_rus7_\_scd_borrowers(raw_rus7_\_borrowers)

Transform the borrowers table.

### pudl.transform.rus7.\_core_rus7_\_yearly_employee_statistics(raw_rus7_\_employee_statistics)

Transform the employee statistics table.

### pudl.transform.rus7.\_core_rus7_\_yearly_energy_efficiency(raw_rus7_\_energy_efficiency)

Transform the energy efficiency table.

### pudl.transform.rus7.\_core_rus7_\_power_requirements(raw_rus7_\_power_requirements)

Early transform an internal power_requirements table.

This main input gets used serval times in several downstream
`core_rus7__yearly_power_requirements*` assets. The raw asset needs some
cleaning and dropping of duplicate records so we do it once.

### pudl.transform.rus7.\_core_rus7_\_yearly_power_requirements_electric_sales(\_core_rus7_\_power_requirements: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the power requirements of electric sales table.

The resulting table is a portion of the power_requirements tables, which
pertains to the sales and revenue of electricity.

### pudl.transform.rus7.\_core_rus7_\_yearly_power_requirements_electric_customers(\_core_rus7_\_power_requirements: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the power requirements of electric customers table.

The resulting table is a portion of the power_requirements tables, which
pertains to the number of customers in different customer classes.

### pudl.transform.rus7.\_core_rus7_\_yearly_power_requirements(\_core_rus7_\_power_requirements: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the power requirements table.

The resulting table is a portion of the power_requirements tables, which
pertains to the revenue from several portions of the borrower’s business as well
as several types of electricity generated, purchased or used.

### pudl.transform.rus7.\_core_rus7_\_yearly_investments(raw_rus7_\_investments: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the investments table.

### pudl.transform.rus7.\_core_rus7_\_yearly_long_term_debt(raw_rus7_\_long_term_debt: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the core_rus7_\_yearly_investments table.

### pudl.transform.rus7.\_core_rus7_\_yearly_patronage_capital(raw_rus7_\_patronage_capital: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the patronage capital table.

### pudl.transform.rus7.\_core_rus7_\_yearly_statement_of_operations(raw_rus7_\_statement_of_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the statement of operations table.

### pudl.transform.rus7.\_core_rus7_\_consumer_debt(raw_rus7_\_owed_by_customers: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transform the owed by consumer table.

This transform splits the owed_by_consumers table into one table describing general
consumer debts and one table describing the status of energy efficiency and
conservation loan program debts.

### pudl.transform.rus7.\_core_rus7_\_yearly_service_interruptions(raw_rus7_\_service_interruptions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the service_interruptions table.

### pudl.transform.rus7.\_core_rus7_\_transmission_and_distribution(raw_rus7_\_transmission_and_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transform the transmission_and_distribution table.

### pudl.transform.rus7.\_core_rus7_\_yearly_long_term_leases(raw_rus7_\_long_term_leases: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the long term leases table.

### pudl.transform.rus7.\_core_rus7_\_yearly_loans(raw_rus7_\_loan_guarantees: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_rus7_\_loans: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus7_\_loans and raw_rus7_\_loan_guarantees tables.

### pudl.transform.rus7.\_core_rus7_\_yearly_external_financial_risk_ratio(raw_rus7_\_external_financial_risk_ratio: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus7_\_external_financial_risk_ratio table.

### pudl.transform.rus7.\_core_rus7_\_yearly_energy_purchased(raw_rus7_\_energy_purchased: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the raw_rus7_\_energy_purchased table.

### pudl.transform.rus7.\_core_rus7_\_yearly_materials_and_supplies(raw_rus7_\_materials_and_supplies: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the materials and supplies table.

### pudl.transform.rus7.\_core_rus7_\_yearly_utility_plant_changes(raw_rus7_\_utility_plant_changes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transform the utility plant changes table.

### pudl.transform.rus7.\_CORE_RUS7_TABLES

### pudl.transform.rus7.core_rus7_\_entity_borrowers(context, \*\*clean_dfs)

Harvesting IDs & consistent static attributes for RUS7 entity.

### pudl.transform.rus7.finished_rus_assets
