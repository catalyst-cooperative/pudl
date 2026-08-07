# pudl.dagster.assets.core.eiaapi_electricity

Dagster assets for EIA API electricity aggregates.

This module defines asset logic for the aggregate EIA electricity data products that
PUDL derives from the archived EIA API electricity JSON data and then loads into the
core asset graph.

## Attributes

| [`logger`](#pudl.dagster.assets.core.eiaapi_electricity.logger)   |    |
|-------------------------------------------------------------------|----|

## Functions

| [`core_eia__yearly_fuel_receipts_costs_aggs`](#pudl.dagster.assets.core.eiaapi_electricity.core_eia__yearly_fuel_receipts_costs_aggs)(context)   | Extract and transform EIA API electricity aggregates.   |
|--------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|

## Module Contents

### pudl.dagster.assets.core.eiaapi_electricity.logger

### pudl.dagster.assets.core.eiaapi_electricity.core_eia_\_yearly_fuel_receipts_costs_aggs(context)

Extract and transform EIA API electricity aggregates.

* **Returns:**
  A dictionary of DataFrames whose keys are the names of the corresponding
  database table.
