# pudl.output.eiaapi

Interim and output tables derived from the EIA Bulk Electricity data.

## Attributes

| [`logger`](#pudl.output.eiaapi.logger)   |    |
|------------------------------------------|----|

## Functions

| [`_out_eia__monthly_state_fuel_prices`](#pudl.output.eiaapi._out_eia__monthly_state_fuel_prices)(→ pandas.DataFrame)   | Get state-level average fuel costs from EIA's bulk electricity data.   |
|------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|

## Module Contents

### pudl.output.eiaapi.logger

### pudl.output.eiaapi.\_out_eia_\_monthly_state_fuel_prices(core_eia_\_yearly_fuel_receipts_costs_aggs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get state-level average fuel costs from EIA’s bulk electricity data.

This data is used to fill in missing fuel prices in the
[core_eia923_\_fuel_receipts_costs](../../../../data_dictionaries/pudl_db.md#core-eia923-fuel-receipts-costs) table. It was created as a drop-in replacement
for data we were previously obtaining from EIA’s unreliable API.
