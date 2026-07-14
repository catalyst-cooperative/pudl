# pudl.output.eia923

Denormalized, aggregated, and filled versions of the basic EIA-923 tables.

## Attributes

| [`logger`](#pudl.output.eia923.logger)                                                       |    |
|----------------------------------------------------------------------------------------------|----|
| [`FIRST_COLS`](#pudl.output.eia923.FIRST_COLS)                                               |    |
| [`generation_fuel_agg_eia923_assets`](#pudl.output.eia923.generation_fuel_agg_eia923_assets) |    |

## Functions

| [`denorm_by_plant`](#pudl.output.eia923.denorm_by_plant)(→ pandas.DataFrame)                                           | Denormalize a table that is reported on a per-plant basis.                                                                           |
|------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [`denorm_by_gen`](#pudl.output.eia923.denorm_by_gen)(→ pandas.DataFrame)                                               | Denormalize a table that is reported on a per-generator basis.                                                                       |
| [`denorm_by_boil`](#pudl.output.eia923.denorm_by_boil)(→ pandas.DataFrame)                                             | Denormalize a table that is reported on a per-boiler basis.                                                                          |
| [`_fill_fuel_costs_by_state`](#pudl.output.eia923._fill_fuel_costs_by_state)(→ pandas.DataFrame)                       | Fill in missing fuel costs with state-level averages.                                                                                |
| [`drop_ytd_for_annual_tables`](#pudl.output.eia923.drop_ytd_for_annual_tables)(→ pandas.DataFrame)                     | Drop records in annual tables where data_maturity is incremental_ytd.                                                                |
| [`out_eia923__generation`](#pudl.output.eia923.out_eia923__generation)(→ pandas.DataFrame)                             | Denormalize the [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-generation) table.   |
| [`out_eia923__generation_fuel_combined`](#pudl.output.eia923.out_eia923__generation_fuel_combined)(→ pandas.DataFrame) | Denormalize the generation_fuel_combined_eia923 table.                                                                               |
| [`out_eia923__boiler_fuel`](#pudl.output.eia923.out_eia923__boiler_fuel)(→ pandas.DataFrame)                           | Denormalize the [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel) table. |
| [`out_eia923__fuel_receipts_costs`](#pudl.output.eia923.out_eia923__fuel_receipts_costs)(→ pandas.DataFrame)           | Denormalize the [core_eia923_\_fuel_receipts_costs](../../../../data_dictionaries/pudl_db.md#core-eia923-fuel-receipts-costs) table. |
| [`time_aggregated_eia923_asset_factory`](#pudl.output.eia923.time_aggregated_eia923_asset_factory)(...)                | Build EIA-923 asset definitions, aggregated by year or month.                                                                        |

## Module Contents

### pudl.output.eia923.logger

### pudl.output.eia923.FIRST_COLS *= ['report_date', 'plant_id_eia', 'plant_id_pudl', 'plant_name_eia', 'utility_id_eia',...*

### pudl.output.eia923.denorm_by_plant(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pu: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), first_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize a table that is reported on a per-plant basis.

### pudl.output.eia923.denorm_by_gen(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pu: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bga: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), first_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize a table that is reported on a per-generator basis.

### pudl.output.eia923.denorm_by_boil(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pu: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), bga: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), first_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize a table that is reported on a per-boiler basis.

### pudl.output.eia923.\_fill_fuel_costs_by_state(frc_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), fuel_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill in missing fuel costs with state-level averages.

### pudl.output.eia923.drop_ytd_for_annual_tables(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), freq: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Drop records in annual tables where data_maturity is incremental_ytd.

This avoids accidental aggregation errors due to sub-annually reported data.

* **Parameters:**
  * **df** – A pd.DataFrame that contains a data_maturity column and for
    which you want to drop values where data_maturity = incremental_ytd.
  * **freq** – either MS or YS to indicate the level of aggretation for a specific table.
* **Returns:**
  The same input pd.DataFrames but without any rows where
  : data_maturity = incremental_ytd.
* **Return type:**
  pd.DataFrame

### pudl.output.eia923.out_eia923_\_generation(core_eia923_\_monthly_generation: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia_\_plants_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_assn_boiler_generator: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize the [core_eia923_\_monthly_generation](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-generation) table.

### pudl.output.eia923.out_eia923_\_generation_fuel_combined(core_eia923_\_monthly_generation_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia923_\_monthly_generation_fuel_nuclear: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia_\_plants_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize the generation_fuel_combined_eia923 table.

This asset first combines the [core_eia923_\_monthly_generation_fuel](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-generation-fuel) and
[core_eia923_\_monthly_generation_fuel_nuclear](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-generation-fuel-nuclear) into a single table with a
uniform primary key (consolidating multiple nuclear unit IDs into a single plant
record) and then denormalizes it by merging in some addition plant and utility level
columns.

This table contains the records at their originally reported temporal resolution,
so it’s outside of [`time_aggregated_eia923_asset_factory()`](#pudl.output.eia923.time_aggregated_eia923_asset_factory).

### pudl.output.eia923.out_eia923_\_boiler_fuel(core_eia923_\_monthly_boiler_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia_\_plants_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_assn_boiler_generator: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize the [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel) table.

The total heat content is also calculated as it’s useful in its own right and
required later to calculate average heat content per unit of fuel.

### pudl.output.eia923.out_eia923_\_fuel_receipts_costs(context, core_eia923_\_fuel_receipts_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia923_\_entity_coalmine: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia_\_plants_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia_\_monthly_state_fuel_prices: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_entity_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalize the [core_eia923_\_fuel_receipts_costs](../../../../data_dictionaries/pudl_db.md#core-eia923-fuel-receipts-costs) table.

### pudl.output.eia923.time_aggregated_eia923_asset_factory(freq: Literal['YS', 'MS'], io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)]

Build EIA-923 asset definitions, aggregated by year or month.

### pudl.output.eia923.generation_fuel_agg_eia923_assets
