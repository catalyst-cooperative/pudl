# pudl.transform.rus

Code for transforming RUS data that pertains to more than one RUS Form.

## Attributes

| [`logger`](#pudl.transform.rus.logger)   |    |
|------------------------------------------|----|

## Classes

| [`RusEntity`](#pudl.transform.rus.RusEntity)   | Enum for the different types of RUS entities.   |
|------------------------------------------------|-------------------------------------------------|

## Functions

| [`early_check_pk`](#pudl.transform.rus.early_check_pk)(→ None)                                             | Check the expected primary key of the table.                    |
|------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| [`early_transform`](#pudl.transform.rus.early_transform)(→ pandas.DataFrame)                               | Standard transforms for raw RUS data.                           |
| [`convert_units`](#pudl.transform.rus.convert_units)(→ pandas.DataFrame)                                   | Convert units within a column and rename column with new units. |
| [`finished_rus_asset_factory`](#pudl.transform.rus.finished_rus_asset_factory)(→ dagster.AssetsDefinition) | An asset factory for finished RUS tables.                       |

## Module Contents

### pudl.transform.rus.logger

### pudl.transform.rus.early_check_pk(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pk_early: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = ['report_date', 'borrower_id_rus'], raise_fail=True) → [None](https://docs.python.org/3/library/constants.html#None)

Check the expected primary key of the table.

By default the expected primary key is [“report_date”, “borrower_id_rus”].

### pudl.transform.rus.early_transform(raw_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), boolean_columns_to_fix=[], string_cols_to_simplify=[]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Standard transforms for raw RUS data.

### pudl.transform.rus.convert_units(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), old_unit: [str](https://docs.python.org/3/library/stdtypes.html#str), new_unit: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), converter: [float](https://docs.python.org/3/library/functions.html#float) | [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert units within a column and rename column with new units.

This function assumes that the old units are suffixes in the snake-cased column
names, separated by an underscore.

Ex: if you want to convert from kWh’s to MWh’s the df must have column names like
`electric_sales_kwh` or `purchased_kwh`, the old unit would be `kwh`, the new
unit would be `mwh` and the converter would be `0.001`.

* **Parameters:**
  * **df** – data table with units you’d like to convert.
  * **old_unit** – the unit in the df. This must be the suffix of the column
    names you’d like to convert.
  * **new_unit** – the new unit label you want as the new suffix of the resulting
    dataframe. If you want no new unit added, this value can be None or an
    empty string ()””).
  * **converter** – the float or integer you need to multiply the old values by to
    convert the units.

### *class* pudl.transform.rus.RusEntity

Bases: [`enum.StrEnum`](https://docs.python.org/3/library/enum.html#enum.StrEnum)

Enum for the different types of RUS entities.

#### BORROWERS

### pudl.transform.rus.finished_rus_asset_factory(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), \_core_table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

An asset factory for finished RUS tables.

* **Parameters:**
  * **table_name** – the name of the core table.
  * **\_core_table_name** – the name of the unharvested input table
  * **io_manager_key** – the name of the IO Manager of the final asset.
* **Returns:**
  A RUS asset.
