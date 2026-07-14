# pudl.output.rus

Create output tables for RUS-7 and RUS-12.

## Attributes

| [`out_rus7_assets`](#pudl.output.rus.out_rus7_assets)   |    |
|---------------------------------------------------------|----|
| [`out_rus12_assets`](#pudl.output.rus.out_rus12_assets) |    |

## Functions

| [`out_rus_asset_factory`](#pudl.output.rus.out_rus_asset_factory)(→ dagster.AssetsDefinition)   | An asset factory for finished RUS output tables.   |
|-------------------------------------------------------------------------------------------------|----------------------------------------------------|

## Module Contents

### pudl.output.rus.out_rus_asset_factory(core_table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), borrower_table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

An asset factory for finished RUS output tables.

* **Parameters:**
  * **core_table_name** – the name of the core table.
  * **borrower_table_name** – the name of the borrower table which we
    want to merge onto the core table.
  * **io_manager_key** – the name of the IO Manager of the final asset.
* **Returns:**
  A RUS output asset.

### pudl.output.rus.out_rus7_assets

### pudl.output.rus.out_rus12_assets
