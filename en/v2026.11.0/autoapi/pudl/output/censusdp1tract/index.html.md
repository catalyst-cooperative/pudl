# pudl.output.censusdp1tract

Functions for reading data out of the Census DP1 SQLite Database.

## Attributes

| [`logger`](#pudl.output.censusdp1tract.logger)            |    |
|--------------------------------------------------------------------|----|
| [`CensusLayer`](#pudl.output.censusdp1tract.CensusLayer)       |    |
| [`census_dp1_layers`](#pudl.output.censusdp1tract.census_dp1_layers) |    |

## Classes

| [`LayerParams`](#pudl.output.censusdp1tract.LayerParams)   | Simple class defining the expected structure of the layer processing params.   |
|----------------------------------------------------------------|--------------------------------------------------------------------------------|

## Functions

| [`census_asset_factory`](#pudl.output.censusdp1tract.census_asset_factory)(layer) → dagster.AssetsDefinition   | An asset factory for finished EIA tables.   |
|-----------------------------------------------------------------------------------------------------------|---------------------------------------------|

## Module Contents

### pudl.output.censusdp1tract.logger

### *type* pudl.output.censusdp1tract.CensusLayer *= Literal['state', 'county', 'tract']*

### *class* pudl.output.censusdp1tract.LayerParams

Bases: `TypedDict`

Simple class defining the expected structure of the layer processing params.

#### plural *: [str](https://docs.python.org/3/builtins/stdtypes.html#str)*

#### rename *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]*

### pudl.output.censusdp1tract.census_asset_factory(layer: [CensusLayer](#pudl.output.censusdp1tract.CensusLayer)) → [dagster.AssetsDefinition](https://dagster.io/docs/api/dagster/assets/#dagster.AssetsDefinition)

An asset factory for finished EIA tables.

### pudl.output.censusdp1tract.census_dp1_layers
