# pudl.output.censusdp1tract

Functions for reading data out of the Census DP1 SQLite Database.

## Attributes

| [`logger`](#pudl.output.censusdp1tract.logger)                       |    |
|----------------------------------------------------------------------|----|
| [`census_dp1_layers`](#pudl.output.censusdp1tract.census_dp1_layers) |    |

## Classes

| [`LayerParams`](#pudl.output.censusdp1tract.LayerParams)   | Simple class defining the expected structure of the layer processing params.   |
|------------------------------------------------------------|--------------------------------------------------------------------------------|

## Functions

| [`census_asset_factory`](#pudl.output.censusdp1tract.census_asset_factory)(→ dagster.AssetsDefinition)   | An asset factory for finished EIA tables.   |
|----------------------------------------------------------------------------------------------------------|---------------------------------------------|

## Module Contents

### pudl.output.censusdp1tract.logger

### *class* pudl.output.censusdp1tract.LayerParams

Bases: `TypedDict`

Simple class defining the expected structure of the layer processing params.

#### plural *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### rename *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.output.censusdp1tract.census_asset_factory(layer: Literal['state', 'county', 'tract']) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

An asset factory for finished EIA tables.

### pudl.output.censusdp1tract.census_dp1_layers
