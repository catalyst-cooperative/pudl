# pudl.scripts.pudl_service_territories

CLI for compiling historical utility and balancing area service territory geometries.

## Functions

| [`main`](#pudl.scripts.pudl_service_territories.main)(→ int)   | Compile historical utility and balancing area service territory geometries.   |
|----------------------------------------------------------------|-------------------------------------------------------------------------------|

## Module Contents

### pudl.scripts.pudl_service_territories.main(entity_type: Literal['utility', 'balancing_authority'], dissolve: [bool](https://docs.python.org/3/library/functions.html#bool), output_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), limit_by_state: [bool](https://docs.python.org/3/library/functions.html#bool), years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], logfile: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), loglevel: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [int](https://docs.python.org/3/library/functions.html#int)

Compile historical utility and balancing area service territory geometries.

This script produces GeoParquet files describing the historical service territories
of utilities and balancing authorities based on data reported in the EIA Form 861
and county geometries from the US Census DP1 geodatabase.

See: [https://geoparquet.org/](https://geoparquet.org/) for more on the GeoParquet file format.

Usage examples:

pudl_service_territories –entity-type balancing_authority –dissolve –limit-by-state
pudl_service_territories –entity-type utility
