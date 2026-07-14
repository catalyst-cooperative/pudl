# pudl.scripts.pudl_datastore

CLI for managing raw data inputs to the PUDL data processing pipeline.

## Attributes

| [`_KNOWN_DATASETS`](#pudl.scripts.pudl_datastore._KNOWN_DATASETS)   |    |
|---------------------------------------------------------------------|----|

## Functions

| [`_print_partitions`](#pudl.scripts.pudl_datastore._print_partitions)(→ None)           | Print known partition keys and values for each of the datasets.   |
|-----------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| [`_parse_key_values`](#pudl.scripts.pudl_datastore._parse_key_values)(→ dict[str, str]) | Parse key-value pairs into a Python dictionary.                   |
| [`main`](#pudl.scripts.pudl_datastore.main)(→ int)                                      | Manage the raw data inputs to the PUDL data processing pipeline.  |

## Module Contents

### pudl.scripts.pudl_datastore.\_KNOWN_DATASETS

### pudl.scripts.pudl_datastore.\_print_partitions(dstore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), datasets: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [None](https://docs.python.org/3/library/constants.html#None)

Print known partition keys and values for each of the datasets.

### pudl.scripts.pudl_datastore.\_parse_key_values(ctx: click.core.Context, param: click.Option, values: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Parse key-value pairs into a Python dictionary.

Transforms a command line argument of the form: k1=v1,k2=v2,k3=v3…
into: {k1:v1, k2:v2, k3:v3, …}

### pudl.scripts.pudl_datastore.main(datasets: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis], all_datasets: [bool](https://docs.python.org/3/library/functions.html#bool), validate: [bool](https://docs.python.org/3/library/functions.html#bool), list_partitions: [bool](https://docs.python.org/3/library/functions.html#bool), partition: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int) | [str](https://docs.python.org/3/library/stdtypes.html#str)], cloud_cache_path: [str](https://docs.python.org/3/library/stdtypes.html#str), bypass_local_cache: [bool](https://docs.python.org/3/library/functions.html#bool), logfile: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), loglevel: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [int](https://docs.python.org/3/library/functions.html#int)

Manage the raw data inputs to the PUDL data processing pipeline.

Download the raw FERC Form 2 data:

pudl_datastore ferc2

Download the raw FERC Form 2 data only for 2021:

pudl_datastore ferc2 –partition year=2021

Re-download the raw FERC Form 2 data for 2021 even if you already have it:

pudl_datastore ferc2 –partition year=2021 –bypass-local-cache

Validate all California EPA CEMS data in the local datastore:

pudl_datastore epacems –validate –partition state=ca

List the available partitions in the EIA-860 and EIA-923 datasets:

pudl_datastore eia860 eia923 –list-partitions

Download all known datasets (e.g. in automation):

pudl_datastore –all
