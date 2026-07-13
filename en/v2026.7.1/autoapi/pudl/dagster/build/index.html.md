# pudl.dagster.build

Assembles Dagster Definitions from default values and specified overrides.

Define helpers here that compose those building blocks into a code location, especially
when tests, CLI entrypoints, or specialized environments need to override part of the
default assembly. Avoid putting asset or resource implementations here.

For the underlying Dagster concept, see
[https://docs.dagster.io/getting-started/concepts#definitions](https://docs.dagster.io/getting-started/concepts#definitions)

## Functions

| [`build_defs`](#pudl.dagster.build.build_defs)(→ dagster.Definitions)                         | Build a fresh PUDL `Definitions` object with optional overrides.           |
|-----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| [`build_interactive_defs`](#pudl.dagster.build.build_interactive_defs)(→ dagster.Definitions) | Build defs for interactive in-process use with concrete default resources. |

## Module Contents

### pudl.dagster.build.build_defs(, resource_overrides: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, asset_overrides: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, asset_check_overrides: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[dagster.AssetChecksDefinition](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetChecksDefinition)] | [None](https://docs.python.org/3/library/constants.html#None) = None, job_overrides: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, sensor_overrides: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[dagster.SensorDefinition](https://docs.dagster.io/api/dagster/schedules-sensors/#dagster.SensorDefinition)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.Definitions](https://docs.dagster.io/api/dagster/definitions/#dagster.Definitions)

Build a fresh PUDL `Definitions` object with optional overrides.

Note that resource_overrides are used to update the default resources, while all
other overrides replace the defaults entirely.

### pudl.dagster.build.build_interactive_defs(, global_data_config_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, pudl_input: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, pudl_output: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, zenodo_dois_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.Definitions](https://docs.dagster.io/api/dagster/definitions/#dagster.Definitions)

Build defs for interactive in-process use with concrete default resources.

Dagster’s asset value loader does not resolve the FERC SQLite IO managers when
they reference partially configured nested resources, which happens when you’re
trying to load assets outside of a dg-spawned environment. So, in notebooks,
REPLs, and local scripts, we need to explicitly construct the FERC IO managers.
