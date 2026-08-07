# pudl.workspace.setup

Tools for setting up and managing PUDL workspaces.

## Attributes

| [`logger`](#pudl.workspace.setup.logger)   |    |
|--------------------------------------------|----|

## Classes

| [`PudlPaths`](#pudl.workspace.setup.PudlPaths)   | These settings provide access to various PUDL directories.   |
|--------------------------------------------------|--------------------------------------------------------------|

## Module Contents

### pudl.workspace.setup.logger

### *class* pudl.workspace.setup.PudlPaths(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`pydantic_settings.BaseSettings`](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.BaseSettings)

These settings provide access to various PUDL directories.

It is primarily configured via PUDL_INPUT and PUDL_OUTPUT environment
variables. Other paths of relevance are derived from these.

#### pudl_input *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### pudl_output *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### *classmethod* normalize_paths(value: Any) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Normalize configured paths to absolute `Path` objects.

#### create_directories()

Create PUDL input and output directories if they don’t already exist.

#### *property* pudl_db *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Returns url of locally stored pudl sqlite database.

#### sqlite_db_uri(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns url of locally stored pudl sqlite database with given name.

The name is expected to be the name of the database without the .sqlite
suffix. E.g. pudl, ferc1 and so on.

#### parquet_path(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Return path to parquet file for given database and table.

#### sqlite_db_path(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Return path to locally stored SQLite DB file.

#### duckdb_db_path(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Return path to locally stored SQLite DB file.

#### output_file(filename: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Path to file in PUDL output directory.

#### *static* set_path_overrides(input_dir: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, output_dir: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [None](https://docs.python.org/3/library/constants.html#None)

Set PUDL_INPUT and/or PUDL_OUTPUT env variables.

* **Parameters:**
  * **input_dir** – if set, overrides PUDL_INPUT env variable.
  * **output_dir** – if set, overrides PUDL_OUTPUT env variable.
