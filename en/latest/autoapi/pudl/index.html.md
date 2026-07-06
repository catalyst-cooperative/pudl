# pudl

The Public Utility Data Liberation (PUDL) Project.

## Submodules

* [pudl._version](_version/index.md)
* [pudl.analysis](analysis/index.md)
* [pudl.dagster](dagster/index.md)
* [pudl.dbt_schema](dbt_schema/index.md)
* [pudl.definitions](definitions/index.md)
* [pudl.deploy](deploy/index.md)
* [pudl.extract](extract/index.md)
* [pudl.glue](glue/index.md)
* [pudl.helpers](helpers/index.md)
* [pudl.logging_helpers](logging_helpers/index.md)
* [pudl.metadata](metadata/index.md)
* [pudl.output](output/index.md)
* [pudl.package_data](package_data/index.md)
* [pudl.scripts](scripts/index.md)
* [pudl.settings](settings/index.md)
* [pudl.transform](transform/index.md)
* [pudl.validate](validate/index.md)
* [pudl.workspace](workspace/index.md)

## Attributes

| [`PUDL_ROOT_PATH`](#pudl.PUDL_ROOT_PATH)                               | Resolved absolute path to the repository root.        |
|------------------------------------------------------------------------|-------------------------------------------------------|
| [`PUDL_SETTINGS_PATH`](#pudl.PUDL_SETTINGS_PATH)                       | Resolved absolute path to the package_data directory. |
| [`PUDL_DBT_PATH`](#pudl.PUDL_DBT_PATH)                                 | Resolved absolute path to the dbt directory.          |
| [`PUDL_DOCS_PATH`](#pudl.PUDL_DOCS_PATH)                               | Resolved absolute path to the docs directory.         |
| [`PUDL_NIGHTLY_BUILDS_BASE_PATH`](#pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH) | Base path to PUDL nightly builds outputs.             |
| [`PUDL_EEL_HOLE_BASE_PATH`](#pudl.PUDL_EEL_HOLE_BASE_PATH)             | Base path to eel-hole s3 outputs.                     |
| [`__author__`](#pudl.__author__)                                       |                                                       |
| [`__contact__`](#pudl.__contact__)                                     |                                                       |
| [`__maintainer__`](#pudl.__maintainer__)                               |                                                       |
| [`__license__`](#pudl.__license__)                                     |                                                       |
| [`__maintainer_email__`](#pudl.__maintainer_email__)                   |                                                       |
| [`__version__`](#pudl.__version__)                                     |                                                       |
| [`__docformat__`](#pudl.__docformat__)                                 |                                                       |
| [`__description__`](#pudl.__description__)                             |                                                       |
| [`__long_description__`](#pudl.__long_description__)                   |                                                       |
| [`__projecturl__`](#pudl.__projecturl__)                               |                                                       |
| [`__downloadurl__`](#pudl.__downloadurl__)                             |                                                       |

## Functions

| [`configure_root_logger`](#pudl.configure_root_logger)(→ None)   | Configure the root catalystcoop logger.   |
|------------------------------------------------------------------|-------------------------------------------|

## Package Contents

### pudl.configure_root_logger(logfile: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, loglevel: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] = 'INFO', dependency_loglevels: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)] | [None](https://docs.python.org/3/library/constants.html#None) = None, color_logs: [bool](https://docs.python.org/3/library/functions.html#bool) = True, propagate: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [None](https://docs.python.org/3/library/constants.html#None)

Configure the root catalystcoop logger.

* **Parameters:**
  * **logfile** – Path to logfile or None.
  * **loglevel** – Level of detail at which to log. Defaults to `INFO`.
  * **dependency_loglevels** – Dictionary mapping dependency name to desired loglevel.
    This allows us to filter excessive logs from dependencies.
  * **color_logs** – Whether to emit ANSI color codes. Defaults to `True`.
  * **propagate** – Whether to propagate logs to ancestor loggers. Useful for ensuring
    that pytest has access to PUDL logs during testing.

### pudl.PUDL_ROOT_PATH *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Resolved absolute path to the repository root.

### pudl.PUDL_SETTINGS_PATH *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Resolved absolute path to the package_data directory.

### pudl.PUDL_DBT_PATH *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Resolved absolute path to the dbt directory.

### pudl.PUDL_DOCS_PATH *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Resolved absolute path to the docs directory.

### pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH *: upath.UPath*

Base path to PUDL nightly builds outputs.

### pudl.PUDL_EEL_HOLE_BASE_PATH *: upath.UPath*

Base path to eel-hole s3 outputs.

### pudl.\_\_author_\_ *= 'Catalyst Cooperative'*

### pudl.\_\_contact_\_ *= 'pudl@catalyst.coop'*

### pudl.\_\_maintainer_\_ *= 'Catalyst Cooperative'*

### pudl.\_\_license_\_ *= 'MIT License'*

### pudl.\_\_maintainer_email_\_ *= 'zane.selvans@catalyst.coop'*

### pudl.\_\_version_\_ *= '0.0.0.dev0'*

### pudl.\_\_docformat_\_ *= 'restructuredtext en'*

### pudl.\_\_description_\_ *= 'Tools for liberating public US electric utility data.'*

### pudl.\_\_long_description_\_ *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""
This Public Utility Data Liberation (PUDL) project is a collection of tools
that allow programmatic access to and manipulation of many public data sets
related to electric utilities in the United States. These data sets are
often collected by state and federal agencies, but are publicized in ways
that are not well standardized, or intended for interoperability. PUDL
seeks to allow more transparent and useful access to this important public
data, with the goal of enabling climate advocates, academic researchers, and
data journalists to better understand the electricity system and its impacts
on climate.
"""
```

</details>

### pudl.\_\_projecturl_\_ *= 'https://catalyst.coop/pudl/'*

### pudl.\_\_downloadurl_\_ *= 'https://github.com/catalyst-cooperative/pudl/'*
