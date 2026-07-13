# pudl.scripts.dghome

Manage UUID-named directories in a Dagster storage directory.

This CLI helps manage the Dagster storage directory to reclaim disk space by
listing and removing run artifacts (represented as UUID-named directories)
based on modification date.

### Examples

List all UUID-named directories:
: dghome ls

List directories modified on or before 2026-01-15:
: dghome ls 2026-01-15

List directories modified 10 days or more ago:
: dghome ls 10d

Remove directories modified 1 month or more ago:
: dghome rm 1m

## Attributes

| [`UUID_PATTERN`](#pudl.scripts.dghome.UUID_PATTERN)                 |    |
|---------------------------------------------------------------------|----|
| [`_DATE_FORMATS_EPILOG`](#pudl.scripts.dghome._DATE_FORMATS_EPILOG) |    |

## Functions

| [`_parse_date`](#pudl.scripts.dghome._parse_date)(→ datetime.datetime)        | Parse a cutoff datetime from one of several formats.                         |
|-------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| [`_human_readable`](#pudl.scripts.dghome._human_readable)(→ str)              | Format a kilobyte count as a human-readable string (e.g. 1.2M, 3.4G).        |
| [`_row_color`](#pudl.scripts.dghome._row_color)(→ str | tuple[int, int, int]) | Get the right color of output for the filesize.                              |
| [`_collect`](#pudl.scripts.dghome._collect)(→ list[dict])                     | Return UUID dirs from $DAGSTER_HOME/storage, filtered to mtime <= cutoff_ts. |
| [`dghome`](#pudl.scripts.dghome.dghome)(→ None)                               | Manage UUID-named directories in Dagster storage.                            |
| [`ls`](#pudl.scripts.dghome.ls)(→ None)                                       | List UUID directories, optionally filtered by modification date.             |
| [`rm`](#pudl.scripts.dghome.rm)(→ None)                                       | Remove UUID directories last modified on or before DATE.                     |
| [`main`](#pudl.scripts.dghome.main)(→ None)                                   | Entry point for the dghome CLI.                                              |

## Module Contents

### pudl.scripts.dghome.UUID_PATTERN

### pudl.scripts.dghome.\_parse_date(date_str: [str](https://docs.python.org/3/library/stdtypes.html#str), now: [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)

Parse a cutoff datetime from one of several formats.

Accepts any absolute time string that dateutil can handle, plus relative
time strings like “<N>d/w/m” (for days/weeks/months).

### pudl.scripts.dghome.\_human_readable(kb: [int](https://docs.python.org/3/library/functions.html#int)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Format a kilobyte count as a human-readable string (e.g. 1.2M, 3.4G).

### pudl.scripts.dghome.\_row_color(size_kb: [int](https://docs.python.org/3/library/functions.html#int)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[int](https://docs.python.org/3/library/functions.html#int), [int](https://docs.python.org/3/library/functions.html#int), [int](https://docs.python.org/3/library/functions.html#int)]

Get the right color of output for the filesize.

< 100 MB: green
<   1 GB: yellow
<   5 GB: orange
else: red

### pudl.scripts.dghome.\_collect(cutoff_ts: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]

Return UUID dirs from $DAGSTER_HOME/storage, filtered to mtime <= cutoff_ts.

Raises click.ClickException if DAGSTER_HOME is unset or the storage dir is missing.
Results are sorted by modification time (oldest first).

### pudl.scripts.dghome.dghome() → [None](https://docs.python.org/3/library/constants.html#None)

Manage UUID-named directories in Dagster storage.

### pudl.scripts.dghome.\_DATE_FORMATS_EPILOG *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""
Accepts any absolute time string that dateutil can handle, plus relative time strings like '<N>d/w/m' (for days/weeks/months)."""
```

</details>

### pudl.scripts.dghome.ls(date: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [None](https://docs.python.org/3/library/constants.html#None)

List UUID directories, optionally filtered by modification date.

Without DATE, lists all directories. With DATE, lists only those
last modified on or before DATE.

### pudl.scripts.dghome.rm(date: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [None](https://docs.python.org/3/library/constants.html#None)

Remove UUID directories last modified on or before DATE.

Without DATE, reports what would be removed but does nothing.

Confirms before deletion.

### pudl.scripts.dghome.main() → [None](https://docs.python.org/3/library/constants.html#None)

Entry point for the dghome CLI.
