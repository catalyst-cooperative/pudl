# pudl.transform.ferc

Module for shared helpers for FERC Form transforms.

## Attributes

| [`logger`](#pudl.transform.ferc.logger)   |    |
|-------------------------------------------|----|

## Functions

| [`__apply_diffs`](#pudl.transform.ferc.__apply_diffs)(→ pandas.DataFrame)                                   | Take the latest reported non-null value for each group.             |
|-------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| [`__best_snapshot`](#pudl.transform.ferc.__best_snapshot)(→ pandas.DataFrame)                               | Take the row that has most non-null values out of each group.       |
| [`__compare_dedupe_methodologies`](#pudl.transform.ferc.__compare_dedupe_methodologies)(applied_diffs, ...) | Compare deduplication methodologies.                                |
| [`filter_for_freshest_data_xbrl`](#pudl.transform.ferc.filter_for_freshest_data_xbrl)(→ pandas.DataFrame)   | Get most updated values for each XBRL context.                      |
| [`get_primary_key_raw_xbrl`](#pudl.transform.ferc.get_primary_key_raw_xbrl)(→ list[str])                    | Get the primary key for a raw XBRL table from the XBRL datapackage. |

## Module Contents

### pudl.transform.ferc.logger

### pudl.transform.ferc.\_\_apply_diffs(duped_groups: pandas.core.groupby.DataFrameGroupBy) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Take the latest reported non-null value for each group.

### pudl.transform.ferc.\_\_best_snapshot(duped_groups: pandas.core.groupby.DataFrameGroupBy) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Take the row that has most non-null values out of each group.

### pudl.transform.ferc.\_\_compare_dedupe_methodologies(applied_diffs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), best_snapshot: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), xbrl_context_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)])

Compare deduplication methodologies.

By cross-referencing these we can make sure that the apply-diff
methodology isn’t doing something unexpected.

The main things we want to keep tabs on are: whether apply-diff is
adding more than expected differences compared to best-snapshot and
whether or not apply-diff is giving us more values than best-snapshot.

### pudl.transform.ferc.filter_for_freshest_data_xbrl(xbrl_table: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), primary_keys: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], compare_methods: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get most updated values for each XBRL context.

An XBRL context includes an entity ID, the time period the data applies to, and
other dimensions such as utility type. Each context has its own ID, but they are
frequently redefined with the same contents but different IDs - so we identify
them by their actual content.

Each row in our SQLite database includes all the facts for one context/filing
pair.

If one context is represented in multiple filings, we take the most
recently-reported non-null value.

This means that if a utility reports a non-null value, then later
either reports a null value for it or simply omits it from the report,
we keep the old non-null value, which may be erroneous. This appears to
be fairly rare, affecting < 0.005% of reported values.

### pudl.transform.ferc.get_primary_key_raw_xbrl(sched_table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), ferc_form: Literal['ferc1', 'ferc714'], pudl_paths: [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Get the primary key for a raw XBRL table from the XBRL datapackage.

If the sched_table_name does not exist in the datapackage, an empty list
will be returned. This is expected often because we attempt to grab both
the instant and the duration raw tables, despite those not always
existing in the original data.
