<a id="data-validation"></a>

# Data validation quickstart

## Setup

The `dbt/` directory contains the PUDL dbt project which manages our [data tests](https://docs.getdbt.com/docs/build/data-tests). dbt is part of the PUDL pixi
environment.

The data validation tests run on the Parquet outputs that are in your
`$PUDL_OUTPUT/parquet/` directory. It’s important that you ensure the outputs you’re
testing are actually the result of the code on your current branch, otherwise you may
be surprised when the data test passes locally but fails in CI or the nightly builds.

We have a script, [`pudl.scripts.dbt_helper`](../autoapi/pudl/scripts/dbt_helper/index.md#module-pudl.scripts.dbt_helper), to help with some common workflows.

## Running the data validation tests

`dbt_helper validate` runs the data validation tests.
It’s a wrapper around the official dbt tool, `dbt build` - see [Running dbt directly](data_validation_reference.md#dbt-build).

`dbt_helper validate` provides rich output when a test fails,
and allows us to use the [Dagster asset selection syntax](https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference).

Example usage:

```bash
# for *all* assets
dbt_helper validate
# for just a single asset
dbt_helper validate --asset-select "key:out_eia__yearly_generators"
# for this asset as well as all upstream assets
dbt_helper validate --asset-select "+key:out_eia__yearly_generators"
# same as above, but skip row counts
dbt_helper validate --asset-select "+key:out_eia__yearly_generators" --exclude "*check_row_counts*"
```

See `dbt_helper validate --help` for usage details.

<a id="update-dbt-schema"></a>

## Updating table schemas

dbt stores information about a table’s schema and what tests are defined in a special
YAML file that you need to keep up to date.

We generate that file automatically by combining the existing metadata in
`pudl.metadata.resources.RESOURCE_METADATA` with human-generated data tests
defined in `pudl/dbt/schema_inputs/<data_source>/<table_name>/schemahuman.yml`.

The final generated file lives in
`pudl/dbt/models/<data_source>/<table_name>/schema.yml`.

When you change a table’s schema or update the data tests, you need to
regenerate the dbt schema:

For more details on adding data tests, see [Adding new tables](data_validation_reference.md#adding-new-tables).

```bash
dbt_helper update-tables --schema table_to_update
```

<a id="row-counts"></a>

## Updating row counts

#### WARNING
You should rarely if ever need to edit the row-counts file directly. It needs to be
kept sorted to minimize diffs in git, and manually calculating and editing row counts
is both tedious and error prone.

To create or update the row count expectations for a given table you need to:

* Make sure a fresh version of the table is available in `$PUDL_OUTPUT/parquet`.
  The expectations will be derived from what’s observed in that file.
* Add `check_row_counts_by_partition` to the `data_tests` section
  of the the table’s `schema.yml`,
  if it isn’t there already.

When ready to generate row count expectations,
the `data_tests` for a new table might look like this:

```yaml
version: 2
sources:
  - name: pudl
    tables:
      - name: new_table_name
        data_tests:
          - check_row_counts_per_partition:
              arguments:
                table_name: new_table_name
                partition_expr: "EXTRACT(YEAR FROM report_date)"
```

Then you can run:

```bash
dbt_helper update-tables --row-counts new_table_name # for one table at a time
dbt_helper update-tables --row-counts new_table1 new_table2 # for multiple tables
dbt_helper update-tables --row-counts all # for all new tables
```

If this is a brand new table, you should see changes appear in
`dbt/seeds/etl_full_row_counts.csv`. If you’re updating the row counts for a table
that already exists, you’ll need to use the `--clobber` option to make the script
overwrite existing row counts:

```bash
dbt_helper update-tables --row-counts --clobber new_table_name
```
