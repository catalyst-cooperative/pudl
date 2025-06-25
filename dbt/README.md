# Overview

This directory contains an initial setup of a `dbt` project meant to write [data
tests](https://docs.getdbt.com/docs/build/data-tests) for PUDL data. The project is
setup with profiles that allow you to select running tests on `etl-full`, or `etl-fast`
outputs. Both the `etl-full` and `etl-fast` profiles will look for parquet files based
on your `PUDL_OUTPUT` environment variable. See the `Usage` section below for examples
using these profiles. `etl-full` is the default.

# Development

To setup the `dbt` project, simply install the PUDL `conda` environment as normal,
then run the following commands from this directory.

```bash
dbt deps
dbt seed
```

## Adding new tables

### Helper script

To add a new table to the project, you must add it as a [dbt
source](https://docs.getdbt.com/docs/build/sources). We've included a helper script to
automate the process which is installed as the `dbt_helper` command. The script
itself lives in `src/pudl/scripts/dbt_helper.py`

### Usage

#### `update-tables`

The first command provided by the helper script is `update-tables`. It is useful
when adding new tables or changing the schemas or row count expectations of existing
tables.

When adding new tables, the command:

```bash
dbt_helper update-tables --schema {table_name(s)}
```

will add a file called `dbt/models/{data_source}/{table_name}/schema.yml` for each
listed table. This yaml file tells `dbt` about the table and its schema. If the
table already exists and you need to update it, you'll have to add `--clobber`

It will also specify the `check_row_counts_per_partition` test. This test works by
comparing expected row counts for partitions within a table (typically distinct
`report_date` values) stored in `etl_fast_row_counts.csv` and `etl_full_row_counts.csv`
against the actual row counts in the materialized tables.

To update the expected row counts based on the number of rows found in existing
materialized tables, you can run:

```bash
dbt_helper update-tables --target etl-full --row-counts {table_name(s)}
```

To see all options for this command run:

```bash
dbt_helper update-tables --help
```

#### `validate`

If you want to check if a materialized asset passes the validation tests defined in DBT, you can use `dbt_helper validate`. You can pass in the `--select` parameter to filter to a specific set of validation tests, and `--target` to set the DBT target.

This will run the associated tests, then print out the test query output so you can see what went wrong.

Example usage:

```bash
dbt_helper validate --select "source:pudl_dbt.pudl.out_eia__yearly_generators"
```

See [official selection syntax documentation](https://docs.getdbt.com/reference/node-selection/syntax) for details.

## Adding tests

### Default case

Once a table is included as a `source`, you can add tests for the table. You can either
add a generic test directly in `src/pudl/dbt/models/{table_name}/schema.yml`, or create
a `sql` file in the directory `src/pudl/dbt/tests/`, which references the `source`. When
adding `sql` tests like this, you should construct a query that `SELECT`'s rows that
indicate a failure. That is, if the query returns any rows, `dbt` will raise a failure
for that test.

The project includes [dbt-expectations](https://github.com/metaplane/dbt-expectations)
and [dbt-utils](https://github.com/dbt-labs/dbt-utils) as dependencies. These packages
include useful tests out of the box that can be applied to any tables in the project.
There are several examples in
`src/pudl/dbt/models/out_vcerare__hourly_available_capacity_factor/schema.yml` which use
`dbt-expectations`.

### Modifying a table before test

In some cases you may want to modify the table before applying tests. There are two ways
to accomplish this. First, you can add the table as a `source` as described above, then
create a SQL file in the `tests/` directory like `tests/{data_source}/{table_name}.yml`.
From here you can construct a SQL query to modify the table and execute a test on the
intermediate table you've created. `dbt` expects a SQL test to be a query that returns 0
rows for a successful test. See the `dbt` [source
function](https://docs.getdbt.com/reference/dbt-jinja-functions/source) for guidance on
how to reference a `source` from a SQL file.

The second method is to create a [model](https://docs.getdbt.com/docs/build/models)
which will produce the intermediate table you want to execute tests on. To use this
approach, simply add a sql file to `dbt/models/{data_source}/{table_name}/`. Now, add a
SQL file to this directory named `validate_{table_name}` and define your model for
producing the intermediate table here. Finally, add the model to the `schema.yml` file
and define tests exactly as you would for a `source` table. See
`models/ferc1/out_ferc1__yearly_steam_plants_fuel_by_plant_sched402` for an example of
this pattern.

Note: when adding a model, it will be stored as a SQL `view` in the file
`{PUDL_OUTPUT}/pudl_dbt_tests.duckdb`.

## Running tests

There are a few ways to execute tests. To run all tests with a single command:

```bash
dbt build
```

This command will first run any models, then execute all tests.

For more fine grained control, you can use the `--select` option to only run tests
on a specific table.

To run all tests for a single source table:

```bash
dbt build --select source:pudl.{table_name}
```

To run all tests on a table that uses an intermediate `dbt model`, you can do:

```bash
dbt build --select {model_name}
```

### Selecting target profile

To select between `etl-full`, and `etl-fast` profiles, append `--target {target_name}`
to any of the previous commands.

## Updating a table

### Modify `schema.yml`

Once we have generated an initial `schema.yml` file, we expect this configuration to be
maintained/updated manually in the future. For example, we can add
[data-tests](https://docs.getdbt.com/docs/build/data-tests) as described in the `dbt`
docs, or add/remove columns if the table schema is changed.

In the future we might migrate much of our schema/constraint testing into `dbt` as well.
In this case the easiest approach would be to update the `dbt_helper` script to
generate the necessary configuration from our existing metadata structures. In this
case, we should be careful to make the script not overwrite any manual configuration
changes that we make between now and then.

### Update row counts

When we run the `update-tables` command, it generates a test for each table called
`check_row_counts_per_partition`. This test uses row counts that are stored in CSV files
`etl_fast_row_counts.csv` and `etl_full_row_counts.csv` and compares these counts
to the row counts found in the actual table when the test is run. The test partitions
row counts by year, so there are a number of rows in these CSV files for each table
(unless the table has no time dimension).

During development row counts often change for normal and expected reasons like adding
new data, updating transformations, etc. When these changes happen, the tests will fail
unless we update the row counts stored in the csv files mentioned above. To see where
these tests failed, you can run:

```bash
dbt build --select source:pudl.{table_name} --store-failures --target={etl-fast|etl-full}
```

The output of this command should show you a `sql` query you can use to see partitions
where the row count test failed. To see these, you can do:

```bash
duckdb {PUDL_OUTPUT}/pudl_dbt_tests.duckdb
```

Then copy and paste the query into the duckdb CLI (you'll need to add a semicolon to the
end). This should show you the years and the expected and found row counts. If the
changes seem reasonable and expected, you can manually update these files, or you can
run the command:

```bash
dbt_helper update-tables --target etl-full --row-counts --clobber {table_name}
```

This will tell the helper script to overwrite the existing row counts with new row
counts from the table in your local `PUDL_OUTPUT` stash. If you want to update the
`etl-fast` row counts, use `--target etl-fast` instead of the default `--target etl-full`.

### Debugging dbt test failures

When a more complex test that relies on custom SQL fails, we can debug it using `duckdb`.
There are many ways to interact with `duckdb`, here will use the CLI. See the
[here](https://duckdb.org/docs/installation/) for installation directions. To launch the
CLI, navigate to the directory that your `PUDL_OUTPUT` environment variable points to,
and execute:

```bash
duckdb pudl_dbt_tests.duckdb
```

For debugging purposes, we'll often want to execute portions of the compiled SQL
produced by `dbt`. To find this, look at the output of the test failure, and you should
see a line under the test failure that looks like `compiled code at {path_to_sql}`.
Looking at this file, for a failing test that looks at weighted quantiles, we might
pull out the section:

```sql
WITH CumulativeWeights AS (
    SELECT
        capacity_factor,
        capacity_mw,
        SUM(capacity_mw) OVER (ORDER BY capacity_factor) AS cumulative_weight,
        SUM(capacity_mw) OVER () AS total_weight
    FROM '/your/local/pudl_output/parquet/out_eia__yearly_generators.parquet'
    WHERE capacity_factor IS NOT NULL OR capacity_mw IS NOT NULL
),
QuantileData AS (
    SELECT
        capacity_factor,
        capacity_mw,
        cumulative_weight,
        total_weight,
        cumulative_weight / total_weight AS cumulative_probability
    FROM CumulativeWeights
)
SELECT capacity_factor
FROM QuantileData
WHERE cumulative_probability >= 0.65
ORDER BY capacity_factor
LIMIT 1
```

This is where the weighted quantile is actually calculated. We can copy this into the
`duckdb` CLI, add a semicolon to the end of the last line and hit `Enter`. This produces
the output:

| capacity_factor float |
|-----------------------|
|            0.82587963 |

This is failing because the `max_value` is set to `0.65`. If we change this value to
0.83, this test should now pass (though if this is an unexpected change in the
capacity factor, you would want to investigate why it changed before updating the
test threshold!)

### `bespoke` tests

As the name implies, `bespoke` tests will require some degree of custom handling.
That being said, there are common patterns for how/where to add these tests. Some
tests can also potentially make use of builtin tests provided by `dbt_expectations`,
`dbt_utils`, and `dbt` itself.

For an example, we'll demonstrate migrating the test `test_idle_capacity`. This
test is applied to the `out_eia__{freq}_generators` tables (where `freq` can be
yearly or monthly). Because this test is applied to two different tables, we'll define
it as a [generic data
test](https://docs.getdbt.com/docs/build/data-tests#generic-data-tests). This allows us
to reuse a single test on both tables. If this test was only used on a single table, we
could create a [singular data
test](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests).

To create a new generic test, we will add a new file called `dbt/macros/test_idle_capacity.sql`.
Then, we develop a SQL query to mimic the behavior of the original test. For details
see the SQL file. It uses `jinja` fairly extensively to set/access bounds. Once this
new test has been developed, we can apply it to the tables by modifying their `schema.yml`
as so:

```yaml
version: 2
sources:
  - name: pudl
    tables:
      - name: out_eia__yearly_generators
        data_tests:
          - test_idle_capacity
```
