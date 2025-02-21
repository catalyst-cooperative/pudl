## Overview
This directory contains an initial setup of a `dbt` project meant to write
[data tests](https://docs.getdbt.com/docs/build/data-tests) for PUDL data. The
project is setup with profiles that allow you to select running tests on `nightly`
builds, `etl-full`, or `etl-fast` outputs. The `nightly` profile will operate
directly on parquet files in our S3 bucket, while both the `etl-full` and `etl-fast`
profiles will look for parquet files based on your `PUDL_OUTPUT` environment
variable. See the `Usage` section below for examples using these profiles.


## Development
To setup the `dbt` project, simply install the PUDL `conda` environment as normal,
then run the following commands from this directory.

```
dbt deps
dbt seed
```

### Adding new tables
#### Helper script
To add a new table to the project, you must add it as a
[dbt source](https://docs.getdbt.com/docs/build/sources). We've included a helper
script to automate the process at `devtools/dbt_helper.py`.

#### Usage
##### `add-tables`
The first command provided by the helper script is `add-tables`. This has already
been run for all existing tables, but may be useful when we add new tables to
PUDL.

```
python devtools/dbt_helper.py add-tables {table_name(s)}
```

This will add a file called `dbt/models/{data_source}/{table_name}/schema.yml` for
each table. This yaml file tells `dbt` about the table and it's schema. It will also apply the test
`check_row_counts_per_partition`. This test works by storing row counts per year (or all row counts
for time independent tables) in the csv files called `etl_fast_row_counts.csv` and `etl_full_row_counts.csv`
and checking these row counts against the actual table.

To see all options for this command run:

```
python devtools/dbt_helper.py add-tables --help
```

##### `migrate-tests`
The second command in the helper script is called `migrate-tests`. This command is used
to migrate existing `vs_bounds` tests. There are many of these tests in our existing
validation framework, and the configuration is relatively verbose, so this script
should be very useful for migrating these tests quickly.

The basic usage of this command looks like:

```
python devtools/dbt_helper.py migrate-tests \
    --test-config-name {config_variable}
    --table-name {table_name}
```

Where `config_variable` points to a variable defined in `src/pudl/validate.py`, which contains
a list of configuration `dict`s for the `vs_bounds` tests, and `table_name` refers to the table
the test should be applied to. See below for an example of using this command.

### Adding tests
#### Default case
Once a table is included as a `source`, you can add tests for the table. You can
either add a generic test directly in `src/pudl/dbt/models/{table_name}/schema.yml`,
or create a `sql` file in the directory `src/pudl/dbt/tests/`, which references the `source`.
When adding `sql` tests like this, you should construct a query that `SELECT`'s rows
that indicate a failure. That is, if the query returns any rows, `dbt` will raise a
failure for that test.

The project includes [dbt-expectations](https://github.com/calogica/dbt-expectations)
and [dbt-utils](https://github.com/dbt-labs/dbt-utils) as dependencies. These
packages include useful tests out of the box that can be applied to any tables
in the project. There are several examples in
`src/pudl/dbt/models/out_vcerare__hourly_available_capacity_factor/schema.yml` which
use `dbt-expectations`.

#### Modifying a table before test
In some cases you may want to modify the table before applying tests. There are two
ways to accomplish this. First, you can add the table as a `source` as described
above, then create a SQL file in the `tests/` directory like
`tests/{data_source}/{table_name}.yml`. From here you can construct a SQL query to
modify the table and execute a test on the intermediate table you've created. `dbt`
expects a SQL test to be a query that returns 0 rows for a successful test. See
the `dbt` [source function](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
for guidance on how to reference a `source` from a SQL file.

The second method is to create a [model](https://docs.getdbt.com/docs/build/models)
which will produce the intermediate table you want to execute tests on. To use this
approach, simply add a sql file to `dbt/models/{data_source}/{table_name}/`.
Now, add a SQL file to this directory named `validate_{table_name}` and define your model
for producing the intermediate table here. Finally, add the model to the `schema.yml` file
and define tests exactly as you would for a `source` table. See
`models/ferc1/out_ferc1__yearly_steam_plants_fuel_by_plant_sched402` for an example of this
pattern.

Note: when adding a model, it will be stored as a SQL `view` in the file
`{PUDL_OUTPUT}/pudl.duckdb`.

### Running tests
There are a few ways to execute tests. To run all tests with a single command:

```
dbt build
```

This command will first run any models, then execute all tests.

For more finegrained control, you can use the `--select` option to only run tests
on a specific table.

To run all tests for a single source table:

```
dbt build --select source:pudl.{table_name}
```

To run all tests on a table that uses an intermediate `dbt model`, you can do:

```
dbt build --select {model_name}
```

#### Selecting target profile
To select between `nightly`, `etl-full`, and `etl-fast` profiles, append
`--target {target_name}` to any of the previous commands.

### Updating a table
#### Modify `schema.yml`
Once we have generated an initial `schema.yml` file, we expect this configuration
to be maintained/updated manually in the future. For example, we can add
[data-tests](https://docs.getdbt.com/docs/build/data-tests) as described in the `dbt`
docs, or add/remove columns if the table schema is changed.

In the future we might migrate much of our schema/constraint testing into `dbt` as well.
In this case the easiest approach would be to update the `dbt_helper.py` script to
generate the necessary configuration from our existing metadata structures. In this case,
we should be careful to make the script not overwrite any manual configuration changes that
we make between now and then.

#### Update row counts
When we run the `add-tables` command, it generates a test for each table called
`check_row_counts_per_partition`. This test uses row counts that are stored in
csv files called `etl_fast_row_counts.csv` and `etl_full_row_counts.csv` and compares
these counts to the row counts found in the actual table when the test is run. The test
partitions row counts by year, so there are a number of rows in these csv files for each
table (unless the table has no time dimension).

During development row counts often change for normal and expected reasons like adding
new data, updating transformations, etc. When these changes happen, the tests will fail
unless we update the row counts stored in the csv files mentioned above. To see where
these tests failed, you can run:

```
dbt build --select source:pudl.{table_name} --store-failures --target={etl-fast|etl-full}
```

The output of this command should show you a `sql` query you can use to see partitions where
the row count test failed. To see these, you can do:

```
duckdb {PUDL_OUTPUT}/pudl.duckdb
```

Then copy and paste the query into the duckdb CLI (you'll need to add a semicolon to the end).
This should show you the years and the expected and found row counts. If the changes seem
reasonable and expected, you can manually update these files, or you can run the command:

```
python devtools/dbt_helper.py add-tables {table_name} --row-counts-only --clobber --use-local-tables
```

This will tell the helper script to overwrite the existing row counts with new row counts from
the table in your local `PUDL_OUTPUT` stash. If you want to update the `etl-fast` row counts,
simply append the option `--etl-fast` to the above command.

### Test migration example
This section will walk through migrating an existing collection of validation
tests. As an example, we'll use the tests in `test/validate/mcoe_test.py`.
This file contains a collection of tests which use some common methods
defined in `pudl/validate.py`. These tests are representative of many
of the types of tests we're looking to migrate.

#### `vs_bounds` tests
We'll start with a set of tests which use the `vs_bounds` method defined
in `validate.py`. These check that a specified quantile of a numeric column
is within a configurable set of bounds. The values to configure the quantiles
and the bounds are defined in config  `dict`'s, in `validate.py`. For
example, the test, `test_gas_capacity_factor`, references
configuration stored in the variable `mcoe_gas_capacity_factor`. Once we
know where the configuration is stored, we need to identify the specific
tables used in the test. This test is producing tables with the `PudlTabl`
method `mcoe_generators`. This method returns the tables
`out_eia__monthly_generators`, and `out_eia__yearly_generators`. To migrate these tests we'll first want generate `dbt` configuration for these
tables.

```
python devtools/dbt_helper.py add-tables out_eia__monthly_generators out_eia__yearly_generators
```

This will create the files `dbt/models/output/out_eia__{AGG}_generators/schema.yml`,
which will tell `dbt` about the table and generate row count tests.
Note the exact format of this file might be modified by our linting tools,
but this does not change functionality.

Next, we want to update the `dbt` configuration to add tests equivalent
to the `vs_bounds` tests. The `dbt_helper` script contains a command that
can automatically generate this `dbt` configuration from the exisitng python
config. To do this you can use a command like:

```
python devtools/dbt_helper.py migrate-tests \
    --table-name out_eia__AGG_generators \
    --test-config-name mcoe_gas_capacity_factor
```

This command will add the necessary tests. There is a chance that formatting
could get messed up slightly in the translation, but will work out of
the box in the general case.

To see the tests generated by these commands, look to the `schema.yml` files
mentioned above. You can also execute these tests with the following command.

```
dbt test --select source:pudl.out_eia__AGG_generators
```

When we run these tests, there will be a couple of errors as the generated tests are
not a perfect translation of the old tests. Namely, the
`expect_column_weighted_quantile_values_to_be_between` tests will compute a discrete
weighted quantile, while the old python tests compute continuous quantiles. First,
the following test on the yearly table will fail:

```
- expect_column_weighted_quantile_values_to_be_between:
  quantile: 0.65
  max_value: 0.7
  row_condition: fuel_type_code_pudl='gas' and report_date>=CAST('2015-01-01' AS DATE) and capacity_factor<>0.0
  weight_column: capacity_mw
  ```

 To debug this test, we can use `duckdb` directly. There are many ways to interact
 with `duckdb`, here will use the CLI. See the
 [here](https://duckdb.org/docs/installation/) for installation directions.
 To launch the CLI, navigate to the directory that your `PUDL_OUTPUT` environment
 variable points to, and execute:

 ```
 duckdb pudl.duckdb
 ```

Now we want to execute portions of the compiled SQL produced by `dbt`. To find this,
look at the output of the test failure, and you should see a line under the test
failure that looks like `compiled code at {path_to_sql}`. Looking at this file, we'll
pull out the section

```sql
WITH CumulativeWeights AS (
    SELECT
        capacity_factor,
        capacity_mw,
        SUM(capacity_mw) OVER (ORDER BY capacity_factor) AS cumulative_weight,
        SUM(capacity_mw) OVER () AS total_weight
    FROM 'https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/out_eia__yearly_generators.parquet'
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

This is where the weighted quantile is actually calculated. We can copy this into
the `duckdb` CLI, add a semicolon to the end of the last line and hit `Enter`.
This produces the output:

```
┌─────────────────┐
│ capacity_factor │
│      float      │
├─────────────────┤
│   0.82587963    │
└─────────────────┘
```

This is failing because the `max_value` is set to `0.7`. If we change this value
to 0.83, this test should now pass.

#### `no_null_cols` tests
The method `test_no_null_cols_mcoe` in `mcoe_test.py` checks a number of tables to
validate that there are no completely null columns. We've added a custom test
to the `dbt` project that can accomplish this. To use this test, simply apply the
`not_all_null` test to a column in question:

```
    columns:
    - name: report_date
      data_tests:
      - not_all_null
```

See `dbt/models/_out_eia__monthly_heat_rate_by_unit/schema.yml` for specific examples.

Note: there is also a builtin test in `dbt` called `not_null` that will make sure
there are no null values at all in a column.

#### `no_null_rows` tests
The method `test_no_null_rows_mcoe` in `mcoe_test.py` checks a number of tables to
validate that there are no completely null rows. We've added a custom test
to the `dbt` project that can accomplish this. To use this test, simply apply the
`no_null_rows` test to a table:

```
version: 2
sources:
  - name: pudl
    tables:
      - name: _out_eia__monthly_derived_generator_attributes
        data_tests:
          - no_null_rows
```

See `dbt/models/_out_eia__monthly_derived_generator_attributes/schema.yml` for
a specific example.

#### `bespoke` tests
As the name implies, `bespoke` tests will require some degree of custom handling.
That being said, there are common patterns for how/where to add these tests. Some
tests can also potentially make use of builtin tests provided by `dbt_expectations`,
`dbt_utils`, and `dbt` itself.

For an example, we'll demonstrate migrating the test `test_idle_capacity`. This
test is applied to the `out_eia__{freq}_generators` tables. Because this test is applied
to two different tables, we'll define it as a
[generic data test](https://docs.getdbt.com/docs/build/data-tests#generic-data-tests).
This allows us to reuse a single test on both tables. If this test was only used
on a single table, we could create a
[singular data test](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests).

To create a new generic test, we will add a new file called `dbt/macros/test_idle_capacity.sql`.
Then, we develop a SQL query to mimic the behavior of the original test. For details
see the SQL file. It uses `jinja` fairly extensively to set/access bounds. Once this
new test has been developed, we can apply it to the tables by modifying their `schema.yml`
as so:

```
version: 2
sources:
  - name: pudl
    tables:
      - name: out_eia__yearly_generators
        data_tests:
          - test_idle_capacity
```
