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
Basic usage of the helper script looks like:

```
python devtools/dbt_helper.py {table_name(s)}
```

This will add a file called `dbt/models/{data_source}/{table_name}/schema.yml` for
each table. This yaml file tells `dbt` about the table and it's schema. It will also apply the test
`check_row_counts_per_partition`, which by default will check row counts per year.
To accomplish this it will add row counts to the file `seeds/row_counts.csv`, which
get compared to observed row counts in the table when running tests.

If a table is not partitioned by year, you can add the option
`--partition-column {column_name}` to the command. This will find row counts per
unique value in the column. This is common for monthly and hourly tables that are
often partitioned by `report_date` and `datetime_utc` respectively.

To see all options for command run:

```
python devtools/dbt_helper.py add-tables --help
```

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

### Usage
There are a few ways to execute tests. To run all tests with a single command:

```
dbt build
```

This command will first run any models, then execute all tests.

For more finegrained control, first run:

```
dbt run
```

This will run all models, thus prepairing any `sql` views that will be referenced in
tests. Once you've done this, you can run all tests with:

```
dbt test
```

To run all tests for a single source table:

```
dbt test --select source:pudl.{table_name}
```

To run all tests for a model table:

```
dbt test --select {model_name}
```

#### Selecting target profile
To select between `nightly`, `etl-full`, and `etl-fast` profiles, append
`--target {target_name}` to any of the previous commands.


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

To see the output of these commands look to the `schema.yml` files
mentioned above.
