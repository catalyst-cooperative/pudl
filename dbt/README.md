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
then run the following command from this directory.

```
dbt deps
```

### Adding new tables
To add a new table to the project, you must add it as a
[dbt source](https://docs.getdbt.com/docs/build/sources). The standard way to do
this is to create a new file `models/{data_source}/{table_name}.yml`. If the the
`data_source` doesn't already have a directory within `models/` you should first
create one and add the yaml file here.

### Adding tests
#### Default case
Once a table is included as a `source`, you can add tests for the table. You can
either add a generic test directly in `src/pudl/dbt/models/schema.yml`, or create
a `sql` file in the directory `src/pudl/dbt/tests/`, which references the `source`.
When adding `sql` tests like this, you should construct a query that `SELECT`'s rows
that indicate a failure. That is, if the query returns any rows, `dbt` will raise a
failure for that test.

The project includes [dbt-expectations](https://github.com/calogica/dbt-expectations)
and [dbt-utils](https://github.com/dbt-labs/dbt-utils) as dependencies. These
packages include useful tests out of the box that can be applied to any tables
in the project. There are several examples in `src/pudl/dbt/models/schema.yml` which
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
approach, first create a directory named `tests/{data_source}/{table_name}/` and move
your yaml file defining the `source` table to `tests/{data_source}/{table_name}/schema.yml`.
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
