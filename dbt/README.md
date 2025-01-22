### Overview
This directory contains an initial setup of a `dbt` project meant to write
[data tests](https://docs.getdbt.com/docs/build/data-tests) for PUDL data. The
project is setup with profiles that allow you to select running tests on `nightly`
builds, `etl-full`, or `etl-fast` outputs. The `nightly` profile will operate
directly on parquet files in our S3 bucket, while both the `etl-full` and `etl-fast`
profiles will look for parquet files based on your `PUDL_OUTPUT` environment
variable. See the `Usage` section below for examples using these profiles.


### Development
To setup the `dbt` project, simply install the PUDL `conda` environment as normal,
then run the following command from this directory.

```
dbt deps
```

#### Adding new tables
To add a new table to the project, you must add it as a
[dbt source](https://docs.getdbt.com/docs/build/sources). You can do this by editing
the file `src/pudl/dbt/models/schema.yml`. I've already added the table
`out_vcerare__hourly_available_capacity_factor`, which can be used as a reference.

#### Adding tests
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
In many cases we modify a table slightly before executing a test. There are a couple
ways to accomplish this. First, when creating a `sql` test in `src/pudl/dbt/tests/`,
you can structure your query to modify the table/column before selecting failure
rows. The second method is to create a [model](https://docs.getdbt.com/docs/build/models) in `src/pudl/dbt/models/validation`. Any models created here will create a view
in a `duckdb` database being used by `dbt`. You can then reference this model in
`src/pudl/dbt/models/schema.yml`, and apply tests as you would with `sources`. There's
an example of this pattern which takes the table `out_ferc1__yearly_steam_plants_fuel_by_plant_sched402`,
computes fuel cost per mmbtu in the `sql` model, then applies `dbt_expectations` tests
to this model.

#### Usage
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

##### Selecting target profile
To select between `nightly`, `etl-full`, and `etl-fast` profiles, append
`--target {target_name}` to any of the previous commands.
