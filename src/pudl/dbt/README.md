### Overview
I've setup a basic `dbt` project here to demonstrate writing tests that will be
executed on our parquet tables. With this current setup, `dbt` will create a temporary
`duckdb` database that will query parquet files directly from our nightly builds.
So far, I've only added the `vcerare` table with 2 example tests to build on.


### Setup
To setup the `dbt` project on your local machine, execute the following commands from
this directory:

```
mkdir ~/.dbt && cp profiles.yml ~/.dbt/
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
I've demonstrated both of the methods on the
`out_vcerare__hourly_available_capacity_factor`.

To run tests simply execute the command:

```
dbt test
```

To run tests for a specific table run a command like:

```
dbt test --select source:pudl_nightly.{table_name}
```
