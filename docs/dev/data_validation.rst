.. _data_validation:

================================================================================
Data Validation Tests
================================================================================

We use a tool called `dbt <https://www.getdbt.com/>`_ to manage our data validation
tests. dbt is a SQL-based data transformation tool that can be used to manage large data
pipelines, but we're currently only using it for data validation.

In dbt, every data validation test is a SQL query meant to select rows that fail the
test. A successful test will return no results. The query can be parametrized so it can
be reused across multiple tables. dbt includes a few `built-in data test definitions
<https://docs.getdbt.com/docs/build/data-tests>`_, and the `dbt_expectations
<https://github.com/metaplane/dbt-expectations>`_ package provides many more. We also
define our own `custom data tests
<https://docs.getdbt.com/best-practices/writing-custom-generic-tests>`_.

.. note:: Why use dbt and SQL instead of Python?

   Modern analytical query engines are extremely fast and memory efficient. By using dbt
   with `DuckDB <https://duckdb.org/>`_ to query the PUDL `Parquet
   <https://parquet.apache.org/>`_ outputs we're able to run thousands of validations
   across hundreds of tables with billions of rows in a minute, instead of the 2-3 hours
   it used to take our much less extensive validation tests to run. Plus we get to learn
   SQL.

--------------------------------------------------------------------------------
Data validation guidelines
--------------------------------------------------------------------------------

We should chat about the minimum level of data validation that we expect for a table.

* row counts (all tables)
* no entirely null columns (all tables)
* ???

--------------------------------------------------------------------------------
Example of typical data validation workflow
--------------------------------------------------------------------------------

* Explain that the tests are looking at whatever data is in ``$PUDL_OUTPUT``.
* Need to be vigilant about the state of the dagster outputs and how they relate to the
  branch / code that you're working on. If you have outputs from multiple different
  branches, and you run the full data validation tests, they might fail because of the
  mismatch.
* Typical workflow: tweaking an asset, remateralize it in Dagster, re-run the data
  validations that pertain to just that table.
* Often also useful to rematerialize the changed table and all of its downstream
  dependencies, and then run the data validations on all of those downstream
  dependencies to see if there were any unforeseen consequences.

--------------------------------------------------------------------------------
Running the data validation tests
--------------------------------------------------------------------------------

Setting up dbt
~~~~~~~~~~~~~~

The dbt directory contains the PUDL dbt project which manages our `data tests
<https://docs.getdbt.com/docs/build/data-tests>`_. To run dbt you'll need to have the
``pudl-dev`` conda environment activated (see :doc:`dev_setup`). Before running the
tests you'll want to run the following from within the ``dbt/`` directory:

.. code-block:: bash

    # Installs additional dbt-specific dependencies in the dbt project
    dbt deps
    # Materializes dbt-specific "seed" tables from CSVs in the seeds/ directory
    # In our case, these contain the expected row counts for PUDL tables
    dbt seed

* Data tests run against the Parquet outputs that are in your ``$PUDL_OUTPUT/parquet``
  directory, so it's important that you make sure the outputs you're testing there are
  in sync with the tests you're running.
* If you want to run the tests against integration test output, you can temporarily
  reset your ``$PUDL_OUTPUT`` environment variable to point dbt at the temporary output
  directory created by ``pytest`` to avoid needing to clobber your full ETL outputs.

Running dbt directly
~~~~~~~~~~~~~~~~~~~~

* Lots of flexibility, documented by dbt. Note the weird thing about PUDL is that nearly
  all of our tables are "sources" rather than "models" managed by dbt.
* Can run multi-threaded and it's much faster, but will sometimes fail because there are
  too many files open / concurrent writes to the failure database. Not related to the
  data tests themselves.

**QUESTION** I think we are on the verge of getting rid of the ``etl-fast`` target
right? Is there anything other than the row counts that really needs to be separate
between them? In which case, we should nix the multi-target documentation.

.. note::

    There are a handful of data validation tests that have been implemented using
    `Dagster's asset checks <https://docs.dagster.io/guides/test/asset-checks>`_.
    Typically these tests weren't well suited to SQL, weren't performance bottlenecks,
    and had already been implemented in Python. E.g. :func:`pudl.validate.no_null_rows`.

Our ``dbt_helper`` script
~~~~~~~~~~~~~~~~~~~~~~~~~
* Updating / creating table schemas
* Updating row-count expectations (never do this manually)
* Running data tests and providing richer failure outputs

Selecting individual tests and tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* dbt selection syntax with ``--select`` and ``--exclude``
* dagster asset selection syntax in ``dbt_helper validate``

Data validation in our integration tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* All of the dbt tests are run as part of our integration tests.
* This means they get run as part of the merge queue before a PR is merged into ``main``
* However the integration tests only process a couple of years of data.
* So this is not an exhaustive validation of the data.
* If you're modifying a table or creating a new table, you should be running the data
  validations against the full output of that table before trying to merge.

Data validation in branch builds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Especially when integrating new months/quarters/years of data or making changes to the
  processing of a table that impacts many downstream assets, it's easy to end up
  altering the expected row-counts for many tables in a single PR.
* Depending on your computer, running the full ETL locally can be extremely time
  consuming and may run into memory limits.

To catch unexpected changes to the data, we keep track of the expected number of rows in
each data table we distribute. These expectations are stored in
``dbt/seeds/etl_full_row_counts.csv`` and they can be updated using the ``dbt_helper``
script. If you can't run the full ETL locally, the nightly builds / branch deployments
also generate updated row count expectations. So you can kick off the
``build-deploy-pudl`` GitHub Action using the ``workflow_dispatch`` trigger on your
branch on GitHub, and then download the updated ``etl_full_row_counts.csv`` file from
the build outputs that are uploaded to
``gs://builds.catalyst.coop/<build-id>/etl_full_row_counts.csv`` once the build has
completed. See the :doc:`nightly_data_builds` documentation for more details on the
nightly builds.

--------------------------------------------------------------------------------
Debugging data validation failures
--------------------------------------------------------------------------------

* Using ``dbt_helper validate``.
* Inspecting and running the compiled SQL yourself. What does "compiled" SQL mean here?
* ``dbt build --store-failures`` and the ``pudl_dbt_tests.duckdb`` output -- what is
  stored in that database anyway?
* using ``duckdb < path/to/compiled.sql``
* Using DuckDB's ``.read path/to/compiled.sql`` to play with data interactively.
* Go through a simpler example before getting into the complicated quantile checks test.

Debugging quantile checks
~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

  This seems quite involved. Can we make it simpler? Improve the test failure output to
  enable some debugging without this level of user engagement? Can we provide additional
  guidance on understanding what to do about the failure, beyond updating the test
  parameters (i.e. how to tell if it's a reasonable evolution of the underlying data
  vs. an indication that something in our data processing has gone wrong).

Run the quantile check by selecting a the table you want to check.  If you want to check
all the tables, you can instead select all the quantile checks by using
``test_name:expect_quantile_constraints`` in the select clause.

In this example, we're running quantile checks for ``out_eia__monthly_generators``.

.. code-block:: console

    [pudl/dbt] $ dbt build --select "source:pudl.out_eia__monthly_generators,test_name:expect_quantile_constraints"
    [...]
    17:54:02  Completed with 1 error, 0 partial successes, and 0 warnings:
    17:54:02
    17:54:02  Failure in test source_expect_quantile_constraints_pudl_out_eia__monthly_generators_capacity_factor___quantile_0_6_min_value_0_5_max_value_0_9____quantile_0_1_min_value_0_04____quantile_0_95_max_value_0_95___fuel_type_code_pudl_coal_and_capacity_factor_0_0__capacity_mw (models/output/out_eia__monthly_generators/schema.yml)
    17:54:02    Got 1 result, configured to fail if != 0
    17:54:02
    17:54:02    compiled code at target/compiled/pudl_dbt/models/output/out_eia__monthly_generators/schema.yml/source_expect_quantile_constra_a53737dceb68a29ccc347708c9467242.sql
    [...]

In this example, one quantile was out of bounds.

Grab the quantile that's failing by running the "compiled code at" SQL file against
the tests db.

.. code-block:: console

  [pudl/dbt] $ duckdb $PUDL_OUTPUT/pudl_dbt_tests.duckdb <target/compiled/pudl_dbt/models/output/out_eia__monthly_generators/schema.yml/source_expect_quantile_constra_a53737dceb68a29ccc347708c9467242.sql
  ┌──────────┬────────────┐
  │ quantile │ expression │
  │ varchar  │  boolean   │
  ├──────────┼────────────┤
  │ 0.1      │ false      │
  └──────────┴────────────┘

In this example, the quantile that failed was quantile 0.1.

Find out how severe it is by running the "debug_quantile_constraints" operation. You
will need the table name (grab from the "compiled code at" path) and the test name
(grab from the "Failure in test" line in the original output). Remember to specify
the same local target.

.. code-block:: console

  [pudl/dbt] $ dbt run-operation debug_quantile_constraints --args "{table: out_eia__monthly_generators, test: source_expect_quantile_constraints_pudl_out_eia__monthly_generators_capacity_factor___quantile_0_6_min_value_0_5_max_value_0_9____quantile_0_1_min_value_0_04____quantile_0_95_max_value_0_95___fuel_type_code_pudl_coal_and_capacity_factor_0_0__capacity_mw}"
  17:59:42  Running with dbt=1.9.3
  17:59:42  Registered adapter: duckdb=1.9.2
  17:59:42  Found 2 models, 377 data tests, 2 seeds, 242 sources, 830 macros
  17:59:43  table: source.pudl_dbt.pudl.out_eia__monthly_generators
  17:59:43  test: expect_quantile_constraints
  17:59:43  column: capacity_factor
  17:59:43  row_condition: fuel_type_code_pudl='coal' and capacity_factor<>0.0
  17:59:43  description:
  17:59:43  quantile |    value |      min |      max
  17:59:43      0.60 |    0.545 |     0.50 |     0.90
  17:59:43      0.10 |    0.036 |     0.04 |     None
  17:59:43      0.95 |    0.826 |     None |     0.95

In this example, quantile 0.1 was expected to be at least 0.04, but was found to be
0.036, which is too low.

Locate the quantile check in the table's ``schema.yml`` file. The path is the same as
the "compiled code at" path with the heads and tails trimmed off -- copy starting from
``models/`` and stop at ``schema.yml``.

Find the column name and the row condition in the debug_quantile_constraints output.
In this example, the check we want is for column ``capacity_factor``, and it's the
entry with a row condition ``fuel_type_code_pudl='coal' and capacity_factor<>0.0``.

.. code-block:: console

  [pudl/dbt] $ $EDITOR models/output/out_eia__monthly_generators/schema.yml

Depending on the situation, from here you can:

* investigate further in a Python notebook
* fix a bug, re-run the pipeline, and repeat the check
* adjust the quantile constraints (& consider leaving a dated note for followup in
  case it gets worse)

--------------------------------------------------------------------------------
Applying pre-defined data validations to existing data
--------------------------------------------------------------------------------

To add an already defined test to an existing table or column, you just need to add
the test and any necessary parameters to the ``schema.yml`` test associated with the
table, found at ``src/pudl/dbt/models/{data_source}/{table_name}/schema.yml``. These
go in the ``data_tests`` section of either the table or column-level schema.

In general, table-level tests depend on multiple columns or test some property of the
table as a whole. Column-level tests typically depend only on values with the column
they are applied to.

Pre-defined tests
~~~~~~~~~~~~~~~~~
Our dbt project includes `dbt-utils <https://github.com/dbt-labs/dbt-utils>`_ and
`dbt-expectations <https://github.com/metaplane/dbt-expectations>`_ as dependencies.
These packages include a bunch of useful tests that can be applied to any table.
There are several examples of applying tests from ``dbt-expectations`` in
``src/pudl/dbt/models/vcerare/out_vcerare__hourly_available_capacity_factor/schema.yml``

See the full package documentation pages for exhaustive details.

Tests defined within PUDL
~~~~~~~~~~~~~~~~~~~~~~~~~

* Using existing PUDL generic tests.
* Need to integrate documentation of our existing generic tests into the docs build.
* Need to convert all bespoke / singular tests into generic tests.

--------------------------------------------------------------------------------
Adding new tables
--------------------------------------------------------------------------------

* How to use ``dbt_helper update-tables`` to create new schemas.
* Manually updating schema files (not generally recommended)
* How we keep the dbt schemas in sync with the PUDL table definitions. Note that the
  unit tests check for consistency between them (table and column names).
* Should almost always add new row count tests for a new table.
* Explain how to do that using ``dbt_helper`` after manually adding the test to the new
  ``schema.yml`` file.
* Should almost always apply ``expect_columns_not_all_null``
* Set expectations for what level of data validation a new table should be subjected to.

--------------------------------------------------------------------------------
Defining new data validation tests
--------------------------------------------------------------------------------

* How to define a new generic test (lean on references to dbt docs when possible)
* Focus on the things that make the PUDL use case unusual.
* DuckDB + Parquet means we can't rely on ``adapter`` object methods (no real DB)
* Almost all our tables are "sources" not "models"

Defining Macros
~~~~~~~~~~~~~~~~~~~~~~~~~~

* In dbt, macros are reusable SQL snippets that can be used to simplify your tests. You
  can define a macro once and then use it in multiple tests. This is particularly useful
  for complex tests that require a lot of boilerplate code.

Testing the Tests
~~~~~~~~~~~~~~~~~~~~~~~~~~

* One reason to create macros for more complex functions is that they can be
  independently unit-tested.

Creating intermediate tables for a test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In some cases you may need to modify a table or calculate some derived values before
you can apply a test. There are two ways to accomplish this. First, you can add the
table as a ``source`` as described above, then create a SQL file in the ``tests/``
directory like ``tests/{data_source}/{table_name}.yml``.  From here you can construct a
SQL query to modify the table and execute a test on the intermediate table you've
created. ``dbt`` expects a SQL test to be a query that returns 0 rows for a successful
test. See the ``dbt`` `source function
<https://docs.getdbt.com/reference/dbt-jinja-functions/source>`_ for guidance on how to
reference a ``source`` from a SQL file.

The second method is to create a `model <https://docs.getdbt.com/docs/build/models>`_
which will produce the intermediate table you want to execute tests on. To use this
approach, simply add a sql file to ``dbt/models/{data_source}/{table_name}/``. Now, add
a SQL file to this directory named ``validate_{table_name}`` and define your model for
producing the intermediate table here. Finally, add the model to the ``schema.yml`` file
and define tests exactly as you would for a ``source`` table. See
``models/ferc1/out_ferc1__yearly_steam_plants_fuel_by_plant_sched402`` for an example of
this pattern.

Note: when adding a model, it will be stored as a SQL ``view`` in the file
``{PUDL_OUTPUT}/pudl_dbt_tests.duckdb``.

================================================================================
Unmigrated Data Validation Docs (cannibalize)
================================================================================

-----------------
Adding new tables
-----------------

The ``dbt_helper`` script
~~~~~~~~~~~~~~~~~~~~~~~~~

To add a new PUDL table to the dbt project, you must add it as a `dbt
source <https://docs.getdbt.com/docs/build/sources>`_. The ``dbt_helper`` script
automates the initial setup. The script lives in ``src/pudl/scripts/dbt_helper.py``
but it can be invoked in the ``pudl-dev`` environment at the command line. For example,
to see the script's help message:

.. code-block:: console

  dbt_helper --help

Usage
^^^^^

``update-tables``
"""""""""""""""""

The first command provided by the helper script is ``update-tables``. It is useful
when adding new tables or changing the schemas or row count expectations of existing
tables.

When adding new tables, the command:

.. code-block:: bash

    dbt_helper update-tables --schema {table_name(s)}

will add a file called ``dbt/models/{data_source}/{table_name}/schema.yml`` for each
listed table. This yaml file tells ``dbt`` about the table and its schema. If the
table already exists and you need to update it, you'll have to add ``--clobber``

It will also specify the ``check_row_counts_per_partition`` test. This test works by
comparing expected row counts for partitions within a table (typically distinct
``report_date`` values) stored in ``etl_fast_row_counts.csv`` and
``etl_full_row_counts.csv`` against the actual row counts in the materialized tables.

To update the expected row counts based on the number of rows found in existing
materialized tables, you can run:

.. code-block:: bash

    dbt_helper update-tables --row-counts {table_name(s)}

To see all options for this command run:

.. code-block:: bash

    dbt_helper update-tables --help

``validate``
""""""""""""

If you want to check if a materialized asset passes the validation tests defined in dbt,
you can use ``dbt_helper validate``.

This understands how to translate dagster asset selections into dbt node selections, and
does some extra legwork to make the test outputs more informative.

See ``dbt_helper validate --help`` for usage details.

Example usage:

.. code-block:: bash

    # for just a single asset
    dbt_helper validate --asset-select "key:out_eia__yearly_generators"
    # for this asset as well as all upstream assets
    dbt_helper validate --asset-select "+key:out_eia__yearly_generators"
    # skip rowcounts
    dbt_helper validate --asset-select "+key:out_eia__yearly_generators" --exclude "*check_row_counts*"
    # if you want to select a dbt node in particular
    dbt_helper validate --select "source:pudl.out_eia__yearly_generators"

See `dbt selection syntax documentation
<https://docs.getdbt.com/reference/node-selection/syntax>`_ and `Dagster selection
syntax documentation
<https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference>`_ to see
all the possibilities.

Adding tests
-----------------


Running tests
-----------------

There are a few ways to execute tests. To run all tests with a single command:

.. code-block:: bash

    dbt build

This command will first run any models, then execute all tests.

For more fine grained control, you can use the ``--select`` option to only run tests
on a specific table.

To run all tests for a single source table:

.. code-block:: bash

    dbt build --select source:pudl.{table_name}

To run all tests on a table that uses an intermediate ``dbt model``, you can do:

.. code-block:: bash

    dbt build --select {model_name}

Updating a table
-----------------

Modify ``schema.yml``
~~~~~~~~~~~~~~~~~~~~~

Once we have generated an initial ``schema.yml`` file, we expect this configuration to
be maintained/updated manually in the future. For example, we can add `data-tests
<https://docs.getdbt.com/docs/build/data-tests>`_ as described in the ``dbt`` docs, or
add/remove columns if the table schema is changed.

Update row counts
~~~~~~~~~~~~~~~~~~~~~

When we run the ``update-tables`` command, it generates a test for each table called
``check_row_counts_per_partition``. This test uses row counts that are stored in CSV
files ``etl_fast_row_counts.csv`` and ``etl_full_row_counts.csv`` and compares these
counts to the row counts found in the actual table when the test is run. The test
partitions row counts by year, so there are a number of rows in these CSV files for each
table (unless the table has no time dimension).

During development row counts often change for normal and expected reasons like adding
new data, updating transformations, etc. When these changes happen, the tests will fail
unless we update the row counts stored in the csv files mentioned above. To see where
these tests failed, you can run:

.. code-block:: bash

    dbt build --select "source:pudl.table_name" --store-failures

The output of this command should show you a ``sql`` query you can use to see partitions
where the row count test failed. To see these, you can do:

.. code-block:: bash

    duckdb $PUDL_OUTPUT/pudl_dbt_tests.duckdb

Then copy and paste the query into the duckdb CLI (you'll need to add a semicolon to the
end). This should show you the years and the expected and found row counts. If the
changes seem reasonable and expected, you can manually update these files, or you can
run the command:

.. code-block:: bash

    dbt_helper update-tables --target etl-full --row-counts --clobber {table_name}

This will tell the helper script to overwrite the existing row counts with new row
counts from the table in your local ``PUDL_OUTPUT`` stash. If you want to update the
``etl-fast`` row counts, use ``--target etl-fast`` instead of the default ``--target
etl-full``.

Debugging dbt test failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a more complex test that relies on custom SQL fails, we can debug it using
``duckdb``.  There are many ways to interact with ``duckdb``, here will use the CLI. See
the `here <https://duckdb.org/docs/installation/>`_ for installation directions. To
launch the CLI, navigate to the directory that your ``PUDL_OUTPUT`` environment variable
points to, and execute:

.. code-block:: bash

    duckdb pudl_dbt_tests.duckdb

For debugging purposes, we'll often want to execute portions of the compiled SQL
produced by ``dbt``. To find this, look at the output of the test failure, and you
should see a line under the test failure that looks like ``compiled code at
{path_to_sql}``.  Looking at this file, for a failing test that looks at weighted
quantiles, we might pull out the section:

.. code-block:: sql

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

This is where the weighted quantile is actually calculated. We can copy this into the
``duckdb`` CLI, add a semicolon to the end of the last line and hit ``Enter``. This
produces the output:

.. list-table::
   :header-rows: 1

   * - capacity_factor float
   * - 0.82587963

This is failing because the ``max_value`` is set to ``0.65``. If we change this value to
0.83, this test should now pass (though if this is an unexpected change in the
capacity factor, you would want to investigate why it changed before updating the
test threshold!)
