.. _data_validation:

================================================================================
Data Validation Tests
================================================================================

Quickstart
----------

Setup
~~~~~

The ``dbt/`` directory contains the PUDL dbt project which manages our `data tests
<https://docs.getdbt.com/docs/build/data-tests>`__. To run dbt you'll need to have the
``pudl-dev`` conda environment activated (see :doc:`dev_setup`).

The data validation tests run on the Parquet outputs that are in your
``$PUDL_OUTPUT/parquet/`` directory. It's important that you ensure the outputs you're
testing are actually the result of the code on your current branch, otherwise you may
be surprised when the data test fails in CI or the nightly builds.

We have a script, :mod:`pudl.scripts.dbt_helper`, to help with some common workflows.

Updating table schemas
~~~~~~~~~~~~~~~~~~~~~~

dbt stores information about a table's schema and what tests are defined
in a special YAML file that you need to keep up to date.

That file lives in ``pudl/dbt/models/<data_source>/<table_name>/schema.yml``.

When you change a table's schema in ``pudl.metadata.resources``,
you need to make sure that file is up to date as well.

For now, you have to update the columns manually,
by editing the ``columns`` list in the appropriate schema file.

.. TODO 2025-08-19 Add `dbt_helper update-tables --schema` usage here.

Updating row counts
~~~~~~~~~~~~~~~~~~~

To create or update the row count expectations for a given table you need to:

* Make sure a fresh version of the table is available ``$PUDL_OUTPUT/parquet``. The
  expectations will be derived from what's observed in that file.
* Add ``check_row_counts_by_partition`` to the ``data_tests`` section of the the table's
  ``schema.yml``.

The initial ``data_tests`` for a new table might look like this:

.. code-block:: yaml

    version: 2
    sources:
      - name: pudl
        tables:
          - name: new_table_name
            data_tests:
              - check_row_counts_per_partition:
                  table_name: new_table_name
                  partition_expr: "EXTRACT(YEAR FROM report_date)"

Then you can run:

.. code-block:: bash

    dbt_helper update-tables --row-counts new_table_name

If this is a brand new table, you should see changes appear in
``dbt/seeds/etl_full_row_counts.csv``. If you're updating the row counts for a table
that already exists, you'll need to use the ``--clobber`` option to make the script
overwrite existing row counts:

.. code-block:: bash

    dbt_helper update-tables --row-counts --clobber new_table_name

.. warning::

  You should rarely if ever need to edit the row-counts file directly. It needs to be
  kept sorted to minimize diffs in git, and manually calculating and editing row counts
  is both tedious and error prone.

Running data validation tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``dbt_helper validate`` runs the data validation tests.
It's a wrapper around the official dbt tool, ``dbt build`` - see :ref:`_dbt_build`.

``dbt_helper validate`` provides rich output when a test fails,
and allows us to use the `Dagster asset selection syntax
<https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference>`__.

Example usage:

.. code-block:: bash

    # for *all* assets
    dbt_helper validate
    # for just a single asset
    dbt_helper validate --asset-select "key:out_eia__yearly_generators"
    # for this asset as well as all upstream assets
    dbt_helper validate --asset-select "+key:out_eia__yearly_generators"
    # same as above, but skip row counts
    dbt_helper validate --asset-select "+key:out_eia__yearly_generators" --exclude "*check_row_counts*"

See ``dbt_helper validate --help`` for usage details.

.. tip::

   You may want to run the validation tests against multiple sets of Parquet files.

   To do this:

   1. Download the Parquet files to ``<any_directory_you_want>/parquet/``.
   2. Set the ``PUDL_OUTPUT`` environment variable to ``<any_directory_you_want>``.
   3. Run any of the ``dbt_helper`` commands you need.

   Some examples of useful Parquet outputs and where to find them:

   * `the most recent nightly builds <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/>`__
   * the fast ETL outputs from your integration tests:
     these are in a temporary directory created by ``pytest``.
     Since these are already on your computer you don't need to download them.
     The path is printed out at the beginning of the ``pytest`` run and will look like:
     ``2025-07-25 16:05:49 [    INFO] test.conftest:386 Using temporary PUDL_OUTPUT:
     /path/to/your/temp/dir``
   * Any :ref:`_branch_build` outputs: if you have access to the internal build bucket,
     `builds.catalyst.coop
     <https://console.cloud.google.com/storage/browser/builds.catalyst.coop>`__,
     you can also use the Parquet files you find there.


--------------------------------------------------------------------------------
Data validation guidelines
--------------------------------------------------------------------------------

We use a tool called `dbt <https://www.getdbt.com/>`__ to manage our data validation
tests. dbt is a SQL-based data transformation tool that can be used to manage large data
pipelines, but we're currently only using it for data validation.

In dbt, every data validation test is a SQL query meant to select rows that fail the
test. A successful test will return no results. The query can be parametrized so it can
be reused across multiple tables. dbt includes a few `built-in data test definitions
<https://docs.getdbt.com/docs/build/data-tests>`__, and the `dbt_expectations
<https://github.com/metaplane/dbt-expectations>`__ package provides many more. We also
define our own `custom data tests
<https://docs.getdbt.com/best-practices/writing-custom-generic-tests>`__.

.. note:: Why use dbt and SQL instead of Python?

   Modern analytical query engines are extremely fast and memory efficient. By using dbt
   with `DuckDB <https://duckdb.org/>`__ to query the PUDL `Parquet
   <https://parquet.apache.org/>`__ outputs we're able to run thousands of validations
   across hundreds of tables with billions of rows in a minute, instead of the 2-3 hours
   it used to take our much less extensive validation tests to run.

.. todo::

   We should chat about the minimum level of data validation that we expect for a table.

   * All dbt tests should pass meaningfully on both the Full ETL and Fast ETL outputs,
     so that we have a chance of catching data issues in CI before the nightly builds.
     The one exception to this is the row count checks -- they only apply to the Full
     ETL outputs.
   * All tables should have row count checks, unless they have a non-deterministic
     number of rows. Tables with non-deterministic row counts should have their length
     checked with `expect_table_row_count_to_be_between <https://github.com/metaplane/dbt-expectations?tab=readme-ov-file#expect_table_row_count_to_be_between>`_
   * There should be no entirely null columns in the Full ETL outputs. For tables that
     contain deprecated columns with no data in the recent years processed by the Fast
     ETL, per-year nullness expectations should be added. See the
     :mod:`pudl.scripts.pudl_null_cols` script.
   * What else?

--------------------------------------------------------------------------------
Example of typical data validation workflow
--------------------------------------------------------------------------------

* Explain that the dbt tests are looking at whatever Parquet files are in
  ``$PUDL_OUTPUT``.
* Need to be aware of the state of your Dagster outputs and how they relate to the
  branch / code that you're working on. If you have mixed outputs from multiple
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

.. _dbt_build:

Running dbt directly
~~~~~~~~~~~~~~~~~~~~

dbt has its own much more `extensive documentation <https://docs.getdbt.com/>`__. PUDL
uses only a small subset of its features


To run all of the data validation tests, from within the ``dbt/`` directory run:

.. code-block:: bash

   dbt build

For more fine-grained control, you can use the ``--select`` option to run only the tests
defined for a particular table, or all instances of a particular test no matter what
table it's associated with. Or you can combine the two to run just a particular test
on a particular table. Some examples:

.. code-block:: bash

   # Run all tests defined for the out_eia__monthly_generators table
   dbt build --select "source:pudl.out_eia__monthly_generators"
   # Run all instances of the expect_columns_not_all_null test
   dbt build --select "test_name:expect_columns_not_all_null"
   # Run expect_columns_not_all_null test on the out_eia__monthly_generators table only
   dbt build --select "test_name:expect_columns_not_all_null,source:pudl.out_eia__monthly_generators"
   # Use a wildcard "*" to run all tests on tables whose names start with out_eia923__
   dbt build --select "source:pudl.out_eia923__*"

Similarly, you can exclude individual tables or tests using ``--exclude``. One case
where this is useful is running the data validation tests against the outputs of the
fast ETL. We do not store expected row-counts for the fast ETL outputs, and so generally
expect the row-count checks to fail. To run all of the data validation tests except for
the row counts and avoid seeing all those spurious failures you could run:

.. code-block:: bash

   dbt build --exclude "test_name:check_row_counts_per_partition"

For more options, see the `dbt selection syntax documentation
<https://docs.getdbt.com/reference/node-selection/syntax>`__.

.. note::

   The dbt tests can be run in parallel to speed them up with the ``--threads`` argument
   but this sometimes results in spurious errors like "too many files open" which are
   not related to the data being tested.

.. note::

   There are a handful of data validation tests that have been implemented using
   `Dagster's asset checks <https://docs.dagster.io/guides/test/asset-checks>`__.
   Typically these tests weren't well suited to SQL, weren't performance bottlenecks,
   and had already been implemented in Python. E.g. :func:`pudl.validate.no_null_rows`.


Data validation in our integration tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dbt data tests are invoked by ``pytest`` as part of our integration tests. This
means they run as part of our continuous integration (CI) checks before a PR can be
merged into ``main``. However, the CI only processes 1-2 years of data, so when the
tests run in CI, they're only checking a small subset of the data we publish. We also
don't run the row count checks in CI, since the fast ETL outputs are more changeable
and less informative than those in the nightly builds.

This means that when you're developing a new table or updating an existing table, it's
important to manually run the dbt tests on the new data in its entirety before the
changes are merged into ``main``.

If the data validations fail in the ``pytest`` integration tests, they should produce
helpful output indicating what failed and why, in the same way as ``dbt_helper
validate``

.. _branch_builds:

Branch builds
~~~~~~~~~~~~~

Depending on your computer, running the full ETL locally can be extremely time consuming
and may run into memory limits. It's also easy to accidentally end up with local outputs
that are the result of code from multiple different branches, and so may not be
consistent with each other. If you're only altering a few tables, rematerializing them
in Dagster and then running the specific dbt tests that apply to them and any tables
downstream of them should work fine.

Kicking off a branch build
^^^^^^^^^^^^^^^^^^^^^^^^^^

When we're doing big quarterly or annual updates, and dozens or hundreds of tables are
changing simultaneously, it is helpful to be able to run the full ETL from scratch, run
all of the data validation tests against the outputs, and use the results to update the
test parameters (especially expected row counts) appropriately. This can be done by
manually kicking off a PUDL deployment on your branch.

To initiate a branch build, in the PUDL repo on GitHub go to `Actions
<https://github.com/catalyst-cooperative/pudl/actions>`__ and select `build-deploy-pudl
<https://github.com/catalyst-cooperative/pudl/actions/workflows/build-deploy-pudl.yml>`__.
On the right hand side select Run Workflow and then select your branch in the dropdown
and click the Run Workflow button. Shortly thereafter you should see a notification in
the ``pudl-deployments`` channel in our Slack saying that the build has kicked off. It
should take about 3 hours to complete. You can track its progress and watch the logs in
the `Google Cloud Console
<https://console.cloud.google.com/monitoring/dashboards/builder/992bbe3f-17e6-49c4-a9e8-8f1925d4ec24>`__.

Getting fresh row counts from a branch build
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To catch unexpected changes to the data, we keep track of the expected number of rows in
each data table we distribute. These expectations are stored in
``dbt/seeds/etl_full_row_counts.csv`` and they can be updated using the ``dbt_helper``
script based on the observed row counts in your local PUDL Parquet outputs. If you can't
run the full ETL locally, the nightly builds / branch build also generate updated row
count expectations. After a branch build completes, you can download the updated
``etl_full_row_counts.csv`` file from the build outputs that are uploaded to
``gs://builds.catalyst.coop/<build-id>/etl_full_row_counts.csv`` See the
:doc:`nightly_data_builds` documentation for more details on accessing the nightly build
outputs. Replace the ``etl_full_row_counts.csv`` in your local PUDL git repo with the
one you've downloaded and use ``git diff`` to see what has changed. Make sure to review
the row count changes closely to see if there's anything unexpected.

.. _pudl_dbt_quirks:

PUDL Specific Design Choices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Our usage of dbt is slightly unusual, since we rely on Dagster to coordinate our data
pipeline, and are only using dbt for data validation. Some quirks of our setup to be
aware of:

* From dbt's point of view, the PUDL tables are
  `sources <https://docs.getdbt.com/docs/build/sources>`__ -- external tables about
  which it knows very little other than the table and column names. It assumes the
  tables will be available, rather than trying to create them. In a typical dbt project,
  most tables would be defined as `models <https://docs.getdbt.com/docs/build/models>`__
  which are somewhat analogous to `Dagster assets
  <https://docs.dagster.io/guides/build/assets/defining-assets>`__.
* As a SQL-based tool, dbt generally expects to be querying a database. However, in our
  case the tables are stored as Apache Parquet files, which we query with SQL via
  DuckDB. This means some of dbt's functionality is not available. For example, we can't
  use `the dbt adapter object
  <https://docs.getdbt.com/reference/dbt-jinja-functions/adapter>`__ in our test
  definitions because it relies on being able to access the underlying database schema,
* One exception to this is any intermediate tables that are defined as dbt models (see
  below). These will be created as materialized views in a DuckDB database at
  ``$PUDL_OUTPUT/pudl_dbt_tests.duckdb``. Any time you need to refer to those tables
  while debugging, you'll need to be connected to that database.

--------------------------------------------------------------------------------
Debugging data validation failures
--------------------------------------------------------------------------------

* Using output from ``dbt_helper validate``.
* By inspecting and running the compiled SQL yourself.
* Explain What "compiled" SQL means here.
* Using ``--store-failures`` and the ``pudl_dbt_tests.duckdb`` output -- what is
  stored in that database anyway?
* Using ``duckdb < path/to/compiled.sql``
* Using DuckDB's ``.read path/to/compiled.sql`` to play with data interactively.
* Go through a simpler example before getting into the complicated quantile checks test.

Debugging quantile checks
~~~~~~~~~~~~~~~~~~~~~~~~~

.. todo::

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
Applying pre-defined validations to existing data
--------------------------------------------------------------------------------

Applying an existing generic test to an existing table should be as easy as editing
the ``schema.yml`` file associated with that table, and adding a new test specification
to the ``data_tests`` section of either the table as a whole or an individual column.
The ``schema.yml`` for ``table_name`` can be found at
``dbt/models/{data_source}/{table_name}/schema.yml``.

In general, table-level tests depend on multiple columns or test some property of the
table as a whole, while column-level tests typically depend only on values with the
column they are applied to.

Pre-defined tests
~~~~~~~~~~~~~~~~~
Our dbt project includes `dbt-utils <https://github.com/dbt-labs/dbt-utils>`__ and
`dbt-expectations <https://github.com/metaplane/dbt-expectations>`__ as dependencies.
These packages include a bunch of useful tests that can be applied to any table.
There are several examples of applying tests from ``dbt-expectations`` in
``dbt/models/vcerare/out_vcerare__hourly_available_capacity_factor/schema.yml``
and in general they will look like the below. Each item in a ``data_tests`` section
defines a single test, and may provide named parameters for the test. The tests whose
names have the ``dbt_expectations`` prefix come from that package.

.. code-block:: yaml

    version: 2
    sources:
      - name: pudl
        tables:
          - name: out_vcerare__hourly_available_capacity_factor
            data_tests:
              - expect_columns_not_all_null
              - check_row_counts_per_partition:
                  table_name: out_vcerare__hourly_available_capacity_factor
                  partition_expr: report_year
              - expect_valid_hour_of_year
              - expect_unique_column_combination:
                  columns:
                    - county_id_fips
                    - datetime_utc
            columns:
              - name: state
                data_tests:
                  - not_null
              - name: place_name
                data_tests:
                  - not_null
                  - dbt_expectations.expect_column_values_to_not_be_in_set:
                      value_set:
                        - bedford_city
                        - clifton_forge_city
                        - lake_hurron
                        - lake_st_clair
                  - dbt_expectations.expect_column_values_to_be_in_set:
                      value_set:
                        - oglala lakota
                      row_condition: "county_id_fips = '46012'"
              - name: datetime_utc
                data_tests:
                  - not_null
                  - dbt_expectations.expect_column_values_to_not_be_in_set:
                      value_set:
                        - "{{ dbt_date.date(2020, 12, 31) }}"
              - name: report_year
                data_tests:
                  - not_null
              - name: hour_of_year
                data_tests:
                  - not_null
                  - dbt_expectations.expect_column_max_to_be_between:
                      min_value: 8760
                      max_value: 8760


Tests defined within PUDL
~~~~~~~~~~~~~~~~~~~~~~~~~

Some of the tests in the example above like ``expect_columns_not_all_null`` or
``check_row_counts_per_partition`` are defined by us, and can be found in the SQL
files with the same name under ``dbt/tests/data_tests/generic_tests/``

Documentation for the tests that we define is in
``dbt/tests/data_tests/generic_tests/schema.yml``

.. todo::

   * Integrate documentation of our existing generic tests into the docs build.

--------------------------------------------------------------------------------
Adding new tables
--------------------------------------------------------------------------------

The tables that exist within PUDL are defined by the data structures within
:mod:`pudl.metadata.resources`. Any Dagster asset that's being written out to Parquet
or the PUDL SQLite database needs to be defined there. The ``schema.yml`` files within
our dbt project are derived from that same PUDL metadata. Our unit tests check to make
sure that the dbt schemas haven't drifted away from the canonical PUDL metadata. To make
sure that the two sets of database table descriptions stay in sync, we try to create and
update the dbt schemas programmatically when possible.

Using ``dbt_helper update-tables``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To add a new PUDL table to the dbt project, you must add it as a `dbt
source <https://docs.getdbt.com/docs/build/sources>`__. The ``dbt_helper`` script
automates the initial setup with the ``update-tables`` subcommand.

To add a new table called ``new_table_name`` that has already been defined as a resource
that will be written out to Parquet in the PUDL metadata:

.. code-block:: bash

    dbt_helper update-tables --schema new_table_name

This will add a file called ``dbt/models/{data_source}/new_table_name/schema.yml``. You
can also give it a list of tables and they will all be created at once.  This yaml file
tells ``dbt`` about the table and its schema, but initially it will not have any data
validations defined. Tests need to be added by hand.

Initial data tests
~~~~~~~~~~~~~~~~~~

There are a few tests that we apply to every table, which should be defined as soon as
you've added a new table. These include ``check_row_counts_by_partition`` and
``expect_columns_not_all_null``.


Checking for entirely null columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A test we apply to basically all tables is ``expect_columns_not_all_null``. In
its most basic form it verifies that there are no columns in the table which are
completely null, since that is typically indicative of a bad ``ENUM`` constraint, a
column naming error, or a bad merge, and should be investigated. To add this basic
default, you add the test to the table level ``data_tests`` with no parameters, which
building on the above example would look like:

.. code-block:: yaml

    version: 2
    sources:
      - name: pudl
        tables:
          - name: new_table_name
            data_tests:
              - expect_columns_not_all_null
              - check_row_counts_per_partition:
                  table_name: new_table_name
                  partition_expr: "EXTRACT(YEAR FROM report_date)"

--------------------------------------------------------------------------------
Defining new data validation tests
--------------------------------------------------------------------------------

Sometimes you will want to test a property that can't be expressed
using the existing dbt tests like ``check_row_counts_per_partition`` (in
``dbt/tests/data_tests/generic_tests``) or the tests in `dbt_expectations
<https://hub.getdbt.com/metaplane/dbt_expectations/latest/>`__ or `dbt_utils
<https://hub.getdbt.com/dbt-labs/dbt_utils/latest/>`__.

In those cases you'll need to define a new *type* of data validation test using
dbt!

Writing tests in dbt means they'll be located next to all the other data
validation we're defining in the dbt schemas, which is nice. They also tend to
be quite performant.

In a few rare cases you may need to write the check with access to all of
the tools within Python. In those cases, you can use `Dagster's asset checks
<https://docs.dagster.io/guides/test/asset-checks>`__, but in general we prefer
using dbt tests.

How do I write a new dbt test?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A dbt test is a templated SQL query that runs on your output data to look for
problems in the data. The query should be designed to return no rows if there
are no problems with the data. If the query returns any rows at all, then the
test will fail.

The test will need to live as a templated piece of SQL within
``pudl/dbt/tests/data_tests/generic_tests``. dbt has `official docs
<https://docs.getdbt.com/best-practices/writing-custom-generic-tests>`__ for
doing this, but the core steps are:

1. Check to see whether the test you need is already provided by `dbt-utils
   <https://hub.getdbt.com/dbt-labs/dbt_utils/latest/>`__ or `dbt-expectations
   <https://github.com/metaplane/dbt-expectations>`__.
2. Make a file called ``pudl/dbt/tests/data_tests/generic_tests/your_test.sql``.
3. Add ``{% test your_test(some_test_params...) %}`` to the top of the file
   and ``{% endtest %}`` to the end. By default, if a test is defined at the
   **table** level, it will receive the ``model`` parameter; if it's defined
   at the **column** level, it will receive both ``model`` and ``column``
   parameters; and you can add more custom parameters in the test signature
   which will be read out of the schema YAML.
4. Write a SQL ``SELECT`` statement that returns any data that would fail your
   test, as well as useful debugging information. See our existing tests in
   ``dbt/tests/data_tests/generic_tests`` to see some common patterns.
   ``dbt/tests/data_tests/generic_tests/expect_consistent_years.sql`` may be
   of particular use as a simple example that returns useful debugging context
   along with the failing rows.

If you're not already familiar with SQL, here are some useful resources:

* `Interactive Mode SQL Tutorial <https://mode.com/sql-tutorial>`__
* `Greg Wilson's Querynomicon <https://third-bit.com/sql/>`__
* `Interactive DuckDB SQL Tutorial <https://motherduckdb.github.io/sql-tutorial/>`__
* `DuckDB SQL Introduction <https://duckdb.org/docs/stable/sql/introduction.html>`__
* `SQL for Data Scientists <https://www.oreilly.com/library/view/sql-for-data/9781119669364/>`__ (book)

.. note::

  Refer to :ref:`pudl_dbt_quirks` above for an explanation of some details of our dbt
  setup that may affect what functionality is available when writing new tests.

Testing the Tests
~~~~~~~~~~~~~~~~~

OK, now you have a new test, which *seems* to be working.
How can we check to make sure it's doing what we want?

dbt has robust macro testing tools, and tests are basically macros,
but unfortunately you still have to jump through a couple hoops:

1. Pull the test logic out into a macro
2. Use the test as a *very* thin wrapper around the logic macro
3. Test the logic macro

First, we pull the test logic out into a macro (let's call it ``your_macro()``):

1. Move the test file from above to ``pudl/dbt/macros/your_macro.sql``
2. Replace ``{% test your_macro(...) %}`` with ``{% macro your_macro(...) %}``
3. Replace ``{% endtest %}`` with ``{% endmacro %}``

The logic macro is now available to use in tests. Next, use the test as a
wrapper around the logic macro you just wrote. Make the test file read
like this:


.. code-block:: jinja

  {% test your_test(model, custom_param) %}

  {{ your_macro(model, custom_param) }}

  {% endtest %}

This makes it a very simple wrapper that allows the test logic to be accessed
from a ``data_tests`` block within the schema.

Finally, write a test in ``pudl/dbt/tests/unit_tests/test_your_macro.sql``. This
SQL file doesn't need any special ``{% ... %}`` stuff in it.

The structure is easiest to explain with an example. Let's walk through a test
that checks if the row-counts macro is working as expected:

.. code-block:: sql

  WITH test_row_counts AS (
      SELECT * FROM (VALUES
          ('test_table', 2022, 1),
          ('test_table', 2023, 1),
      ) AS t(table_name, partition, row_count)
  ),

Here, ``test_row_counts`` is setting up the expected row counts per partition.
We use that ``SELECT * FROM (VALUES`` construction to make a temporary SQL table
with that literal data - 2 rows saying that "``test_table`` should have 1 row in
2022 and 1 in 2023". Continuing on:

.. code-block:: sql

  test_table AS (
      SELECT * FROM (VALUES
          (2022, 'x'),
          (2023, 'x'),
      ) AS t(report_year, dummy_col)
  ),

Here, we define ``test_table``, the actual table we're counting rows for. You
can see we've added one row for 2022 and one for 2023 - so we expect the test
to pass! Next:

.. code-block:: sql

  expected_mismatch_counts as (
      SELECT * FROM (VALUES
          ('test_table', 0),
      ) AS t(table_name, num_mismatches)
  ),

We're saying here that ``expected_mismatch_counts`` is 0 - there are *no*
partitions where we expect there to be a mismatch. Next, we call the macro:

.. code-block:: jinja

  result_comparison AS (
      SELECT (SELECT COUNT(*)
      FROM ({{
          row_counts_per_partition('test_table', 'test_table', 'report_year', force_row_counts_table='test_row_counts')
      }})) as observed_mismatch_count,
      num_mismatches AS expected_mismatch_count,
      FROM expected_mismatch_counts
  )

This one is a bit more complicated.

Let's start from the macro call ``{{ row_counts_per_partition(...) }}``. This
gets us one row per partition that has a mismatched number of rows between
the expected row counts (``test_row_counts``) and the observed row counts in
``test_table``.

Then we wrap that in ``SELECT COUNT(*)`` which tells us how many rows that macro
call returned (in this case, 0).

Finally, we wrap that in ``SELECT (SELECT COUNT(*) FROM ...) as
observed_mismatch_count ...``. That makes a table where the columns are the
observed mismatch count (0, as counted by the macro) and the expected mismatch
count (directly pulled from the ``expected_mismatch_counts`` table we set up
earlier). Finally we are ready to actually run the top-level ``SELECT`` - much
like other tests, we are looking for problem rows - if the ``SELECT`` returns 0
rows that means a passing test:

.. code-block:: sql

  SELECT *
  FROM result_comparison
  WHERE observed_mismatch_count != expected_mismatch_count

So if we observe a different number of mismatched partitions than what we
expect, this test will fail. We can repeat this structure with different input
data to cover many different use cases of the macro.

If the test is particularly weird and hard to get good debug
info for, you can add custom debug handlers for your test type in
:func:`pudl.dbt_wrapper.build_with_context`, which gives you access to the full
power of Python.


Creating intermediate tables for a test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes you'll need to do a test in two steps. For example, if you want to
use a column test (such as ``expect_quantile_constraints``) on the ratio of two
columns, you will need to calculate that ratio as a separate column.

This can be done by creating a new `dbt model
<https://docs.getdbt.com/docs/build/models>`__ that materializes an
intermediate table you want to execute tests on. Add a SQL file to
``dbt/models/{data_source}/{source_table_name}/{intermediate_table_name}.sql``
containing a ``SELECT`` statement that builds your new table. For
example, if you need to divide the ``source_table_name.a`` column by
``source_table_name.b``::

  select
  a / b as my_ratio
  from {{ source('pudl', 'source_table_name') }}

Then add the model to the ``schema.yml`` file under the ``models`` top-level
key, and define tests exactly as you would for a ``source`` table. See
``models/ferc1/out_ferc1__yearly_steam_plants_fuel_by_plant_sched402`` for an
example of
this pattern.

Note: when adding a model, it will be stored as a SQL ``view`` in the file
``$PUDL_OUTPUT/pudl_dbt_tests.duckdb``.

--------------------------------------------------------------------------------
Unmigrated Data Validation Docs (cannibalize)
--------------------------------------------------------------------------------

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

    dbt_helper update-tables --row-counts --clobber {table_name}

This will tell the helper script to overwrite the existing row counts with new row
counts from the table in your local ``PUDL_OUTPUT`` stash.

Debugging dbt test failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a more complex test that relies on custom SQL fails, we can debug it using
``duckdb``.  There are many ways to interact with ``duckdb``, here will use the CLI. See
the `here <https://duckdb.org/docs/installation/>`__ for installation directions. To
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
