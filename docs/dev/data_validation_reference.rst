=========================
Data validation reference
=========================

--------
Overview
--------

In dbt, every data validation test is a SQL query meant to select rows that fail the
test. A successful test will return no results. The query can be parametrized so it can
be reused across multiple tables. dbt includes a few `built-in data test definitions
<https://docs.getdbt.com/docs/build/data-tests>`__, and the `dbt_expectations
<https://github.com/metaplane/dbt-expectations>`__ package provides many more. We also
define our own `custom data tests
<https://docs.getdbt.com/best-practices/writing-custom-generic-tests>`__.

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

----------------
Common workflows
----------------

* Typical workflow: tweaking an asset, remateralize it in Dagster, re-run the data
  validations that pertain to just that table.
* Often also useful to rematerialize the changed table and all of its downstream
  dependencies, and then run the data validations on all of those downstream
  dependencies to see if there were any unforeseen consequences.

.. _pudl_dbt_quirks:

----------------------------
PUDL Specific Design Choices
----------------------------

Our usage of dbt is slightly unusual, since we rely on Dagster to coordinate our data
pipeline, and are only using dbt for data validation. Some quirks of our setup to be
aware of:

* From dbt's point of view, the PUDL tables are all
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
  definitions because it relies on being able to access the underlying database schema.
* One place we use true dbt models instead of sources is when
  we define intermediate tables to simplify test definitions.
  See :ref:`intermediate_tables`.
  These intermediate tables are created as materialized views in a DuckDB database
  at ``$PUDL_OUTPUT/pudl_dbt_tests.duckdb``.
  In this case, the underlying database schema *will* be accessible to dbt.
  Additionally, any time you need to refer to those tables while debugging,
  you'll need to be connected to that database.


.. _branch_builds:

-------------
Branch builds
-------------

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

.. _dbt_build:

--------------------
Running dbt directly
--------------------

dbt has its own much more `extensive documentation <https://docs.getdbt.com/>`__.
PUDL uses only a small subset of its features.


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


----------------------------------------
Data validation in our integration tests
----------------------------------------

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


--------------------------------------------------------------------------------
Debugging data validation failures
--------------------------------------------------------------------------------

So, you've run the data validations, but one or more of them has failed. Now what?

We'll go over some general strategies first, then look at the two most common
failures: row counts and quantiles.

General strategies and tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``dbt_helper validate``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you habitually use ``dbt``, try using ``dbt_helper validate`` instead -- it
will print out additional context information to help with debugging failures.

**Example: validation ``expect_columns_not_all_null`` on table
``out_eia__yearly_generators``**

If you run this validation in ``dbt``, it tells you:

* the test failed
* there was 1 failure row
* the compiled SQL query for the test is at
  ``target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql``

& that's it:

.. code-block:: console

    $ dbt build --select source:pudl.out_eia__yearly_generators,test_name:expect_columns_not_all_null
    20:38:56  Running with dbt=1.10.6
    20:38:57  Registered adapter: duckdb=1.9.4
    20:38:57  Unable to do partial parsing because config vars, config profile, or config target have changed
    20:39:00  Found 2 models, 700 data tests, 1 seed, 240 sources, 850 macros
    20:39:00
    20:39:00  Concurrency: 1 threads (target='etl-full')
    20:39:00
    20:39:00  1 of 1 START test source_expect_columns_not_all_null_pudl_out_eia__yearly_generators_False__EXTRACT_year_FROM_report_date_2008__[...]EXTRACT_year_FROM_report_date_2009  [RUN]
    20:39:01  1 of 1 FAIL 1 source_expect_columns_not_all_null_pudl_out_eia__yearly_generators_False__EXTRACT_year_FROM_report_date_2008__[...]EXTRACT_year_FROM_report_date_2009  [FAIL 1 in 0.15s]
    20:39:01
    20:39:01  Finished running 1 test in 0 hours 0 minutes and 0.21 seconds (0.21s).
    20:39:01
    20:39:01  Completed with 1 error, 0 partial successes, and 0 warnings:
    20:39:01
    20:39:01  Failure in test source_expect_columns_not_all_null_pudl_out_eia__yearly_generators_False__EXTRACT_year_FROM_report_date_2008__[...]EXTRACT_year_FROM_report_date_2009 (models/eia/out_eia__yearly_generators/schema.yml)
    20:39:01    Got 1 result, configured to fail if != 0
    20:39:01
    20:39:01    compiled code at target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql
    20:39:01
    20:39:01  Done. PASS=0 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=1

It doesn't tell you what the failure row was; you'd have to run the compiled
query yourself to figure that out (see below for details on what that means and
how to do it).

If you run this validation in ``dbt_helper``, it shows you the dbt output, but
it **also** runs the compiled SQL query and gives you the results:

.. code-block:: console

    $ dbt_helper validate --select source:pudl.out_eia__yearly_generators,test_name:expect_columns_not_all_null
    [...]
    20:37:49  Finished running 1 test in 0 hours 0 minutes and 0.17 seconds (0.17s).
    20:37:49
    20:37:49  Completed with 1 error, 0 partial successes, and 0 warnings:
    20:37:49
    20:37:49  Failure in test source_expect_columns_not_all_null_pudl_out_eia__yearly_generators_False__EXTRACT_year_FROM_report_date_2008__[...]EXTRACT_year_FROM_report_date_2009 (models/eia/out_eia__yearly_generators/schema.yml)
    20:37:49    Got 1 result, configured to fail if != 0
    20:37:49
    20:37:49    compiled code at target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql
    20:37:49
    20:37:49  Done. PASS=0 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=1
    Traceback (most recent call last):
    [...]
    AssertionError: failure contexts:
    source_expect_columns_not_all_null_pudl_out_eia__yearly_generators_False__EXTRACT_year_FROM_report_date_2008__[...]EXTRACT_year_FROM_report_date_2009:

    | table_name                 | failing_column   | failure_reason                         | row_condition                         |   total_rows_matching_condition |   non_null_count |
    |:---------------------------|:-----------------|:---------------------------------------|:--------------------------------------|--------------------------------:|-----------------:|
    | out_eia__yearly_generators | unit_id_pudl     | Conditional check failed: EXTRACT(year | EXTRACT(year FROM report_date) < 2008 |                          136918 |                0 |
    |                            |                  | FROM report_date) < 2008               |                                       |                                 |                  |

This saves you a step. Most times, this is enough to figure out what has gone
wrong, and you never need to look at the compiled SQL query at all.

Inspect the SQL query for the test and run it yourself
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some tests have very long failure row output that ``dbt_helper``
doesn't handle very well. To debug those failures, you will need to
run the SQL yourself, and explore duckdb's `output formatting options
<https://duckdb.org/docs/stable/clients/cli/output_formats.html>`__ to display
the results legibly.

Some tests have terrible failure row output that doesn't tell you anything
useful. To debug those failures, you will need to bust into the SQL to pull out
enough information to figure out what went wrong.

Both of these cases require you to touch the compiled SQL query for the test directly.

dbt gives you a path for the "compiled code" for the failing test, in a line
that looks like this:

.. code-block:: console

    20:37:49    compiled code at target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql

"Compiled" is important here because the source code for each test is merely
a template. The template cannot directly be used to query the database. Each
instance of a test is configured in the schema.yml for the table being tested.
dbt compiles the SQL query for that test by filling in the template values using
information from the test config. It saves the resulting SQL query to a new file
in the ``target/compiled`` directory. This is the compiled query.

Running this query in duckdb will generate the failure row output. There are two
ways to run the query.

Run once using the shell
++++++++++++++++++++++++

You can run duckdb against the test database, and input the compiled code path
using ``<``.

If you are in the ``pudl`` working directory, you may need to add ``dbt/`` to
the front of the compiled code path. Like this:

.. code-block:: console

    $ duckdb $PUDL_OUTPUT/pudl_dbt_tests.duckdb <dbt/target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql
    ┌────────────────────────────┬────────────────┬─────────────────────────────────────────────────────────────────┬───────────────────────────────────────┬───────────────────────────────┬────────────────┐
    │         table_name         │ failing_column │                         failure_reason                          │             row_condition             │ total_rows_matching_condition │ non_null_count │
    │          varchar           │    varchar     │                             varchar                             │                varchar                │             int64             │     int64      │
    ├────────────────────────────┼────────────────┼─────────────────────────────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────┤
    │ out_eia__yearly_generators │ unit_id_pudl   │ Conditional check failed: EXTRACT(year FROM report_date) < 2008 │ EXTRACT(year FROM report_date) < 2008 │            136918             │       0        │
    └────────────────────────────┴────────────────┴─────────────────────────────────────────────────────────────────┴───────────────────────────────────────┴───────────────────────────────┴────────────────┘

The advantage of this approach is that it is very quick, and it immediately
returns you to a shell.

**Variation: change output modes**

To transpose very wide output, consider setting ``.mode``. Using the ``-cmd``
argument to duckdb will execute a command before processing input provided using
``<``. Like this:

.. code-block:: console

    $ duckdb -cmd '.mode line' $PUDL_OUTPUT/pudl_dbt_tests.duckdb <dbt/target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql
                       table_name = out_eia__yearly_generators
                   failing_column = unit_id_pudl
                   failure_reason = Conditional check failed: EXTRACT(year FROM report_date) < 2008
                    row_condition = EXTRACT(year FROM report_date) < 2008
    total_rows_matching_condition = 136918
                   non_null_count = 0

There are lots of `output format modes available <https://duckdb.org/docs/stable/clients/cli/output_formats.html>`__; hopefully one of them will be legible for your failure row output!


Run inside a duckdb session
+++++++++++++++++++++++++++

You can open a duckdb session against the test database, and input the compiled
code path using the duckdb ``.read`` command. Like this:

.. code-block:: console

    $ duckdb $PUDL_OUTPUT/pudl_dbt_tests.duckdb
    v1.2.0 5f5512b827
    Enter ".help" for usage hints.
    D .read dbt/target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_columns_not_all__790ceaac9ad08187ce2e9323e6b58961.sql
    ┌────────────────────────────┬────────────────┬─────────────────────────────────────────────────────────────────┬───────────────────────────────────────┬───────────────────────────────┬────────────────┐
    │         table_name         │ failing_column │                         failure_reason                          │             row_condition             │ total_rows_matching_condition │ non_null_count │
    │          varchar           │    varchar     │                             varchar                             │                varchar                │             int64             │     int64      │
    ├────────────────────────────┼────────────────┼─────────────────────────────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────┤
    │ out_eia__yearly_generators │ unit_id_pudl   │ Conditional check failed: EXTRACT(year FROM report_date) < 2008 │ EXTRACT(year FROM report_date) < 2008 │            136918             │       0        │
    └────────────────────────────┴────────────────┴─────────────────────────────────────────────────────────────────┴───────────────────────────────────────┴───────────────────────────────┴────────────────┘

You can type other SQL queries and duckdb commands at the duckdb prompt as well.

The advantage of this approach is that you are at a database prompt, and can
immediately run other queries to narrow down what has gone wrong.

The disadvantage of this approach is that you have to remember to quit (CTRL-D
or ``.quit``) before you can run more dbt commands. duckdb does not like having
multiple programs accessing the database simultaneously.

Dealing with terrible failure row output
++++++++++++++++++++++++++++++++++++++++

If the failure row output for a test says something useless like "false" with
no other identifying information, you'll need to actually read the SQL query and
adapt some portion of it to give you the context you need.

Such as:

.. code-block:: console

    $ duckdb $PUDL_OUTPUT/pudl_dbt_tests.duckdb <target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/dbt_expectations_source_expect_33dc33ad0a260e896f11f41b4422dda8.sql
    ┌─────────────┐
    │ expression  │
    │   boolean   │
    ├─────────────┤
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │   ·         │
    │   ·         │
    │   ·         │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    │ false       │
    ├─────────────┤
    │ 570499 rows │
    │ (20 shown)  │
    └─────────────┘

The only thing this tells us is that there are 570,499 rows that failed the test.
We need to know which rows they are in order to debug further.

Opening ``dbt_expectations_source_expect_33dc33ad0a260e896f11f41b4422dda8.sql``
in a pager or text editor yields the following:

.. code-block:: sql

        with grouped_expression as (
        select



      unit_id_pudl is not null as expression


        from '/Users/catalyst/pudl_output/parquet/out_eia__yearly_generators.parquet'


    ),
    validation_errors as (

        select
            *
        from
            grouped_expression
        where
            not(expression = true)

    )

    select *
    from validation_errors

Here we'd have several options:

* Add a few primary key columns to the ``grouped_expression`` table, so that
  they pop out in the final ``select``
* Adapt the inner ``select`` from ``grouped_expression`` to work on its own
* Grab only the parquet path and put it in a custom query like ``select
  plant_id_eia, generator_id, report_date from {parquet path} where unit_id_pudl
  is null``

The query you build using any of the above could be copied and pasted into a
duckdb session, and the results interrogated further from there.

.. _row-countfailures:

Debugging and fixing row count failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Row count checks fail when
the local data has a different number of rows in it than dbt expected.

There are two cases to consider:

* you expect the local data to have a new number of rows
* you expect the local data to have the same number of rows as before

If you expect the local data to have a new number of rows -
you're adding a new year of data,
you're changing how the data is filtered or dropped,
etc.
Then it's a good thing if the row count check fails at first.
That means you have some new rows!
First, we can use ``dbt_helper`` to count up the new row counts,
and see how they differ from the old ones.

.. code-block:: console

    $ dbt_helper update-tables <TABLE_NAME> --row-counts --clobber # we use --clobber so the changes are actually written to disk
    $ git diff # to see the difference

You may see that a row count for a partition has been added:

.. code-block:: diff

    diff --git a/dbt/seeds/etl_full_row_counts.csv b/dbt/seeds/etl_full_row_counts.csv
    index d9a5f0ec7..2b40f3ad7 100644
    --- a/dbt/seeds/etl_full_row_counts.csv
    +++ b/dbt/seeds/etl_full_row_counts.csv
    @@ -3318,7 +3318,7 @@ out_ferc1__yearly_steam_plants_fuel_sched402,2020,1250
     out_ferc1__yearly_steam_plants_fuel_sched402,2021,1152
     out_ferc1__yearly_steam_plants_fuel_sched402,2022,1196
     out_ferc1__yearly_steam_plants_fuel_sched402,2023,1210
    +out_ferc1__yearly_steam_plants_fuel_sched402,2024,1221
     out_ferc1__yearly_steam_plants_sched402,1994,1411
     out_ferc1__yearly_steam_plants_sched402,1995,1448
     out_ferc1__yearly_steam_plants_sched402,1996,1395

This is what you'd expect if you were adding a new year of data.
In this case,
we want to double-check if that is a reasonable number of rows for a new partition.
This will be different for each table,
but consider these heuristic questions:

* How does this compare to previous years?
  Do I expect there to be more rows, fewer rows, or about the same number?
* Have I made changes to how the data is filtered or merged with other data?
* Did other partitions change, too, or did I just see a new partition?

If any of those questions raise alarm bells,
you should probably look at the actual output data in a notebook.
If, after investigation, you're sure the row counts are correct,
commit the changes to the expected row counts.

You might also get a row count test failure when you don't expect it.
That will probably look like a change in row count for one or more partitions:

.. code-block:: diff

    diff --git a/dbt/seeds/etl_full_row_counts.csv b/dbt/seeds/etl_full_row_counts.csv
    index d9a5f0ec7..2b40f3ad7 100644
    --- a/dbt/seeds/etl_full_row_counts.csv
    +++ b/dbt/seeds/etl_full_row_counts.csv
    @@ -3318,7 +3318,7 @@ out_ferc1__yearly_steam_plants_fuel_sched402,2020,1250
     out_ferc1__yearly_steam_plants_fuel_sched402,2021,1152
     out_ferc1__yearly_steam_plants_fuel_sched402,2022,1196
    -out_ferc1__yearly_steam_plants_fuel_sched402,2023,1210
    +out_ferc1__yearly_steam_plants_fuel_sched402,2023,1215
    -out_ferc1__yearly_steam_plants_fuel_sched402,2024,1224
    +out_ferc1__yearly_steam_plants_fuel_sched402,2024,1221
     out_ferc1__yearly_steam_plants_sched402,1994,1411
     out_ferc1__yearly_steam_plants_sched402,1995,1448
     out_ferc1__yearly_steam_plants_sched402,1996,1395

If you don't expect your code to have caused these row-count changes,
it's time to investigate.
There's likely a bug somewhere.
Investigation will be different for each table,
but here are some ideas to get you started:

* Check that everything is up to date -
  do you have the latest changes from ``main`` in your branch?
  Did you re-materialize your asset using fresh upstream data?
* Compare your local data to the data in the last nightly build:
  Using the primary key,
  merge the two tables and see what rows are in both,
  what rows are only in the nightly build,
  and what rows are only in your local build.

Once you understand why the row counts are different,
either fix the bug or commit the new expected row counts.

Debugging quantile checks
^^^^^^^^^^^^^^^^^^^^^^^^^

Run the quantile check by selecting the table you want to check.
If you want to check all the tables, you can instead select all the quantile checks.
Note that we're using ``--select`` to use **dbt** selection syntax,
not ``--asset-select`` for **Dagster** selection syntax.

.. code-block:: console

    $ dbt_helper validate --select "test_name:expect_quantile_constraints"

In this example, we're running quantile checks for ``out_eia__yearly_generators``.

.. code-block:: console

    $ dbt_helper validate --select "source:pudl_dbt.pudl.out_eia__yearly_generators"
    [...]
    18:39:46  Finished running 24 data tests in 0 hours 0 minutes and 1.01 seconds (1.01s).
    18:39:46
    18:39:46  Completed with 1 error, 0 partial successes, and 0 warnings:
    18:39:46
    18:39:46  Failure in test source_expect_quantile_constraints_pudl_out_eia__yearly_generators_capacity_factor___quantile_0_65_min_value_0_5_max_value_0_6____quantile_0_15_min_value_0_005____quantile_0_95_max_value_0_95___fuel_type_code_pudl_gas_and_report_date_CAST_2015_01_01_AS_DATE_and_capacity_factor_0_0__capacity_mw (models/eia/out_eia__yearly_generators/schema.yml)
    18:39:46    Got 1 result, configured to fail if != 0
    18:39:46
    18:39:46    compiled code at target/compiled/pudl_dbt/models/eia/out_eia__yearly_generators/schema.yml/source_expect_quantile_constra_392a2df5d1590fb6bc46821e0b879c86.sql
    18:39:46
    18:39:46  Done. PASS=23 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=24
    Traceback (most recent call last):
      File "/Users/catalyst/bin/miniforge3/envs/pudl-dev/bin/dbt_helper", line 7, in <module>
        sys.exit(dbt_helper())
                 ~~~~~~~~~~^^
      File "/Users/catalyst/bin/miniforge3/envs/pudl-dev/lib/python3.13/site-packages/click/core.py", line 1161, in __call__
        return self.main(*args, **kwargs)
               ~~~~~~~~~^^^^^^^^^^^^^^^^^
      File "/Users/catalyst/bin/miniforge3/envs/pudl-dev/lib/python3.13/site-packages/click/core.py", line 1082, in main
        rv = self.invoke(ctx)
      File "/Users/catalyst/bin/miniforge3/envs/pudl-dev/lib/python3.13/site-packages/click/core.py", line 1697, in invoke
        return _process_result(sub_ctx.command.invoke(sub_ctx))
                               ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^
      File "/Users/catalyst/bin/miniforge3/envs/pudl-dev/lib/python3.13/site-packages/click/core.py", line 1443, in invoke
        return ctx.invoke(self.callback, **ctx.params)
               ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/Users/catalyst/bin/miniforge3/envs/pudl-dev/lib/python3.13/site-packages/click/core.py", line 788, in invoke
        return __callback(*args, **kwargs)
      File "/Users/catalyst/Documents/work/catalyst/pudl/src/pudl/scripts/dbt_helper.py", line 673, in validate
        raise AssertionError(
            f"failure contexts:\n{test_result.format_failure_contexts()}"
        )
    AssertionError: failure contexts:
    source_expect_quantile_constraints_pudl_out_eia__yearly_generators_capacity_factor___quantile_0_65_min_value_0_5_max_value_0_6____quantile_0_15_min_value_0_005____quantile_0_95_max_value_0_95___fuel_type_code_pudl_gas_and_report_date_CAST_2015_01_01_AS_DATE_and_capacity_factor_0_0__capacity_mw:

     table: source.pudl_dbt.pudl.out_eia__yearly_generators
     test: expect_quantile_constraints
     column: capacity_factor
     row_condition: fuel_type_code_pudl='gas' and report_date>=CAST('2015-01-01' AS DATE) and capacity_factor<>0.0
     weight column: capacity_mw
     description: Historical note, EIA natural gas reporting really only becomes usable in 2015.
      quantile |     value |       min |       max
          0.65 | 0.4638245 |     0.500 |      0.60
          0.15 | 0.0246494 |     0.005 |      None
          0.95 | 0.7754576 |      None |      0.95

In this example, quantile 0.65 was expected to be between 0.5 and 0.6,
but was instead 0.46, outside of the expected range.

Locate the quantile check in the table's ``schema.yml`` file.
This will be at ``dbt/models/<data_source>/<table_name>/schema.yml``.

Find the column name and the row condition in the failure output.
In this example, the check we want is for column ``capacity_factor``,
and it's the entry with the row condition
``fuel_type_code_pudl='gas'
and report_date>=CAST('2015-01-01' AS DATE)
and capacity_factor<>0.0``.

.. code-block:: console

  [pudl/dbt] $ $EDITOR models/eia/out_eia__monthly_generators/schema.yml

Depending on the situation, from here you can:

* investigate further in a Python notebook -
  how has this data changed from the version in the nightly builds which passed
  this check?
* ask folks if we expect these quantiles to have shifted
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
^^^^^^^^^^^^^^^^^
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
                  arguments:
                    table_name: out_vcerare__hourly_available_capacity_factor
                    partition_expr: report_year
              - expect_valid_hour_of_year
              - expect_unique_column_combination:
                  arguments:
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
                      arguments:
                        value_set:
                          - bedford_city
                          - clifton_forge_city
                          - lake_hurron
                          - lake_st_clair
                  - dbt_expectations.expect_column_values_to_be_in_set:
                      arguments:
                        value_set:
                          - oglala lakota
                        row_condition: "county_id_fips = '46012'"
              - name: datetime_utc
                data_tests:
                  - not_null
                  - dbt_expectations.expect_column_values_to_not_be_in_set:
                      arguments:
                        value_set:
                          - "{{ dbt_date.date(2020, 12, 31) }}"
              - name: report_year
                data_tests:
                  - not_null
              - name: hour_of_year
                data_tests:
                  - not_null
                  - dbt_expectations.expect_column_max_to_be_between:
                      arguments:
                        min_value: 8760
                        max_value: 8760


Tests defined within PUDL
^^^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a new PUDL table to the dbt project, you must add it as a `dbt
source <https://docs.getdbt.com/docs/build/sources>`__. The ``dbt_helper`` script
automates the initial setup with the ``update-tables`` subcommand.

Before adding a table as a dbt source, you need to:

* define that table as a resource in :mod:`pudl.metadata.resources`
* make sure that table is written out to Parquet

Then you can use the ``dbt_helper update-tables`` command to initialize the file.

.. code-block:: bash

    dbt_helper update-tables --schema new_table_name

This will add a file called ``dbt/models/{data_source}/new_table_name/schema.yml``. You
can also give it a list of tables and they will all be created at once.  This yaml file
tells ``dbt`` about the table and its schema, but initially it will not have any data
validations defined. Tests need to be added by hand.

Initial data tests
^^^^^^^^^^^^^^^^^^

There are a few tests that we apply to every table,
which should be defined as soon as you've added a new table.
These include ``check_row_counts_by_partition`` and ``expect_columns_not_all_null``.
We talk about ``check_row_counts_by_partition`` in :ref:`row_counts`.


Checking for entirely null columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A test we apply to basically all tables is ``expect_columns_not_all_null``. In
its most basic form it verifies that there are no columns in the table which are
completely null, since that is typically indicative of a bad ``ENUM`` constraint, a
column naming error, or a bad merge, and should be investigated. To add this basic
default, you add the test to the table level ``data_tests`` with no parameters:

.. code-block:: yaml

    version: 2
    sources:
      - name: pudl
        tables:
          - name: new_table_name
            data_tests:
              - expect_columns_not_all_null

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Testing your new test
^^^^^^^^^^^^^^^^^^^^^

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


.. _intermediate_tables:

Creating intermediate tables for a test
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
