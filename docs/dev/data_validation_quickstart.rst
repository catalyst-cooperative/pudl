.. _data_validation:

==========================
Data validation quickstart
==========================

Setup
-----

The ``dbt/`` directory contains the PUDL dbt project which manages our `data tests
<https://docs.getdbt.com/docs/build/data-tests>`__. To run dbt you'll need to have the
``pudl-dev`` conda environment activated (see :doc:`dev_setup`).

The data validation tests run on the Parquet outputs that are in your
``$PUDL_OUTPUT/parquet/`` directory. It's important that you ensure the outputs you're
testing are actually the result of the code on your current branch, otherwise you may
be surprised when the data test passes locally but fails in CI or the nightly builds.

We have a script, :mod:`pudl.scripts.dbt_helper`, to help with some common workflows.

Running the data validation tests
---------------------------------

``dbt_helper validate`` runs the data validation tests.
It's a wrapper around the official dbt tool, ``dbt build`` - see :ref:`dbt_build`.

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
      (*note* use an absolute path!)
   3. Run any of the ``dbt_helper`` commands you need.

   Some examples of useful Parquet outputs and where to find them:

   * `the most recent nightly builds <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl_parquet.zip>`__
   * the fast ETL outputs from your integration tests:
     these are in a temporary directory created by ``pytest``.
     Since these are already on your computer you don't need to download them.
     The path is printed out at the beginning of the ``pytest`` run and will look like:
     ``2025-07-25 16:05:49 [    INFO] test.conftest:386 Using temporary PUDL_OUTPUT:
     /path/to/your/temp/dir``
   * Any :ref:`branch_builds` outputs: if you have access to the internal build bucket,
     `builds.catalyst.coop
     <https://console.cloud.google.com/storage/browser/builds.catalyst.coop>`__,
     you can also use the Parquet files you find there.

.. _update_dbt_schema:

Updating table schemas
----------------------

dbt stores information about a table's schema and what tests are defined in a special
YAML file that you need to keep up to date.

That file lives in ``pudl/dbt/models/<data_source>/<table_name>/schema.yml``.

When you change a table's schema in ``pudl.metadata.resources``, you need to make a
matching change to the corresponding dbt YAML file.

In simple cases, ``dbt_helper`` can automatically update the schema of an existing
table with:

.. code-block:: bash

  dbt_helper update-tables --schema table_to_update

This will work so long as none of the columns being updated have data tests or other
manually defined metadata associated with them. If the script finds tests or metadata it
will abort, leaving the schema unchanged, and you will have to update the schema
manually, by editing the ``columns`` list in the appropriate ``schema.yml`` file. If you
want to destructively replace an existing schema **including any manually added tests or
metadata** you can use ``--clobber``:

.. code-block:: bash

  dbt_helper update-tables --schema --clobber table_to_replace_entirely

.. _row_counts:

Updating row counts
-------------------

To create or update the row count expectations for a given table you need to:

* Make sure a fresh version of the table is available in ``$PUDL_OUTPUT/parquet``.
  The expectations will be derived from what's observed in that file.
* Add ``check_row_counts_by_partition`` to the ``data_tests`` section
  of the the table's ``schema.yml``,
  if it isn't there already.

When ready to generate row count expectations,
the ``data_tests`` for a new table might look like this:

.. code-block:: yaml

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
