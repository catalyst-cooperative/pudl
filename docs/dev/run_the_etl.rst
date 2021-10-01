.. _run-the-etl:

===============================================================================
Running the ETL Pipeline
===============================================================================

So you want to run the PUDL data processing pipeline? This is the most involved way
to get access to PUDL data. It's only recommended if you want to edit the ETL process
or contribute to the code base. Check out the :doc:`/data_access` documentation if you
just want to use already processed data.

These instructions assume you have already gone through the development setup
(see: :ref:`dev_setup`).

There are two main scripts that are involved in the PUDL processing pipeline:

1. ``ferc1_to_sqlite`` :doc:`converts the FERC Form 1 DBF files <clone_ferc1>` into a
   single large `SQLite <https://sqlite.org>`__ database so that the data is easier
   to extract, and so all of the raw FERC Form 1 data is available in a modern format.
2. ``pudl_etl`` coordinates the "Extract, Transform, Load" process that processes
   20+ years worth of data from the FERC Form 1 database, dozens of EIA spreadsheets,
   and the thousands of CSV files that make up the EPA CEMS hourly emissions data into
   a clean, well normalized SQLite database (for the FERC and EIA data), and an `Apache
   Parquet <https://parquet.apache.org/>`__ dataset that is partitioned by state and
   year (for the EPA CEMS).

A settings file dictates which datasets, years, tables, or states get run through the
the processing pipeline. Two example settings files are provided in the ``settings``
folder that is created when you run ``pudl_setup``: the Fast ETL and the
Full ETL.

.. seealso::

    * :ref:`install-workspace` for more on how to create a PUDL data workspace.
    * :ref:`settings_files` for info details on the contents of the settings files.

The Fast ETL
------------
Running the Fast ETL processes one year of data for each dataset. This is what
we do in our :doc:`software integration tests <testing>`. Depending on your computer,
it should take around 15 minutes total.

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_fast.yml
    $ pudl_etl settings/etl_fast.yml

The Full ETL
------------
The Full ETL settings includes all all available data that PUDL can process. All
the years, all the states, and all the tables, including the ~1 billion record
EPA CEMS dataset. Assuming you already have the data downloaded, on a computer
with at least 16 GB of RAM, and a solid-state disk, he Full ETL including EPA
CEMS should take around 2 hours.

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_full.yml
    $ pudl_etl settings/etl_full.yml

Processing EPA CEMS Separately
------------------------------
If you don't want to process the EPA CEMS data (which takes about an hour and 20
minutes), you can delete or comment out its lines in the settings file, and
process it independently later without reprocessing the FERC and EIA data using
the ``epacems_to_parquet`` script, which will refer to your existing PUDL
database for the information it needs, as if the FERC and EIA ETL had just been
run.

.. warning::

    If you process the EPA CEMS data after the fact, be careful that the version
    of PUDL used to generate the DB is the same as the one you're using to
    process the CEMS data. Otherwise the process and data may be incompatible
    with unpredictable results.

Additional Notes
----------------
The commands above should result in a bunch of Python :mod:`logging` output
describing what the script is doing, and file outputs in the ``sqlite``,  and
``parquet`` directories within your workspace. When the ETL is complete, you
should see new files at ``sqlite/ferc1.sqlite`` and ``sqlite/pudl.sqlite`` as
well as a new directory at ``parquet/epacems`` containing nested directories
named by year and state.

You can use the ``pudl_etl`` script to process more or different data by copying
and editing either of the settings files and running the script again with your
new settings file as an argument. Comments in the example settings file explain
the available parameters. However, these examples are the only configurations
that are tested automatically and known to work reliably.

If you need to re-run ``ferc1_to_sqlite`` or ``pudl_etl`` and want to overwrite
their previous outputs you can use the ``--clobber`` option.  All of the PUDL
scripts have help messages if you want additional information (run ``script_name
--help``).
