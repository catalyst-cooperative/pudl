.. _run-the-etl:

===============================================================================
Running the ETL Pipeline
===============================================================================

So you want to run the PUDL data processing pipeline? This is the most involved way
to get access to PUDL data. It's only recommended if you want to edit the ETL process
or contribute to the code base. Check out the :doc:`/data_access` documentation if you
just want to use the processed data.

These instructions assume you have already gone through the development setup
(see: :ref:`dev_setup`).

There are four main scripts that are involved in the PUDL processing pipeline:

1. ``ferc1_to_sqlite`` :doc:`converts the FERC Form 1 DBF files <clone_ferc1>` into a
   single large SQLite database so that the data is easier to extract.
2. ``pudl_etl`` is where the magic happens. This is the main script which
   coordinates the "Extract, Transform, Load" process that generates
   `Tabular Data Packages <https://frictionlessdata.io/specs/tabular-data-package/>`__.
3. ``datapkg_to_sqlite`` converts the Tabular Data Packages into a SQLite
   database. We recommend doing this for all of the smaller to medium sized tables,
   which is currently everything but the hourly EPA CEMS data.
4. ``epacems_to_parquet`` converts the (~1 billion row) EPA CEMS Data Package into
   Apache Parquet files for fast on-disk querying.

Settings files dictate which datasets, years, tables, or states get run through the
the processing pipeline. Two example settings files are provided in the ``settings``
folder that is created when you run ``pudl_setup``.

.. seealso::

    * :ref:`install-workspace` for more on how to create a PUDL data workspace.
    * :ref:`settings_files` for info details on the contents of the settings files.

The Fast ETL
------------
Running the fast ETL processes one year of data for each dataset. This is what
we do in our :doc:`software integration tests <testing>`.

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_fast.yml
    $ pudl_etl settings/etl_fast.yml
    $ datapkg_to_sqlite \
        datapkg/pudl-fast/ferc1/datapackage.json \
        datapkg/pudl-fast/epacems-eia/datapackage.json
    $ epacems_to_parquet --years 2019 --states ID -- \
        datapkg/pudl-fast/epacems-eia/datapackage.json

The Full ETL
------------
The full ETL setting file includes all the datasets with all of the years and
tables with the exception of EPA CEMS. A full ETL for EPA CEMS can take up to
15 hours of processing time, so the example setting here is all years of CEMS
for one state (Idaho!) and takes around 20 minutes to process.

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_full.yml
    $ pudl_etl settings/etl_full.yml
    $ datapkg_to_sqlite datapkg/pudl-full/ferc1/datapackage.json \
        datapkg/pudl-full/eia/datapackage.json
    $ epacems_to_parquet --states ID -- datapkg/pudl-full/epacems-eia/datapackage.json

Additional Notes
----------------
These commands should result in a bunch of Python :mod:`logging` output describing
what the script is doing, file outputs in the ``sqlite``, ``datapkg``, and
``parquet`` directories within your workspace. When the ETL is complete, you should
see new files at ``sqlite/ferc1.sqlite`` and ``sqlite/pudl.sqlite`` as well as a new
directory at ``datapkg/pudl-fast`` or ``datapkg/pudl-full`` containing several
datapackage directories -- one for each of the ``ferc1``, ``eia`` (Forms 860 and
923), and ``epacems-eia`` datasets.

Each of the data packages that are part of the bundle have metadata describing their
structure. This metadata is stored in the associated ``datapackage.json`` file.
The data are stored in a bunch of CSV files (some of which may be :mod:`gzip`
compressed) in the ``data/`` directories of each data package.

You can use the ``pudl_etl`` script to process more or different data by copying and
editing either of the settings files and running the script again with your new
settings file as an argument. Comments in the example settings file explain the
available parameters. Know that these example files are the only configurations that
are tested automatically and supported.

If you want to re-run ``pudl_etl`` and replace an existing bundle of data packages,
you can use ``--clobber``. If you want to generate a new data packages with a new or
modified settings file, you can change the name of the output datapackage bundle in
the configuration file.

All of the PUDL scripts have help messages if you want additional information (run
``script_name --help``).
