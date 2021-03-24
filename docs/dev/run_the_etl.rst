.. _run-the-etl:

=======================
Walk, Don't Run the ETL
=======================

So you want to run the PUDL data processing pipeline? This is the most involved
way to get access to PUDL data. It is only suggested if you want to edit the
ETL process or contribute to the code base. Check the basic usage page for the
other access methods (see: :ref:`basic-usage`). Measure twice, cut once.

These instructions assume you have already gone through the development setup
(see: :ref:`dev_setup`).

There are four main scripts that are involved in the PUDL processing pipeline:

1. `ferc1_to_sqlite` :doc: clones FERC Form 1's dbf files <clone_ferc1> as
   a sqlite database - a necessary prep extract step for FERC Form 1.
2. `pudl_etl` is where the magic happens. This is the main script which
   coordinates the "Extract, Transform, Load" steps which results in
   `Tabular Data Packages <https://frictionlessdata.io/specs/tabular-data-package/>`_.
3. `datapkg_to_sqlite` converts the Tabular Data Packages into a SQLite
   database. We recommend this for all of the small-medium tables which is
   currently everything but the hourly EPA CEMS data.
4. `epacems_to_parquet` converts the Tabular Data Packages into parquet files
   for the large datasets - which is currently the hourly EPA CEMS data.

Settings files dictate which datasets, years, tables or states get processed in
this processing pipeline. Two examples are provided in the ``settings`` folder
that is created when you run ``pudl_setup`` (see: :ref:`install-workspace` for
setup tools and see: :ref:`settings_files` for info on settings files). If you
want to run an ETL with different configurations, feel free to edit or
duplicate these files,

Fast ETL
--------
Running the fast ETL typically involves one-year of data for each dataset.

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_fast.yml
    $ pudl_etl settings/etl_fast.yml
    $ datapkg_to_sqlite \
        datapkg/pudl-fast/ferc1/datapackage.json \
        datapkg/pudl-fast/epacems-eia/datapackage.json
    $ epacems_to_parquet datapkg/pudl-fast/epacems-eia/datapackage.json


Full ETL
--------
The full ETL setting file includes all the datasets with all of the years and
tables with the exception of EPA CEMS. A full ETL for EPA CEMS can take up to
15 hours of processing time so the example setting here is all years of CEMS
for one state (Idaho!).

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_full.yml
    $ pudl_etl settings/etl_full.yml
    $ datapkg_to_sqlite datapkg/pudl-full/ferc1/datapackage.json \
        datapkg/pudl-full/eia/datapackage.json
    $ epacems_to_parquet datapkg/pudl-full/epacems-eia/datapackage.json

Additional Notes
----------------
These commands should result in a bunch of Python :mod:`logging` output,
describing what the script is doing, and outputs in the ``sqlite``,
``datapkg``, and ``parquet`` directories within your workspace. In particular,
you should see new files at ``sqlite/ferc1.sqlite`` and ``sqlite/pudl.sqlite``,
and a new directory at ``datapkg/pudl-fast`` or ``datapkg/pudl-full``
containing several datapackage directories, one for each of the ``ferc1``,
``eia`` (Forms 860 and 923), and ``epacems-eia`` datasets.

Each of the data packages which are part of the bundle have metadata describing
their structure, stored in a file called ``datapackage.json`` The data itself
is stored in a bunch of CSV files (some of which may be :mod:`gzip` compressed)
in the ``data/`` directories of each data package.

You can use the ``pudl_etl`` script to process more or different data by
copying and editing the ``settings/etl_one_year.yml`` file, and running the
script again with your new settings file as an argument. Comments in the
example settings file explain the available parameters. Know that these example
files are the only configurations that are tested and supported.

If you want to re-run ``pudl_etl`` and replace an existing bundle of data
packages, you can use ``--clobber``. If you want to generate a new data
packages with a new or modified settings file, you can change the name of the
output datapackage bundle in the configuration file.
