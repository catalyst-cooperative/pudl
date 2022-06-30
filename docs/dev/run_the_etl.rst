.. _run-the-etl:

===============================================================================
Running the ETL Pipeline
===============================================================================

So you want to run the PUDL data processing pipeline? This is the most involved way
to get access to PUDL data. It's only recommended if you want to edit the ETL process
or contribute to the code base. Check out the :doc:`/data_access` documentation if you
just want to use already processed data.

These instructions assume you have already gone through the :ref:`dev_setup`.

There are two main scripts involved in the PUDL processing pipeline:

1. ``ferc1_to_sqlite`` :doc:`converts the FERC Form 1 DBF files <clone_ferc1>` into a
   single large `SQLite <https://sqlite.org>`__ database so that the data is easier
   to extract, and so all of the raw FERC Form 1 data is available in a modern format.
2. ``pudl_etl`` coordinates the "Extract, Transform, Load" process that processes
   20+ years worth of data from the FERC Form 1 database, dozens of EIA spreadsheets,
   and the thousands of CSV files that make up the EPA CEMS hourly emissions data into
   a clean, well normalized SQLite database (for the FERC and EIA data), and an `Apache
   Parquet <https://parquet.apache.org/>`__ dataset that is partitioned by state and
   year (for the EPA CEMS).

Settings Files
--------------
These scripts use YAML settings files in place of command line arguments. This avoids
undue complexity and preserves a record of how the script was run. The YAML file
dictates which datasets, years, tables, or states get run through the the processing
pipeline. Two example files are deployed in the ``settings`` folder that is created when
you run ``pudl_setup``. (see: :ref:`install-workspace`).

- ``etl_fast.yml`` processes one year of data
- ``etl_full.yml`` processes all years of data

Each file contains instructions for how to process the data under
"full" or "fast" conditions. You can copy, rename, and modify these files to suit your
needs. Updating the ``name``, ``title``, and ``description`` fields will
ensure that new ``pudl-etl`` script outputs won't override old ones. The layout of
these files is depicted below:

.. code-block::

      # FERC1 to SQLite settings
      ferc1_to_sqlite_settings:
        ├── dataset f2s parameter (e.g. tables) : editable list of tables
        ├── dataset f2s parameter (e.g. years) : editable list of years

      # PUDL ETL settings
      name : unique name identifying the etl outputs
      title : short human readable title for the etl outputs
      description : a longer description of the etl outputs
      datasets:
        ├── dataset name
        │    ├── dataset etl parameter (e.g. tables) : editable list of tables
        │    └── dataset etl parameter (e.g. years) : editable list of years
        └── dataset name
        │    ├── dataset etl parameter (e.g. states) : editable list of states
        │    └── dataset etl parameter (e.g. years) : editable list of years

.. note::

    Do not change anything other than the dataset parameters and the name, title, and
    description fields unless you want to remove an entire dataset. For example, CEMS
    data takes a long time to load so you can comment out or delete all settings
    pertaining to CEMS. See below for a way to add it later.

Both scripts enable you to choose which **years** and **tables** you want to include:

.. list-table::
   :header-rows: 1
   :widths: auto

   * - Parameter
     - Description
   * - ``years``
     - A list of years to be included in the FERC Form 1 Raw DB or the PUDL DB. You
       should only use a continuous range of years. Check the :doc:`/data_sources/index`
       pages for the earliest available years.
   * - ``tables``
     - A list of strings indicating what tables to load. The list of acceptable
       tables can be found in the the example settings file. **We recommend including
       all the tables for a given dataset due to dependences. The impact on runtime is
       negligible.**

The ``pudl_etl`` script CEMS data does not allow you to select which tables to include
(it's just the one) but it does allow you to select **years** and **states**.

.. list-table::
   :header-rows: 1
   :widths: auto

   * - Parameter
     - Description
   * - ``years``
     - A list of the years you'd like to process CEMS data for. You should
       only use a continuous range of years. Check the :doc:`/data_sources/epacems` page
       for the earliest available years.
   * - ``states``
     - A list of the state codes you'd like to process CEMS data for. You can specify
       ``all`` if you want to process data for all states. This may take a while!

.. seealso::

      For an exhaustive listing of the available parameters, see the ``etl_full.yml``
      file.

There are a few notable dependencies to be wary of when fiddling with these
settings:

- EPA CEMS cannot be loaded without EIA data unless you have existing PUDL database
  containing EIA. This is because CEMS relies on IDs from EIA860

- EIA Forms 860 and 923 are very tightly related. You can load only EIA 860, but the
  settings verification will automatically add in a few 923 tables that are needed
  to generate the complete list of plants and generators. The settings verification
  will also automatically add all 860 tables if only 923 is specified. This is
  because of the harvesting process that standardizes duplicate and deviant data
  between the two sources.

.. warning::

    If you are processing the EIA 860/923 data, we **strongly recommend**
    including the same years in both datasets. We only test two combinations of
    inputs, as specified by the ``etl_fast.yml`` and ``etl_full.yml`` settings
    distributed with the package.  Other combinations of years may yield
    unexpected results.

Now that your settings are configured, you're ready to run the scripts

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
with at least 16 GB of RAM, and a solid-state disk, the Full ETL including EPA
CEMS should take around 2 hours.

.. code-block:: console

    $ ferc1_to_sqlite settings/etl_full.yml
    $ pudl_etl settings/etl_full.yml

Custom ETL
----------
You've changed the settings and renamed the file to CUSTOM_ETL.yml

.. code-block:: console

    $ ferc1_to_sqlite settings/CUSTOM_ETL.yml
    $ pudl_etl settings/CUSTOM_ETL.yml


Processing EPA CEMS Separately
------------------------------
As mentioned above, CEMS takes a while to process. Luckily, we've designed PUDL so that
if you delete or comment out CEMS lines in the settings file, you can process it
independently later without reprocessing the FERC and EIA data. The following script
will refer to your existing PUDL database for the information it needs and act as if the
FERC and EIA ETL had just been run. This may go without saying, but you need an existing
PUDL DB with the appropriate EIA files in order for the script to work.

.. code-block:: console

    $ epacems_to_parquet -y [YEARS] -s [STATES]

This script does not have a YAML settings file, so you must specify which years and
states to include via command line arguments. Run ``epacems_to_parquet --help`` to
verify your options. Changing CEMS settings in a YAML file will not inform this script!
Running the script without any arguments will automatically process all states and
years.

.. warning::

    If you process the EPA CEMS data after the fact (i.e., with the
    ``epacems_to_parquet`` script), be careful that the version of PUDL used to generate
    the DB is the same as the one you're using to process the CEMS data. Otherwise the
    process and data may be incompatible with unpredictable results.

Additional Notes
----------------
The commands above should result in a bunch of Python :mod:`logging` output
describing what the script is doing, and file outputs in the ``sqlite``,  and
``parquet`` directories within your workspace. When the ETL is complete, you
should see new files at ``sqlite/ferc1.sqlite`` and ``sqlite/pudl.sqlite`` as
well as a new directory at ``parquet/epacems`` containing nested directories
named by year and state.

If you need to re-run ``ferc1_to_sqlite`` or ``pudl_etl`` and want to overwrite
their previous outputs you can add ``--clobber`` (run ``script_name --clobber``).
All of the PUDL scripts also have help messages if you want additional information
(run ``script_name --help``).
