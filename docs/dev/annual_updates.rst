===============================================================================
Annual Updates
===============================================================================
Much of the data we work with is released in a "final" state annually. We typically
integrate the new year of data over 2-4 weeks in October of each year, since by that
time the final release for the previous year have been published by EIA and FERC.

As of fall 2021 the annual updates include:

* :doc:`/data_sources/eia860`
* :ref:`data-eia861`
* :doc:`/data_sources/eia923`
* :doc:`/data_sources/epacems`
* :doc:`/data_sources/ferc1`
* :ref:`data-ferc714`

This document outlines all the tasks required to complete the annual update, based on
our experience in 2021. You can look at :issue:`1255` to see all the commits that went
into integrating the 2020 data.

Obtaining New Raw Data
----------------------
* Scrape a new copy of the raw inputs from agency websites using the tools in the
  `pudl-scrapers repository <https://github.com/catalyst-cooperative/pudl-scrapers>`__.
  If the structure of the web pages or the URLs have changed, you may need to update
  the scrapers themselves.
* Use the newly scraped files and the tools in the
  `pudl-zenodo-storage repository <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
  to create new raw input achives on Zenodo. If we've
  previously archived the dataset this will add a new version to an existing archive.
* It is useful to run the ``zenodo_store.py`` script using the ``--noop`` option first
  to see what actions will be taken. Take note of any older years of data that have
  been changed, so that you can keep an eye out for those changes later in the data
  integration process.
* Update the dictionary of production DOIs in :mod:`pudl.workspace.datastore` to refer
  to the new raw input archives.
* Update the :py:const:`pudl.constants.WORKING_PARTITIONS` dictionary to reflect the
  data that is now available within each dataset.
* Update the years of data to be processed in the ``etl_full.yml`` and ``etl_fast.yml``
  settings files stored under ``src/pudl/package_data/settings`` in the PUDL repo.
  Note that you will also need to have your own working copies of the settings files
  that you edit throughout the process of integrating the new year of data.
* Use the ``pudl_datastore`` script to download the new raw data archives in bulk so
  that network hiccups don't cause issues during ETL.

Mapping the Structure of New Data
---------------------------------

FERC Form 1
^^^^^^^^^^^
The path to the directory containing the database files stored within the annual FERC 1
zipfiles changes from year to year, and will need to be updated for the new year of
data. We store this information in ``src/pudl/package_data/ferc1/file_map.csv``

The process we use for :doc:`clone_ferc1` uses the most recent annual database to
define the schema for our multi-year FERC 1 DB. This only works because historically the
FERC 1 DB has only added tables and columns over time. To check whether the new year of
data continues this pattern, you can run:

.. code-block:: bash

  pytest --etl_settings src/pudl/package_data/settings/etl_full.yml \
    test/integration/etl_test.py::test_ferc1_schema

EIA Forms 860/861/923
^^^^^^^^^^^^^^^^^^^^^
As with FERC Form 1, EIA often alters the structure of their published spreadsheets from
year to year. This includes changing file names; adding, removing, or re-ordering tabs;
changing the number of header and footer rows; and adding, removing, re-ordering, or
re-naming the data columns. We track this information in the following files:

* ``src/pudl/package_data/${data_source}/file_map.csv``: Paths (within the
  annual zip archive) to the files we parse.
* ``src/pudl/package_data/${data_source}/page_map.csv``: Mapping between the named
  spreadsheet pages we refer to across years, and the numerical index of that page
  within the workbook.
* ``src/pudl/package_data/${data_source}/skiprows.csv``: A per-page, per-year number of
  rows that should be skipped when reading the spreadsheet.
* ``src/pudl/package_data/${data_source}/skipfooter.csv``: A per-page, per-year number
  of rows that should be ignored at the end of the page when reading the spreadsheet.
* ``src/pudl/package_data/${data_source}/column_maps/${page_name}.csv``: A mapping
  from annual spreadsheet columns to consistent inter-year column names that we refer
  to in the raw dataframes during the extract step. The spreadsheet columns can be
  referred to either by their simplified ``snake_case`` column header (in ``eia860``,
  ``eia860m``, and ``eia923``) or numerical column index (``eia861``).

In the above ``${data_source}`` is one of our data source short codes (``eia860``,
``eia923`` etc.) and ``${page_name}`` is a label we use to refer to a given spreadsheet
tab over the years (e.g. ``boiler_fuel``). However ``page_name`` does not necessarily
correspond directly to PUDL database table names because we don't load the data from all
pages, and some pages result in more than one database table after normalization.

.. note::

    Sometimes EIA will change files published several years ago without providing any
    explanation. When creating new raw input archives for Zenodo, note which years of
    data have been altered so you can be particularly alert to changes in those files.

If files, spreadsheet pages, or individual columns with new semantic meanings have
appeared -- meaning they don’t correspond to any of the previously mapped files,
pages, or columns, then new mapping structures analogous to the above need to be created
to track their structure over time.

In all of the the above CSV files we use a value of ``-1`` to indicate that the data
does not exist in a given year.

Initial Data Extraction
-----------------------

FERC Form 1
^^^^^^^^^^^
At this point it should be possible to clone the all of the FERC 1 data (including the
new year) into SQLite with:

.. code-block:: bash

    ferc1_to_sqlite src/pudl/package_data/settings/etl_full.yml

This is necessary to enable mapping associations between the FERC 1 and EIA plants and
utilities later.

EIA Forms 860/861/923
^^^^^^^^^^^^^^^^^^^^^
It should also be possible to extract all years of data from the EIA 860/861/923
spreadsheets to generate raw dataframes. The Jupyter notebook
``devtools/eia-etl-debug.ipynb`` will let you run one step of the process at a time,
independently for each dataset. This makes debugging issues easier. Given that there are
hundreds of columns mapped across all the different EIA spreadsheets, you'll almost
certainly find some typos or errors in the extract process and need to revise your work.

Update table & column transformations
-------------------------------------

FERC Form 1
^^^^^^^^^^^
Some FERC 1 tables store different variables in different rows instead of or in addition
to using columns. Rows are identified by ``row_number``. What row number corresponds to
which variable changes from year to year.  We catalog this correspondence in the FERC 1
row maps, a collection of CSV files stored under
``src/pudl/package_data/ferc1/row_maps`` and organized by original FERC 1 DB table name.
The easiest way to check whether the data associated with a given row number has changed
is to look at the table's entries in the ``f1_row_lit_tbl`` table. This table stores the
descriptive strings associated with each row in the FERC Form 1, and also indicates the
last year that the string was changed in the ``row_chg_yr`` column. The
``devtools/ferc1/ferc1-new-year.ipynb`` notebook can make this process less tedious.

EIA Forms 860/861/923
^^^^^^^^^^^^^^^^^^^^^
Using the EIA ETL Debugging notebook you can attempt to run the initial transform step
on all tables of the new year of data and debug any failures. If any new tables were
added in the new year of data you will need to add a new transform function for the
corresponding dataframe. If new columns have been added, they should also be inspected
for cleanup.

Update the PUDL DB schema
-------------------------
If new columns or tables have been added, you will probably need to update the PUDL DB
schema, defining column types, giving them meaningful descriptions, applying appropriate
ENUM constraints, etc. This happens in the :mod:`pudl.metadata` subpackage. Otherwise
when the system tries to write dataframes into SQLite, it will fail.

You will need to differentiate between columns which should be harvested from the
transformed dataframes in the normalization and entity resolution process (and
associated with a generator, boiler, plant, utility, or balancing authority entity), and
those that should remain in the table where they are reported.

You may also need to define new coding/labeling tables, or add new codes  or code fixes
to the existing coding tables.

Run a Siloed EIA ETL
--------------------
Before moving on you should ensure that the EIA ETL is fully functional by running it
for all years and all EIA data sources. You'll need to create a temporary ETL settings
file that includes only the EIA data, and all available years of it. You may need to
debug inconsistencies in the harvested values.

Integration between datasets
----------------------------
Once you have a PUDL DB containing **ALL OF AND ONLY** the EIA data (including the new
year of data), and a cloned FERC 1 DB containing all years of available data, you can
start associating the plant & utility entities that are reported in the two datasets.

* FERC 1 to EIA plant & utility mappings

* Add location data for any plant that shows up in EPA CEMS but not in the EIA 860 data
  to ``src/pudl/package_data/epacems/additional_epacems_plants.csv``

Update the output routines
--------------------------
* Are there new columns that should be getting output?
* Are there new tables that need to have an output function?

Running the tests and Full ETL
------------------------------
* Run the FAST tests to make sure the new year of data can be processed
* Run the FULL ETL to generate complete FERC 1 & PUDL DBs, and EPA CEMS Parquet files.
* Run the FULL tests against these live resources to make sure all the years of data
  work with each other

Update data validations
-----------------------
* Update expected number of rows in the minmax_row validation tests.
* May need to update expected distribution of fuel costs

Run additional standalone analyses
----------------------------------
* If there are any important analyses that haven't been integrated into the CI tests
  yet they should be run with the new year of data for sanity checking.
* E.g. the state level hourly demand allocations, the EIA plant parts list
  generation.

Update documentation
--------------------
* README
* Release notes
* data source specific pages

Do a new software & data release
--------------------------------
