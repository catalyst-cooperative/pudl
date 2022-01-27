===============================================================================
Annual Updates
===============================================================================
Much of the data we work with is released in a "final" state annually. We typically
integrate the new year of data over 2-4 weeks in October of each year, since by that
time the final release for the previous year have been published by EIA and FERC.

As of fall 2021 the annual updates include:

* :doc:`/data_sources/eia860` (and eia860m)
* :ref:`data-eia861`
* :doc:`/data_sources/eia923`
* :doc:`/data_sources/epacems`
* :doc:`/data_sources/ferc1`
* :ref:`data-ferc714`

This document outlines all the tasks required to complete the annual update, based on
our experience in 2021. You can look at :issue:`1255` to see all the commits that went
into integrating the 2020 data.

Obtain fresh data
-----------------
Scrape a new copy of the raw PUDL inputs from agency websites using the tools in the
`pudl-scrapers repository <https://github.com/catalyst-cooperative/pudl-scrapers>`__.
If the structure of the web pages or the URLs have changed, you may need to update the
scrapers themselves.

Use the newly scraped files and the tools in the `pudl-zenodo-storage repository
<https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__ to create new raw input
achives on Zenodo. If we've previously archived the dataset this will add a new version
to an existing archive.

.. note::

    It is useful to run the ``zenodo_store.py`` script using the ``--noop`` option first
    to see what actions will be taken. Take note of any older years of data that have
    been changed, so that you can keep an eye out for those changes later in the data
    integration process.

Update the dictionary of production DOIs in :mod:`pudl.workspace.datastore` to refer to
the new raw input archives and update the :py:const:`pudl.constants.WORKING_PARTITIONS`
dictionary to reflect the data that is now available within each dataset.

Update the years of data to be processed in the ``etl_full.yml`` and ``etl_fast.yml``
settings files stored under ``src/pudl/package_data/settings`` in the PUDL repo.  Note
that you will also need to have your own working copies of the settings files that you
edit throughout the process of integrating the new year of data.

Use the ``pudl_datastore`` script to download the new raw data archives in bulk so that
network hiccups don't cause issues during ETL.

Mapping the Structure of New Data
---------------------------------

EIA Forms 860/860m/861/923
^^^^^^^^^^^^^^^^^^^^^^^^^^
EIA often alters the structure of their published spreadsheets from year to year. This
includes changing file names; adding, removing, or re-ordering spreadsheet pages;
changing the number of header and footer rows; and adding, removing, re-ordering, or
re-naming the data columns. We track this information in the following files which can
be found under ``src/pudl/package_data`` in the PUDL repository:

* ``${data_source}/file_map.csv``: Paths (within the annual zip archive) to the files we
  parse.
* ``${data_source}/page_map.csv``: Mapping between the named spreadsheet pages we refer
  to across years, and the numerical index of that page within the workbook.
* ``${data_source}/skiprows.csv``: A per-page, per-year number of rows that should be
  skipped when reading the spreadsheet.
* ``${data_source}/skipfooter.csv``: A per-page, per-year number of rows that should be
  ignored at the end of the page when reading the spreadsheet.
* ``${data_source}/column_maps/${page_name}.csv``: A mapping from annual spreadsheet
  columns to consistent inter-year column names that we refer to in the raw dataframes
  during the extract step. The spreadsheet columns can be referred to either by their
  simplified ``snake_case`` column header (in ``eia860``, ``eia860m``, and ``eia923``)
  or numerical column index (``eia861``).

Here ``${data_source}`` is one of our data source short codes (``eia860``, ``eia923``
etc.) and ``${page_name}`` is a label we use to refer to a given spreadsheet tab over
the years (e.g. ``boiler_fuel``). However ``page_name`` does not necessarily correspond
directly to PUDL database table names because we don't load the data from all pages, and
some pages result in more than one database table after normalization.

.. note::

    As mentioned above, sometimes EIA will change files published several years ago
    without providing any explanation. When creating new raw input archives for Zenodo,
    note which years of data have been altered so you can be particularly alert to
    changes in those files.

If files, spreadsheet pages, or individual columns with new semantic meanings have
appeared -- meaning they don’t correspond to any of the previously mapped files, pages,
or columns, then new mappings analogous to the above need to be created to track that
information over time.

In all of the the above CSV files we use a value of ``-1`` to indicate that the data
does not exist in a given year.

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

FERC Form 714
^^^^^^^^^^^^^
FERC Form 714 is distributed as an archive of CSV files, each of which spans all
available years of data. This means there's much less structure to keep track of. The
main thing that changes from year to year is the names of the CSV files within the ZIP
archive. The mapping between extracted dataframes and those filenames is currently
stored in the :py:const:`pudl.extract.ferc714.TABLE_FNAME` dictionary.

The character encodings of these CSV files vary, with some of them using ``iso-8859-1``
(Latin) rather than ``utf-8`` (Unicode). The per-file encoding is stored in
:py:const:`pudl.extract.ferc714.TABLE_ENCODING` and could change over time.

Initial Data Extraction
-----------------------

EIA Forms 860/860m/861/923
^^^^^^^^^^^^^^^^^^^^^^^^^^
It should also be possible to extract all years of data from the EIA 860/861/923
spreadsheets to generate raw dataframes. The Jupyter notebook
``devtools/eia-etl-debug.ipynb`` will let you run one step of the process at a time,
independently for each dataset. This makes debugging issues easier. Given that there are
hundreds of columns mapped across all the different EIA spreadsheets, you'll almost
certainly find some typos or errors in the extract process and need to revise your work.

FERC Form 1
^^^^^^^^^^^
At this point it should be possible to clone the all of the FERC 1 data (including the
new year) into SQLite with:

.. code-block:: bash

    ferc1_to_sqlite src/pudl/package_data/settings/etl_full.yml

This is necessary to enable mapping associations between the FERC 1 and EIA plants and
utilities later.

Update table & column transformations
-------------------------------------

EIA Forms 860/860m/861/923
^^^^^^^^^^^^^^^^^^^^^^^^^^
Using the EIA ETL Debugging notebook you can attempt to run the initial transform step
on all tables of the new year of data and debug any failures. If any new tables were
added in the new year of data you will need to add a new transform function for the
corresponding dataframe. If new columns have been added, they should also be inspected
for cleanup.

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

The ``plant_kind`` and ``construction_type`` fields in the ``plants_steam_ferc1`` table
and the ``fuel_type`` and ``fuel_unit`` fields in the ``fuel_ferc1`` table are reported
as freeform strings and need to be converted to simple categorical values to be useful.
If the new year of data contains strings that have never been encountered before, they
need to be added to the string cleaning dictionaries defined in
:mod:`pudl.transform.ferc1`. The ``devtools/ferc1/ferc1-new-year.ipynb`` notebook and
:func:`pudl.helpers.find_new_ferc1_strings` will help with this process. Every string
observed in these fileds should ultimately be mapped to one of the defined categories.

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

FERC 1 & EIA Plants & Utilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Once you have a PUDL DB containing **ALL OF AND ONLY THE EIA DATA** (including the new
year of data), and a cloned FERC 1 DB containing all years of available data, you can
start associating the plant & utility entities that are reported in the two datasets.

Refer to the :doc:`pudl_id_mapping` page for further instructions.

.. note::

    **All** FERC 1 respondent IDs and plant names and **all** EIA plant and utility IDs
    should end up in the mapping spreadsheet with PUDL plant and utility IDs, but only a
    small subset of them will end up being linked together with a shared ID. Only EIA
    plants with a capacity of more than 5 MW and EIA utilities that actually report data
    in the EIA 923 data tables are considered for linkage to their FERC Form 1
    counterparts. All FERC 1 plants and utilities should be linked to their EIA
    counterparts (there are far fewer of them).

Update missing EIA plant locations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If there are any plants that appear in the EPA CEMS dataset that do not appear in the
``plants_entity_eia`` table or that are missing latitute and longitude values, the
missing information should be compiled and added to
``src/pudl/package_data/epacems/additional_epacems_plants.csv`` to enable accurate
adjustment of the EPA CEMS timestamps to UTC. This information can usually be obtained
with the ``plant_id_eia`` and the
`EPA's FACT API <https://www.epa.gov/airmarkets/field-audit-checklist-tool-fact-api>`__
but in some cases you may need to resort to Google Maps. If no coordinates can be found
then at least the plant's state should be included, so that an approximate timezone can
be inferred.

Run the ETL
-----------
Once the FERC 1 and EIA utilities and plants have been associated with each other, you
can try and run the ETL with all datasets included. See: :doc:`run_the_etl`.

* First run the ETL for just the new year of data, using the ``etl_fast.yml`` settings
  file.
* Once the fast ETL works, run the full ETL using the ``etl_full.yml`` settings to
  populate complete FERC 1 & PUDL DBs and EPA CEMS Parquet files.

Update the output routines and run full tests
---------------------------------------------
With a full PUDL DB update the denormalized table outputs and derived analytical
routines to accommodate the new data if necessary. These are generally called from
within the :class:`pudl.output.pudltabl.PudlTabl` class.

* Are there new columns that should incorporated into the output tables?
* Are there new tables that need to have an output function defined for them?

To ensure that you (more) fully exercise all of the possible output functions, you
should run the entire CI test suite against your live databases with:

.. code-block:: bash

    tox -e full -- --live-dbs

Run and update data validations
-------------------------------
When the CI tests are passing against all years of data, sanity check the data in the
database and the derived outputs by running

.. code-block:: bash

    tox -e validate

We expect at least some of the validation tests to fail initially, because we haven't
updated the number of records we expect to see in each table. Updating the expected
number of records should be the last thing you do, as any other changes to the ETL
process are likely to affect those numbers.

You may also need to update the expected distribution of fuel prices if they were
particularly high or low in the new year of data. Other values like expected heat
content per unit of fuel should be relatively stable. If the required adjustments are
large, or there are other types of validations failing, they should be investigated.

When updating the expected number of rows in the minmax_row validation tests you should
pay attention to how far off of previous expectations the new tables are. E.g. if there
are already 20 years of data, and you're integrating 1 new year of data, probably the
number of rows in the tables should be increasing by around 5% (since 1/20 = 0.05).

Run additional standalone analyses
----------------------------------
If there are any important analyses that haven't been integrated into the CI tests yet
they should be run including the new year of data for sanity checking. For example
the :mod:`pudl.analysis.state_demand` script or generating the EIA Plant Parts List
for integration with FERC 1 data.

Update the documentation
------------------------
Once the new year of data is integrated, the documentation should be updated to reflect
the new state of affairs. This will include updating at least:

* the top-level :doc:`README </index>`
* the :doc:`/release_notes`
* any updated :doc:`data sources </data_sources/index>`
