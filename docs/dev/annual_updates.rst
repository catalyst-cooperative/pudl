===============================================================================
Annual Updates
===============================================================================
Much of the data we work with is released in a "final" state annually. We typically
integrate the new year of data over 2-4 weeks in October of each year, since by that
time the final release for the previous year have been published by EIA and FERC. We
also integrate EIA early release data when available. The ``data_maturity`` field will
indicate whether the data is final or provisional.

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

1. Obtain Fresh Data
--------------------
**1.1)** Scrape a new copy of the raw PUDL inputs from agency websites using the tools
in the
`pudl-scrapers repository <https://github.com/catalyst-cooperative/pudl-scrapers>`__.
If the structure of the web pages or the URLs have changed, you may need to update the
scrapers themselves.

**1.2)** Use the newly scraped files and the tools in the `pudl-zenodo-storage
repository <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__ to create
new raw input achives on Zenodo. If we've previously archived the dataset this will add
a new version to an existing archive.

.. note::
    It is useful to run the ``zenodo_store.py`` script using the ``--noop`` option first
    to see what actions will be taken. In addition to the new year, **take note of any
    older years of data that have been retroactively changed** so that you can address
    them later on in the file mapping and transform updates.

**1.3)** Update the dictionary of production DOIs in :mod:`pudl.workspace.datastore` to
refer to the new raw input archives.

**1.4)** Update the working partitions in the :mod:`pudl.metadata.sources` dictionary to
reflect the years of data that are available within each dataset.

**1.5)** Update the years of data to be processed in the ``etl_full.yml`` and
``etl_fast.yml`` settings files stored under ``src/pudl/package_data/settings`` in the
PUDL repo.

**1.6)** Update the settings files in your PUDL workspace to reflect the new
years by running ``pudl_setup {path to your pudl_work directory} -c``. Don't worry, it
won't remove any custom settings files you've added under a diffrent name.

.. note::

    EIA861 is not yet included in the ETL, so you can skip 1.5 if you're updating 861.
    You can also skip all steps after 3.

**1.6)** Use the ``pudl_datastore`` script (see :doc:`datastore`) to download the new
raw data archives in bulk so that network hiccups don't cause issues during ETL.

2. Map the Structure of the New Data
------------------------------------

A. EIA Forms 860/860m/861/923
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

**2.A.1)** Add a column for the new year of data to each of the aforementioned files. If
there are any changes too prior years, make sure to address those too. (See note above).
If you are updating early release data with final release data, replace the values in
the appropriate year column.

.. note::

   If you are adding EIA's early release data, make sure the raw files have
   ``Early_Release`` at the end of the file name. This is how the excel extractor knows
   to label the data as provisional vs. final.

   Early release files also tend to have one extra row at the top and one extra column
   on the right of each file indicating that it is early release. This means that the
   skiprows and column map values will probably be off by 1 when you update from early
   release to final release.

**2.A.2)** If there are files, spreadsheet pages, or individual columns with new
semantic meaning (i.e. they don't correspond to any of the previously mapped files,
pages, or columns) then create new mappings to track that information over time.

.. note::

    In all of the the above CSV files we use a value of ``-1`` to indicate that the data
    does not exist in a given year.

B. FERC Form 1
^^^^^^^^^^^^^^
**2.B.1)** Update the path to the directory containing the database files stored within
the annual FERC 1 zipfiles to reflect the new year of data. We store this information in
``src/pudl/package_data/ferc1/file_map.csv``

**2.B.2)** The process we use for :doc:`clone_ferc1` uses the most recent annual
database to define the schema for our multi-year FERC 1 DB. This only works because
historically the FERC 1 DB has only added tables and columns over time. To check whether
the new year of data continues this pattern, you can run:

.. code-block:: bash

  pytest --etl_settings src/pudl/package_data/settings/etl_full.yml \
    test/integration/etl_test.py::test_ferc1_schema

C. FERC Form 714
^^^^^^^^^^^^^^^^
**2.C.1)** FERC Form 714 is distributed as an archive of CSV files, each of which spans
all available years of data. This means there's much less structure to keep track of.
The main thing that changes from year to year is the names of the CSV files within the
ZIP archive. Update the mapping between extracted dataframes and those filenames in the
:py:const:`pudl.extract.ferc714.TABLE_FNAME` dictionary.

**2.C.2)** The character encodings of these CSV files may vary with some of them using
``iso-8859-1`` (Latin) rather than ``utf-8`` (Unicode). Note the per-file encoding
in :py:const:`pudl.extract.ferc714.TABLE_ENCODING` and that it may change over time.

3. Initial Data Extraction
--------------------------

A. EIA Forms 860/860m/861/923
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**3.A.1)** Use the Jupyter notebook ``devtools/eia-etl-debug.ipynb`` to run one step of
the process at a time, independently for each dataset. This makes debugging issues
easier. Given that there are hundreds of columns mapped across all the different EIA
spreadsheets, you'll almost certainly find some typos or errors in the extract process
and need to revise your work.

B. FERC Form 1
^^^^^^^^^^^^^^
**3.B.1)** Clone the all of the FERC 1 data (including the new year) into SQLite with:

.. code-block:: bash

    ferc_to_sqlite src/pudl/package_data/settings/etl_full.yml

This is necessary to enable mapping associations between the FERC 1 and EIA plants and
utilities later.

4. Update Table & Column Transformations
----------------------------------------

A. EIA Forms 860/860m/861/923
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**4.A.1)** Use the EIA ETL Debugging notebook mentioned above to run the initial
transform step on all tables of the new year of data and debug any failures. If any new
tables were added in the new year of data you will need to add a new transform function
for the corresponding dataframe. If new columns have been added, they should also be
inspected for cleanup.

B. FERC Form 1
^^^^^^^^^^^^^^
Some FERC 1 tables store different variables in different rows instead of or in addition
to using columns. Rows are identified by ``row_number``. What row number corresponds to
which variable changes from year to year.  We catalog this correspondence in the FERC 1
row maps, a collection of CSV files stored under
``src/pudl/package_data/ferc1/row_maps`` and organized by original FERC 1 DB table name.

**4.B.1)** Check whether the data associated with a given row number has changed
by looking at the table's entries in the ``f1_row_lit_tbl`` table. This table stores the
descriptive strings associated with each row in the FERC Form 1, and also indicates the
last year that the string was changed in the ``row_chg_yr`` column. The
``devtools/ferc1/ferc1-new-year.ipynb`` notebook can make this process less tedious.

**4.B.2)** The ``plant_kind`` and ``construction_type`` fields in the
``plants_steam_ferc1`` table and the ``fuel_type`` and ``fuel_unit`` fields in the
``fuel_ferc1`` table are reported as freeform strings and need to be converted to simple
categorical values to be useful. If the new year of data contains strings that have
never been encountered before, they need to be added to the string cleaning dictionaries
defined in :mod:`pudl.transform.ferc1`. The ``devtools/ferc1/ferc1-new-year.ipynb``
notebook and :func:`pudl.helpers.find_new_ferc1_strings` will help with this process.
Every string observed in these fileds should ultimately be mapped to one of the defined
categories.

5. Update the PUDL DB schema
----------------------------
**5.1)** If new columns or tables have been added, you will need to update the
PUDL DB schema, defining column types, giving them meaningful descriptions, applying
appropriate ENUM constraints, etc. This happens in the :mod:`pudl.metadata` subpackage.
Otherwise when the system tries to write dataframes into SQLite, it will fail.

**5.2)** Differentiate between columns which should be harvested from the transformed
dataframes in the normalization and entity resolution process (and associated with a
generator, boiler, plant, utility, or balancing authority entity), and those that should
remain in the table where they are reported.

.. note::

    You may also need to define new coding/labeling tables, or add new codes or code
    fixes to the existing coding tables.

6. Run a Siloed EIA ETL
-----------------------
.. note::

    This section should probably be updated to include reference to the new ``tox`` test
    called ``get_unmapped_ids`` that was implemented for the FERC1 XBRL integration. We
    may be able to fully skip this step because ``get_unmapped_ids`` runs the ETL with
    ``--ignore-foreign-key-constraints`` and saves the unmapped IDs.

**6.1)** Before moving on you should ensure that the EIA ETL is fully functional by
running it for all years and all EIA data sources. Create a temporary ETL settings file
that includes only the EIA data and all available years of it. You may need to debug
inconsistencies in the harvested values. See: :doc:`run_the_etl` for more details, but
you'll need to use the ``--ignore-foreign-key-constraints`` argument because new plants
and utilities probably need to be mapped (read on into next section).

7. Integration Between Datasets
-------------------------------

A. FERC 1 & EIA Plants & Utilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**7.A.1)** Once you have a PUDL DB containing **ALL OF AND ONLY THE EIA DATA**
(including the new year of data), and a cloned FERC 1 DB containing all years of
available data, you should link the plant & utility entities that are reported in the
two datasets. Refer to the :doc:`pudl_id_mapping` page for further instructions.

.. note::

    **All** FERC 1 respondent IDs and plant names and **all** EIA plant and utility IDs
    should end up in the mapping spreadsheet with PUDL plant and utility IDs, but only a
    small subset of them will end up being linked together with a shared ID. Only EIA
    plants with a capacity of more than 5 MW and EIA utilities that actually report data
    in the EIA 923 data tables are considered for linkage to their FERC Form 1
    counterparts. All FERC 1 plants and utilities should be linked to their EIA
    counterparts (there are far fewer of them).

B. Missing EIA Plant Locations from CEMS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**7.B.1)** If there are any plants that appear in the EPA CEMS dataset that do not
appear in the ``plants_entity_eia`` table or that are missing latitute and longitude
values, the missing information should be compiled and added to
``src/pudl/package_data/epacems/additional_epacems_plants.csv`` to enable accurate
adjustment of the EPA CEMS timestamps to UTC. This information can usually be obtained
with the ``plant_id_eia`` and the
`EPA's FACT API <https://www.epa.gov/airmarkets/field-audit-checklist-tool-fact-api>`__.
In some cases you may need to resort to Google Maps. If no coordinates can be found
then at least the plant's state should be included so that an approximate timezone can
be inferred.

8. Run the ETL
--------------
Once the FERC 1 and EIA utilities and plants have been associated with each other, you
can try and run the ETL with all datasets included. See: :doc:`run_the_etl`.

**8.1)** First run the ETL for just the new year of data, using the ``etl_fast.yml``
settings file.

**8.2)** Once the fast ETL works, run the full ETL using the ``etl_full.yml`` settings
to populate complete FERC 1 & PUDL DBs and EPA CEMS Parquet files.

9. Update the Output Routines and Run Full Tests
------------------------------------------------
**9.1)** With a full PUDL DB, update the denormalized table outputs and derived
analytical routines to accommodate the new data if necessary. These are generally
called from within the :class:`pudl.output.pudltabl.PudlTabl` class.

* Are there new columns that should incorporated into the output tables?
* Are there new tables that need to have an output function defined for them?

**9.2)** To ensure that you (more) fully exercise all of the possible output functions,
run the entire CI test suite against your live databases with:

.. code-block:: bash

    tox -e full -- --live-dbs

10. Run and Update Data Validations
-----------------------------------
**10.1)** When the CI tests are passing against all years of data, sanity check the data
in the database and the derived outputs by running

.. code-block:: bash

    tox -e validate

We expect at least some of the validation tests to fail initially because we haven't
updated the number of records we expect to see in each table.

**10.2)** You may also need to update the expected distribution of fuel prices if they
were particularly high or low in the new year of data. Other values like expected heat
content per unit of fuel should be relatively stable. If the required adjustments are
large, or there are other types of validations failing, they should be investigated.

**10.3)** Update the expected number of rows in the minmax_row validation tests. Pay
attention to how far off of previous expectations the new tables are. E.g. if there
are already 20 years of data, and you're integrating 1 new year of data, probably the
number of rows in the tables should be increasing by around 5% (since 1/20 = 0.05).

11. Run Additional Standalone Analyses
--------------------------------------
**11.1)** Run any important analyses that haven't been integrated into the CI
tests on the new year of data for sanity checking. For example the
:mod:`pudl.analysis.state_demand` script or generating the EIA Plant Parts List for
integration with FERC 1 data.

12. Update the Documentation
----------------------------
**12.1)** Once the new year of data is integrated, update the documentation
to reflect the new state of affairs. This will include updating at least:

* the top-level :doc:`README </index>`
* the :doc:`/release_notes`
* any updated :doc:`data sources </data_sources/index>`
