===============================================================================
Existing Data Updates
===============================================================================

Many of the raw data inputs for PUDL are published on an annual or monthly basis. These
instructions explain the process for integrating new versions of existing data into
PUDL.

We update EIA monthly data and EPA CEMS hourly data on a quarterly basis.

EIA typically publishes an "early release" version of their annual data in the summer
followed by a final release in the fall. Our ``data_maturity`` column indicates
which version has been integrated into PUDL ("final" vs. "provisional"). This column
also shows when data are derived from monthly updates ("monthly_update") or contain
incomplete year-to-date data ("incremental_ytd"). Annual EIA data is updated
first when early release data is published (around June-July), and then again when
final data is released (around September-October).

FERC publishes form submissions on a rolling basis meaning there is no official
date that the data are considered final or complete. To figure out when the data are
likely complete, we compare the number of respondents from prior years to the number of
current respondents. We usually update FERC once a year around when we integrate EIA's
final release in the fall.

Finally, we currently update NREL ATB, the EIA-EPA crosswalk, and PHMSA once a year.

To see what data we have available for each dataset, click on the links below and look
at the "Years Liberated" field.

* :doc:`/data_sources/eia860` (and eia860m)
* :doc:`/data_sources/eia861`
* :doc:`/data_sources/eia923`
* :doc:`/data_sources/eia930`
* :doc:`/data_sources/epacems`
* :doc:`/data_sources/ferc1`
* :doc:`/data_sources/ferc714`

1. Obtain Fresh Data
--------------------
**1.1)** Add a new copy of the raw PUDL inputs from agency websites using the tools
in the
`pudl-archiver repository <https://github.com/catalyst-cooperative/pudl-archiver>`__.
If the structure of the web pages or the URLs has changed, you may need to update the
archivers themselves.

**1.2)** Update the dictionary of production DOIs in :mod:`pudl.workspace.datastore` to
refer to the new raw input archives.

**1.3)** In :py:const:`pudl.metadata.sources.SOURCES`, update the ``working_partitions``
to reflect the years, months, or quarters of data that are available for each dataset
and the ``records_liberated`` to show how many records are available. Check to make
sure other fields such as ``source_format`` or ``path`` are still accurate.

.. note::

  If you're updating EIA861, you can skip the rest of the steps in this section and
  all steps after step two because 861 is not yet included in the ETL.

**1.4)** Update the partitions of data to be processed in
the ``etl_full.yml`` and ``etl_fast.yml`` settings files stored under
``src/pudl/package_data/settings`` in the PUDL repo.

**1.5)** Use the ``pudl_datastore`` script (see :doc:`datastore`) to download the new
raw data archives in bulk so that network hiccups don't cause issues during the ETL.

2. Map the Structure of the New Data
------------------------------------

A. EIA Forms
^^^^^^^^^^^^
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

**2.A.1)** If you're adding a new year, add a column for the new year of data to each of
the aforementioned files. If there are any changes to prior years, make sure to address
those too. (See note above). If you are updating early release data with final release
data, replace the values in the appropriate year column.

.. note::

   **If you are adding EIA's early release data**, make sure the raw files have
   ``Early_Release`` at the end of the file name. This is how the excel extractor knows
   to label the data as provisional vs. final.

   **If you are updating early release data to final release data** - early release
   files tend to have one extra row at the top and one extra column on the right of each
   file indicating that it is early release. This means that the skiprows and column map
   values will probably be off by 1.

**2.A.2)** If there are files, spreadsheet pages, or individual columns with new
semantic meaning (i.e. they don't correspond to any of the previously mapped files,
pages, or columns) then create new mappings to track that information over time.

.. note::

    In all of the the above CSV files we use a value of ``-1`` to indicate that the data
    does not exist in a given year.

B. FERC Form 714
^^^^^^^^^^^^^^^^
FERC Form 714 is distributed as an archive of CSV files, each of which spans
all available years of data. This means there's much less structure to keep track of.
The main thing that changes from year to year is the names of the CSV files within the
ZIP archive.

**2.B.1)** Update the mapping between extracted dataframes and those filenames in the
:py:const:`pudl.extract.ferc714.TABLE_FNAME` dictionary.

**2.B.2)** The character encodings of these CSV files may vary with some of them using
``iso-8859-1`` (Latin) rather than ``utf-8`` (Unicode). Note the per-file encoding
in :py:const:`pudl.extract.ferc714.TABLE_ENCODING` and that it may change over time.

C. NREL ATB
^^^^^^^^^^^
Inspect the raw data. Following the instructions for EIA data described above, map
the raw column headers to shared column names in the ``data.csv`` spreadsheet located
in ``src/pudl/package_data/nrelatb``.

3. Test Data Extraction
-----------------------

A. EIA Forms
^^^^^^^^^^^^
**3.A.1)** You can either materialize the raw assets (ex: ``raw_eia860``) in Dagster
(learn more about Dagster in :doc:`run_the_etl`) or use the Jupyter notebook
``devtools/eia-etl-debug.ipynb`` to run the extract process for a given data set. There
are hundreds of columns mapped across all the different EIA spreadsheets, you'll almost
certainly encounter typos or errors that will cause the extraction to fail. Interpret
these errors and revise your work from step 2. Using Dagster will help speed up the
debugging process because it allows you to load individual, problematic assets rather
than the whole suite of tables from a source.

.. note::

    If you've created or removed any assets, you'll need to refresh the code location in
    Dagster before materializing any assets. You can do this by clicking on the circular
    arrow in the upper left hand corner next to the text "Job in <NAME OF JOB>".

B. FERC Form 1
^^^^^^^^^^^^^^
**3.B.1)** Clone all of the FERC 1 data (including the new year) into SQLite with:

.. code-block:: bash

    ferc_to_sqlite src/pudl/package_data/settings/etl_full.yml

This is necessary to enable mapping associations between the FERC 1 and EIA plants and
utilities later.

**3.B.2)** Like EIA, you can either materialize the raw assets in Dagster or
use the ``devtools/ferc1-etl-debug.ipynb`` notebook to run the extract process for
each table.

C. EPA CEMS
^^^^^^^^^^^
**3.C.1)** The CEMS data are so large that it doesn't make sense to store a raw and
cleaned version of the data in the database. We'll test the extraction and
transformation steps together in the next section.

D. NREL ATB
^^^^^^^^^^^^
**3.D.1)** Materialize the raw assets (``raw_nrelatb``) in Dagster. If any errors occur,
revisit the column mapping spreadsheets and check for any errors.

4. Update Table & Column Transformations
----------------------------------------
Currently, our FERC and EIA tables utilize different transform processes.

A. EIA Forms
^^^^^^^^^^^^
**4.A.1)** You can either materialize the ``_core`` (clean) and ``core`` (normalized)
dagster asset groups for your dataset of interest (ex: ``_core_eia860`` and
``core_eia860``) or use the EIA ETL Debugging notebook mentioned above to run the
initial transform step on all tables of the new year of data. As mentioned in 3.A.1,
the debugging process is significantly faster with Dagster. If any new tables were added
in the new year, you will need to add a new transform function for the corresponding
dataframe. If new columns have been added, they should also be inspected for cleanup.
Debug and rematerialize the assets until they load successfully.

.. note::

    As with the extract phase, if new Dagster assets are added to the pipeline, you'll
    need to refresh the code location in Dagster by clicking on the circular
    arrow in the upper left hand corner next to the text "Job in <NAME OF JOB>" before
    materializing the new assets.

B. FERC Form 1
^^^^^^^^^^^^^^

**4.B.1)** If you're mapping FERC tables that have not been included in the ETL yet,
look at the ``src/pudl/package_data/ferc1/dbf_to_xbrl_tables.csv`` for our preliminary
estimation of which DBF tables connect to which XBRL tables. Note that this spreadsheet
is not referenced anywhere in the code and should only be used as a reference. Once
you've verified that these tables are indeed a match, input them into the
:py:const:`pudl/extract.ferc1.TABLE_NAME_MAP_FERC1` dictionary for extraction.

**4.B.2)** For these new tables (or to address changes in xbrl taxonomy), add or update
the relationship between DBF rows and XBRL rows in
``src/pudl/package_data/ferc1/dbf_to_xbrl.csv``. See the note below for instructions.

.. note::

    **How to use the mapping spreadsheets:**

    In the Pre-2021 data (from the DBF files), rows are identified by ``row_number``,
    and the row number that corresponds to a given variable changes from year to year.
    We cataloged this correspondence, and the connection to the post-2021 data (from
    XBRL), in ``src/pudl/package_data/ferc1/dbf_to_xbrl.csv``.

    The ``dbf_to_xbrl.csv`` maps row numbers from the DBF data with taxonomy factoids
    from the XBRL data therefore allowing us to merge the data into one continuous
    timeseries. The ``row_literal`` column is the DBF label for the ``row_number`` in
    question. This ``row_literal`` must be mapped to an ``xbrl_factoid`` from the XBRL
    data. These ``xbrl_factoid`` entries are the value columns from the raw XBRL data.

    Look at the ``row_literal`` values for a given table and see which XBRL columns they
    correspond to. It's helpful to
    `view the XBRL taxonomy <https://xbrlview.ferc.gov/>`__ for the table in question.

    The ``row_literals`` may contain elements of the FERC 1 form such as
    headers that don't map to an XBRL factoid. These can be marked as ``headers`` in the
    ``row_type`` column. Other values are either marked as ``report_value``: a directly
    reported value in the DBF data, meaning it is not calculated from other values in
    that table (it may in fact correspond to some calculation derived from values
    reported in other tables); or a ``calculated_value``: a value which is derived from
    other values in that table -- typically a sum (Total rows) or a net value
    (credit - debit) of some kind. Often there's an annotation in the row_literal field
    that indicates (to humans) what other rows are used to calculate the value. These
    values will typically also appear in XBRL, with a formula for their calculation
    reported in the XBRL metadata.

    The ``dbf_only`` column is marked ``TRUE`` if the ``row_literal`` only shows up in
    the DBF files. A common example is when several fields are aggregated in the DBF
    data but not in XBRL. The ``notes`` column is a place to indicate complexity or
    reasoning and is intended for humans (vs. computers) to read.


**4.B.3)** Either materialize the clean and/or normalized FERC 1 dagster asset groups or
use the FERC 1 debugging notebook ``devtools/ferc1-etl-debug.ipynb`` to run the
transforms for each table. Heed any errors or warnings that pop up in the logs. One of
the most likely bugs will be uncategorized strings (think new, strange fuel type
spellings).

**4.B.4)** If there's a new column, add it to the transform process. At the very least,
you'll need to include it in the ``rename_columns`` dictionary in
:py:const:`pudl.transform.params.ferc1.TRANSFORM_PARAMS` for the appropriate table.

* Consider whether the column could benefit from any of the standard transforms in
  :mod:`pudl.transform.classes` or :mod:`pudl.transform.ferc1`. If so, add them to
  :py:const:`pudl.transform.params.ferc1.TRANSFORM_PARAMS`. Make sure that the
  parameter you've added to ``TRANSFORM_PARAMS`` corresponds to a method that gets
  called in one of the high-level transform functions in
  :class:`pudl.transform.ferc1.Ferc1AbstractTableTransformer` (``process_xbrl``,
  ``process_dbf``, ``transform_start``, ``transform_main``) and/or any
  table-specific overrides in the relevant table transformer class.

* Consider whether the column could benefit from custom transformations. If it's
  something that could be applicable to other tables from other sources, consider
  building it in :mod:`pudl.transform.classes`. If it's specific to FERC1, build it in
  :mod:`pudl.transform.ferc1`. If it will only ever be relevant to one table in FERC1,
  build it in the table-specific class in :mod:`pudl.transform.ferc1`, create an
  override for one of the high-level transform functions, and call it there. Make sure
  to write a unit test for any new functions.

**4.B.5)** If there's a new table, add it to the transform process. You'll need to build
or augment a table transformer in :mod:`pudl.transform.ferc1` and follow all
instructions applicable to new columns.

**4.B.6)** To see if the transformations work, you can run the transform module as a
script in the terminal. From within the pudl repo directory, run:

.. code-block:: bash

    python src/pudl/transform/ferc1.py

C. EPA CEMS
^^^^^^^^^^^

**4.C.1)** Use dagster to materialize the ``core_epacems`` asset group and debug. The
most common errors will occur when new CEMS plants lack timezone data in the EIA
database. See section 6.B.1 for instructions on how to fix this. Once you've updated the
spreadsheet tracking these errors, reload the ``core_epacems`` assets in Dagster.

D. NREL ATB
^^^^^^^^^^^^
**4.D.1)** Materialize the ``_core_nrelatb__ transform_start`` asset in Dagster. If
there are new primary keys or ``core_metric_parameters``, this should raise errors. New
core parameters should be renamed in ``core_metric_parameters_rename``, and new primary
keys should be renamed in ``rename_dict``. Debug any remaining errors.

**4.D.2)** If there are any new primary key columns (e.g.,
``model_tax_credit_case_nrelatb``), add them to the ``idx`` of the table whose
``core_metric_parameters`` they describe as a primary key. You may have to create a new
table, as needed.

**4.D.3)** If there are new ``core_metric_parameters`` (e.g., ``inflation_rate``),
identify which table they should live in.

* Are they reported by model case, reference year, projection year and technology
  description? If so, add them to the ``rate_table`` dictionary in
  :class:`pudl.transform.nrelatb.Unstacker`.
* Are they further broken out by scenario, tax credit case, and cost recovery period?
  Add them to the ``scenario_table``.
* Are they even further broken out by ``technology_description_detail_1`` or
  ``technology_description_detail_2``?

How do you ascertain this? The use of asterisks (\*) denotes wildcard values.
Generally when an asterisk is in one of the ``IDX_ALL`` columns, the corresponding
``core_metric_parameter`` should be associated with a table without that column as one
of its ``idx``.

**4.D.4)** To test the prior two steps, add these fields to the schema as described in
Step 5 below. Then, materialize the ``core_nrelatb`` assets. Any errors pointing to
duplicated indices or primary keys will likely point to an error in one of the steps
above. Continue to iterate and debug until assets generate successfully.

**4.D.5)** Finally, if any fields were added that are descriptive categoricals (e.g.,
``technology_description_1``, ``units``), add them to
:class:`pudl.transform.nrelatb.Normalizer` to create small subset tables. As needed,
create new tables in :mod:`pudl.metadata.resources.nrelatb` for these descriptors,
following the example of ``core_nrelatb__yearly_technology_status``.

5. Update the PUDL DB Schema
----------------------------
If new columns or tables have been added, you must also update the PUDL DB schema,
define column types, give them meaningful descriptions, apply appropriate ENUM
constraints, etc. This happens in the :mod:`pudl.metadata` subpackage. Otherwise when
the system tries to write dataframes into SQLite, it will fail or simply exclude any new
columns.

**5.1)** Check whether new columns exist in
:py:const:`pudl.metadata.fields.FIELD_METADATA`. If they do, make sure the descriptions
and data types match. If the descriptions don't match, you may need to define that
column by source: :py:const:`pudl.metadata.fields.FIELD_METADATA_BY_GROUP` or by table:
:py:const:`pudl.metadata.fields.FIELD_METADATA_BY_RESOURCE`. If the column is not in
:py:const:`pudl.metadata.fields.FIELD_METADATA`, add it.

**5.2)** Add new columns and tables to the ``RESOURCE_METADATA`` dictionaries in the
appropriate :mod:`pudl.metadata.resources` modules.

**5.3)** Update any :mod:`pudl.metadata.codes`, :mod:`pudl.metadata.labels`, or
:mod:`pudl.metadata.enums` pertaining to new or existing columns with novel content.

**5.4)** Differentiate between columns which should be harvested from the transformed
dataframes in the normalization and entity resolution process (and associated with a
generator, boiler, plant, utility, or balancing authority entity), and those that should
remain in the table where they are reported.

**5.5)** Once you've updated the metadata, you'll need to update the alembic version.
See the instructions for doing so in :doc:`run_the_etl`. You may have already updated
alembic if you used Dagster to materialize the raw and clean assets.

6. Connect Datasets
-------------------

A. FERC 1 & EIA Plants & Utilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**6.A.1)** Run the following command in the terminal, and refer to the
:doc:`pudl_id_mapping` page for further instructions.


.. code-block:: console

    $ make unmapped-ids

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
**6.B.1)** If there are any plants that appear in the EPA CEMS dataset that do not
appear in the ``core_eia__entity_plants`` table, or that are missing latitude and
longitude values, you'll get a warning when you try and materialize assets downstream
from ``core_epacems`` (``_core_epacems__emissions_unit_ids`` and
``core_epa__assn_eia_epacamd_subplant_ids``). You'll need to manually compile the
missing information and add it to
``src/pudl/package_data/epacems/additional_epacems_plants.csv`` to enable accurate
adjustment of the EPA CEMS timestamps to UTC. Using the Plant ID from the warning, look
up the plant coordinates in the
`EPA FACT API <https://www.epa.gov/airmarkets/field-audit-checklist-tool-fact-api>`__.
In some cases you may need to resort to Google Maps. If no coordinates can be found
then at least the plant's state should be included so that an approximate timezone can
be inferred.

7. Update the Output Routines
-----------------------------
**7.1)** Update the denormalized table outputs and derived analytical routines to
accommodate the new data if necessary.

* Are there new columns that should be incorporated into the output tables?
* Are there new tables that need to have an output function defined for them?

8. Run the ETL
--------------
Once the FERC 1 and EIA utilities and plants have been associated with each other, you
can try and run the ETL with all datasets included. See: :doc:`run_the_etl`.

**8.1)** First run the ETL for just the new year of data, using the ``etl_fast.yml``
settings file.

**8.2)** Once the fast ETL works, run the full ETL using the ``etl_full.yml`` settings
to populate complete FERC 1 & PUDL DBs and EPA CEMS Parquet files.


9. Run and Update Data Validations
----------------------------------

**9.1)** To ensure that you fully exercise all of the possible output functions,
run all the integration tests against your live PUDL DB with:

.. code-block:: console

    $ make pytest-integration-full

**9.2)** When the CI tests are passing against all years of data, sanity check the data
in the database and the derived outputs by running

.. code-block:: console

    $ make pytest-validate

We expect at least some of the validation tests to fail initially because we haven't
updated the number of records we expect to see in each table.

**9.3)** You may also need to update the expected distribution of fuel prices if they
were particularly high or low in the new year of data. Other values like expected heat
content per unit of fuel should be relatively stable. If the required adjustments are
large, or there are other types of validations failing, they should be investigated.

**9.4)** Update the expected number of rows in the ``dbt`` row count tests. Pay
attention to how far off of previous expectations the new tables are. E.g. if there
are already 20 years of data, and you're integrating 1 new year of data, probably the
number of rows in the tables should be increasing by around 5% (since 1/20 = 0.05).

10. Update the Documentation
----------------------------
**10.1)** Once the new year of data is integrated, update the documentation to reflect
the new state of affairs. This will include updating at least:

* the top-level :doc:`README </index>`
* the :doc:`data access </data_access>` page
* the :doc:`/release_notes`
* any updated :doc:`data sources </data_sources/index>`

Check that the docs still build with

.. code-block:: console

    $ make docs-build
