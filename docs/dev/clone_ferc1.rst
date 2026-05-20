===============================================================================
Converting raw FERC data to SQLite
===============================================================================

FERC publishes its data (e.g. :doc:`../data_sources/ferc1`) in particularly difficult
to use formats.  From 1994-2020 it used the proprietary `FoxPro database
<https://en.wikipedia.org/wiki/FoxPro>`__ binary format. Then in 2021 it switched to
`XBRL <https://en.wikipedia.org/wiki/XBRL>`__, a dialect of XML used for financial
reporting.

In addition to using two different difficult to parse file formats, the data itself is
unclean and poorly organized. As a result, very few people are currently able to use it.
This means that, while we have not yet integrated the vast majority of the available
data into PUDL, it's useful to just provide programmatic access to the bulk raw data,
independent of the cleaner subset of the data included within PUDL.

To provide that access, we've broken the :mod:`pudl.extract.ferc1` process down into
several distinct steps:

#. Clone the 1994-2020 annual database from FoxPro (DBF) into a local
   file-based :mod:`sqlite3` database.
#. Clone the 2021 and later data from XBRL into another :mod:`sqlite3` database,
   with a different structure, derived from the
   `FERC Form 1 XBRL taxonomy <https://xbrlview.ferc.gov/yeti/resources/yeti-gwt/Yeti.jsp#tax~(id~8*v~72)!net~(a~143*l~35)!lang~(code~en)!rg~(rg~4*p~1)>`__.
#. Select a limited subset of the tables in these databases for further processing and
   integration into the PUDL :mod:`sqlite3` database.

The FoxPro / XBRL derived FERC Form 1 databases include 100+ tables, containing 3000+
columns.

If you need to work with this relatively unprocessed data, we highly recommend
downloading it from one of our stable data releases or nightly build outputs, which
can be found in the PUDL :ref:`access-zenodo` or :ref:`access-cloud`.

The conversion of the raw FERC data is represented in the PUDL Dagster project by the
``raw_ferc_to_sqlite`` asset group which is defined in :data:`pudl.dagster.defs`. If you
only need the raw FERC SQLite (or experimental DuckDB) outputs, use the dedicated
``ferc_to_sqlite`` job. If you are running the full ETL from scratch, use the
``pudl_with_ferc_to_sqlite`` job, which also includes FERC SQLite assets.

The raw FERC conversion flow within ``ferc_to_sqlite`` looks like this:

.. mermaid::

   flowchart TD
       A[FERC archives in Zenodo datastore] --> B[raw_ferc_to_sqlite assets]
       B --> C[DBF extraction<br/>1994-2020]
       B --> D[XBRL extraction<br/>2021-present]

       C --> E[ferc1_dbf.sqlite]

       D --> F[ferc1_xbrl.sqlite]
       D --> G[ferc1_xbrl.duckdb<br/>experimental]
       D --> H[ferc1_xbrl_datapackage.json]
       D --> I[ferc1_xbrl_taxonomy_metadata.json]

       E --> J[Clean FERC tables in PUDL<br/>1994-present]
       F --> J
       H --> J
       I --> J

The separation between FERC and PUDL is intentional: the raw FERC conversion step
produces large source databases with hundreds of tables and thousands of columns that
can be reused across multiple downstream PUDL runs (and external applications), while
the ``pudl`` job standardizes and cleans a smaller subset of those tables in the main
PUDL data products.

The conversion can be done using the Dagster UI (see :ref:`run-dagster-ui`) or the
Dagster CLI (see :ref:`run-cli`). For local command-line usage, options include:

.. code-block:: console

  $ pixi run ferc-to-sqlite
  $ pixi run dg launch --job ferc_to_sqlite --config src/pudl/package_data/settings/dg_full.yml

.. note::

  We recommend using the Dagster UI to execute the ETL as it provides additional
  functionality for re-execution and viewing dependences.

Executing either the ``ferc_to_sqlite`` job or the full ``pudl_with_ferc_to_sqlite`` job
will create several outputs in your ``$PUDL_OUTPUT`` directory. For example the FERC
Form 1 outputs will include:

 * ``$PUDL_OUTPUT/ferc1_dbf.sqlite``: Data from 1994-2020 (FoxPro/DBF)
 * ``$PUDL_OUTPUT/ferc1_xbrl.sqlite``: Data from 2021 onward (XBRL)
 * ``$PUDL_OUTPUT/ferc1_xbrl.duckdb``: Experimental DuckDB output for 2021 and later.
 * ``$PUDL_OUTPUT/ferc1_xbrl_datapackage.json``: `Frictionless data package
   <https://specs.frictionlessdata.io/data-package/>`__ descriptor for the XBRL derived
   database.
 * ``pudl_output/ferc1_xbrl_taxonomy_metadata.json``: A JSON version of the
   XBRL Taxonomy, containing additional metadata.

By default, the ``ferc_to_sqlite`` job converts all available years and tables of data
for all available FERC Data, which includes Forms 1, 2, 6, 60 and 714. You can also
choose to materialize any combination of form an format (DBF or XBRL). Note that the
earlier FERC 714 data (2006-2020) is distributed as CSVs, and PUDL does not yet load
all available tables.

FERC SQLite provenance compatibility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PUDL records provenance metadata when FERC SQLite assets are materialized, including
the Zenodo DOI and the years extracted.
Before downstream ``pudl`` assets use those databases,
PUDL checks the metadata to make sure they are compatible with the current run.
This helps avoid scenarios where the FERC databases were built with old inputs
or don't have all the years needed by the current run. This metadata is also stored
in a table of each SQLite DB, so we can use outputs in nightly builds as a cache for
local development and CI runs.

Incompatible provenance metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The provenance check looks at three things:

* Whether the Zenodo DOI changed for a FERC dataset. This would mean that the
  raw input data used to build the existing SQLite DB is different from what is
  expected.
* Whether the current ETL config requests years of FERC data that are not present in the
  existing SQLite DB.
* The version of ``catalystcoop.ferc_xbrl_extractor`` that was used to perform the
  conversion when considering XBRL outputs.

When either of these criteria aren't met, Dagster raises an error telling you to refresh
the FERC SQLite assets.

Regenerating FERC SQLite assets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To rebuild FERC SQLite databases so they match the current input data and configuration,
you will need to rerun the ``ferc_to_sqlite`` job. By default, this job will attempt to
download the latest version of each SQLite DB from nightly builds and check to see
if these are compatible with the requirements of the current run. If they are found
to be compatible, then the conversion process will be skipped.

.. code-block:: console

  # Our task shortcut
  $ pixi run ferc-to-sqlite
  # Equivalent to
  $ pixi run dg launch --job ferc_to_sqlite --config src/pudl/package_data/settings/dg_full.yml

You can also materialize just the specific FERC SQLite assets that are out of date,
either in the Dagster UI or using the command line:

.. code-block:: console

   $ pixi run dg launch --assets raw_ferc1_xbrl__sqlite --config src/pudl/package_data/settings/dg_full.yml

After the FERC databases are refreshed, rerun the ``pudl`` job or selected downstream
assets.

Forcing ``ferc_to_sqlite`` conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to force the full conversion process to be run, then you can set the
following environment variable:

.. code-block:: console

  $ export PUDL_FORCE_FERC_TO_SQLITE=true

This is set for nightly builds, so we always run the full ``ferc_to_sqlite`` process
during builds.
