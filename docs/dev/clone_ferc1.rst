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

Cloning the original FERC database is the first step in the PUDL ETL process. This can
be done using the dagster UI (see :ref:`run-dagster-ui`) or with the ``ferc_to_sqlite``
script (see :ref:`run-cli`).

.. note::

  We recommend using the Dagster UI to execute the ETL as it provides additional
  functionality for re-execution and viewing dependences.

Executing a ``ferc_to_sqlite`` job will create several outputs in your ``$PUDL_OUTPUT``
directory. For example the FERC Form 1 outputs will include:

 * ``$PUDL_OUTPUT/ferc1_dbf.sqlite``: Data from 1994-2020 (FoxPro/DBF)
 * ``$PUDL_OUTPUT/ferc1_xbrl.sqlite``: Data from 2021 onward (XBRL)
 * ``$PUDL_OUTPUT/ferc1_xbrl_datapackage.json``: `Frictionless data package
   <https://specs.frictionlessdata.io/data-package/>`__ descriptor for the XBRL derived
   database.
 * ``pudl_output/ferc1_xbrl_taxonomy_metadata.json``: A JSON version of the
   XBRL Taxonomy, containing additional metadata.

By default, the script pulls in all available years and tables of data. The output is
roughly 1GB on disk. The ``ferc_to_sqlite`` jobs also extracts the XBRL data for FERC
Form 1, 2, 6, 60 and 714.
