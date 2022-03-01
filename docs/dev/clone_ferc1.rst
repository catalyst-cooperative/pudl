===============================================================================
Cloning the FERC Form 1 DB
===============================================================================

FERC Form 1 is... special.

The :doc:`../data_sources/ferc1` is published in a particularly inaccessible
format (proprietary binary `FoxPro database <https://en.wikipedia.org/wiki/FoxPro>`__ files),
and the data itself is unclean and poorly organized. As a result, very few
people are currently able to use it. This means that, while we have not yet integrated
the vast majority of the available data into PUDL, it's useful to
just provide programmatic access to the bulk raw data, independent of the
cleaner subset of the data included within PUDL.

To provide that access, we've broken the :mod:`pudl.extract.ferc1` process
down into two distinct steps:

#. Clone the *entire* FERC Form 1 database from FoxPro into a local
   file-based :mod:`sqlite3` database. This includes 116 distinct tables,
   with thousands of fields, covering the time period from 1994 to the
   present.
#. Pull a subset of the data out of that database for further processing and
   integration into the PUDL :mod:`sqlite3` database.

If you want direct access to the original FERC Form 1 database, you can just do
the database cloning and connect directly to the resulting database. This has
become especially useful since Microsoft recently discontinued the database
driver that until late 2018 had allowed users to load the FoxPro database files
into Microsoft Access.

In any case, cloning the original FERC database is the first step in the PUDL
ETL process. This can be done with the ``ferc1_to_sqlite`` script (which is an
entrypoint into the :mod:`pudl.convert.ferc1_to_sqlite` module) which is
installed as part of the PUDL Python package. It takes its instructions from a
YAML file, an example of which is included in the ``settings`` directory in
your PUDL workspace. Once you've :ref:`created a datastore <datastore>`, you can
try this example:

.. code-block:: console

   $ ferc1_to_sqlite settings/etl-full.yml

This should create an SQLite database that you can find in your workspace at
``sqlite/ferc1.sqlite`` By default, the script pulls in all available years of
data and all but 3 of the 100+ database tables. The excluded tables
(``f1_footnote_tbl``, ``f1_footnote_data`` and ``f1_note_fin_stmnt``) contain
unreadable binary data, and increase the overall size of the database by a
factor of ~10 (to ~8 GB rather than 800 MB). If for some reason you need access
to those tables, you can create your own settings file and un-comment those
tables in the list of tables that it directs the script to load.

.. note::

    This script pulls *all* of the FERC Form 1 data into a *single* database,
    but FERC distributes a *separate* database for each year. Virtually all
    the database tables contain a ``report_year`` column that indicates which
    year they came from, preventing collisions between records in the merged
    multi-year database. One notable exception is the ``f1_respondent_id``
    table, which maps ``respondent_id`` to the names of the respondents. For
    that table, we have allowed the most recently reported record to take
    precedence, overwriting previous mappings if they exist.

.. note::

   There are a handful of ``respondent_id`` values that appear in the FERC
   Form 1 database tables but do not show up in ``f1_respondent_id``.
   This renders the foreign key relationships between those tables invalid.
   During the database cloning process we add these ``respondent_id`` values to
   the ``f1_respondent_id`` table with a ``respondent_name`` indicating that
   the ID was filled in by PUDL.
