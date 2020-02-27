===============================================================================
Cloning the FERC Form 1 DB
===============================================================================

FERC Form 1 is... special.

The :ref:`Form 1 data <data-ferc1>` is published in a particularly inaccessible
format (proprietary binary `FoxPro database <https://en.wikipedia.org/wiki/FoxPro>`__ files),
and the data itself is unclean and poorly organized. As a result, very few
people are currently able to use it at all, and we have not yet integrated the
vast majority of the available data into PUDL. This also means it's useful to
just provide programmatic access to the bulk raw data, independent of the
cleaner subset of the data included within PUDL.

To provide that access, we've broken the :mod:`pudl.extract.ferc1` process
down into two distinct steps:

#. Clone the *entire* FERC Form 1 database from FoxPro into a local
   file-based :mod:`sqlite3` database. This includes 116 distinct tables,
   with thousands of fields, covering the time period from 1994 to the
   present.
#. Pull a subset of the data out of that database for further processing and
   integration into the PUDL data packages and :mod:`sqlite3` database.

If you want direct access to the original FERC Form 1 database, you can just do
the database cloning, and connect directly to the resulting database. This has
become especially useful since Microsoft recently discontinued the database
driver that until late 2018 had allowed users to load the FoxPro database files
into Microsoft Access.

In any case, cloning the original FERC database is the first step in the PUDL
ETL process. This can be done with the ``ferc1_to_sqlite`` script (which is an
entrypoint into the :mod:`pudl.convert.ferc1_to_sqlite` module) which is
installed as part of the PUDL Python package. It takes its instructions from a
YAML file, an example of which is included in the ``settings`` directory in
your PUDL workspace. Once you've :ref:`created a datastore <datastore>` you can
try this example:

.. code-block:: console

   $ ferc1_to_sqlite settings/ferc1_to_sqlite_example.yml

This should create an SQLite database that you can find in your workspace at
``sqlite/ferc1.sqlite`` By default, the script pulls in all available years of
data, and all but 3 of the 100+ database tables. The excluded tables
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

Sadly, the FERC Form 1 database is not particularly... relational. The only
foreign key relationships that exist map ``respondent_id`` fields in the
individual data tables back to ``f1_respondent_id``. In theory, most of the
data tables use ``report_year``, ``respondent_id``, ``row_number``,
``spplmnt_num`` and ``report_prd`` as a composite primary key (According to
:download:`this FERC Form 1 database schema from 2015
<ferc/form1/ferc_form1_database_design_diagram_2015.pdf>`.

In practice, there are several thousand records (out of ~12 million), including
some in almost every table, that violate the uniqueness constraint on those
primary keys. Since there aren't many meaningful foreign key relationships
anyway, rather than dropping the records with non-unique natural composite
keys, we chose to preserve all of the records and use surrogate
auto-incrementing primary keys in the cloned SQLite database.
