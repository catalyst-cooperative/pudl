===============================================================================
Usage
===============================================================================

.. _usage-etl:

-------------------------------------------------------------------------------
Running the ETL Pipeline
-------------------------------------------------------------------------------

PUDL implements a data processing pipeline. This pipeline takes raw data
provided by public agencies in a variety of formats and integrates it together
into a single (more) coherent whole. In the data-science world this is often
called "ETL" which stands for "Extract, Transform, Load."

* **Extract** the data from its original source formats and into
  :mod:`pandas.DataFrame` objects for easy manipulation.
* **Transform** the extracted data into tidy tabular data structures, applying
  a variety of cleaning routines, and creating connections both within and
  between the various datasets.
* **Load** the data into a unified output container, in our case platform
  independent CSV/JSON based `Tabular Data Packages
  <https://frictionlessdata.io/specs/tabular-data-package/>`__.

The PUDL python package is organized into these steps as well, with
:mod:`pudl.extract` and :mod:`pudl.transform` subpackages containing dataset
specific modules like :mod:`pudl.extract.ferc1` and
:mod:`pudl.transform.eia923`. The Load step is currently just a single module
called :mod:`pudl.load`.

If you've worked through the :doc:`installation and setup <install>` process,
then you've already run the ETL pipeline on at least some of the data.
This process is coordinated by the top-level :mod:`pudl.etl_pkg` module,
which has a command line interface accessible via the ``pudl_etl`` script that
is installed by the PUDL Python package. The script is reads a YAML file as
input. An example is provided in the ``settings`` folder that is created when
you run ``pudl_setup`` (see: :ref:`install-workspace`).

To run ETL pipeline for the example, from within your PUDL workspace you do:

.. code-block:: console

    $ pudl_etl settings/pudl_etl_example.yml

This should result in a bunch of Python :mod:`logging` output, describing what
the script is doing, and some outputs in the ``sqlite`` and ``datapackages``
directories within your workspace. In particular, you should see new file at
``sqlite/ferc1.sqlite`` and a new directory at ``datapackages/pudl-example``.

Under the hood, the ``pudl_etl`` script has downloaded data from the federal
agencies and organized it into a datastore locally, cloned the original FERC
Form 1 database into that ``ferc1.sqlite`` file, extracted a bunch of data from
that database and a variety of Microsoft Excel spreadsheets and CSV files, and
combined it all into the ``pudl-example`` `tabular datapackage
<https://frictionlessdata.io/specs/tabular-data-package/>`__. The metadata
describing the overall structure of the output is found in
``datapackages/pudl-example/datapackage.json`` and the associated data is
stored in a bunch of CSV files (some of which may be :mod:`gzip` compressed) in
the ``datapackages/pudl-example/data/`` directory.

You can use the ``pudl_etl`` script to download and process more or different
data by copying and editing the ``settings/pudl_etl_example.yml`` file, and
running the script again with your new settings file as an argument. Comments
in the example settings file explain the available parameters.

.. todo::

    * Create updated example settings file, ensure it explains all available
      options.
    * Integrate datastore management and ferc1 DB cloning into ``pudl_etl``
      script.

It's sometimes useful to update the datastore or clone the :ref:`FERC Form 1
<data-ferc1>` database independent of running the full ETL pipeline. Those
processes are explained below.

.. _usage-datastore:

-------------------------------------------------------------------------------
Creating a Datastore
-------------------------------------------------------------------------------

The input data that PUDL processes comes from a variety of US government
agencies. These agencies typically make the data available via their websites
or FTP, but without really planning for programmatic access. PUDL implements
some simple data management tools in the :mod:`pudl.datastore.datastore` module
and makes them available via a script called ``pudl_datastore``. The script can
download the original data from EIA, FERC, and EPA, and organize it on your
system so that the rest of the software knows how to find it. For details on
what data is available, for what time periods, and how much of it there is, see
the :doc:`data_catalog`.

.. todo::

    Should we allow / require ``pudl_datastore`` to read its options from a
    settings file for the sake of consistency? And also to be able to put all
    these settings explicitly in the ``pudl_etl_example.yml`` input file? Or do
    we want the obtaining of data to be **only** implicit / automatic?

For example, if you wanted to download the 2018 :ref:`data-epacems` data for
Colorado:

.. code-block:: console

    $ pudl_datastore --sources epacems --states CO --years 2018

If you do not specify years, the script will retrieve all available data. So
to get everything for :ref:`data-eia860` and :ref:`data-eia923` you would run:

.. code-block:: console

    $ pudl_datastore --sources eia860 eia923

The script will download from all sources in parallel, so if you have a fast
internet connection and need a lot of data, doing it all in one go makes sense.
To pull down **all** the available data for all the sources (10+ GB) you would
run:

.. code-block:: console

    $ pudl_datastore --sources eia860 eia923 epacems ferc1 epaipm

For more detailed usage information, see:

.. code-block:: console

    $ pudl_datastore --help

The downloaded data will be used by the script to populate a data store under
the ``data`` directory, organized by data source, form, and date:

.. code-block::

    <PUDL_DIR>/data/eia/form860/
    <PUDL_DIR>/data/eia/form923/
    <PUDL_DIR>/data/epa/cems/
    <PUDL_DIR>/data/epa/ipm/
    <PUDL_DIR>/data/ferc/form1/

If the download fails (e.g. the FTP server times out), this command can be run
repeatedly until all the files are downloaded. It will not try and re-download
data which is already present locally, unless you use the ``--clobber`` option.
Depending on which data sources, how many years or states you have requested
data for, and the speed of your internet connection, this may take minutes to
hours to complete, and can consume 20+ GB of disk space even when the data is
compressed.

.. _usage-cloning-ferc1:

-------------------------------------------------------------------------------
Cloning the FERC Form 1 DB
-------------------------------------------------------------------------------

FERC Form 1 is special. It is published in a particularly inaccessible format
(binary FoxPro database files), and the data itself is particularly unclean and
poorly organized. As a result, very few people are currently able to make use
of it at all, and we have not yet integrated the vast majority of the available
data into PUDL. This also means there is significant value in simply providing
programmatic access to the bulk raw data, separately from the smaller cleaned
up subset of the data within PUDL.

In order to provide that access, we've broken the :mod:`pudl.extract.ferc1``
process into two distinct steps:

#. Clone the *entire* FERC Form 1 database from FoxPro into a local
   file-based :mod:`sqlite3` database. This includes 116 distinct tables,
   with thousands of fields, covering the time period from 1994 to the
   present.
#. Pull a subset of the data out of that `SQLite <https://www.sqlite.org/>`__
   database for further processing, and integration into the PUDL data
   packages.

If you want direct access to the original FERC Form 1 database, you can just do
the database cloning, and connect directly to the SQLite database. This is
particularly useful now, as Microsoft has discontinued the database driver that
until late 2018 had allowed users to load the FoxPro database files into
Microsoft Access.

In any case, cloning the original database is the first step in the PUDL ETL
process. This can be done with the ``ferc1_to_sqlite`` script, which is
installed as part of the PUDL python package. It takes its instructions from a
YAML file, an example of which is included in the ``settings`` directory in
your PUDL workspace:

.. code-block:: console

   $ ferc1_to_sqlite settings/ferc1_to_sqlite_example.yml

This should create a SQLite database that you can find in your workspace at
``sqlite/ferc1.sqlite`` By default, the script pulls in all available years of
data, and all but 3 of the 100+ database tables. The excluded tables
(``f1_footnote_tbl``, ``f1_footnote_data`` and ``f1_note_fin_stmnt``) contain
unreadable binary data, and increase the overall size of the database by a
factor of ~10 (to ~8 GB rather than 800 MB). If for some reason you need access
to those tables, you can create your own settings file and un-comment those
tables in the list of tables that it directs the script to load.

Note that this script pulls *all* the FERC Form 1 data into a single database.
The original data distributed by FERC are a collection of distinct annual
databases. Virtually all the database tables contain a ``report_year`` column
that indicates which year they came from, which prevents collisions in the
merged multi-year database that we create. One notable exception is the
``f1_respondent_id`` table, which maps ``respondent_id`` to the names of the
respondents. For that table, we have allowed the most recently reported record
to take precedence, overwriting previous mappings if they exist.

Sadly, the FERC Form 1 database is not particularly... relational. The only
foreign key relationships that exist map ``respondent_id`` fields in the
individual data tables back to ``f1_respondent_id``. In theory, most of the
data tables use ``report_year``, ``respondent_id``, ``row_number``,
``spplmnt_num`` and ``report_prd`` as a composite primary key (According to
:download:`this FERC Form 1 database schema from 2015
<ferc/form1/FERC_Form1_Database_Design_Diagram_2015.pdf>`.

In practice, there are several thousand records (out of ~12 million), including
some in almost every database table, that violate the uniqueness constraint on
those primary keys.  Given the lack of meaningful foreign key relationships,
rather than dropping the records with non-unique natural composite keys, we
chose to preserve all of the records and use surrogate auto-incrementing
primary keys in the cloned SQLite database.


-------------------------------------------------------------------------------
Published Data Packages
-------------------------------------------------------------------------------

After the initial release of the PUDL software, we will automate the creation
of a corresponding set of data packages containing all of the currently
integrated data. Users who are not working with Python, and would prefer not to
set up and run the data processing pipeline described above will be able to
download and use the data packages directly,

Zenodo
^^^^^^

These data packages will be archived alongside the `software release
<https://guides.github.com/activities/citable-code/>`__ that was used to
generated, on `Zenodo <https://zenodo.org/>`__. Both the software release and
the data packages will be issued DOIs (digital object identifiers) so that they
can be uniquely referenced in research and other publications. Our goal is to
make replication of any analyses that depend on the released code and published
data as easy to replicate as possible.

Datahub
^^^^^^^
We also intend to regularly publish new data packages via `Datahub.io
<https://datahub.io/catalystcooperative>`__, a open data
portal which natively understands data packages, parses the included metadata,
and can help integrate the PUDL data with other open public data.


-------------------------------------------------------------------------------
Using the Data Packages
-------------------------------------------------------------------------------

Once you've generated or downloaded the tabular data packages you can use them
to do analysis on almost any platform. Below are a few examples of how to
access them. Let us know if you have examples of how to pull them into other
tools!

Python, Pandas, and Jupyter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can read the datapackages into :mod:`pandas.DataFrame` for interactive
in-memory use within
`JupyterLab <https://jupyterlab.readthedocs.io/en/stable/>`__,
or for programmatic use in your own Python modules. Several example Jupyter
notebooks are deployed into your PUDL workspace ``notebooks`` directory by the
``pudl_setup`` script.

With the ``pudl`` conda environment activated you can start up a notebook
server and experiment with those notebooks by running:

.. code-block:: console

    $ jupyter-lab --notebook-dir=notebooks

Then select the ``pudl_intro.ipynb`` notebook from the file browser on the left
hand side of the JupyterLab interface.

.. todo::

    Update ``pudl_intro.ipynb`` to read the example datapackage.

If you are using Python and need to work with larger-than-memory data,
especially the :ref:`data-epacems` dataset, we recommend checking out `the Dask
project <https://dask.org>`__, which extends Pandas for serialized, parallel
and distributed processing tasks. It can also speed up processing for in-memory
tasks, especially if you have a powerful system with multiple cores, a solid
state disk, and plenty of memory.

The R programming language
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. todo::

    Get someone who uses R to give us an example here... maybe we can get
    someone from OKFN to do it?

SQLite
^^^^^^

If you'd rather access the data via SQL, you can easily load the datapackages
into a local :mod:`sqlite3` database.

.. todo::

    Write and document datapackage bundle to SQLite script.

Microsoft Access / Excel
^^^^^^^^^^^^^^^^^^^^^^^^^

If you'd rather do spreadsheet based analysis, here's how you can pull the
datapackages into Microsoft Access and Excel.

.. todo::

    Document process for pulling data packages or datapackage bundles into
    Microsoft Access / Excel


-------------------------------------------------------------------------------
PUDL in the Cloud
-------------------------------------------------------------------------------

As the volume of data integrated into PUDL continues to increase, asking users
to either run the processing pipeline themselves, or to download many GB of
data packages to do their own analyses will be become more challenging.

Instead we are working on automatically deploying each data release  in cloud
computing environments that allow many users to remotely access the same data,
as well as computational resources required to work with that data. We hope
that this will minimize the technical and administrative overhead associated
with using PUDL.

Pangeo
^^^^^^^

Our focus right now is on the `Pangeo <https://pangeo.io>`__ platform, which
solves a similar problem for within the Earth science research community.
Pangeo uses a `JupyterHub <https://jupyterhub.readthedocs.io/en/stable/>`__
deployment, and includes commonly used scientific software packages and a
shared domain specific data repository, which users may access via JupyterLab.

BigQuery
^^^^^^^^^

We are also looking at making the published data packages available for live
querying by inserting them into Google's
`BigQuery data warehouse <https://cloud.google.com/bigquery/>`__.
