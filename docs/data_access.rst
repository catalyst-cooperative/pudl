=======================================================================================
Data Access
=======================================================================================

We publish the PUDL pipeline outputs in several ways to serve
different users and use cases. We're always trying to increase accessibility of the
PUDL data, so if you have a suggestion please `open a GitHub issue
<https://github.com/catalyst-cooperative/pudl/issues>`__. If you have a question you
can `create a GitHub discussion <https://github.com/orgs/catalyst-cooperative/discussions/new?category=help-me>`__.

PUDL's primary data output is the ``pudl.sqlite`` database. We recommend working with
tables with the ``out_`` prefix, as these tables contain the most complete and easiest
to work with data. For more information about the different types
of tables, read through :ref:`PUDL's naming conventions <asset-naming>`.

.. _access-modes:

---------------------------------------------------------------------------------------
How Should You Access PUDL Data?
---------------------------------------------------------------------------------------

We provide four primary ways of interacting with PUDL data. Here's how to find out
which one is right for you and your use case.

.. list-table::
   :widths: auto
   :header-rows: 1

   * - Access Method
     - Types of User
     - Use Cases
   * - :ref:`access-datasette`
     - Curious Explorer, Spreadsheet Analyst, Web Developer
     - Explore the PUDL database interactively in a web browser.
       Select data to download as CSVs for local analysis in spreadsheets.
       Create sharable links to a particular selection of data.
       Access PUDL data via a REST API.
   * - :ref:`access-kaggle`
     - Data scientist, data analyst, Jupyter notebook user
     - Easy Jupyter notebook access to all PUDL data products, including example
       notebooks. Updated weekly based on the nightly builds.
   * - :ref:`access-nightly-builds`
     - Cloud Developer, Database User, Beta Tester
     - Get the freshest data that has passed all of our data validations, updated most
       weekday mornings. Fast, free downloads from AWS S3 storage buckets.
   * - :ref:`access-stable`
     - Researcher, Database User, Notebook Analyst
     - Use a stable, citable, fully processed version of the PUDL on your own computer.
       Access the SQLite DB and Parquet files directly using any toolset.
   * - :ref:`access-raw`
     - Researcher, Data Wrangler
     - Access the data that feeds into PUDL, unmodified from its original source.
   * - :ref:`access-development`
     - Python Developer, Data Wrangler
     - Run the PUDL data processing pipeline on your own computer.
       Edit the PUDL source code and run the software tests and data validations.
       Integrate a new data source or newly released data from one of existing sources.

.. _access-datasette:

---------------------------------------------------------------------------------------
Datasette
---------------------------------------------------------------------------------------

We provide web-based access to the PUDL data via a
`Datasette <https://datasette.io>`__ deployment at `<https://data.catalyst.coop>`__.

Datasette is an open source tool that wraps SQLite databases in an interactive
front-end. It allows users to browse database tables, select portions of them using
dropdown menus, build their own SQL queries, and download data to CSVs. It also
creates a REST API allowing the data in the database to be queried programmatically.
All the query parameters are stored in the URL so you can also share links to the
data you've selected.

.. note::

   The only SQLite database containing cleaned and integrated data is `the core PUDL database
   <https://data.catalyst.coop/pudl>`__. There are also several FERC SQLite databases
   derived from their old Visual FoxPro and new XBRL data formats, which we publish as
   SQLite to improve accessibility of the raw inputs, but they should generally not be
   used directly if the data you need has integrated into the PUDL database.

.. _access-kaggle:

---------------------------------------------------------------------------------------
Kaggle
---------------------------------------------------------------------------------------

Want to explore the PUDL data interactively in a Jupyter Notebook without needing to do
any setup? Our nightly build outputs (see below) automatically update `the PUDL Project
Dataset on Kaggle <https://www.kaggle.com/datasets/catalystcooperative/pudl-project>`__
once a week. There are `several notebooks <https://www.kaggle.com/datasets/catalystcooperative/pudl-project/code>`__
associated with the dataset, both curated by Catalyst and contributed by other Kaggle
users which you can use to get oriented to the PUDL database.

.. _access-nightly-builds:

---------------------------------------------------------------------------------------
Nightly Builds
---------------------------------------------------------------------------------------

Every night we attempt to process all of the data that's part of PUDL using the most
recent version of the `main branch
<https://github.com/catalyst-cooperative/pudl/tree/main>`__. If the ETL succeeds and the
resulting outputs pass all of the data validation tests we've defined, the outputs are
automatically uploaded to the `AWS Open Data Registry
<https://registry.opendata.aws/catalyst-cooperative-pudl/>`__, and used to deploy a new
version of Datasette (see above). These nightly build outputs can be accessed using the
AWS CLI, or programmatically via the S3 API. They can also be downloaded directly over
HTTPS using the following links:

* `PUDL SQLite DB <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.sqlite.gz>`__
* `EPA CEMS Hourly Emissions Parquet (1995Q1-2023Q4) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/core_epacems__hourly_emissions.parquet>`__
* `Census DP1 SQLite DB (2010) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/censusdp1tract.sqlite.gz>`__

* Raw FERC Form 1:

  * `FERC-1 SQLite derived from DBF (1994-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_dbf.sqlite.gz>`__
  * `FERC-1 SQLite derived from XBRL (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl.sqlite.gz>`__
  * `FERC-1 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl_datapackage.json>`__
  * `FERC-1 XBRL Taxonomy Metadata as JSON (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 2:

  * `FERC-2 SQLite derived from DBF (1996-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_dbf.sqlite.gz>`__
  * `FERC-2 SQLite derived from XBRL (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl.sqlite.gz>`__
  * `FERC-2 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl_datapackage.json>`__
  * `FERC-2 XBRL Taxonomy Metadata as JSON (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 6:

  * `FERC-6 SQLite derived from DBF (2000-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_dbf.sqlite.gz>`__
  * `FERC-6 SQLite derived from XBRL (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl.sqlite.gz>`__
  * `FERC-6 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl_datapackage.json>`__
  * `FERC-6 XBRL Taxonomy Metadata as JSON (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 60:

  * `FERC-60 SQLite derived from DBF (2006-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_dbf.sqlite.gz>`__
  * `FERC-60 SQLite derived from XBRL (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl.sqlite.gz>`__
  * `FERC-60 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl_datapackage.json>`__
  * `FERC-60 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 714:

  * `FERC-714 SQLite derived from XBRL (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl.sqlite.gz>`__
  * `FERC-714 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_datapackage.json>`__
  * `FERC-714 XBRL Taxonomy Metadata as JSON (2021-2022) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_taxonomy_metadata.json>`__


To reduce network transfer times, we compress the SQLite databases using ``gzip``. To
decompress them locally, at the command line on Linux, MacOS, or Windows you can use the
``gunzip`` command. (Git for Windows installs ``gzip`` / ``gunzip`` by default, and it
can also be installed using the conda package manager).

.. code-block:: console

   $ gunzip *.sqlite.gz

If you're not familiar with using Unix command line tools in Windows you can also use a
3rd party tool like `7zip <https://www.7-zip.org/download.html>`__.

.. _access-stable:

---------------------------------------------------------------------------------------
Stable Builds
---------------------------------------------------------------------------------------

If you want a specific, immutable version of our data for any reason, you can
find them all `here on Zenodo
<https://zenodo.org/doi/10.5281/zenodo.3653158>`__. Zenodo assigns long-lived
DOIs to each archive, suitable for citation in academic journals and other
publications. The most recent versioned PUDL data release can be found using
this Concept DOI: https://doi.org/10.5281/zenodo.3653158

The documentation for the latest such stable build is `here
<https://catalystcoop-pudl.readthedocs.io/en/stable/>`__. You can access the
documentation for a specific version by hovering over the version selector at
the bottom left of the page.

If you're not after a *specific* version, but rather the *latest stable
version*, you can find them `AWS Open Data Registry
<https://registry.opendata.aws/catalyst-cooperative-pudl/>`__, in the
``stable/`` namespace. You can run ``aws s3 ls --no-sign-request
s3://pudl.catalyst.coop/stable/`` to see what's available.

.. _access-raw:

---------------------------------------------------------------------------------------
Raw Data
---------------------------------------------------------------------------------------

Sometimes you want to see the raw data that is published by the government, but
it's hard to find or difficult to download, or you want to see what an older version of
the published data looked like prior to being revised or deleted.

We use Zenodo to archive and version our raw data inputs. You can find all of
our archives in `the Catalyst Cooperative Community
<https://zenodo.org/communities/catalyst-cooperative/>`__.

These have been minimally processed - in some cases we've compressed them or
grouped them into ZIP archives to fit the Zenodo repository requirements. In
all cases we've added some metadata to help identify the resources you're
looking for. But, apart from that, these datasets are unmodified.

.. _access-development:

---------------------------------------------------------------------------------------
Development Environment
---------------------------------------------------------------------------------------

If you want to run the PUDL data processing pipeline yourself from scratch, run the
software tests, or make changes to the source code, you'll need to set up our
development environment. This is a bit involved, so it has its
:doc:`own separate documentation <dev/dev_setup>`.

Most users shouldn't need to do this, and will probably find working with the
pre-processed data via one of the other access modes easier. But if you want to
:doc:`contribute to the project <CONTRIBUTING>` please give it a shot!
