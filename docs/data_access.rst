=======================================================================================
Data Access
=======================================================================================

We publish the :doc:`PUDL pipeline <intro>` outputs in several ways to serve
different users and use cases. We're always trying to increase accessibility of the
PUDL data, so if you have a suggestion please `open a GitHub issue
<https://github.com/catalyst-cooperative/pudl/issues>`__. If you have a question you
can `create a GitHub discussion <https://github.com/orgs/catalyst-cooperative/discussions/new?category=help-me>`__.

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
   * - :ref:`access-nightly-builds`
     - Cloud Developer, Database User, Beta Tester
     - Get the freshest data that has passed all data validations, updated most weekday
       mornings. Fast downloads from AWS S3 storage buckets.
   * - :ref:`access-zenodo`
     - Researcher, Database User, Notebook Analyst
     - Use a stable, citable, fully processed version of the PUDL on your own computer.
       Use PUDL in Jupyer Notebooks running in a stable, archived Docker container.
       Access the SQLite DB and Parquet files directly using any toolset.
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

Note that only data that has been fully integrated into the SQLite databases are
available here. Currently this includes `the core PUDL database
<https://data.catalyst.coop/pudl>`__ and our concatenation of `all historical FERC
Form 1 databases <https://data.catalyst.coop/ferc1>`__.

.. _access-nightly-builds:

---------------------------------------------------------------------------------------
Nightly Builds
---------------------------------------------------------------------------------------

Every night we attempt to process all of the data that's part of PUDL using the most
recent version of the `dev branch
<https://github.com/catalyst-cooperative/pudl/tree/dev>`__. If the ETL succeeds and the
resulting outputs pass all of the data validation tests we've defined, the outputs are
automatically uploaded to the `AWS Open Data Registry
<https://registry.opendata.aws/catalyst-cooperative-pudl/>`__, and used to deploy a new
version of Datasette (see above). These nightly build outputs can be accessed using the
AWS CLI, or programmatically via the S3 API. They can also be downloaded directly over
HTTPS using the following links:

* `PUDL SQLite DB <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/pudl.sqlite>`__
* `EPA CEMS Hourly Emissions Parquet (1995-2022) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/hourly_emissions_epacems.parquet>`__
* `Census DP1 SQLite DB (2010) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/censusdp1tract.sqlite>`__

* Raw FERC Form 1:

  * `FERC-1 SQLite derived from DBF (1994-2020) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1.sqlite>`__
  * `FERC-1 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1_xbrl.sqlite>`__
  * `FERC-1 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1_xbrl_datapackage.json>`__
  * `FERC-1 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 2:

  * `FERC-2 SQLite derived from DBF (1996-2020) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2.sqlite>`__
  * `FERC-2 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2_xbrl.sqlite>`__
  * `FERC-2 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2_xbrl_datapackage.json>`__
  * `FERC-2 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 6:

  * `FERC-6 SQLite derived from DBF (2000-2020) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6.sqlite>`__
  * `FERC-6 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6_xbrl.sqlite>`__
  * `FERC-6 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6_xbrl_datapackage.json>`__
  * `FERC-6 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 60:

  * `FERC-60 SQLite derived from DBF (2006-2020) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60.sqlite>`__
  * `FERC-60 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60_xbrl.sqlite>`__
  * `FERC-60 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60_xbrl_datapackage.json>`__
  * `FERC-60 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 714:

  * `FERC-714 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc714_xbrl.sqlite>`__
  * `FERC-714 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc714_xbrl_datapackage.json>`__
  * `FERC-714 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc714_xbrl_taxonomy_metadata.json>`__


.. _access-zenodo:

---------------------------------------------------------------------------------------
Zenodo Archives
---------------------------------------------------------------------------------------

We use Zenodo to archive our fully processed data as SQLite databases and
Parquet files. We also archive a Docker image that contains the software environment
required to use PUDL within Jupyter Notebooks. You can find all our archived data
products in `the Catalyst Cooperative Community on Zenodo
<https://zenodo.org/communities/catalyst-cooperative/>`__.

* The current version of the archived data and Docker container can be
  downloaded from `This Zenodo archive <https://doi.org/10.5281/zenodo.3653158>`__
* Detailed instructions on how to access the archived PUDL data using a Docker
  container can be found in our `PUDL Examples repository
  <https://github.com/catalyst-cooperative/pudl-examples/>`__.
* The SQLite databases and Parquet files containing the PUDL data, the complete FERC 1
  database, and EPA CEMS hourly data are contained in that same archive, if you want
  to access them directly without using PUDL.

.. note::

   If you're already familiar with Docker, you can also pull
   `the image we use <https://hub.docker.com/r/catalystcoop/pudl-jupyter>`__ to run
   Jupyter directly:

   .. code-block:: console

      $ docker pull catalystcoop/pudl-jupyter:latest

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
