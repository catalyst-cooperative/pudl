=======================================================================================
Data Access
=======================================================================================

We publish the :doc:`PUDL pipeline <intro>` outputs in several ways to serve
different users and use cases. We're always trying to increase accessibility of the
PUDL data, so if you have suggestions or questions please `open a GitHub issue
<https://github.com/catalyst-cooperative/pudl/issues>`__ or email us at
pudl@catalyst.coop.

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
   * - :ref:`access-zenodo`
     - Researcher, Database User, Notebook Analyst
     - Use a stable, citable, fully processed version of the PUDL on your own computer.
       Use PUDL in Jupyer Notebooks running in a stable, archived Docker container.
       Access the SQLite DB and Parquet files directly using any toolset.
   * - :ref:`access-jupyterhub`
     - New Python User, Notebook Analyst
     - Work through the PUDL example notebooks without any downloads or setup.
       Perform your own notebook-based analyses using PUDL data and limited
       computational resources.
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

.. _access-jupyterhub:

---------------------------------------------------------------------------------------
JupyterHub
---------------------------------------------------------------------------------------

We've set up a `JupyterHub <https://jupyter.org/hub>`__ in collaboration with
`2i2c.org <https://2i2c.org>`__ to provide access to all of the processed PUDL
data and the software environment required to work with it. You don't have to
download or install anything to use it, but we do need to create an account for you.

* Request an account by submitting `this form <https://forms.gle/TN3GuE2e2mnWoFC4A>`__.
* Once we've created an account for you
  `follow this link <https://bit.ly/pudl-examples-01>`__ to log in and open up the first
  example notebook on the JupyterHub.
* You can create your own notebooks and upload, save, and download modest amounts of
  data on the hub.

We can only offer a small amount of memory (4-6GB) and processing power (1 CPU) per
user on the JupyterHub for free. If you need to work with lots of data or do
computationally intensive analysis, you may want to look into using the
:ref:`access-zenodo` option on your own computer. The JupyterHub uses exactly the
same data and software environment as the Zenodo Archives. Eventually we also want to
offer paid access to the JupyterHub with plenty of computing power.

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
