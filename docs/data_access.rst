=======================================================================================
Data Access
=======================================================================================

We publish the PUDL pipeline outputs in several ways to serve different users and use
cases. We're always trying to increase the accessibility of the PUDL data, so if you
have a suggestion, please `open a GitHub issue
<https://github.com/catalyst-cooperative/pudl/issues>`__. If you have a question, you
can `create a GitHub discussion
<https://github.com/orgs/catalyst-cooperative/discussions/new?category=help-me>`__.

We recommend working with tables with the ``out_`` prefix, as these tables contain the
most complete and easiest to work with data. For more information about the different
types of tables, read through :ref:`PUDL's naming conventions <asset-naming>`.

---------------------------------------------------------------------------------------
Quick Reference
---------------------------------------------------------------------------------------

.. list-table::
   :widths: auto
   :header-rows: 1

   * - :ref:`Platform <access-platform>`
     - :ref:`Format <access-format>`
     - :ref:`Version <access-version>`
     - User Types
     - Use Cases

   * - :ref:`access-viewer`
     - Parquet, CSV
     - ``nightly``
     - Data Explorer, Spreadsheet Analyst, Jupyter Notebook User
     - Explore PUDL data interactively in a web browser, including hourly
       timeseries data. Select data to download as CSVs for local analysis in
       spreadsheets. Download full tables as Parquet files to play with
       programmatically.
   * - :ref:`access-datasette`
     - SQLite, CSV
     - ``nightly``
     - Data Explorer, Spreadsheet Analyst, SQL User
     - **DEPRECATED - in early-to-mid-2025 we will switch to**
       :ref:`access-viewer`. Run SQL queries on our SQLite database within your
       browser. Select data to download as CSVs for local analysis in
       spreadsheets. Create sharable links to a particular selection of data.
   * - :ref:`access-kaggle`
     - SQLite, Parquet
     - ``nightly``
     - Data Scientist, Data Analyst, Jupyter Notebook User
     - Work with PUDL data products in Jupyter Notebooks via the web with minimal setup.
       Explore curated and contributed analyses and visualizations using PUDL data.
       notebooks.
       Create and share your own interactive notebooks using PUDL data.
   * - :ref:`access-cloud`
     - SQLite, Parquet
     - ``nightly``, ``stable``
     - Data Scientist, Analytics Engineer, Data Engineer, Cloud Developer
     - Performant remote queries of clearly versioned PUDL Parquet outputs from cloud
       computing platforms or GitHub Actions.
       Fast bulk download of SQLite or Parquet outputs for local use.
       Parquet based data warehouse for large-scale data analysis in the cloud.
       Integrates well with Pandas, DuckDB, and other dataframe libraries.
   * - :ref:`access-zenodo`
     - SQLite, Parquet
     - ``stable``
     - Researcher, Publisher, Archivist
     - Access a specific, immutable version of the PUDL data by DOI for citation in
       academic publications or other applications where long-term reproducibility is
       needed. Web-based bulk download of data for local analysis.

.. _access-modes:

---------------------------------------------------------------------------------------
How Should You Access PUDL Data?
---------------------------------------------------------------------------------------

In order to serve a wider variety of users, we provide several ways to access PUDL data.
When choosing an access method you'll want to consider:

- What tool or platform do you want to use to access the data?
- What data format are you most comfortable with?
- Which historical version of the data do you want?

.. _access-platform:

Data Platform
^^^^^^^^^^^^^

PUDL data is distributed on a number of different platforms to accommodate a variety of
different use cases. These include :ref:`access-viewer`,
:ref:`access-datasette`, :ref:`access-kaggle`,
:ref:`access-cloud`, and :ref:`access-zenodo`.

.. _access-format:

Data Format
^^^^^^^^^^^

PUDL data is distributed in two main file formats

- `SQLite <https://www.sqlite.org>`__: a self-contained relational database that holds
  many tables in a single file, supported by many programming languages and tools.
- `Apache Parquet <https://parquet.apache.org/docs/>`__: a compressed,
  columnar storage format in which each file stores a single table. Parquet supports
  rich data types and metadata, and is highly performant.

All data is distributed with both formats, except:

- **Parquet Only**: The hourly data tables are distributed only as Parquet files.
  These tables have ``hourly`` in their names.
- **SQLite Only**: The :ref:`minimally processed FERC data <access-raw-ferc>` which we
  have converted from XBRL and DBF into SQLite are only available in SQLite.

All Parquet data is available through :ref:`access-viewer`, and can be
downloaded as a CSV through that platform.

All SQLite data is available through :ref:`access-datasette`,
and can be downloaded as a CSV through that platform.

.. _access-version:

Data Version
^^^^^^^^^^^^

We assign a version number to our quarterly data releases so they can be easily
identified. These versions are based on the date of publication. For example,
``v2024.11.0`` would be the first release of the data that happened in November 2024.
These are referred to as ``stable`` releases, and are archived for long-term access and
citation.

We also provide access to a ``nightly`` development build of the data, which is updated
most weekday mornings. These builds are useful for beta testing new outputs, but are
ephemeral and may not be as well validated as the ``stable`` releases.

.. _access-viewer:

---------------------------------------------------------------------------------------
PUDL Viewer
---------------------------------------------------------------------------------------

We recently released the `PUDL Viewer <https://viewer.catalyst.coop/>`__ in beta.

It provides flexible search of table metadata, live data preview with filtering
and sorting, and CSV export of up to 5 million rows. It also provides access to
tables that were too large for Datasette, such as the EPA CEMS emissions data
and the VCE RARE hourly renewable capacity factors data.

Finally, it also has links to the Parquet downloads for each table, which you
can view directly with tools like `Tad <https://www.tadviewer.com/>`__.

Note that the raw :ref:`FERC SQLite databases <access-raw-ferc>` derived from
the old Visual FoxPro and new XBRL data formats are not available here yet - if
you need that, see :ref:`access-datasette`.

.. _access-datasette:

---------------------------------------------------------------------------------------
Datasette
---------------------------------------------------------------------------------------

.. warning::

  Our Datasette instance is deprecated. For performance reasons, we will be
  moving all data access to our new :ref:`access-viewer` in early-mid 2025.

We provide web-based access to the PUDL data via a
`Datasette <https://datasette.io>`__ deployment at:

  `<https://data.catalyst.coop>`__

Datasette is an open source tool developed by
`Simon Willison <https://https://simonwillison.net/>`__ that wraps SQLite databases in
an interactive front-end. It allows users to the PUDL database and metadata, filter the
data them using dropdown menus or SQL, and download the selected data to CSVs.  All the
query parameters are stored in the URL so you can also share links to the data you've
selected.

.. note::

   The only SQLite database containing cleaned and integrated data is `the core PUDL
   database <https://data.catalyst.coop/pudl>`__. There are also several
   :ref:`FERC SQLite databases <access-raw-ferc>` derived from the old Visual FoxPro
   and new XBRL data formats, which we publish as SQLite to improve accessibility of the
   raw inputs, but they should generally not be used directly if the data you need has
   been integrated into the PUDL database.

.. note::

   Only PUDL database tables that are available in SQLite are accessible via Datasette.
   Due to their size, we currently do not load any of the hourly tables into SQLite, and
   distribute them only as Parquet files. For access to the hourly tables, see
   the :ref:`access-viewer`.

.. _access-kaggle:

---------------------------------------------------------------------------------------
Kaggle
---------------------------------------------------------------------------------------

Are you comfortable with Jupyter Notebooks? Want to explore a fresh version of all
available PUDL data without needing to do any environment setup? Our nightly build
outputs automatically update `the PUDL Project Dataset on Kaggle
<https://www.kaggle.com/datasets/catalystcooperative/pudl-project>`__ once a week. There
are `several notebooks
<https://www.kaggle.com/datasets/catalystcooperative/pudl-project/code>`__ associated
with the dataset, both curated by Catalyst and contributed by other Kaggle users.

.. _access-cloud:

---------------------------------------------------------------------------------------
Cloud Storage
---------------------------------------------------------------------------------------

All PUDL data products are freely available in the
`AWS Open Data Registry <https://registry.opendata.aws/catalyst-cooperative-pudl/>`__
including both ``stable`` and ``nightly`` outputs and multiple years of past stable
releases. These include data in both SQLite and Parquet formats. The AWS S3 bucket is:

.. code-block:: bash

   s3://pudl.catalyst.coop

The same outputs are available in a similarly named "requester pays" Google Cloud
Storage bucket. However, you will need to authenticate your GCP account. The GCS
bucket is:

.. code-block:: bash

   gs://pudl.catalyst.coop

SQLite databases must be downloaded for local use, but Parquet files can be queried
remotely using a number of different tools. Some examples below:

Pandas
^^^^^^

Using `Pandas read_parquet() <https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html>`__

.. note::

   You will need to install pandas with the
   `extra cloud dependencies <https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html#install-optional-dependencies>`__.

.. code-block:: python

   import pandas as pd

   # Outputs from the most recent nightly build:
   nightly_df = pd.read_parquet("s3://pudl.catalyst.coop/nightly/core_eia__codes_energy_sources.parquet")
   # Outputs from the most recent stable data release:
   stable_df = pd.read_parquet("s3://pudl.catalyst.coop/stable/core_eia__codes_energy_sources.parquet")
   # A specific stable version of the data:
   versioned_df = pd.read_parquet("s3://pudl.catalyst.coop/v2024.11.0/core_eia__codes_energy_sources.parquet")

DuckDB
^^^^^^

Using `DuckDB <https://duckdb.org/2021/06/25/querying-parquet.html>`__
and the `httpfs extension <https://duckdb.org/docs/guides/network_cloud_storage/s3_import.html>`__

.. code-block:: sql

   -- Install the httpfs extension once and it will be available in subsequent sessions
   INSTALL httpfs;
   SELECT * FROM read_parquet('s3://pudl.catalyst.coop/nightly/core_eia__codes_energy_sources.parquet');

Other Dataframe Libraries
^^^^^^^^^^^^^^^^^^^^^^^^^

Similar functionality exists for the `dplyr library in R
<https://www.pmassicotte.com/posts/2024-05-01-query-s3-duckplyr/>`__, the `polars
library in Rust <https://docs.pola.rs/user-guide/io/cloud-storage/>`__, and many other
programmatic data analysis tools.

The AWS CLI
^^^^^^^^^^^

You can also use `the AWS CLI <https://aws.amazon.com/cli/>`__ to see what data is
available and download it locally. For example, to list the contents of the AWS S3
bucket to see what historic versions are available:

.. code-block:: bash

   aws s3 ls --no-sign-request s3://pudl.catalyst.coop/

To list the contents of a particular version:

.. code-block:: bash

   aws s3 ls --no-sign-request s3://pudl.catalyst.coop/v2024.8.0/

And then download the full PUDL SQLite database from the nightly build outputs:

.. code-block:: bash

   aws s3 cp --no-sign-request s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip .

Direct Links for Bulk Download
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The links below allow bulk download the most recent ``nightly`` builds of all the SQLite
databases produced by PUDL, as well as their associated metadata in JSON.

Fully Processed SQLite Databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `Main PUDL Database <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.sqlite.zip>`__ (~3GB)
* `US Census DP1 Database (2010) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/censusdp1tract.sqlite.zip>`__

.. _access-raw-ferc:

Raw FERC DBF & XBRL data converted to SQLite
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* FERC Form 1:

  * `FERC-1 SQLite derived from DBF (1994-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_dbf.sqlite.zip>`__
  * `FERC-1 SQLite derived from XBRL (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl.sqlite.zip>`__
  * `FERC-1 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl_datapackage.json>`__
  * `FERC-1 XBRL Taxonomy Metadata as JSON (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl_taxonomy_metadata.json>`__

* FERC Form 2:

  * `FERC-2 SQLite derived from DBF (1996-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_dbf.sqlite.zip>`__
  * `FERC-2 SQLite derived from XBRL (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl.sqlite.zip>`__
  * `FERC-2 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl_datapackage.json>`__
  * `FERC-2 XBRL Taxonomy Metadata as JSON (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl_taxonomy_metadata.json>`__

* FERC Form 6:

  * `FERC-6 SQLite derived from DBF (2000-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_dbf.sqlite.zip>`__
  * `FERC-6 SQLite derived from XBRL (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl.sqlite.zip>`__
  * `FERC-6 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl_datapackage.json>`__
  * `FERC-6 XBRL Taxonomy Metadata as JSON (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl_taxonomy_metadata.json>`__

* FERC Form 60:

  * `FERC-60 SQLite derived from DBF (2006-2020) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_dbf.sqlite.zip>`__
  * `FERC-60 SQLite derived from XBRL (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl.sqlite.zip>`__
  * `FERC-60 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl_datapackage.json>`__
  * `FERC-60 XBRL Taxonomy Metadata as JSON (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl_taxonomy_metadata.json>`__

* FERC Form 714:

  * `FERC-714 SQLite derived from XBRL (2021-2023) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl.sqlite.zip>`__
  * `FERC-714 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_datapackage.json>`__
  * `FERC-714 XBRL Taxonomy Metadata as JSON (2021-2023) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_taxonomy_metadata.json>`__

.. _access-zenodo:

---------------------------------------------------------------------------------------
Zenodo Archives
---------------------------------------------------------------------------------------

If you want a specific, immutable version of our data for any reason, you can find them
all `here on Zenodo <https://zenodo.org/doi/10.5281/zenodo.3653158>`__. Zenodo assigns
long-lived DOIs to each archive, suitable for citation in academic journals and other
publications. The most recent versioned PUDL data release can always be found using this
Concept DOI: https://doi.org/10.5281/zenodo.3653158

From Zenodo you can download individual SQLite databases and a zipfile containing all
the Parquet files bundled together.

The documentation for the latest such stable build is `here
<https://catalystcoop-pudl.readthedocs.io/en/stable/>`__. You can access the
documentation for a specific version by hovering over the version selector at the bottom
left of the page.

.. _access-raw:

---------------------------------------------------------------------------------------
Raw Data
---------------------------------------------------------------------------------------

Sometimes you want to see the raw data that is published by the government, but it's
hard to find or difficult to download, or you want to see what an older version of the
published data looked like prior to being revised or deleted.

We use Zenodo to archive and version our raw data inputs. You can find all of our
archives in `the Catalyst Cooperative Community
<https://zenodo.org/communities/catalyst-cooperative/>`__.

These have been minimally processed - in some cases, we've compressed them or grouped
them into ZIP archives to fit the Zenodo repository requirements. In all cases we've
added some metadata to help identify the resources you're looking for. But, apart from
that, these datasets are unmodified.

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
:doc:`contribute to the project <CONTRIBUTING>`, please give it a shot!
