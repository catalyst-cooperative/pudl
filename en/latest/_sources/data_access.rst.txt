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
   * - :ref:`access-kaggle`
     - Parquet, DuckDB, SQLite
     - ``nightly``
     - Data Scientist, Data Analyst, Jupyter Notebook User
     - Work with PUDL data products in Jupyter Notebooks via the web with minimal setup.
       Explore curated and contributed analyses and visualizations using PUDL data.
       notebooks.
       Create and share your own interactive notebooks using PUDL data.
   * - :ref:`access-cloud`
     - Parquet, DuckDB, SQLite
     - ``nightly``, ``stable``
     - Data Scientist, Analytics Engineer, Data Engineer, Cloud Developer
     - Performant remote queries of clearly versioned PUDL Parquet outputs from cloud
       computing platforms or GitHub Actions.
       Fast bulk download of outputs for local use.
       Parquet based data warehouse for large-scale data analysis in the cloud.
       Integrates well with Pandas, Polars, DuckDB, and other dataframe libraries.
   * - :ref:`access-zenodo`
     - Parquet, DuckDB, SQLite
     - ``stable``
     - Researcher, Publisher, Archivist
     - Access a specific, immutable version of the PUDL data by DOI for citation in
       academic publications or other applications where long-term reproducibility is
       needed. Web-based bulk download of data for local analysis.
   * - :ref:`access-agent-skill`
     - Parquet
     - ``nightly``, ``stable``
     - Coding Agent User
     - Let an AI coding agent (Claude Code, OpenCode, Pi, etc.) discover PUDL tables,
       look up column meanings and data-quality caveats, and load the Parquet outputs
       into a notebook or script.

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
different use cases. These include :ref:`access-viewer`, :ref:`access-kaggle`,
:ref:`access-cloud`, :ref:`access-zenodo`, and the :ref:`PUDL agent skill
<access-agent-skill>` for use with AI coding agents.

.. _access-format:

Data Format
^^^^^^^^^^^

PUDL data is distributed in several file formats:

- `Apache Parquet <https://parquet.apache.org/docs/>`__: a compressed, columnar storage
  format in which each file stores a single table. Parquet supports rich data types and
  metadata, and is highly performant. **Parquet is PUDL's primary output format**, and
  the format we recommend for all new work.
- `DuckDB <https://duckdb.org>`__: a fast, self-contained analytical database that holds
  many tables in a single file. We publish the fully processed PUDL data as a single
  ``pudl.duckdb`` database, assembled from the Parquet outputs. It preserves the full
  set of column types and primary key constraints, but omits foreign key constraints to
  keep the file size down.
- `SQLite <https://www.sqlite.org>`__: a self-contained relational database that holds
  many tables in a single file, supported by many programming languages and tools.

.. warning::

   **The ``pudl.sqlite`` database is being deprecated.** It is currently provided
   purely for backwards compatibility, and **we will stop producing SQLite versions of
   the fully processed PUDL data in 2027**. Please migrate to the Parquet outputs
   or the new ``pudl.duckdb`` database. This deprecation does **not** affect the
   :ref:`minimally processed raw FERC data <access-raw-ferc>`, which will continue to be
   distributed as SQLite for the time being.

Not every table is available in every format:

- **Parquet only**: The hourly data tables are distributed only as Parquet files. These
  tables have ``hourly`` in their names, and are excluded from the ``pudl.duckdb`` and
  ``pudl.sqlite`` databases.
- **SQLite only**: The :ref:`minimally processed FERC data <access-raw-ferc>` which we
  have converted from XBRL and DBF into SQLite are only available in SQLite (and,
  experimentally, DuckDB — see :ref:`access-raw-ferc-duckdb`).

All Parquet data is available through :ref:`access-viewer` for previewing. It can be
downloaded as a CSV through that platform if you need to work with it in spreadsheets.
For programmatic use we **strongly recommend** that you access the Parquet files in
S3 directly. See :ref:`access-cloud`.

The ``pudl.duckdb`` and ``pudl.sqlite`` databases and the raw FERC SQLite databases can
be downloaded from S3 (see :ref:`access-cloud`) or our regular versioned releases (see
:ref:`access-zenodo`). All the converted FERC databases can also be accessed through
:ref:`access-viewer`.

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
PUDL Data Viewer
---------------------------------------------------------------------------------------

The `PUDL Data Viewer <https://data.catalyst.coop/>`__ provides flexible search of table
metadata, live data preview with filtering and sorting, and CSV export of up to 5
million rows. It provides access to all of the PUDL Parquet outputs, the minimally
processed FERC Form 1, 2, 6, 60, and 714 data, and the FERC EQR.

It also provides links to download Parquet files for each table, which you can view
locally with tools like `Tad <https://www.tadviewer.com/>`__.

.. _access-kaggle:

---------------------------------------------------------------------------------------
Kaggle
---------------------------------------------------------------------------------------

Are you comfortable with Jupyter Notebooks? Want to explore a fresh version of all
available PUDL data without needing to do any environment setup?  We provide several
`example notebooks on Kaggle <https://www.kaggle.com/catalystcooperative/code>`__.
(they are also pushed to our `PUDL Examples Repo on GitHub
<https://github.com/catalyst-cooperative/pudl-examples>`__). These notebooks pull data
directly from the Parquet outputs in S3 (see below) and so can also be used locally if
you're familiar with setting up a Python environment and running Jupyter.

Our nightly build outputs also automatically update the `PUDL Kaggle dataset
<https://www.kaggle.com/datasets/catalystcooperative/pudl-project>`__ once a week. This
dataset contains all the PUDL outputs, so it's quite large and can take a few minutes
to copy into your Kaggle notebook's private workspace, but once it's copied, access will
be fast.

.. _access-cloud:

---------------------------------------------------------------------------------------
Cloud Storage
---------------------------------------------------------------------------------------

All PUDL data products are freely available in the
`AWS Open Data Registry <https://registry.opendata.aws/catalyst-cooperative-pudl/>`__
including both ``stable`` and ``nightly`` outputs and multiple years of past stable
releases. These include data in Parquet, DuckDB, and SQLite formats. The AWS S3 bucket
is:

.. code-block:: bash

   s3://pudl.catalyst.coop

The same outputs are available in a similarly named "requester pays" Google Cloud
Storage bucket. However, you will need to authenticate your GCP account. The GCS
bucket is:

.. code-block:: bash

   gs://pudl.catalyst.coop

The SQLite databases must be downloaded for local use, but Parquet and DuckDB files
can be queried remotely using a number of different tools. Some examples below:

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

`DuckDB <https://duckdb.org/2021/06/25/querying-parquet.html>`__ with the `httpfs
extension <https://duckdb.org/docs/guides/network_cloud_storage/s3_import.html>`__ can
query the PUDL outputs in place on S3 — you don't need to download anything first.

.. note::

    Our bucket name contains dots (``pudl.catalyst.coop``), which breaks DuckDB's
    default virtual-host S3 addressing: the request goes to
    ``pudl.catalyst.coop.s3.us-west-2.amazonaws.com``, and AWS's wildcard TLS
    certificate (``*.s3.us-west-2.amazonaws.com``) only covers a single label, so the
    handshake fails. Create an anonymous S3 secret that uses path-style addressing to
    avoid this. It applies to both ``read_parquet()`` and ``ATTACH``:

.. code-block:: sql

   -- Install the httpfs extension once and it will be available in subsequent sessions
   INSTALL httpfs;
   LOAD httpfs;
   CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path');

To query an individual Parquet file:

.. code-block:: sql

   SELECT * FROM read_parquet('s3://pudl.catalyst.coop/nightly/core_eia__codes_energy_sources.parquet');

To query the full ``pudl.duckdb`` database, attach it read-only and refer to its tables
by name:

.. code-block:: sql

   ATTACH 's3://pudl.catalyst.coop/nightly/pudl.duckdb' AS pudl (READ_ONLY);
   SELECT
       report_date,
       plant_id_eia,
       generator_id,
       capacity_mw
   FROM pudl.out_eia__yearly_generators
   WHERE technology_description = 'Nuclear';

The same works from the `DuckDB Python API
<https://duckdb.org/docs/stable/clients/python/overview>`__:

.. code-block:: python

   import duckdb

   con = duckdb.connect()
   con.execute("INSTALL httpfs; LOAD httpfs;")
   con.execute("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path')")
   con.execute("ATTACH 's3://pudl.catalyst.coop/nightly/pudl.duckdb' AS pudl (READ_ONLY)")
   df = con.execute(
       "SELECT * FROM pudl.out_eia__yearly_generators WHERE technology_description = 'Nuclear'"
   ).df()

Downloading ``pudl.duckdb`` for local use works too, and will be faster if you plan to
run many queries — see :ref:`direct-download`.

Polars
^^^^^^

`Polars <https://docs.pola.rs/>`__ can scan the `Parquet outputs directly from S3
<https://docs.pola.rs/user-guide/io/cloud-storage/>`__, using lazy evaluation so that
filters and column selection are pushed down and only the data you actually need is
downloaded:

.. code-block:: python

   import polars as pl

   lf = pl.scan_parquet(
       "s3://pudl.catalyst.coop/nightly/out_eia__yearly_generators.parquet",
       storage_options={"aws_region": "us-west-2", "aws_skip_signature": "true"},
   )
   df = (
       lf.filter(pl.col("technology_description") == "Nuclear")
       .select("report_date", "plant_id_eia", "generator_id", "capacity_mw")
       .collect()
   )

R (dplyr)
^^^^^^^^^

The `arrow <https://arrow.apache.org/docs/r/>`__ package lets you `open a Parquet file
directly from S3 <https://www.pmassicotte.com/posts/2024-05-01-query-s3-duckplyr/>`__
with `dplyr <https://dplyr.tidyverse.org/>`__ verbs. Column selection and row filters
are pushed down to Arrow, so only the data you ask for is read; call ``collect()`` to
pull the result into a regular data frame:

.. code-block:: r

   library(arrow)
   library(dplyr)

   generators <- open_dataset(
     "s3://pudl.catalyst.coop/nightly/out_eia__yearly_generators.parquet",
     format = "parquet"
   )

   df <- generators |>
     filter(technology_description == "Nuclear") |>
     select(report_date, plant_id_eia, generator_id, capacity_mw) |>
     collect()

The AWS CLI
^^^^^^^^^^^

You can also use `the AWS CLI <https://aws.amazon.com/cli/>`__ to see what data is
available and download it locally. For example, to list the contents of the AWS S3
bucket to see what historic versions are available:

.. code-block:: bash

   aws s3 ls --no-sign-request s3://pudl.catalyst.coop/

To list the contents of a particular version:

.. code-block:: bash

   aws s3 ls --no-sign-request s3://pudl.catalyst.coop/v2026.8.0/

To download the full PUDL DuckDB database (9 GB) from the nightly build outputs:

.. code-block:: bash

   aws s3 cp --no-sign-request s3://pudl.catalyst.coop/nightly/pudl.duckdb .

.. _direct-download:

Direct Links for Bulk Download
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The links below allow bulk download the most recent ``nightly`` builds of the PUDL
Parquet, DuckDB, and SQLite outputs, as well as their associated metadata in JSON.

Fully Processed Parquet Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `PUDL Parquet Datapackage (JSON) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl_parquet_datapackage.json>`__:
  a `Frictionless Data Package v2 <https://datapackage.org/>`__ descriptor listing all
  PUDL Parquet tables with full column types, constraints, and foreign keys. Browse the
  schema of every table without downloading any data.
* `PUDL Parquet Archive (ZIP) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl_parquet.zip>`__:
  all PUDL Parquet files bundled together with the ``datapackage.json`` descriptor
  inside. Suitable for bulk download to a local machine.

Fully Processed DuckDB Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `Main PUDL Database <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.duckdb>`__

Fully Processed SQLite Databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

   The fully processed ``pudl.sqlite`` database is deprecated and will no longer be
   produced starting in 2027. See :ref:`access-format`. Use the Parquet outputs or the
   ``pudl.duckdb`` database instead.

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

  * `FERC-714 SQLite derived from XBRL (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl.sqlite.zip>`__
  * `FERC-714 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_datapackage.json>`__
  * `FERC-714 XBRL Taxonomy Metadata as JSON (2021-2024) <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_taxonomy_metadata.json>`__

.. _access-raw-ferc-duckdb:

Raw FERC XBRL data converted to DuckDB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable remote querying of the converted FERC databases, we have started using DuckDB
as an output format. Currently it only includes the more recent XBRL data. Within DuckDB
you can now do queries like this:

.. code-block:: sql

   INSTALL httpfs;
   LOAD httpfs;
   -- Path-style S3 addressing is required because our bucket name contains dots.
   -- See the DuckDB notes under Cloud Storage above.
   CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path');
   ATTACH 's3://pudl.catalyst.coop/nightly/ferc1_xbrl.duckdb' AS ferc1_xbrl (READ_ONLY);
   SELECT * FROM ferc1_xbrl.transmission_lines_added_during_year_424_duration;

You can also use the `DuckDB Python API <https://duckdb.org/docs/stable/clients/python/overview>`__
to create a connection and execute the above statements, and then use that connection
with :meth:`pandas.read_sql` and other libraries that understand DBAPI connections.

* FERC Form 1: ``s3://pudl.catalyst.coop/nightly/ferc1_xbrl.duckdb`` (`direct download <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl.duckdb>`__)
* FERC Form 2: ``s3://pudl.catalyst.coop/nightly/ferc2_xbrl.duckdb`` (`direct download <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl.duckdb>`__)
* FERC Form 6: ``s3://pudl.catalyst.coop/nightly/ferc6_xbrl.duckdb`` (`direct download <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl.duckdb>`__)
* FERC Form 60: ``s3://pudl.catalyst.coop/nightly/ferc60_xbrl.duckdb`` (`direct download <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl.duckdb>`__)
* FERC Form 714: ``s3://pudl.catalyst.coop/nightly/ferc714_xbrl.duckdb`` (`direct download <https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl.duckdb>`__)

.. _access-ferceqr:

FERC EQR (Form 920)
^^^^^^^^^^^^^^^^^^^

In early 2026 we started processing and distributing the
:doc:`FERC Electric Quarterly Reports (EQR) <data_sources/ferceqr>` dataset as a
partitioned Apache Parquet dataset. Even as Parquet the EQR is close to 100 GB, so
unlike the other PUDL data products, we can't archive it on Zenodo or provide access to
multiple historical versions through S3. Instead, we update a single set of outputs in
S3 on a quarterly basis. The current outputs can be found at:

.. code-block:: bash

   aws s3 ls --no-sign-request s3://pudl.catalyst.coop/ferceqr/

Each of the subdirectories corresponds to a table. Like the main PUDL database, these
tables are documented in the :doc:`data_dictionaries/pudl_db`:

   * :ref:`core_ferceqr__contracts`
   * :ref:`core_ferceqr__quarterly_identity`
   * :ref:`core_ferceqr__quarterly_index_pub`
   * :ref:`core_ferceqr__transactions`

The subdirectories contain a number of Parquet files (``2025q3.parquet``,
``2025q2.parquet``, etc.), each containing one quarter of data for that table. Most
tools for reading Parquet files out of cloud storage are able to query and read from
many files sharing the same schema at the same time, typically with file globbing
wildcards. Note that the EQR tables (particularly :ref:`core_ferceqr__transactions`) can
be much larger than memory, so you will need to select only a subset of the data and/or
use tools that are designed to work with large data efficiently like `DuckDB
<https://duckdb.org/docs/stable/>`__ or `Polars <https://docs.pola.rs/>`__. Some brief
examples:

.. tab-set::

   .. tab-item:: SQL (DuckDB)

      .. code:: sql

         INSTALL httpfs;
         LOAD httpfs;
         -- Path-style S3 addressing is required because our bucket name contains dots.
         CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path');
         SELECT * FROM 's3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet'
         WHERE seller_company_name LIKE '%Bonneville%'
         LIMIT 10;

   .. tab-item:: Python (DuckDB/Pandas)

      .. code:: python

         import duckdb
         # Path-style S3 addressing is required because our bucket name contains dots.
         con = duckdb.connect()
         con.execute("INSTALL httpfs; LOAD httpfs;")
         con.execute("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path')")
         # Query S3 with DuckDB and convert the result to pandas
         df = con.execute("""
            SELECT *
            FROM 's3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet'
            WHERE seller_company_name LIKE '%Bonneville%'
            LIMIT 10
         """).to_df()

   .. tab-item:: Python (Polars)

      .. code:: python

         import polars as pl
         # Use scan_parquet (lazy evaluation) and filter
         df = (
            pl.scan_parquet(
                  "s3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet",
                  storage_options={"aws_region": "us-west-2", "aws_skip_signature": "True"},
            )
            .filter(pl.col("seller_company_name").str.contains("Bonneville"))
            .head(10)
            .collect()
         )

.. _access-agent-skill:

---------------------------------------------------------------------------------------
Coding Agents (PUDL Agent Skill)
---------------------------------------------------------------------------------------

If you already work with an AI coding agent (Claude Code, OpenCode, Pi, and similar
tools), you can install the **PUDL agent skill** from the
`catalyst-cooperative/agent-skills
<https://github.com/catalyst-cooperative/agent-skills>`__ repository on GitHub. The
skill teaches your agent how to:

- discover which PUDL tables exist and what each table and column means,
- surface data-quality caveats and usage warnings recorded in the table metadata, and
- load the Parquet outputs directly from S3 or a local directory into a notebook or
  script.

It reads the same published Parquet outputs and Frictionless metadata described
elsewhere on this page, and does not require the ``pudl`` Python package to be
installed. See the `skill's README
<https://github.com/catalyst-cooperative/agent-skills/tree/main/skills/pudl>`__ for
installation and usage instructions. It builds on a companion ``datapackage`` skill
defined in the same repository which provides similar help for any dataset described by
a ``datapackage.json`` descriptor.

.. _access-zenodo:

---------------------------------------------------------------------------------------
Zenodo Archives
---------------------------------------------------------------------------------------

If you want a specific, immutable version of our data for any reason, you can find them
all `here on Zenodo <https://zenodo.org/doi/10.5281/zenodo.3653158>`__. Zenodo assigns
long-lived DOIs to each archive, suitable for citation in academic journals and other
publications. The most recent versioned PUDL data release can always be found using this
Concept DOI: https://doi.org/10.5281/zenodo.3653158

From Zenodo you can download the ``pudl.duckdb`` database, individual SQLite databases,
a zipfile containing all the Parquet files bundled together (``pudl_parquet.zip``, which
includes a ``datapackage.json`` descriptor), and the standalone
``pudl_parquet_datapackage.json`` descriptor for browsing the schema without
downloading any data.

The documentation for the latest such stable build is `here
<https://docs.catalyst.coop/pudl/en/stable/>`__. You can access the
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
