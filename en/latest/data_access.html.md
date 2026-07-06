# Data Access

We publish the PUDL pipeline outputs in several ways to serve different users and use
cases. We’re always trying to increase the accessibility of the PUDL data, so if you
have a suggestion, please [open a GitHub issue](https://github.com/catalyst-cooperative/pudl/issues). If you have a question, you
can [create a GitHub discussion](https://github.com/orgs/catalyst-cooperative/discussions/new?category=help-me).

We recommend working with tables with the `out_` prefix, as these tables contain the
most complete and easiest to work with data. For more information about the different
types of tables, read through [PUDL’s naming conventions](dev/naming_conventions.md#asset-naming).

## Quick Reference

| [Platform](#access-platform)       | [Format](#access-format)   | [Version](#access-version)   | User Types                                                         | Use Cases                                                                                                                                                                                                                                                                                                                                         |
|------------------------------------|----------------------------|------------------------------|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [PUDL Data Viewer](#access-viewer) | Parquet, CSV               | `nightly`                    | Data Explorer, Spreadsheet Analyst, Jupyter Notebook User          | Explore PUDL data interactively in a web browser, including hourly<br/>timeseries data. Select data to download as CSVs for local analysis in<br/>spreadsheets. Download full tables as Parquet files to play with<br/>programmatically.                                                                                                          |
| [Kaggle](#access-kaggle)           | SQLite, Parquet            | `nightly`                    | Data Scientist, Data Analyst, Jupyter Notebook User                | Work with PUDL data products in Jupyter Notebooks via the web with minimal setup.<br/>Explore curated and contributed analyses and visualizations using PUDL data.<br/>notebooks.<br/>Create and share your own interactive notebooks using PUDL data.                                                                                            |
| [Cloud Storage](#access-cloud)     | SQLite, Parquet            | `nightly`, `stable`          | Data Scientist, Analytics Engineer, Data Engineer, Cloud Developer | Performant remote queries of clearly versioned PUDL Parquet outputs from cloud<br/>computing platforms or GitHub Actions.<br/>Fast bulk download of SQLite or Parquet outputs for local use.<br/>Parquet based data warehouse for large-scale data analysis in the cloud.<br/>Integrates well with Pandas, DuckDB, and other dataframe libraries. |
| [Zenodo Archives](#access-zenodo)  | SQLite, Parquet            | `stable`                     | Researcher, Publisher, Archivist                                   | Access a specific, immutable version of the PUDL data by DOI for citation in<br/>academic publications or other applications where long-term reproducibility is<br/>needed. Web-based bulk download of data for local analysis.                                                                                                                   |

<a id="access-modes"></a>

## How Should You Access PUDL Data?

In order to serve a wider variety of users, we provide several ways to access PUDL data.
When choosing an access method you’ll want to consider:

- What tool or platform do you want to use to access the data?
- What data format are you most comfortable with?
- Which historical version of the data do you want?

<a id="access-platform"></a>

### Data Platform

PUDL data is distributed on a number of different platforms to accommodate a variety of
different use cases. These include [PUDL Data Viewer](#access-viewer), [Kaggle](#access-kaggle),
[Cloud Storage](#access-cloud), and [Zenodo Archives](#access-zenodo).

<a id="access-format"></a>

### Data Format

PUDL data is distributed in two main file formats

- [SQLite](https://www.sqlite.org): a self-contained relational database that holds
  many tables in a single file, supported by many programming languages and tools.
- [Apache Parquet](https://parquet.apache.org/docs/): a compressed,
  columnar storage format in which each file stores a single table. Parquet supports
  rich data types and metadata, and is highly performant.

All data is distributed with both formats, except:

- **Parquet Only**: The hourly data tables are distributed only as Parquet files.
  These tables have `hourly` in their names.
- **SQLite Only**: The [minimally processed FERC data](#access-raw-ferc) which we
  have converted from XBRL and DBF into SQLite are only available in SQLite.

All Parquet data is available through [PUDL Data Viewer](#access-viewer) for previewing. It can be
downloaded as a CSV through that platform if you need to work with it in spreadsheets.
For programmatic use we **strongly recommend** that you access the Parquet files in
S3 directly. See [Cloud Storage](#access-cloud).

All SQLite data can be downloaded from S3 (see [Cloud Storage](#access-cloud)) or our regular
versionsed releases (see [Zenodo Archives](#access-zenodo)). We are [working on integrating all
converted FERC databases](https://github.com/catalyst-cooperative/eel-hole/issues/4).
into [PUDL Data Viewer](#access-viewer).

<a id="access-version"></a>

### Data Version

We assign a version number to our quarterly data releases so they can be easily
identified. These versions are based on the date of publication. For example,
`v2024.11.0` would be the first release of the data that happened in November 2024.
These are referred to as `stable` releases, and are archived for long-term access and
citation.

We also provide access to a `nightly` development build of the data, which is updated
most weekday mornings. These builds are useful for beta testing new outputs, but are
ephemeral and may not be as well validated as the `stable` releases.

<a id="access-viewer"></a>

## PUDL Data Viewer

We recently released the [PUDL Data Viewer](https://data.catalyst.coop/) in beta.

It provides flexible search of table metadata, live data preview with filtering
and sorting, and CSV export of up to 5 million rows.

Finally, it also has links to the Parquet downloads for each table, which you
can view directly with tools like [Tad](https://www.tadviewer.com/).

<a id="access-kaggle"></a>

## Kaggle

Are you comfortable with Jupyter Notebooks? Want to explore a fresh version of all
available PUDL data without needing to do any environment setup?  We provide several
[example notebooks on Kaggle](https://www.kaggle.com/catalystcooperative/code).
(they are also pushed to our [PUDL Examples Repo on GitHub](https://github.com/catalyst-cooperative/pudl-examples)). These notebooks pull data
directly from the Parquet outputs in S3 (see below) and so can also be used locally if
you’re familiar with setting up a Python environment and running Jupyter.

Our nightly build outputs also automatically update the [PUDL Kaggle dataset](https://www.kaggle.com/datasets/catalystcooperative/pudl-project) once a week. This
dataset contains all the PUDL outputs, so it’s quite large and can take a few minutes
to copy into your Kaggle notebook’s private workspace, but once it’s copied, access will
be fast.

<a id="access-cloud"></a>

## Cloud Storage

All PUDL data products are freely available in the
[AWS Open Data Registry](https://registry.opendata.aws/catalyst-cooperative-pudl/)
including both `stable` and `nightly` outputs and multiple years of past stable
releases. These include data in both SQLite and Parquet formats. The AWS S3 bucket is:

```bash
s3://pudl.catalyst.coop
```

The same outputs are available in a similarly named “requester pays” Google Cloud
Storage bucket. However, you will need to authenticate your GCP account. The GCS
bucket is:

```bash
gs://pudl.catalyst.coop
```

SQLite databases must be downloaded for local use, but Parquet files can be queried
remotely using a number of different tools. Some examples below:

### Pandas

Using [Pandas read_parquet()](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html)

#### NOTE
You will need to install pandas with the
[extra cloud dependencies](https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html#install-optional-dependencies).

```python
import pandas as pd

# Outputs from the most recent nightly build:
nightly_df = pd.read_parquet("s3://pudl.catalyst.coop/nightly/core_eia__codes_energy_sources.parquet")
# Outputs from the most recent stable data release:
stable_df = pd.read_parquet("s3://pudl.catalyst.coop/stable/core_eia__codes_energy_sources.parquet")
# A specific stable version of the data:
versioned_df = pd.read_parquet("s3://pudl.catalyst.coop/v2024.11.0/core_eia__codes_energy_sources.parquet")
```

### DuckDB

Using [DuckDB](https://duckdb.org/2021/06/25/querying-parquet.html)
and the [httpfs extension](https://duckdb.org/docs/guides/network_cloud_storage/s3_import.html)

```sql
-- Install the httpfs extension once and it will be available in subsequent sessions
INSTALL httpfs;
SELECT * FROM read_parquet('s3://pudl.catalyst.coop/nightly/core_eia__codes_energy_sources.parquet');
```

### Other Dataframe Libraries

Similar functionality exists for the [dplyr library in R](https://www.pmassicotte.com/posts/2024-05-01-query-s3-duckplyr/), the [polars
library in Rust](https://docs.pola.rs/user-guide/io/cloud-storage/), and many other
programmatic data analysis tools.

### The AWS CLI

You can also use [the AWS CLI](https://aws.amazon.com/cli/) to see what data is
available and download it locally. For example, to list the contents of the AWS S3
bucket to see what historic versions are available:

```bash
aws s3 ls --no-sign-request s3://pudl.catalyst.coop/
```

To list the contents of a particular version:

```bash
aws s3 ls --no-sign-request s3://pudl.catalyst.coop/v2024.8.0/
```

And then download the full PUDL SQLite database from the nightly build outputs:

```bash
aws s3 cp --no-sign-request s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip .
```

### Direct Links for Bulk Download

The links below allow bulk download the most recent `nightly` builds of the PUDL
parquet and SQLite outputs, as well as their associated metadata in JSON.

#### Fully Processed Parquet Data

* [PUDL Parquet Datapackage (JSON)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl_parquet_datapackage.json):
  a [Frictionless Data Package v2](https://datapackage.org/) descriptor listing all
  PUDL Parquet tables with full column types, constraints, and foreign keys. Browse the
  schema of every table without downloading any data.
* [PUDL Parquet Archive (ZIP)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl_parquet.zip):
  all PUDL Parquet files bundled together with the `datapackage.json` descriptor
  inside. Suitable for bulk download to a local machine.

#### Fully Processed SQLite Databases

* [Main PUDL Database](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.sqlite.zip) (~3GB)
* [US Census DP1 Database (2010)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/censusdp1tract.sqlite.zip)

<a id="access-raw-ferc"></a>

#### Raw FERC DBF & XBRL data converted to SQLite

* FERC Form 1:
  * [FERC-1 SQLite derived from DBF (1994-2020)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_dbf.sqlite.zip)
  * [FERC-1 SQLite derived from XBRL (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl.sqlite.zip)
  * [FERC-1 Datapackage (JSON) describing SQLite derived from XBRL](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl_datapackage.json)
  * [FERC-1 XBRL Taxonomy Metadata as JSON (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl_taxonomy_metadata.json)
* FERC Form 2:
  * [FERC-2 SQLite derived from DBF (1996-2020)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_dbf.sqlite.zip)
  * [FERC-2 SQLite derived from XBRL (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl.sqlite.zip)
  * [FERC-2 Datapackage (JSON) describing SQLite derived from XBRL](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl_datapackage.json)
  * [FERC-2 XBRL Taxonomy Metadata as JSON (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl_taxonomy_metadata.json)
* FERC Form 6:
  * [FERC-6 SQLite derived from DBF (2000-2020)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_dbf.sqlite.zip)
  * [FERC-6 SQLite derived from XBRL (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl.sqlite.zip)
  * [FERC-6 Datapackage (JSON) describing SQLite derived from XBRL](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl_datapackage.json)
  * [FERC-6 XBRL Taxonomy Metadata as JSON (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl_taxonomy_metadata.json)
* FERC Form 60:
  * [FERC-60 SQLite derived from DBF (2006-2020)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_dbf.sqlite.zip)
  * [FERC-60 SQLite derived from XBRL (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl.sqlite.zip)
  * [FERC-60 Datapackage (JSON) describing SQLite derived from XBRL](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl_datapackage.json)
  * [FERC-60 XBRL Taxonomy Metadata as JSON (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl_taxonomy_metadata.json)
* FERC Form 714:
  * [FERC-714 SQLite derived from XBRL (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl.sqlite.zip)
  * [FERC-714 Datapackage (JSON) describing SQLite derived from XBRL](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_datapackage.json)
  * [FERC-714 XBRL Taxonomy Metadata as JSON (2021-2024)](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl_taxonomy_metadata.json)

<a id="access-raw-ferc-duckdb"></a>

#### Raw FERC XBRL data converted to DuckDB (EXPERIMENTAL)

To enable remote querying of the converted FERC databases, we are experimenting with
DuckDB as an output format. Currently it only includes the more recent XBRL data. Within
DuckDB you can now do queries like this:

```sql
INSTALL httpfs; LOAD httpfs;
ATTACH 's3://pudl.catalyst.coop/nightly/ferc1_xbrl.duckdb' AS ferc1_xbrl (READ_ONLY);
SELECT * FROM ferc1_xbrl.transmission_lines_added_during_year_424_duration;
```

You can also use the [DuckDB Python API](https://duckdb.org/docs/stable/clients/python/overview)
to create a connection and execute the above statements, and then use that connection
with `pandas.read_sql()` and other libraries that understand DBAPI connections.

* FERC Form 1: `s3://pudl.catalyst.coop/nightly/ferc1_xbrl.duckdb` ([direct download](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc1_xbrl.duckdb))
* FERC Form 2: `s3://pudl.catalyst.coop/nightly/ferc2_xbrl.duckdb` ([direct download](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl.duckdb))
* FERC Form 6: `s3://pudl.catalyst.coop/nightly/ferc6_xbrl.duckdb` ([direct download](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc6_xbrl.duckdb))
* FERC Form 60: `s3://pudl.catalyst.coop/nightly/ferc60_xbrl.duckdb` ([direct download](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc60_xbrl.duckdb))
* FERC Form 714: `s3://pudl.catalyst.coop/nightly/ferc714_xbrl.duckdb` ([direct download](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/ferc714_xbrl.duckdb))

<a id="access-ferceqr"></a>

### FERC EQR (EXPERIMENTAL)

In early 2026 we started processing and distributing the
[FERC Electric Quarterly Reports (EQR)](data_sources/ferceqr.md) dataset as a
partitioned Apache Parquet dataset. Even as Parquet the EQR is close to 100 GB, so
unlike the other PUDL data products, we can’t archive it on Zenodo or provide access to
multiple historical versions through S3. Instead, we update a single set of outputs in
S3 on a quarterly basis. The current outputs can be found at:

```bash
aws s3 ls --no-sign-request s3://pudl.catalyst.coop/ferceqr/
```

Each of the subdirectories corresponds to a table. Like the main PUDL database, these
tables are documented in the [PUDL Data Dictionary](data_dictionaries/pudl_db.md):

> * [core_ferceqr_\_contracts](data_dictionaries/pudl_db.md#core-ferceqr-contracts)
> * [core_ferceqr_\_quarterly_identity](data_dictionaries/pudl_db.md#core-ferceqr-quarterly-identity)
> * [core_ferceqr_\_quarterly_index_pub](data_dictionaries/pudl_db.md#core-ferceqr-quarterly-index-pub)
> * [core_ferceqr_\_transactions](data_dictionaries/pudl_db.md#core-ferceqr-transactions)

The subdirectories contain a number of Parquet files (`2025q3.parquet`,
`2025q2.parquet`, etc.), each containing one quarter of data for that table. Most
tools for reading Parquet files out of cloud storage are able to query and read from
many files sharing the same schema at the same time, typically with file globbing
wildcards. Note that the EQR tables (particularly [core_ferceqr_\_transactions](data_dictionaries/pudl_db.md#core-ferceqr-transactions)) can
be much larger than memory, so you will need to select only a subset of the data and/or
use tools that are designed to work with large data efficiently like [DuckDB](https://duckdb.org/docs/stable/) or [Polars](https://docs.pola.rs/). Some brief
examples:

### SQL (DuckDB)

```sql
SELECT * FROM 's3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet'
WHERE seller_company_name LIKE '%Bonneville%'
LIMIT 10;
```

### Python (DuckDB/Pandas)

```python
import duckdb
import pandas as pd
# Query S3 with DuckDB and convert the result to pandas
df = duckdb.query("""
   SELECT *
   FROM 's3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet'
   WHERE seller_company_name LIKE '%Bonneville%'
   LIMIT 10
""").to_df()
```

### Python (Polars)

```python
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
```

<a id="access-zenodo"></a>

## Zenodo Archives

If you want a specific, immutable version of our data for any reason, you can find them
all [here on Zenodo](https://zenodo.org/doi/10.5281/zenodo.3653158). Zenodo assigns
long-lived DOIs to each archive, suitable for citation in academic journals and other
publications. The most recent versioned PUDL data release can always be found using this
Concept DOI: [https://doi.org/10.5281/zenodo.3653158](https://doi.org/10.5281/zenodo.3653158)

From Zenodo you can download individual SQLite databases, a zipfile containing all the
Parquet files bundled together (`pudl_parquet.zip`, which includes a
`datapackage.json` descriptor), and the standalone
`pudl_parquet_datapackage.json` descriptor for browsing the schema without
downloading any data.

The documentation for the latest such stable build is [here](https://docs.catalyst.coop/pudl/en/stable/). You can access the
documentation for a specific version by hovering over the version selector at the bottom
left of the page.

<a id="access-raw"></a>

## Raw Data

Sometimes you want to see the raw data that is published by the government, but it’s
hard to find or difficult to download, or you want to see what an older version of the
published data looked like prior to being revised or deleted.

We use Zenodo to archive and version our raw data inputs. You can find all of our
archives in [the Catalyst Cooperative Community](https://zenodo.org/communities/catalyst-cooperative/).

These have been minimally processed - in some cases, we’ve compressed them or grouped
them into ZIP archives to fit the Zenodo repository requirements. In all cases we’ve
added some metadata to help identify the resources you’re looking for. But, apart from
that, these datasets are unmodified.

<a id="access-development"></a>

## Development Environment

If you want to run the PUDL data processing pipeline yourself from scratch, run the
software tests, or make changes to the source code, you’ll need to set up our
development environment. This is a bit involved, so it has its
[own separate documentation](dev/dev_setup.md).

Most users shouldn’t need to do this, and will probably find working with the
pre-processed data via one of the other access modes easier. But if you want to
[contribute to the project](CONTRIBUTING.md), please give it a shot!
