=======================================================================================
PUDL Overview
=======================================================================================

PUDL is a data processing pipeline that cleans, connects, and standardizes data from
some of the most widely used public energy datasets. The data serve researchers,
activists, journalists, and policy makers that might not have the means to purchase
processed data from existing commercial providers, the technical expertise to
access it as published, or the time to clean and prepare the data for bulk analysis.

Currently, PUDL processes, integrates, and provides enhanced access to data from EIA
Forms 860 and 923, FERC Forms 1 and 714, and EPA CEMS. See our
:doc:`data_sources/wip_future` page for more data in the works!

If you want to get started using PUDL data, visit our :doc:`usage_modes` page. Read on
to learn about the components of the data processing pipeline.

.. _raw-data-archive:

---------------------------------------------------------------------------------------
Raw Data Archive
---------------------------------------------------------------------------------------

In order for scripts that extract data published online to be reliable, they need
consistent access to consistent data. When data providers remove data, change its
location, or retrospectively update the formatting, the scripts designed to grab the old
version will either fail or output bad data. To avoid this undue reliance and
uncertainty, we’ve archived all of the raw data inputs on `Zenodo
<https://zenodo.org/>`_, an open-access repository operated by CERN. Reading inputs from
Zenodo rather than external sources guarantees PUDL access to the same, verified data
inputs.

.. _etl-process:

---------------------------------------------------------------------------------------
ETL Process
---------------------------------------------------------------------------------------

The magic of PUDL occurs in the ETL (extract, transform, and load) process. This is how
the data are cleaned and made programmatically accessible.

Extract :mod:`pudl.extract`
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The extract step reads the raw data CSVs from the Zenodo archives and feeds them into
the transform step.

Transform :mod:`pudl.transform`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The transform step processes the raw data so that they are more readily usable for data
analysis and aggregation. Anyone working with public data knows the immense overhead
needed to make data analysis-ready. With large datasets, users almost always encounter
errors and formatting discrepancies that make bulk processing unreliable, if not
impossible. PUDL takes care of this so that you don’t have to.

Some of the basic transformations that PUDL implements include:

* Aggregation of all available years of data into one table
* Correction of spelling errors
* Removal of excess white space
* Removal of duplicate data
* Removal of non-data inputs (such as headers, notes etc.)
* Clarification of column headers (enumerate acronyms and add units)
* Standardization of units (ex: $1000 to $1)
* Standardization of categorical inputs (ex: oil = OIL, oil1, petro, pet…)
* Standardization of N/A values (ex: <NA> = .,  , None)

Some of the more complex data transformations include:

* Data normalization (combining and comparing similar data from different tables)
* Creation of plant id for FERC records
* Cross-association of boilers, generating units, and plants
* Linkage of FERC plants with EIA plants

Many of the original datasets are published as a series of data tables with duplicated
fields. For instance, the EIA reports `plant_name` in all tables with otherwise unique
plant-related data. The transform step eliminates this duplicate information by
normalizing the tables in accordance with tidy data principles. As a part of the
transform step, we “harvest” duplicated data fields, combine them, and check for
consistency. The data harvesting and normalization process makes for efficient for data
storage and easy integration with relational databases, like SQL, that we use to provide
quick programmatic access to data.

For a list of the specific transformations applied to each table, see the `doc-strings <https://catalystcoop-pudl.readthedocs.io/en/latest/api/pudl.transform.html>`_
for each of the transform methods.

Load :mod:`pudl.load`
^^^^^^^^^^^^^^^^^^^^^

Once transformed, the data are loaded into `frictionless data
packages <https://specs.frictionlessdata.io/data-package/>`_ that can be used to populate
a database, or read directly with Python, R, Microsoft Access, and many other tools.

.. _db-and-outputs:

---------------------------------------------------------------------------------------
Database & Output Tables
---------------------------------------------------------------------------------------

Once passed through the ETL, the data are clean but not yet suitable for bulk
programmatic use. PUDL solves this by loading the data packages into a SQL database that
can be queried for quick access to particular tables.

Due to their enormous size, the continuous emissions monitoring system (CEMS) data are
loaded into parquet files, organized by state and year, that are queryable with Dask.

Because the SQL tables and the parquet files do not contain duplicate information, it’s
likely that you’ll want to combine multiple tables to build a dataset that’s useful
(i.e. combine plant location with plant capacity with some other information). Perhaps
you just want a cleaned version of the EIA tables you’re familiar with. PUDL offers that
too! Our output tables are denormalized to include the duplicate columns that were
harvested during the transform step. PUDL also creates and provides access to
specialized analysis tables that contain common calculations and imputations otherwise
absent from the original data. You can access the SQL database, the denormalized output
tables, and the analysis tables through a handful of access modes that cater to users
with differing use cases and technical expertise.

.. _test-and-verify:

---------------------------------------------------------------------------------------
Testing & Verification
---------------------------------------------------------------------------------------
We have created a rigorous testing environment to ensure that PUDL accurately reads and
transforms raw data. Tests are executed automatically and upon request with a given
script. Depending on the severity of an error, the data processing with either stop or
notify the user of a problem when the tests fail.

Our current suite of tests include:

* Checking raw data column and row length
* Verifying reported heat rates against known standards
* More

See our :doc:`dev/testing` page for more information.
