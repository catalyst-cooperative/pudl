=======================================================================================
Overview
=======================================================================================

PUDL is a data processing pipeline that cleans, integrates, and standardizes some of
the most widely used public energy datasets in the US. The data serve researchers,
activists, journalists, and policy makers that might not have the means to purchase
processed data from existing commercial providers, the technical expertise to access
it in its raw form, or the time to clean and prepare the data for bulk analysis.

---------------------------------------------------------------------------------------
Available Data
---------------------------------------------------------------------------------------

Currently, PUDL has deeply integrated data from:

* :doc:`data_sources/eia860` (including EIA 860m)
* :doc:`data_sources/eia923`
* :doc:`data_sources/ferc1` and
* :doc:`data_sources/epacems`

We also have preliminary integrations for EIA 861 and FERC 714. See
:doc:`data_sources/wip_future` for more information.

In addition, we distribute an SQLite databases containing all available years of the
`raw FERC Form 1 data <https://doi.org/10.5281/zenodo.3677547>`__ and an SQLite
version of the `US Census DP1 geodatabase
<https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html>`__

If you want to get started using PUDL data, visit our :doc:`usage_modes` page. Read
on to learn about the components of the data processing pipeline.

.. _raw-data-archive:

---------------------------------------------------------------------------------------
Raw Input Data Archives
---------------------------------------------------------------------------------------

In order for the PUDL data processing pipeline to run successfully, it needs to have
programmatic access to a particular version of the raw input data. Unfortunately, the
agencies that publish this data often alter it long after the "final" release,
changing the contents, structure, and filenames. Older versions of the data typically
do not remain available.

To avoid this issue, we periodically create archives of `the raw inputs on Zenodo
<https://zenodo.org/communities/catalyst-cooperative>`_, where they are issued DOIs
and made available via Zenodo's REST API. Each data source can have several different
versions, each with its own unique DOI. Each release of the PUDL Python package has a
set of DOIs embedded in it, indicating which version of the raw inputs it is meant to
process.

These raw inputs are organized into `Frictionless Data Packages
<https://specs.frictionlessdata.io/data-package/>`__ with some extra metadata
indicating how they are partitioned (by year, state, etc.). The format of the
underlying data varies from source to source, and in some cases from year to year,
and includes CSVs, Excel spreadsheets, and Visual FoxPro database (DBF) files.

The PUDL software will download a copy of the appropriate raw inputs automatically as
needed, and organizes them in a local :doc:`datastore <datastore>`.

.. seealso::

    The software that creates and archives the raw inputs can be found in our `PUDL
    Scrapers <https://github.com/catalyst-cooperative/pudl-scraper>`__ and `PUDL
    Zenodo Storage <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
    repositories on GitHub.

.. _etl-process:

---------------------------------------------------------------------------------------
The ETL Process
---------------------------------------------------------------------------------------

The core of PUDL's work takes place in the ETL (Extract, Transform, and Load)
process.

Extract
^^^^^^^

The Extract step reads the raw data from its original heterogeneous formats into a
collection of :class:`pandas.DataFrame` with uniform column names across all years,
so that it can be easily processed in bulk. In the case of data distributed as binary
database files like DBF such as the FERC Form 1, it may be converted into a unified
SQLite database before individual dataframes are created.

.. seealso::

    Module documentation within the :mod:`pudl.extract` subpackage.

Transform
^^^^^^^^^

The Transform step is generally broken down into two phases. The first focuses on
cleaning and organizing data within individual tables, and the focuses on the
integration and deduplication of data between tables. These tasks can be tedious
`data wrangling toil <https://sre.google/sre-book/eliminating-toil/>`__ that impose a
huge amount of overhead on anyone trying to do analysis based on the publicly
available data. PUDL implements common data cleaning operations in the hopes that we
can all work on more interesting problems most of the time. These operations include:

* Standardization of units (e.g. dollars not thousands of dollars)
* Standardization of N/A values
* Standardization of freeform names and IDs
* Use of controlled vocabularies for categorical values like fuel type
* Use of more readable codes and column names
* Imposition of well defined, rich data types for each column
* Converting local timestamps to UTC
* Reshaping of data into well normalized tables which minimize data duplication
* Inferring Plant IDs which link records across many years of FERC Form 1 data
* Inferring linkages between FERC and EIA Plants and Utilities.
* Inferring more complete associations between EIA boilers and generators

.. seealso::

    The module and per-table transform functions in the :mod:`pudl.transform`
    sub-package have more details on the specific transformations applied to each
    table.

Many of the original datasets contain large amounts of duplicated data. For instance,
the EIA reports the name of each power plant in every table that refers to otherwise
unique plant-related data. Similarly, many attributes like plant latitude and
longitude are reported separately every year. Often these reported values are not
self-consistent. There may be several different spellings of a plant's name, or an
incorrectly reported latitude in one year.

The transform step attempts to eliminate this kind of inconsistent duplicate
information when normalizing the tables, choosing only the most consistently reported
value for inclusion in the final database. If a value which should be static is not
consistently reported, it may also be set to N/A.

.. seealso::

    * `Tidy Data <https://vita.had.co.nz/papers/tidy-data.pdf>`__ by Hadley
      Wickham, Journal of Statistical Software (2014).
    * `A Simple Guide to the Five Normal Forms in Relational Database Theory <https://www.bkent.net/Doc/simple5.htm>`__
      by William Kent, Communications of the ACM (1983).

Load
^^^^

At the end of the Transform step, we have collections of DataFrames which correspond
to database tables. These written out to ("loaded" into) platform indepenent `tabular
data packages <https://specs.frictionlessdata.io/tabular-data-package/>`__ where the
data is stored as CSV files, and the metadata is stored as JSON. These sttatic,
text-based output formats are archive-friendly, and can be used to populate a
database, or read with Python, R, and many other tools.

.. note::

    Starting with v0.5.0 of PUDL, we will begin generating SQLite database and Apache
    Parquet file outputs directly, and using those formats to distribute the
    processed data.

.. seealso::

    Module documentation within the :mod:`pudl.load` sub-package.

.. _db-and-outputs:

---------------------------------------------------------------------------------------
Database & Output Tables
---------------------------------------------------------------------------------------

Tabular Data Packages are archive friendly and platform independent, but given the
size and complexity of the data within PUDL, this format isn't ideal for day to day
interactive use. In practice, we take the clean, processed data in the data packages
and use it to populate a local SQLite database. To handle the ~1 billion row EPA CEMS
hourly time series we convert the data package into Apache Parquet dataset which is
partitioned by state and year.

Denormalized Outputs
^^^^^^^^^^^^^^^^^^^^

Working with the PUDL data interactively, you'll often want to combine information
from more than one table to make the data more readable and readily interpretable. For
example the name that EIA uses to refer to a power plant is only stored in the
:ref:`plants_entity_eia` table in association with the plant's unique numeric ID. If you
are working with data from the :ref:`fuel_receipts_costs_eia923` table, which records
monthly per-plant fuel deliveries, you may want to have the name of the plant alongside
the fuel delivery information since it's more recognizable than the plant ID.

Rather than requiring everyone to write their own SQL ``SELECT`` and ``JOIN``
statements or do a bunch of :func:`pandas.merge` operations to bring together data,
PUDL provides a variety of predefined queries as methods of the
:class:`pudl.output.pudltabl.PudlTabl` class, which do common joins and return
dataframes that are convenient for interactive use. This avoids duplicating data in the
database (which often leads to data integrity issues), but still provides convenient
user access.

.. note::

    In the future we intend to replace the simple denormalized output tables with
    database views which are integrated into the distributed SQLite database directly.
    This will provide the same convenience without requiring use of the Python software
    layer.

Analysis Outputs
^^^^^^^^^^^^^^^^

There are several analytical routines built into the
:mod:`pudl.output.pudltabl.PudlTabl` output objects for calculating derived values
like the heat rate by generation unit (:meth:`hr_by_unit
<pudl.output.pudltabl.PudlTabl.hr_by_unit>`), or the capacity factor by generator
(:meth:`cap_fact <pudl.output.pudltabl.PudlTabl.cap_fact>`). We intend to integrate
more analytical output into the library over time.

.. seealso::

    * `The PUDL Examples GitHub repo <https://github.com/catalyst-cooperative/pudl-examples>`__
      to see how to access the PUDL Database directly, use the output functions, or
      work with the EPA CEMS data using Dask.
    * `How to Learn Dask in 2021 <https://coiled.io/blog/how-to-learn-dask-in-2021/>`__
      is a great collection of self-guided resources if you are already familiar with
      Python, Pandas, and NumPy.

.. _test-and-validate:

---------------------------------------------------------------------------------------
Data Validation
---------------------------------------------------------------------------------------
We have a growing collection of data validation test cases which we run before
publishing a data release to try and avoid publishing data wth known issues. Most of
these validations are described in the :mod:`pudl.validate` module. They check things
like:

* The heat content of various fuel types are within expected bounds.
* Coal ash, moisture, mercury, sulfur etc. content are within expected bounds
* Generator heat rates and capacity factors are realistic for the type of prime mover
  being reported.

Some data validations are currently only specified within our test suite, including:

* The expected number of records within each table
* The fact that there are no entirely N/A columns

A variety of database integrity checks are also run either during the ETL process or
when the data is loaded into SQLite.

See our :doc:`dev/testing` documentation for more information.
