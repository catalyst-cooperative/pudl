=======================================================================================
What is PUDL?
=======================================================================================

Welcome to the Public Utilities Data Liberation Project (PUDL)! Our README explains that
PUDL has three core components:

- **Raw Data Archives** (raw, versioned inputs)
- **ETL Pipeline** (code to process, clean, and organize the raw inputs)
- **Data Warehouse** (location where ETL outputs, both interim and final, are stored)

Let's dig into each of these...

.. _raw-data-archive:

---------------------------------------------------------------------------------------
Raw Data Archives
---------------------------------------------------------------------------------------

PUDL depends on "raw" data inputs from sources that are known to occasionally update
their data or alter the published format. These changes may be incompatible with the way
the data are read and interpreted by PUDL, so, to ensure the integrity of our data
processing, we periodically create archives of `the raw inputs on Zenodo
<https://zenodo.org/communities/catalyst-cooperative>`__. Each of the data inputs may
have several different versions archived, and all are assigned a unique DOI and made
available through the REST API.  Each release of the PUDL Python package is embedded
with a set of of DOIs to indicate which version of the raw inputs it is meant to
process. This process helps ensure that our outputs are replicable.

To enable programmatic access to individual partitions of the data (by year, state,
etc.), we archive the raw inputs as `Frictionless Data Packages
<https://specs.frictionlessdata.io/data-package/>`__. The data packages contain both the
raw data in their originally published format (CSVs, Excel spreadsheets, and Visual
FoxPro database (DBF) files) and metadata that describes how each the
dataset is partitioned.

The PUDL software will download a copy of the appropriate raw inputs automatically as
needed and organize them in a local :doc:`datastore <dev/datastore>`.

.. seealso::

    The software that creates and archives the raw inputs can be found in our
    `PUDL Archiver <https://github.com/catalyst-cooperative/pudl-archiver>`__
    repository on GitHub.

.. _etl-process:

---------------------------------------------------------------------------------------
The ETL Pipeline
---------------------------------------------------------------------------------------

Dagster stuff, etc. I feel like this is similar to the data warehouse stuff, or rather,
it informs the structure? Talk about validation tests n stuff here.

Data Validation
^^^^^^^^^^^^^^^

We have a growing collection of data validation test cases that we run before
publishing a data release to try and avoid publishing data with known issues. Most of
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

---------------------------------------------------------------------------------------
The Data Warehouse
---------------------------------------------------------------------------------------

The Data Warehouse contains all the cleaned data outputs and interim outputs from the
ETL pipeline.

Data passing through the ETL pipeline passes different phases or "layers"....ADD MORE

Raw Layer
^^^^^^^^^

Assets in the Raw layer read the raw data from the original heterogeneous formats into
a collection of :class:`pandas.DataFrame` with uniform column names across all years so
that it can be easily processed in bulk. Data distributed as binary database files, such
as the DBF files from FERC Form 1, may be converted into a unified SQLite database
before individual dataframes are created. Raw data assets are not written to
``pudl.sqlite``, persisted to pickle files and not distributed to users.

.. seealso::

    Module documentation within the :mod:`pudl.extract` subpackage.

Core Layer
^^^^^^^^^^

The Core layer contains well-modeled assets that serve as building blocks for
downstream wide tables and analyses. Well-modeled means tables in the database
have logical primary keys, foreign keys, datatypes and generally follow
:ref:`Tidy Data standards <tidy-data>`. The assets are loaded to a SQLite
database or Parquet file.

These outputs can be accessed via Python, R, and many other tools. See the
:doc:`data_dictionaries/pudl_db` page for a list of the normalized database tables and
their contents.

Data processing in the Core layer is generally broken down into two phases. Phase one
focuses on cleaning and organizing data within individual tables while phase two focuses
on the integration and deduplication of data between tables. These tasks can be tedious
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
longitude are reported separately every year. Often, these reported values are not
self-consistent. There may be several different spellings of a plant's name, or an
incorrectly reported latitude in one year.

Assets in the Core layer attempt to eliminate this kind of inconsistent and duplicate
information when normalizing the tables by choosing only the most consistently reported
value for inclusion in the final database. If a value which should be static is not
consistently reported, it may also be set to N/A.

Output Layer
^^^^^^^^^^^^^^^^^^^^

Assets in the Core layer normalize the data to make storage more efficient and avoid
data integrity issues, but you may want to combine information from more than one of
the tables to make the data more readable and readily interpretable. For example, PUDL
stores the name that EIA uses to refer to a power plant in the
:ref:`core_eia__entity_plants` table in association with the plant's unique numeric ID.
If you are working with data from the :ref:`core_eia923__monthly_fuel_receipts_costs`
table, which records monthly per-plant fuel deliveries, you may want to have the name
of the plant alongside the fuel delivery information since it's more recognizable than
the plant ID.

Rather than requiring everyone to write their own SQL ``SELECT`` and ``JOIN`` statements
or do a bunch of :func:`pandas.merge` operations to bring together data, PUDL provides a
variety of output tables that contain all of the useful information in one place. In
some cases, like with EIA, the output tables are composed to closely resemble the raw
spreadsheet tables you're familiar with.

The Output layer also contains tables produced by analytical routines for
calculating derived values like the heat rate by generation unit (:meth:`hr_by_unit
<pudl.output.pudltabl.PudlTabl.hr_by_unit>`) or the capacity factor by generator
(:meth:`capacity_factor <pudl.output.pudltabl.PudlTabl.capacity_factor>`). We intend to
integrate more analytical outputs into the library over time.

.. seealso::

    * `The PUDL Examples GitHub repo <https://github.com/catalyst-cooperative/pudl-examples>`__
      to see how to access the PUDL Database directly, use the output functions, or
      work with the EPA CEMS data using Dask.
    * `How to Learn Dask in 2021 <https://coiled.io/blog/how-to-learn-dask-in-2021/>`__
      is a great collection of self-guided resources if you are already familiar with
      Python, Pandas, and NumPy.

.. _test-and-validate:
