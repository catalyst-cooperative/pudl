=======================================================================================
PUDL Release Notes
=======================================================================================

---------------------------------------------------------------------------------------
v0.4.0 (2021-07-XX)
---------------------------------------------------------------------------------------
This is a ridiculously large update including more than a year and a half's
worth of work.

New Data Coverage
^^^^^^^^^^^^^^^^^

* EIA 860 for 2004-2008 + 2019
* EIA 860m as of 2020-11
* EIA 923 for 2001-2008 + 2019
* EPA CEMS for 2019-2020
* FERC Form 1 for 2019
* US Census Demographic Profile (DP1) for 2010
* FERC Form 714 for 2006-2019 (experimental)
* EIA 861 for 2001-2019 (experimental)

Data Cleaning & Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now inject placeholder utilities in the cloned FERC Form 1 database when
  respondent IDs appear in the data tables, but not in the respondent table.
  This addresses a bunch of structural inconsistencies in the original database.

Hourly Electricity Demand and Historical Utility Territories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With support from GridLab and in collaboration with researchers at the Lawrence
Berkeley National Laboratory, we did a bunch of work on spatially attributing
hourly historical electricity demand. This included:

* Compilation of historical utility and balancing authority service territory
  geometries based on the counties associated with utilities, and the utilities
  associated with balancing authorities in the EIA 861 (2001-2019).
* Allocation of hourly electricity demand from FERC 714 to US states based on
  the historical utility service territories described above.
* A fast timeseries outlier detection routine for cleaning up the FERC 714
  hourly data using correlations between the time series reported by all of the
  different entities.

Data Management and Archiving
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now use a series of web scrapers to collect snapshots of the raw input data
  that is processed by PUDL. These original data are archived as Frictionless
  Data Packages on Zenodo, so that they can be accessed reproducibly and
  programmatically via a REST API. This addresses the problems we were having
  with the v0.3.x releases, in which the original data on the agency websites
  was liable to be modified long after its "final" release, rendering it
  incompatible with our software.
* We have decided to shift to producing a combination of relational databases
  (SQLite files) and columnar data stores (Apache Parquet files) as the primary
  outputs of PUDL. Tabular Data Packages didn't end up serving either database
  or spreadsheet users very well. The CSV file were often too large to access
  via spreadsheets, and users missed out on the relationships between data
  tables. Needing to separately load the data packages into SQLite and Parquet
  was a hassle and generated a lot of overly complicated and fragile code.

Known Issues
^^^^^^^^^^^^

* With the recent addition of new years of EIA and FERC data, we haven't yet
  updated the expected number of rows in each table, so there are some data
  validations which are failing. See issue: and issue: for more details.
* The EIA 861 and FERC 714 data are not yet integrated into the SQLite database
  outputs, because we need to overhaul our entity resolution process to
  accommodate them in the database structure. That work is ongoing.

---------------------------------------------------------------------------------------
v0.3.2 (2020-02-17) Integration of EIA 860 for 2009-2010
---------------------------------------------------------------------------------------
The primary changes in this release:

* The 2009-2010 data for EIA 860 have been integrated, including updates
  to the data validation test cases.
* Output tables are more uniform and less restrictive in what they
  include, no longer requiring PUDL Plant & Utility IDs in some tables.  This
  release was used to compile v1.1.0 of the PUDL Data Release, which is archived
  at Zenodo under this DOI: https://doi.org/10.5281/zenodo.3672068

  With this release, the EIA 860 & 923 data now (finally!) cover the same span
  of time. We do not anticipate integrating any older EIA 860 or 923 data at
  this time.


---------------------------------------------------------------------------------------
v0.3.1 (2020-02-05)
---------------------------------------------------------------------------------------
A couple of minor bugs were found in the preparation of the first PUDL data
release:

* No maximum version of Python was being specified in setup.py. PUDL currently
  only works on Python 3.7, not 3.8.

* ``epacems_to_parquet`` conversion script was erroneously attempting to
  verify the availability of raw input data files, despite the fact that it now
  relies on the packaged post-ETL epacems data. Didn't catch this before since
  it was always being run in a context where the original data was lying
  around... but that's not the case when someone just downloads the released
  data packages and tries to load them.

---------------------------------------------------------------------------------------
v0.3.0 (2020-01-30)
---------------------------------------------------------------------------------------
This release is mostly about getting the infrastructure in place to do regular
data releases via Zenodo, and updating ETL with 2018 data.

Added lots of data validation / quality assurance test cases in anticipation of
archiving data. See the pudl.validate module for more details.

New data since v0.2.0 of PUDL:

* EIA Form 860 for 2018
* EIA Form 923 for 2018
* FERC Form 1 for 1994-2003 and 2018 (select tables)

We removed the FERC Form 1 accumulated depreciation table from PUDL because it
requires detailed row-mapping in order to be accurate across all the years. It
and many other FERC tables will be integrated soon, using new row-mapping
methods.

Lots of new plants and utilities integrated into the PUDL ID mapping process,
for the earlier years (1994-2003).  All years of FERC 1 data should be
integrated for all future ferc1 tables.

Command line interfaces of some of the ETL scripts have changed, see their help
messages for details.

---------------------------------------------------------------------------------------
v0.2.0 (2019-09-17)
---------------------------------------------------------------------------------------
This is the first release of PUDL to generate data packages as the canonical
output, rather than loading data into a local PostgreSQL database. The data
packages can then be used to generate a local SQLite database, without relying
on any software being installed outside of the Python requirements specified for
the catalyst.coop package.

This change will enable easier installation of PUDL, as well as archiving and
bulk distribution of the data products in a platform independent format.

---------------------------------------------------------------------------------------
v0.1.0 (2019-09-12)
---------------------------------------------------------------------------------------

v0.1.0 This is the only release of PUDL that will be made that makes use of
PostgreSQL as the primary data product. It is provided for reference, in case
there are users relying on this setup who need access to a well defined release.
