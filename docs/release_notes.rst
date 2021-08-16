=======================================================================================
PUDL Release Notes
=======================================================================================

---------------------------------------------------------------------------------------
0.4.0 (2021-08-16)
---------------------------------------------------------------------------------------
This is a ridiculously large update including more than a year and a half's
worth of work.

New Data Coverage
^^^^^^^^^^^^^^^^^

* :doc:`data_sources/eia860` for 2004-2008 + 2019, plus eia860m through 2020.
* :doc:`data_sources/eia923` for 2001-2008 + 2019
* :doc:`data_sources/epacems` for 2019-2020
* :doc:`data_sources/ferc1` for 2019
* :ref:`US Census Demographic Profile (DP1) <data-censusdp1tract>` for 2010
* :ref:`data-ferc714` for 2006-2019 (experimental)
* :ref:`data-eia861` for 2001-2019 (experimental)

Documentation & Data Accessibility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We've updated and (hopefully) clarified the documentation, and no longer expect
most users to perform the data processing on their own. Instead, we are offering
several methods of directly accessing already processed data:

* Processed data archives on Zenodo that include a Docker container preserving
  the required software environment for working with the data.
* `A repository of PUDL example notebooks <https://github.com/catalyst-cooperative/pudl-examples>`__
* `A JupyterHub instance <https://catalyst-cooperative.pilot.2i2c.cloud/>`__
  hosted in collaboration with `2i2c <https://2i2c.org>`__
* Browsable database access via `Datasette <https://datasette.io>`__ at
  https://data.catalyst.coop

Users who still want to run the ETL themselves will need to set up the
:doc:`set up the PUDL development environment <dev/dev_setup>`

Data Cleaning & Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now inject placeholder utilities in the cloned FERC Form 1 database when
  respondent IDs appear in the data tables, but not in the respondent table.
  This addresses a bunch of unsatisfied foreign key constraints in the original
  databases published by FERC.
* We're doing much more software testing and data validation, and so hopefully
  we're catching more issues early on.

Hourly Electricity Demand and Historical Utility Territories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With support from `GridLab <https://gridlab.org>`__ and in collaboration with
researchers at Berkeley's `Center for Environmental Public Policy
<https://gspp.berkeley.edu/faculty-and-impact/centers/cepp>`__, we did a bunch
of work on spatially attributing hourly historical electricity demand. This work
was largely done by :user:`ezwelty` and :user:`yashkumar1803` and included:

* Semi-programmatic compilation of historical utility and balancing authority
  service territory geometries based on the counties associated with utilities,
  and the utilities associated with balancing authorities in the EIA 861
  (2001-2019). See e.g. :pr:`670` but also many others.
* A method for spatially allocating hourly electricity demand from FERC 714 to
  US states based on the overlapping historical utility service territories
  described above. See :pr:`741`
* A fast timeseries outlier detection routine for cleaning up the FERC 714
  hourly data using correlations between the time series reported by all of the
  different entities. See :pr:`871`

Net Generation and Fuel Consumption for All Generators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We have developed an experimental methodology to produce net generation and
fuel consumption for all generators. The process has known issues and is being
actively developed. See :pr:`989`

Net electricity generation and fuel consumption are reported in multiple ways in
the EIA 923. The :ref:`generation_fuel_eia923` table reports both generation and
fuel consumption, and breaks them down by plant, prime mover, and fuel. In
parallel, the :ref:`generation_eia923` table reports generation by generator,
and the :ref:`boiler_fuel_eia923` table reports fuel consumption by boiler.

The :ref:`generation_fuel_eia923` table is more complete, but the
:ref:`generation_eia923` + :ref:`boiler_fuel_eia923` tables are more granular.
The :ref:`generation_eia923` table includes only ~55% of the total MWhs reported
in the :ref:`generation_fuel_eia923` table.

The :mod:`pudl.analysis.allocate_net_gen` module estimates the net electricity
generation and fuel consumption attributable to individual generators based on
the more expansive reporting of the data in the :ref:`generation_fuel_eia923`
table.

Data Management and Archiving
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now use a series of web scrapers to collect snapshots of the raw input data
  that is processed by PUDL. These original data are archived as
  `Frictionless Data Packages <https://specs.frictionlessdata.io/data-package/>`__
  on `Zenodo <https://zenodo.org>`__, so that they can be accessed reproducibly
  and programmatically via a REST API. This addresses the problems we were
  having with the v0.3.x releases, in which the original data on the agency
  websites was liable to be modified long after its "final" release, rendering
  it incompatible with our software. These scrapers and the Zenodo archiving
  scripts can be found in our
  `pudl-scrapers <https://github.com/catalyst-cooperative/pudl-scrapers>`__ and
  `pudl-zenodo-storage <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
  repositories. The archives themselves can be found within the
  `Catalyst Cooperative community on Zenodo <https://zenodo.org/communities/catalyst-cooperative/>`__
* There's an experimental caching system that allows these Zenodo archives to
  work as long-term "cold storage" for citation and reproducibility, with
  cloud object storage acting as a much faster way to access the same data for
  day to day non-local use, implemented by :user:`rousik`
* We've decided to shift to producing a combination of relational databases
  (SQLite files) and columnar data stores (Apache Parquet files) as the primary
  outputs of PUDL. `Tabular Data Packages <https://specs.frictionlessdata.io/tabular-data-package/>`__
  didn't end up serving either database or spreadsheet users very well. The CSV
  file were often too large to access via spreadsheets, and users missed out on
  the relationships between data tables. Needing to separately load the data
  packages into SQLite and Parquet was a hassle and generated a lot of overly
  complicated and fragile code.

Known Issues
^^^^^^^^^^^^

* The EIA 861 and FERC 714 data are not yet integrated into the SQLite database
  outputs, because we need to overhaul our entity resolution process to
  accommodate them in the database structure. That work is ongoing, see
  :issue:`639`
* The EIA 860 and EIA 923 data don't cover exactly the same rage of years. EIA
  860 only goes back to 2004, while EIA 923 goes back to 2001. This is because
  the pre-2004 EIA 860 data is stored in the DBF file format, and we need to
  update our extraction code to deal with the different format. This means some
  analyses that require both EIA 860 and EIA 923 data (like the calculation of
  heat rates) can only be performed as far back as 2004 at the moment. See
  :issue:`848`
* There are 387 EIA utilities and 228 EIA palnts which appear in the EIA 923,
  but which haven't yet been assigned PUDL IDs and associated with the
  corresponding utilities and plants reported in the FERC Form 1. These entities
  show up in the 2001-2008 EIA 923 data that was just integrated. These older
  plants and utilities can't yet be used in conjuction with FERC data. When the
  EIA 860 data for 2001-2003 has been integrated, we will finish this manual
  ID assignment process. See :issue:`848,1069`
* 52 of the algorithmically assigned ``plant_id_ferc1`` values found in the
  ``plants_steam_ferc1`` table are currently associated with more than one
  ``plant_id_pudl`` value (99 PUDL plant IDs are involved), indicating either
  that the algorithm is making poor assignments, or that the manually assigned
  ``plant_id_pudl`` values are incorrect. This is out of several thousand
  distinct ``plant_id_ferc1`` values. See :issue:`954`
* The county FIPS codes associated with coal mines reported in the Fuel Receipts and
  Costs table are being treated inconsistently in terms of their data types, especially
  in the output functions, so they are currently being output as floating point numbers
  that have been cast to strings, rather than zero-padded integers that are strings. See
  :issue:`1119`

---------------------------------------------------------------------------------------
0.3.2 (2020-02-17)
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
0.3.1 (2020-02-05)
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
0.3.0 (2020-01-30)
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
0.2.0 (2019-09-17)
---------------------------------------------------------------------------------------
This is the first release of PUDL to generate data packages as the canonical
output, rather than loading data into a local PostgreSQL database. The data
packages can then be used to generate a local SQLite database, without relying
on any software being installed outside of the Python requirements specified for
the catalyst.coop package.

This change will enable easier installation of PUDL, as well as archiving and
bulk distribution of the data products in a platform independent format.

---------------------------------------------------------------------------------------
0.1.0 (2019-09-12)
---------------------------------------------------------------------------------------

This is the only release of PUDL that will be made that makes use of
PostgreSQL as the primary data product. It is provided for reference, in case
there are users relying on this setup who need access to a well defined release.
