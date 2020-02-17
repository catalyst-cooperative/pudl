.. _data-catalog:

===============================================================================
Data Catalog
===============================================================================

.. contents::

-------------------------------------------------------------------------------
Available Data
-------------------------------------------------------------------------------

.. todo::

    Write up more extensive descriptions of each dataset, what's in them, what
    the ETL process looks like for each of them, etc. Maybe use this page as an
    index, with each dataset having its own catalog page. We've got a lot of
    this information written up elsewhere and should be able to cut-and-paste.

.. _data-eia860:

EIA Form 860
^^^^^^^^^^^^

=================== ===========================================================
Source URL          https://www.eia.gov/electricity/data/eia860/
Source Format       Microsoft Excel (.xls/.xlsx)
Source Years        2001-2018
Size (Download)     127 MB
Size (Uncompressed) 247 MB
PUDL Code           ``eia860``
Years Liberated     2009-2018
Records Liberated   ~600,000
Issues              `open EIA 860 issues <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aeia860>`__
=================== ===========================================================

Nearly all of the data reported to the EIA on Form 860 is being pulled into the
PUDL database for the years 2009-2018. This data is tightly integrated with the
EIA 923 data, for which we integrate the same set of years. We do not
anticipate integrating EIA 860 data from before 2009 at at this time, but if
you need that data, let us know.

.. _data-eia923:

EIA Form 923
^^^^^^^^^^^^

=================== ===========================================================
Source URL          https://www.eia.gov/electricity/data/eia923/
Source Format       Microsoft Excel (.xls/.xlsx)
Source Years        2001-2018
Size (Download)     196 MB
Size (Uncompressed) 299 MB
PUDL Code           ``eia923``
Years Liberated     2009-2018
Records Liberated   ~3.2 million
Issues              `Open EIA 923 issues <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aeia923>`__
=================== ===========================================================

Nearly all of EIA Form 923 is being pulled into the PUDL database, for years
2009-2018. Earlier data is available from EIA, but the reporting format for
earlier years is substantially different from the present day, and will require
more work to integrate. Monthly year to date releases are not yet being
integrated, and only larger utilities are required to make monthly reports.

We have not yet integrated tables reporting fuel stocks on hand, data from
Puerto Rico, or EIA 923 schedules 6, 7, and 8.

.. _data-epacems:

EPA CEMS Hourly
^^^^^^^^^^^^^^^

=================== ===========================================================
Source URL          ftp://newftp.epa.gov/dmdnload/emissions/hourly/monthly
Source Format       Comma Separated Value (.csv)
Source Years        1995-2018
Size (Download)     7.6 GB
Size (Uncompressed) ~100 GB
PUDL Code           ``epacems``
Years Liberated     1995-2018
Records Liberated   ~1 billion
Issues              `Open EPA CEMS issues <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aepacems>`__
=================== ===========================================================

All of the EPA's hourly Continuous Emissions Monitoring System (CEMS) data is
available. It is by far the largest dataset in PUDL at the moment, with hourly
records for thousands of plants covering decades. Note that the ETL process
can easily take all day for the full dataset. PUDL also provides a script that
converts the raw EPA CEMS data into Apache Parquet files, which can be read
and queried very efficiently from disk. For usage details run:

.. code-block:: console

    $ epacems_to_parquet --help

Thanks to `Karl Dunkle Werner <https://github.com/karldw>`_ for contributing
much of the EPA CEMS Hourly ETL code.

.. _data-epaipm:

EPA IPM
^^^^^^^

=================== ===========================================================
Source URL          https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6
Source Format       Microsoft Excel (.xlsx)
Source Years        N/A
Size (Download)     14 MB
Size (Uncompressed) 14 MB
PUDL Code           ``epaipm``
Years Liberated     N/A
Records Liberated   ~650,000
Issues              `Open EPA IPM Issues <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aepaipm>`__
=================== ===========================================================

.. todo::

    Get `Greg Schivley <https://github.com/gschivley>`__ to write up a
    description of the EPA IPM dataset.

.. _data-ferc1:

FERC Form 1
^^^^^^^^^^^^

=================== ===========================================================
Source URL          https://www.ferc.gov/docs-filing/forms/form-1/data.asp
Source Format       FoxPro Database (.DBC/.DBF)
Source Years        1994-2018
Size (Download)     1.4 GB
Size (Uncompressed) 10 GB
PUDL Code           ``ferc1``
Years Liberated     1994-2018
Records Liberated   ~12 million (116 raw tables), ~280,000 (7 clean tables)
Issues              `Open FERC Form 1 issues <https://github.com/catalyst-cooperative/pudl/issues?q=is%3Aissue+is%3Aopen+label%3Aferc1>`__
=================== ===========================================================

The FERC Form 1 database consists of 116 data tables containing ~8GB of data,
distributed as separate annual FoxPro databases for the years 1994-2018. PUDL
can extract all of those tables and load them into a single SQLite database
together (See :doc:`Cloning FERC Form 1 <clone_ferc1>`). Thus far we have only
integrated 7 of those tables into the full PUDL ETL pipeline. Mostly we
focused on tables pertaining to power plants, their capital & operating
expenses, and fuel consumption. However, we have the tools required to pull
just about any other table in as well.

We continue to improve the integration between the FERC Form 1 plants and the
EIA plants and generators, many of which represent the same utility assets.
Over time we will pull in and clean up additional FERC Form 1 tables. If
there's data you need from Form 1 in bulk you can
`hire us <https://catalyst.coop/hire-catalyst/>`__ to liberate it first.

-------------------------------------------------------------------------------
Work in Progress
-------------------------------------------------------------------------------

Thanks to a grant from the `Alfred P. Sloan Foundation Energy & Environment
Program <https://sloan.org/programs/research/energy-and-environment>`__, we
have support to integrate the following new datasets.

.. _data-eia861:

EIA Form 861
^^^^^^^^^^^^

=================== ===========================================================
Source URL          https://www.eia.gov/electricity/data/eia861/
Source Format       Microsoft Excel (.xls/.xlsx)
Source Years        2001-2017
Size (Download)     --
Size (Uncompressed) --
PUDL Code           ``eia861``
Years Liberated     --
Records Liberated   --
Issues              `open issues labeled epacems <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aeia861>`__
=================== ===========================================================

This form includes information about utility demand side management programs,
distribution systems, total sales by customer class, net generation, ultimate
disposition of power, and other information. This is a smaller dataset (~100s
of MB) distributed as Microsoft Excel spreadsheets.

.. _data-tmolmp:

ISO/RTO LMP
^^^^^^^^^^^

Locational marginal electricity pricing information from the various grid
operators (e.g. MISO, CAISO, NEISO, PJM, ERCOT...). At high time resolution,
with many different delivery nodes, this will be a very large dataset (hundreds
of GB). The format for the data is different for each of the ISOs. Physical
location of the delivery nodes is not always publicly available.

-------------------------------------------------------------------------------
Future Data
-------------------------------------------------------------------------------

There's a huge variety and quantity of data about the US electric utility
system available to the public. The data listed above is just the beginning!
Other data we've heard demand for are listed below. If you're interested in
using one of them, and would like to add it to PUDL, check out :doc:`our
contribution guidelines <CONTRIBUTING>`. If there are other datasets you think
we should be looking at integration, don't hesitate to `open an issue on Github
<https://github.com/catalyst-cooperative/pudl/issues>`_ requesting the data and
explaining why it would be useful.

.. _data-eiah20:

EIA Water Usage
^^^^^^^^^^^^^^^

`EIA Water <https://www.eia.gov/electricity/data/water/>`_ records water use by
thermal generating stations in the US.

.. _data-ferc714:

FERC Form 714
^^^^^^^^^^^^^

`FERC Form 714 <https://www.ferc.gov/docs-filing/forms/form-714/data.asp>`_
includes hourly loads, reported by load balancing authorities annually. This is
a modestly sized dataset, in the 100s of MB, distributed as Microsoft Excel
spreadsheets.

.. _data-ferceqr:

FERC EQR
^^^^^^^^^

The `FERC EQR <https://www.ferc.gov/docs-filing/eqr/q2-2013/data/database.asp>`_
Also known as the Electricity Quarterly Report or Form 920, this dataset
includes the details of many transactions between different utilities, and
between utilities and merchant generators. It covers ancillary services as well
as energy and capacity, time and location of delivery, prices, contract length,
etc. It's one of the few public sources of information about renewable energy
power purchase agreements (PPAs). This is a large (~100s of GB) dataset,
composed of a very large number of relatively clean CSV files, but it requires
fuzzy processing to get at some of the interesting and only indirectly
reported attributes.

MSHA Mines and Production
^^^^^^^^^^^^^^^^^^^^^^^^^

The `MSHA Mines & Production
<https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp>`_ dataset describes
coal production by mine and operating company, along with statistics about
labor productivity and safety. This is a smaller dataset (100s of MB) available
as relatively clean and well structured CSV files.

PHMSA Natural Gas Pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `PHMSA Natural Gas Pipelines <https://cms.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids>`_
dataset, published by the Pipeline and Hazardous Materials Safety
Administration (which is part of the US Dept. of Transportation) collects data
about the natural gas transmission and distribution system, including their
age, length, diameter, materials, and carrying capacity.

Transmission and Distribution Systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to run electricity system operations models and cost optimizations,
you need some kind of model of the interconnections between generation and
loads. There doesn't appear to be a generally accepted, publicly available set
of these network descriptions (yet!).
