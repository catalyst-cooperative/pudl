===============================================================================
Data Catalog
===============================================================================

.. contents::

-------------------------------------------------------------------------------
Available Data
-------------------------------------------------------------------------------

.. _data-eia860:

EIA Form 860
^^^^^^^^^^^^

=================== ============================================
Source URL          https://www.eia.gov/electricity/data/eia860/
Source Format       Microsoft Excel (.xlsx)
Source Years        2001-2017
Size (Download)     127 MB
Size (Uncompressed) 247 MB
PUDL Code           ``eia860``
Years Liberated     2011-2017
Records Liberated   ~500,000
=================== ============================================

Nearly all of the data reported to the EIA on Form 860 is being pulled into the
PUDL database, for the years 2011-2017. Earlier years use a different reporting
format, and will require more work to integrate. Monthly year to date releases
are not yet being integrated.

.. _data-eia923:

EIA Form 923
^^^^^^^^^^^^

=================== ============================================
Source URL          https://www.eia.gov/electricity/data/eia923/
Source Format       Microsoft Excel (.xlsx)
Source Years        2001-2017
Size (Download)     196 MB
Size (Uncompressed) 299 MB
PUDL Code           ``eia923``
Years Liberated     2009-2017
Records Liberated   ~2 million
=================== ============================================

Nearly all of EIA Form 923 is being pulled into the PUDL database, for years
2009-2017. Earlier data is available from EIA, but the reporting format for
earlier years is substantially different from the present day, and will require
more work to integrate. Monthly year to date releases are not yet being
integrated.

.. _data-epacems:

EPA CEMS Hourly
^^^^^^^^^^^^^^^

=================== ======================================================
Source URL          ftp://newftp.epa.gov/dmdnload/emissions/hourly/monthly
Source Format       Comma Separated Value (.csv)
Source Years        1995-2018
Size (Download)     7.6 GB
Size (Uncompressed) ~100 GB
PUDL Code           ``epacems``
Years Liberated     1995-2018
Records Liberated   ~1 billion
=================== ======================================================

The EPA's hourly Continuous Emissions Monitoring System (CEMS) data is in the
process of being integrated. However, it is a much larger dataset than the FERC
or EIA data we've already brought in, and so has required some changes to the
overall ETL process. Data from 1995-2017 can be loaded, but it has not yet
been fully integrated. The ETL process for all states and all years takes about
8 hours on a fast laptop. Many thanks to `Karl Dunkle Werner <https://github.com/karldw>`_ for contributing much of the EPA CEMS Hourly ETL code.

.. _data-epaipm:

EPA IPM
^^^^^^^

=================== ============================================================================
Source URL          https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6
Source Format       Microsoft Excel (.xlsx)
Source Years        N/A
Size (Download)     14 MB
Size (Uncompressed) 14 MB
PUDL Code           ``epaipm``
Years Liberated     N/A
Records Liberated   ~650,000
=================== ============================================================================

.. todo::

    Get Greg Schivley to write up a description of the EPA IPM dataset.

.. _data-ferc1:

FERC Form 1
^^^^^^^^^^^^

=================== ======================================================
Source URL          https://www.ferc.gov/docs-filing/forms/form-1/data.asp
Source Format       FoxPro Database (.DBC/.DBF)
Source Years        1994-2018
Size (Download)     1.4 GB
Size (Uncompressed) 2.5 GB
PUDL Code           ``ferc1``
Years Liberated     1994-2018 (raw), 2004-2017 (parboiled)
Records Liberated   ~12 million (raw), ~270,000 (parboiled)
=================== ======================================================

A subset of the FERC Form 1 data, mostly pertaining to power plants, their
capital & operating expenses, and fuel consumption. This data has been
integrated into PUDL for the years 2004-2017. More work will be required to
integrate the rest of the years and data. However we make *all* of the FERC
Form 1 data available (7.2 GB of data in 116 tables, going back to 1994) in its
raw form via an SQLite database.

-------------------------------------------------------------------------------
Work in Progress
-------------------------------------------------------------------------------

Better EPA CEMS Hourly
^^^^^^^^^^^^^^^^^^^^^^

`Finalizing the EPA CEMS Hourly
<https://github.com/catalyst-cooperative/pudl/projects/9>`_  data integration
with at least "good enough" timezone cleanup, linkages to the EIA860 plants and
generators, and the most accessible output we can manage for a dataset with
almost a billion records.

More FERC Form 1
^^^^^^^^^^^^^^^^

`FERC Form 1 <https://github.com/catalyst-cooperative/pudl/projects/3>`_ with
at least a "good enough" integration of the plant and fuel data, so that the
non-fuel operating costs can be used to estimate the marginal cost of
electricity on a per-generator basis, in combination with the fuel costs from
EIA 923.

More EIA Form 860
^^^^^^^^^^^^^^^^^

The 2009-2010 data is similar in format to later years, and should be
relatively easy to integrate. This would give us the same coverage as EIA 923,
which would be good since the two datasets are so tightly integrated. Currently
we are extending the EIA 860 data back to 2009 when necessary to integrate with
EIA 923.

EIA Form 861
^^^^^^^^^^^^

This form includes information about utility demand side management programs,
distribution systems, total sales by customer class, net generation, ultimate
disposition of power, and other information. This is a smaller dataset (~100s
of MB) distributed as Microsoft Excel spreadsheets.

ISO/RTO LMP
^^^^^^^^^^^

Locational marginal electricity pricing information from the various grid
operators (e.g. MISO, CAISO, NEISO, PJM, ERCOT...). At high time resolution,
with many different delivery nodes, this can become a very large dataset (100s
of GB). The format for the data is different for each of the ISOs. Physical
location of the delivery nodes is not always publicly available.


-------------------------------------------------------------------------------
Future Data of Interest
-------------------------------------------------------------------------------

There's a huge variety and quantity of data about the US electric utility
system available to the public. The data listed above is just the beginning!
Other data we've heard demand for are listed below. If you're interested in
using one of them, and would like to add it to PUDL, check out :doc:`our
contribution guidelines <CONTRIBUTING>`. If there are other datasets you think
we should be looking at integration, don't hesitate to `open an issue on Github
<https://github.com/catalyst-cooperative/pudl/issues>`_ requesting the data and
explaining why it would be useful.

EIA Water Usage
^^^^^^^^^^^^^^^

`EIA Water <https://www.eia.gov/electricity/data/water/>`_ records water use by
thermal generating stations in the US.

FERC Form 714
^^^^^^^^^^^^^

`FERC Form 714 <https://www.ferc.gov/docs-filing/forms/form-714/data.asp>`_
includes hourly loads, reported by load balancing authorities annually. This is
a modestly sized dataset, in the 100s of MB, distributed as Microsoft Excel
spreadsheets.

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

US Transmission and Distribution System Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to run electricity system operations models and cost optimizations,
you need some kind of model of the interconnections between generation and
loads. There doesn't appear to be a generally accepted, publicly available set
of these network descriptions (yet!).
