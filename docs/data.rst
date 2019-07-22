Data Catalog
==================================

.. list-table::
   :header-rows: 1

   * - Data Source
     - Years
     - Download
     - On Disk
     - # of Records
   * - **\ `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`_\ **
     - 2001-2017
     - 100 MB
     - 400 MB
     - ~500,000
   * - **\ `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`_\ **
     - 2001-2017
     - 200 MB
     - 500 MB
     - ~2 million
   * - **\ `EPA CEMS <https://ampd.epa.gov/ampd/>`_\ **
     - 1995-2018
     - 10 GB
     - 100 GB
     - ~1 billion
   * - **\ `FERC Form 1 <https://www.ferc.gov/docs-filing/forms/form-1/data.asp>`_\ **
     - 1994-2017
     - 1 GB
     - 10 GB
     - ~1 million

Available Data
--------------

.. _data-eia860:

EIA Form 860
^^^^^^^^^^^^

* Years Available: 2001-2017
* Years Integrated: 2011-2017
* Download Size: ~400 MB
* Size on Disk:
* Number of Records: ~500,000
* Original Format: Microsoft Excel (.xlsx)
* Download: https://www.eia.gov/electricity/data/eia860/

Nearly all of the data reported to the EIA on Form 860 is being pulled into the
PUDL database, for the years 2011-2017. Earlier years use a different reporting
format, and will require more work to integrate. Monthly year to date releases
are not yet being integrated.

.. _data-eia923:

EIA Form 923
^^^^^^^^^^^^

* Years available: 2001-2017
* Years integrated: 2009-2017
* Total download size: ~500 MB
* Size on disk:
* Number of records: ~2 million
* Original Format: Microsoft Excel (.xlsx)
* Download: https://www.eia.gov/electricity/data/eia923/

Nearly all of EIA Form 923 is being pulled into the PUDL database, for years
2009-2017. Earlier data is available from EIA, but the reporting format for
earlier years is substantially different from the present day, and will require
more work to integrate. Monthly year to date releases are not yet being
integrated.

.. _data-epacems:

EPA CEMS Hourly
^^^^^^^^^^^^^^^

* Years Available: 1995-2018
* Years Integrated: 1995-2018
* Download Size: 8 GB
* Size on Disk (uncompressed): ~100 GB
* Number of Records: ~1 billion
* Original Format: Comma Separated Value (.csv)
* Download: https://ampd.epa.gov/ampd/

The EPA's hourly Continuous Emissions Monitoring System (CEMS) data is in the
process of being integrated. However, it is a much larger dataset than the FERC
or EIA data we've already brought in, and so has required some changes to the
overall ETL process. Data from 1995-2017 can be loaded, but it has not yet
been fully integrated. The ETL process for all states and all years takes about
8 hours on a fast laptop. Many thanks to `Karl Dunkle Werner <https://github.com/karldw>`_ for contributing much of the EPA CEMS Hourly ETL code.

.. _data-epaipm:

EPA IPM
^^^^^^^
* Years Available: N/A
* Download Size:
* Size on Disk:
* Original Format: Microsoft Excel (.xlsx)
* Number of Records:
* Download:

.. todo::

    Get Greg Schivley to write up a description of the EPA IPM dataset.

.. _data-ferc1:

FERC Form 1
^^^^^^^^^^^^

* Years Available: 1994-2018
* Total Download size: 1 GB
* Total Size on disk: 8 GB
* Total Records: ~12 million
* Original Format: FoxPro Database (.DBC/.DBF)
* Download: https://www.ferc.gov/docs-filing/forms/form-1/data.asp

* Data integrated: 2004-2017
* Size on disk: ~100 MB
* Number of records: ~1 million

A subset of the FERC Form 1 data, mostly pertaining to power plants, their
capital & operating expenses, and fuel consumption. This data has been
integrated into PUDL for the years 2004-2017. More work will be required to
integrate the rest of the years and data. However we make *all* of the FERC
Form 1 data available (7.2 GB of data in 116 tables, going back to 1994) in its
raw form via an SQLite database.

Works in Progress
-------------------------

Improving & Expanding FERC Form 1
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`FERC Form 1 <https://github.com/catalyst-cooperative/pudl/projects/3>`_ with
at least a "good enough" integration of the plant and fuel data, so that the
non-fuel operating costs can be used to estimate the marginal cost of
electricity on a per-generator basis, in combination with the fuel costs from
EIA 923.

Finalizing EPA CEMS Hourly
^^^^^^^^^^^^^^^^^^^^^^^^^^

`Finalizing the EPA CEMS Hourly
<https://github.com/catalyst-cooperative/pudl/projects/9>`_  data integration
with at least "good enough" timezone cleanup, linkages to the EIA860 plants and
generators, and the most accessible output we can manage for a dataset with
almost a billion records.

Expanding EIA Form 860
^^^^^^^^^^^^^^^^^^^^^^

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

Monthly Updates
^^^^^^^^^^^^^^^

Several of the datasets we have integrated are updated more frequently than
annually, and ideally PUDL would be able to pull that more recent data, even if
it is incomplete or provisional, to ensure that users have the most up to date
information available. This would include at least EIA 860, EIA 923, EPA CEMS,
and (eventually) the RTO/ISO LMP data.

Future Data of Interest
------------------------

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
