.. _wip_future:

===============================================================================
Work in Progress & Future Datasets
===============================================================================

.. contents::

-------------------------------------------------------------------------------
Work in Progress
-------------------------------------------------------------------------------

Thanks to a grant from the `Alfred P. Sloan Foundation Energy & Environment Program
<https://sloan.org/programs/research/energy-and-environment>`__, we have support to
integrate the following new datasets.


.. _data-tmolmp:

ISO/RTO LMP
^^^^^^^^^^^

Locational marginal electricity pricing information from the various grid operators
(e.g. MISO, CAISO, NEISO, PJM, ERCOT...). At high time resolution, with many different
delivery nodes, this will be a very large dataset (hundreds of GB). The format for the
data is different for each of the ISOs. Physical location of the delivery nodes is not
always publicly available.

-------------------------------------------------------------------------------
Future Data
-------------------------------------------------------------------------------

There's a huge variety and quantity of data about the US electric utility system
available to the public. The data listed above is just the beginning! Other data we've
heard demand for are listed below. If you're interested in using one of them, and would
like to add it to PUDL, check out :doc:`our contribution guidelines <../CONTRIBUTING>`.
If there are other datasets you think we should be looking at integration, don't
hesitate to `open an issue on Github
<https://github.com/catalyst-cooperative/pudl/issues>`_ requesting the data and
explaining why it would be useful.

.. _data-eiah20:

EIA Water Usage
^^^^^^^^^^^^^^^

`EIA Water <https://www.eia.gov/electricity/data/water/>`_ records water use by thermal
generating stations in the US.

.. _data-ferc714:

FERC Form 714
^^^^^^^^^^^^^

`FERC Form 714 <https://www.ferc.gov/docs-filing/forms/form-714/data.asp>`_ includes
hourly loads, reported by load balancing authorities annually. This is a modestly sized
dataset, in the 100s of MB, distributed as Microsoft Excel spreadsheets.

.. _data-ferceqr:

FERC EQR
^^^^^^^^^

The `FERC EQR <https://www.ferc.gov/docs-filing/eqr/q2-2013/data/database.asp>`_ Also
known as the Electricity Quarterly Report or Form 920, this dataset includes the details
of many transactions between different utilities, and between utilities and merchant
generators. It covers ancillary services as well as energy and capacity, time and
location of delivery, prices, contract length, etc. It's one of the few public sources
of information about renewable energy power purchase agreements (PPAs). This is a large
(~100s of GB) dataset, composed of a very large number of relatively clean CSV files,
but it requires fuzzy processing to get at some of the interesting and only indirectly
reported attributes.

MSHA Mines and Production
^^^^^^^^^^^^^^^^^^^^^^^^^

The `MSHA Mines & Production <https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp>`_
dataset describes coal production by mine and operating company, along with statistics
about labor productivity and safety. This is a smaller dataset (100s of MB) available as
relatively clean and well structured CSV files.

PHMSA Natural Gas Pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `PHMSA Natural Gas Pipelines
<https://cms.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids>`_
dataset, published by the Pipeline and Hazardous Materials Safety Administration (which
is part of the US Dept. of Transportation) collects data about the natural gas
transmission and distribution system, including their age, length, diameter, materials,
and carrying capacity.

Transmission and Distribution Systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to run electricity system operations models and cost optimizations, you need
some kind of model of the interconnections between generation and loads. There doesn't
appear to be a generally accepted, publicly available set of these network descriptions
(yet!).
