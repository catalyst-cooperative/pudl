.. _wip_future:

===============================================================================
Work in Progress & Future Datasets
===============================================================================

-------------------------------------------------------------------------------
Work in Progress
-------------------------------------------------------------------------------

Thanks to a grant from the `Alfred P. Sloan Foundation Energy & Environment Program
<https://sloan.org/programs/research/energy-and-environment>`__, we have support to
integrate the following new datasets between April 2021 and March 2024.

There's a huge variety and quantity of data about the US electric utility system
available to the public. The data we have integrated is just the beginning! Other data
we've heard demand for are listed below. If you're interested in using one of them and
would like to add it to PUDL check out :doc:`our contribution guidelines
<../CONTRIBUTING>`. If there are other datasets you think we should be looking at
integration, don't hesitate to `open an issue on Github
<https://github.com/catalyst-cooperative/pudl/issues>`__ requesting the data and
explaining why it would be useful.

.. _data-censusdp1tract:

Census DP1
^^^^^^^^^^
The `US Census Demographic Profile 1 (DP1) <https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html>`__
provides Census tract, county, and state-level demographic information, along with the
geometries defining those areas. We use this information in generating historical
utility and balancing authority service territories based on FERC 714 and EIA 861 data.
Currently, we are distributing the Census DP1 data as a standalone SQLite DB.

.. _data-eia176:

EIA Form 176
^^^^^^^^^^^^

EIA `Form 176 <https://www.eia.gov/dnav/ng/TblDefs/NG_DataSources.html#s176>`__, also
known as the **Annual Report of Natural and Supplemental Gas Supply and Disposition**,
describes the origins, suppliers, and disposition of natural gas on a yearly and state
by state basis.

.. _data-ferceqr:

FERC EQR
^^^^^^^^

The `FERC Electric Quarterly Reports (EQR) <https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr>`__,
also known as FERC Form 920, includes the details of transactions
between different utilities and transactions between utilities and merchant generators.
It covers ancillary services as well as energy and capacity, time and location of
delivery, prices, contract length, etc. It's one of the few public sources of
information about renewable energy power purchase agreements (PPAs). This is a large
(~100s of GB) dataset composed of a very large number of relatively clean CSV files,
but it requires fuzzy processing to get at some of the interesting and only indirectly
reported attributes.

.. _data-ferc2:

FERC Form 2
^^^^^^^^^^^

`FERC Form 2 <https://www.ferc.gov/industries-data/natural-gas/overview/general-information/natural-gas-industry-forms/form-22a-data>`__
is analogous to FERC Form 1, but it pertains to gas rather than electric utilities.
The data paint a detailed picture of the finances of natural gas utilities.

.. _data-phmsa:

PHMSA Natural Gas Pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `PHMSA Natural Gas Annual Report <https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids>`__,
published by the Pipeline and Hazardous Materials Safety Administration (part of the US
Dept. of Transportation), collects data about natural gas
gathering and transmission and distribution systems (including their age, length,
diameter, materials, and carrying capacity). PHAMSA also has information about natural
gas storage facilities and liquefied natural gas shipping facilities.

.. _data-ces:

Machine Readable Clean Energy Standards
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`Renewable Portfolio Standards (RPS) <https://www.ncsl.org/research/energy/renewable-portfolio-standards.aspx>`__
and Clean Energy Standards (CES) have emerged as one of the primary policy tools to
decarbonize the US electricity supply. Researchers who model future electricity systems
need to include these binding regulations as constraints on their models to ensure that
the systems they explore are legally compliant. Unfortunately for modelers, RPS and CES
regulations vary from state to state. Sometimes there are carve outs for different types
of generation, and sometimes there are different requirements for different types of
utilities or distributed resources. Our goal is to compile a programmatically usable
database of RPS/CES policies in the US for quick and easy reference by modelers.

-------------------------------------------------------------------------------
Future Data of Interest
-------------------------------------------------------------------------------

.. _data-tds:

Transmission and Distribution Systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to run electricity system operations models and cost optimizations, you need
some kind of model of the interconnections between generation and loads. There doesn't
appear to be a generally accepted, publicly available set of these network descriptions
(yet!).

.. _data-eiah20:

EIA Water Usage
^^^^^^^^^^^^^^^

`EIA Water <https://www.eia.gov/electricity/data/water/>`__ records water use by thermal
generating stations in the US.

.. _data-msha:

MSHA Mines and Production
^^^^^^^^^^^^^^^^^^^^^^^^^

The `MSHA Mines & Production <https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp>`__
dataset describes coal production by mine and operating company along with statistics
about labor productivity and safety. This is a smaller dataset (100s of MB) available as
relatively clean and well structured CSV files.
