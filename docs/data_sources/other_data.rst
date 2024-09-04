.. _other_data:

===============================================================================
Other Data in PUDL
===============================================================================

This page describes minor datasets that are included in PUDL, datasets from which we've
only integrated a small portion of the available data, or datasets that are included
with little to no processing, and thus don't yet have their own dedicated page under
:doc:`index`. Or just data sources for which we haven't yet compiled a complete
description.

.. _data-censusdp1tract:

Census DP1
^^^^^^^^^^

The `US Census Demographic Profile 1 (DP1) <https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html>`__
provides Census tract, county, and state-level demographic information, along with the
geometries defining those areas. We use this information in generating historical
utility and balancing authority service territories based on FERC 714 and EIA 861 data.
Currently, we are distributing the Census DP1 data as a standalone SQLite DB which is
converted directly from the original geodatabase distributed by the US Census Bureau.

.. _data-epacamd_eia:

EPA CAMD to EIA Power Sector Data Crosswalk
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This crosswalk is periodically updated through a collaboration between the US EPA and
the EIA, and connects EPA CAMD emissions units (smokestacks) which appear in
:doc:`epacems` with corresponding EIA plant components reported in EIA Forms 860
and 923 (``plant_id_eia``, ``boiler_id``, ``generator_id``).  This one-to-many
connection is necessary because pollutants from various plant parts are collecitvely
emitted and measured from one point-source. We augment the crosswalk that they publish
on GitHub.

The crosswalk is distributed by EPA `via GitHub <https://github.com/USEPA/camd-eia-crosswalk>`__.

.. _data-eiaaeo:

EIA Annual Energy Outlook (AEO)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The `EIA's Annual Energy Outlook <https://www.eia.gov/outlooks/aeo/>`__ underwent a
major overhaul in 2024, but we've integrated a few key tables from the earlier data.
These are just a small subset of the dozens of tables that have historically been part
of the AEO. Look for ``eiaaeo`` in the table name to find this data.

.. _data-nrelatb:

NREL Annual Technology Baseline (ATB)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

NREL publishes `Annual Technology Baseline (ATB) <https://atb.nrel.gov>`__ data for the
`Electricity <https://atb.nrel.gov/electricity>`__ and
`Transportation <https://atb.nrel.gov/transportation>`__ sectors. We have integrated the
Electricity sector data into the PUDL DB, but haven't yet fully documented the data
source. Look for ``nrelatb`` in the table name.

FERC DBF & XBRL Data
^^^^^^^^^^^^^^^^^^^^
FERC publishes Forms 1, 2, 6, and 60 data as VisualFoxPro DBF files (2020 and earlier)
and XBRL documents (2021 and later). We distribute these data as standalone SQLite
database files which contain all the data from the original FERC filings, but converted
to a more easily accessible format. Only a few dozen of the highest priority FERC Form 1
tables have been integrated into the main PUDL database. See the :doc:`../data_access`
page for detailed instructions.

.. _data-ferc2:

FERC Form 2
-----------

`FERC Form 2 <https://www.ferc.gov/industries-data/natural-gas/overview/general-information/natural-gas-industry-forms/form-22a-data>`__
is analogous to FERC Form 1, but reports on the finances of gas, rather than electric
utilities. Unfortunately because FERC's jurisdiction over gas utilities is more limited
than for electricity, Form 2 mostly describes interstate gas transmission pipeline
companies, and not local gas distribution utilities.

.. _data-ferc6:

FERC Form 6
-----------

`FERC Form 6 <https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-66-q-overview-orders>`__
(Annual Report of Oil Pipeline Companies) is a comprehensive financial and operating
report submitted for oil pipelines rate regulation and financial audits.

.. _data-ferc60:

FERC Form 60
------------

`FERC Form 60 <https://www.ferc.gov/ferc-online/ferc-online/filing-forms/service-companies-filing-forms/form-60-annual-report>`__
(Annual Report of Centralized Service Companies) is a comprehensive financial and
operating report submitted for centralized service companies. These are utility
subsidaries that provide services to more than one type of utility (electric, gas, or
oil pipeline) such that they don't fit into any of the above Forms 1, 2, or 6.
