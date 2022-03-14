===============================================================================
EPA CEMS Hourly
===============================================================================

.. list-table::
   :widths: auto
   :header-rows: 0
   :stub-columns: 1

   * - Source URL
     - ftp://newftp.epa.gov/dmdnload/emissions/hourly/monthly
   * - Source Description
     - Hourly CO2, SO2, NOx emissions and gross load
   * - Respondents
     - Coal and high-sulfur fueled plants
   * - Source Format
     - Comma Separated Value (.csv)
   * - Source Years
     - 1995-2020
   * - Size (Download)
     - 8.1 GB
   * - PUDL Code
     - ``epacems``
   * - Years Liberated
     - 1995-2020
   * - Records Liberated
     - ~800 million
   * - Issues
     - `Open EPA CEMS issues <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aepacems>`__


Background
^^^^^^^^^^

As depicted by the EPA, `Continuous Emissions Monitoring Systems (CEMS)
<https://www.epa.gov/emc/emc-continuous-emission-monitoring-systems>`__ are the
“total equipment necessary for the determination of a gas or particulate matter
concentration or emission rate.” They are used to determine compliance with EPA
emissions standards and are therefore associated with a given “smokestack” and are
categorized in the raw data by a corresponding ``unitid``. Because point sources of
pollution are not alway correlated on a one-to-one basis with generation units, the
CEMS ``unitid`` serves as its own unique grouping. The EPA in collaboration with the
EIA has developed `a crosswalk table <https://github.com/USEPA/camd-eia-crosswalk>`__
that maps the EPA’s ``unitid`` onto EIA’s ``boiler_id``, ``generator_id``, and
``plant_id_eia``. This file has been integrated into the SQL database.

The EPA `Clean Air Markets Division (CAMD) <https://www.epa.gov/airmarkets>`__ has
collected emissions data from CEMS units stretching back to 1995. Among the data
included in CEMS are hourly SO2, CO2, NOx emission and gross load.

Who is required to install CEMS and report to EPA?
--------------------------------------------------

`Part 75 <https://www.ecfr.gov/cgi-bin/retrieveECFR?gp=&SID=d20546b42dd4ea978d0de7eabe15cbf4&mc=true&n=pt40.18.75&r=PART&ty=HTML#se40.18.75_12>`__
of the Federal Code of Regulations (FRC), the backbone of the Clean Air Act Title IV and
Acid Rain Program, requires coal and other solid-combusting units (see §72.2) to install
and use CEMS (see §75.2, §72.6). Certain low-sulfur fueled gas and oil units (see §72.2)
may seek exemption or alternative means of monitoring their emissions if desired (see
§§75.23, §§75.48, §§75.66). Once CEMS are installed, Part 75 requires hourly data
recording, including during startup, shutdown, and instances of malfunction as well as
quarterly data reporting to the EPA. The regulation further details the protocol for
missing data calculations and backup monitoring for instances of CEMS failure (see
§§75,31-37).

A plain English explanation of the requirements of Part 75 is available in section
`2.0 Overview of Part 75 Monitoring Requirements <https://www.epa.gov/sites/production/files/2015-05/documents/plain_english_guide_to_the_part_75_rule.pdf>`__

What does the original data look like?
--------------------------------------

EPA CAMD publishes the CEMS data in an online `data portal <https://ampd.epa.gov/ampd/>`__
. The files are available in a prepackaged format, accessible via a `user interface <https://ampd.epa.gov/ampd/>`__
or `FTP site <ftp://newftp.epa.gov/DMDnLoad>`__ with each downloadable zip file
encompassing a year of data.

How much of the data is accessible through PUDL?
------------------------------------------------

All of it!

Notable Irregularities
^^^^^^^^^^^^^^^^^^^^^^
CEMS is by far the largest dataset in PUDL at the moment with hourly records for
thousands of plants spanning decades. Note that the ETL process can easily take all
day for the full dataset. PUDL also provides a script that converts the raw EPA CEMS
data into Apache Parquet files that can be read and queried very efficiently with
Dask. Check out the `EPA CEMS example notebook <https://github.com/catalyst-cooperative/pudl-examples/blob/main/notebooks/03-pudl-parquet.ipynb>`__
in our
`pudl-examples repository <https://github.com/catalyst-cooperative/pudl-examples>`__
on GitHub for pointers on how to access this big dataset efficiently using :mod:`dask`.

PUDL Data Tables
^^^^^^^^^^^^^^^^

Clicking on the links will show you a description of the table as well as the names and
descriptions of each of its fields.

.. list-table::
   :header-rows: 1
   :widths: auto

   * - Data Dictionary
     - Browse Online
   * - :ref:`hourly_emissions_epacems`
     - Not Available via Datasette


PUDL Data Transformations
^^^^^^^^^^^^^^^^^^^^^^^^^

The PUDL transformation process cleans the input data so that it is adjusted for
uniformity, corrected for errors, and ready for bulk programmatic use.

To see the transformations applied to the data in each table, you can read the
documentation for :mod:`pudl.transform.epacems` created for their respective
transform functions.

Thanks to `Karl Dunkle Werner <https://github.com/karldw>`__ for contributing
much of the EPA CEMS Hourly ETL code!
