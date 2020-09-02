===============================================================================
Datasets
===============================================================================

------------------------------------------------------------------------------
EIA 861
------------------------------------------------------------------------------

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

Though it captures relatively consistent information, the EIA 861 Form has
changed over the years. As a result, output tables published by EIA possess
formatting and column attributes unique to a specific iteration of the form.
This makes multi-year comparisons a bit tricky. To enable greater analytical
capability, we combine data from all available reporting years into tables
resembling those seen in the most recent EIA 861 zip files, however, because
mapping table columns across years doesn't always match up, some of our output
tables have columns with consistent data up until a particular date, after
which the values appear as Null.

Below is a description of each of the tables, subtables, their columns, any
notable differences between reporting years, and how we've altered the values
from the original EIA outputs.

Columns with an asterisk are index columns.


Advanced Metering Infrastructure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Advanced Metering Infrastructure**

+------------------------------------+----------+-----------------------------+
|**Columns**                         |**Dtype** |**Contents / Difference from |
|                                    |          |EIA output files**           |
+====================================+==========+=============================+
|\* ``utility_id_eia``               |int       |Unique EIA utility code      |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``state``                        |str       |State where a utility        |
|                                    |          |operates. Utilities may have |
|                                    |          |multiple entries if they     |
|                                    |          |opperate in multiple states. |
|                                    |          |As seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``balancing_authority_code_eia`` |category  |Acronym for a utility's      |
|                                    |          |balancing authority as seen  |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|\* ``report_date``                  |datetime  |Date of form completion.     |
|                                    |          |Converted from year to Jan   |
|                                    |          |1st date.                    |
+------------------------------------+----------+-----------------------------+
|\* ``customer_class``               |category  |Customer breakdown           |
|                                    |          |(residential, commercial,    |
|                                    |          |etc.) extracted for          |
|                                    |          |wide-to-tall format.         |
+------------------------------------+----------+-----------------------------+
|``short_form``                      |bool      |Whether the utility also     |
|                                    |          |completed the short form.    |
|                                    |          |Adapted from Y/N as seen in  |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|``utility_name_eia``                |str       |Full name of the utility as  |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``advanced_metering_infrastructure``|int       |Number of AMI meters as seen |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|``automated_meter_reading``         |int       |Number of AMR meters as seen |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|``daily_digital_access_customers``  |int       |Number of Customers able to  |
|                                    |          |access daily energy usage    |
|                                    |          |through a webportal or other |
|                                    |          |electronic means. As seen in |
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``direct_load_control_customers``   |int       |Number of customers with     |
|                                    |          |direct load control. As seen |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|``energy_served_ami_mwh``           |float     |Energy served through AMI as |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``home_area_network``               |int       |Number of AMI Meters with    |
|                                    |          |home area network (HAN)      |
|                                    |          |gateway enabled. As seen in  |
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``non_amr_ami``                     |int       |Number of non AMR / AMI      |
|                                    |          |meters. As seen in EIA output|
|                                    |          |files.                       |
+------------------------------------+----------+-----------------------------+
|``total_meters``                    |int       |Total number of meters of all|
|                                    |          |types. As seen in EIA output |
|                                    |          |files.                       |
+------------------------------------+----------+-----------------------------+


Balancing Authority
^^^^^^^^^^^^^^^^^^^


Demand Response
^^^^^^^^^^^^^^^

The demand response table originated in 2013 as a two-pronged evolution of the
Demand Side Management table. Despite containing similar columns, EIA has
deemed the DSM and DR tables seperate entites that can't be compared. As a
result, we've kept the DR, DSM, and EE tables separate rather than try and map
post-2013 Demand Response and Energy Efficiency tables onto pre-2013 Demand
Side Management table. All of the values in the following tables begin in 2013.

**Demand Response**

+------------------------------------+----------+-----------------------------+
|**Columns**                         |**Dtype** |**Contents / Difference from |
|                                    |          |EIA output files**           |
+====================================+==========+=============================+
|\* ``utility_id_eia``               |int       |Unique EIA utility code      |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``state``                        |str       |State where a utility        |
|                                    |          |operates. Utilities may have |
|                                    |          |multiple entries if they     |
|                                    |          |opperate in multiple states. |
|                                    |          |As seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``balancing_authority_code_eia`` |category  |Acronym for a utility's      |
|                                    |          |balancing authority as seen  |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|\* ``report_date``                  |datetime  |Date of form completion.     |
|                                    |          |Converted from year to Jan   |
|                                    |          |1st date.                    |
+------------------------------------+----------+-----------------------------+
|\* ``customer_class``               |category  |Customer breakdown           |
|                                    |          |(residential, commercial,    |
|                                    |          |etc.) extracted for          |
|                                    |          |wide-to-tall format.         |
+------------------------------------+----------+-----------------------------+
|``actual_peak_demand_savings_mw``   |float     |Demand reduction actually    |
|                                    |          |achieved by demand response  |
|                                    |          |activities. As seen in EIA   |
|                                    |          |output files.                |
+------------------------------------+----------+-----------------------------+
|``customer_incentives_cost``        |float     |Total financial value        |
|                                    |          |provided to a customer for   |
|                                    |          |program participation.       |
|                                    |          |Reported in $1000, converted |
|                                    |          |to $1.                       |
+------------------------------------+----------+-----------------------------+
|``customers``                       |float     |Number of customers enrolled |
|                                    |          |in demand response programs. |
|                                    |          |As seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|``energy_savings_mwh``              |float     |Total energy savings as seen |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|``other_costs``                     |float     |All other costs to run the   |
|                                    |          |demand Response program.     |
|                                    |          |Reported in $1000, converted |
|                                    |          |to $1.                       |
+------------------------------------+----------+-----------------------------+
|``potential_peak_demand_savings_mw``|float     |Total demand savings that    |
|                                    |          |could occur at the time of   |
|                                    |          |the system peak hour assuming|
|                                    |          |all demand response is called|
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+


**Demand Response Water Heater**

+------------------------------------+----------+-----------------------------+
|**Columns**                         |**Dtype** |**Contents / Difference from |
|                                    |          |EIA output files**           |
+====================================+==========+=============================+
|\* ``utility_id_eia``               |int       |Unique EIA utility code      |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``state``                        |str       |State where a utility        |
|                                    |          |operates. Utilities may have |
|                                    |          |multiple entries if they     |
|                                    |          |opperate in multiple states. |
|                                    |          |As seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``balancing_authority_code_eia`` |category  |Acronym for a utility's      |
|                                    |          |balancing authority as seen  |
|                                    |          |in EIA output files.         |
+------------------------------------+----------+-----------------------------+
|\* ``report_date``                  |datetime  |Date of form completion.     |
|                                    |          |Converted from year to Jan   |
|                                    |          |1st date.                    |
+------------------------------------+----------+-----------------------------+
|``water_heater``                    |int       |Number of grid-enabled water |
|                                    |          |heaters added in a given     |
|                                    |          |report year. As seen in EIA  |
|                                    |          |output files.                |
+------------------------------------+----------+-----------------------------+


Demand Side Management
^^^^^^^^^^^^^^^^^^^^^^

The Demand Side Management table exists pre-2013 and combines load management
(demand response) and energy efficiency data. See Demand Response table for
further description. We've broken the EIA output table into multiple tables
so as to...

**Demand Side Management EE DR**

+------------------------------------+----------+-----------------------------+
|**Columns**                         |**Dtype** |**Contents / Difference from |
|                                    |          |EIA output files**           |
+====================================+==========+=============================+
|\* ``utility_id_eia``               |int       |Unique EIA utility code      |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``state``                        |str       |State where a utility        |
|                                    |          |operates. Utilities may have |
|                                    |          |multiple entries if they     |
|                                    |          |opperate in multiple states. |
|                                    |          |As seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|\* ``nerc_region``                  |category  |Acronym for physical NERC    |
|                                    |          |region in which a utility    |
|                                    |          |operates, not just transacts.|
|                                    |          |Cleaned and corrected.       |
+------------------------------------+----------+-----------------------------+
|\* ``report_date``                  |datetime  |Date of form completion.     |
|                                    |          |Converted from year to Jan   |
|                                    |          |1st date.                    |
+------------------------------------+----------+-----------------------------+
|\* ``customer_class``               |category  |Customer breakdown           |
|                                    |          |(residential, commercial,    |
|                                    |          |etc.) extracted for          |
|                                    |          |wide-to-tall format.         |
+------------------------------------+----------+-----------------------------+



Distributed Generation
^^^^^^^^^^^^^^^^^^^^^^

Distribution Systems
^^^^^^^^^^^^^^^^^^^^

Dynamic Pricing
^^^^^^^^^^^^^^^

Energy Efficiency
^^^^^^^^^^^^^^^^^

Green Pricing
^^^^^^^^^^^^^

Mergers
^^^^^^^

Net Metering
^^^^^^^^^^^^

Non Net Metering
^^^^^^^^^^^^^^^^

Operational Data
^^^^^^^^^^^^^^^^

Reliability
^^^^^^^^^^^

Sales
^^^^^

Service Territory
^^^^^^^^^^^^^^^^^

Utility Data
^^^^^^^^^^^^
