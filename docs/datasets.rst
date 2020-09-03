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

**Demand Side Management EE DR** -- check whether incentive payment in 1000s

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
|``energy_efficiency_annual_cost``   |float     |Costs directly attributable  |
|                                    |          |to EE programs in the given  |
|                                    |          |report year. Reported as     |
|                                    |          |$1000, converted to $1.      |
+------------------------------------+----------+-----------------------------+
|``energy_efficiency_annual_actual_  |float     |Peak reductions resulting    |
|peak_reduction_mw``                 |          |from all energy efficiency   |
|                                    |          |programs operating in the    |
|                                    |          |given report year. As seen in|
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``energy_efficiency_annual_         |float     |Energy saved as a result of  |
|effects_mwh``                       |          |all energy efficiency        |
|                                    |          |programs operating in the    |
|                                    |          |given report year. As seen in|
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``energy_efficiency_annual_         |float     |Total financial value        |
|incentive_payment``                 |          |provided to a customer for EE|
|                                    |          |program participation. As    |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``energy_efficiency_incremental_    |float     |Peak reductions resulting    |
|actual_peak_reduction_mw``          |          |from new EE participants and |
|                                    |          |new EE programs. As seen in  |
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``energy_efficiency_incremental_    |float     |Energy saved as a result of  |
|effects_mwh``                       |          |new EE participants and new  |
|                                    |          |EE programs in the given     |
|                                    |          |report year. As seen in EIA  |
|                                    |          |output files.                |
+------------------------------------+----------+-----------------------------+
|``load_management_annual_cost``     |float     |Costs directly attributable  |
|                                    |          |to DR programs in the given  |
|                                    |          |report year. Reported as     |
|                                    |          |$1000, converted to $1.      |
+------------------------------------+----------+-----------------------------+
|``load_management_annual_actual_    |float     |Peak reductions resulting    |
|peak_reduction_mw``                 |          |from all demand response     |
|                                    |          |programs operating in the    |
|                                    |          |given report year. As seen in|
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``load_management_annual_effects_   |float     |Energy saved as a result of  |
|mwh``                               |          |all demand response          |
|                                    |          |programs operating in the    |
|                                    |          |given report year. As seen in|
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``load_management_annual_incentive  |float     |Total financial value        |
|_payment``                          |          |provided to a customer for DR|
|                                    |          |program participation. As    |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``load_management_annual_potential  |float     |                             |
|_peak_reduction_mw``                |          |                             |
+------------------------------------+----------+-----------------------------+
|``load_management_incremental_      |float     |Peak reductions resulting    |
|actual_peak_reduction_mw``          |          |from new DR participants and |
|                                    |          |new DR programs. As seen in  |
|                                    |          |EIA output files.            |
+------------------------------------+----------+-----------------------------+
|``load_management_incremental_      |float     |Energy saved as a result of  |
|effects_mwh``                       |          |new DR participants and new  |
|                                    |          |DR programs in the given     |
|                                    |          |report year. As seen in EIA  |
|                                    |          |output files.                |
+------------------------------------+----------+-----------------------------+
|``load_management_incremental_      |float     |                             |
|potential_peak_reduction_mw``       |          |                             |
+------------------------------------+----------+-----------------------------+
|``price_responsiveness_customers``  |int       |Number of customers          |
|                                    |          |participating in price       |
|                                    |          |responsiveness programs. As  |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``time_responsiveness_customers``   |int       |Number of customers          |
|                                    |          |participating in time        |
|                                    |          |responsiveness programs. As  |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+

**Demand Side Management Sales**

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
|``sales_for_resale_mwh``            |float     |Energy sold for resale       |
|                                    |          |purposes. As seen in EIA     |
|                                    |          |output forms.                |
+------------------------------------+----------+-----------------------------+
|``sales_to_ultimate_consumers_mwh`` |float     |Energy sold to the ultimate  |
|                                    |          |customer. As seen in EIA     |
|                                    |          |output files.                |
+------------------------------------+----------+-----------------------------+

**Demand Side Management Misc.** -- need to define ENTITY_TYPE better

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
|``energy_savings_estimates_         |bool      |Whether savings estimates    |
|independently_verified``            |          |were verified through        |
|                                    |          |independent evaluation.      |
|                                    |          |Adapted from Y/N as seen in  |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|``energy_savings_independently      |bool      |Whether savings were verified|
|_verified``                         |          |through independent          |
|                                    |          |evaluation. Adapted from Y/N |
|                                    |          |as seen in EIA output files  |
+------------------------------------+----------+-----------------------------+
|``major_program_changes``           |bool      |Whether there have been major|
|                                    |          |program changes that would   |
|                                    |          |affect year-to-year          |
|                                    |          |comparison. Adapted from Y/N |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|``price_responsive_programs``       |bool      |Whether the utility offers   |
|                                    |          |price responsiveness         |
|                                    |          |programs. Adapted from Y/N as|
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``short_form``                      |bool      |Whether the utility also     |
|                                    |          |completed the short form.    |
|                                    |          |Adapted from Y/N as seen in  |
|                                    |          |as seen in EIA output files. |
+------------------------------------+----------+-----------------------------+
|``time_responsive_programs``        |bool      |Whether the utility offers   |
|                                    |          |time responsiveness          |
|                                    |          |programs. Adapted from Y/N as|
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``entity_type``                     |category  |Entity that owns XX such as  |
|                                    |          |Municipal, Cooperative, etc. |
|                                    |          |From first letter (M, C etc.)|
|                                    |          |to full name.                |
+------------------------------------+----------+-----------------------------+
|``reported_as_another_company``     |str       |If applicable, the name of   |
|                                    |          |the company that also reports|
|                                    |          |the same DSM data.           |
+------------------------------------+----------+-----------------------------+
|``utility_name_eia``                |str       |Full name of the utility as  |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+


Distributed Generation
^^^^^^^^^^^^^^^^^^^^^^

Distribution Systems
^^^^^^^^^^^^^^^^^^^^

Dynamic Pricing
^^^^^^^^^^^^^^^

Energy Efficiency
^^^^^^^^^^^^^^^^^

**Energy Efficiency**

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
|``utility_name_eia``                |str       |Full name of the utility as  |
|                                    |          |seen in EIA output files.    |
+------------------------------------+----------+-----------------------------+
|``customer_incentives_incremental   |          |                             |
|_cost``                             |          |                             |
+------------------------------------+----------+-----------------------------+
|``customer_incentives_incremental_  |          |                             |
|life_cycle_cost``                   |          |                             |
+------------------------------------+----------+-----------------------------+
|``customer_other_costs_incremental_ |          |                             |
|life_cycle_cost``                   |          |                             |
+------------------------------------+----------+-----------------------------+
|``incremental_energy_savings_mwh``  |          |                             |
+------------------------------------+----------+-----------------------------+
|``incremental_life_cycle_energy_    |          |                             |
|savings_mwh``                       |          |                             |
+------------------------------------+----------+-----------------------------+
|``incremental_life_cycle_peak_      |          |                             |
|reduction_mwh``                     |          |                             |
+------------------------------------+----------+-----------------------------+
|``incremental_peak_reduction_mw``   |          |                             |
+------------------------------------+----------+-----------------------------+
|``other_costs_incremental_cost``    |          |                             |
+------------------------------------+----------+-----------------------------+
|``weighted_average_life_years``     |          |                             |
+------------------------------------+----------+-----------------------------+


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
