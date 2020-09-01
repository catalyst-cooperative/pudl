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


Advanced Metering Infrastructure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Advanced Metering Infrastructure**

===================================== ========== ==============================
**Columns**                           **Dtype**  Contents/diff from publ output
------------------------------------- ---------- ------------------------------
``utility_id_eia``                    int
------------------------------------- ---------- ------------------------------
``state``                             str
------------------------------------- ---------- ------------------------------
``balancing_authority_code_eia``      cat
------------------------------------- ---------- ------------------------------
``report_date``                       datetime
------------------------------------- ---------- ------------------------------
``short_form``                        bool
------------------------------------- ---------- ------------------------------
``utility_name_eia``                  str
------------------------------------- ---------- ------------------------------
``customer_class``                    cat
------------------------------------- ---------- ------------------------------
``advanced_metering_infrastructure``
------------------------------------- ---------- ------------------------------
``automated_meter_reading``
------------------------------------- ---------- ------------------------------
``daily_digital_access_customers``
------------------------------------- ---------- ------------------------------
``direct_load_control_customers``
------------------------------------- ---------- ------------------------------
``energy_served_ami_mwh``
------------------------------------- ---------- ------------------------------
``home_area_network``
------------------------------------- ---------- ------------------------------
``non_amr_ami``
------------------------------------- ---------- ------------------------------
``total_meters``
===================================== ========== ==============================


Balancing Authority
^^^^^^^^^^^^^^^^^^^


Demand Response
^^^^^^^^^^^^^^^


Demand Side Management
^^^^^^^^^^^^^^^^^^^^^^


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
