
===============================================================================
PUDL Data Dictionary
===============================================================================

The following data tables have been cleaned and transformed by our ETL process.


.. _assn_gen_eia_unit_epa:

-------------------------------------------------------------------------------
assn_gen_eia_unit_epa
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/assn_gen_eia_unit_epa>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - generator_id
    - string
    - Generator identification code. Often numeric, but sometimes includes letters. It&#39;s a string!
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - unit_id_epa
    - string
    - Smokestack unit monitored by EPA CEMS.

.. _assn_plant_id_eia_epa:

-------------------------------------------------------------------------------
assn_plant_id_eia_epa
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/assn_plant_id_eia_epa>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - plant_id_epa
    - integer
    - N/A

.. _boiler_fuel_eia923:

-------------------------------------------------------------------------------
boiler_fuel_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/boiler_fuel_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - ash_content_pct
    - number
    - Ash content percentage by weight to the nearest 0.1 percent.
  * - boiler_id
    - string
    - Boiler identification code. Alphanumeric.
  * - fuel_consumed_units
    - number
    - Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.
  * - fuel_mmbtu_per_unit
    - number
    - Heat content of the fuel in millions of Btus per physical unit.
  * - fuel_type_code
    - string
    - The fuel code reported to EIA. Two or three letter alphanumeric.
  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - report_date
    - date
    - Date reported.
  * - sulfur_content_pct
    - number
    - Sulfur content percentage by weight to the nearest 0.01 percent.

.. _boiler_generator_assn_eia860:

-------------------------------------------------------------------------------
boiler_generator_assn_eia860
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/boiler_generator_assn_eia860>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - bga_source
    - string
    - The source from where the unit_id_pudl is compiled. The unit_id_pudl comes directly from EIA 860, or string association (which looks at all the boilers and generators that are not associated with a unit and tries to find a matching string in the respective collection of boilers or generator), or from a unit connection (where the unit_id_eia is employed to find additional boiler generator connections).
  * - boiler_id
    - string
    - EIA-assigned boiler identification code.
  * - generator_id
    - string
    - EIA-assigned generator identification code.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - report_date
    - date
    - Date reported.
  * - unit_id_eia
    - string
    - EIA-assigned unit identification code.
  * - unit_id_pudl
    - integer
    - Dynamically assigned PUDL unit id. WARNING: This ID is not guaranteed to be static long term as the input data and algorithm may evolve over time.

.. _boilers_entity_eia:

-------------------------------------------------------------------------------
boilers_entity_eia
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/boilers_entity_eia>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - boiler_id
    - string
    - The EIA-assigned boiler identification code. Alphanumeric.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - prime_mover_code
    - string
    - Code for the type of prime mover (e.g. CT, CG)

.. _coalmine_eia923:

-------------------------------------------------------------------------------
coalmine_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/coalmine_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - county_id_fips
    - integer
    - County ID from the Federal Information Processing Standard Publication 6-4.
  * - mine_id_msha
    - integer
    - MSHA issued mine identifier.
  * - mine_id_pudl
    - integer
    - PUDL issued surrogate key.
  * - mine_name
    - string
    - Coal mine name.
  * - mine_type_code
    - string
    - Type of mine. P: Preparation plant, U: Underground, S: Surface, SU: Mostly Surface with some Underground, US: Mostly Underground with some Surface.
  * - state
    - string
    - Two letter US state abbreviations and three letter ISO-3166-1 country codes for international mines.

.. _energy_source_eia923:

-------------------------------------------------------------------------------
energy_source_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/energy_source_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - abbr
    - string
    - N/A
  * - source
    - string
    - N/A

.. _ferc_accounts:

-------------------------------------------------------------------------------
ferc_accounts
-------------------------------------------------------------------------------

Account numbers from the FERC Uniform System of Accounts for Electric Plant,
which is defined in Code of Federal Regulations (CFR) Title 18, Chapter I,
Subchapter C, Part 101. (See e.g.
https://www.law.cornell.edu/cfr/text/18/part-101).
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/ferc_accounts>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - description
    - string
    - Long description of the FERC Account.
  * - ferc_account_id
    - string
    - Account number, from FERC&#39;s Uniform System of Accounts for Electric Plant. Also includes higher level labeled categories.

.. _ferc_depreciation_lines:

-------------------------------------------------------------------------------
ferc_depreciation_lines
-------------------------------------------------------------------------------

PUDL assigned FERC Form 1 line identifiers and long descriptions from FERC
Form 1 page 219, Accumulated Provision for Depreciation of Electric Utility
Plant (Account 108).
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/ferc_depreciation_lines>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - description
    - string
    - Description of the FERC depreciation account, as listed on FERC Form 1, Page 219.
  * - line_id
    - string
    - A human readable string uniquely identifying the FERC depreciation account. Used in lieu of the actual line number, as those numbers are not guaranteed to be consistent from year to year.

.. _fuel_ferc1:

-------------------------------------------------------------------------------
fuel_ferc1
-------------------------------------------------------------------------------

Annual fuel cost and quanitiy for steam plants with a capacity of 25+ MW,
internal combustion and gas-turbine plants of 10+ MW, and all nuclear plants.
As reported on page 402 of FERC Form 1 and extracted from the f1_fuel table in
FERC&#39;s FoxPro Database.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/fuel_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - fuel_cost_per_mmbtu
    - number
    - Average cost of fuel consumed in the report year, in nominal USD per mmBTU of fuel heat content.
  * - fuel_cost_per_unit_burned
    - number
    - Average cost of fuel consumed in the report year, in nominal USD per reported fuel unit.
  * - fuel_cost_per_unit_delivered
    - number
    - Average cost of fuel delivered in the report year, in nominal USD per reported fuel unit.
  * - fuel_mmbtu_per_unit
    - number
    - Average heat content of fuel consumed in the report year, in mmBTU per reported fuel unit.
  * - fuel_qty_burned
    - number
    - Quantity of fuel consumed in the report year, in terms of the reported fuel units.
  * - fuel_type_code_pudl
    - string
    - PUDL assigned code indicating the general fuel type.
  * - fuel_unit
    - string
    - PUDL assigned code indicating reported fuel unit of measure.
  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _fuel_receipts_costs_eia923:

-------------------------------------------------------------------------------
fuel_receipts_costs_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/fuel_receipts_costs_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - ash_content_pct
    - number
    - Ash content percentage by weight to the nearest 0.1 percent.
  * - chlorine_content_ppm
    - number
    - N/A
  * - contract_expiration_date
    - date
    - Date contract expires.Format:  MMYY.
  * - contract_type_code
    - string
    - Purchase type under which receipts occurred in the reporting month. C: Contract, NC: New Contract, S: Spot Purchase, T: Tolling Agreement.
  * - energy_source_code
    - string
    - The fuel code associated with the fuel receipt. Two or three character alphanumeric.
  * - fuel_cost_per_mmbtu
    - number
    - All costs incurred in the purchase and delivery of the fuel to the plant in cents per million Btu(MMBtu) to the nearest 0.1 cent.
  * - fuel_group_code
    - string
    - Groups the energy sources into fuel groups that are located in the Electric Power Monthly:  Coal, Natural Gas, Petroleum, Petroleum Coke.
  * - fuel_group_code_simple
    - string
    - Simplified grouping of fuel_group_code, with Coal and Petroluem Coke as well as Natural Gas and Other Gas grouped together.
  * - fuel_qty_units
    - number
    - Quanity of fuel received in tons, barrel, or Mcf.
  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.
  * - heat_content_mmbtu_per_unit
    - number
    - Heat content of the fuel in millions of Btus per physical unit to the nearest 0.01 percent.
  * - id
    - integer
    - PUDL issued surrogate key.
  * - mercury_content_ppm
    - number
    - Mercury content in parts per million (ppm) to the nearest 0.001 ppm.
  * - mine_id_pudl
    - integer
    - PUDL mine identification number.
  * - moisture_content_pct
    - number
    - N/A
  * - natural_gas_delivery_contract_type_code
    - string
    - Contract type for natrual gas delivery service:
  * - natural_gas_transport_code
    - string
    - Contract type for natural gas transportation service.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - primary_transportation_mode_code
    - string
    - Transportation mode for the longest distance transported.
  * - report_date
    - date
    - Date reported.
  * - secondary_transportation_mode_code
    - string
    - Transportation mode for the second longest distance transported.
  * - sulfur_content_pct
    - number
    - Sulfur content percentage by weight to the nearest 0.01 percent.
  * - supplier_name
    - string
    - Company that sold the fuel to the plant or, in the case of Natural Gas, pipline owner.

.. _fuel_type_aer_eia923:

-------------------------------------------------------------------------------
fuel_type_aer_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/fuel_type_aer_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - abbr
    - string
    - N/A
  * - fuel_type
    - string
    - N/A

.. _fuel_type_eia923:

-------------------------------------------------------------------------------
fuel_type_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/fuel_type_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - abbr
    - string
    - N/A
  * - fuel_type
    - string
    - N/A

.. _generation_eia923:

-------------------------------------------------------------------------------
generation_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/generation_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - generator_id
    - string
    - Generator identification code. Often numeric, but sometimes includes letters. It&#39;s a string!
  * - net_generation_mwh
    - number
    - Net generation for specified period in megawatthours (MWh).
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - report_date
    - date
    - Date reported.

.. _generation_fuel_eia923:

-------------------------------------------------------------------------------
generation_fuel_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/generation_fuel_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - fuel_consumed_for_electricity_mmbtu
    - number
    - Total consumption of fuel to produce electricity, in physical units, year to date.
  * - fuel_consumed_for_electricity_units
    - number
    - Consumption for electric generation of the fuel type in physical units.
  * - fuel_consumed_mmbtu
    - number
    - Total consumption of fuel in physical units, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.
  * - fuel_consumed_units
    - number
    - Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.
  * - fuel_mmbtu_per_unit
    - number
    - Heat content of the fuel in millions of Btus per physical unit.
  * - fuel_type
    - string
    - The fuel code reported to EIA. Two or three letter alphanumeric.
  * - fuel_type_code_aer
    - string
    - A partial aggregation of the reported fuel type codes into larger categories used by EIA in, for example, the Annual Energy Review (AER).Two or three letter alphanumeric.
  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.
  * - net_generation_mwh
    - number
    - Net generation, year to date in megawatthours (MWh). This is total electrical output net of station service.  In the case of combined heat and power plants, this value is intended to include internal consumption of electricity for the purposes of a production process, as well as power put on the grid.
  * - nuclear_unit_id
    - integer
    - For nuclear plants only. This unit ID appears to correspond directly to the generator ID, as reported in the EIA-860. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level. Note that nuclear plants only report their fuel consumption and net generation in the generation_fuel_eia923 table and not elsewhere.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - prime_mover_code
    - string
    - Type of prime mover.
  * - report_date
    - date
    - Date reported.

.. _generators_eia860:

-------------------------------------------------------------------------------
generators_eia860
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/generators_eia860>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - capacity_mw
    - number
    - The highest value on the generator nameplate in megawatts rounded to the nearest tenth.
  * - carbon_capture
    - boolean
    - Indicates whether the generator uses carbon capture technology.
  * - cofire_fuels
    - boolean
    - Can the generator co-fire fuels?.
  * - current_planned_operating_date
    - date
    - The most recently updated effective date on which the generator is scheduled to start operation
  * - data_source
    - string
    - Source of EIA 860 data. Either Annual EIA 860 or the year-to-date updates from EIA 860M.
  * - deliver_power_transgrid
    - boolean
    - Indicate whether the generator can deliver power to the transmission grid.
  * - distributed_generation
    - boolean
    - Whether the generator is considered distributed generation
  * - energy_source_1_transport_1
    - string
    - Primary Mode of Transportaion for Energy Source 1
  * - energy_source_1_transport_2
    - string
    - Secondary Mode of Transportaion for Energy Source 1
  * - energy_source_1_transport_3
    - string
    - Third Mode of Transportaion for Energy Source 1
  * - energy_source_2_transport_1
    - string
    - Primary Mode of Transportaion for Energy Source 2
  * - energy_source_2_transport_2
    - string
    - Secondary Mode of Transportaion for Energy Source 2
  * - energy_source_2_transport_3
    - string
    - Third Mode of Transportaion for Energy Source 2
  * - energy_source_code_1
    - string
    - The code representing the most predominant type of energy that fuels the generator.
  * - energy_source_code_2
    - string
    - The code representing the second most predominant type of energy that fuels the generator
  * - energy_source_code_3
    - string
    - The code representing the third most predominant type of energy that fuels the generator
  * - energy_source_code_4
    - string
    - The code representing the fourth most predominant type of energy that fuels the generator
  * - energy_source_code_5
    - string
    - The code representing the fifth most predominant type of energy that fuels the generator
  * - energy_source_code_6
    - string
    - The code representing the sixth most predominant type of energy that fuels the generator
  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.
  * - generator_id
    - string
    - Generator identification number.
  * - minimum_load_mw
    - number
    - The minimum load at which the generator can operate at continuosuly.
  * - multiple_fuels
    - boolean
    - Can the generator burn multiple fuels?
  * - nameplate_power_factor
    - number
    - The nameplate power factor of the generator.
  * - operational_status
    - string
    - The operating status of the generator. This is based on which tab the generator was listed in in EIA 860.
  * - operational_status_code
    - string
    - The operating status of the generator.
  * - other_modifications_date
    - date
    - Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete.
  * - other_planned_modifications
    - boolean
    - Indicates whether there are there other modifications planned for the generator.
  * - owned_by_non_utility
    - boolean
    - Whether any part of generator is owned by a nonutilty
  * - ownership_code
    - string
    - Identifies the ownership for each generator.
  * - planned_derate_date
    - date
    - Planned effective month that the generator is scheduled to enter operation after the derate modification.
  * - planned_energy_source_code_1
    - string
    - New energy source code for the planned repowered generator.
  * - planned_modifications
    - boolean
    - Indicates whether there are any planned capacity uprates/derates, repowering, other modifications, or generator retirements scheduled for the next 5 years.
  * - planned_net_summer_capacity_derate_mw
    - number
    - Decrease in summer capacity expected to be realized from the derate modification to the equipment.
  * - planned_net_summer_capacity_uprate_mw
    - number
    - Increase in summer capacity expected to be realized from the modification to the equipment.
  * - planned_net_winter_capacity_derate_mw
    - number
    - Decrease in winter capacity expected to be realized from the derate modification to the equipment.
  * - planned_net_winter_capacity_uprate_mw
    - number
    - Increase in winter capacity expected to be realized from the uprate modification to the equipment.
  * - planned_new_capacity_mw
    - number
    - The expected new namplate capacity for the generator.
  * - planned_new_prime_mover_code
    - string
    - New prime mover for the planned repowered generator.
  * - planned_repower_date
    - date
    - Planned effective date that the generator is scheduled to enter operation after the repowering is complete.
  * - planned_retirement_date
    - date
    - Planned effective date of the scheduled retirement of the generator.
  * - planned_uprate_date
    - date
    - Planned effective date that the generator is scheduled to enter operation after the uprate modification.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - reactive_power_output_mvar
    - number
    - Reactive Power Output (MVAr)
  * - report_date
    - date
    - Date reported.
  * - retirement_date
    - date
    - Date of the scheduled or effected retirement of the generator.
  * - startup_source_code_1
    - string
    - The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.
  * - startup_source_code_2
    - string
    - The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.
  * - startup_source_code_3
    - string
    - The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.
  * - startup_source_code_4
    - string
    - The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.
  * - summer_capacity_estimate
    - boolean
    - Whether the summer capacity value was an estimate
  * - summer_capacity_mw
    - number
    - The net summer capacity.
  * - summer_estimated_capability_mw
    - number
    - EIA estimated summer capacity (in MWh).
  * - switch_oil_gas
    - boolean
    - Indicates whether the generator switch between oil and natural gas.
  * - syncronized_transmission_grid
    - boolean
    - Indicates whether standby generators (SB status) can be synchronized to the grid.
  * - technology_description
    - string
    - High level description of the technology used by the generator to produce electricity.
  * - time_cold_shutdown_full_load_code
    - string
    - The minimum amount of time required to bring the unit to full load from shutdown.
  * - turbines_inverters_hydrokinetics
    - string
    - Number of wind turbines, or hydrokinetic buoys.
  * - turbines_num
    - integer
    - Number of wind turbines, or hydrokinetic buoys.
  * - uprate_derate_completed_date
    - date
    - The date when the uprate or derate was completed.
  * - uprate_derate_during_year
    - boolean
    - Was an uprate or derate completed on this generator during the reporting year?
  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.
  * - winter_capacity_estimate
    - boolean
    - Whether the winter capacity value was an estimate
  * - winter_capacity_mw
    - number
    - The net winter capacity.
  * - winter_estimated_capability_mw
    - number
    - EIA estimated winter capacity (in MWh).

.. _generators_entity_eia:

-------------------------------------------------------------------------------
generators_entity_eia
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/generators_entity_eia>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - associated_combined_heat_power
    - boolean
    - Indicates whether the generator is associated with a combined heat and power system
  * - bypass_heat_recovery
    - boolean
    - Can this generator operate while bypassing the heat recovery steam generator?
  * - duct_burners
    - boolean
    - Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas
  * - fluidized_bed_tech
    - boolean
    - Indicates whether the generator uses fluidized bed technology
  * - generator_id
    - string
    - Generator identification number
  * - operating_date
    - date
    - Date the generator began commercial operation
  * - operating_switch
    - string
    - Indicates whether the fuel switching generator can switch when operating
  * - original_planned_operating_date
    - date
    - The date the generator was originally scheduled to be operational
  * - other_combustion_tech
    - boolean
    - Indicates whether the generator uses other combustion technologies
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - previously_canceled
    - boolean
    - Indicates whether the generator was previously reported as indefinitely postponed or canceled
  * - prime_mover_code
    - string
    - EIA assigned code for the prime mover (i.e. the engine, turbine, water wheel, or similar machine that drives an electric generator)
  * - pulverized_coal_tech
    - boolean
    - Indicates whether the generator uses pulverized coal technology
  * - rto_iso_lmp_node_id
    - string
    - The designation used to identify the price node in RTO/ISO Locational Marginal Price reports
  * - rto_iso_location_wholesale_reporting_id
    - string
    - The designation used to report ths specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report
  * - solid_fuel_gasification
    - boolean
    - Indicates whether the generator is part of a solid fuel gasification system
  * - stoker_tech
    - boolean
    - Indicates whether the generator uses stoker technology
  * - subcritical_tech
    - boolean
    - Indicates whether the generator uses subcritical technology
  * - supercritical_tech
    - boolean
    - Indicates whether the generator uses supercritical technology
  * - topping_bottoming_code
    - string
    - If the generator is associated with a combined heat and power system, indicates whether the generator is part of a topping cycle or a bottoming cycle
  * - ultrasupercritical_tech
    - boolean
    - Indicates whether the generator uses ultra-supercritical technology

.. _hourly_emissions_epacems:

-------------------------------------------------------------------------------
hourly_emissions_epacems
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/hourly_emissions_epacems>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - co2_mass_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.
  * - co2_mass_tons
    - number
    - Carbon dioxide emissions in short tons.
  * - facility_id
    - integer
    - New EPA plant ID.
  * - gross_load_mw
    - number
    - Average power in megawatts delivered during time interval measured.
  * - heat_content_mmbtu
    - number
    - The energy contained in fuel burned, measured in million BTU.
  * - nox_mass_lbs
    - number
    - NOx emissions in pounds.
  * - nox_mass_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.
  * - nox_rate_lbs_mmbtu
    - number
    - The average rate at which NOx was emitted during a given time period.
  * - nox_rate_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.
  * - operating_datetime_utc
    - datetime
    - Date and time measurement began (UTC).
  * - operating_time_hours
    - number
    - Length of time interval measured.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - so2_mass_lbs
    - number
    - Sulfur dioxide emissions in pounds.
  * - so2_mass_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.
  * - state
    - string
    - State the plant is located in.
  * - steam_load_1000_lbs
    - number
    - Total steam pressure produced by a unit during the reported hour.
  * - unit_id_epa
    - integer
    - Smokestack unit monitored by EPA CEMS.
  * - unitid
    - string
    - Facility-specific unit id (e.g. Unit 4)

.. _ownership_eia860:

-------------------------------------------------------------------------------
ownership_eia860
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/ownership_eia860>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - fraction_owned
    - number
    - Proportion of generator ownership.
  * - generator_id
    - string
    - Generator identification number.
  * - owner_city
    - string
    - City of owner.
  * - owner_name
    - string
    - Name of owner.
  * - owner_state
    - string
    - Two letter US &amp; Canadian state and territory abbreviations.
  * - owner_street_address
    - string
    - Steet address of owner.
  * - owner_utility_id_eia
    - integer
    - EIA-assigned owner&#39;s identification number.
  * - owner_zip_code
    - string
    - Zip code of owner.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - report_date
    - date
    - Date reported.
  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.

.. _plant_in_service_ferc1:

-------------------------------------------------------------------------------
plant_in_service_ferc1
-------------------------------------------------------------------------------

Balances and changes to FERC Electric Plant in Service accounts, as reported
on FERC Form 1. Data originally from the f1_plant_in_srvce table in FERC&#39;s
FoxPro database. Account numbers correspond to the FERC Uniform System of
Accounts for Electric Plant, which is defined in Code of Federal Regulations
(CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g.
https://www.law.cornell.edu/cfr/text/18/part-101). Each FERC respondent
reports starting and ending balances for each account annually. Balances are
organization wide, and are not broken down on a per-plant basis. End of year
balance should equal beginning year balance plus the sum of additions,
retirements, adjustments, and transfers.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plant_in_service_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - amount_type
    - string
    - String indicating which original FERC Form 1 column the listed amount came from. Each field should have one (potentially NA) value of each type for each utility in each year, and the ending_balance should equal the sum of starting_balance, additions, retirements, adjustments, and transfers.
  * - distribution_acct360_land
    - number
    - FERC Account 360: Distribution Plant Land and Land Rights.
  * - distribution_acct361_structures
    - number
    - FERC Account 361: Distribution Plant Structures and Improvements.
  * - distribution_acct362_station_equip
    - number
    - FERC Account 362: Distribution Plant Station Equipment.
  * - distribution_acct363_storage_battery_equip
    - number
    - FERC Account 363: Distribution Plant Storage Battery Equipment.
  * - distribution_acct364_poles_towers
    - number
    - FERC Account 364: Distribution Plant Poles, Towers, and Fixtures.
  * - distribution_acct365_overhead_conductors
    - number
    - FERC Account 365: Distribution Plant Overhead Conductors and Devices.
  * - distribution_acct366_underground_conduit
    - number
    - FERC Account 366: Distribution Plant Underground Conduit.
  * - distribution_acct367_underground_conductors
    - number
    - FERC Account 367: Distribution Plant Underground Conductors and Devices.
  * - distribution_acct368_line_transformers
    - number
    - FERC Account 368: Distribution Plant Line Transformers.
  * - distribution_acct369_services
    - number
    - FERC Account 369: Distribution Plant Services.
  * - distribution_acct370_meters
    - number
    - FERC Account 370: Distribution Plant Meters.
  * - distribution_acct371_customer_installations
    - number
    - FERC Account 371: Distribution Plant Installations on Customer Premises.
  * - distribution_acct372_leased_property
    - number
    - FERC Account 372: Distribution Plant Leased Property on Customer Premises.
  * - distribution_acct373_street_lighting
    - number
    - FERC Account 373: Distribution PLant Street Lighting and Signal Systems.
  * - distribution_acct374_asset_retirement
    - number
    - FERC Account 374: Distribution Plant Asset Retirement Costs.
  * - distribution_total
    - number
    - Distribution Plant Total (FERC Accounts 360-374).
  * - electric_plant_in_service_total
    - number
    - Total Electric Plant in Service (FERC Accounts 101, 102, 103 and 106)
  * - electric_plant_purchased_acct102
    - number
    - FERC Account 102: Electric Plant Purchased.
  * - electric_plant_sold_acct102
    - number
    - FERC Account 102: Electric Plant Sold (Negative).
  * - experimental_plant_acct103
    - number
    - FERC Account 103: Experimental Plant Unclassified.
  * - general_acct389_land
    - number
    - FERC Account 389: General Land and Land Rights.
  * - general_acct390_structures
    - number
    - FERC Account 390: General Structures and Improvements.
  * - general_acct391_office_equip
    - number
    - FERC Account 391: General Office Furniture and Equipment.
  * - general_acct392_transportation_equip
    - number
    - FERC Account 392: General Transportation Equipment.
  * - general_acct393_stores_equip
    - number
    - FERC Account 393: General Stores Equipment.
  * - general_acct394_shop_equip
    - number
    - FERC Account 394: General Tools, Shop, and Garage Equipment.
  * - general_acct395_lab_equip
    - number
    - FERC Account 395: General Laboratory Equipment.
  * - general_acct396_power_operated_equip
    - number
    - FERC Account 396: General Power Operated Equipment.
  * - general_acct397_communication_equip
    - number
    - FERC Account 397: General Communication Equipment.
  * - general_acct398_misc_equip
    - number
    - FERC Account 398: General Miscellaneous Equipment.
  * - general_acct399_1_asset_retirement
    - number
    - FERC Account 399.1: Asset Retirement Costs for General Plant.
  * - general_acct399_other_property
    - number
    - FERC Account 399: General Plant Other Tangible Property.
  * - general_subtotal
    - number
    - General Plant Subtotal (FERC Accounts 389-398).
  * - general_total
    - number
    - General Plant Total (FERC Accounts 389-399.1).
  * - hydro_acct330_land
    - number
    - FERC Account 330: Hydro Land and Land Rights.
  * - hydro_acct331_structures
    - number
    - FERC Account 331: Hydro Structures and Improvements.
  * - hydro_acct332_reservoirs_dams_waterways
    - number
    - FERC Account 332: Hydro Reservoirs, Dams, and Waterways.
  * - hydro_acct333_wheels_turbines_generators
    - number
    - FERC Account 333: Hydro Water Wheels, Turbins, and Generators.
  * - hydro_acct334_accessory_equip
    - number
    - FERC Account 334: Hydro Accessory Electric Equipment.
  * - hydro_acct335_misc_equip
    - number
    - FERC Account 335: Hydro Miscellaneous Power Plant Equipment.
  * - hydro_acct336_roads_railroads_bridges
    - number
    - FERC Account 336: Hydro Roads, Railroads, and Bridges.
  * - hydro_acct337_asset_retirement
    - number
    - FERC Account 337: Asset Retirement Costs for Hydraulic Production.
  * - hydro_total
    - number
    - Hydraulic Production Plant Total (FERC Accounts 330-337)
  * - intangible_acct301_organization
    - number
    - FERC Account 301: Intangible Plant Organization.
  * - intangible_acct302_franchises_consents
    - number
    - FERC Account 302: Intangible Plant Franchises and Consents.
  * - intangible_acct303_misc
    - number
    - FERC Account 303: Miscellaneous Intangible Plant.
  * - intangible_total
    - number
    - Intangible Plant Total (FERC Accounts 301-303).
  * - major_electric_plant_acct101_acct106_total
    - number
    - Total Major Electric Plant in Service (FERC Accounts 101 and 106).
  * - nuclear_acct320_land
    - number
    - FERC Account 320: Nuclear Land and Land Rights.
  * - nuclear_acct321_structures
    - number
    - FERC Account 321: Nuclear Structures and Improvements.
  * - nuclear_acct322_reactor_equip
    - number
    - FERC Account 322: Nuclear Reactor Plant Equipment.
  * - nuclear_acct323_turbogenerators
    - number
    - FERC Account 323: Nuclear Turbogenerator Units
  * - nuclear_acct324_accessory_equip
    - number
    - FERC Account 324: Nuclear Accessory Electric Equipment.
  * - nuclear_acct325_misc_equip
    - number
    - FERC Account 325: Nuclear Miscellaneous Power Plant Equipment.
  * - nuclear_acct326_asset_retirement
    - number
    - FERC Account 326: Asset Retirement Costs for Nuclear Production.
  * - nuclear_total
    - number
    - Total Nuclear Production Plant (FERC Accounts 320-326)
  * - other_acct340_land
    - number
    - FERC Account 340: Other Land and Land Rights.
  * - other_acct341_structures
    - number
    - FERC Account 341: Other Structures and Improvements.
  * - other_acct342_fuel_accessories
    - number
    - FERC Account 342: Other Fuel Holders, Products, and Accessories.
  * - other_acct343_prime_movers
    - number
    - FERC Account 343: Other Prime Movers.
  * - other_acct344_generators
    - number
    - FERC Account 344: Other Generators.
  * - other_acct345_accessory_equip
    - number
    - FERC Account 345: Other Accessory Electric Equipment.
  * - other_acct346_misc_equip
    - number
    - FERC Account 346: Other Miscellaneous Power Plant Equipment.
  * - other_acct347_asset_retirement
    - number
    - FERC Account 347: Asset Retirement Costs for Other Production.
  * - other_total
    - number
    - Total Other Production Plant (FERC Accounts 340-347).
  * - production_total
    - number
    - Total Production Plant (FERC Accounts 310-347).
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - rtmo_acct380_land
    - number
    - FERC Account 380: RTMO Land and Land Rights.
  * - rtmo_acct381_structures
    - number
    - FERC Account 381: RTMO Structures and Improvements.
  * - rtmo_acct382_computer_hardware
    - number
    - FERC Account 382: RTMO Computer Hardware.
  * - rtmo_acct383_computer_software
    - number
    - FERC Account 383: RTMO Computer Software.
  * - rtmo_acct384_communication_equip
    - number
    - FERC Account 384: RTMO Communication Equipment.
  * - rtmo_acct385_misc_equip
    - number
    - FERC Account 385: RTMO Miscellaneous Equipment.
  * - rtmo_total
    - number
    - Total RTMO Plant (FERC Accounts 380-386)
  * - steam_acct310_land
    - number
    - FERC Account 310: Steam Plant Land and Land Rights.
  * - steam_acct311_structures
    - number
    - FERC Account 311: Steam Plant Structures and Improvements.
  * - steam_acct312_boiler_equip
    - number
    - FERC Account 312: Steam Boiler Plant Equipment.
  * - steam_acct313_engines
    - number
    - FERC Account 313: Steam Engines and Engine-Driven Generators.
  * - steam_acct314_turbogenerators
    - number
    - FERC Account 314: Steam Turbogenerator Units.
  * - steam_acct315_accessory_equip
    - number
    - FERC Account 315: Steam Accessory Electric Equipment.
  * - steam_acct316_misc_equip
    - number
    - FERC Account 316: Steam Miscellaneous Power Plant Equipment.
  * - steam_acct317_asset_retirement
    - number
    - FERC Account 317: Asset Retirement Costs for Steam Production.
  * - steam_total
    - number
    - Total Steam Production Plant (FERC Accounts 310-317).
  * - transmission_acct350_land
    - number
    - FERC Account 350: Transmission Land and Land Rights.
  * - transmission_acct352_structures
    - number
    - FERC Account 352: Transmission Structures and Improvements.
  * - transmission_acct353_station_equip
    - number
    - FERC Account 353: Transmission Station Equipment.
  * - transmission_acct354_towers
    - number
    - FERC Account 354: Transmission Towers and Fixtures.
  * - transmission_acct355_poles
    - number
    - FERC Account 355: Transmission Poles and Fixtures.
  * - transmission_acct356_overhead_conductors
    - number
    - FERC Account 356: Overhead Transmission Conductors and Devices.
  * - transmission_acct357_underground_conduit
    - number
    - FERC Account 357: Underground Transmission Conduit.
  * - transmission_acct358_underground_conductors
    - number
    - FERC Account 358: Underground Transmission Conductors.
  * - transmission_acct359_1_asset_retirement
    - number
    - FERC Account 359.1: Asset Retirement Costs for Transmission Plant.
  * - transmission_acct359_roads_trails
    - number
    - FERC Account 359: Transmission Roads and Trails.
  * - transmission_total
    - number
    - Total Transmission Plant (FERC Accounts 350-359.1)
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _plant_unit_epa:

-------------------------------------------------------------------------------
plant_unit_epa
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plant_unit_epa>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - plant_id_epa
    - integer
    - N/A
  * - unit_id_epa
    - string
    - Smokestack unit monitored by EPA CEMS.

.. _plants_eia:

-------------------------------------------------------------------------------
plants_eia
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_eia>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - plant_id_pudl
    - integer
    - N/A
  * - plant_name_eia
    - string
    - N/A

.. _plants_eia860:

-------------------------------------------------------------------------------
plants_eia860
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_eia860>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - ash_impoundment
    - string
    - Is there an ash impoundment (e.g. pond, reservoir) at the plant?
  * - ash_impoundment_lined
    - string
    - If there is an ash impoundment at the plant, is the impoundment lined?
  * - ash_impoundment_status
    - string
    - If there is an ash impoundment at the plant, the ash impoundment status as of December 31 of the reporting year.
  * - datum
    - string
    - N/A
  * - energy_storage
    - string
    - Indicates if the facility has energy storage capabilities.
  * - ferc_cogen_docket_no
    - string
    - The docket number relating to the FERC qualifying facility cogenerator status.
  * - ferc_exempt_wholesale_generator_docket_no
    - string
    - The docket number relating to the FERC qualifying facility exempt wholesale generator status.
  * - ferc_small_power_producer_docket_no
    - string
    - The docket number relating to the FERC qualifying facility small power producer status.
  * - liquefied_natural_gas_storage
    - string
    - Indicates if the facility have the capability to store the natural gas in the form of liquefied natural gas.
  * - natural_gas_local_distribution_company
    - string
    - Names of Local Distribution Company (LDC), connected to natural gas burning power plants.
  * - natural_gas_pipeline_name_1
    - string
    - The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.
  * - natural_gas_pipeline_name_2
    - string
    - The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.
  * - natural_gas_pipeline_name_3
    - string
    - The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.
  * - natural_gas_storage
    - string
    - Indicates if the facility have on-site storage of natural gas.
  * - nerc_region
    - string
    - NERC region in which the plant is located
  * - net_metering
    - string
    - Did this plant have a net metering agreement in effect during the reporting year?  (Only displayed for facilities that report the sun or wind as an energy source). This field was only reported up until 2015
  * - pipeline_notes
    - string
    - Additional owner or operator of natural gas pipeline.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - regulatory_status_code
    - string
    - Indicates whether the plant is regulated or non-regulated.
  * - report_date
    - date
    - Date reported.
  * - transmission_distribution_owner_id
    - string
    - EIA-assigned code for owner of transmission/distribution system to which the plant is interconnected.
  * - transmission_distribution_owner_name
    - string
    - Name of the owner of the transmission or distribution system to which the plant is interconnected.
  * - transmission_distribution_owner_state
    - string
    - State location for owner of transmission/distribution system to which the plant is interconnected.
  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.
  * - water_source
    - string
    - Name of water source associater with the plant.

.. _plants_entity_eia:

-------------------------------------------------------------------------------
plants_entity_eia
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_entity_eia>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - balancing_authority_code_eia
    - string
    - The plant&#39;s balancing authority code.
  * - balancing_authority_name_eia
    - string
    - The plant&#39;s balancing authority name.
  * - city
    - string
    - The plant&#39;s city.
  * - county
    - string
    - The plant&#39;s county.
  * - ferc_cogen_status
    - string
    - Indicates whether the plant has FERC qualifying facility cogenerator status.
  * - ferc_exempt_wholesale_generator
    - string
    - Indicates whether the plant has FERC qualifying facility exempt wholesale generator status
  * - ferc_small_power_producer
    - string
    - Indicates whether the plant has FERC qualifying facility small power producer status
  * - grid_voltage_2_kv
    - number
    - Plant&#39;s grid voltage at point of interconnection to transmission or distibution facilities
  * - grid_voltage_3_kv
    - number
    - Plant&#39;s grid voltage at point of interconnection to transmission or distibution facilities
  * - grid_voltage_kv
    - number
    - Plant&#39;s grid voltage at point of interconnection to transmission or distibution facilities
  * - iso_rto_code
    - string
    - The code of the plant&#39;s ISO or RTO. NA if not reported in that year.
  * - latitude
    - number
    - Latitude of the plant&#39;s location, in degrees.
  * - longitude
    - number
    - Longitude of the plant&#39;s location, in degrees.
  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.
  * - plant_name_eia
    - string
    - Plant name.
  * - primary_purpose_naics_id
    - number
    - North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant
  * - sector_id
    - number
    - Plant-level sector number, designated by the primary purpose, regulatory status and plant-level combined heat and power status
  * - sector_name
    - string
    - Plant-level sector name, designated by the primary purpose, regulatory status and plant-level combined heat and power status
  * - service_area
    - string
    - Service area in which plant is located; for unregulated companies, it&#39;s the electric utility with which plant is interconnected
  * - state
    - string
    - Plant state. Two letter US state and territory abbreviations.
  * - street_address
    - string
    - Plant street address
  * - timezone
    - string
    - IANA timezone name
  * - zip_code
    - string
    - Plant street address

.. _plants_ferc1:

-------------------------------------------------------------------------------
plants_ferc1
-------------------------------------------------------------------------------

Name, utility, and PUDL id for steam plants with a capacity of 25,000+ kW,
internal combustion and gas-turbine plants of 10,000+ kW, and all nuclear
plants.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - plant_id_pudl
    - integer
    - A manually assigned PUDL plant ID. May not be constant over time.
  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _plants_hydro_ferc1:

-------------------------------------------------------------------------------
plants_hydro_ferc1
-------------------------------------------------------------------------------

Generating plant statistics for hydroelectric plants with an installed
nameplate capacity of 10 MW. As reported on FERC Form 1, pages 406-407 and
extracted from the f1_hydro table in FERC&#39;s FoxPro database.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_hydro_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - asset_retirement_cost
    - number
    - Cost of plant: asset retirement costs. Nominal USD.
  * - avg_num_employees
    - number
    - Average number of employees.
  * - capacity_mw
    - number
    - Total installed (nameplate) capacity, in megawatts.
  * - capex_equipment
    - number
    - Cost of plant: equipment. Nominal USD.
  * - capex_facilities
    - number
    - Cost of plant: reservoirs, dams, and waterways. Nominal USD.
  * - capex_land
    - number
    - Cost of plant: land and land rights. Nominal USD.
  * - capex_per_mw
    - number
    - Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD.
  * - capex_roads
    - number
    - Cost of plant: roads, railroads, and bridges. Nominal USD.
  * - capex_structures
    - number
    - Cost of plant: structures and improvements. Nominal USD.
  * - capex_total
    - number
    - Total cost of plant. Nominal USD.
  * - construction_type
    - string
    - Type of plant construction (&#39;outdoor&#39;, &#39;semioutdoor&#39;, or &#39;conventional&#39;). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.
  * - construction_year
    - year
    - Four digit year of the plant&#39;s original construction.
  * - installation_year
    - year
    - Four digit year in which the last unit was installed.
  * - net_capacity_adverse_conditions_mw
    - number
    - Net plant capability under the least favorable operating conditions, in megawatts.
  * - net_capacity_favorable_conditions_mw
    - number
    - Net plant capability under the most favorable operating conditions, in megawatts.
  * - net_generation_mwh
    - number
    - Net generation, exclusive of plant use, in megawatt hours.
  * - opex_dams
    - number
    - Production expenses: maintenance of reservoirs, dams, and waterways. Nominal USD.
  * - opex_electric
    - number
    - Production expenses: electric expenses. Nominal USD.
  * - opex_engineering
    - number
    - Production expenses: maintenance, supervision, and engineering. Nominal USD.
  * - opex_generation_misc
    - number
    - Production expenses: miscellaneous hydraulic power generation expenses. Nominal USD.
  * - opex_hydraulic
    - number
    - Production expenses: hydraulic expenses. Nominal USD.
  * - opex_misc_plant
    - number
    - Production expenses: maintenance of miscellaneous hydraulic plant. Nominal USD.
  * - opex_operations
    - number
    - Production expenses: operation, supervision, and engineering. Nominal USD.
  * - opex_per_mwh
    - number
    - Production expenses per net megawatt hour generated. Nominal USD.
  * - opex_plant
    - number
    - Production expenses: maintenance of electric plant. Nominal USD.
  * - opex_rents
    - number
    - Production expenses: rent. Nominal USD.
  * - opex_structures
    - number
    - Production expenses: maintenance of structures. Nominal USD.
  * - opex_total
    - number
    - Total production expenses. Nominal USD.
  * - opex_water_for_power
    - number
    - Production expenses: water for power. Nominal USD.
  * - peak_demand_mw
    - number
    - Net peak demand on the plant (60-minute integration), in megawatts.
  * - plant_hours_connected_while_generating
    - number
    - Hours the plant was connected to load while generating.
  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.
  * - plant_type
    - string
    - Kind of plant (Run-of-River or Storage).
  * - project_num
    - integer
    - FERC Licensed Project Number.
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _plants_pudl:

-------------------------------------------------------------------------------
plants_pudl
-------------------------------------------------------------------------------

Home table for PUDL assigned plant IDs. These IDs are manually generated each
year when new FERC and EIA reporting is integrated, and any newly identified
plants are added to the list with a new ID. Each ID maps to a power plant
which is reported in at least one FERC or EIA data set. This table is read in
from a spreadsheet stored in the PUDL repository:
src/pudl/package_data/glue/mapping_eia923_ferc1.xlsx
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_pudl>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - plant_id_pudl
    - integer
    - A manually assigned PUDL plant ID. May not be constant over time.
  * - plant_name_pudl
    - string
    - Plant name, chosen arbitrarily from the several possible plant names available in the plant matching process. Included for human readability only.

.. _plants_pumped_storage_ferc1:

-------------------------------------------------------------------------------
plants_pumped_storage_ferc1
-------------------------------------------------------------------------------

Generating plant statistics for hydroelectric pumped storage plants with an
installed nameplate capacity of 10+ MW. As reported on page 408 of FERC Form 1
and extracted from the f1_pumped_storage table in FERC&#39;s FoxPro Database.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_pumped_storage_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - asset_retirement_cost
    - number
    - Cost of plant: asset retirement costs. Nominal USD.
  * - avg_num_employees
    - number
    - Average number of employees.
  * - capacity_mw
    - number
    - Total installed (nameplate) capacity, in megawatts.
  * - capex_equipment_electric
    - number
    - Cost of plant: accessory electric equipment. Nominal USD.
  * - capex_equipment_misc
    - number
    - Cost of plant: miscellaneous power plant equipment. Nominal USD.
  * - capex_facilities
    - number
    - Cost of plant: reservoirs, dams, and waterways. Nominal USD.
  * - capex_land
    - number
    - Cost of plant: land and land rights. Nominal USD.
  * - capex_per_mw
    - number
    - Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD.
  * - capex_roads
    - number
    - Cost of plant: roads, railroads, and bridges. Nominal USD.
  * - capex_structures
    - number
    - Cost of plant: structures and improvements. Nominal USD.
  * - capex_total
    - number
    - Total cost of plant. Nominal USD.
  * - capex_wheels_turbines_generators
    - number
    - Cost of plant: water wheels, turbines, and generators. Nominal USD.
  * - construction_type
    - string
    - Type of plant construction (&#39;outdoor&#39;, &#39;semioutdoor&#39;, or &#39;conventional&#39;). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.
  * - construction_year
    - year
    - Four digit year of the plant&#39;s original construction.
  * - energy_used_for_pumping_mwh
    - number
    - Energy used for pumping, in megawatt-hours.
  * - installation_year
    - year
    - Four digit year in which the last unit was installed.
  * - net_generation_mwh
    - number
    - Net generation, exclusive of plant use, in megawatt hours.
  * - net_load_mwh
    - number
    - Net output for load (net generation - energy used for pumping) in megawatt-hours.
  * - opex_dams
    - number
    - Production expenses: maintenance of reservoirs, dams, and waterways. Nominal USD.
  * - opex_electric
    - number
    - Production expenses: electric expenses. Nominal USD.
  * - opex_engineering
    - number
    - Production expenses: maintenance, supervision, and engineering. Nominal USD.
  * - opex_generation_misc
    - number
    - Production expenses: miscellaneous pumped storage power generation expenses. Nominal USD.
  * - opex_misc_plant
    - number
    - Production expenses: maintenance of miscellaneous hydraulic plant. Nominal USD.
  * - opex_operations
    - number
    - Production expenses: operation, supervision, and engineering. Nominal USD.
  * - opex_per_mwh
    - number
    - Production expenses per net megawatt hour generated. Nominal USD.
  * - opex_plant
    - number
    - Production expenses: maintenance of electric plant. Nominal USD.
  * - opex_production_before_pumping
    - number
    - Total production expenses before pumping. Nominal USD.
  * - opex_pumped_storage
    - number
    - Production expenses: pumped storage. Nominal USD.
  * - opex_pumping
    - number
    - Production expenses: We are here to PUMP YOU UP! Nominal USD.
  * - opex_rents
    - number
    - Production expenses: rent. Nominal USD.
  * - opex_structures
    - number
    - Production expenses: maintenance of structures. Nominal USD.
  * - opex_total
    - number
    - Total production expenses. Nominal USD.
  * - opex_water_for_power
    - number
    - Production expenses: water for power. Nominal USD.
  * - peak_demand_mw
    - number
    - Net peak demand on the plant (60-minute integration), in megawatts.
  * - plant_capability_mw
    - number
    - Net plant capability in megawatts.
  * - plant_hours_connected_while_generating
    - number
    - Hours the plant was connected to load while generating.
  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.
  * - project_num
    - integer
    - FERC Licensed Project Number.
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _plants_small_ferc1:

-------------------------------------------------------------------------------
plants_small_ferc1
-------------------------------------------------------------------------------

Generating plant statistics for steam plants with less than 25 MW installed
nameplate capacity and internal combustion plants, gas turbine-plants,
conventional hydro plants, and pumped storage plants with less than 10 MW
installed nameplate capacity. As reported on FERC Form 1 pages 410-411, and
extracted from the FERC FoxPro database table f1_gnrt_plant.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_small_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - capacity_mw
    - number
    - Name plate capacity in megawatts.
  * - capex_per_mw
    - number
    - Plant costs (including asset retirement costs) per megawatt. Nominal USD.
  * - construction_year
    - year
    - Original year of plant construction.
  * - ferc_license_id
    - integer
    - FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible.
  * - fuel_cost_per_mmbtu
    - number
    - Average fuel cost per mmBTU (if applicable). Nominal USD.
  * - fuel_type
    - string
    - Kind of fuel. Originally reported to FERC as a freeform string. Assigned a canonical value by PUDL based on our best guess.
  * - net_generation_mwh
    - number
    - Net generation excluding plant use, in megawatt-hours.
  * - opex_fuel
    - number
    - Production expenses: Fuel. Nominal USD.
  * - opex_maintenance
    - number
    - Production expenses: Maintenance. Nominal USD.
  * - opex_total
    - number
    - Total plant operating expenses, excluding fuel. Nominal USD.
  * - peak_demand_mw
    - number
    - Net peak demand for 60 minutes. Note: in some cases peak demand for other time periods may have been reported instead, if hourly peak demand was unavailable.
  * - plant_name_ferc1
    - string
    - PUDL assigned simplified plant name.
  * - plant_name_original
    - string
    - Original plant name in the FERC Form 1 FoxPro database.
  * - plant_type
    - string
    - PUDL assigned plant type. This is a best guess based on the fuel type, plant name, and other attributes.
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - total_cost_of_plant
    - number
    - Total cost of plant. Nominal USD.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _plants_steam_ferc1:

-------------------------------------------------------------------------------
plants_steam_ferc1
-------------------------------------------------------------------------------

Generating plant statistics for steam plants with a capacity of 25+ MW,
internal combustion and gas-turbine plants of 10+ MW, and all nuclear plants.
As reported on page 402 of FERC Form 1 and extracted from the f1_gnrt_plant
table in FERC&#39;s FoxPro Database.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/plants_steam_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - asset_retirement_cost
    - number
    - Asset retirement cost.
  * - avg_num_employees
    - number
    - Average number of plant employees during report year.
  * - capacity_mw
    - number
    - Total installed plant capacity in MW.
  * - capex_equipment
    - number
    - Capital expense for equipment.
  * - capex_land
    - number
    - Capital expense for land and land rights.
  * - capex_per_mw
    - number
    - Capital expenses per MW of installed plant capacity.
  * - capex_structures
    - number
    - Capital expense for structures and improvements.
  * - capex_total
    - number
    - Total capital expenses.
  * - construction_type
    - string
    - Type of plant construction (&#39;outdoor&#39;, &#39;semioutdoor&#39;, or &#39;conventional&#39;). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.
  * - construction_year
    - year
    - Year the plant&#39;s oldest still operational unit was built.
  * - installation_year
    - year
    - Year the plant&#39;s most recently built unit was installed.
  * - net_generation_mwh
    - number
    - Net generation (exclusive of plant use) in MWh during report year.
  * - not_water_limited_capacity_mw
    - number
    - Plant capacity in MW when not limited by condenser water.
  * - opex_allowances
    - number
    - Allowances.
  * - opex_boiler
    - number
    - Maintenance of boiler (or reactor) plant.
  * - opex_coolants
    - number
    - Cost of coolants and water (nuclear plants only)
  * - opex_electric
    - number
    - Electricity expenses.
  * - opex_engineering
    - number
    - Maintenance, supervision, and engineering.
  * - opex_fuel
    - number
    - Total cost of fuel.
  * - opex_misc_power
    - number
    - Miscellaneous steam (or nuclear) expenses.
  * - opex_misc_steam
    - number
    - Maintenance of miscellaneous steam (or nuclear) plant.
  * - opex_operations
    - number
    - Production expenses: operations, supervision, and engineering.
  * - opex_per_mwh
    - number
    - Total operating expenses per MWh of net generation.
  * - opex_plants
    - number
    - Maintenance of electrical plant.
  * - opex_production_total
    - number
    - Total operating epxenses.
  * - opex_rents
    - number
    - Rents.
  * - opex_steam
    - number
    - Steam expenses.
  * - opex_steam_other
    - number
    - Steam from other sources.
  * - opex_structures
    - number
    - Maintenance of structures.
  * - opex_transfer
    - number
    - Steam transferred (Credit).
  * - peak_demand_mw
    - number
    - Net peak demand experienced by the plant in MW in report year.
  * - plant_capability_mw
    - number
    - Net continuous plant capability in MW
  * - plant_hours_connected_while_generating
    - number
    - Total number hours the plant was generated and connected to load during report year.
  * - plant_id_ferc1
    - integer
    - Algorithmically assigned PUDL FERC Plant ID. WARNING: NOT STABLE BETWEEN PUDL DB INITIALIZATIONS.
  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.
  * - plant_type
    - string
    - Simplified plant type, categorized by PUDL based on our best guess of what was intended based on freeform string reported to FERC. Unidentifiable types are null.
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.
  * - water_limited_capacity_mw
    - number
    - Plant capacity in MW when limited by condenser water.

.. _prime_movers_eia923:

-------------------------------------------------------------------------------
prime_movers_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/prime_movers_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - abbr
    - string
    - N/A
  * - prime_mover
    - string
    - N/A

.. _purchased_power_ferc1:

-------------------------------------------------------------------------------
purchased_power_ferc1
-------------------------------------------------------------------------------

Purchased Power (Account 555) including power exchanges (i.e. transactions
involving a balancing of debits and credits for energy, capacity, etc.) and
any settlements for imbalanced exchanges. Reported on pages 326-327 of FERC
Form 1. Extracted from the f1_purchased_pwr table in FERC&#39;s FoxPro database.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/purchased_power_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - billing_demand_mw
    - number
    - Monthly average billing demand (for requirements purchases, and any transactions involving demand charges). In megawatts.
  * - coincident_peak_demand_mw
    - number
    - Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier&#39;s system reaches its monthly peak. In megawatts.
  * - delivered_mwh
    - number
    - Gross megawatt-hours delivered in power exchanges and used as the basis for settlement.
  * - demand_charges
    - number
    - Demand charges. Nominal USD.
  * - energy_charges
    - number
    - Energy charges. Nominal USD.
  * - non_coincident_peak_demand_mw
    - number
    - Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts.
  * - other_charges
    - number
    - Other charges, including out-of-period adjustments. Nominal USD.
  * - purchase_type
    - string
    - Categorization based on the original contractual terms and conditions of the service. Must be one of &#39;requirements&#39;, &#39;long_firm&#39;, &#39;intermediate_firm&#39;, &#39;short_firm&#39;, &#39;long_unit&#39;, &#39;intermediate_unit&#39;, &#39;electricity_exchange&#39;, &#39;other_service&#39;, or &#39;adjustment&#39;. Requirements service is ongoing high reliability service, with load integrated into system resource planning. &#39;Long term&#39; means 5+ years. &#39;Intermediate term&#39; is 1-5 years. &#39;Short term&#39; is less than 1 year. &#39;Firm&#39; means not interruptible for economic reasons. &#39;unit&#39; indicates service from a particular designated generating unit. &#39;exchange&#39; is an in-kind transaction.
  * - purchased_mwh
    - number
    - Megawatt-hours shown on bills rendered to the respondent.
  * - received_mwh
    - number
    - Gross megawatt-hours received in power exchanges and used as the basis for settlement.
  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.
  * - report_year
    - year
    - Four-digit year in which the data was reported.
  * - seller_name
    - string
    - Name of the seller, or the other party in an exchange transaction.
  * - tariff
    - string
    - FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)
  * - total_settlement
    - number
    - Sum of demand, energy, and other charges. For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative. Nominal USD.
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

.. _transport_modes_eia923:

-------------------------------------------------------------------------------
transport_modes_eia923
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/transport_modes_eia923>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - abbr
    - string
    - N/A
  * - mode
    - string
    - N/A

.. _utilities_eia:

-------------------------------------------------------------------------------
utilities_eia
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/utilities_eia>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - utility_id_eia
    - integer
    - The EIA Utility Identification number.
  * - utility_id_pudl
    - integer
    - A manually assigned PUDL utility ID. May not be stable over time.
  * - utility_name_eia
    - string
    - The name of the utility.

.. _utilities_eia860:

-------------------------------------------------------------------------------
utilities_eia860
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/utilities_eia860>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - address_2
    - string
    - N/A
  * - attention_line
    - string
    - N/A
  * - city
    - string
    - Name of the city in which operator/owner is located
  * - contact_firstname
    - string
    - N/A
  * - contact_firstname_2
    - string
    - N/A
  * - contact_lastname
    - string
    - N/A
  * - contact_lastname_2
    - string
    - N/A
  * - contact_title
    - string
    - N/A
  * - contact_title_2
    - string
    - N/A
  * - entity_type
    - string
    - Entity type of principle owner (C = Cooperative, I = Investor-Owned Utility, Q = Independent Power Producer, M = Municipally-Owned Utility, P = Political Subdivision, F = Federally-Owned Utility, S = State-Owned Utility, IND = Industrial, COM = Commercial
  * - phone_extension_1
    - string
    - Phone extension for contact 1
  * - phone_extension_2
    - string
    - Phone extension for contact 2
  * - phone_number_1
    - string
    - Phone number for contact 1
  * - phone_number_2
    - string
    - Phone number for contact 2
  * - plants_reported_asset_manager
    - string
    - Is the reporting entity an asset manager of power plants reported on Schedule 2 of the form?
  * - plants_reported_operator
    - string
    - Is the reporting entity an operator of power plants reported on Schedule 2 of the form?
  * - plants_reported_other_relationship
    - string
    - Does the reporting entity have any other relationship to the power plants reported on Schedule 2 of the form?
  * - plants_reported_owner
    - string
    - Is the reporting entity an owner of power plants reported on Schedule 2 of the form?
  * - report_date
    - date
    - Date reported.
  * - state
    - string
    - State of the operator/owner
  * - street_address
    - string
    - Street address of the operator/owner
  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.
  * - zip_code
    - string
    - Zip code of the operator/owner
  * - zip_code_4
    - string
    - N/A

.. _utilities_entity_eia:

-------------------------------------------------------------------------------
utilities_entity_eia
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/utilities_entity_eia>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - utility_id_eia
    - integer
    - The EIA Utility Identification number.
  * - utility_name_eia
    - string
    - The name of the utility.

.. _utilities_ferc1:

-------------------------------------------------------------------------------
utilities_ferc1
-------------------------------------------------------------------------------

This table maps the manually assigned PUDL utility ID to a FERC respondent ID,
enabling a connection between the FERC and EIA data sets. It also stores the
utility name associated with the FERC respondent ID. Those values originate in
the f1_respondent_id table in FERC&#39;s FoxPro database, which is stored in a
file called F1_1.DBF. This table is generated from a spreadsheet stored in the
PUDL repository: results/id_mapping/mapping_eia923_ferc1.xlsx
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/utilities_ferc1>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.
  * - utility_id_pudl
    - integer
    - A manually assigned PUDL utility ID. May not be stable over time.
  * - utility_name_ferc1
    - string
    - Name of the responding utility, as it is reported in FERC Form 1. For human readability only.

.. _utilities_pudl:

-------------------------------------------------------------------------------
utilities_pudl
-------------------------------------------------------------------------------

Home table for PUDL assigned utility IDs. These IDs are manually generated
each year when new FERC and EIA reporting is integrated, and any newly found
utilities are added to the list with a new ID. Each ID maps to a power plant
owning or operating entity which is reported in at least one FERC or EIA data
set. This table is read in from a spreadsheet stored in the PUDL repository:
src/pudl/package_data/glue/mapping_eia923_ferc1.xlsx
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/utilities_pudl>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - utility_id_pudl
    - integer
    - A manually assigned PUDL utility ID. May not be stable over time.
  * - utility_name_pudl
    - string
    - Utility name, chosen arbitrarily from the several possible utility names available in the utility matching process. Included for human readability only.

.. _utility_plant_assn:

-------------------------------------------------------------------------------
utility_plant_assn
-------------------------------------------------------------------------------

Pending description.
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/utility_plant_assn>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**
  * - plant_id_pudl
    - integer
    - N/A
  * - utility_id_pudl
    - integer
    - N/A
