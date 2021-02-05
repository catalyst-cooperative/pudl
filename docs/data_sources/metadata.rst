

.. _fuel_ferc1:

Contents of fuel_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.

  * - fuel_type_code_pudl
    - string
    - PUDL assigned code indicating the general fuel type.

  * - fuel_unit
    - string
    - PUDL assigned code indicating reported fuel unit of measure.

  * - fuel_qty_burned
    - number
    - Quantity of fuel consumed in the report year, in terms of the reported fuel units.

  * - fuel_mmbtu_per_unit
    - number
    - Average heat content of fuel consumed in the report year, in mmBTU per reported fuel unit.

  * - fuel_cost_per_unit_burned
    - number
    - Average cost of fuel consumed in the report year, in nominal USD per reported fuel unit.

  * - fuel_cost_per_unit_delivered
    - number
    - Average cost of fuel delivered in the report year, in nominal USD per reported fuel unit.

  * - fuel_cost_per_mmbtu
    - number
    - Average cost of fuel consumed in the report year, in nominal USD per mmBTU of fuel heat content.


.. _load_curves_epaipm:

Contents of load_curves_epaipm table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - region_id_epaipm
    - string
    - Name of the IPM region

  * - month
    - integer
    - Month of the year

  * - day_of_year
    - integer
    - Day of the year

  * - hour
    - integer
    - Hour of the day (0-23). Original IPM values were 1-24.

  * - time_index
    - integer
    - 8760 index hour of the year

  * - load_mw
    - number
    - Load (MW) in an hour of the day for the IPM region


.. _plant_region_map_epaipm:

Contents of plant_region_map_epaipm table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - region
    - string
    - Name of the IPM region


.. _ferc_depreciation_lines:

Contents of ferc_depreciation_lines table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - line_id
    - string
    - A human readable string uniquely identifying the FERC depreciation account. Used in lieu of the actual line number, as those numbers are not guaranteed to be consistent from year to year.

  * - description
    - string
    - Description of the FERC depreciation account, as listed on FERC Form 1, Page 219.


.. _utilities_eia:

Contents of utilities_eia table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_eia
    - integer
    - The EIA Utility Identification number.

  * - utility_name_eia
    - string
    - The name of the utility.

  * - utility_id_pudl
    - integer
    - A manually assigned PUDL utility ID. May not be stable over time.


.. _energy_source_eia923:

Contents of energy_source_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - abbr
    - string
    - N/A

  * - source
    - string
    - N/A


.. _datasets:

Contents of datasets table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - datasource
    - string
    - Code identifying a dataset available within PUDL.

  * - active
    - boolean
    - Indicates whether or not the dataset has been pulled into PUDL by the extract transform load process.


.. _fuel_type_eia923:

Contents of fuel_type_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - abbr
    - string
    - N/A

  * - fuel_type
    - string
    - N/A


.. _utilities_pudl:

Contents of utilities_pudl table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_pudl
    - integer
    - A manually assigned PUDL utility ID. May not be stable over time.

  * - utility_name_pudl
    - string
    - Utility name, chosen arbitrarily from the several possible utility names available in the utility matching process. Included for human readability only.


.. _plants_ferc1:

Contents of plants_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.

  * - plant_id_pudl
    - integer
    - A manually assigned PUDL plant ID. May not be constant over time.


.. _generation_eia923:

Contents of generation_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - generator_id
    - string
    - Generator identification code. Often numeric, but sometimes includes letters. It's a string!

  * - report_date
    - date
    - Date reported.

  * - net_generation_mwh
    - number
    - Net generation for specified period in megawatthours (MWh).


.. _utilities_entity_eia:

Contents of utilities_entity_eia table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_eia
    - integer
    - The EIA Utility Identification number.

  * - utility_name_eia
    - string
    - The name of the utility.


.. _generators_entity_eia:

Contents of generators_entity_eia table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - generator_id
    - string
    - Generator identification number

  * - prime_mover_code
    - string
    - EIA assigned code for the prime mover (i.e. the engine, turbine, water wheel, or similar machine that drives an electric generator)

  * - duct_burners
    - boolean
    - Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas

  * - operating_date
    - date
    - Date the generator began commercial operation

  * - topping_bottoming_code
    - string
    - If the generator is associated with a combined heat and power system, indicates whether the generator is part of a topping cycle or a bottoming cycle

  * - solid_fuel_gasification
    - boolean
    - Indicates whether the generator is part of a solid fuel gasification system

  * - pulverized_coal_tech
    - boolean
    - Indicates whether the generator uses pulverized coal technology

  * - fluidized_bed_tech
    - boolean
    - Indicates whether the generator uses fluidized bed technology

  * - subcritical_tech
    - boolean
    - Indicates whether the generator uses subcritical technology

  * - supercritical_tech
    - boolean
    - Indicates whether the generator uses supercritical technology

  * - ultrasupercritical_tech
    - boolean
    - Indicates whether the generator uses ultra-supercritical technology

  * - stoker_tech
    - boolean
    - Indicates whether the generator uses stoker technology

  * - other_combustion_tech
    - boolean
    - Indicates whether the generator uses other combustion technologies

  * - bypass_heat_recovery
    - boolean
    - Can this generator operate while bypassing the heat recovery steam generator?

  * - rto_iso_lmp_node_id
    - string
    - The designation used to identify the price node in RTO/ISO Locational Marginal Price reports

  * - rto_iso_location_wholesale_reporting_id
    - string
    - The designation used to report ths specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report

  * - associated_combined_heat_power
    - boolean
    - Indicates whether the generator is associated with a combined heat and power system

  * - original_planned_operating_date
    - date
    - The date the generator was originally scheduled to be operational

  * - operating_switch
    - string
    - Indicates whether the fuel switching generator can switch when operating

  * - previously_canceled
    - boolean
    - Indicates whether the generator was previously reported as indefinitely postponed or canceled


.. _regions_entity_epaipm:

Contents of regions_entity_epaipm table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - region_id_epaipm
    - string
    - N/A


.. _plants_hydro_ferc1:

Contents of plants_hydro_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.

  * - project_num
    - integer
    - FERC Licensed Project Number.

  * - plant_type
    - string
    - Kind of plant (Run-of-River or Storage).

  * - construction_type
    - string
    - Type of plant construction ('outdoor', 'semioutdoor', or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.

  * - construction_year
    - year
    - Four digit year of the plant's original construction.

  * - installation_year
    - year
    - Four digit year in which the last unit was installed.

  * - capacity_mw
    - number
    - Total installed (nameplate) capacity, in megawatts.

  * - peak_demand_mw
    - number
    - Net peak demand on the plant (60-minute integration), in megawatts.

  * - plant_hours_connected_while_generating
    - number
    - Hours the plant was connected to load while generating.

  * - net_capacity_favorable_conditions_mw
    - number
    - Net plant capability under the most favorable operating conditions, in megawatts.

  * - net_capacity_adverse_conditions_mw
    - number
    - Net plant capability under the least favorable operating conditions, in megawatts.

  * - avg_num_employees
    - number
    - Average number of employees.

  * - net_generation_mwh
    - number
    - Net generation, exclusive of plant use, in megawatt hours.

  * - capex_land
    - number
    - Cost of plant: land and land rights. Nominal USD.

  * - capex_structures
    - number
    - Cost of plant: structures and improvements. Nominal USD.

  * - capex_facilities
    - number
    - Cost of plant: reservoirs, dams, and waterways. Nominal USD.

  * - capex_equipment
    - number
    - Cost of plant: equipment. Nominal USD.

  * - capex_roads
    - number
    - Cost of plant: roads, railroads, and bridges. Nominal USD.

  * - asset_retirement_cost
    - number
    - Cost of plant: asset retirement costs. Nominal USD.

  * - capex_total
    - number
    - Total cost of plant. Nominal USD.

  * - capex_per_mw
    - number
    - Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD.

  * - opex_operations
    - number
    - Production expenses: operation, supervision, and engineering. Nominal USD.

  * - opex_water_for_power
    - number
    - Production expenses: water for power. Nominal USD.

  * - opex_hydraulic
    - number
    - Production expenses: hydraulic expenses. Nominal USD.

  * - opex_electric
    - number
    - Production expenses: electric expenses. Nominal USD.

  * - opex_generation_misc
    - number
    - Production expenses: miscellaneous hydraulic power generation expenses. Nominal USD.

  * - opex_rents
    - number
    - Production expenses: rent. Nominal USD.

  * - opex_engineering
    - number
    - Production expenses: maintenance, supervision, and engineering. Nominal USD.

  * - opex_structures
    - number
    - Production expenses: maintenance of structures. Nominal USD.

  * - opex_dams
    - number
    - Production expenses: maintenance of reservoirs, dams, and waterways. Nominal USD.

  * - opex_plant
    - number
    - Production expenses: maintenance of electric plant. Nominal USD.

  * - opex_misc_plant
    - number
    - Production expenses: maintenance of miscellaneous hydraulic plant. Nominal USD.

  * - opex_total
    - number
    - Total production expenses. Nominal USD.

  * - opex_per_mwh
    - number
    - Production expenses per net megawatt hour generated. Nominal USD.


.. _plant_in_service_ferc1:

Contents of plant_in_service_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - amount_type
    - string
    - String indicating which original FERC Form 1 column the listed amount came from. Each field should have one (potentially NA) value of each type for each utility in each year, and the ending_balance should equal the sum of starting_balance, additions, retirements, adjustments, and transfers.

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

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


.. _purchased_power_ferc1:

Contents of purchased_power_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - seller_name
    - string
    - Name of the seller, or the other party in an exchange transaction.

  * - purchase_type
    - string
    - Categorization based on the original contractual terms and conditions of the service. Must be one of 'requirements', 'long_firm', 'intermediate_firm', 'short_firm', 'long_unit', 'intermediate_unit', 'electricity_exchange', 'other_service', or 'adjustment'. Requirements service is ongoing high reliability service, with load integrated into system resource planning. 'Long term' means 5+ years. 'Intermediate term' is 1-5 years. 'Short term' is less than 1 year. 'Firm' means not interruptible for economic reasons. 'unit' indicates service from a particular designated generating unit. 'exchange' is an in-kind transaction.

  * - tariff
    - string
    - FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)

  * - billing_demand_mw
    - number
    - Monthly average billing demand (for requirements purchases, and any transactions involving demand charges). In megawatts.

  * - non_coincident_peak_demand_mw
    - number
    - Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts.

  * - coincident_peak_demand_mw
    - number
    - Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier's system reaches its monthly peak. In megawatts.

  * - purchased_mwh
    - number
    - Megawatt-hours shown on bills rendered to the respondent.

  * - received_mwh
    - number
    - Gross megawatt-hours received in power exchanges and used as the basis for settlement.

  * - delivered_mwh
    - number
    - Gross megawatt-hours delivered in power exchanges and used as the basis for settlement.

  * - demand_charges
    - number
    - Demand charges. Nominal USD.

  * - energy_charges
    - number
    - Energy charges. Nominal USD.

  * - other_charges
    - number
    - Other charges, including out-of-period adjustments. Nominal USD.

  * - total_settlement
    - number
    - Sum of demand, energy, and other charges. For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative. Nominal USD.


.. _generators_eia860:

Contents of generators_eia860 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - generator_id
    - string
    - Generator identification number.

  * - report_date
    - date
    - Date reported.

  * - operational_status_code
    - string
    - The operating status of the generator.

  * - operational_status
    - string
    - The operating status of the generator. This is based on which tab the generator was listed in in EIA 860.

  * - ownership_code
    - string
    - Identifies the ownership for each generator.

  * - owned_by_non_utility
    - boolean
    - Whether any part of generator is owned by a nonutilty

  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.

  * - capacity_mw
    - number
    - The highest value on the generator nameplate in megawatts rounded to the nearest tenth.

  * - reactive_power_output_mvar
    - number
    - Reactive Power Output (MVAr)

  * - summer_capacity_mw
    - number
    - The net summer capacity.

  * - winter_capacity_mw
    - number
    - The net winter capacity.

  * - summer_capacity_estimate
    - boolean
    - Whether the summer capacity value was an estimate

  * - winter_capacity_estimate
    - boolean
    - Whether the winter capacity value was an estimate

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

  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.

  * - distributed_generation
    - boolean
    - Whether the generator is considered distributed generation

  * - multiple_fuels
    - boolean
    - Can the generator burn multiple fuels?

  * - deliver_power_transgrid
    - boolean
    - Indicate whether the generator can deliver power to the transmission grid.

  * - syncronized_transmission_grid
    - boolean
    - Indicates whether standby generators (SB status) can be synchronized to the grid.

  * - turbines_num
    - integer
    - Number of wind turbines, or hydrokinetic buoys.

  * - planned_modifications
    - boolean
    - Indicates whether there are any planned capacity uprates/derates, repowering, other modifications, or generator retirements scheduled for the next 5 years.

  * - planned_net_summer_capacity_uprate_mw
    - number
    - Increase in summer capacity expected to be realized from the modification to the equipment.

  * - planned_net_winter_capacity_uprate_mw
    - number
    - Increase in winter capacity expected to be realized from the uprate modification to the equipment.

  * - planned_uprate_date
    - date
    - Planned effective date that the generator is scheduled to enter operation after the uprate modification.

  * - planned_net_summer_capacity_derate_mw
    - number
    - Decrease in summer capacity expected to be realized from the derate modification to the equipment.

  * - planned_net_winter_capacity_derate_mw
    - number
    - Decrease in winter capacity expected to be realized from the derate modification to the equipment.

  * - planned_derate_date
    - date
    - Planned effective month that the generator is scheduled to enter operation after the derate modification.

  * - planned_new_prime_mover_code
    - string
    - New prime mover for the planned repowered generator.

  * - planned_energy_source_code_1
    - string
    - New energy source code for the planned repowered generator.

  * - planned_repower_date
    - date
    - Planned effective date that the generator is scheduled to enter operation after the repowering is complete.

  * - other_planned_modifications
    - boolean
    - Indicates whether there are there other modifications planned for the generator.

  * - other_modifications_date
    - date
    - Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete.

  * - planned_retirement_date
    - date
    - Planned effective date of the scheduled retirement of the generator.

  * - carbon_capture
    - boolean
    - Indicates whether the generator uses carbon capture technology.

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

  * - technology_description
    - string
    - High level description of the technology used by the generator to produce electricity.

  * - turbines_inverters_hydrokinetics
    - string
    - Number of wind turbines, or hydrokinetic buoys.

  * - time_cold_shutdown_full_load_code
    - string
    - The minimum amount of time required to bring the unit to full load from shutdown.

  * - planned_new_capacity_mw
    - number
    - The expected new namplate capacity for the generator.

  * - cofire_fuels
    - boolean
    - Can the generator co-fire fuels?.

  * - switch_oil_gas
    - boolean
    - Indicates whether the generator switch between oil and natural gas.

  * - nameplate_power_factor
    - number
    - The nameplate power factor of the generator.

  * - minimum_load_mw
    - number
    - The minimum load at which the generator can operate at continuosuly.

  * - uprate_derate_during_year
    - boolean
    - Was an uprate or derate completed on this generator during the reporting year?

  * - uprate_derate_completed_date
    - date
    - The date when the uprate or derate was completed.

  * - current_planned_operating_date
    - date
    - The most recently updated effective date on which the generator is scheduled to start operation

  * - summer_estimated_capability_mw
    - number
    - EIA estimated summer capacity (in MWh).

  * - winter_estimated_capability_mw
    - number
    - EIA estimated winter capacity (in MWh).

  * - retirement_date
    - date
    - Date of the scheduled or effected retirement of the generator.

  * - data_source
    - string
    - Source of EIA 860 data. Either Annual EIA 860 or the year-to-date updates from EIA 860M.


.. _ownership_eia860:

Contents of ownership_eia860 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - report_date
    - date
    - Date reported.

  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - generator_id
    - string
    - Generator identification number.

  * - owner_utility_id_eia
    - integer
    - EIA-assigned owner's identification number.

  * - owner_name
    - string
    - Name of owner.

  * - owner_state
    - string
    - Two letter US & Canadian state and territory abbreviations.

  * - owner_city
    - string
    - City of owner.

  * - owner_street_address
    - string
    - Steet address of owner.

  * - owner_zip_code
    - string
    - Zip code of owner.

  * - fraction_owned
    - number
    - Proportion of generator ownership.


.. _plants_pudl:

Contents of plants_pudl table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_pudl
    - integer
    - A manually assigned PUDL plant ID. May not be constant over time.

  * - plant_name_pudl
    - string
    - Plant name, chosen arbitrarily from the several possible plant names available in the plant matching process. Included for human readability only.


.. _fuel_type_aer_eia923:

Contents of fuel_type_aer_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - abbr
    - string
    - N/A

  * - fuel_type
    - string
    - N/A


.. _accumulated_depreciation_ferc1:

Contents of accumulated_depreciation_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_ferc1
    - integer
    - FERC-assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - line_id
    - string
    - Line numbers, and corresponding FERC account number from FERC Form 1, page 2019, Accumulated Provision for Depreciation of Electric Utility Plant (Account 108).

  * - total
    - number
    - Total of Electric Plant In Service, Electric Plant Held for Future Use, and Electric Plant Leased to Others. Nominal USD.

  * - electric_plant
    - number
    - Electric Plant In Service. Nominal USD.

  * - future_plant
    - number
    - Electric Plant Held for Future Use. Nominal USD.

  * - leased_plant
    - number
    - Electric Plant Leased to Others. Nominal USD.


.. _prime_movers_eia923:

Contents of prime_movers_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - abbr
    - string
    - N/A

  * - prime_mover
    - string
    - N/A


.. _fuel_receipts_costs_eia923:

Contents of fuel_receipts_costs_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - id
    - integer
    - PUDL issued surrogate key.

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - report_date
    - date
    - Date reported.

  * - contract_type_code
    - string
    - Purchase type under which receipts occurred in the reporting month. C: Contract, NC: New Contract, S: Spot Purchase, T: Tolling Agreement.

  * - contract_expiration_date
    - date
    - Date contract expires.Format:  MMYY.

  * - energy_source_code
    - string
    - The fuel code associated with the fuel receipt. Two or three character alphanumeric.

  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.

  * - fuel_group_code
    - string
    - Groups the energy sources into fuel groups that are located in the Electric Power Monthly:  Coal, Natural Gas, Petroleum, Petroleum Coke.

  * - fuel_group_code_simple
    - string
    - Simplified grouping of fuel_group_code, with Coal and Petroluem Coke as well as Natural Gas and Other Gas grouped together.

  * - mine_id_pudl
    - integer
    - PUDL mine identification number.

  * - supplier_name
    - string
    - Company that sold the fuel to the plant or, in the case of Natural Gas, pipline owner.

  * - fuel_qty_units
    - number
    - Quanity of fuel received in tons, barrel, or Mcf.

  * - heat_content_mmbtu_per_unit
    - number
    - Heat content of the fuel in millions of Btus per physical unit to the nearest 0.01 percent.

  * - sulfur_content_pct
    - number
    - Sulfur content percentage by weight to the nearest 0.01 percent.

  * - ash_content_pct
    - number
    - Ash content percentage by weight to the nearest 0.1 percent.

  * - mercury_content_ppm
    - number
    - Mercury content in parts per million (ppm) to the nearest 0.001 ppm.

  * - fuel_cost_per_mmbtu
    - number
    - All costs incurred in the purchase and delivery of the fuel to the plant in cents per million Btu(MMBtu) to the nearest 0.1 cent.

  * - primary_transportation_mode_code
    - string
    - Transportation mode for the longest distance transported.

  * - secondary_transportation_mode_code
    - string
    - Transportation mode for the second longest distance transported.

  * - natural_gas_transport_code
    - string
    - Contract type for natural gas transportation service.

  * - natural_gas_delivery_contract_type_code
    - string
    - Contract type for natrual gas delivery service:

  * - moisture_content_pct
    - number
    - N/A

  * - chlorine_content_ppm
    - number
    - N/A


.. _utilities_ferc1:

Contents of utilities_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - utility_name_ferc1
    - string
    - Name of the responding utility, as it is reported in FERC Form 1. For human readability only.

  * - utility_id_pudl
    - integer
    - A manually assigned PUDL utility ID. May not be stable over time.


.. _boiler_generator_assn_eia860:

Contents of boiler_generator_assn_eia860 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - report_date
    - date
    - Date reported.

  * - generator_id
    - string
    - EIA-assigned generator identification code.

  * - boiler_id
    - string
    - EIA-assigned boiler identification code.

  * - unit_id_eia
    - string
    - EIA-assigned unit identification code.

  * - unit_id_pudl
    - integer
    - PUDL-assigned unit identification number.

  * - bga_source
    - string
    - The source from where the unit_id_pudl is compiled. The unit_id_pudl comes directly from EIA 860, or string association (which looks at all the boilers and generators that are not associated with a unit and tries to find a matching string in the respective collection of boilers or generator), or from a unit connection (where the unit_id_eia is employed to find additional boiler generator connections).


.. _natural_gas_transport_eia923:

Contents of natural_gas_transport_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - abbr
    - string
    - N/A

  * - status
    - string
    - N/A


.. _transport_modes_eia923:

Contents of transport_modes_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - abbr
    - string
    - N/A

  * - mode
    - string
    - N/A


.. _coalmine_eia923:

Contents of coalmine_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

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

  * - county_id_fips
    - integer
    - County ID from the Federal Information Processing Standard Publication 6-4.

  * - mine_id_msha
    - integer
    - MSHA issued mine identifier.


.. _plants_eia:

Contents of plants_eia table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - plant_name_eia
    - string
    - N/A

  * - plant_id_pudl
    - integer
    - N/A


.. _ferc_accounts:

Contents of ferc_accounts table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - ferc_account_id
    - string
    - Account number, from FERC's Uniform System of Accounts for Electric Plant. Also includes higher level labeled categories.

  * - description
    - string
    - Long description of the FERC Account.


.. _utility_plant_assn:

Contents of utility_plant_assn table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_pudl
    - integer
    - N/A

  * - plant_id_pudl
    - integer
    - N/A


.. _plants_eia860:

Contents of plants_eia860 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - report_date
    - date
    - Date reported.

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

  * - natural_gas_storage
    - string
    - Indicates if the facility have on-site storage of natural gas.

  * - natural_gas_pipeline_name_1
    - string
    - The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.

  * - natural_gas_pipeline_name_2
    - string
    - The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.

  * - natural_gas_pipeline_name_3
    - string
    - The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.

  * - nerc_region
    - string
    - NERC region in which the plant is located

  * - net_metering
    - string
    - Did this plant have a net metering agreement in effect during the reporting year?  (Only displayed for facilities that report the sun or wind as an energy source). This field was only reported up until 2015

  * - pipeline_notes
    - string
    - Additional owner or operator of natural gas pipeline.

  * - regulatory_status_code
    - string
    - Indicates whether the plant is regulated or non-regulated.

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


.. _generation_fuel_eia923:

Contents of generation_fuel_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - report_date
    - date
    - Date reported.

  * - nuclear_unit_id
    - integer
    - For nuclear plants only, the unit number .One digit numeric. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level.

  * - fuel_type
    - string
    - The fuel code reported to EIA. Two or three letter alphanumeric.

  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.

  * - fuel_type_code_aer
    - string
    - A partial aggregation of the reported fuel type codes into larger categories used by EIA in, for example, the Annual Energy Review (AER).Two or three letter alphanumeric.

  * - prime_mover_code
    - string
    - Type of prime mover.

  * - fuel_consumed_units
    - number
    - Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.

  * - fuel_consumed_for_electricity_units
    - number
    - Consumption for electric generation of the fuel type in physical units.

  * - fuel_mmbtu_per_unit
    - number
    - Heat content of the fuel in millions of Btus per physical unit.

  * - fuel_consumed_mmbtu
    - number
    - Total consumption of fuel in physical units, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.

  * - fuel_consumed_for_electricity_mmbtu
    - number
    - Total consumption of fuel to produce electricity, in physical units, year to date.

  * - net_generation_mwh
    - number
    - Net generation, year to date in megawatthours (MWh). This is total electrical output net of station service.  In the case of combined heat and power plants, this value is intended to include internal consumption of electricity for the purposes of a production process, as well as power put on the grid.


.. _plants_small_ferc1:

Contents of plants_small_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - plant_name_original
    - string
    - Original plant name in the FERC Form 1 FoxPro database.

  * - plant_name_ferc1
    - string
    - PUDL assigned simplified plant name.

  * - plant_type
    - string
    - PUDL assigned plant type. This is a best guess based on the fuel type, plant name, and other attributes.

  * - ferc_license_id
    - integer
    - FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible.

  * - construction_year
    - year
    - Original year of plant construction.

  * - capacity_mw
    - number
    - Name plate capacity in megawatts.

  * - peak_demand_mw
    - number
    - Net peak demand for 60 minutes. Note: in some cases peak demand for other time periods may have been reported instead, if hourly peak demand was unavailable.

  * - net_generation_mwh
    - number
    - Net generation excluding plant use, in megawatt-hours.

  * - total_cost_of_plant
    - number
    - Total cost of plant. Nominal USD.

  * - capex_per_mw
    - number
    - Plant costs (including asset retirement costs) per megawatt. Nominal USD.

  * - opex_total
    - number
    - Total plant operating expenses, excluding fuel. Nominal USD.

  * - opex_fuel
    - number
    - Production expenses: Fuel. Nominal USD.

  * - opex_maintenance
    - number
    - Production expenses: Maintenance. Nominal USD.

  * - fuel_type
    - string
    - Kind of fuel. Originally reported to FERC as a freeform string. Assigned a canonical value by PUDL based on our best guess.

  * - fuel_cost_per_mmbtu
    - number
    - Average fuel cost per mmBTU (if applicable). Nominal USD.


.. _plants_pumped_storage_ferc1:

Contents of plants_pumped_storage_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.

  * - project_num
    - integer
    - FERC Licensed Project Number.

  * - construction_type
    - string
    - Type of plant construction ('outdoor', 'semioutdoor', or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.

  * - construction_year
    - year
    - Four digit year of the plant's original construction.

  * - installation_year
    - year
    - Four digit year in which the last unit was installed.

  * - capacity_mw
    - number
    - Total installed (nameplate) capacity, in megawatts.

  * - peak_demand_mw
    - number
    - Net peak demand on the plant (60-minute integration), in megawatts.

  * - plant_hours_connected_while_generating
    - number
    - Hours the plant was connected to load while generating.

  * - plant_capability_mw
    - number
    - Net plant capability in megawatts.

  * - avg_num_employees
    - number
    - Average number of employees.

  * - net_generation_mwh
    - number
    - Net generation, exclusive of plant use, in megawatt hours.

  * - energy_used_for_pumping_mwh
    - number
    - Energy used for pumping, in megawatt-hours.

  * - net_load_mwh
    - number
    - Net output for load (net generation - energy used for pumping) in megawatt-hours.

  * - capex_land
    - number
    - Cost of plant: land and land rights. Nominal USD.

  * - capex_structures
    - number
    - Cost of plant: structures and improvements. Nominal USD.

  * - capex_facilities
    - number
    - Cost of plant: reservoirs, dams, and waterways. Nominal USD.

  * - capex_wheels_turbines_generators
    - number
    - Cost of plant: water wheels, turbines, and generators. Nominal USD.

  * - capex_equipment_electric
    - number
    - Cost of plant: accessory electric equipment. Nominal USD.

  * - capex_equipment_misc
    - number
    - Cost of plant: miscellaneous power plant equipment. Nominal USD.

  * - capex_roads
    - number
    - Cost of plant: roads, railroads, and bridges. Nominal USD.

  * - asset_retirement_cost
    - number
    - Cost of plant: asset retirement costs. Nominal USD.

  * - capex_total
    - number
    - Total cost of plant. Nominal USD.

  * - capex_per_mw
    - number
    - Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD.

  * - opex_operations
    - number
    - Production expenses: operation, supervision, and engineering. Nominal USD.

  * - opex_water_for_power
    - number
    - Production expenses: water for power. Nominal USD.

  * - opex_pumped_storage
    - number
    - Production expenses: pumped storage. Nominal USD.

  * - opex_electric
    - number
    - Production expenses: electric expenses. Nominal USD.

  * - opex_generation_misc
    - number
    - Production expenses: miscellaneous pumped storage power generation expenses. Nominal USD.

  * - opex_rents
    - number
    - Production expenses: rent. Nominal USD.

  * - opex_engineering
    - number
    - Production expenses: maintenance, supervision, and engineering. Nominal USD.

  * - opex_structures
    - number
    - Production expenses: maintenance of structures. Nominal USD.

  * - opex_dams
    - number
    - Production expenses: maintenance of reservoirs, dams, and waterways. Nominal USD.

  * - opex_plant
    - number
    - Production expenses: maintenance of electric plant. Nominal USD.

  * - opex_misc_plant
    - number
    - Production expenses: maintenance of miscellaneous hydraulic plant. Nominal USD.

  * - opex_production_before_pumping
    - number
    - Total production expenses before pumping. Nominal USD.

  * - opex_pumping
    - number
    - Production expenses: We are here to PUMP YOU UP! Nominal USD.

  * - opex_total
    - number
    - Total production expenses. Nominal USD.

  * - opex_per_mwh
    - number
    - Production expenses per net megawatt hour generated. Nominal USD.


.. _boiler_fuel_eia923:

Contents of boiler_fuel_eia923 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - boiler_id
    - string
    - Boiler identification code. Alphanumeric.

  * - fuel_type_code
    - string
    - The fuel code reported to EIA. Two or three letter alphanumeric.

  * - fuel_type_code_pudl
    - string
    - Standardized fuel codes in PUDL.

  * - report_date
    - date
    - Date reported.

  * - fuel_consumed_units
    - number
    - Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.

  * - fuel_mmbtu_per_unit
    - number
    - Heat content of the fuel in millions of Btus per physical unit.

  * - sulfur_content_pct
    - number
    - Sulfur content percentage by weight to the nearest 0.01 percent.

  * - ash_content_pct
    - number
    - Ash content percentage by weight to the nearest 0.1 percent.


.. _hourly_emissions_epacems:

Contents of hourly_emissions_epacems table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - state
    - string
    - State the plant is located in.

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - unitid
    - string
    - Facility-specific unit id (e.g. Unit 4)

  * - operating_datetime_utc
    - datetime
    - Date and time measurement began (UTC).

  * - operating_time_hours
    - number
    - Length of time interval measured.

  * - gross_load_mw
    - number
    - Average power in megawatts delivered during time interval measured.

  * - steam_load_1000_lbs
    - number
    - Total steam pressure produced by a unit during the reported hour.

  * - so2_mass_lbs
    - number
    - Sulfur dioxide emissions in pounds.

  * - so2_mass_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.

  * - nox_rate_lbs_mmbtu
    - number
    - The average rate at which NOx was emitted during a given time period.

  * - nox_rate_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.

  * - nox_mass_lbs
    - number
    - NOx emissions in pounds.

  * - nox_mass_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.

  * - co2_mass_tons
    - number
    - Carbon dioxide emissions in short tons.

  * - co2_mass_measurement_code
    - string
    - Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.

  * - heat_content_mmbtu
    - number
    - The energy contained in fuel burned, measured in million BTU.

  * - facility_id
    - integer
    - New EPA plant ID.

  * - unit_id_epa
    - integer
    - Smokestack' unit monitored by EPA CEMS.


.. _transmission_single_epaipm:

Contents of transmission_single_epaipm table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - region_from
    - string
    - Name of the IPM region sending electricity

  * - region_to
    - string
    - Name of the IPM region receiving electricity

  * - firm_ttc_mw
    - number
    - Transfer capacity with N-1 lines (used for reserve margins)

  * - nonfirm_ttc_mw
    - number
    - Transfer capacity with N-0 lines (used for energy sales)

  * - tariff_mills_kwh
    - number
    - Cost to transfer electricity between regions


.. _plants_steam_ferc1:

Contents of plants_steam_ferc1 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - record_id
    - string
    - Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.

  * - utility_id_ferc1
    - integer
    - FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.

  * - report_year
    - year
    - Four-digit year in which the data was reported.

  * - plant_id_ferc1
    - integer
    - Algorithmically assigned PUDL FERC Plant ID. WARNING: NOT STABLE BETWEEN PUDL DB INITIALIZATIONS.

  * - plant_name_ferc1
    - string
    - Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.

  * - plant_type
    - string
    - Simplified plant type, categorized by PUDL based on our best guess of what was intended based on freeform string reported to FERC. Unidentifiable types are null.

  * - construction_type
    - string
    - Type of plant construction ('outdoor', 'semioutdoor', or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.

  * - construction_year
    - year
    - Year the plant's oldest still operational unit was built.

  * - installation_year
    - year
    - Year the plant's most recently built unit was installed.

  * - capacity_mw
    - number
    - Total installed plant capacity in MW.

  * - peak_demand_mw
    - number
    - Net peak demand experienced by the plant in MW in report year.

  * - plant_hours_connected_while_generating
    - number
    - Total number hours the plant was generated and connected to load during report year.

  * - plant_capability_mw
    - number
    - Net continuous plant capability in MW

  * - water_limited_capacity_mw
    - number
    - Plant capacity in MW when limited by condenser water.

  * - not_water_limited_capacity_mw
    - number
    - Plant capacity in MW when not limited by condenser water.

  * - avg_num_employees
    - number
    - Average number of plant employees during report year.

  * - net_generation_mwh
    - number
    - Net generation (exclusive of plant use) in MWh during report year.

  * - capex_land
    - number
    - Capital expense for land and land rights.

  * - capex_structures
    - number
    - Capital expense for structures and improvements.

  * - capex_equipment
    - number
    - Capital expense for equipment.

  * - capex_total
    - number
    - Total capital expenses.

  * - capex_per_mw
    - number
    - Capital expenses per MW of installed plant capacity.

  * - opex_operations
    - number
    - Production expenses: operations, supervision, and engineering.

  * - opex_fuel
    - number
    - Total cost of fuel.

  * - opex_coolants
    - number
    - Cost of coolants and water (nuclear plants only)

  * - opex_steam
    - number
    - Steam expenses.

  * - opex_steam_other
    - number
    - Steam from other sources.

  * - opex_transfer
    - number
    - Steam transferred (Credit).

  * - opex_electric
    - number
    - Electricity expenses.

  * - opex_misc_power
    - number
    - Miscellaneous steam (or nuclear) expenses.

  * - opex_rents
    - number
    - Rents.

  * - opex_allowances
    - number
    - Allowances.

  * - opex_engineering
    - number
    - Maintenance, supervision, and engineering.

  * - opex_structures
    - number
    - Maintenance of structures.

  * - opex_boiler
    - number
    - Maintenance of boiler (or reactor) plant.

  * - opex_plants
    - number
    - Maintenance of electrical plant.

  * - opex_misc_steam
    - number
    - Maintenance of miscellaneous steam (or nuclear) plant.

  * - opex_production_total
    - number
    - Total operating epxenses.

  * - opex_per_mwh
    - number
    - Total operating expenses per MWh of net generation.

  * - asset_retirement_cost
    - number
    - Asset retirement cost.


.. _utilities_eia860:

Contents of utilities_eia860 table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - utility_id_eia
    - integer
    - EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.

  * - report_date
    - date
    - Date reported.

  * - street_address
    - string
    - Street address of the operator/owner

  * - city
    - string
    - Name of the city in which operator/owner is located

  * - state
    - string
    - State of the operator/owner

  * - zip_code
    - string
    - Zip code of the operator/owner

  * - plants_reported_owner
    - string
    - Is the reporting entity an owner of power plants reported on Schedule 2 of the form?

  * - plants_reported_operator
    - string
    - Is the reporting entity an operator of power plants reported on Schedule 2 of the form?

  * - plants_reported_asset_manager
    - string
    - Is the reporting entity an asset manager of power plants reported on Schedule 2 of the form?

  * - plants_reported_other_relationship
    - string
    - Does the reporting entity have any other relationship to the power plants reported on Schedule 2 of the form?

  * - entity_type
    - string
    - Entity type of principle owner (C = Cooperative, I = Investor-Owned Utility, Q = Independent Power Producer, M = Municipally-Owned Utility, P = Political Subdivision, F = Federally-Owned Utility, S = State-Owned Utility, IND = Industrial, COM = Commercial

  * - attention_line
    - string
    - N/A

  * - address_2
    - string
    - N/A

  * - address_3
    - string
    - N/A

  * - zip_code_4
    - string
    - N/A

  * - contact_firstname
    - string
    - N/A

  * - contact_lastname
    - string
    - N/A

  * - contact_title
    - string
    - N/A

  * - contact_firstname_2
    - string
    - N/A

  * - contact_lastname_2
    - string
    - N/A

  * - contact_title_2
    - string
    - N/A

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


.. _boilers_entity_eia:

Contents of boilers_entity_eia table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - boiler_id
    - string
    - The EIA-assigned boiler identification code. Alphanumeric.

  * - prime_mover_code
    - string
    - Code for the type of prime mover (e.g. CT, CG)


.. _plants_entity_eia:

Contents of plants_entity_eia table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - plant_name_eia
    - string
    - Plant name.

  * - balancing_authority_code_eia
    - string
    - The plant's balancing authority code.

  * - balancing_authority_name_eia
    - string
    - The plant's balancing authority name.

  * - city
    - string
    - The plant's city.

  * - county
    - string
    - The plant's county.

  * - ferc_cogen_status
    - string
    - Indicates whether the plant has FERC qualifying facility cogenerator status.

  * - ferc_exempt_wholesale_generator
    - string
    - Indicates whether the plant has FERC qualifying facility exempt wholesale generator status

  * - ferc_small_power_producer
    - string
    - Indicates whether the plant has FERC qualifying facility small power producer status

  * - grid_voltage_kv
    - number
    - Plant's grid voltage at point of interconnection to transmission or distibution facilities

  * - grid_voltage_2_kv
    - number
    - Plant's grid voltage at point of interconnection to transmission or distibution facilities

  * - grid_voltage_3_kv
    - number
    - Plant's grid voltage at point of interconnection to transmission or distibution facilities

  * - iso_rto_code
    - string
    - The code of the plant's ISO or RTO. NA if not reported in that year.

  * - latitude
    - number
    - Latitude of the plant's location, in degrees.

  * - longitude
    - number
    - Longitude of the plant's location, in degrees.

  * - primary_purpose_naics_id
    - number
    - North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant

  * - sector_name
    - string
    - Plant-level sector name, designated by the primary purpose, regulatory status and plant-level combined heat and power status

  * - sector_id
    - number
    - Plant-level sector number, designated by the primary purpose, regulatory status and plant-level combined heat and power status

  * - service_area
    - string
    - Service area in which plant is located; for unregulated companies, it's the electric utility with which plant is interconnected

  * - state
    - string
    - Plant state. Two letter US state and territory abbreviations.

  * - street_address
    - string
    - Plant street address

  * - zip_code
    - string
    - Plant street address

  * - timezone
    - string
    - IANA timezone name


.. _transmission_joint_epaipm:

Contents of transmission_joint_epaipm table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - joint_constraint_id
    - integer
    - Identification of groups that make up a single joint constraint

  * - region_from
    - string
    - Name of the IPM region sending electricity

  * - region_to
    - string
    - Name of the IPM region receiving electricity

  * - firm_ttc_mw
    - number
    - Transfer capacity with N-1 lines (used for reserve margins)

  * - nonfirm_ttc_mw
    - number
    - Transfer capacity with N-0 lines (used for energy sales)


.. _plant_unit_epa:

Contents of plant_unit_epa table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_epa
    - integer
    - N/A

  * - unit_id_epa
    - string
    - Smokestack' unit monitored by EPA CEMS.


.. _assn_plant_id_eia_epa:

Contents of assn_plant_id_eia_epa table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_epa
    - integer
    - N/A

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.


.. _assn_gen_eia_unit_epa:

Contents of assn_gen_eia_unit_epa table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**

  * - plant_id_eia
    - integer
    - The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.

  * - unit_id_epa
    - string
    - Smokestack' unit monitored by EPA CEMS.

  * - generator_id
    - string
    - Generator identification code. Often numeric, but sometimes includes letters. It's a string!
