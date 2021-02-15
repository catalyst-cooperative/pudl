"""Field metadata."""
from typing import Any, Dict, List

FIELD_LIST: List[Dict[str, Any]] = [
    {
        "name": "abbr",
        "type": "string"
    },
    {
        "name": "active",
        "type": "boolean",
        "description": "Indicates whether or not the dataset has been pulled into PUDL by the extract transform load process."
    },
    {
        "name": "amount_type",
        "type": "string",
        "description": "String indicating which original FERC Form 1 column the listed amount came from. Each field should have one (potentially NA) value of each type for each utility in each year, and the ending_balance should equal the sum of starting_balance, additions, retirements, adjustments, and transfers.",
        "constraints": {
            "enum": [
                "starting_balance",
                "retirements",
                "transfers",
                "adjustments",
                "ending_balance",
                "additions"
            ]
        }
    },
    {
        "name": "ash_content_pct",
        "type": "number",
        "description": "Ash content percentage by weight to the nearest 0.1 percent."
    },
    {
        "name": "ash_impoundment",
        "type": "string",
        "description": "Is there an ash impoundment (e.g. pond, reservoir) at the plant?"
    },
    {
        "name": "ash_impoundment_lined",
        "type": "string",
        "description": "If there is an ash impoundment at the plant, is the impoundment lined?"
    },
    {
        "name": "ash_impoundment_status",
        "type": "string",
        "description": "If there is an ash impoundment at the plant, the ash impoundment status as of December 31 of the reporting year."
    },
    {
        "name": "asset_retirement_cost",
        "type": "number",
        "description": "Asset retirement cost (USD)."
    },
    {
        "name": "associated_combined_heat_power",
        "type": "boolean",
        "description": "Indicates whether the generator is associated with a combined heat and power system"
    },
    {
        "name": "balancing_authority_code_eia",
        "type": "string",
        "description": "The plant's balancing authority code."
    },
    {
        "name": "balancing_authority_name_eia",
        "type": "string",
        "description": "The plant's balancing authority name."
    },
    {
        "name": "bga_source",
        "type": "string",
        "description": "The source from where the unit_id_pudl is compiled. The unit_id_pudl comes directly from EIA 860, or string association (which looks at all the boilers and generators that are not associated with a unit and tries to find a matching string in the respective collection of boilers or generator), or from a unit connection (where the unit_id_eia is employed to find additional boiler generator connections)."
    },
    {
        "name": "billing_demand_mw",
        "type": "number",
        "description": "Monthly average billing demand (for requirements purchases, and any transactions involving demand charges). In megawatts."
    },
    {
        "name": "bypass_heat_recovery",
        "type": "boolean",
        "description": "Can this generator operate while bypassing the heat recovery steam generator?"
    },
    {
        "name": "capex_equipment",
        "type": "number",
        "description": "Cost of plant: equipment (USD)."
    },
    {
        "name": "capex_equipment_electric",
        "type": "number",
        "description": "Cost of plant: accessory electric equipment (USD)."
    },
    {
        "name": "capex_equipment_misc",
        "type": "number",
        "description": "Cost of plant: miscellaneous power plant equipment (USD)."
    },
    {
        "name": "capex_facilities",
        "type": "number",
        "description": "Cost of plant: reservoirs, dams, and waterways (USD)."
    },
    {
        "name": "capex_land",
        "type": "number",
        "description": "Cost of plant: land and land rights (USD)."
    },
    {
        "name": "capex_roads",
        "type": "number",
        "description": "Cost of plant: roads, railroads, and bridges (USD)."
    },
    {
        "name": "capex_structures",
        "type": "number",
        "description": "Cost of plant: structures and improvements (USD)."
    },
    {
        "name": "capex_total",
        "type": "number",
        "description": "Total cost of plant (USD)."
    },
    {
        "name": "capex_wheels_turbines_generators",
        "type": "number",
        "description": "Cost of plant: water wheels, turbines, and generators (USD)."
    },
    {
        "name": "carbon_capture",
        "type": "boolean",
        "description": "Indicates whether the generator uses carbon capture technology."
    },
    {
        "name": "chlorine_content_ppm",
        "type": "number"
    },
    {
        "name": "co2_mass_measurement_code",
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": [
                "",
                "Measured and Substitute",
                "Measured",
                "Unknown Code",
                "Undetermined",
                "Substitute",
                "LME",
                "Other"
            ]
        }
    },
    {
        "name": "co2_mass_tons",
        "type": "number",
        "description": "Carbon dioxide emissions in short tons."
    },
    {
        "name": "cofire_fuels",
        "type": "boolean",
        "description": "Can the generator co-fire fuels?."
    },
    {
        "name": "coincident_peak_demand_mw",
        "type": "number",
        "description": "Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier's system reaches its monthly peak. In megawatts."
    },
    {
        "name": "construction_type",
        "type": "string",
        "description": "Type of plant construction ('outdoor', 'semioutdoor', or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.",
        "constraints": {
            "enum": [
                "unknown",
                "conventional",
                "outdoor",
                "semioutdoor"
            ]
        }
    },
    {
        "name": "contract_expiration_date",
        "type": "date",
        "description": "Date contract expires.Format:  MMYY."
    },
    {
        "name": "contract_type_code",
        "type": "string",
        "description": "Purchase type under which receipts occurred in the reporting month. C: Contract, NC: New Contract, S: Spot Purchase, T: Tolling Agreement.",
        "constraints": {
            "enum": [
                "S",
                "C",
                "NC",
                "T"
            ]
        }
    },
    {
        "name": "county",
        "type": "string",
        "description": "The plant's county."
    },
    {
        "name": "county_id_fips",
        "type": "integer",
        "description": "County ID from the Federal Information Processing Standard Publication 6-4."
    },
    {
        "name": "current_planned_operating_date",
        "type": "date",
        "description": "The most recently updated effective date on which the generator is scheduled to start operation"
    },
    {
        "name": "datasource",
        "type": "string",
        "description": "Code identifying a dataset available within PUDL.",
        "constraints": {
            "enum": [
                "epaipm",
                "eia923",
                "ferc1",
                "epacems",
                "eia860"
            ]
        }
    },
    {
        "name": "day_of_year",
        "type": "integer",
        "description": "Day of the year"
    },
    {
        "name": "deliver_power_transgrid",
        "type": "boolean",
        "description": "Indicate whether the generator can deliver power to the transmission grid."
    },
    {
        "name": "delivered_mwh",
        "type": "number",
        "description": "Gross megawatt-hours delivered in power exchanges and used as the basis for settlement."
    },
    {
        "name": "demand_charges",
        "type": "number",
        "description": "Demand charges (USD)."
    },
    {
        "name": "distribution_acct360_land",
        "type": "number",
        "description": "FERC Account 360: Distribution Plant Land and Land Rights."
    },
    {
        "name": "distribution_acct361_structures",
        "type": "number",
        "description": "FERC Account 361: Distribution Plant Structures and Improvements."
    },
    {
        "name": "distribution_acct362_station_equip",
        "type": "number",
        "description": "FERC Account 362: Distribution Plant Station Equipment."
    },
    {
        "name": "distribution_acct363_storage_battery_equip",
        "type": "number",
        "description": "FERC Account 363: Distribution Plant Storage Battery Equipment."
    },
    {
        "name": "distribution_acct364_poles_towers",
        "type": "number",
        "description": "FERC Account 364: Distribution Plant Poles, Towers, and Fixtures."
    },
    {
        "name": "distribution_acct365_overhead_conductors",
        "type": "number",
        "description": "FERC Account 365: Distribution Plant Overhead Conductors and Devices."
    },
    {
        "name": "distribution_acct366_underground_conduit",
        "type": "number",
        "description": "FERC Account 366: Distribution Plant Underground Conduit."
    },
    {
        "name": "distribution_acct367_underground_conductors",
        "type": "number",
        "description": "FERC Account 367: Distribution Plant Underground Conductors and Devices."
    },
    {
        "name": "distribution_acct368_line_transformers",
        "type": "number",
        "description": "FERC Account 368: Distribution Plant Line Transformers."
    },
    {
        "name": "distribution_acct369_services",
        "type": "number",
        "description": "FERC Account 369: Distribution Plant Services."
    },
    {
        "name": "distribution_acct370_meters",
        "type": "number",
        "description": "FERC Account 370: Distribution Plant Meters."
    },
    {
        "name": "distribution_acct371_customer_installations",
        "type": "number",
        "description": "FERC Account 371: Distribution Plant Installations on Customer Premises."
    },
    {
        "name": "distribution_acct372_leased_property",
        "type": "number",
        "description": "FERC Account 372: Distribution Plant Leased Property on Customer Premises."
    },
    {
        "name": "distribution_acct373_street_lighting",
        "type": "number",
        "description": "FERC Account 373: Distribution PLant Street Lighting and Signal Systems."
    },
    {
        "name": "distribution_acct374_asset_retirement",
        "type": "number",
        "description": "FERC Account 374: Distribution Plant Asset Retirement Costs."
    },
    {
        "name": "distribution_total",
        "type": "number",
        "description": "Distribution Plant Total (FERC Accounts 360-374)."
    },
    {
        "name": "duct_burners",
        "type": "boolean",
        "description": "Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas"
    },
    {
        "name": "electric_plant",
        "type": "number",
        "description": "Electric Plant In Service (USD)."
    },
    {
        "name": "electric_plant_in_service_total",
        "type": "number",
        "description": "Total Electric Plant in Service (FERC Accounts 101, 102, 103 and 106)"
    },
    {
        "name": "electric_plant_purchased_acct102",
        "type": "number",
        "description": "FERC Account 102: Electric Plant Purchased."
    },
    {
        "name": "electric_plant_sold_acct102",
        "type": "number",
        "description": "FERC Account 102: Electric Plant Sold (Negative)."
    },
    {
        "name": "energy_charges",
        "type": "number",
        "description": "Energy charges (USD)."
    },
    {
        "name": "energy_source_code",
        "type": "string",
        "description": "The fuel code associated with the fuel receipt. Two or three character alphanumeric."
    },
    {
        "name": "energy_source_code_1",
        "type": "string",
        "description": "The code representing the most predominant type of energy that fuels the generator."
    },
    {
        "name": "energy_source_code_2",
        "type": "string",
        "description": "The code representing the second most predominant type of energy that fuels the generator"
    },
    {
        "name": "energy_source_code_3",
        "type": "string",
        "description": "The code representing the third most predominant type of energy that fuels the generator"
    },
    {
        "name": "energy_source_code_4",
        "type": "string",
        "description": "The code representing the fourth most predominant type of energy that fuels the generator"
    },
    {
        "name": "energy_source_code_5",
        "type": "string",
        "description": "The code representing the fifth most predominant type of energy that fuels the generator"
    },
    {
        "name": "energy_source_code_6",
        "type": "string",
        "description": "The code representing the sixth most predominant type of energy that fuels the generator"
    },
    {
        "name": "energy_storage",
        "type": "string",
        "description": "Indicates if the facility has energy storage capabilities."
    },
    {
        "name": "energy_used_for_pumping_mwh",
        "type": "number",
        "description": "Energy used for pumping, in megawatt-hours."
    },
    {
        "name": "entity_type",
        "type": "string",
        "description": "Entity type of principle owner (C = Cooperative, I = Investor-Owned Utility, Q = Independent Power Producer, M = Municipally-Owned Utility, P = Political Subdivision, F = Federally-Owned Utility, S = State-Owned Utility, IND = Industrial, COM = Commercial"
    },
    {
        "name": "experimental_plant_acct103",
        "type": "number",
        "description": "FERC Account 103: Experimental Plant Unclassified."
    },
    {
        "name": "facility_id",
        "type": "integer",
        "description": "New EPA plant ID."
    },
    {
        "name": "ferc_account_id",
        "type": "string",
        "description": "Account number, from FERC's Uniform System of Accounts for Electric Plant. Also includes higher level labeled categories."
    },
    {
        "name": "ferc_cogen_docket_no",
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility cogenerator status."
    },
    {
        "name": "ferc_cogen_status",
        "type": "string",
        "description": "Indicates whether the plant has FERC qualifying facility cogenerator status."
    },
    {
        "name": "ferc_exempt_wholesale_generator",
        "type": "string",
        "description": "Indicates whether the plant has FERC qualifying facility exempt wholesale generator status"
    },
    {
        "name": "ferc_exempt_wholesale_generator_docket_no",
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility exempt wholesale generator status."
    },
    {
        "name": "ferc_license_id",
        "type": "integer",
        "description": "FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible."
    },
    {
        "name": "ferc_small_power_producer",
        "type": "string",
        "description": "Indicates whether the plant has FERC qualifying facility small power producer status"
    },
    {
        "name": "ferc_small_power_producer_docket_no",
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility small power producer status."
    },
    {
        "name": "firm_ttc_mw",
        "type": "number",
        "description": "Transfer capacity with N-1 lines (used for reserve margins)"
    },
    {
        "name": "fluidized_bed_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses fluidized bed technology"
    },
    {
        "name": "fraction_owned",
        "type": "number",
        "description": "Proportion of generator ownership."
    },
    {
        "name": "fuel_consumed_for_electricity_mmbtu",
        "type": "number",
        "description": "Total consumption of fuel to produce electricity, in physical units, year to date."
    },
    {
        "name": "fuel_consumed_for_electricity_units",
        "type": "number",
        "description": "Consumption for electric generation of the fuel type in physical units."
    },
    {
        "name": "fuel_consumed_mmbtu",
        "type": "number",
        "description": "Total consumption of fuel in physical units, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production."
    },
    {
        "name": "fuel_consumed_units",
        "type": "number",
        "description": "Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production."
    },
    {
        "name": "fuel_cost_per_unit_burned",
        "type": "number",
        "description": "Average cost of fuel consumed in the report year per reported fuel unit (USD)."
    },
    {
        "name": "fuel_cost_per_unit_delivered",
        "type": "number",
        "description": "Average cost of fuel delivered in the report year per reported fuel unit (USD)."
    },
    {
        "name": "fuel_group_code",
        "type": "string",
        "description": "Groups the energy sources into fuel groups that are located in the Electric Power Monthly:  Coal, Natural Gas, Petroleum, Petroleum Coke.",
        "constraints": {
            "enum": [
                "petroleum",
                "other_gas",
                "petroleum_coke",
                "natural_gas",
                "coal"
            ]
        }
    },
    {
        "name": "fuel_group_code_simple",
        "type": "string",
        "description": "Simplified grouping of fuel_group_code, with Coal and Petroluem Coke as well as Natural Gas and Other Gas grouped together."
    },
    {
        "name": "fuel_qty_burned",
        "type": "number",
        "description": "Quantity of fuel consumed in the report year, in terms of the reported fuel units."
    },
    {
        "name": "fuel_qty_units",
        "type": "number",
        "description": "Quanity of fuel received in tons, barrel, or Mcf."
    },
    {
        "name": "fuel_type_code",
        "type": "string",
        "description": "The fuel code reported to EIA. Two or three letter alphanumeric."
    },
    {
        "name": "fuel_type_code_aer",
        "type": "string",
        "description": "A partial aggregation of the reported fuel type codes into larger categories used by EIA in, for example, the Annual Energy Review (AER).Two or three letter alphanumeric."
    },
    {
        "name": "fuel_unit",
        "type": "string",
        "description": "PUDL assigned code indicating reported fuel unit of measure.",
        "constraints": {
            "enum": [
                "unknown",
                "mmbtu",
                "gramsU",
                "kgU",
                "mwhth",
                "kgal",
                "bbl",
                "klbs",
                "mcf",
                "gal",
                "mwdth",
                "btu",
                "ton"
            ]
        }
    },
    {
        "name": "future_plant",
        "type": "number",
        "description": "Electric Plant Held for Future Use (USD)."
    },
    {
        "name": "general_acct389_land",
        "type": "number",
        "description": "FERC Account 389: General Land and Land Rights."
    },
    {
        "name": "general_acct390_structures",
        "type": "number",
        "description": "FERC Account 390: General Structures and Improvements."
    },
    {
        "name": "general_acct391_office_equip",
        "type": "number",
        "description": "FERC Account 391: General Office Furniture and Equipment."
    },
    {
        "name": "general_acct392_transportation_equip",
        "type": "number",
        "description": "FERC Account 392: General Transportation Equipment."
    },
    {
        "name": "general_acct393_stores_equip",
        "type": "number",
        "description": "FERC Account 393: General Stores Equipment."
    },
    {
        "name": "general_acct394_shop_equip",
        "type": "number",
        "description": "FERC Account 394: General Tools, Shop, and Garage Equipment."
    },
    {
        "name": "general_acct395_lab_equip",
        "type": "number",
        "description": "FERC Account 395: General Laboratory Equipment."
    },
    {
        "name": "general_acct396_power_operated_equip",
        "type": "number",
        "description": "FERC Account 396: General Power Operated Equipment."
    },
    {
        "name": "general_acct397_communication_equip",
        "type": "number",
        "description": "FERC Account 397: General Communication Equipment."
    },
    {
        "name": "general_acct398_misc_equip",
        "type": "number",
        "description": "FERC Account 398: General Miscellaneous Equipment."
    },
    {
        "name": "general_acct399_1_asset_retirement",
        "type": "number",
        "description": "FERC Account 399.1: Asset Retirement Costs for General Plant."
    },
    {
        "name": "general_acct399_other_property",
        "type": "number",
        "description": "FERC Account 399: General Plant Other Tangible Property."
    },
    {
        "name": "general_subtotal",
        "type": "number",
        "description": "General Plant Subtotal (FERC Accounts 389-398)."
    },
    {
        "name": "general_total",
        "type": "number",
        "description": "General Plant Total (FERC Accounts 389-399.1)."
    },
    {
        "name": "grid_voltage_2_kv",
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities"
    },
    {
        "name": "grid_voltage_3_kv",
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities"
    },
    {
        "name": "grid_voltage_kv",
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities"
    },
    {
        "name": "gross_load_mw",
        "type": "number",
        "description": "Average power in megawatts delivered during time interval measured."
    },
    {
        "name": "heat_content_mmbtu",
        "type": "number",
        "description": "The energy contained in fuel burned, measured in million BTU."
    },
    {
        "name": "heat_content_mmbtu_per_unit",
        "type": "number",
        "description": "Heat content of the fuel in millions of Btus per physical unit to the nearest 0.01 percent."
    },
    {
        "name": "hour",
        "type": "integer",
        "description": "Hour of the day (0-23). Original IPM values were 1-24."
    },
    {
        "name": "hydro_acct330_land",
        "type": "number",
        "description": "FERC Account 330: Hydro Land and Land Rights."
    },
    {
        "name": "hydro_acct331_structures",
        "type": "number",
        "description": "FERC Account 331: Hydro Structures and Improvements."
    },
    {
        "name": "hydro_acct332_reservoirs_dams_waterways",
        "type": "number",
        "description": "FERC Account 332: Hydro Reservoirs, Dams, and Waterways."
    },
    {
        "name": "hydro_acct333_wheels_turbines_generators",
        "type": "number",
        "description": "FERC Account 333: Hydro Water Wheels, Turbins, and Generators."
    },
    {
        "name": "hydro_acct334_accessory_equip",
        "type": "number",
        "description": "FERC Account 334: Hydro Accessory Electric Equipment."
    },
    {
        "name": "hydro_acct335_misc_equip",
        "type": "number",
        "description": "FERC Account 335: Hydro Miscellaneous Power Plant Equipment."
    },
    {
        "name": "hydro_acct336_roads_railroads_bridges",
        "type": "number",
        "description": "FERC Account 336: Hydro Roads, Railroads, and Bridges."
    },
    {
        "name": "hydro_acct337_asset_retirement",
        "type": "number",
        "description": "FERC Account 337: Asset Retirement Costs for Hydraulic Production."
    },
    {
        "name": "hydro_total",
        "type": "number",
        "description": "Hydraulic Production Plant Total (FERC Accounts 330-337)"
    },
    {
        "name": "id",
        "type": "integer",
        "description": "PUDL issued surrogate key."
    },
    {
        "name": "intangible_acct301_organization",
        "type": "number",
        "description": "FERC Account 301: Intangible Plant Organization."
    },
    {
        "name": "intangible_acct302_franchises_consents",
        "type": "number",
        "description": "FERC Account 302: Intangible Plant Franchises and Consents."
    },
    {
        "name": "intangible_acct303_misc",
        "type": "number",
        "description": "FERC Account 303: Miscellaneous Intangible Plant."
    },
    {
        "name": "intangible_total",
        "type": "number",
        "description": "Intangible Plant Total (FERC Accounts 301-303)."
    },
    {
        "name": "iso_rto_code",
        "type": "string",
        "description": "The code of the plant's ISO or RTO. NA if not reported in that year."
    },
    {
        "name": "joint_constraint_id",
        "type": "integer",
        "description": "Identification of groups that make up a single joint constraint"
    },
    {
        "name": "latitude",
        "type": "number",
        "description": "Latitude of the plant's location, in degrees."
    },
    {
        "name": "leased_plant",
        "type": "number",
        "description": "Electric Plant Leased to Others (USD)."
    },
    {
        "name": "liquefied_natural_gas_storage",
        "type": "string",
        "description": "Indicates if the facility have the capability to store the natural gas in the form of liquefied natural gas."
    },
    {
        "name": "load_mw",
        "type": "number",
        "description": "Load (MW) in an hour of the day for the IPM region"
    },
    {
        "name": "longitude",
        "type": "number",
        "description": "Longitude of the plant's location, in degrees."
    },
    {
        "name": "major_electric_plant_acct101_acct106_total",
        "type": "number",
        "description": "Total Major Electric Plant in Service (FERC Accounts 101 and 106)."
    },
    {
        "name": "mercury_content_ppm",
        "type": "number",
        "description": "Mercury content in parts per million (ppm) to the nearest 0.001 ppm."
    },
    {
        "name": "mine_id_msha",
        "type": "integer",
        "description": "MSHA issued mine identifier."
    },
    {
        "name": "mine_id_pudl",
        "type": "integer",
        "description": "PUDL issued mine identifier."
    },
    {
        "name": "mine_name",
        "type": "string",
        "description": "Coal mine name."
    },
    {
        "name": "mine_type_code",
        "type": "string",
        "description": "Type of mine. P: Preparation plant, U: Underground, S: Surface, SU: Mostly Surface with some Underground, US: Mostly Underground with some Surface.",
        "constraints": {
            "enum": [
                "US",
                "S",
                "U",
                "SU",
                "P"
            ]
        }
    },
    {
        "name": "minimum_load_mw",
        "type": "number",
        "description": "The minimum load at which the generator can operate at continuosuly."
    },
    {
        "name": "mode",
        "type": "string"
    },
    {
        "name": "moisture_content_pct",
        "type": "number"
    },
    {
        "name": "month",
        "type": "integer",
        "description": "Month of the year"
    },
    {
        "name": "multiple_fuels",
        "type": "boolean",
        "description": "Can the generator burn multiple fuels?"
    },
    {
        "name": "nameplate_power_factor",
        "type": "number",
        "description": "The nameplate power factor of the generator."
    },
    {
        "name": "natural_gas_delivery_contract_type_code",
        "type": "string",
        "description": "Contract type for natrual gas delivery service:",
        "constraints": {
            "enum": [
                "firm",
                "interruptible"
            ]
        }
    },
    {
        "name": "natural_gas_local_distribution_company",
        "type": "string",
        "description": "Names of Local Distribution Company (LDC), connected to natural gas burning power plants."
    },
    {
        "name": "natural_gas_pipeline_name_1",
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility."
    },
    {
        "name": "natural_gas_pipeline_name_2",
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility."
    },
    {
        "name": "natural_gas_pipeline_name_3",
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility."
    },
    {
        "name": "natural_gas_storage",
        "type": "string",
        "description": "Indicates if the facility have on-site storage of natural gas."
    },
    {
        "name": "natural_gas_transport_code",
        "type": "string",
        "description": "Contract type for natural gas transportation service.",
        "constraints": {
            "enum": [
                "firm",
                "interruptible"
            ]
        }
    },
    {
        "name": "nerc_region",
        "type": "string",
        "description": "NERC region in which the plant is located"
    },
    {
        "name": "net_capacity_adverse_conditions_mw",
        "type": "number",
        "description": "Net plant capability under the least favorable operating conditions, in megawatts."
    },
    {
        "name": "net_capacity_favorable_conditions_mw",
        "type": "number",
        "description": "Net plant capability under the most favorable operating conditions, in megawatts."
    },
    {
        "name": "net_load_mwh",
        "type": "number",
        "description": "Net output for load (net generation - energy used for pumping) in megawatt-hours."
    },
    {
        "name": "net_metering",
        "type": "string",
        "description": "Did this plant have a net metering agreement in effect during the reporting year?  (Only displayed for facilities that report the sun or wind as an energy source). This field was only reported up until 2015"
    },
    {
        "name": "non_coincident_peak_demand_mw",
        "type": "number",
        "description": "Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts."
    },
    {
        "name": "nonfirm_ttc_mw",
        "type": "number",
        "description": "Transfer capacity with N-0 lines (used for energy sales)"
    },
    {
        "name": "not_water_limited_capacity_mw",
        "type": "number",
        "description": "Plant capacity in MW when not limited by condenser water."
    },
    {
        "name": "nox_mass_lbs",
        "type": "number",
        "description": "NOx emissions in pounds."
    },
    {
        "name": "nox_mass_measurement_code",
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": [
                "",
                "Calculated",
                "Measured and Substitute",
                "Measured",
                "Unknown Code",
                "Undetermined",
                "Substitute",
                "LME",
                "Other",
                "Not Applicable"
            ]
        }
    },
    {
        "name": "nox_rate_lbs_mmbtu",
        "type": "number",
        "description": "The average rate at which NOx was emitted during a given time period."
    },
    {
        "name": "nox_rate_measurement_code",
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": [
                "",
                "Calculated",
                "Measured and Substitute",
                "Measured",
                "Unknown Code",
                "Undetermined",
                "Substitute",
                "LME",
                "Other",
                "Not Applicable"
            ]
        }
    },
    {
        "name": "nuclear_acct320_land",
        "type": "number",
        "description": "FERC Account 320: Nuclear Land and Land Rights."
    },
    {
        "name": "nuclear_acct321_structures",
        "type": "number",
        "description": "FERC Account 321: Nuclear Structures and Improvements."
    },
    {
        "name": "nuclear_acct322_reactor_equip",
        "type": "number",
        "description": "FERC Account 322: Nuclear Reactor Plant Equipment."
    },
    {
        "name": "nuclear_acct323_turbogenerators",
        "type": "number",
        "description": "FERC Account 323: Nuclear Turbogenerator Units"
    },
    {
        "name": "nuclear_acct324_accessory_equip",
        "type": "number",
        "description": "FERC Account 324: Nuclear Accessory Electric Equipment."
    },
    {
        "name": "nuclear_acct325_misc_equip",
        "type": "number",
        "description": "FERC Account 325: Nuclear Miscellaneous Power Plant Equipment."
    },
    {
        "name": "nuclear_acct326_asset_retirement",
        "type": "number",
        "description": "FERC Account 326: Asset Retirement Costs for Nuclear Production."
    },
    {
        "name": "nuclear_total",
        "type": "number",
        "description": "Total Nuclear Production Plant (FERC Accounts 320-326)"
    },
    {
        "name": "nuclear_unit_id",
        "type": "integer",
        "description": "For nuclear plants only, the unit number .One digit numeric. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level."
    },
    {
        "name": "operating_date",
        "type": "date",
        "description": "Date the generator began commercial operation"
    },
    {
        "name": "operating_datetime_utc",
        "type": "datetime",
        "description": "Date and time measurement began (UTC)."
    },
    {
        "name": "operating_switch",
        "type": "string",
        "description": "Indicates whether the fuel switching generator can switch when operating"
    },
    {
        "name": "operating_time_hours",
        "type": "number",
        "description": "Length of time interval measured."
    },
    {
        "name": "operational_status",
        "type": "string",
        "description": "The operating status of the generator. This is based on which tab the generator was listed in in EIA 860."
    },
    {
        "name": "operational_status_code",
        "type": "string",
        "description": "The operating status of the generator."
    },
    {
        "name": "opex_allowances",
        "type": "number",
        "description": "Allowances."
    },
    {
        "name": "opex_boiler",
        "type": "number",
        "description": "Maintenance of boiler (or reactor) plant."
    },
    {
        "name": "opex_coolants",
        "type": "number",
        "description": "Cost of coolants and water (nuclear plants only)"
    },
    {
        "name": "opex_dams",
        "type": "number",
        "description": "Production expenses: maintenance of reservoirs, dams, and waterways (USD)."
    },
    {
        "name": "opex_electric",
        "type": "number",
        "description": "Production expenses: electric expenses (USD)."
    },
    {
        "name": "opex_engineering",
        "type": "number",
        "description": "Production expenses: maintenance, supervision, and engineering (USD)."
    },
    {
        "name": "opex_fuel",
        "type": "number",
        "description": "Production expenses: fuel (USD)."
    },
    {
        "name": "opex_generation_misc",
        "type": "number",
        "description": "Production expenses: miscellaneous power generation expenses (USD)."
    },
    {
        "name": "opex_hydraulic",
        "type": "number",
        "description": "Production expenses: hydraulic expenses (USD)."
    },
    {
        "name": "opex_maintenance",
        "type": "number",
        "description": "Production expenses: Maintenance (USD)."
    },
    {
        "name": "opex_misc_plant",
        "type": "number",
        "description": "Production expenses: maintenance of miscellaneous hydraulic plant (USD)."
    },
    {
        "name": "opex_misc_power",
        "type": "number",
        "description": "Miscellaneous steam (or nuclear) expenses."
    },
    {
        "name": "opex_misc_steam",
        "type": "number",
        "description": "Maintenance of miscellaneous steam (or nuclear) plant."
    },
    {
        "name": "opex_operations",
        "type": "number",
        "description": "Production expenses: operations, supervision, and engineering (USD)."
    },
    {
        "name": "opex_per_mwh",
        "type": "number",
        "description": "Total production expenses (USD per MWh generated)."
    },
    {
        "name": "opex_plant",
        "type": "number",
        "description": "Production expenses: maintenance of electric plant (USD)."
    },
    {
        "name": "opex_plants",
        "type": "number",
        "description": "Maintenance of electrical plant."
    },
    {
        "name": "opex_production_before_pumping",
        "type": "number",
        "description": "Total production expenses before pumping (USD)."
    },
    {
        "name": "opex_production_total",
        "type": "number",
        "description": "Total operating epxenses."
    },
    {
        "name": "opex_pumped_storage",
        "type": "number",
        "description": "Production expenses: pumped storage (USD)."
    },
    {
        "name": "opex_pumping",
        "type": "number",
        "description": "Production expenses: We are here to PUMP YOU UP! (USD)."
    },
    {
        "name": "opex_rents",
        "type": "number",
        "description": "Production expenses: rents (USD)."
    },
    {
        "name": "opex_steam",
        "type": "number",
        "description": "Steam expenses."
    },
    {
        "name": "opex_steam_other",
        "type": "number",
        "description": "Steam from other sources."
    },
    {
        "name": "opex_structures",
        "type": "number",
        "description": "Production expenses: maintenance of structures (USD)."
    },
    {
        "name": "opex_total",
        "type": "number",
        "description": "Total production expenses, excluding fuel (USD)."
    },
    {
        "name": "opex_transfer",
        "type": "number",
        "description": "Steam transferred (Credit)."
    },
    {
        "name": "opex_water_for_power",
        "type": "number",
        "description": "Production expenses: water for power (USD)."
    },
    {
        "name": "original_planned_operating_date",
        "type": "date",
        "description": "The date the generator was originally scheduled to be operational"
    },
    {
        "name": "other_acct340_land",
        "type": "number",
        "description": "FERC Account 340: Other Land and Land Rights."
    },
    {
        "name": "other_acct341_structures",
        "type": "number",
        "description": "FERC Account 341: Other Structures and Improvements."
    },
    {
        "name": "other_acct342_fuel_accessories",
        "type": "number",
        "description": "FERC Account 342: Other Fuel Holders, Products, and Accessories."
    },
    {
        "name": "other_acct343_prime_movers",
        "type": "number",
        "description": "FERC Account 343: Other Prime Movers."
    },
    {
        "name": "other_acct344_generators",
        "type": "number",
        "description": "FERC Account 344: Other Generators."
    },
    {
        "name": "other_acct345_accessory_equip",
        "type": "number",
        "description": "FERC Account 345: Other Accessory Electric Equipment."
    },
    {
        "name": "other_acct346_misc_equip",
        "type": "number",
        "description": "FERC Account 346: Other Miscellaneous Power Plant Equipment."
    },
    {
        "name": "other_acct347_asset_retirement",
        "type": "number",
        "description": "FERC Account 347: Asset Retirement Costs for Other Production."
    },
    {
        "name": "other_charges",
        "type": "number",
        "description": "Other charges, including out-of-period adjustments (USD)."
    },
    {
        "name": "other_combustion_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses other combustion technologies"
    },
    {
        "name": "other_modifications_date",
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete."
    },
    {
        "name": "other_planned_modifications",
        "type": "boolean",
        "description": "Indicates whether there are there other modifications planned for the generator."
    },
    {
        "name": "other_total",
        "type": "number",
        "description": "Total Other Production Plant (FERC Accounts 340-347)."
    },
    {
        "name": "owner_city",
        "type": "string",
        "description": "City of owner."
    },
    {
        "name": "owner_name",
        "type": "string",
        "description": "Name of owner."
    },
    {
        "name": "owner_state",
        "type": "string",
        "description": "Two letter US & Canadian state and territory abbreviations.",
        "constraints": {
            "enum": [
                "NE",
                "LA",
                "NU",
                "WI",
                "YT",
                "IL",
                "GA",
                "FL",
                "GU",
                "IN",
                "NA",
                "VA",
                "SC",
                "AK",
                "WA",
                "NB",
                "HI",
                "AR",
                "ND",
                "MP",
                "ID",
                "ON",
                "DE",
                "MD",
                "MT",
                "NV",
                "TX",
                "NM",
                "SK",
                "KY",
                "WY",
                "OH",
                "CT",
                "SD",
                "NS",
                "ME",
                "MI",
                "AS",
                "IA",
                "TN",
                "UT",
                "AL",
                "KS",
                "AZ",
                "QC",
                "MA",
                "NL",
                "PA",
                "CO",
                "DC",
                "NJ",
                "CA",
                "BC",
                "MB",
                "AB",
                "NY",
                "VT",
                "PR",
                "OK",
                "VI",
                "PE",
                "RI",
                "OR",
                "NC",
                "NH",
                "NT",
                "WV",
                "MO",
                "MS",
                "CN",
                "MN"
            ]
        }
    },
    {
        "name": "owner_street_address",
        "type": "string",
        "description": "Steet address of owner."
    },
    {
        "name": "owner_utility_id_eia",
        "type": "integer",
        "description": "EIA-assigned owner's identification number."
    },
    {
        "name": "owner_zip_code",
        "type": "string",
        "description": "Zip code of owner."
    },
    {
        "name": "ownership_code",
        "type": "string",
        "description": "Identifies the ownership for each generator."
    },
    {
        "name": "pipeline_notes",
        "type": "string",
        "description": "Additional owner or operator of natural gas pipeline."
    },
    {
        "name": "planned_derate_date",
        "type": "date",
        "description": "Planned effective month that the generator is scheduled to enter operation after the derate modification."
    },
    {
        "name": "planned_energy_source_code_1",
        "type": "string",
        "description": "New energy source code for the planned repowered generator."
    },
    {
        "name": "planned_modifications",
        "type": "boolean",
        "description": "Indicates whether there are any planned capacity uprates/derates, repowering, other modifications, or generator retirements scheduled for the next 5 years."
    },
    {
        "name": "planned_net_summer_capacity_derate_mw",
        "type": "number",
        "description": "Decrease in summer capacity expected to be realized from the derate modification to the equipment."
    },
    {
        "name": "planned_net_summer_capacity_uprate_mw",
        "type": "number",
        "description": "Increase in summer capacity expected to be realized from the modification to the equipment."
    },
    {
        "name": "planned_net_winter_capacity_derate_mw",
        "type": "number",
        "description": "Decrease in winter capacity expected to be realized from the derate modification to the equipment."
    },
    {
        "name": "planned_net_winter_capacity_uprate_mw",
        "type": "number",
        "description": "Increase in winter capacity expected to be realized from the uprate modification to the equipment."
    },
    {
        "name": "planned_new_capacity_mw",
        "type": "number",
        "description": "The expected new namplate capacity for the generator."
    },
    {
        "name": "planned_new_prime_mover_code",
        "type": "string",
        "description": "New prime mover for the planned repowered generator."
    },
    {
        "name": "planned_repower_date",
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the repowering is complete."
    },
    {
        "name": "planned_retirement_date",
        "type": "date",
        "description": "Planned effective date of the scheduled retirement of the generator."
    },
    {
        "name": "planned_uprate_date",
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the uprate modification."
    },
    {
        "name": "plant_id_eia",
        "type": "integer",
        "description": "The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration."
    },
    {
        "name": "plant_id_ferc1",
        "type": "integer",
        "description": "Algorithmically assigned PUDL FERC Plant ID. WARNING: NOT STABLE BETWEEN PUDL DB INITIALIZATIONS."
    },
    {
        "name": "plant_id_pudl",
        "type": "integer",
        "description": "A manually assigned PUDL plant ID. May not be constant over time."
    },
    {
        "name": "plant_name_eia",
        "type": "string",
        "description": "Plant name."
    },
    {
        "name": "plant_name_original",
        "type": "string",
        "description": "Original plant name in the FERC Form 1 FoxPro database."
    },
    {
        "name": "plant_name_pudl",
        "type": "string",
        "description": "Plant name, chosen arbitrarily from the several possible plant names available in the plant matching process. Included for human readability only."
    },
    {
        "name": "plants_reported_asset_manager",
        "type": "string",
        "description": "Is the reporting entity an asset manager of power plants reported on Schedule 2 of the form?"
    },
    {
        "name": "plants_reported_operator",
        "type": "string",
        "description": "Is the reporting entity an operator of power plants reported on Schedule 2 of the form?"
    },
    {
        "name": "plants_reported_other_relationship",
        "type": "string",
        "description": "Does the reporting entity have any other relationship to the power plants reported on Schedule 2 of the form?"
    },
    {
        "name": "plants_reported_owner",
        "type": "string",
        "description": "Is the reporting entity an owner of power plants reported on Schedule 2 of the form?"
    },
    {
        "name": "previously_canceled",
        "type": "boolean",
        "description": "Indicates whether the generator was previously reported as indefinitely postponed or canceled"
    },
    {
        "name": "primary_purpose_naics_id",
        "type": "number",
        "description": "North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant"
    },
    {
        "name": "primary_transportation_mode_code",
        "type": "string",
        "description": "Transportation mode for the longest distance transported."
    },
    {
        "name": "prime_mover",
        "type": "string"
    },
    {
        "name": "production_total",
        "type": "number",
        "description": "Total Production Plant (FERC Accounts 310-347)."
    },
    {
        "name": "project_num",
        "type": "integer",
        "description": "FERC Licensed Project Number."
    },
    {
        "name": "pulverized_coal_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses pulverized coal technology"
    },
    {
        "name": "purchase_type",
        "type": "string",
        "description": "Categorization based on the original contractual terms and conditions of the service. Must be one of 'requirements', 'long_firm', 'intermediate_firm', 'short_firm', 'long_unit', 'intermediate_unit', 'electricity_exchange', 'other_service', or 'adjustment'. Requirements service is ongoing high reliability service, with load integrated into system resource planning. 'Long term' means 5+ years. 'Intermediate term' is 1-5 years. 'Short term' is less than 1 year. 'Firm' means not interruptible for economic reasons. 'unit' indicates service from a particular designated generating unit. 'exchange' is an in-kind transaction.",
        "constraints": {
            "enum": [
                "intermediate_unit",
                "requirement",
                "other_service",
                "electricity_exchange",
                "long_unit",
                "adjustment",
                "long_firm",
                "intermediate_firm",
                "short_firm"
            ]
        }
    },
    {
        "name": "purchased_mwh",
        "type": "number",
        "description": "Megawatt-hours shown on bills rendered to the respondent."
    },
    {
        "name": "received_mwh",
        "type": "number",
        "description": "Gross megawatt-hours received in power exchanges and used as the basis for settlement."
    },
    {
        "name": "record_id",
        "type": "string",
        "description": "Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped."  # noqa: FS003
    },
    {
        "name": "region",
        "type": "string",
        "description": "Name of the IPM region"
    },
    {
        "name": "region_from",
        "type": "string",
        "description": "Name of the IPM region sending electricity"
    },
    {
        "name": "region_id_epaipm",
        "type": "string",
        "description": "Name of the IPM region"
    },
    {
        "name": "region_to",
        "type": "string",
        "description": "Name of the IPM region receiving electricity"
    },
    {
        "name": "regulatory_status_code",
        "type": "string",
        "description": "Indicates whether the plant is regulated or non-regulated."
    },
    {
        "name": "report_date",
        "type": "date",
        "description": "Date reported."
    },
    {
        "name": "report_year",
        "type": "year",
        "description": "Four-digit year in which the data was reported."
    },
    {
        "name": "retirement_date",
        "type": "date",
        "description": "Date of the scheduled or effected retirement of the generator."
    },
    {
        "name": "rtmo_acct380_land",
        "type": "number",
        "description": "FERC Account 380: RTMO Land and Land Rights."
    },
    {
        "name": "rtmo_acct381_structures",
        "type": "number",
        "description": "FERC Account 381: RTMO Structures and Improvements."
    },
    {
        "name": "rtmo_acct382_computer_hardware",
        "type": "number",
        "description": "FERC Account 382: RTMO Computer Hardware."
    },
    {
        "name": "rtmo_acct383_computer_software",
        "type": "number",
        "description": "FERC Account 383: RTMO Computer Software."
    },
    {
        "name": "rtmo_acct384_communication_equip",
        "type": "number",
        "description": "FERC Account 384: RTMO Communication Equipment."
    },
    {
        "name": "rtmo_acct385_misc_equip",
        "type": "number",
        "description": "FERC Account 385: RTMO Miscellaneous Equipment."
    },
    {
        "name": "rtmo_total",
        "type": "number",
        "description": "Total RTMO Plant (FERC Accounts 380-386)"
    },
    {
        "name": "rto_iso_lmp_node_id",
        "type": "string",
        "description": "The designation used to identify the price node in RTO/ISO Locational Marginal Price reports"
    },
    {
        "name": "rto_iso_location_wholesale_reporting_id",
        "type": "string",
        "description": "The designation used to report ths specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report"
    },
    {
        "name": "secondary_transportation_mode_code",
        "type": "string",
        "description": "Transportation mode for the second longest distance transported."
    },
    {
        "name": "sector_id",
        "type": "number",
        "description": "Plant-level sector number, designated by the primary purpose, regulatory status and plant-level combined heat and power status"
    },
    {
        "name": "sector_name",
        "type": "string",
        "description": "Plant-level sector name, designated by the primary purpose, regulatory status and plant-level combined heat and power status"
    },
    {
        "name": "seller_name",
        "type": "string",
        "description": "Name of the seller, or the other party in an exchange transaction."
    },
    {
        "name": "so2_mass_lbs",
        "type": "number",
        "description": "Sulfur dioxide emissions in pounds."
    },
    {
        "name": "so2_mass_measurement_code",
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": [
                "",
                "Measured and Substitute",
                "Measured",
                "Unknown Code",
                "Undetermined",
                "Substitute",
                "LME",
                "Other"
            ]
        }
    },
    {
        "name": "solid_fuel_gasification",
        "type": "boolean",
        "description": "Indicates whether the generator is part of a solid fuel gasification system"
    },
    {
        "name": "source",
        "type": "string"
    },
    {
        "name": "startup_source_code_1",
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    {
        "name": "startup_source_code_2",
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    {
        "name": "startup_source_code_3",
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    {
        "name": "startup_source_code_4",
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    {
        "name": "status",
        "type": "string"
    },
    {
        "name": "steam_acct310_land",
        "type": "number",
        "description": "FERC Account 310: Steam Plant Land and Land Rights."
    },
    {
        "name": "steam_acct311_structures",
        "type": "number",
        "description": "FERC Account 311: Steam Plant Structures and Improvements."
    },
    {
        "name": "steam_acct312_boiler_equip",
        "type": "number",
        "description": "FERC Account 312: Steam Boiler Plant Equipment."
    },
    {
        "name": "steam_acct313_engines",
        "type": "number",
        "description": "FERC Account 313: Steam Engines and Engine-Driven Generators."
    },
    {
        "name": "steam_acct314_turbogenerators",
        "type": "number",
        "description": "FERC Account 314: Steam Turbogenerator Units."
    },
    {
        "name": "steam_acct315_accessory_equip",
        "type": "number",
        "description": "FERC Account 315: Steam Accessory Electric Equipment."
    },
    {
        "name": "steam_acct316_misc_equip",
        "type": "number",
        "description": "FERC Account 316: Steam Miscellaneous Power Plant Equipment."
    },
    {
        "name": "steam_acct317_asset_retirement",
        "type": "number",
        "description": "FERC Account 317: Asset Retirement Costs for Steam Production."
    },
    {
        "name": "steam_load_1000_lbs",
        "type": "number",
        "description": "Total steam pressure produced by a unit during the reported hour."
    },
    {
        "name": "steam_total",
        "type": "number",
        "description": "Total Steam Production Plant (FERC Accounts 310-317)."
    },
    {
        "name": "stoker_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses stoker technology"
    },
    {
        "name": "subcritical_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses subcritical technology"
    },
    {
        "name": "sulfur_content_pct",
        "type": "number",
        "description": "Sulfur content percentage by weight to the nearest 0.01 percent."
    },
    {
        "name": "summer_capacity_mw",
        "type": "number",
        "description": "The net summer capacity."
    },
    {
        "name": "summer_estimated_capability_mw",
        "type": "number",
        "description": "EIA estimated summer capacity (in MWh)."
    },
    {
        "name": "supercritical_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses supercritical technology"
    },
    {
        "name": "supplier_name",
        "type": "string",
        "description": "Company that sold the fuel to the plant or, in the case of Natural Gas, pipline owner."
    },
    {
        "name": "switch_oil_gas",
        "type": "boolean",
        "description": "Indicates whether the generator switch between oil and natural gas."
    },
    {
        "name": "syncronized_transmission_grid",
        "type": "boolean",
        "description": "Indicates whether standby generators (SB status) can be synchronized to the grid."
    },
    {
        "name": "tariff",
        "type": "string",
        "description": "FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)"
    },
    {
        "name": "tariff_mills_kwh",
        "type": "number",
        "description": "Cost to transfer electricity between regions"
    },
    {
        "name": "technology_description",
        "type": "string",
        "description": "High level description of the technology used by the generator to produce electricity."
    },
    {
        "name": "time_cold_shutdown_full_load_code",
        "type": "string",
        "description": "The minimum amount of time required to bring the unit to full load from shutdown."
    },
    {
        "name": "time_index",
        "type": "integer",
        "description": "8760 index hour of the year"
    },
    {
        "name": "timezone",
        "type": "string",
        "description": "IANA timezone name"
    },
    {
        "name": "topping_bottoming_code",
        "type": "string",
        "description": "If the generator is associated with a combined heat and power system, indicates whether the generator is part of a topping cycle or a bottoming cycle"
    },
    {
        "name": "total",
        "type": "number",
        "description": "Total of Electric Plant In Service, Electric Plant Held for Future Use, and Electric Plant Leased to Others (USD)."
    },
    {
        "name": "total_cost_of_plant",
        "type": "number",
        "description": "Total cost of plant (USD)."
    },
    {
        "name": "total_settlement",
        "type": "number",
        "description": "Sum of demand, energy, and other charges (USD). For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative."
    },
    {
        "name": "transmission_acct350_land",
        "type": "number",
        "description": "FERC Account 350: Transmission Land and Land Rights."
    },
    {
        "name": "transmission_acct352_structures",
        "type": "number",
        "description": "FERC Account 352: Transmission Structures and Improvements."
    },
    {
        "name": "transmission_acct353_station_equip",
        "type": "number",
        "description": "FERC Account 353: Transmission Station Equipment."
    },
    {
        "name": "transmission_acct354_towers",
        "type": "number",
        "description": "FERC Account 354: Transmission Towers and Fixtures."
    },
    {
        "name": "transmission_acct355_poles",
        "type": "number",
        "description": "FERC Account 355: Transmission Poles and Fixtures."
    },
    {
        "name": "transmission_acct356_overhead_conductors",
        "type": "number",
        "description": "FERC Account 356: Overhead Transmission Conductors and Devices."
    },
    {
        "name": "transmission_acct357_underground_conduit",
        "type": "number",
        "description": "FERC Account 357: Underground Transmission Conduit."
    },
    {
        "name": "transmission_acct358_underground_conductors",
        "type": "number",
        "description": "FERC Account 358: Underground Transmission Conductors."
    },
    {
        "name": "transmission_acct359_1_asset_retirement",
        "type": "number",
        "description": "FERC Account 359.1: Asset Retirement Costs for Transmission Plant."
    },
    {
        "name": "transmission_acct359_roads_trails",
        "type": "number",
        "description": "FERC Account 359: Transmission Roads and Trails."
    },
    {
        "name": "transmission_distribution_owner_id",
        "type": "string",
        "description": "EIA-assigned code for owner of transmission/distribution system to which the plant is interconnected."
    },
    {
        "name": "transmission_distribution_owner_name",
        "type": "string",
        "description": "Name of the owner of the transmission or distribution system to which the plant is interconnected."
    },
    {
        "name": "transmission_distribution_owner_state",
        "type": "string",
        "description": "State location for owner of transmission/distribution system to which the plant is interconnected."
    },
    {
        "name": "transmission_total",
        "type": "number",
        "description": "Total Transmission Plant (FERC Accounts 350-359.1)"
    },
    {
        "name": "turbines_inverters_hydrokinetics",
        "type": "string",
        "description": "Number of wind turbines, or hydrokinetic buoys."
    },
    {
        "name": "turbines_num",
        "type": "integer",
        "description": "Number of wind turbines, or hydrokinetic buoys."
    },
    {
        "name": "ultrasupercritical_tech",
        "type": "boolean",
        "description": "Indicates whether the generator uses ultra-supercritical technology"
    },
    {
        "name": "unit_id_eia",
        "type": "string",
        "description": "EIA-assigned unit identification code."
    },
    {
        "name": "unit_id_epa",
        "type": "integer",
        "description": "New EPA unit ID."
    },
    {
        "name": "unit_id_pudl",
        "type": "integer",
        "description": "PUDL-assigned unit identification number."
    },
    {
        "name": "unitid",
        "type": "string",
        "description": "Facility-specific unit id (e.g. Unit 4)"
    },
    {
        "name": "uprate_derate_completed_date",
        "type": "date",
        "description": "The date when the uprate or derate was completed."
    },
    {
        "name": "uprate_derate_during_year",
        "type": "boolean",
        "description": "Was an uprate or derate completed on this generator during the reporting year?"
    },
    {
        "name": "utility_name_eia",
        "type": "string",
        "description": "The name of the utility."
    },
    {
        "name": "utility_name_ferc1",
        "type": "string",
        "description": "Name of the responding utility, as it is reported in FERC Form 1. For human readability only."
    },
    {
        "name": "utility_name_pudl",
        "type": "string",
        "description": "Utility name, chosen arbitrarily from the several possible utility names available in the utility matching process. Included for human readability only."
    },
    {
        "name": "utility_id_ferc1",
        "type": "integer",
        "description": "FERC-assigned respondent_id, identifying the reporting entity. Stable from year to year."
    },
    {
        "name": "utility_id_pudl",
        "type": "integer",
        "description": "A manually assigned PUDL utility ID. May not be stable over time."
    },
    {
        "name": "water_limited_capacity_mw",
        "type": "number",
        "description": "Plant capacity in MW when limited by condenser water."
    },
    {
        "name": "water_source",
        "type": "string",
        "description": "Name of water source associater with the plant."
    },
    {
        "name": "winter_capacity_mw",
        "type": "number",
        "description": "The net winter capacity."
    },
    {
        "name": "winter_estimated_capability_mw",
        "type": "number",
        "description": "EIA estimated winter capacity (in MWh)."
    }
]
"""
Field attributes.
"""

FIELDS: Dict[str, Dict[str, Any]] = {f["name"]: f for f in FIELD_LIST}
"""
Field attributes by PUDL identifier (`field.name`).
"""
