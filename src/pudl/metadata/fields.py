"""Field metadata."""
from typing import Any, Dict

from pytz import all_timezones

from .codes import ENERGY_SOURCES_EIA
from .constants import SOURCES
from .enums import (CANADA_PROVINCES_TERRITORIES, CUSTOMER_CLASSES,
                    EPACEMS_MEASUREMENT_CODES, EPACEMS_STATES, FUEL_CLASSES,
                    NERC_REGIONS, RELIABILITY_STANDARDS, REVENUE_CLASSES,
                    RTO_CLASSES, TECH_CLASSES, US_STATES_TERRITORIES)
from .labels import (COALMINE_TYPES_EIA, ENTITY_TYPES, ESTIMATED_OR_ACTUAL,
                     FUEL_UNITS_EIA, MOMENTARY_INTERRUPTIONS,
                     POWER_PURCHASE_TYPES_FERC1)

FIELD_METADATA: Dict[str, Dict[str, Any]] = {
    "active": {
        "type": "boolean",
        "description": "Indicates whether or not the dataset has been pulled into PUDL by the extract transform load process."
    },
    "actual_peak_demand_savings_mw": {
        "type": "number"
    },
    "address_2": {
        "type": "string"
    },
    "advanced_metering_infrastructure": {
        "type": "integer"
    },
    "alternative_fuel_vehicle_2_activity": {
        "type": "boolean"
    },
    "alternative_fuel_vehicle_activity": {
        "type": "boolean"
    },
    "amount_type": {
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
    "annual_indirect_program_cost": {
        "type": "number"
    },
    "annual_total_cost": {
        "type": "number"
    },
    "ash_content_pct": {
        "type": "number",
        "description": "Ash content percentage by weight to the nearest 0.1 percent."
    },
    "ash_impoundment": {
        "type": "boolean",
        "description": "Is there an ash impoundment (e.g. pond, reservoir) at the plant?",
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ash_impoundment_lined": {
        "type": "boolean",
        "description": "If there is an ash impoundment at the plant, is the impoundment lined?"
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ash_impoundment_status": {
        "type": "string",
        "description": "If there is an ash impoundment at the plant, the ash impoundment status as of December 31 of the reporting year."
    },
    "asset_retirement_cost": {
        "type": "number",
        "description": "Asset retirement cost (USD)."
    },
    "associated_combined_heat_power": {
        "type": "boolean",
        "description": "Indicates whether the generator is associated with a combined heat and power system"
    },
    "attention_line": {
        "type": "string"
    },
    "automated_meter_reading": {
        "type": "integer"
    },
    "avg_num_employees": {
        "type": "number"
    },
    "backup_capacity_mw": {
        "type": "number"
    },
    "balancing_authority_code_eia": {
        "type": "string",
        "description": "The plant's balancing authority code."
    },
    "balancing_authority_id_eia": {
        "type": "integer"
    },
    "balancing_authority_name_eia": {
        "type": "string",
        "description": "The plant's balancing authority name."
    },
    "bga_source": {
        "type": "string",
        "description": "The source from where the unit_id_pudl is compiled. The unit_id_pudl comes directly from EIA 860, or string association (which looks at all the boilers and generators that are not associated with a unit and tries to find a matching string in the respective collection of boilers or generator), or from a unit connection (where the unit_id_eia is employed to find additional boiler generator connections)."
    },
    "billing_demand_mw": {
        "type": "number",
        "description": "Monthly average billing demand (for requirements purchases, and any transactions involving demand charges). In megawatts."
    },
    "boiler_id": {
        "type": "string",
        "description": "Alphanumeric boiler ID.",
    },
    "bunded_activity": {
        "type": "boolean"
    },
    "business_model": {
        "type": "string",
        "constraints": {
            "enum": ["retail", "energy_services"]
        }
    },
    "buy_distribution_activity": {
        "type": "boolean"
    },
    "buying_transmission_activity": {
        "type": "boolean"
    },
    "bypass_heat_recovery": {
        "type": "boolean",
        "description": "Can this generator operate while bypassing the heat recovery steam generator?"
    },
    "caidi_w_major_event_dats_minutes": {
        "type": "number"
    },
    "caidi_w_major_event_days_minus_loss_of_service_minutes": {
        "type": "number"
    },
    "caidi_wo_major_event_days_minutes": {
        "type": "number"
    },
    "capacity_mw": {
        "type": "number",
        "description": "Total installed (nameplate) capacity, in megawatts.",
        # TODO: Disambiguate if necessary. Does this mean different things in
        # different tables? It shows up in a lot of places.
    },
    "capex_equipment": {
        "type": "number",
        "description": "Cost of plant: equipment (USD)."
    },
    "capex_equipment_electric": {
        "type": "number",
        "description": "Cost of plant: accessory electric equipment (USD)."
    },
    "capex_equipment_misc": {
        "type": "number",
        "description": "Cost of plant: miscellaneous power plant equipment (USD)."
    },
    "capex_facilities": {
        "type": "number",
        "description": "Cost of plant: reservoirs, dams, and waterways (USD)."
    },
    "capex_land": {
        "type": "number",
        "description": "Cost of plant: land and land rights (USD)."
    },
    "capex_per_mw": {
        "type": "number",
        "description": "Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD.",
    },
    "capex_roads": {
        "type": "number",
        "description": "Cost of plant: roads, railroads, and bridges (USD)."
    },
    "capex_structures": {
        "type": "number",
        "description": "Cost of plant: structures and improvements (USD)."
    },
    "capex_total": {
        "type": "number",
        "description": "Total cost of plant (USD)."
    },
    "capex_wheels_turbines_generators": {
        "type": "number",
        "description": "Cost of plant: water wheels, turbines, and generators (USD)."
    },
    "carbon_capture": {
        "type": "boolean",
        "description": "Indicates whether the generator uses carbon capture technology."
    },
    "chlorine_content_ppm": {
        "type": "number"
    },
    "circuits_with_voltage_optimization": {
        "type": "integer"
    },
    "city": {
        "type": "string",
        # TODO: Disambiguate column. City means different things in different tables.
    },
    "co2_mass_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": EPACEMS_MEASUREMENT_CODES
        }
    },
    "co2_mass_tons": {
        "type": "number",
        "description": "Carbon dioxide emissions in short tons."
    },
    "code": {
        "type": "string",
        "description": "Originally reported short code.",
    },
    "cofire_fuels": {
        "type": "boolean",
        "description": "Can the generator co-fire fuels?."
    },
    "coincident_peak_demand_mw": {
        "type": "number",
        "description": "Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier's system reaches its monthly peak. In megawatts."
    },
    "construction_type": {
        "type": "string",
        "description": "Type of plant construction ('outdoor', 'semioutdoor', or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.",
        "constraints": {
            "enum": ["", "unknown", "conventional", "outdoor", "semioutdoor"]
        }
    },
    "construction_year": {
        "type": "year",
        "description": "Year the plant's oldest still operational unit was built.",
    },
    "consumed_by_facility_mwh": {
        "type": "number"
    },
    "consumed_by_respondent_without_charge_mwh": {
        "type": "number"
    },
    "contact_firstname": {
        "type": "string",
        "description": "First name of utility contact 1."
    },
    "contact_firstname_2": {
        "type": "string",
        "description": "First name of utility contact 2."
    },
    "contact_lastname": {
        "type": "string",
        "description": "Last name of utility contact 1."
    },
    "contact_lastname_2": {
        "type": "string",
        "description": "Last name of utility contact 2."
    },
    "contact_title": {
        "type": "string",
        "description": "Title of of utility contact 1."
    },
    "contact_title_2": {
        "type": "string",
        "description": "Title of utility contact 2."
    },
    "contract_expiration_date": {
        "type": "date",
        "description": "Date contract expires.Format:  MMYY."
    },
    "contract_type_code": {
        "type": "string",
        "description": "Purchase type under which receipts occurred in the reporting month. C: Contract, NC: New Contract, S: Spot Purchase, T: Tolling Agreement.",
        "constraints": {
            "enum": ["S", "C", "NC", "T"]
        }
    },
    "county": {
        "type": "string",
        "description": "The plant's county."
    },
    "county_id_fips": {
        "type": "string",
        "description": "County ID from the Federal Information Processing Standard Publication 6-4."
        # TODO: Add pattern & length constraint.
    },
    "credits_or_adjustments": {
        "type": "number"
    },
    "critical_peak_pricing": {
        "type": "boolean"
    },
    "critical_peak_rebate": {
        "type": "boolean"
    },
    "current_planned_operating_date": {
        "type": "date",
        "description": "The most recently updated effective date on which the generator is scheduled to start operation"
    },
    "customer_class": {
        "type": "string",
        "constraints": {
            "enum": CUSTOMER_CLASSES
        }
    },
    "customer_incentives_cost": {
        "type": "number"
    },
    "customer_incentives_incremental_cost": {
        "type": "number"
    },
    "customer_incentives_incremental_life_cycle_cost": {
        "type": "number"
    },
    "customer_other_costs_incremental_life_cycle_cost": {
        "type": "number"
    },
    "customers": {
        "type": "number"
    },
    "daily_digital_access_customers": {
        "type": "integer"
    },
    "data_observed": {
        "type": "boolean"
    },
    "data_source": {
        "type": "string",
        "description": "Source of EIA 860 data. Either Annual EIA 860 or the year-to-date updates from EIA 860M.",
        "constraints": {
            "enum": ["eia860", "eia860m"]
        },
    },
    "datasource": {
        "type": "string",
        "description": "Code identifying a dataset available within PUDL.",
        "constraints": {
            "enum": list(SOURCES)
        }
    },
    "datum": {
        "type": "string",
        "description": "Geodetic coordinate system identifier (e.g. NAD27, NAD83, or WGS84)."
    },
    "deliver_power_transgrid": {
        "type": "boolean",
        "description": "Indicate whether the generator can deliver power to the transmission grid."
    },
    "delivered_mwh": {
        "type": "number",
        "description": "Gross megawatt-hours delivered in power exchanges and used as the basis for settlement."
    },
    "delivery_customers": {
        "type": "number"
    },
    "demand_annual_mwh": {
        "type": "number"
    },
    "demand_charges": {
        "type": "number",
        "description": "Demand charges (USD)."
    },
    "demand_mwh": {
        "type": "number"
    },
    "description": {
        "type": "string",
        "description": "Long human-readable description of the meaning of a code/label.",
    },
    "ferc_account_description": {
        "type": "string",
    },
    "direct_load_control_customers": {
        "type": "integer"
    },
    "distributed_generation": {
        "type": "boolean",
        "description": "Whether the generator is considered distributed generation",
    },
    "distributed_generation_owned_capacity_mw": {
        "type": "number"
    },
    "distribution_acct360_land": {
        "type": "number",
        "description": "FERC Account 360: Distribution Plant Land and Land Rights."
    },
    "distribution_acct361_structures": {
        "type": "number",
        "description": "FERC Account 361: Distribution Plant Structures and Improvements."
    },
    "distribution_acct362_station_equip": {
        "type": "number",
        "description": "FERC Account 362: Distribution Plant Station Equipment."
    },
    "distribution_acct363_storage_battery_equip": {
        "type": "number",
        "description": "FERC Account 363: Distribution Plant Storage Battery Equipment."
    },
    "distribution_acct364_poles_towers": {
        "type": "number",
        "description": "FERC Account 364: Distribution Plant Poles, Towers, and Fixtures."
    },
    "distribution_acct365_overhead_conductors": {
        "type": "number",
        "description": "FERC Account 365: Distribution Plant Overhead Conductors and Devices."
    },
    "distribution_acct366_underground_conduit": {
        "type": "number",
        "description": "FERC Account 366: Distribution Plant Underground Conduit."
    },
    "distribution_acct367_underground_conductors": {
        "type": "number",
        "description": "FERC Account 367: Distribution Plant Underground Conductors and Devices."
    },
    "distribution_acct368_line_transformers": {
        "type": "number",
        "description": "FERC Account 368: Distribution Plant Line Transformers."
    },
    "distribution_acct369_services": {
        "type": "number",
        "description": "FERC Account 369: Distribution Plant Services."
    },
    "distribution_acct370_meters": {
        "type": "number",
        "description": "FERC Account 370: Distribution Plant Meters."
    },
    "distribution_acct371_customer_installations": {
        "type": "number",
        "description": "FERC Account 371: Distribution Plant Installations on Customer Premises."
    },
    "distribution_acct372_leased_property": {
        "type": "number",
        "description": "FERC Account 372: Distribution Plant Leased Property on Customer Premises."
    },
    "distribution_acct373_street_lighting": {
        "type": "number",
        "description": "FERC Account 373: Distribution PLant Street Lighting and Signal Systems."
    },
    "distribution_acct374_asset_retirement": {
        "type": "number",
        "description": "FERC Account 374: Distribution Plant Asset Retirement Costs."
    },
    "distribution_activity": {
        "type": "boolean"
    },
    "distribution_circuits": {
        "type": "integer"
    },
    "distribution_total": {
        "type": "number",
        "description": "Distribution Plant Total (FERC Accounts 360-374)."
    },
    "duct_burners": {
        "type": "boolean",
        "description": "Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas"
    },
    "eia_code": {
        "type": "integer"
    },
    "electric_plant": {
        "type": "number",
        "description": "Electric Plant In Service (USD)."
    },
    "electric_plant_in_service_total": {
        "type": "number",
        "description": "Total Electric Plant in Service (FERC Accounts 101, 102, 103 and 106)"
    },
    "electric_plant_purchased_acct102": {
        "type": "number",
        "description": "FERC Account 102: Electric Plant Purchased."
    },
    "electric_plant_sold_acct102": {
        "type": "number",
        "description": "FERC Account 102: Electric Plant Sold (Negative)."
    },
    "energy_charges": {
        "type": "number",
        "description": "Energy charges (USD)."
    },
    "energy_displaced_mwh": {
        "type": "number"
    },
    "energy_efficiency_annual_actual_peak_reduction_mw": {
        "type": "number"
    },
    "energy_efficiency_annual_cost": {
        "type": "number"
    },
    "energy_efficiency_annual_effects_mwh": {
        "type": "number"
    },
    "energy_efficiency_annual_incentive_payment": {
        "type": "number"
    },
    "energy_efficiency_incremental_actual_peak_reduction_mw": {
        "type": "number"
    },
    "energy_efficiency_incremental_effects_mwh": {
        "type": "number"
    },
    "energy_savings_estimates_independently_verified": {
        "type": "boolean"
    },
    "energy_savings_independently_verified": {
        "type": "boolean"
    },
    "energy_savings_mwh": {
        "type": "number"
    },
    "energy_served_ami_mwh": {
        "type": "number"
    },
    "energy_source": {
        "type": "string"
    },
    "energy_source_code": {
        "type": "string",
        "description": "The fuel code associated with the fuel receipt. Two or three character alphanumeric."
    },
    "energy_source_1_transport_1": {
        "type": "string",
        "description": "Primary mode of transport for energy source 1.",
    },
    "energy_source_1_transport_2": {
        "type": "string",
        "description": "Secondary mode of transport for energy source 1.",
    },
    "energy_source_1_transport_3": {
        "type": "string",
        "description": "Tertiary mode of transport for energy source 1.",
    },
    "energy_source_2_transport_1": {
        "type": "string",
        "description": "Primary mode of transport for energy source 2.",
    },
    "energy_source_2_transport_2": {
        "type": "string",
        "description": "Secondary mode of transport for energy source 2.",
    },
    "energy_source_2_transport_3": {
        "type": "string",
        "description": "Tertiary mode of transport for energy source 2.",
    },
    "energy_source_code_1": {
        "type": "string",
        "description": "The code representing the most predominant type of energy that fuels the generator."
    },
    "energy_source_code_2": {
        "type": "string",
        "description": "The code representing the second most predominant type of energy that fuels the generator"
    },
    "energy_source_code_3": {
        "type": "string",
        "description": "The code representing the third most predominant type of energy that fuels the generator"
    },
    "energy_source_code_4": {
        "type": "string",
        "description": "The code representing the fourth most predominant type of energy that fuels the generator"
    },
    "energy_source_code_5": {
        "type": "string",
        "description": "The code representing the fifth most predominant type of energy that fuels the generator"
    },
    "energy_source_code_6": {
        "type": "string",
        "description": "The code representing the sixth most predominant type of energy that fuels the generator"
    },
    "energy_storage": {
        "type": "boolean",
        "description": "Indicates if the facility has energy storage capabilities."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "energy_used_for_pumping_mwh": {
        "type": "number",
        "description": "Energy used for pumping, in megawatt-hours."
    },
    "entity_type": {
        "type": "string",
        "description": "Entity type of principal owner.",
        "constraints": {
            "enum": list(ENTITY_TYPES.values())
        }
    },
    "estimated_or_actual_capacity_data": {
        "type": "string",
        "constraints": {
            "enum": list(ESTIMATED_OR_ACTUAL.values())
        }
    },
    "estimated_or_actual_fuel_data": {
        "type": "string",
        "constraints": {
            "enum": list(ESTIMATED_OR_ACTUAL.values())
        }
    },
    "estimated_or_actual_tech_data": {
        "type": "string",
        "constraints": {
            "enum": list(ESTIMATED_OR_ACTUAL.values())
        }
    },
    "exchange_energy_delivered_mwh": {
        "type": "number"
    },
    "exchange_energy_recieved_mwh": {
        "type": "number"
    },
    "experimental_plant_acct103": {
        "type": "number",
        "description": "FERC Account 103: Experimental Plant Unclassified."
    },
    "facility_id": {
        "type": "integer",
        "description": "New EPA plant ID."
    },
    "ferc_account_id": {
        "type": "string",
        "description": "Account number, from FERC's Uniform System of Accounts for Electric Plant. Also includes higher level labeled categories."
    },
    "ferc_cogen_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility cogenerator status."
    },
    "ferc_cogen_status": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility cogenerator status."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ferc_exempt_wholesale_generator": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility exempt wholesale generator status"
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ferc_exempt_wholesale_generator_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility exempt wholesale generator status."
    },
    "ferc_license_id": {
        "type": "integer",
        "description": "FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible."
    },
    "ferc_small_power_producer": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility small power producer status"
    },
    "ferc_small_power_producer_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility small power producer status."
    },
    "fluidized_bed_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses fluidized bed technology"
    },
    "fraction_owned": {
        "type": "number",
        "description": "Proportion of generator ownership."
    },
    "fuel_class": {
        "type": "string",
        # TODO: Needs a description / better name. EIA-861 distributed generation only.
        "constraints": {
            "enum": FUEL_CLASSES
        }
    },
    "fuel_consumed_for_electricity_mmbtu": {
        "type": "number",
        "description": "Total consumption of fuel to produce electricity, in physical units, year to date."
    },
    "fuel_consumed_for_electricity_units": {
        "type": "number",
        "description": "Consumption for electric generation of the fuel type in physical units."
    },
    "fuel_consumed_mmbtu": {
        "type": "number",
        "description": "Total consumption of fuel in physical units, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production."
    },
    "fuel_consumed_units": {
        "type": "number",
        "description": "Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production."
    },
    "fuel_cost_per_mmbtu": {
        "type": "number",
        "description": "Average fuel cost per mmBTU of heat content in nominal USD.",
    },
    "fuel_cost_per_unit_burned": {
        "type": "number",
        "description": "Average cost of fuel consumed in the report year per reported fuel unit (USD)."
    },
    "fuel_cost_per_unit_delivered": {
        "type": "number",
        "description": "Average cost of fuel delivered in the report year per reported fuel unit (USD)."
    },
    "fuel_derived_from": {
        "type": "string",
        "description": "Original fuel from which this refined fuel was derived.",
        "constraints": {
            "enum": sorted(set(ENERGY_SOURCES_EIA["df"]["fuel_derived_from"]))
        }
    },
    "fuel_group_code": {
        "type": "string",
        "description": "Fuel groups used in the Electric Power Monthly",
        "constraints": {
            "enum": ["petroleum", "other_gas", "petroleum_coke", "natural_gas", "coal"]
        }
    },
    "fuel_group_eia": {
        "type": "string",
        "description": "High level fuel group defined in the 2021-2023 EIA Form 860 instructions, Table 28.",
        "constraints": {
            "enum": sorted(set(ENERGY_SOURCES_EIA["df"]["fuel_group_eia"]))
        }
    },
    "fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Heat content of the fuel in millions of Btus per physical unit.",
    },
    "fuel_pct": {
        "type": "number"
    },
    "fuel_phase": {
        "type": "string",
        "description": "Physical phase of matter of the fuel.",
        "constraints": {
            "enum": sorted(set(ENERGY_SOURCES_EIA["df"]["fuel_phase"].dropna()))
        }
    },
    "fuel_received_units": {
        "type": "number",
        "description": "Quanity of fuel received in tons, barrel, or Mcf."
    },
    "fuel_type": {
        "type": "string",
        # TODO disambiguate column name. This should be just FERC 1 tables, as the EIA
        # fuel types are now all energy_source_code
    },
    "fuel_type_code_aer": {
        "type": "string",
        "description": "A partial aggregation of the reported fuel type codes into larger categories used by EIA in, for example, the Annual Energy Review (AER). Two or three letter alphanumeric."
    },
    "fuel_type_code_pudl": {
        "type": "string",
        "description": "Simplified fuel type code used in PUDL",
        "constraints": {
            "enum": sorted(set(ENERGY_SOURCES_EIA["df"].fuel_type_code_pudl))
        }
    },
    "fuel_units": {
        "type": "string",
        "description": "Reported units of measure for fuel.",
        # Note: Different ENUM constraints are applied below on EIA vs. FERC1
    },
    "furnished_without_charge_mwh": {
        "type": "number"
    },
    "future_plant": {
        "type": "number",
        "description": "Electric Plant Held for Future Use (USD)."
    },
    "general_acct389_land": {
        "type": "number",
        "description": "FERC Account 389: General Land and Land Rights."
    },
    "general_acct390_structures": {
        "type": "number",
        "description": "FERC Account 390: General Structures and Improvements."
    },
    "general_acct391_office_equip": {
        "type": "number",
        "description": "FERC Account 391: General Office Furniture and Equipment."
    },
    "general_acct392_transportation_equip": {
        "type": "number",
        "description": "FERC Account 392: General Transportation Equipment."
    },
    "general_acct393_stores_equip": {
        "type": "number",
        "description": "FERC Account 393: General Stores Equipment."
    },
    "general_acct394_shop_equip": {
        "type": "number",
        "description": "FERC Account 394: General Tools, Shop, and Garage Equipment."
    },
    "general_acct395_lab_equip": {
        "type": "number",
        "description": "FERC Account 395: General Laboratory Equipment."
    },
    "general_acct396_power_operated_equip": {
        "type": "number",
        "description": "FERC Account 396: General Power Operated Equipment."
    },
    "general_acct397_communication_equip": {
        "type": "number",
        "description": "FERC Account 397: General Communication Equipment."
    },
    "general_acct398_misc_equip": {
        "type": "number",
        "description": "FERC Account 398: General Miscellaneous Equipment."
    },
    "general_acct399_1_asset_retirement": {
        "type": "number",
        "description": "FERC Account 399.1: Asset Retirement Costs for General Plant."
    },
    "general_acct399_other_property": {
        "type": "number",
        "description": "FERC Account 399: General Plant Other Tangible Property."
    },
    "general_subtotal": {
        "type": "number",
        "description": "General Plant Subtotal (FERC Accounts 389-398)."
    },
    "general_total": {
        "type": "number",
        "description": "General Plant Total (FERC Accounts 389-399.1)."
    },
    "generation_activity": {
        "type": "boolean"
    },
    "generator_id": {
        "type": "string",
        "description": "Generator ID is usually numeric, but sometimes includes letters. Make sure you treat it as a string!",
    },
    "generators_num_less_1_mw": {
        "type": "number"
    },
    "generators_number": {
        "type": "number"
    },
    "green_pricing_revenue": {
        "type": "number"
    },
    "grid_voltage_2_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities"
    },
    "grid_voltage_3_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities"
    },
    "grid_voltage_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities"
    },
    "gross_load_mw": {
        "type": "number",
        "description": "Average power in megawatts delivered during time interval measured."
    },
    "heat_content_mmbtu": {
        "type": "number",
        "description": "The energy contained in fuel burned, measured in million BTU."
    },
    "highest_distribution_voltage_kv": {
        "type": "number"
    },
    "home_area_network": {
        "type": "integer"
    },
    "hydro_acct330_land": {
        "type": "number",
        "description": "FERC Account 330: Hydro Land and Land Rights."
    },
    "hydro_acct331_structures": {
        "type": "number",
        "description": "FERC Account 331: Hydro Structures and Improvements."
    },
    "hydro_acct332_reservoirs_dams_waterways": {
        "type": "number",
        "description": "FERC Account 332: Hydro Reservoirs, Dams, and Waterways."
    },
    "hydro_acct333_wheels_turbines_generators": {
        "type": "number",
        "description": "FERC Account 333: Hydro Water Wheels, Turbins, and Generators."
    },
    "hydro_acct334_accessory_equip": {
        "type": "number",
        "description": "FERC Account 334: Hydro Accessory Electric Equipment."
    },
    "hydro_acct335_misc_equip": {
        "type": "number",
        "description": "FERC Account 335: Hydro Miscellaneous Power Plant Equipment."
    },
    "hydro_acct336_roads_railroads_bridges": {
        "type": "number",
        "description": "FERC Account 336: Hydro Roads, Railroads, and Bridges."
    },
    "hydro_acct337_asset_retirement": {
        "type": "number",
        "description": "FERC Account 337: Asset Retirement Costs for Hydraulic Production."
    },
    "hydro_total": {
        "type": "number",
        "description": "Hydraulic Production Plant Total (FERC Accounts 330-337)"
    },
    "inactive_accounts_included": {
        "type": "boolean"
    },
    "incremental_energy_savings_mwh": {
        "type": "number"
    },
    "incremental_life_cycle_energy_savings_mwh": {
        "type": "number"
    },
    "incremental_life_cycle_peak_reduction_mwh": {
        "type": "number"
    },
    "incremental_peak_reduction_mw": {
        "type": "number"
    },
    "installation_year": {
        "type": "year",
        "description": "Year the plant's most recently built unit was installed.",
    },
    "intangible_acct301_organization": {
        "type": "number",
        "description": "FERC Account 301: Intangible Plant Organization."
    },
    "intangible_acct302_franchises_consents": {
        "type": "number",
        "description": "FERC Account 302: Intangible Plant Franchises and Consents."
    },
    "intangible_acct303_misc": {
        "type": "number",
        "description": "FERC Account 303: Miscellaneous Intangible Plant."
    },
    "intangible_total": {
        "type": "number",
        "description": "Intangible Plant Total (FERC Accounts 301-303)."
    },
    "iso_rto_code": {
        "type": "string",
        "description": "The code of the plant's ISO or RTO. NA if not reported in that year."
    },
    "label": {
        "type": "string",
        "description": "Longer human-readable code using snake_case",
    },
    "latitude": {
        "type": "number",
        "description": "Latitude of the plant's location, in degrees."
    },
    "leased_plant": {
        "type": "number",
        "description": "Electric Plant Leased to Others (USD)."
    },
    "line_id": {
        "type": "string",
        "description": "A human readable string uniquely identifying the FERC depreciation account. Used in lieu of the actual line number, as those numbers are not guaranteed to be consistent from year to year.",
        # TODO disambiguate column name
    },
    "liquefied_natural_gas_storage": {
        "type": "boolean",
        "description": "Indicates if the facility have the capability to store the natural gas in the form of liquefied natural gas."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "load_management_annual_actual_peak_reduction_mw": {
        "type": "number"
    },
    "load_management_annual_cost": {
        "type": "number"
    },
    "load_management_annual_effects_mwh": {
        "type": "number"
    },
    "load_management_annual_incentive_payment": {
        "type": "number"
    },
    "load_management_annual_potential_peak_reduction_mw": {
        "type": "number"
    },
    "load_management_incremental_actual_peak_reduction_mw": {
        "type": "number"
    },
    "load_management_incremental_effects_mwh": {
        "type": "number"
    },
    "load_management_incremental_potential_peak_reduction_mw": {
        "type": "number"
    },
    "longitude": {
        "type": "number",
        "description": "Longitude of the plant's location, in degrees."
    },
    "major_electric_plant_acct101_acct106_total": {
        "type": "number",
        "description": "Total Major Electric Plant in Service (FERC Accounts 101 and 106)."
    },
    "major_program_changes": {
        "type": "boolean"
    },
    "max_fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Maximum heat content per physical unit of fuel in MMBtu.",
    },
    "mercury_content_ppm": {
        "type": "number",
        "description": "Mercury content in parts per million (ppm) to the nearest 0.001 ppm."
    },
    "merge_address": {
        "type": "string"
    },
    "merge_city": {
        "type": "string"
    },
    "merge_company": {
        "type": "string"
    },
    "merge_date": {
        "type": "date"
    },
    "merge_state": {
        "type": "string",
        "description": "Two letter US state abbreviations and three letter ISO-3166-1 country codes for international mines.",
        # TODO: Add ENUM constraint.
    },
    "merge_zip_4": {
        "type": "string"
        # TODO Standardize with other zip codes and apply pattern constraint
    },
    "merge_zip_5": {
        "type": "string"
        # TODO Standardize with other zip codes and apply pattern constraint
    },
    "min_fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Minimum heat content per physical unit of fuel in MMBtu.",
    },
    "mine_id_msha": {
        "type": "integer",
        "description": "MSHA issued mine identifier."
    },
    "mine_id_pudl": {
        "type": "integer",
        "description": "Dynamically assigned PUDL mine identifier."
    },
    "mine_name": {
        "type": "string",
        "description": "Coal mine name."
    },
    "mine_type": {
        "type": "string",
        "description": "Type of coal mine.",
        "constraints": {
            "enum": list(COALMINE_TYPES_EIA.values()),
        }
    },
    "minimum_load_mw": {
        "type": "number",
        "description": "The minimum load at which the generator can operate at continuosuly."
    },
    "fuel_transportation_mode": {
        "type": "string"
    },
    "moisture_content_pct": {
        "type": "number"
    },
    "momentary_interruption_definition": {
        "type": "string",
        "constraints": {
            "enum": list(MOMENTARY_INTERRUPTIONS.values())
        }
    },
    "month": {
        "type": "integer",
        "description": "Month of the year"
    },
    "multiple_fuels": {
        "type": "boolean",
        "description": "Can the generator burn multiple fuels?"
    },
    "nameplate_power_factor": {
        "type": "number",
        "description": "The nameplate power factor of the generator."
    },
    "natural_gas_delivery_contract_type_code": {
        "type": "string",
        "description": "Contract type for natrual gas delivery service:",
        "constraints": {
            "enum": ["", "firm", "interruptible"]
        }
    },
    "natural_gas_local_distribution_company": {
        "type": "string",
        "description": "Names of Local Distribution Company (LDC), connected to natural gas burning power plants."
    },
    "natural_gas_pipeline_name_1": {
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility."
    },
    "natural_gas_pipeline_name_2": {
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility."
    },
    "natural_gas_pipeline_name_3": {
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility."
    },
    "natural_gas_storage": {
        "type": "boolean",
        "description": "Indicates if the facility have on-site storage of natural gas."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "natural_gas_transport_code": {
        "type": "string",
        "description": "Contract type for natural gas transportation service.",
        "constraints": {
            "enum": ["", "firm", "interruptible"]
        }
    },
    "nerc_region": {
        "type": "string",
        "description": "NERC region in which the plant is located",
        "constraints": {
            "enum": NERC_REGIONS
        }
    },
    "nerc_regions_of_operation": {
        "type": "string",
        "constraints": {
            "enum": NERC_REGIONS
        }
    },
    "net_capacity_adverse_conditions_mw": {
        "type": "number",
        "description": "Net plant capability under the least favorable operating conditions, in megawatts."
    },
    "net_capacity_favorable_conditions_mw": {
        "type": "number",
        "description": "Net plant capability under the most favorable operating conditions, in megawatts."
    },
    "net_generation_mwh": {
        "type": "number",
        "description": "Net electricity generation for the specified period in megawatt-hours (MWh).",
        # TODO: disambiguate as this column means something different in
        # generation_fuel_eia923:
        # "description": "Net generation, year to date in megawatthours (MWh). This is total electrical output net of station service.  In the case of combined heat and power plants, this value is intended to include internal consumption of electricity for the purposes of a production process, as well as power put on the grid.",
    },
    "net_load_mwh": {
        "type": "number",
        "description": "Net output for load (net generation - energy used for pumping) in megawatt-hours."
    },
    "net_metering": {
        "type": "boolean",
        "description": "Did this plant have a net metering agreement in effect during the reporting year?  (Only displayed for facilities that report the sun or wind as an energy source). This field was only reported up until 2015"
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "net_power_exchanged_mwh": {
        "type": "number"
    },
    "net_wheeled_power_mwh": {
        "type": "number"
    },
    "new_parent": {
        "type": "string"
    },
    "non_amr_ami": {
        "type": "integer"
    },
    "non_coincident_peak_demand_mw": {
        "type": "number",
        "description": "Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts."
    },
    "not_water_limited_capacity_mw": {
        "type": "number",
        "description": "Plant capacity in MW when not limited by condenser water."
    },
    "nox_mass_lbs": {
        "type": "number",
        "description": "NOx emissions in pounds."
    },
    "nox_mass_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": EPACEMS_MEASUREMENT_CODES
        }
    },
    "nox_rate_lbs_mmbtu": {
        "type": "number",
        "description": "The average rate at which NOx was emitted during a given time period."
    },
    "nox_rate_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": EPACEMS_MEASUREMENT_CODES
        }
    },
    "nuclear_acct320_land": {
        "type": "number",
        "description": "FERC Account 320: Nuclear Land and Land Rights."
    },
    "nuclear_acct321_structures": {
        "type": "number",
        "description": "FERC Account 321: Nuclear Structures and Improvements."
    },
    "nuclear_acct322_reactor_equip": {
        "type": "number",
        "description": "FERC Account 322: Nuclear Reactor Plant Equipment."
    },
    "nuclear_acct323_turbogenerators": {
        "type": "number",
        "description": "FERC Account 323: Nuclear Turbogenerator Units"
    },
    "nuclear_acct324_accessory_equip": {
        "type": "number",
        "description": "FERC Account 324: Nuclear Accessory Electric Equipment."
    },
    "nuclear_acct325_misc_equip": {
        "type": "number",
        "description": "FERC Account 325: Nuclear Miscellaneous Power Plant Equipment."
    },
    "nuclear_acct326_asset_retirement": {
        "type": "number",
        "description": "FERC Account 326: Asset Retirement Costs for Nuclear Production."
    },
    "nuclear_total": {
        "type": "number",
        "description": "Total Nuclear Production Plant (FERC Accounts 320-326)"
    },
    "nuclear_unit_id": {
        "type": "string",
        "description": "For nuclear plants only, the unit number .One digit numeric. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level."
    },
    "operates_generating_plant": {
        "type": "boolean"
    },
    "operating_date": {
        "type": "date",
        "description": "Date the generator began commercial operation"
    },
    "operating_datetime_utc": {
        "type": "datetime",
        "description": "Date and time measurement began (UTC)."
    },
    "operating_switch": {
        "type": "string",
        "description": "Indicates whether the fuel switching generator can switch when operating"
    },
    "operating_time_hours": {
        "type": "number",
        "description": "Length of time interval measured."
    },
    "operational_status": {
        "type": "string",
        "description": "The operating status of the generator. This is based on which tab the generator was listed in in EIA 860."
    },
    "operational_status_code": {
        "type": "string",
        "description": "The operating status of the generator."
    },
    "opex_allowances": {
        "type": "number",
        "description": "Allowances."
    },
    "opex_boiler": {
        "type": "number",
        "description": "Maintenance of boiler (or reactor) plant."
    },
    "opex_coolants": {
        "type": "number",
        "description": "Cost of coolants and water (nuclear plants only)"
    },
    "opex_dams": {
        "type": "number",
        "description": "Production expenses: maintenance of reservoirs, dams, and waterways (USD)."
    },
    "opex_electric": {
        "type": "number",
        "description": "Production expenses: electric expenses (USD)."
    },
    "opex_engineering": {
        "type": "number",
        "description": "Production expenses: maintenance, supervision, and engineering (USD)."
    },
    "opex_fuel": {
        "type": "number",
        "description": "Production expenses: fuel (USD)."
    },
    "opex_generation_misc": {
        "type": "number",
        "description": "Production expenses: miscellaneous power generation expenses (USD)."
    },
    "opex_hydraulic": {
        "type": "number",
        "description": "Production expenses: hydraulic expenses (USD)."
    },
    "opex_maintenance": {
        "type": "number",
        "description": "Production expenses: Maintenance (USD)."
    },
    "opex_misc_plant": {
        "type": "number",
        "description": "Production expenses: maintenance of miscellaneous hydraulic plant (USD)."
    },
    "opex_misc_power": {
        "type": "number",
        "description": "Miscellaneous steam (or nuclear) expenses."
    },
    "opex_misc_steam": {
        "type": "number",
        "description": "Maintenance of miscellaneous steam (or nuclear) plant."
    },
    "opex_operations": {
        "type": "number",
        "description": "Production expenses: operations, supervision, and engineering (USD)."
    },
    "opex_per_mwh": {
        "type": "number",
        "description": "Total production expenses (USD per MWh generated)."
    },
    "opex_plant": {
        "type": "number",
        "description": "Production expenses: maintenance of electric plant (USD)."
    },
    "opex_plants": {
        "type": "number",
        "description": "Maintenance of electrical plant."
    },
    "opex_production_before_pumping": {
        "type": "number",
        "description": "Total production expenses before pumping (USD)."
    },
    "opex_production_total": {
        "type": "number",
        "description": "Total operating epxenses."
    },
    "opex_pumped_storage": {
        "type": "number",
        "description": "Production expenses: pumped storage (USD)."
    },
    "opex_pumping": {
        "type": "number",
        "description": "Production expenses: We are here to PUMP YOU UP! (USD)."
    },
    "opex_rents": {
        "type": "number",
        "description": "Production expenses: rents (USD)."
    },
    "opex_steam": {
        "type": "number",
        "description": "Steam expenses."
    },
    "opex_steam_other": {
        "type": "number",
        "description": "Steam from other sources."
    },
    "opex_structures": {
        "type": "number",
        "description": "Production expenses: maintenance of structures (USD)."
    },
    "opex_total": {
        "type": "number",
        "description": "Total production expenses, excluding fuel (USD)."
    },
    "opex_transfer": {
        "type": "number",
        "description": "Steam transferred (Credit)."
    },
    "opex_water_for_power": {
        "type": "number",
        "description": "Production expenses: water for power (USD)."
    },
    "original_planned_operating_date": {
        "type": "date",
        "description": "The date the generator was originally scheduled to be operational"
    },
    "other": {
        "type": "number"
    },
    "other_acct340_land": {
        "type": "number",
        "description": "FERC Account 340: Other Land and Land Rights."
    },
    "other_acct341_structures": {
        "type": "number",
        "description": "FERC Account 341: Other Structures and Improvements."
    },
    "other_acct342_fuel_accessories": {
        "type": "number",
        "description": "FERC Account 342: Other Fuel Holders, Products, and Accessories."
    },
    "other_acct343_prime_movers": {
        "type": "number",
        "description": "FERC Account 343: Other Prime Movers."
    },
    "other_acct344_generators": {
        "type": "number",
        "description": "FERC Account 344: Other Generators."
    },
    "other_acct345_accessory_equip": {
        "type": "number",
        "description": "FERC Account 345: Other Accessory Electric Equipment."
    },
    "other_acct346_misc_equip": {
        "type": "number",
        "description": "FERC Account 346: Other Miscellaneous Power Plant Equipment."
    },
    "other_acct347_asset_retirement": {
        "type": "number",
        "description": "FERC Account 347: Asset Retirement Costs for Other Production."
    },
    "other_charges": {
        "type": "number",
        "description": "Other charges, including out-of-period adjustments (USD)."
    },
    "other_combustion_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses other combustion technologies"
    },
    "other_costs": {
        "type": "number"
    },
    "other_costs_incremental_cost": {
        "type": "number"
    },
    "other_modifications_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete."
    },
    "other_planned_modifications": {
        "type": "boolean",
        "description": "Indicates whether there are there other modifications planned for the generator."
    },
    "other_total": {
        "type": "number",
        "description": "Total Other Production Plant (FERC Accounts 340-347)."
    },
    "outages_recorded_automatically": {
        "type": "boolean"
    },
    "owned_by_non_utility": {
        "type": "boolean",
        "description": "Whether any part of generator is owned by a nonutilty",
    },
    "owner_city": {
        "type": "string",
        "description": "City of owner."
    },
    "owner_name": {
        "type": "string",
        "description": "Name of owner."
    },
    "owner_state": {
        "type": "string",
        "description": "Two letter US & Canadian state and territory abbreviations.",
        "constraints": {
            "enum": sorted({
                **US_STATES_TERRITORIES,
                **CANADA_PROVINCES_TERRITORIES,
            }.keys())
        }
    },
    "owner_street_address": {
        "type": "string",
        "description": "Steet address of owner."
    },
    "owner_utility_id_eia": {
        "type": "integer",
        "description": "EIA-assigned owner's identification number."
    },
    "owner_zip_code": {
        "type": "string",
        "description": "Zip code of owner."
        # TODO Standardize with other zip codes and apply pattern constraint
    },
    "ownership_code": {
        "type": "string",
        "description": "Identifies the ownership for each generator."
    },
    "peak_demand_mw": {
        "type": "number",
        "description": "Net peak demand for 60 minutes. Note: in some cases peak demand for other time periods may have been reported instead, if hourly peak demand was unavailable.",
        # TODO Disambiguate column names. Usually this is over 60 minutes, but in
        # other tables it's not specified.
    },
    "peak_demand_summer_mw": {
        "type": "number"
    },
    "peak_demand_winter_mw": {
        "type": "number"
    },
    "phone_extension": {
        "type": "string",
        "description": "Phone extension for utility contact 1"
    },
    "phone_extension_2": {
        "type": "string",
        "description": "Phone extension for utility contact 2"
    },
    "phone_number": {
        "type": "string",
        "description": "Phone number for utility contact 1."
    },
    "phone_number_2": {
        "type": "string",
        "description": "Phone number for utility contact 2."
    },
    "pipeline_notes": {
        "type": "string",
        "description": "Additional owner or operator of natural gas pipeline."
    },
    "planned_derate_date": {
        "type": "date",
        "description": "Planned effective month that the generator is scheduled to enter operation after the derate modification."
    },
    "planned_energy_source_code_1": {
        "type": "string",
        "description": "New energy source code for the planned repowered generator."
    },
    "planned_modifications": {
        "type": "boolean",
        "description": "Indicates whether there are any planned capacity uprates/derates, repowering, other modifications, or generator retirements scheduled for the next 5 years."
    },
    "planned_net_summer_capacity_derate_mw": {
        "type": "number",
        "description": "Decrease in summer capacity expected to be realized from the derate modification to the equipment."
    },
    "planned_net_summer_capacity_uprate_mw": {
        "type": "number",
        "description": "Increase in summer capacity expected to be realized from the modification to the equipment."
    },
    "planned_net_winter_capacity_derate_mw": {
        "type": "number",
        "description": "Decrease in winter capacity expected to be realized from the derate modification to the equipment."
    },
    "planned_net_winter_capacity_uprate_mw": {
        "type": "number",
        "description": "Increase in winter capacity expected to be realized from the uprate modification to the equipment."
    },
    "planned_new_capacity_mw": {
        "type": "number",
        "description": "The expected new namplate capacity for the generator."
    },
    "planned_new_prime_mover_code": {
        "type": "string",
        "description": "New prime mover for the planned repowered generator."
    },
    "planned_repower_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the repowering is complete."
    },
    "planned_retirement_date": {
        "type": "date",
        "description": "Planned effective date of the scheduled retirement of the generator."
    },
    "planned_uprate_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the uprate modification."
    },
    "plant_capability_mw": {
        "type": "number",
        "description": "Net plant capability in megawatts.",
    },
    "plant_hours_connected_while_generating": {
        "type": "number",
        "description": "Hours the plant was connected to load while generating in the report year.",
        # TODO Add min/max constraint. 0 <= X <= 8784
    },
    "plant_id_eia": {
        "type": "integer",
        "description": "The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration."
    },
    "plant_id_epa": {
        "type": "integer",
        "description": "The ORISPL ID used by EPA to refer to the plant. Usually but not always the same as plant_id_eia.",
    },
    "plant_id_ferc1": {
        "type": "integer",
        "description": "Algorithmically assigned PUDL FERC Plant ID. WARNING: NOT STABLE BETWEEN PUDL DB INITIALIZATIONS."
    },
    "plant_id_pudl": {
        "type": "integer",
        "description": "A manually assigned PUDL plant ID. May not be constant over time."
    },
    "plant_name_eia": {
        "type": "string",
        "description": "Plant name."
    },
    "plant_name_ferc1": {
        "type": "string",
        "description": "Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.",
    },
    "plant_name_clean": {
        "type": "string",
        "description": "A semi-manually cleaned version of the freeform FERC 1 plant name."
    },
    "plant_name_pudl": {
        "type": "string",
        "description": "Plant name, chosen arbitrarily from the several possible plant names available in the plant matching process. Included for human readability only."
    },
    "plant_type": {
        "type": "string"
        # TODO Disambiguate column name and apply table specific ENUM constraints. There
        # are different allowable values in different tables.
    },
    "plants_reported_asset_manager": {
        "type": "boolean",
        "description": "Is the reporting entity an asset manager of power plants reported on Schedule 2 of the form?"
    },
    "plants_reported_operator": {
        "type": "boolean",
        "description": "Is the reporting entity an operator of power plants reported on Schedule 2 of the form?"
    },
    "plants_reported_other_relationship": {
        "type": "boolean",
        "description": "Does the reporting entity have any other relationship to the power plants reported on Schedule 2 of the form?"
    },
    "plants_reported_owner": {
        "type": "boolean",
        "description": "Is the reporting entity an owner of power plants reported on Schedule 2 of the form?"
    },
    "potential_peak_demand_savings_mw": {
        "type": "number"
    },
    "previously_canceled": {
        "type": "boolean",
        "description": "Indicates whether the generator was previously reported as indefinitely postponed or canceled"
    },
    "price_responsive_programes": {
        "type": "boolean"
    },
    "price_responsiveness_customers": {
        "type": "integer"
    },
    "primary_purpose_id_naics": {
        "type": "integer",
        "description": "North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant"
    },
    "primary_transportation_mode_code": {
        "type": "string",
        "description": "Transportation mode for the longest distance transported.",
    },
    "prime_mover": {
        "type": "string",
        "description": "Full description of the type of prime mover."
    },
    "prime_mover_code": {
        "type": "string",
        "description": "Code for the type of prime mover (e.g. CT, CG)",
    },
    "production_total": {
        "type": "number",
        "description": "Total Production Plant (FERC Accounts 310-347)."
    },
    "project_num": {
        "type": "integer",
        "description": "FERC Licensed Project Number."
    },
    "pulverized_coal_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses pulverized coal technology"
    },
    "purchase_type": {
        "type": "string",
        "description": "Categorization based on the original contractual terms and conditions of the service. Must be one of 'requirements', 'long_firm', 'intermediate_firm', 'short_firm', 'long_unit', 'intermediate_unit', 'electricity_exchange', 'other_service', or 'adjustment'. Requirements service is ongoing high reliability service, with load integrated into system resource planning. 'Long term' means 5+ years. 'Intermediate term' is 1-5 years. 'Short term' is less than 1 year. 'Firm' means not interruptible for economic reasons. 'unit' indicates service from a particular designated generating unit. 'exchange' is an in-kind transaction.",
        "constraints": {
            "enum": list(POWER_PURCHASE_TYPES_FERC1.values())
        }
    },
    "purchased_mwh": {
        "type": "number",
        "description": "Megawatt-hours shown on bills rendered to the respondent."
    },
    "pv_current_flow_type": {
        "type": "string",
        "constraints": {
            "enum": ["AC", "DC"]
        }
    },
    "reactive_power_output_mvar": {
        "type": "number",
        "description": "Reactive Power Output (MVAr)",
    },
    "real_time_pricing_program": {
        "type": "boolean"
    },
    "rec_revenue": {
        "type": "number"
    },
    "rec_sales_mwh": {
        "type": "number"
    },
    "received_mwh": {
        "type": "number",
        "description": "Gross megawatt-hours received in power exchanges and used as the basis for settlement."
    },
    "record_id": {
        "type": "string",
        "description": "Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped."  # noqa: FS003
    },
    "regulatory_status_code": {
        "type": "string",
        "description": "Indicates whether the plant is regulated or non-regulated."
    },
    "report_date": {
        "type": "date",
        "description": "Date reported."
    },
    "report_year": {
        "type": "year",
        "description": "Four-digit year in which the data was reported."
    },
    "reported_as_another_company": {
        "type": "string"
    },
    "respondent_frequency": {
        "type": "string",
        "constraints": {
            "enum": ["A", "M", "AM"]
        }
    },
    "respondent_id_ferc714": {
        "type": "integer"
    },
    "respondent_name_ferc714": {
        "type": "string"
    },
    "respondent_type": {
        "type": "string",
        "constraints": {
            "enum": ["utility", "balancing_authority"]
        }
    },
    "retail_marketing_activity": {
        "type": "boolean"
    },
    "retail_sales": {
        "type": "number"
    },
    "retail_sales_mwh": {
        "type": "number"
    },
    "retirement_date": {
        "type": "date",
        "description": "Date of the scheduled or effected retirement of the generator."
    },
    "revenue_class": {
        "type": "string",
        "constraints": {
            "enum": REVENUE_CLASSES
        }
    },
    "rtmo_acct380_land": {
        "type": "number",
        "description": "FERC Account 380: RTMO Land and Land Rights."
    },
    "rtmo_acct381_structures": {
        "type": "number",
        "description": "FERC Account 381: RTMO Structures and Improvements."
    },
    "rtmo_acct382_computer_hardware": {
        "type": "number",
        "description": "FERC Account 382: RTMO Computer Hardware."
    },
    "rtmo_acct383_computer_software": {
        "type": "number",
        "description": "FERC Account 383: RTMO Computer Software."
    },
    "rtmo_acct384_communication_equip": {
        "type": "number",
        "description": "FERC Account 384: RTMO Communication Equipment."
    },
    "rtmo_acct385_misc_equip": {
        "type": "number",
        "description": "FERC Account 385: RTMO Miscellaneous Equipment."
    },
    "rtmo_total": {
        "type": "number",
        "description": "Total RTMO Plant (FERC Accounts 380-386)"
    },
    "rto_iso_lmp_node_id": {
        "type": "string",
        "description": "The designation used to identify the price node in RTO/ISO Locational Marginal Price reports"
    },
    "rto_iso_location_wholesale_reporting_id": {
        "type": "string",
        "description": "The designation used to report ths specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report"
    },
    "rtos_of_operation": {
        "type": "string",
        "constraints": {
            "enum": RTO_CLASSES
        }
    },
    "saidi_w_major_event_dats_minus_loss_of_service_minutes": {
        "type": "number"
    },
    "saidi_w_major_event_days_minutes": {
        "type": "number"
    },
    "saidi_wo_major_event_days_minutes": {
        "type": "number"
    },
    "saifi_w_major_event_days_customers": {
        "type": "number"
    },
    "saifi_w_major_event_days_minus_loss_of_service_customers": {
        "type": "number"
    },
    "saifi_wo_major_event_days_customers": {
        "type": "number"
    },
    "sales_for_resale": {
        "type": "number"
    },
    "sales_for_resale_mwh": {
        "type": "number"
    },
    "sales_mwh": {
        "type": "number"
    },
    "sales_revenue": {
        "type": "number"
    },
    "sales_to_ultimate_consumers_mwh": {
        "type": "number"
    },
    "secondary_transportation_mode_code": {
        "type": "string",
        "description": "Transportation mode for the second longest distance transported.",
    },
    "sector_id_eia": {
        "type": "integer",
        "description": "EIA assigned sector ID, corresponding to high level NAICS sector, designated by the primary purpose, regulatory status and plant-level combined heat and power status"
    },
    "sector_name_eia": {
        "type": "string",
        "description": "EIA assigned sector name, corresponding to high level NAICS sector, designated by the primary purpose, regulatory status and plant-level combined heat and power status"
    },
    "seller_name": {
        "type": "string",
        "description": "Name of the seller, or the other party in an exchange transaction."
    },
    "service_area": {
        "type": "string",
        "description": "Service area in which plant is located; for unregulated companies, it's the electric utility with which plant is interconnected",
    },
    "service_type": {
        "type": "string",
        "constraints": {
            "enum": ["bundled", "energy", "delivery"]
        }
    },
    "short_form": {
        "type": "boolean"
    },
    "so2_mass_lbs": {
        "type": "number",
        "description": "Sulfur dioxide emissions in pounds."
    },
    "so2_mass_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {
            "enum": EPACEMS_MEASUREMENT_CODES
        }
    },
    "sold_to_utility_mwh": {
        "type": "number"
    },
    "solid_fuel_gasification": {
        "type": "boolean",
        "description": "Indicates whether the generator is part of a solid fuel gasification system"
    },
    "standard": {
        "type": "string",
        "constraints": {
            "enum": RELIABILITY_STANDARDS
        }
    },
    "startup_source_code_1": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    "startup_source_code_2": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    "startup_source_code_3": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    "startup_source_code_4": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator."
    },
    "state": {
        "type": "string"
        # TODO: disambiguate the column name. State means different things in
        # different tables. E.g. state of the utility's HQ address vs. state that a
        # plant is located in vs. state in which a utility provides service.
    },
    "state_id_fips": {
        "type": "string",
        # TODO: add pattern / length constraint.
    },
    "status": {
        "type": "string"
        # TODO: Disambiguate column name.
    },
    "steam_acct310_land": {
        "type": "number",
        "description": "FERC Account 310: Steam Plant Land and Land Rights."
    },
    "steam_acct311_structures": {
        "type": "number",
        "description": "FERC Account 311: Steam Plant Structures and Improvements."
    },
    "steam_acct312_boiler_equip": {
        "type": "number",
        "description": "FERC Account 312: Steam Boiler Plant Equipment."
    },
    "steam_acct313_engines": {
        "type": "number",
        "description": "FERC Account 313: Steam Engines and Engine-Driven Generators."
    },
    "steam_acct314_turbogenerators": {
        "type": "number",
        "description": "FERC Account 314: Steam Turbogenerator Units."
    },
    "steam_acct315_accessory_equip": {
        "type": "number",
        "description": "FERC Account 315: Steam Accessory Electric Equipment."
    },
    "steam_acct316_misc_equip": {
        "type": "number",
        "description": "FERC Account 316: Steam Miscellaneous Power Plant Equipment."
    },
    "steam_acct317_asset_retirement": {
        "type": "number",
        "description": "FERC Account 317: Asset Retirement Costs for Steam Production."
    },
    "steam_load_1000_lbs": {
        "type": "number",
        "description": "Total steam pressure produced by a unit during the reported hour."
    },
    "steam_total": {
        "type": "number",
        "description": "Total Steam Production Plant (FERC Accounts 310-317)."
    },
    "stoker_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses stoker technology"
    },
    "storage_capacity_mw": {
        "type": "number"
    },
    "storage_customers": {
        "type": "integer"
    },
    "street_address": {
        "type": "string",
        # TODO: Disambiguate as this means different things in different tables.
    },
    "subcritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses subcritical technology"
    },
    "sulfur_content_pct": {
        "type": "number",
        "description": "Sulfur content percentage by weight to the nearest 0.01 percent."
    },
    "summer_capacity_estimate": {
        "type": "boolean",
        "description": "Whether the summer capacity value was an estimate",
    },
    "summer_capacity_mw": {
        "type": "number",
        "description": "The net summer capacity."
    },
    "summer_estimated_capability_mw": {
        "type": "number",
        "description": "EIA estimated summer capacity (in MWh)."
    },
    "summer_peak_demand_mw": {
        "type": "number"
    },
    "supercritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses supercritical technology"
    },
    "supplier_name": {
        "type": "string",
        "description": "Company that sold the fuel to the plant or, in the case of Natural Gas, pipline owner."
    },
    "switch_oil_gas": {
        "type": "boolean",
        "description": "Indicates whether the generator switch between oil and natural gas."
    },
    "syncronized_transmission_grid": {
        "type": "boolean",
        "description": "Indicates whether standby generators (SB status) can be synchronized to the grid."
    },
    "tariff": {
        "type": "string",
        "description": "FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)"
    },
    "tech_class": {
        "type": "string",
        "constraints": {
            "enum": TECH_CLASSES
        }
    },
    "technology_description": {
        "type": "string",
        "description": "High level description of the technology used by the generator to produce electricity."
    },
    "time_cold_shutdown_full_load_code": {
        "type": "string",
        "description": "The minimum amount of time required to bring the unit to full load from shutdown."
    },
    "time_of_use_pricing_program": {
        "type": "boolean"
    },
    "time_responsive_programs": {
        "type": "boolean"
    },
    "time_responsiveness_customers": {
        "type": "integer"
    },
    "timezone": {
        "type": "string",
        "description": "IANA timezone name",
        "constraints": {
            "enum": all_timezones
        }
    },
    "topping_bottoming_code": {
        "type": "string",
        "description": "If the generator is associated with a combined heat and power system, indicates whether the generator is part of a topping cycle or a bottoming cycle"
    },
    "total": {
        "type": "number",
        "description": "Total of Electric Plant In Service, Electric Plant Held for Future Use, and Electric Plant Leased to Others (USD)."
    },
    "total_capacity_less_1_mw": {
        "type": "number"
    },
    "total_cost_of_plant": {
        "type": "number",
        "description": "Total cost of plant (USD)."
    },
    "total_disposition_mwh": {
        "type": "number"
    },
    "total_energy_losses_mwh": {
        "type": "number"
    },
    "total_meters": {
        "type": "integer"
    },
    "total_settlement": {
        "type": "number",
        "description": "Sum of demand, energy, and other charges (USD). For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative."
    },
    "total_sources_mwh": {
        "type": "number"
    },
    "transmission": {
        "type": "number"
    },
    "transmission_acct350_land": {
        "type": "number",
        "description": "FERC Account 350: Transmission Land and Land Rights."
    },
    "transmission_acct352_structures": {
        "type": "number",
        "description": "FERC Account 352: Transmission Structures and Improvements."
    },
    "transmission_acct353_station_equip": {
        "type": "number",
        "description": "FERC Account 353: Transmission Station Equipment."
    },
    "transmission_acct354_towers": {
        "type": "number",
        "description": "FERC Account 354: Transmission Towers and Fixtures."
    },
    "transmission_acct355_poles": {
        "type": "number",
        "description": "FERC Account 355: Transmission Poles and Fixtures."
    },
    "transmission_acct356_overhead_conductors": {
        "type": "number",
        "description": "FERC Account 356: Overhead Transmission Conductors and Devices."
    },
    "transmission_acct357_underground_conduit": {
        "type": "number",
        "description": "FERC Account 357: Underground Transmission Conduit."
    },
    "transmission_acct358_underground_conductors": {
        "type": "number",
        "description": "FERC Account 358: Underground Transmission Conductors."
    },
    "transmission_acct359_1_asset_retirement": {
        "type": "number",
        "description": "FERC Account 359.1: Asset Retirement Costs for Transmission Plant."
    },
    "transmission_acct359_roads_trails": {
        "type": "number",
        "description": "FERC Account 359: Transmission Roads and Trails."
    },
    "transmission_activity": {
        "type": "boolean"
    },
    "transmission_by_other_losses_mwh": {
        "type": "number"
    },
    "transmission_distribution_owner_id": {
        "type": "integer",
        "description": "EIA-assigned code for owner of transmission/distribution system to which the plant is interconnected."
    },
    "transmission_distribution_owner_name": {
        "type": "string",
        "description": "Name of the owner of the transmission or distribution system to which the plant is interconnected."
    },
    "transmission_distribution_owner_state": {
        "type": "string",
        "description": "State location for owner of transmission/distribution system to which the plant is interconnected."
    },
    "transmission_total": {
        "type": "number",
        "description": "Total Transmission Plant (FERC Accounts 350-359.1)"
    },
    "turbines_inverters_hydrokinetics": {
        "type": "integer",
        "description": "Number of wind turbines, or hydrokinetic buoys."
    },
    "turbines_num": {
        "type": "integer",
        "description": "Number of wind turbines, or hydrokinetic buoys."
    },
    "ultrasupercritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses ultra-supercritical technology"
    },
    "unbundled_revenues": {
        "type": "number"
    },
    "unit_id_eia": {
        "type": "string",
        "description": "EIA-assigned unit identification code."
    },
    "unit_id_epa": {
        "type": "string",
        "description": "Emissions (smokestake) unit monitored by EPA CEMS."
    },
    "unit_id_pudl": {
        "type": "integer",
        "description": "Dynamically assigned PUDL unit id. WARNING: This ID is not guaranteed to be static long term as the input data and algorithm may evolve over time.",
    },
    "unitid": {
        "type": "string",
        "description": "Facility-specific unit id (e.g. Unit 4)"
    },
    "uprate_derate_completed_date": {
        "type": "date",
        "description": "The date when the uprate or derate was completed."
    },
    "uprate_derate_during_year": {
        "type": "boolean",
        "description": "Was an uprate or derate completed on this generator during the reporting year?"
    },
    "utc_datetime": {
        "type": "datetime"
    },
    "utility_attn": {
        "type": "string"
    },
    "utility_id_eia": {
        "type": "integer",
        "description": "The EIA Utility Identification number.",
        # TODO: Disambiguate column name. In some cases this specifically refers to
        # the utility which operates a given plant or generator, but comes from the
        # same set of IDs as all the utility IDs.
        # E.g. in ownership_eia860 or generators_eia860 it would be something like:
        # "description": "EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.",
    },
    "utility_id_ferc1": {
        "type": "integer",
        "description": "FERC-assigned respondent_id, identifying the reporting entity. Stable from year to year."
    },
    "utility_id_pudl": {
        "type": "integer",
        "description": "A manually assigned PUDL utility ID. May not be stable over time."
    },
    "utility_name_eia": {
        "type": "string",
        "description": "The name of the utility."
    },
    "utility_name_ferc1": {
        "type": "string",
        "description": "Name of the responding utility, as it is reported in FERC Form 1. For human readability only."
    },
    "utility_name_pudl": {
        "type": "string",
        "description": "Utility name, chosen arbitrarily from the several possible utility names available in the utility matching process. Included for human readability only."
    },
    "utility_owned_capacity_mw": {
        "type": "number"
    },
    "utility_pobox": {
        "type": "string"
    },
    "utility_zip_ext": {
        "type": "string",
        # TODO: Standardize with other zip codes and impose pattern constraint.
    },
    "variable_peak_pricing_program": {
        "type": "boolean"
    },
    "virtual_capacity_mw": {
        "type": "number"
    },
    "virtual_customers": {
        "type": "integer"
    },
    "water_heater": {
        "type": "integer"
    },
    "water_limited_capacity_mw": {
        "type": "number",
        "description": "Plant capacity in MW when limited by condenser water."
    },
    "water_source": {
        "type": "string",
        "description": "Name of water source associated with the plant."
    },
    "weighted_average_life_years": {
        "type": "number"
    },
    "wheeled_power_delivered_mwh": {
        "type": "number"
    },
    "wheeled_power_recieved_mwh": {
        "type": "number"
    },
    "wholesale_marketing_activity": {
        "type": "boolean"
    },
    "wholesale_power_purchases_mwh": {
        "type": "number"
    },
    "winter_capacity_estimate": {
        "type": "boolean",
        "description": "Whether the winter capacity value was an estimate",
    },
    "winter_capacity_mw": {
        "type": "number",
        "description": "The net winter capacity."
    },
    "winter_estimated_capability_mw": {
        "type": "number",
        "description": "EIA estimated winter capacity (in MWh)."
    },
    "winter_peak_demand_mw": {
        "type": "number"
    },
    "zip_code": {
        "type": "string",
        "description": "Five digit US Zip Code."
        # TODO Standardize with other zip codes and apply pattern constraint
    },
    "zip_code_4": {
        "type": "string",
        "description": "Four digit US Zip Code suffix."
        # TODO Standardize with other zip codes and apply pattern constraint
    }
}
"""
Field attributes by PUDL identifier (`field.name`).

Keys are in alphabetical order.
"""

FIELD_METADATA_BY_GROUP: Dict[str, Dict[str, Any]] = {
    "epacems": {
        "state": {
            "constraints": {
                "enum": EPACEMS_STATES
            }
        }
    },
    "eia": {
        "fuel_units": {
            "constraints": {
                "enum": sorted(FUEL_UNITS_EIA.keys())
            }
        }
    },
    "ferc1": {
        "fuel_units": {
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
        }
    }
}
"""
Field attributes by resource group (`resource.group`) and PUDL identifier.

If a field exists in more than one data group (e.g. both ``eia`` and ``ferc1``)
and has distinct metadata in those groups, this is the place to specify the
override. Only those elements which should be overridden need to be specified.
"""

FIELD_METADATA_BY_RESOURCE: Dict[str, Dict[str, Any]] = {}
