"""Field metadata."""
from copy import deepcopy
from typing import Any

import pandas as pd
from pytz import all_timezones

from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.constants import FIELD_DTYPES_PANDAS
from pudl.metadata.enums import (
    COUNTRY_CODES_ISO3166,
    CUSTOMER_CLASSES,
    DIVISION_CODES_US_CENSUS,
    EPACEMS_MEASUREMENT_CODES,
    EPACEMS_STATES,
    FUEL_CLASSES,
    NERC_REGIONS,
    PLANT_PARTS,
    RELIABILITY_STANDARDS,
    REVENUE_CLASSES,
    RTO_CLASSES,
    SUBDIVISION_CODES_ISO3166,
    TECH_CLASSES,
    TECH_DESCRIPTIONS,
)
from pudl.metadata.labels import (
    ESTIMATED_OR_ACTUAL,
    FUEL_UNITS_EIA,
    MOMENTARY_INTERRUPTIONS,
)
from pudl.metadata.sources import SOURCES

# from pudl.transform.params.ferc1 import (
#    PLANT_TYPE_CATEGORIES,
#    PLANT_TYPE_CATEGORIES_HYDRO,
# )

FIELD_METADATA: dict[str, dict[str, Any]] = {
    "active": {
        "type": "boolean",
        "description": "Indicates whether or not the dataset has been pulled into PUDL by the extract transform load process.",
    },
    "actual_peak_demand_savings_mw": {"type": "number", "unit": "MW"},
    "additions": {
        "type": "number",
        "description": "Cost of acquisition of items classified within the account.",
        "unit": "USD",
    },
    "address_2": {"type": "string"},
    "adjustments": {
        "type": "number",
        "description": "Cost of adjustments to the account.",
        "unit": "USD",
    },
    "advanced_metering_infrastructure": {"type": "integer"},
    "alternative_fuel_vehicle_2_activity": {"type": "boolean"},
    "alternative_fuel_vehicle_activity": {"type": "boolean"},
    "annual_indirect_program_cost": {"type": "number", "unit": "USD"},
    "annual_total_cost": {"type": "number", "unit": "USD"},
    "appro_part_label": {
        "type": "string",
        "description": "Plant part of the associated true granularity record.",
        "constraints": {"enum": PLANT_PARTS},
    },
    "appro_record_id_eia": {
        "type": "string",
        "description": "EIA record ID of the associated true granularity record.",
    },
    "ash_content_pct": {
        "type": "number",
        "description": "Ash content percentage by weight to the nearest 0.1 percent.",
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
        "description": "If there is an ash impoundment at the plant, the ash impoundment status as of December 31 of the reporting year.",
    },
    "asset_retirement_cost": {
        "type": "number",
        "description": "Asset retirement cost (USD).",
        "unit": "USD",
    },
    "associated_combined_heat_power": {
        "type": "boolean",
        "description": "Indicates whether the generator is associated with a combined heat and power system",
    },
    "attention_line": {"type": "string"},
    "automated_meter_reading": {"type": "integer"},
    "avg_num_employees": {"type": "number"},
    "backup_capacity_mw": {"type": "number", "unit": "MW"},
    "balancing_authority_code_eia": {
        "type": "string",
        "description": "EIA short code identifying a balancing authority.",
    },
    "balancing_authority_id_eia": {
        "type": "integer",
        "description": "EIA balancing authority ID. This is often (but not always!) the same as the utility ID associated with the same legal entity.",
    },
    "balancing_authority_name_eia": {
        "type": "string",
        "description": "Name of the balancing authority.",
    },
    "bga_source": {
        "type": "string",
        "description": "The source from where the unit_id_pudl is compiled. The unit_id_pudl comes directly from EIA 860, or string association (which looks at all the boilers and generators that are not associated with a unit and tries to find a matching string in the respective collection of boilers or generator), or from a unit connection (where the unit_id_eia is employed to find additional boiler generator connections).",
    },
    "billing_demand_mw": {
        "type": "number",
        "description": "Monthly average billing demand (for requirements purchases, and any transactions involving demand charges). In megawatts.",
        "unit": "MW",
    },
    "boiler_generator_assn_type_code": {
        "type": "string",
        "description": (
            "Indicates whether boiler associations with generator during the year were "
            "actual or theoretical. Only available before 2013."
        ),
    },
    "boiler_id": {
        "type": "string",
        "description": "Alphanumeric boiler ID.",
    },
    "bundled_activity": {"type": "boolean"},
    "business_model": {
        "type": "string",
        "constraints": {"enum": ["retail", "energy_services"]},
    },
    "buying_distribution_activity": {"type": "boolean"},
    "buying_transmission_activity": {"type": "boolean"},
    "bypass_heat_recovery": {
        "type": "boolean",
        "description": "Can this generator operate while bypassing the heat recovery steam generator?",
    },
    "caidi_w_major_event_days_minutes": {"type": "number", "unit": "min"},
    "caidi_w_major_event_days_minus_loss_of_service_minutes": {
        "type": "number",
        "unit": "min",
    },
    "caidi_wo_major_event_days_minutes": {"type": "number", "unit": "min"},
    "capacity_eoy_mw": {
        "type": "number",
        "description": "Total end of year installed (nameplate) capacity for a plant part, in megawatts.",
        "unit": "MW",
    },
    "capacity_factor": {
        "type": "number",
        "description": "Fraction of potential generation that was actually reported for a plant part.",
    },
    "capacity_mw": {
        "type": "number",
        "description": "Total installed (nameplate) capacity, in megawatts.",
        "unit": "MW",
        # TODO: Disambiguate if necessary. Does this mean different things in
        # different tables? It shows up in a lot of places.
    },
    "capex_equipment": {
        "type": "number",
        "description": "Cost of plant: equipment (USD).",
        "unit": "USD",
    },
    "capex_equipment_electric": {
        "type": "number",
        "description": "Cost of plant: accessory electric equipment (USD).",
        "unit": "USD",
    },
    "capex_equipment_misc": {
        "type": "number",
        "description": "Cost of plant: miscellaneous power plant equipment (USD).",
        "unit": "USD",
    },
    "capex_facilities": {
        "type": "number",
        "description": "Cost of plant: reservoirs, dams, and waterways (USD).",
        "unit": "USD",
    },
    "capex_land": {
        "type": "number",
        "description": "Cost of plant: land and land rights (USD).",
        "unit": "USD",
    },
    "capex_per_mw": {
        "type": "number",
        "description": "Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD.",
        "unit": "USD_per_MW",
    },
    "capex_roads": {
        "type": "number",
        "description": "Cost of plant: roads, railroads, and bridges (USD).",
        "unit": "USD",
    },
    "capex_structures": {
        "type": "number",
        "description": "Cost of plant: structures and improvements (USD).",
        "unit": "USD",
    },
    "capex_total": {
        "type": "number",
        "description": "Total cost of plant (USD).",
        "unit": "USD",
    },
    "capex_wheels_turbines_generators": {
        "type": "number",
        "description": "Cost of plant: water wheels, turbines, and generators (USD).",
        "unit": "USD",
    },
    "carbon_capture": {
        "type": "boolean",
        "description": "Indicates whether the generator uses carbon capture technology.",
    },
    "chlorine_content_ppm": {"type": "number", "unit": "ppm"},
    "circuits_with_voltage_optimization": {"type": "integer"},
    "city": {
        "type": "string",
        # TODO: Disambiguate column. City means different things in different tables.
    },
    "co2_mass_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {"enum": EPACEMS_MEASUREMENT_CODES},
    },
    "co2_mass_tons": {
        "type": "number",
        "description": "Carbon dioxide emissions in short tons.",
        "unit": "short_ton",
    },
    "code": {
        "type": "string",
        "description": "Originally reported short code.",
    },
    "cofire_fuels": {
        "type": "boolean",
        "description": "Can the generator co-fire fuels?.",
    },
    "coincident_peak_demand_mw": {
        "type": "number",
        "description": "Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier's system reaches its monthly peak. In megawatts.",
        "unit": "MW",
    },
    "construction_type": {
        "type": "string",
        "description": "Type of plant construction ('outdoor', 'semioutdoor', or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings.",
        "constraints": {"enum": ["conventional", "outdoor", "semioutdoor"]},
    },
    "construction_year": {
        "type": "integer",
        "description": "Year the plant's oldest still operational unit was built.",
    },
    "consumed_by_facility_mwh": {"type": "number", "unit": "MWh"},
    "consumed_by_respondent_without_charge_mwh": {"type": "number", "unit": "MWh"},
    "contact_firstname": {
        "type": "string",
        "description": "First name of utility contact 1.",
    },
    "contact_firstname_2": {
        "type": "string",
        "description": "First name of utility contact 2.",
    },
    "contact_lastname": {
        "type": "string",
        "description": "Last name of utility contact 1.",
    },
    "contact_lastname_2": {
        "type": "string",
        "description": "Last name of utility contact 2.",
    },
    "contact_title": {
        "type": "string",
        "description": "Title of of utility contact 1.",
    },
    "contact_title_2": {"type": "string", "description": "Title of utility contact 2."},
    "contract_expiration_date": {
        "type": "date",
        "description": "Date contract expires.Format:  MMYY.",
    },
    "contract_type_code": {
        "type": "string",
        "description": "Purchase type under which receipts occurred in the reporting month. C: Contract, NC: New Contract, S: Spot Purchase, T: Tolling Agreement.",
        "constraints": {"enum": ["S", "C", "NC", "T"]},
    },
    "county": {"type": "string", "description": "County name."},
    "county_id_fips": {
        "type": "string",
        "description": "County ID from the Federal Information Processing Standard Publication 6-4.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "country_code": {
        "type": "string",
        "description": "Three letter ISO-3166 country code (e.g. USA or CAN).",
        "constraints": {"enum": COUNTRY_CODES_ISO3166},
    },
    "country_name": {
        "type": "string",
        "description": "Full country name (e.g. United States of America).",
    },
    "credits_or_adjustments": {"type": "number"},
    "critical_peak_pricing": {"type": "boolean"},
    "critical_peak_rebate": {"type": "boolean"},
    "current_planned_operating_date": {
        "type": "date",
        "description": "The most recently updated effective date on which the generator is scheduled to start operation",
    },
    "customer_class": {
        "type": "string",
        "description": "High level categorization of customer type.",
        "constraints": {"enum": CUSTOMER_CLASSES},
    },
    "customer_incentives_cost": {"type": "number"},
    "customer_incentives_incremental_cost": {"type": "number"},
    "customer_incentives_incremental_life_cycle_cost": {"type": "number"},
    "customer_other_costs_incremental_life_cycle_cost": {"type": "number"},
    "customers": {"description": "Number of customers.", "type": "number"},
    "daily_digital_access_customers": {"type": "integer"},
    "data_observed": {
        "type": "boolean",
        "description": "Is the value observed (True) or imputed (False).",
    },
    "data_maturity": {
        "type": "string",
        "description": "Level of maturity of the data record. Some data sources report less-than-final data. PUDL sometimes includes this data, but use at your own risk.",
    },
    "datasource": {
        "type": "string",
        "description": "Code identifying a dataset available within PUDL.",
        "constraints": {"enum": list(SOURCES)},
    },
    "datum": {
        "type": "string",
        "description": "Geodetic coordinate system identifier (e.g. NAD27, NAD83, or WGS84).",
    },
    "deliver_power_transgrid": {
        "type": "boolean",
        "description": "Indicate whether the generator can deliver power to the transmission grid.",
    },
    "delivered_mwh": {
        "type": "number",
        "description": "Gross megawatt-hours delivered in power exchanges and used as the basis for settlement.",
        "unit": "MWh",
    },
    "delivery_customers": {"type": "number"},
    "demand_annual_mwh": {"type": "number", "unit": "MWh"},
    "demand_charges": {
        "type": "number",
        "description": "Demand charges (USD).",
        "unit": "USD",
    },
    "demand_mwh": {"type": "number", "unit": "MWh"},
    "description": {
        "type": "string",
        "description": "Long human-readable description of the meaning of a code/label.",
    },
    "doi": {
        "type": "string",
        "description": "Unique digitial object identifier of Zenodo archive.",
    },
    "ending_balance": {
        "type": "number",
        "description": "Account balance at end of year.",
        "unit": "USD",
    },
    "energy_mwh": {
        "type": "number",
        "unit": "MWh",
        "description": "Sources and uses of energy in MWh.",
    },
    "energy_source_type": {
        "type": "string",
        "description": "Type of energy source reported in the electric_energy_sources_ferc1 table. There are three groups of energy sources: generation, power exchanges and transmission.",
    },
    "energy_disposition_type": {
        "type": "string",
        "description": "Type of energy disposition reported in the electric_energy_dispositions_ferc1. Dispositions include sales to customers, re-sales of energy, energy used internally, losses, etc.",
    },
    "ferc_account_description": {"type": "string"},
    "fuel_agg": {
        "type": "string",
        "description": "Category of fuel aggregation in EIA bulk electricity data.",
    },
    "geo_agg": {
        "type": "string",
        "description": "Category of geographic aggregation in EIA bulk electricity data.",
    },
    "sector_agg": {
        "type": "string",
        "description": "Category of sectoral aggregation in EIA bulk electricity data.",
    },
    "temporal_agg": {
        "type": "string",
        "description": "Category of temporal aggregation in EIA bulk electricity data.",
    },
    "fuel_received_mmbtu": {
        "type": "number",
        "description": "Aggregated fuel receipts, in MMBtu, in EIA bulk electricity data.",
        "unit": "MMBtu",
    },
    "direct_load_control_customers": {"type": "integer"},
    "distributed_generation": {
        "type": "boolean",
        "description": "Whether the generator is considered distributed generation",
    },
    "distributed_generation_owned_capacity_mw": {"type": "number", "unit": "MW"},
    "distribution_activity": {"type": "boolean"},
    "distribution_circuits": {"type": "integer"},
    "division_code_us_census": {
        "type": "string",
        "description": (
            "Three-letter US Census division code as it appears in the bulk "
            "electricity data published by the EIA. Note that EIA splits the Pacific "
            "division into distinct contiguous (CA, OR, WA) and non-contiguous (AK, "
            "HI) states. For reference see this US Census region and division map: "
            "https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf"
        ),
        "constraints": {"enum": DIVISION_CODES_US_CENSUS},
    },
    "division_name_us_census": {
        "type": "string",
        "description": "Longer human readable name describing the US Census division.",
    },
    "duct_burners": {
        "type": "boolean",
        "description": "Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas",
    },
    "eia_code": {"type": "integer"},
    "electric_plant": {
        "type": "number",
        "description": "Electric Plant In Service (USD).",
        "unit": "USD",
    },
    "emissions_unit_id_epa": {
        "type": "string",
        "description": "Emissions (smokestack) unit monitored by EPA CEMS.",
    },
    "energy_charges": {
        "type": "number",
        "description": "Energy charges (USD).",
        "unit": "USD",
    },
    "energy_displaced_mwh": {"type": "number", "unit": "MWh"},
    "energy_efficiency_annual_actual_peak_reduction_mw": {
        "type": "number",
        "unit": "MW",
    },
    "energy_efficiency_annual_cost": {"type": "number"},
    "energy_efficiency_annual_effects_mwh": {"type": "number", "unit": "MWh"},
    "energy_efficiency_annual_incentive_payment": {"type": "number"},
    "energy_efficiency_incremental_actual_peak_reduction_mw": {
        "type": "number",
        "unit": "MW",
    },
    "energy_efficiency_incremental_effects_mwh": {"type": "number", "unit": "MWh"},
    "energy_savings_estimates_independently_verified": {"type": "boolean"},
    "energy_savings_independently_verified": {"type": "boolean"},
    "energy_savings_mwh": {"type": "number", "unit": "MWh"},
    "energy_served_ami_mwh": {"type": "number", "unit": "MWh"},
    "energy_source": {"type": "string"},
    "energy_source_code": {
        "type": "string",
        "description": "The fuel code associated with the fuel receipt. Two or three character alphanumeric.",
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
        "description": "The code representing the most predominant type of energy that fuels the generator.",
    },
    "energy_source_code_2": {
        "type": "string",
        "description": "The code representing the second most predominant type of energy that fuels the generator",
    },
    "energy_source_code_3": {
        "type": "string",
        "description": "The code representing the third most predominant type of energy that fuels the generator",
    },
    "energy_source_code_4": {
        "type": "string",
        "description": "The code representing the fourth most predominant type of energy that fuels the generator",
    },
    "energy_source_code_5": {
        "type": "string",
        "description": "The code representing the fifth most predominant type of energy that fuels the generator",
    },
    "energy_source_code_6": {
        "type": "string",
        "description": "The code representing the sixth most predominant type of energy that fuels the generator",
    },
    "energy_storage": {
        "type": "boolean",
        "description": "Indicates if the facility has energy storage capabilities."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "energy_storage_capacity_mwh": {
        "type": "number",
        "description": "Energy storage capacity in MWh (e.g. for batteries).",
        "unit": "MWh",
    },
    "energy_used_for_pumping_mwh": {
        "type": "number",
        "description": "Energy used for pumping, in megawatt-hours.",
        "unit": "MWh",
    },
    "entity_type": {
        "type": "string",
        "description": "Entity type of principal owner.",
    },
    "estimated_or_actual_capacity_data": {
        "type": "string",
        "constraints": {"enum": list(ESTIMATED_OR_ACTUAL.values())},
    },
    "estimated_or_actual_fuel_data": {
        "type": "string",
        "constraints": {"enum": list(ESTIMATED_OR_ACTUAL.values())},
    },
    "estimated_or_actual_tech_data": {
        "type": "string",
        "constraints": {"enum": list(ESTIMATED_OR_ACTUAL.values())},
    },
    "exchange_energy_delivered_mwh": {"type": "number", "unit": "MWh"},
    "exchange_energy_received_mwh": {"type": "number", "unit": "MWh"},
    "ferc_account": {
        "type": "string",
        "description": "Actual FERC Account number (e.g. '359.1') if available, or a PUDL assigned ID when FERC accounts have been split or combined in reporting.",
    },
    "ferc_account_id": {
        "type": "string",
        "description": "Account identifier from FERC's Uniform System of Accounts for Electric Plant. Includes higher level labeled categories.",
    },
    "ferc_account_label": {
        "type": "string",
        "description": "Long FERC account identifier derived from values reported in the XBRL taxonomies. May also refer to aggregations of individual FERC accounts.",
    },
    "ferc_acct_name": {
        "type": "string",
        "description": "Name of FERC account, derived from technology description and prime mover code.",
        "constraints": {"enum": ["Hydraulic", "Nuclear", "Steam", "Other"]},
    },
    "ferc_cogen_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC cogenerator status. See FERC Form 556.",
    },
    "ferc_cogen_status": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility cogenerator status. See FERC Form 556."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ferc_exempt_wholesale_generator": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility exempt wholesale generator status"
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ferc_exempt_wholesale_generator_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility exempt wholesale generator status.",
    },
    "license_id_ferc1": {
        "type": "integer",
        "description": "FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible.",
    },
    "ferc_small_power_producer": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility small power producer status. See FERC Form 556.",
    },
    "ferc_small_power_producer_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility small power producer status. See FERC Form 556.",
    },
    "ferc_qualifying_facility_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility cogenerator status. See FERC Form 556.",
    },
    "ferc_qualifying_facility": {
        "type": "boolean",
        "description": "Indicatates whether or not a generator is a qualifying FERC cogeneation facility.",
    },
    "fluidized_bed_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses fluidized bed technology",
    },
    "fraction_owned": {
        "type": "number",
        "description": "Proportion of generator ownership.",
    },
    "fuel_class": {
        "type": "string",
        # TODO: Needs a description / better name. EIA-861 distributed generation only.
        "constraints": {"enum": FUEL_CLASSES},
    },
    "fuel_consumed_for_electricity_mmbtu": {
        "type": "number",
        "description": "Total consumption of fuel to produce electricity, in physical units, year to date.",
        "unit": "MMBtu",
    },
    "fuel_consumed_for_electricity_units": {
        "type": "number",
        "description": "Consumption for electric generation of the fuel type in physical units.",
    },
    "fuel_consumed_mmbtu": {
        "type": "number",
        "description": "Total consumption of fuel in physical units, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.",
        "unit": "MMBtu",
    },
    "fuel_consumed_units": {
        "type": "number",
        "description": "Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.",
    },
    "fuel_cost_per_mmbtu": {
        "type": "number",
        "description": "Average fuel cost per mmBTU of heat content in nominal USD.",
        "unit": "USD_per_MMBtu",
    },
    "fuel_cost_per_mwh": {
        "type": "number",
        "description": "Derived from MCOE, a unit level value. Average fuel cost per MWh of heat content in nominal USD.",
        "unit": "USD_per_MWh",
    },
    "fuel_cost_per_unit_burned": {
        "type": "number",
        "description": "Average cost of fuel consumed in the report year per reported fuel unit (USD).",
        "unit": "USD",
    },
    "fuel_cost_per_unit_delivered": {
        "type": "number",
        "description": "Average cost of fuel delivered in the report year per reported fuel unit (USD).",
        "unit": "USD",
    },
    "fuel_derived_from": {
        "type": "string",
        "description": "Original fuel from which this refined fuel was derived.",
        "constraints": {
            "enum": sorted(
                set(CODE_METADATA["energy_sources_eia"]["df"]["fuel_derived_from"])
            )
        },
    },
    "fuel_group_code": {
        "type": "string",
        "description": "Fuel groups used in the Electric Power Monthly",
        "constraints": {
            "enum": ["petroleum", "other_gas", "petroleum_coke", "natural_gas", "coal"]
        },
    },
    "fuel_group_eia": {
        "type": "string",
        "description": "High level fuel group defined in the 2021-2023 EIA Form 860 instructions, Table 28.",
        "constraints": {
            "enum": sorted(
                set(CODE_METADATA["energy_sources_eia"]["df"]["fuel_group_eia"])
            )
        },
    },
    "fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Heat content of the fuel in millions of Btus per physical unit.",
        "unit": "MMBtu_per_unit",
    },
    "fuel_pct": {"type": "number"},
    "fuel_phase": {
        "type": "string",
        "description": "Physical phase of matter of the fuel.",
        "constraints": {
            "enum": sorted(
                set(CODE_METADATA["energy_sources_eia"]["df"]["fuel_phase"].dropna())
            )
        },
    },
    "fuel_received_units": {
        "type": "number",
        "description": "Quanity of fuel received in tons, barrel, or Mcf.",
    },
    "fuel_type": {
        "type": "string",
        # TODO disambiguate column name. This should be just FERC 1 tables, as the EIA
        # fuel types are now all energy_source_code
    },
    "fuel_type_code_aer": {
        "type": "string",
        "description": "A partial aggregation of the reported fuel type codes into larger categories used by EIA in, for example, the Annual Energy Review (AER). Two or three letter alphanumeric.",
    },
    "fuel_type_code_pudl": {
        "type": "string",
        "description": "Simplified fuel type code used in PUDL",
        "constraints": {
            "enum": sorted(
                set(CODE_METADATA["energy_sources_eia"]["df"].fuel_type_code_pudl)
            )
        },
    },
    "fuel_units": {
        "type": "string",
        "description": "Reported units of measure for fuel.",
        # Note: Different ENUM constraints are applied below on EIA vs. FERC1
    },
    "furnished_without_charge_mwh": {"type": "number", "unit": "MWh"},
    "future_plant": {
        "type": "number",
        "description": "Electric Plant Held for Future Use (USD).",
        "unit": "USD",
    },
    "generation_activity": {"type": "boolean"},
    "generator_id": {
        "type": "string",
        "description": (
            "Generator ID is usually numeric, but sometimes includes letters. Make "
            "sure you treat it as a string!"
        ),
    },
    "generator_id_epa": {
        "type": "string",
        "description": "Generator ID used by the EPA.",
    },
    "generators_num_less_1_mw": {"type": "number", "unit": "MW"},
    "generators_number": {"type": "number"},
    "green_pricing_revenue": {"type": "number", "unit": "USD"},
    "grid_voltage_1_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities",
        "unit": "kV",
    },
    "grid_voltage_2_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities",
        "unit": "kV",
    },
    "grid_voltage_3_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distibution facilities",
        "unit": "kV",
    },
    "gross_load_mw": {
        "type": "number",
        "description": "Average power in megawatts delivered during time interval measured.",
        "unit": "MW",
    },
    "heat_content_mmbtu": {
        "type": "number",
        "description": "The energy contained in fuel burned, measured in million BTU.",
        "unit": "MMBtu",
    },
    "heat_rate_mmbtu_mwh": {
        "type": "number",
        "description": "Fuel content per unit of electricity generated. Coming from MCOE calculation.",
        "unit": "MMBtu_MWh",
    },
    "highest_distribution_voltage_kv": {"type": "number", "unit": "kV"},
    "home_area_network": {"type": "integer"},
    "inactive_accounts_included": {"type": "boolean"},
    "incremental_energy_savings_mwh": {"type": "number", "unit": "MWh"},
    "incremental_life_cycle_energy_savings_mwh": {"type": "number", "unit": "MWh"},
    "incremental_life_cycle_peak_reduction_mwh": {"type": "number", "unit": "MWh"},
    "incremental_peak_reduction_mw": {"type": "number", "unit": "MW"},
    "installation_year": {
        "type": "integer",
        "description": "Year the plant's most recently built unit was installed.",
    },
    "is_epacems_state": {
        "type": "boolean",
        "description": (
            "Indicates whether the associated state reports data within the EPA's "
            "Continuous Emissions Monitoring System."
        ),
    },
    "is_total": {
        "type": "boolean",
        "description": "Indicates whether the row is a total.",
    },
    "iso_rto_code": {
        "type": "string",
        "description": "The code of the plant's ISO or RTO. NA if not reported in that year.",
    },
    "label": {
        "type": "string",
        "description": "Longer human-readable code using snake_case",
    },
    "latitude": {
        "type": "number",
        "description": "Latitude of the plant's location, in degrees.",
    },
    "leased_plant": {
        "type": "number",
        "description": "Electric Plant Leased to Others (USD).",
        "unit": "USD",
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
    "load_management_annual_actual_peak_reduction_mw": {"type": "number", "unit": "MW"},
    "load_management_annual_cost": {"type": "number"},
    "load_management_annual_effects_mwh": {"type": "number", "unit": "MWh"},
    "load_management_annual_incentive_payment": {"type": "number"},
    "load_management_annual_potential_peak_reduction_mw": {
        "type": "number",
        "unit": "MW",
    },
    "load_management_incremental_actual_peak_reduction_mw": {
        "type": "number",
        "unit": "MW",
    },
    "load_management_incremental_effects_mwh": {"type": "number", "unit": "MWh"},
    "load_management_incremental_potential_peak_reduction_mw": {
        "type": "number",
        "unit": "MW",
    },
    "longitude": {
        "type": "number",
        "description": "Longitude of the plant's location, in degrees.",
    },
    "major_program_changes": {"type": "boolean"},
    "max_fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Maximum heat content per physical unit of fuel in MMBtu.",
        "unit": "MMBtu",
    },
    "mercury_content_ppm": {
        "type": "number",
        "description": "Mercury content in parts per million (ppm) to the nearest 0.001 ppm.",
        "unit": "ppm",
    },
    "merge_address": {"type": "string"},
    "merge_city": {"type": "string"},
    "merge_company": {"type": "string"},
    "merge_date": {"type": "date"},
    "merge_state": {
        "type": "string",
        "description": "Two letter US state abbreviations and three letter ISO-3166-1 country codes for international mines.",
        # TODO: Add ENUM constraint.
    },
    "min_fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Minimum heat content per physical unit of fuel in MMBtu.",
        "unit": "MMBtu",
    },
    "mine_id_msha": {"type": "integer", "description": "MSHA issued mine identifier."},
    "mine_id_pudl": {
        "type": "integer",
        "description": "Dynamically assigned PUDL mine identifier.",
    },
    "mine_name": {"type": "string", "description": "Coal mine name."},
    "mine_type_code": {
        "type": "string",
        "description": "Type of coal mine.",
    },
    "minimum_load_mw": {
        "type": "number",
        "description": "The minimum load at which the generator can operate at continuosuly.",
        "unit": "MW",
    },
    "fuel_transportation_mode": {"type": "string"},
    "moisture_content_pct": {"type": "number"},
    "momentary_interruption_definition": {
        "type": "string",
        "constraints": {"enum": list(MOMENTARY_INTERRUPTIONS.values())},
    },
    "month": {"type": "integer", "description": "Month of the year"},
    "multiple_fuels": {
        "type": "boolean",
        "description": "Can the generator burn multiple fuels?",
    },
    "nameplate_power_factor": {
        "type": "number",
        "description": "The nameplate power factor of the generator.",
    },
    "natural_gas_delivery_contract_type_code": {
        "type": "string",
        "description": "Contract type for natrual gas delivery service:",
        "constraints": {"enum": ["firm", "interruptible"]},
    },
    "natural_gas_local_distribution_company": {
        "type": "string",
        "description": "Names of Local Distribution Company (LDC), connected to natural gas burning power plants.",
    },
    "natural_gas_pipeline_name_1": {
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.",
    },
    "natural_gas_pipeline_name_2": {
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.",
    },
    "natural_gas_pipeline_name_3": {
        "type": "string",
        "description": "The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.",
    },
    "natural_gas_storage": {
        "type": "boolean",
        "description": "Indicates if the facility have on-site storage of natural gas."
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "natural_gas_transport_code": {
        "type": "string",
        "description": "Contract type for natural gas transportation service.",
        "constraints": {"enum": ["firm", "interruptible"]},
    },
    "nerc_region": {
        "type": "string",
        "description": "NERC region in which the plant is located",
        "constraints": {"enum": NERC_REGIONS},
    },
    "nerc_regions_of_operation": {
        "type": "string",
        "constraints": {"enum": NERC_REGIONS},
    },
    "net_capacity_adverse_conditions_mw": {
        "type": "number",
        "description": "Net plant capability under the least favorable operating conditions, in megawatts.",
        "unit": "MW",
    },
    "net_capacity_favorable_conditions_mw": {
        "type": "number",
        "description": "Net plant capability under the most favorable operating conditions, in megawatts.",
        "unit": "MW",
    },
    "net_capacity_mwdc": {
        "type": "number",
        "description": (
            "Generation capacity in megawatts of direct current that is subject to a "
            "net metering agreement. Typically used for behind-the-meter solar PV."
        ),
        "unit": "MW",
    },
    "net_generation_mwh": {
        "type": "number",
        "description": "Net electricity generation for the specified period in megawatt-hours (MWh).",
        "unit": "MWh",
        # TODO: disambiguate as this column means something different in
        # generation_fuel_eia923:
        # "description": "Net generation, year to date in megawatthours (MWh). This is total electrical output net of station service.  In the case of combined heat and power plants, this value is intended to include internal consumption of electricity for the purposes of a production process, as well as power put on the grid.",
    },
    "net_load_mwh": {
        "type": "number",
        "description": "Net output for load (net generation - energy used for pumping) in megawatt-hours.",
        "unit": "MWh",
    },
    "net_metering": {
        "type": "boolean",
        "description": "Did this plant have a net metering agreement in effect during the reporting year?  (Only displayed for facilities that report the sun or wind as an energy source). This field was only reported up until 2015"
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "net_power_exchanged_mwh": {"type": "number", "unit": "MWh"},
    "net_wheeled_power_mwh": {"type": "number", "unit": "MWh"},
    "new_parent": {"type": "string"},
    "non_amr_ami": {"type": "integer"},
    "non_coincident_peak_demand_mw": {
        "type": "number",
        "description": "Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts.",
        "unit": "MW",
    },
    "notes": {
        "type": "string",
        "description": "Notes previously in the plant_name_ferc1 field that were extracted and associated with the right plant row.",
    },
    "not_water_limited_capacity_mw": {
        "type": "number",
        "description": "Plant capacity in MW when not limited by condenser water.",
        "unit": "MW",
    },
    "nox_mass_lbs": {
        "type": "number",
        "description": "NOx emissions in pounds.",
        "unit": "lb",
    },
    "nox_mass_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {"enum": EPACEMS_MEASUREMENT_CODES},
    },
    "nuclear_unit_id": {
        "type": "string",
        "description": "For nuclear plants only, the unit number .One digit numeric. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level.",
    },
    "operates_generating_plant": {"type": "boolean"},
    "operating_date": {
        "type": "date",
        "description": "Date the generator began commercial operation",
    },
    "operating_datetime_utc": {
        "type": "datetime",
        "description": "Date and time measurement began (UTC).",
    },
    "operating_switch": {
        "type": "string",
        "description": "Indicates whether the fuel switching generator can switch when operating",
    },
    "operating_time_hours": {
        "type": "number",
        "description": "Length of time interval measured.",
        "unit": "hr",
    },
    "operating_year": {
        "type": "integer",
        "description": "Year a generator went into service.",
    },
    "operational_status": {
        "type": "string",
        "description": "The operating status of the generator. This is based on which tab the generator was listed in in EIA 860.",
    },
    "operational_status_code": {
        "type": "string",
        "description": "The operating status of the generator.",
    },
    "operational_status_pudl": {
        "type": "string",
        "description": "The operating status of the generator using PUDL categories.",
        "constraints": {"enum": ["operating", "retired", "proposed"]},
    },
    "opex_allowances": {"type": "number", "description": "Allowances.", "unit": "USD"},
    "opex_boiler": {
        "type": "number",
        "description": "Maintenance of boiler (or reactor) plant.",
        "unit": "USD",
    },
    "opex_coolants": {
        "type": "number",
        "description": "Cost of coolants and water (nuclear plants only)",
        "unit": "USD",
    },
    "opex_dams": {
        "type": "number",
        "description": "Production expenses: maintenance of reservoirs, dams, and waterways (USD).",
        "unit": "USD",
    },
    "opex_electric": {
        "type": "number",
        "description": "Production expenses: electric expenses (USD).",
        "unit": "USD",
    },
    "opex_engineering": {
        "type": "number",
        "description": "Production expenses: maintenance, supervision, and engineering (USD).",
        "unit": "USD",
    },
    "opex_fuel": {
        "type": "number",
        "description": "Production expenses: fuel (USD).",
        "unit": "USD",
    },
    "opex_generation_misc": {
        "type": "number",
        "description": "Production expenses: miscellaneous power generation expenses (USD).",
        "unit": "USD",
    },
    "opex_hydraulic": {
        "type": "number",
        "description": "Production expenses: hydraulic expenses (USD).",
        "unit": "USD",
    },
    "opex_maintenance": {
        "type": "number",
        "description": "Production expenses: Maintenance (USD).",
        "unit": "USD",
    },
    "opex_misc_plant": {
        "type": "number",
        "description": "Production expenses: maintenance of miscellaneous hydraulic plant (USD).",
        "unit": "USD",
    },
    "opex_misc_power": {
        "type": "number",
        "description": "Miscellaneous steam (or nuclear) expenses.",
        "unit": "USD",
    },
    "opex_misc_steam": {
        "type": "number",
        "description": "Maintenance of miscellaneous steam (or nuclear) plant.",
        "unit": "USD",
    },
    "opex_operations": {
        "type": "number",
        "description": "Production expenses: operations, supervision, and engineering (USD).",
        "unit": "USD",
    },
    "opex_per_mwh": {
        "type": "number",
        "description": "Total production expenses (USD per MWh generated).",
        "unit": "USD per MWh",
    },
    "opex_plant": {
        "type": "number",
        "description": "Production expenses: maintenance of electric plant (USD).",
        "unit": "USD",
    },
    "opex_plants": {
        "type": "number",
        "description": "Maintenance of electrical plant.",
        "unit": "USD",
    },
    "opex_production_before_pumping": {
        "type": "number",
        "description": "Total production expenses before pumping (USD).",
        "unit": "USD",
    },
    "opex_production_total": {
        "type": "number",
        "description": "Total operating expenses.",
        "unit": "USD",
    },
    "opex_pumped_storage": {
        "type": "number",
        "description": "Production expenses: pumped storage (USD).",
        "unit": "USD",
    },
    "opex_pumping": {
        "type": "number",
        "description": "Production expenses: We are here to PUMP YOU UP! (USD).",
        "unit": "USD",
    },
    "opex_rents": {
        "type": "number",
        "description": "Production expenses: rents (USD).",
        "unit": "USD",
    },
    "opex_steam": {"type": "number", "description": "Steam expenses.", "unit": "USD"},
    "opex_steam_other": {
        "type": "number",
        "description": "Steam from other sources.",
        "unit": "USD",
    },
    "opex_structures": {
        "type": "number",
        "description": "Production expenses: maintenance of structures (USD).",
        "unit": "USD",
    },
    "opex_total": {
        "type": "number",
        "description": "Total production expenses, excluding fuel (USD).",
        "unit": "USD",
    },
    "opex_transfer": {"type": "number", "description": "Steam transferred (Credit)."},
    "opex_water_for_power": {
        "type": "number",
        "description": "Production expenses: water for power (USD).",
        "unit": "USD",
    },
    "original_planned_operating_date": {
        "type": "date",
        "description": "The date the generator was originally scheduled to be operational",
    },
    "other": {"type": "number"},
    "other_charges": {
        "type": "number",
        "description": "Other charges, including out-of-period adjustments (USD).",
        "unit": "USD",
    },
    "other_combustion_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses other combustion technologies",
    },
    "other_costs": {"type": "number", "unit": "USD"},
    "other_costs_incremental_cost": {"type": "number", "unit": "USD"},
    "other_modifications_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete.",
    },
    "other_planned_modifications": {
        "type": "boolean",
        "description": "Indicates whether there are there other modifications planned for the generator.",
    },
    "other_total": {
        "type": "number",
        "description": "Total Other Production Plant (FERC Accounts 340-347).",
    },
    "outages_recorded_automatically": {"type": "boolean"},
    "owned_by_non_utility": {
        "type": "boolean",
        "description": "Whether any part of generator is owned by a nonutilty",
    },
    "owner_city": {"type": "string", "description": "City of owner."},
    "owner_country": {
        "type": "string",
        "description": "Three letter ISO-3166 country code.",
        "constraints": {"enum": COUNTRY_CODES_ISO3166},
    },
    "owner_name": {"type": "string", "description": "Name of owner."},
    "owner_state": {
        "type": "string",
        "description": "Two letter ISO-3166 political subdivision code.",
        "constraints": {"enum": SUBDIVISION_CODES_ISO3166},
    },
    "owner_street_address": {
        "type": "string",
        "description": "Steet address of owner.",
    },
    "owner_utility_id_eia": {
        "type": "integer",
        "description": "EIA-assigned owner's identification number.",
    },
    "owner_zip_code": {
        "type": "string",
        "description": "Zip code of owner.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "ownership_record_type": {
        "type": "string",
        "description": "Whether each generator record is for one owner or represents a total of all ownerships.",
        "constraints": {"enum": ["owned", "total"]},
    },
    "ownership_code": {
        "type": "string",
        "description": "Identifies the ownership for each generator.",
    },
    "ownership_dupe": {
        "type": "boolean",
        "description": "Whether a plant part record has a duplicate record with different ownership status.",
    },
    "partitions": {
        "type": "string",
        "description": "The data parititions used to generate this instance of the database.",
    },
    "peak_demand_mw": {
        "type": "number",
        "unit": "MW",
        "description": "Net peak demand for 60 minutes. Note: in some cases peak demand for other time periods may have been reported instead, if hourly peak demand was unavailable.",
        # TODO Disambiguate column names. Usually this is over 60 minutes, but in
        # other tables it's not specified.
    },
    "peak_demand_summer_mw": {"type": "number", "unit": "MW"},
    "peak_demand_winter_mw": {"type": "number", "unit": "MW"},
    "phone_extension": {
        "type": "string",
        "description": "Phone extension for utility contact 1",
    },
    "phone_extension_2": {
        "type": "string",
        "description": "Phone extension for utility contact 2",
    },
    "phone_number": {
        "type": "string",
        "description": "Phone number for utility contact 1.",
    },
    "phone_number_2": {
        "type": "string",
        "description": "Phone number for utility contact 2.",
    },
    "pipeline_notes": {
        "type": "string",
        "description": "Additional owner or operator of natural gas pipeline.",
    },
    "planned_derate_date": {
        "type": "date",
        "description": "Planned effective month that the generator is scheduled to enter operation after the derate modification.",
    },
    "planned_energy_source_code_1": {
        "type": "string",
        "description": "New energy source code for the planned repowered generator.",
    },
    "planned_modifications": {
        "type": "boolean",
        "description": "Indicates whether there are any planned capacity uprates/derates, repowering, other modifications, or generator retirements scheduled for the next 5 years.",
    },
    "planned_net_summer_capacity_derate_mw": {
        "type": "number",
        "description": "Decrease in summer capacity expected to be realized from the derate modification to the equipment.",
        "unit": "MW",
    },
    "planned_net_summer_capacity_uprate_mw": {
        "type": "number",
        "description": "Increase in summer capacity expected to be realized from the modification to the equipment.",
        "unit": "MW",
    },
    "planned_net_winter_capacity_derate_mw": {
        "type": "number",
        "description": "Decrease in winter capacity expected to be realized from the derate modification to the equipment.",
        "unit": "MW",
    },
    "planned_net_winter_capacity_uprate_mw": {
        "type": "number",
        "description": "Increase in winter capacity expected to be realized from the uprate modification to the equipment.",
        "unit": "MW",
    },
    "planned_new_capacity_mw": {
        "type": "number",
        "description": "The expected new namplate capacity for the generator.",
        "unit": "MW",
    },
    "planned_new_prime_mover_code": {
        "type": "string",
        "description": "New prime mover for the planned repowered generator.",
    },
    "planned_repower_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the repowering is complete.",
    },
    "planned_retirement_date": {
        "type": "date",
        "description": "Planned effective date of the scheduled retirement of the generator.",
    },
    "planned_uprate_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the uprate modification.",
    },
    "plant_capability_mw": {
        "type": "number",
        "description": "Net plant capability in megawatts.",
        "unit": "MW",
    },
    "plant_hours_connected_while_generating": {
        "type": "number",
        "description": "Hours the plant was connected to load while generating in the report year.",
        "unit": "hr",
        # TODO Add min/max constraint. 0 <= X <= 8784
    },
    "plant_id_eia": {
        "type": "integer",
        "description": "The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.",
    },
    "plant_id_epa": {
        "type": "integer",
        "description": "The ORISPL ID used by EPA to refer to the plant. Usually but not always the same as plant_id_eia.",
    },
    "plant_id_ferc1": {
        "type": "integer",
        "description": "Algorithmically assigned PUDL FERC Plant ID. WARNING: NOT STABLE BETWEEN PUDL DB INITIALIZATIONS.",
    },
    "plant_id_pudl": {
        "type": "integer",
        "description": "A manually assigned PUDL plant ID. May not be constant over time.",
    },
    "plant_id_report_year": {
        "type": "string",
        "description": "PUDL plant ID and report year of the record.",
    },
    "plant_name_eia": {"type": "string", "description": "Plant name."},
    "plant_name_ferc1": {
        "type": "string",
        "description": "Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.",
    },
    "plant_name_ppe": {
        "type": "string",
        "description": "Derived plant name that includes EIA plant name and other strings associated with ID and PK columns of the plant part.",
    },
    "plant_name_pudl": {
        "type": "string",
        "description": "Plant name, chosen arbitrarily from the several possible plant names available in the plant matching process. Included for human readability only.",
    },
    "plant_part": {
        "type": "string",
        "description": "The part of the plant a record corresponds to.",
        "constraints": {"enum": PLANT_PARTS},
    },
    "plant_part_id_eia": {
        "type": "string",
        "description": "Contains EIA plant ID, plant part, ownership, and EIA utility id",
    },
    "plant_type": {
        "type": "string"  # if plant_type is categorized w/ categorize_strings, add enum in FIELD_METADATA_BY_RESOURCE
    },
    "plants_reported_asset_manager": {
        "type": "boolean",
        "description": "Is the reporting entity an asset manager of power plants reported on Schedule 2 of the form?",
    },
    "plants_reported_operator": {
        "type": "boolean",
        "description": "Is the reporting entity an operator of power plants reported on Schedule 2 of the form?",
    },
    "plants_reported_other_relationship": {
        "type": "boolean",
        "description": "Does the reporting entity have any other relationship to the power plants reported on Schedule 2 of the form?",
    },
    "plants_reported_owner": {
        "type": "boolean",
        "description": "Is the reporting entity an owner of power plants reported on Schedule 2 of the form?",
    },
    "potential_peak_demand_savings_mw": {"type": "number", "unit": "MW"},
    "previously_canceled": {
        "type": "boolean",
        "description": "Indicates whether the generator was previously reported as indefinitely postponed or canceled",
    },
    "price_responsive_programs": {"type": "boolean"},
    "price_responsiveness_customers": {"type": "integer"},
    "primary_purpose_id_naics": {
        "type": "integer",
        "description": "North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant",
    },
    "primary_transportation_mode_code": {
        "type": "string",
        "description": "Transportation mode for the longest distance transported.",
    },
    "prime_mover": {
        "type": "string",
        "description": "Full description of the type of prime mover.",
    },
    "prime_mover_code": {
        "type": "string",
        "description": "Code for the type of prime mover (e.g. CT, CG)",
    },
    "project_num": {"type": "integer", "description": "FERC Licensed Project Number."},
    "pudl_version": {
        "type": "string",
        "description": "The version of PUDL used to generate this database.",
    },
    "pulverized_coal_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses pulverized coal technology",
    },
    "purchase_type_code": {
        "type": "string",
        "description": "Categorization based on the original contractual terms and conditions of the service. Must be one of 'requirements', 'long_firm', 'intermediate_firm', 'short_firm', 'long_unit', 'intermediate_unit', 'electricity_exchange', 'other_service', or 'adjustment'. Requirements service is ongoing high reliability service, with load integrated into system resource planning. 'Long term' means 5+ years. 'Intermediate term' is 1-5 years. 'Short term' is less than 1 year. 'Firm' means not interruptible for economic reasons. 'unit' indicates service from a particular designated generating unit. 'exchange' is an in-kind transaction.",
    },
    "purchased_mwh": {
        "type": "number",
        "description": "Megawatt-hours shown on bills rendered to the respondent.",
        "unit": "MWh",
    },
    "pv_current_flow_type": {"type": "string", "constraints": {"enum": ["AC", "DC"]}},
    "reactive_power_output_mvar": {
        "type": "number",
        "description": "Reactive Power Output (MVAr)",
        "unit": "MVAr",
    },
    "real_time_pricing": {"type": "boolean"},
    "rec_revenue": {"type": "number", "unit": "USD"},
    "rec_sales_mwh": {"type": "number", "unit": "MWh"},
    "received_mwh": {
        "type": "number",
        "description": "Gross megawatt-hours received in power exchanges and used as the basis for settlement.",
        "unit": "MWh",
    },
    "record_count": {
        "type": "integer",
        "description": "Number of distinct generator IDs that partcipated in the aggregation for a plant part list record.",
    },
    "record_id": {
        "type": "string",
        "description": "Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.",  # noqa: FS003
    },
    "region_name_us_census": {
        "type": "string",
        "description": "Human-readable name of a US Census region.",
    },
    "regulatory_status_code": {
        "type": "string",
        "description": "Indicates whether the plant is regulated or non-regulated.",
    },
    "report_date": {"type": "date", "description": "Date reported."},
    "report_year": {
        "type": "integer",
        "description": "Four-digit year in which the data was reported.",
    },
    "reported_as_another_company": {"type": "string"},
    "reporting_frequency_code": {
        "type": "string",
        "description": "Code that specifies what time period data has to be reported (i.e. monthly data or annual totals) and how often the power plant reports this data to EIA. See reporting_frequencies_eia for more details.",
        "constraints": {
            "enum": sorted(
                set(CODE_METADATA["reporting_frequencies_eia"]["df"]["code"])
            )
        },
    },
    "respondent_id_ferc714": {"type": "integer"},
    "respondent_name_ferc714": {"type": "string"},
    "respondent_type": {
        "type": "string",
        "constraints": {"enum": ["utility", "balancing_authority"]},
    },
    "retail_marketing_activity": {"type": "boolean"},
    "retail_sales": {"type": "number"},
    "retail_sales_mwh": {"type": "number", "unit": "MWh"},
    "retirement_date": {
        "type": "date",
        "description": "Date of the scheduled or effected retirement of the generator.",
    },
    "retirements": {
        "type": "number",
        "description": "Cost of disposal of items classified within the account.",
        "unit": "USD",
    },
    "revenue": {"type": "number", "unit": "USD"},
    "revenue_class": {"type": "string", "constraints": {"enum": REVENUE_CLASSES}},
    "row_type_xbrl": {
        "type": "string",
        "description": "Indicates whether the value reported in the row is calculated, or uniquely reported within the table.",
        "constraints": {"enum": ["calculated_value", "reported_value"]},
    },
    "rto_iso_lmp_node_id": {
        "type": "string",
        "description": "The designation used to identify the price node in RTO/ISO Locational Marginal Price reports",
    },
    "rto_iso_location_wholesale_reporting_id": {
        "type": "string",
        "description": "The designation used to report ths specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report",
    },
    "rtos_of_operation": {"type": "string", "constraints": {"enum": RTO_CLASSES}},
    "saidi_w_major_event_days_minus_loss_of_service_minutes": {
        "type": "number",
        "unit": "min",
    },
    "saidi_w_major_event_days_minutes": {"type": "number", "unit": "min"},
    "saidi_wo_major_event_days_minutes": {"type": "number", "unit": "min"},
    "saifi_w_major_event_days_customers": {"type": "number"},
    "saifi_w_major_event_days_minus_loss_of_service_customers": {"type": "number"},
    "saifi_wo_major_event_days_customers": {"type": "number"},
    "sales_for_resale": {"type": "number"},
    "sales_for_resale_mwh": {"type": "number", "unit": "MWh"},
    "sales_mwh": {
        "description": "Quantity of electricity sold in MWh.",
        "type": "number",
        "unit": "MWh",
    },
    "sales_revenue": {
        "description": "Revenue from electricity sold.",
        "type": "number",
        "unit": "USD",
    },
    "sales_to_ultimate_consumers_mwh": {"type": "number", "unit": "MWh"},
    "secondary_transportation_mode_code": {
        "type": "string",
        "description": "Transportation mode for the second longest distance transported.",
    },
    "sector_id_eia": {
        "type": "integer",
        "description": "EIA assigned sector ID, corresponding to high level NAICS sector, designated by the primary purpose, regulatory status and plant-level combined heat and power status",
    },
    "sector_name_eia": {
        "type": "string",
        "description": "EIA assigned sector name, corresponding to high level NAICS sector, designated by the primary purpose, regulatory status and plant-level combined heat and power status",
    },
    "seller_name": {
        "type": "string",
        "description": "Name of the seller, or the other party in an exchange transaction.",
    },
    "service_area": {
        "type": "string",
        "description": "Service area in which plant is located; for unregulated companies, it's the electric utility with which plant is interconnected",
    },
    "service_type": {
        "type": "string",
        "constraints": {"enum": ["bundled", "energy", "delivery"]},
    },
    "short_form": {"type": "boolean"},
    "so2_mass_lbs": {
        "type": "number",
        "description": "Sulfur dioxide emissions in pounds.",
        "unit": "lb",
    },
    "so2_mass_measurement_code": {
        "type": "string",
        "description": "Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.",
        "constraints": {"enum": EPACEMS_MEASUREMENT_CODES},
    },
    "sold_to_utility_mwh": {"type": "number", "unit": "MWh"},
    "solid_fuel_gasification": {
        "type": "boolean",
        "description": "Indicates whether the generator is part of a solid fuel gasification system",
    },
    "standard": {"type": "string", "constraints": {"enum": RELIABILITY_STANDARDS}},
    "starting_balance": {
        "type": "number",
        "description": "Account balance at beginning of year.",
        "unit": "USD",
    },
    "startup_source_code_1": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.",
    },
    "startup_source_code_2": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.",
    },
    "startup_source_code_3": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.",
    },
    "startup_source_code_4": {
        "type": "string",
        "description": "The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.",
    },
    "state": {
        "type": "string",
        # TODO: disambiguate the column name. State means different things in
        # different tables. E.g. state of the utility's HQ address vs. state that a
        # plant is located in vs. state in which a utility provides service.
        "description": "Two letter US state abbreviation.",
    },
    "state_id_fips": {
        "type": "string",
        "description": "Two digit state FIPS code.",
        "constraints": {
            "pattern": r"^\d{2}$",
        },
    },
    "status": {
        "type": "string"
        # TODO: Disambiguate column name.
    },
    "steam_load_1000_lbs": {
        "type": "number",
        "description": "Total steam pressure produced by a unit during the reported hour.",
        "unit": "lb",
    },
    "steam_plant_type_code": {
        "type": "integer",
        "description": "Code that describes types of steam plants from EIA 860. See steam_plant_types_eia table for more details.",
    },
    "stoker_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses stoker technology",
    },
    "storage_capacity_mw": {"type": "number", "unit": "MW"},
    "storage_customers": {"type": "integer"},
    "street_address": {
        "type": "string",
        # TODO: Disambiguate as this means different things in different tables.
    },
    "subcritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses subcritical technology",
    },
    "subdivision_code": {
        "type": "string",
        "description": (
            "Two-letter ISO-3166 political subdivision code (e.g. US state "
            "or Canadian provice abbreviations like CA or AB)."
        ),
        "constraints": {"enum": SUBDIVISION_CODES_ISO3166},
    },
    "subdivision_name": {
        "type": "string",
        "description": (
            "Full name of political subdivision (e.g. US state or Canadian "
            "province names like California or Alberta."
        ),
    },
    "subdivision_type": {
        "type": "string",
        "description": (
            "ISO-3166 political subdivision type. E.g. state, province, outlying_area."
        ),
    },
    "sulfur_content_pct": {
        "type": "number",
        "description": "Sulfur content percentage by weight to the nearest 0.01 percent.",
    },
    "summer_capacity_estimate": {
        "type": "boolean",
        "description": "Whether the summer capacity value was an estimate",
    },
    "summer_capacity_mw": {
        "type": "number",
        "description": "The net summer capacity.",
        "unit": "MW",
    },
    "summer_estimated_capability_mw": {
        "type": "number",
        "description": "EIA estimated summer capacity (in MWh).",
        "unit": "MWh",
    },
    "summer_peak_demand_mw": {"type": "number", "unit": "MW"},
    "supercritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses supercritical technology",
    },
    "supplier_name": {
        "type": "string",
        "description": "Company that sold the fuel to the plant or, in the case of Natural Gas, pipline owner.",
    },
    "switch_oil_gas": {
        "type": "boolean",
        "description": "Indicates whether the generator switch between oil and natural gas.",
    },
    "syncronized_transmission_grid": {
        "type": "boolean",
        "description": "Indicates whether standby generators (SB status) can be synchronized to the grid.",
    },
    "tariff": {
        "type": "string",
        "description": "FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)",
    },
    "tech_class": {"type": "string", "constraints": {"enum": TECH_CLASSES}},
    "technology_description": {
        "type": "string",
        "description": "High level description of the technology used by the generator to produce electricity.",
    },
    "time_cold_shutdown_full_load_code": {
        "type": "string",
        "description": "The minimum amount of time required to bring the unit to full load from shutdown.",
    },
    "time_of_use_pricing": {"type": "boolean"},
    "time_responsive_programs": {"type": "boolean"},
    "time_responsiveness_customers": {"type": "integer"},
    "timezone": {
        "type": "string",
        "description": "IANA timezone name",
        "constraints": {"enum": all_timezones},
    },
    "timezone_approx": {
        "type": "string",
        "description": (
            "IANA timezone name of the timezone which encompasses the largest portion "
            "of the population in the associated geographic area."
        ),
        "constraints": {"enum": all_timezones},
    },
    "topping_bottoming_code": {
        "type": "string",
        "description": "If the generator is associated with a combined heat and power system, indicates whether the generator is part of a topping cycle or a bottoming cycle",
    },
    "total": {
        "type": "number",
        "description": "Total of Electric Plant In Service, Electric Plant Held for Future Use, and Electric Plant Leased to Others (USD).",
        "unit": "USD",
    },
    "total_capacity_less_1_mw": {"type": "number", "unit": "MW"},
    "total_disposition_mwh": {"type": "number", "unit": "MWh"},
    "total_energy_losses_mwh": {"type": "number", "unit": "MWh"},
    "total_fuel_cost": {
        "type": "number",
        "description": "Total annual reported fuel costs for the plant part. Includes costs from all fuels.",
    },
    "total_meters": {"type": "integer", "unit": "m"},
    "total_mmbtu": {
        "type": "number",
        "description": "Total annual heat content of fuel consumed by a plant part record in the plant parts list.",
    },
    "total_settlement": {
        "type": "number",
        "description": "Sum of demand, energy, and other charges (USD). For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative.",
        "unit": "USD",
    },
    "total_sources_mwh": {"type": "number", "unit": "MWh"},
    "transmission": {"type": "number"},
    "transmission_activity": {"type": "boolean"},
    "transmission_by_other_losses_mwh": {"type": "number", "unit": "MWh"},
    "transmission_distribution_owner_id": {
        "type": "integer",
        "description": "EIA-assigned code for owner of transmission/distribution system to which the plant is interconnected.",
    },
    "transfers": {
        "type": "number",
        "description": "Cost of transfers into (out of) the account.",
        "unit": "USD",
    },
    "transmission_distribution_owner_name": {
        "type": "string",
        "description": "Name of the owner of the transmission or distribution system to which the plant is interconnected.",
    },
    "transmission_distribution_owner_state": {
        "type": "string",
        "description": "State location for owner of transmission/distribution system to which the plant is interconnected.",
    },
    "true_gran": {
        "type": "boolean",
        "description": "Indicates whether a plant part list record is associated with the highest priority plant part for all identical records.",
    },
    "turbines_inverters_hydrokinetics": {
        "type": "integer",
        "description": "Number of wind turbines, or hydrokinetic buoys.",
    },
    "turbines_num": {
        "type": "integer",
        "description": "Number of wind turbines, or hydrokinetic buoys.",
    },
    "ultrasupercritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses ultra-supercritical technology",
    },
    "unbundled_revenues": {"type": "number", "unit": "USD"},
    "unit_id_eia": {
        "type": "string",
        "description": "EIA-assigned unit identification code.",
    },
    "unit_id_pudl": {
        "type": "integer",
        "description": "Dynamically assigned PUDL unit id. WARNING: This ID is not guaranteed to be static long term as the input data and algorithm may evolve over time.",
    },
    "uprate_derate_completed_date": {
        "type": "date",
        "description": "The date when the uprate or derate was completed.",
    },
    "uprate_derate_during_year": {
        "type": "boolean",
        "description": "Was an uprate or derate completed on this generator during the reporting year?",
    },
    "utc_datetime": {"type": "datetime"},
    "utility_attn": {"type": "string"},
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
        "description": "PUDL-assigned utility ID, identifying a FERC1 utility. This is an auto-incremented ID and is not expected to be stable from year to year.",
    },
    "utility_id_ferc1_dbf": {
        "type": "integer",
        "description": "FERC-assigned respondent_id from DBF reporting years, identifying the reporting entity. Stable from year to year.",
    },
    "utility_id_ferc1_xbrl": {
        "type": "string",
        "description": "FERC-assigned entity_id from XBRL reporting years, identifying the reporting entity. Stable from year to year.",
    },
    "utility_id_pudl": {
        "type": "integer",
        "description": "A manually assigned PUDL utility ID. May not be stable over time.",
    },
    "utility_name_eia": {"type": "string", "description": "The name of the utility."},
    "utility_name_ferc1": {
        "type": "string",
        "description": "Name of the responding utility, as it is reported in FERC Form 1. For human readability only.",
    },
    "utility_name_pudl": {
        "type": "string",
        "description": "Utility name, chosen arbitrarily from the several possible utility names available in the utility matching process. Included for human readability only.",
    },
    "utility_owned_capacity_mw": {"type": "number", "unit": "MW"},
    "utility_plant_asset_type": {"type": "string"},  # description
    "utility_plant_value": {"type": "number", "description": "Utility plant value."},
    "utility_pobox": {"type": "string"},
    "utility_type": {"type": "string"},  # enum me + description
    "utility_type_other": {"type": "string"},
    "variable_peak_pricing": {"type": "boolean"},
    "virtual_capacity_mw": {"type": "number", "unit": "MW"},
    "virtual_customers": {"type": "integer"},
    "water_heater": {"type": "integer"},
    "water_limited_capacity_mw": {
        "type": "number",
        "description": "Plant capacity in MW when limited by condenser water.",
        "unit": "MW",
    },
    "water_source": {
        "type": "string",
        "description": "Name of water source associated with the plant.",
    },
    "weighted_average_life_years": {"type": "number"},
    "wheeled_power_delivered_mwh": {"type": "number", "unit": "MWh"},
    "wheeled_power_received_mwh": {"type": "number", "unit": "MWh"},
    "wholesale_marketing_activity": {"type": "boolean"},
    "wholesale_power_purchases_mwh": {"type": "number", "unit": "MWh"},
    "winter_capacity_estimate": {
        "type": "boolean",
        "description": "Whether the winter capacity value was an estimate",
    },
    "winter_capacity_mw": {
        "type": "number",
        "description": "The net winter capacity.",
        "unit": "MW",
    },
    "winter_estimated_capability_mw": {
        "type": "number",
        "description": "EIA estimated winter capacity (in MWh).",
        "unit": "MWh",
    },
    "winter_peak_demand_mw": {"type": "number", "unit": "MW"},
    "year": {
        "type": "integer",
        "description": "Year the data was reported in, used for partitioning EPA CEMS.",
    },
    "zip_code": {
        "type": "string",
        "description": "Five digit US Zip Code.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "zip_code_4": {
        "type": "string",
        "description": "Four digit US Zip Code suffix.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
}
"""Field attributes by PUDL identifier (`field.name`).

Keys are in alphabetical order.
"""

FIELD_METADATA_BY_GROUP: dict[str, dict[str, Any]] = {
    "epacems": {
        "state": {"constraints": {"enum": EPACEMS_STATES}},
        "operating_datetime_utc": {
            "constraints": {
                "required": True,
            }
        },
        "year": {
            "constraints": {
                "required": True,
            }
        },
    },
    "eia": {"fuel_units": {"constraints": {"enum": sorted(FUEL_UNITS_EIA.keys())}}},
    "ferc1": {
        "fuel_units": {
            "constraints": {
                "enum": [
                    "mmbtu",
                    "gramsU",
                    "kg",
                    "mwhth",
                    "kgal",
                    "bbl",
                    "klbs",
                    "mcf",
                    "gal",
                    "mwdth",
                    "btu",
                    "ton",
                ]
            }
        }
    },
}
"""Field attributes by resource group (`resource.group`) and PUDL identifier.

If a field exists in more than one data group (e.g. both ``eia`` and ``ferc1``)
and has distinct metadata in those groups, this is the place to specify the
override. Only those elements which should be overridden need to be specified.
"""

FIELD_METADATA_BY_RESOURCE: dict[str, dict[str, Any]] = {
    "sector_consolidated_eia": {"code": {"type": "integer"}},
    "plants_hydro_ferc1": {
        "plant_type": {
            "type": "string",
            "constraints": {
                # This ENUM is made up of the keys from
                # pudl.transform.params.ferc1.PLANT_TYPE_CATEGORIES_HYDRO, which it
                # would be better to use directly, but we have to work out some cross
                # subpackage import dependencies
                "enum": {
                    "hydro",
                    "na_category",
                    "run_of_river_with_storage",
                    "run_of_river",
                    "storage",
                }
            },
        }
    },
    "plants_steam_ferc1": {
        "plant_type": {
            "type": "string",
            "constraints": {
                # This ENUM is made up of the keys from
                # pudl.transform.params.ferc1.PLANT_TYPE_CATEGORIES, which it
                # would be better to use directly, but we have to work out some cross
                # subpackage import dependencies
                "enum": {
                    "combined_cycle",
                    "combustion_turbine",
                    "geothermal",
                    "internal_combustion",
                    "na_category",
                    "nuclear",
                    "photovoltaic",
                    "solar_thermal",
                    "steam",
                    "wind",
                }
            },
        }
    },
    "plant_parts_eia": {
        "energy_source_code_1": {
            "constraints": {"enum": set(CODE_METADATA["energy_sources_eia"]["df"].code)}
        },
        "prime_movers_eia": {
            "constraints": {"enum": set(CODE_METADATA["prime_movers_eia"]["df"].code)}
        },
        "technology_description": {"constraints": {"enum": set(TECH_DESCRIPTIONS)}},
    },
}


def get_pudl_dtypes(
    group: str | None = None,
    field_meta: dict[str, Any] | None = FIELD_METADATA,
    field_meta_by_group: dict[str, Any] | None = FIELD_METADATA_BY_GROUP,
    dtype_map: dict[str, Any] | None = FIELD_DTYPES_PANDAS,
) -> dict[str, Any]:
    """Compile a dictionary of field dtypes, applying group overrides.

    Args:
        group: The data group (e.g. ferc1, eia) to use for overriding the default
            field types. If None, no overrides are applied and the default types
            are used.
        field_meta: Field metadata dictionary which at least describes a "type".
        field_meta_by_group: Field metadata type overrides to apply based on the data
            group that the field is part of, if any.
        dtype_map: Mapping from canonical PUDL data types to some other set of data
            types. Uses pandas data types by default.

    Returns:
        A mapping of PUDL field names to their associated data types.
    """
    field_meta = deepcopy(field_meta)
    dtypes = {}
    for f in field_meta:
        if f in field_meta_by_group.get(group, []):
            field_meta[f].update(field_meta_by_group[group][f])
        dtypes[f] = dtype_map[field_meta[f]["type"]]

    return dtypes


def apply_pudl_dtypes(
    df: pd.DataFrame,
    group: str | None = None,
    field_meta: dict[str, Any] | None = FIELD_METADATA,
    field_meta_by_group: dict[str, Any] | None = FIELD_METADATA_BY_GROUP,
) -> pd.DataFrame:
    """Apply dtypes to those columns in a dataframe that have PUDL types defined.

    Note at ad-hoc column dtypes can be defined and merged with default PUDL field
    metadata before it's passed in as `field_meta` if you have module specific column
    types you need to apply alongside the standard PUDL field types.

    Args:
        df: The dataframe to apply types to. Not all columns need to have types
            defined in the PUDL metadata.
        group: The data group to use for overrides, if any. E.g. "eia", "ferc1".
        field_meta: A dictionary of field metadata, where each key is a field name
            and the values are dictionaries which must have a "type" element. By
            default this is pudl.metadata.fields.FIELD_METADATA.
        field_meta_by_group: A dictionary of field metadata to use as overrides,
            based on the value of `group`, if any. By default it uses the overrides
            defined in pudl.metadata.fields.FIELD_METADATA_BY_GROUP.

    Returns:
        The input dataframe, but with standard PUDL types applied.
    """
    dtypes = get_pudl_dtypes(
        group=group,
        field_meta=field_meta,
        field_meta_by_group=field_meta_by_group,
        dtype_map=FIELD_DTYPES_PANDAS,
    )

    return df.astype({col: dtypes[col] for col in df.columns if col in dtypes})
