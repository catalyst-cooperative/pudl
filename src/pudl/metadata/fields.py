"""Field metadata."""

from copy import deepcopy
from typing import Any

import pandas as pd
from pytz import all_timezones

from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.constants import FIELD_DTYPES_PANDAS
from pudl.metadata.dfs import BALANCING_AUTHORITY_SUBREGIONS_EIA
from pudl.metadata.enums import (
    ASSET_TYPES_FERC1,
    COUNTRY_CODES_ISO3166,
    CUSTOMER_CLASSES,
    DIVISION_CODES_US_CENSUS,
    ELECTRICITY_MARKET_MODULE_REGIONS,
    ENERGY_DISPOSITION_TYPES_FERC1,
    ENERGY_SOURCE_TYPES_FERC1,
    ENERGY_USE_TYPES_EIAAEO,
    EPACEMS_MEASUREMENT_CODES,
    EPACEMS_STATES,
    FUEL_CLASSES,
    FUEL_TYPES_EIAAEO,
    GENERATION_ENERGY_SOURCES_EIA930,
    IMPUTATION_CODES,
    INCOME_TYPES_FERC1,
    LIABILITY_TYPES_FERC1,
    MODEL_CASES_EIAAEO,
    NERC_REGIONS,
    PLANT_PARTS,
    RELIABILITY_STANDARDS,
    REVENUE_CLASSES,
    RTO_CLASSES,
    SUBDIVISION_CODES_ISO3166,
    TECH_CLASSES,
    TECH_DESCRIPTIONS,
    TECH_DESCRIPTIONS_EIAAEO,
    TECH_DESCRIPTIONS_NRELATB,
    US_TIMEZONES,
    UTILITY_PLANT_ASSET_TYPES_FERC1,
)
from pudl.metadata.labels import ESTIMATED_OR_ACTUAL, FUEL_UNITS_EIA
from pudl.metadata.sources import SOURCES

# from pudl.transform.params.ferc1 import (
#    PLANT_TYPE_CATEGORIES,
#    PLANT_TYPE_CATEGORIES_HYDRO,
# )

FIELD_METADATA: dict[str, dict[str, Any]] = {
    "acid_gas_control": {
        "type": "boolean",
        "description": "Indicates whether the emissions control equipment controls acid (HCl) gas.",
    },
    "actual_peak_demand_savings_mw": {
        "type": "number",
        "description": "Demand reduction actually achieved by demand response activities. Measured at the time of the company's annual system peak hour.",
        "unit": "MW",
    },
    "additions": {
        "type": "number",
        "description": "Cost of acquisition of items classified within the account.",
        "unit": "USD",
    },
    "address_2": {"type": "string", "description": "Second line of the address."},
    "adjustments": {
        "type": "number",
        "description": "Cost of adjustments to the account.",
        "unit": "USD",
    },
    "advanced_metering_infrastructure": {
        "type": "integer",
        "description": (
            "Number of meters that measure and record usage data at a minimum, in "
            "hourly intervals and provide usage data at least daily to energy "
            "companies and may also provide data to consumers. Data are used for "
            "billing and other purposes. Advanced meters include basic hourly interval "
            "meters and extend to real-time meters with built-in two-way communication "
            "capable of recording and transmitting instantaneous data."
        ),
    },
    "aggregation_group": {
        "type": "string",
        "description": "A label identifying a group of aggregated generator capacity factors.",
    },
    "aggregation_level": {
        "type": "string",
        "description": "Indicates the spacial granularity of aggregated value.",
        "constraints": {
            "enum": ["region", "interconnect", "conus"],
        },
    },
    "air_flow_100pct_load_cubic_feet_per_minute": {
        "type": "number",
        "unit": "cfm",
        "description": "Total air flow including excess air at 100 percent load, reported at standard temperature and pressure (i.e. 68 F and one atmosphere pressure).",
    },
    "alternative_fuel_vehicle_2_activity": {
        "type": "boolean",
        "description": "Whether the utility plants to operate alternative-fueled vehicles this coming year.",
    },
    "alternative_fuel_vehicle_activity": {
        "type": "boolean",
        "description": "Whether the utility operates alternative-fueled vehicles during the year.",
    },
    "annual_average_consumption_rate_gallons_per_minute": {
        "description": "Annual average consumption rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "annual_average_discharge_rate_gallons_per_minute": {
        "description": "Annual average discharge rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "annual_average_withdrawal_rate_gallons_per_minute": {
        "description": "Annual average withdrawal rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "annual_indirect_program_cost": {
        "type": "number",
        "description": (
            "Costs that have not been included in any program category, but could be "
            "meaningfully identified with operating the company’s DSM programs (e.g., "
            "Administrative, Marketing, Monitoring & Evaluation, Company-Earned "
            "Incentives, Other)."
        ),
        "unit": "USD",
    },
    "annual_maximum_intake_summer_temperature_fahrenheit": {
        "description": "Maximum cooling water temperature at intake during the summer",
        "type": "number",
        "unit": "F",
    },
    "annual_maximum_intake_winter_temperature_fahrenheit": {
        "description": "Maximum cooling water temperature at intake in winter",
        "type": "number",
        "unit": "F",
    },
    "annual_maximum_outlet_summer_temperature_fahrenheit": {
        "description": "Maximum cooling water temperature at outlet in summer",
        "type": "number",
        "unit": "F",
    },
    "annual_maximum_outlet_winter_temperature_fahrenheit": {
        "description": "Maximum cooling water temperature at outlet in winter",
        "type": "number",
        "unit": "F",
    },
    "annual_total_chlorine_lbs": {
        "description": (
            "Amount of elemental chlorine added to cooling water annually. "
            "May be just the amount of chlorine-containing compound if "
            "schedule 9 is filled out."
        ),
        "type": "number",
        "unit": "lb",
    },
    "annual_total_cost": {
        "type": "number",
        "description": (
            "The sum of direct program costs, indirect program costs, and incentive "
            "payments associated with utility demand side management programs."
        ),
        "unit": "USD",
    },
    "amount": {
        "description": "Reported amount of dollars. This could be a balance or a change in value.",
        "type": "number",
        "unit": "USD",
    },
    "amount_type": {
        "type": "string",
        "description": "Label describing the type of amount being reported. This could be a balance or a change in value.",
    },
    "appro_part_label": {
        "type": "string",
        "description": "Plant part of the associated true granularity record.",
        "constraints": {"enum": PLANT_PARTS},
    },
    "appro_record_id_eia": {
        "type": "string",
        "description": "EIA record ID of the associated true granularity record.",
    },
    "area_km2": {"type": "number", "description": "County area in km2.", "unit": "km2"},
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
        "description": "If there is an ash impoundment at the plant, is the impoundment lined?",
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
    "asset_type": {
        "type": "string",
        "description": "Type of asset being reported to the core_ferc1__yearly_balance_sheet_assets_sched110 table.",
        "constraints": {
            "enum": ASSET_TYPES_FERC1.extend(
                # Add all possible correction records into enum
                [
                    "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility_correction",
                    "assets_and_other_debits_correction",
                    "construction_work_in_progress_correction",
                    "current_and_accrued_assets_correction",
                    "deferred_debits_correction",
                    "nuclear_fuel_in_process_of_refinement_conversion_enrichment_and_fabrication_correction",
                    "nuclear_fuel_net_correction",
                    "nuclear_materials_held_for_sale_correction",
                    "other_property_and_investments_correction",
                    "utility_plant_and_construction_work_in_progress_correction",
                    "utility_plant_and_nuclear_fuel_net_correction",
                    "utility_plant_net_correction",
                ]
            )
        },
    },
    "associated_combined_heat_power": {
        "type": "boolean",
        "description": "Indicates whether the generator is associated with a combined heat and power system",
    },
    "attention_line": {
        "type": "string",
        "description": "Mail attention name of the operator/owner.",
    },
    "automated_meter_reading": {
        "type": "integer",
        "description": (
            "Number of meters that collect data for billing purposes only and transmit "
            "this data one way, usually from the customer to the distribution utility."
        ),
    },
    "avg_customers_per_month": {
        "type": "number",
        "description": "Average number of customers per month.",
    },
    "avg_num_employees": {
        "type": "number",
        "description": "The average number of employees assigned to each plant.",
    },
    "backup_capacity_mw": {
        "type": "number",
        "description": (
            "The total nameplate capacity of generators that are used only for "
            "emergency backup service."
        ),
        "unit": "MW",
    },
    "balance": {
        "type": "string",
        "description": "Indication of whether a column is a credit or debit, as reported in the XBRL taxonomy.",
    },
    "balancing_authority_code_adjacent_eia": {
        "type": "string",
        "description": "EIA short code for the other adjacent balancing authority, with which interchange is occurring. Includes Canadian and Mexican BAs.",
    },
    "balancing_authority_code_eia": {
        "type": "string",
        "description": "EIA short code identifying a balancing authority. May include Canadian and Mexican BAs.",
    },
    "balancing_authority_code_eia_consistent_rate": {
        "type": "number",
        "description": "Percentage consistency of balancing authority code across entity records.",
    },
    "balancing_authority_id_eia": {
        "type": "integer",
        "description": "EIA balancing authority ID. This is often (but not always!) the same as the utility ID associated with the same legal entity.",
    },
    "balancing_authority_name_eia": {
        "type": "string",
        "description": "Name of the balancing authority.",
    },
    "balancing_authority_retirement_date": {
        "type": "date",
        "description": "Date on which the balancing authority ceased independent operation.",
    },
    "balancing_authority_region_code_eia": {
        "type": "string",
        "description": "EIA balancing authority region code.",
        "constraints": {
            "enum": set(
                CODE_METADATA["core_eia__codes_balancing_authorities"]["df"][
                    "balancing_authority_region_code_eia"
                ].dropna()
            )
        },
    },
    "balancing_authority_region_name_eia": {
        "type": "string",
        "description": "Human-readable name of the EIA balancing region.",
    },
    "balancing_authority_subregion_code_eia": {
        "type": "string",
        "description": "Code identifying subregions of larger balancing authorities.",
        "constraints": {
            "enum": sorted(
                set(
                    BALANCING_AUTHORITY_SUBREGIONS_EIA[
                        "balancing_authority_subregion_code_eia"
                    ]
                )
            )
        },
    },
    "balancing_authority_subregion_name_eia": {
        "type": "string",
        "description": "Name of the balancing authority subregion.",
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
    "billing_status": {
        "type": "string",
        "description": (
            "Whether an amount is billed, unbilled, or both. Billed amounts pertain to "
            "the exchange of energy and unbilled amounts pertain to other sources of "
            "revenue such as contracts with peaker plants to keep them on standby "
            "or charging rent to host cell antennas on transmission towers."
        ),
    },
    "boiler_fuel_code_1": {
        "type": "string",
        "description": "The code representing the most predominant type of energy that fuels the boiler.",
    },
    "boiler_fuel_code_2": {
        "type": "string",
        "description": "The code representing the second most predominant type of energy that fuels the boiler.",
    },
    "boiler_fuel_code_3": {
        "type": "string",
        "description": "The code representing the third most predominant type of energy that fuels the boiler.",
    },
    "boiler_fuel_code_4": {
        "type": "string",
        "description": "The code representing the fourth most predominant type of energy that fuels the boiler.",
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
    "boiler_manufacturer": {
        "type": "string",
        "description": "Name of boiler manufacturer.",
    },
    "boiler_manufacturer_code": {
        "type": "string",
        "description": "EIA short code for boiler manufacturer.",
    },
    "boiler_operating_date": {
        "type": "date",
        "description": "Date the boiler began or is planned to begin commercial operation.",
    },
    "boiler_retirement_date": {
        "type": "date",
        "description": "Date of the scheduled or effected retirement of the boiler.",
    },
    "boiler_status": {
        "type": "string",
        "description": "EIA short code identifying boiler operational status.",
    },
    "boiler_type": {
        "type": "string",
        "description": "EIA short code indicating the standards under which the boiler is operating as described in the U.S. EPA regulation under 40 CFR.",
    },
    "bulk_agg_fuel_cost_per_mmbtu": {
        "type": "number",
        "description": (
            "Fuel cost per mmbtu reported in the EIA bulk electricity data. This is an "
            "aggregate average fuel price for a whole state, region, month, sector, "
            "etc. Used to fill in missing fuel prices."
        ),
    },
    "bundled_activity": {
        "type": "boolean",
        "description": (
            "Whether a utility engaged in combined utility services (electricity plus "
            "other services such as gas, water, etc. in addition to electric services) "
            "during the year."
        ),
    },
    "business_model": {
        "type": "string",
        "description": "Business model.",
        "constraints": {"enum": ["retail", "energy_services"]},
    },
    "buying_distribution_activity": {
        "type": "boolean",
        "description": (
            "Whether a utility bought any distribution on other electrical systems "
            "during the year."
        ),
    },
    "buying_transmission_activity": {
        "type": "boolean",
        "description": (
            "Whether a utility bought any transmission services on other electrical "
            "systems during the year."
        ),
    },
    "bypass_heat_recovery": {
        "type": "boolean",
        "description": "Can this generator operate while bypassing the heat recovery steam generator?",
    },
    "byproduct_recovery": {
        "type": "boolean",
        "description": "Is salable byproduct is recovered by the unit?",
    },
    "caidi_w_major_event_days_minutes": {
        "type": "number",
        "description": (
            "Average number of minutes per interruption (SAIDI/SAIFI) including major "
            "event days."
        ),
        "unit": "min",
    },
    "caidi_w_major_event_days_minus_loss_of_service_minutes": {
        "type": "number",
        "description": (
            "Average number of minutes per interruption (SAIDI/SAIFI) including major "
            "event days and excluding reliability events caused by a loss of supply."
        ),
        "unit": "min",
    },
    "caidi_wo_major_event_days_minutes": {
        "type": "number",
        "description": (
            "Average number of minutes per interruption (SAIDI/SAIFI) excluding major "
            "event days."
        ),
        "unit": "min",
    },
    "can_cofire_100_oil": {
        "type": "boolean",
        "description": "Whether the generator can co-fire 100 oil.",
    },
    "can_cofire_oil_and_gas": {
        "type": "boolean",
        "description": "Whether the generator can co-fire oil and gas.",
    },
    "can_fuel_switch": {
        "type": "boolean",
        "description": "Whether a unit is able to switch fuels.",
    },
    "can_switch_when_operating": {
        "type": "boolean",
        "description": "Whether the generator can switch fuel while operating.",
    },
    "capacity_eoy_mw": {
        "type": "number",
        "description": "Total end of year installed (nameplate) capacity for a plant part, in megawatts.",
        "unit": "MW",
    },
    "capacity_factor": {
        "type": "number",
        "description": "Fraction of potential generation that was actually reported for a plant part.",
    },
    "capacity_factor_eia": {
        "type": "number",
        "description": "Fraction of potential generation that was actually reported for a plant part.",
    },
    "capacity_factor_ferc1": {
        "type": "number",
        "description": "Fraction of potential generation that was actually reported for a plant part.",
    },
    "capacity_factor_offshore_wind": {
        "type": "number",
        "description": (
            "Estimated capacity factor (0-1) calculated for offshore wind "
            "assuming a 140m hub height and 120m rotor diameter."
            "Based on outputs from the NOAA HRRR operational numerical "
            "weather prediction model. Capacity factors are normalized "
            "to unity for maximal power output. "
            "Vertical slices of the atmosphere are considered across the "
            "defined rotor swept area. Bringing together wind speed, density, "
            "temperature and icing information, a power capacity is estimated "
            "using a representative power coefficient (Cp) curve to determine "
            "the power from a given wind speed, atmospheric density and "
            "temperature. There is no wake modeling included in the dataset."
        ),
    },
    "capacity_factor_onshore_wind": {
        "type": "number",
        "description": (
            "Estimated capacity factor (0-1) calculated for onshore wind "
            "assuming a 100m hub height and 120m rotor diameter."
            "Based on outputs from the NOAA HRRR operational numerical "
            "weather prediction model. Capacity factors are normalized "
            "to unity for maximal power output. "
            "Vertical slices of the atmosphere are considered across the "
            "defined rotor swept area. Bringing together wind speed, density, "
            "temperature and icing information, a power capacity is estimated "
            "using a representative power coefficient (Cp) curve to determine "
            "the power from a given wind speed, atmospheric density and "
            "temperature. There is no wake modeling included in the dataset."
        ),
    },
    "capacity_factor_solar_pv": {
        "type": "number",
        "description": (
            "Estimated capacity factor (0-1) calculated for solar PV "
            "assuming a fixed axis panel tilted at latitude and DC power "
            "outputs. Due to power production performance being correlated "
            "with panel temperatures, during cold sunny periods, some solar "
            "capacity factor values are greater than 1 (but less that 1.1)."
            "All values are based on outputs from the NOAA HRRR operational "
            "numerical weather prediction model. Capacity factors are "
            "normalized to unity for maximal power output. "
            "Pertinent surface weather variables are pulled such as "
            "incoming short wave radiation, direct normal irradiance "
            "(calculated in the HRRR 2016 forward), surface temperature "
            "and other parameters. These are used in a non-linear I-V curve "
            "translation to power capacity factors."
        ),
    },
    "capacity_mw": {
        "type": "number",
        "description": "Total installed (nameplate) capacity, in megawatts.",
        "unit": "MW",
        # TODO: Disambiguate if necessary. Does this mean different things in
        # different tables? It shows up in a lot of places.
    },
    "capacity_mw_eia": {
        "type": "number",
        "description": "Total installed (nameplate) capacity, in megawatts.",
        "unit": "MW",
    },
    "capacity_mw_ferc1": {
        "type": "number",
        "description": "Total installed (nameplate) capacity, in megawatts.",
        "unit": "MW",
    },
    "capex_annual_addition": {
        "type": "number",
        "description": "Annual capital addition into `capex_total`.",
        "unit": "USD",
    },
    "capex_annual_addition_rolling": {
        "type": "number",
        "description": "Year-to-date capital addition into `capex_total`.",
        "unit": "USD",
    },
    "capex_annual_per_kw": {
        "type": "number",
        "description": "Annual capital addition into `capex_total` per kw.",
        "unit": "USD_per_kw",
    },
    "capex_annual_per_mw": {
        "type": "number",
        "description": "Annual capital addition into `capex_total` per MW.",
        "unit": "USD_per_MW",
    },
    "capex_annual_per_mw_rolling": {
        "type": "number",
        "description": "Year-to-date capital addition into `capex_total` per MW.",
        "unit": "USD_per_MW",
    },
    "capex_annual_per_mwh": {
        "type": "number",
        "description": "Annual capital addition into `capex_total` per MWh.",
        "unit": "USD_per_MWh",
    },
    "capex_annual_per_mwh_rolling": {
        "type": "number",
        "description": "Year-to-date capital addition into `capex_total` per MWh.",
        "unit": "USD_per_MWh",
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
    "capex_other": {
        "type": "number",
        "description": "Other costs associated with the plant (USD).",
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
    "capex_wo_retirement_total": {
        "type": "number",
        "description": "Total cost of plant (USD) without retirements.",
        "unit": "USD",
    },
    "capex_per_kw": {
        "type": "number",
        "description": "Capital cost (USD). Expenditures required to achieve commercial operation of the generation plant.",
        "unit": "USD",
    },
    "capex_grid_connection_per_kw": {
        "type": "number",
        "description": "Overnight capital cost includes a nominal-distance spur line (<1 mi) for all technologies, and for offshore wind, it includes export cable and construction period transit costs for a 30-km distance from shore. Project-specific costs lines that are based on distance to existing transmission are not included. This only applies to offshore wind.",
    },
    "capex_overnight_per_kw": {
        "type": "number",
        "description": "capex if plant could be constructed overnight (i.e., excludes construction period financing); includes on-site electrical equipment (e.g., switchyard), a nominal-distance spur line (<1 mi), and necessary upgrades at a transmission substation.",
        "unit": "USD",
    },
    "capex_overnight_additional_per_kw": {
        "type": "number",
        "description": "capex for retrofits if plant could be constructed overnight (i.e., excludes construction period financing); includes on-site electrical equipment (e.g., switchyard), a nominal-distance spur line (<1 mi), and necessary upgrades at a transmission substation.",
        "unit": "USD",
    },
    "capex_construction_finance_factor": {
        "type": "number",
        "description": (
            "Portion of all-in capital cost associated with construction period "
            "financing. This factor is applied to an overnight capital cost to represent "
            "the financing costs incurred during the construction period."
        ),
    },
    "carbon_capture": {
        "type": "boolean",
        "description": "Indicates whether the generator uses carbon capture technology.",
    },
    "central_index_key": {
        "type": "string",
        "description": "Identifier of the company in SEC database.",
    },
    "chlorine_equipment_cost": {
        "description": (
            "Actual installed cost for the existing chlorine discharge "
            "control system or the anticipated cost to bring the chlorine "
            "discharge control system into commercial operation"
        ),
        "type": "number",
        "unit": "USD",
    },
    "chlorine_equipment_operating_date": {
        "description": (
            "Actual or projected in-service date for chlorine discharge "
            "control structures and equipment"
        ),
        "type": "date",
    },
    "chlorine_content_ppm": {
        "type": "number",
        "description": (
            "For coal only: the chlorine content in parts per million (ppm) to the "
            "nearest 0.001 ppm. If lab tests of the coal do not include the chlorine "
            "content, this field contains the amount specified in the contract with "
            "the supplier."
        ),
        "unit": "ppm",
    },
    "circuits_with_voltage_optimization": {
        "type": "integer",
        "description": (
            "Number of distribution circuits that employ voltage/VAR optimization "
            "(VVO)."
        ),
    },
    "city": {
        "type": "string",
        # TODO: Disambiguate column. City means different things in different tables.
        "description": "Name of the city.",
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
    "coal_fraction_cost": {
        "type": "number",
        "description": "Coal cost as a percentage of overall fuel cost.",
    },
    "coal_fraction_mmbtu": {
        "type": "number",
        "description": "Coal heat content as a percentage of overall fuel heat content (mmBTU).",
    },
    "coalmine_county_id_fips": {
        "type": "string",
        "description": (
            "County ID from the Federal Information Processing Standard Publication "
            "6-4. This is the county where the coal mine is located."
        ),
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "code": {
        "type": "string",
        "description": "Originally reported short code.",
    },
    "can_cofire_fuels": {
        "type": "boolean",
        "description": "Whether the generator can co-fire fuels.",
    },
    "cofire_energy_source_1": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be co-fired.",
    },
    "cofire_energy_source_2": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be co-fired.",
    },
    "cofire_energy_source_3": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be co-fired.",
    },
    "cofire_energy_source_4": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be co-fired.",
    },
    "cofire_energy_source_5": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be co-fired.",
    },
    "cofire_energy_source_6": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be co-fired.",
    },
    "coincident_peak_demand_mw": {
        "type": "number",
        "description": "Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier's system reaches its monthly peak. In megawatts.",
        "unit": "MW",
    },
    "company_name": {
        "type": "string",
        "description": "Name of company submitting SEC 10k filing.",
    },
    "company_name_new": {
        "type": "string",
        "description": "Name of company after name change.",
    },
    "company_name_old": {
        "type": "string",
        "description": "Name of company prior to name change.",
    },
    "compliance_year_nox": {
        "type": "integer",
        "description": "Year boiler was or is expected to be in compliance with federal, state and/or local regulations for nitrogen oxide emissions.",
    },
    "compliance_year_mercury": {
        "type": "integer",
        "description": "Year boiler was or is expected to be in compliance with federal, state and/or local regulations for mercury emissions.",
    },
    "compliance_year_particulate": {
        "type": "integer",
        "description": "Year boiler was or is expected to be in compliance with federal, state and/or local regulations for particulate matter emissions.",
    },
    "compliance_year_so2": {
        "type": "integer",
        "description": "Year boiler was or is expected to be in compliance with federal, state and/or local regulations for sulfur dioxide emissions.",
    },
    "conductor_size_and_material": {
        "type": "string",
        "description": "Size of transmission conductor and material of the transmission line.",
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
    "construction_year_eia": {
        "type": "integer",
        "description": "Year the plant's oldest still operational unit was built.",
    },
    "construction_year_ferc1": {
        "type": "integer",
        "description": "Year the plant's oldest still operational unit was built.",
    },
    "consumed_by_facility_mwh": {
        "type": "number",
        "description": "The amount of electricity used by the facility.",
        "unit": "MWh",
    },
    "consumed_by_respondent_without_charge_mwh": {
        "type": "number",
        "description": (
            "The amount of electricity used by the electric utility in its electric "
            "and other departments without charge."
        ),
        "unit": "MWh",
    },
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
    "cooling_equipment_total_cost": {
        "description": (
            "Actual installed cost for the existing system or the "
            "anticipated cost to bring the total system into commercial "
            "operation"
        ),
        "type": "number",
        "unit": "USD",
    },
    "cooling_id_eia": {
        "description": (
            "EIA Identification code for cooling system (if multiple cooling "
            "systems are not distinguished by separate IDs, the word "
            "'PLANT' is listed to encompass the cooling system for the "
            "entire plant)"
        ),
        "type": "string",
    },
    "cooling_status_code": {
        "description": "Operating status of cooling system",
        "type": "string",
    },
    "cooling_system_operating_date": {
        "description": "The actual or projected in-service datetime of this cooling system",
        "type": "date",
    },
    "cooling_type": {
        "description": "Type of cooling system",
        "type": "string",
    },
    "cooling_type_1": {
        "description": "Type of cooling system",
        "type": "string",
    },
    "cooling_type_2": {
        "description": "Type of cooling system",
        "type": "string",
    },
    "cooling_type_3": {
        "description": "Type of cooling system",
        "type": "string",
    },
    "cooling_type_4": {
        "description": "Type of cooling system",
        "type": "string",
    },
    "cooling_water_discharge": {
        "description": (
            "Name of river, lake, or water source that cooling water is discharged into"
        ),
        "type": "string",
    },
    "cooling_water_source": {
        "description": "Name of river, lake, or water source that provides cooling water",
        "type": "string",
    },
    "county": {"type": "string", "description": "County name."},
    "county_id_fips": {
        "type": "string",
        "description": "County ID from the Federal Information Processing Standard Publication 6-4.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "county_name_census": {
        "type": "string",
        "description": "County name as specified in Census DP1 Data.",
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
    "critical_peak_pricing": {
        "type": "boolean",
        "description": (
            "Whether customers are participating in critical peak "
            "pricing, a program in which rate and/or price structure is designed to "
            "encourage reduced consumption during periods of high wholesale market "
            "prices or system contingencies, by imposing a pre-specified high rate or "
            "price for a limited number of days or hours."
        ),
    },
    "critical_peak_rebate": {
        "type": "boolean",
        "description": (
            "Whether customers are participating in critical peak rebates, a program "
            "in which rate and/or price structure is designed to encourage reduced "
            "consumption during periods of high wholesale market prices or system "
            "contingencies, by providing a rebate to the customer on a limited number "
            "of days and for a limited number of hours, at the request of the energy "
            "provider."
        ),
    },
    "current_planned_generator_operating_date": {
        "type": "date",
        "description": "The most recently updated effective date on which the generator is scheduled to start operation",
    },
    "customer_class": {
        "type": "string",
        "description": f"High level categorization of customer type: {CUSTOMER_CLASSES}.",
        "constraints": {"enum": CUSTOMER_CLASSES},
    },
    "customer_incentives_cost": {
        "type": "number",
        "description": (
            "Total cost of customer incentives in a given report year. Customer "
            "incentives are the total financial value provided to a customer for "
            "program participation, whether, for example, cash payment, or lowered "
            "tariff rates relative to non-participants, in-kind services (e.g. "
            "design work), or other benefits directly provided to the customer for "
            "their program participation."
        ),
        "unit": "USD",
    },
    "customer_incentives_incremental_cost": {
        "type": "number",
        "description": (
            "The cost of customer incentives resulting from new participants in "
            "existing energy efficiency programs and all participants in new energy "
            "efficiency programs. Customer incentives are the total financial value "
            "provided to a customer for program participation, whether, for example, "
            "cash payment, or lowered tariff rates relative to non-participants, "
            "in-kind services (e.g. design work), or other benefits directly provided "
            "to the customer for their program participation."
        ),
        "unit": "USD",
    },
    "customer_incentives_incremental_life_cycle_cost": {
        "type": "number",
        "description": (
            "All anticipated costs of the customer incentives including reporting year "
            "incremental costs and all future costs. Customer incentives are the "
            "total financial value provided to a customer for program participation, "
            "whether, for example, cash payment, or lowered tariff rates relative to "
            "non-participants, in-kind services (e.g. design work), or other benefits "
            "directly provided to the customer for their program participation."
        ),
        "unit": "USD",
    },
    "customer_other_costs_incremental_life_cycle_cost": {
        "type": "number",
        "description": (
            "All anticipated costs other than customer incentives. Includes reporting "
            "year incremental costs and all future costs."
        ),
        "unit": "USD",
    },
    "customers": {"description": "Number of customers.", "type": "number"},
    "daily_digital_access_customers": {
        "type": "integer",
        "description": (
            "Number of customers able to access daily energy usage through a webportal "
            "or other electronic means."
        ),
    },
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
    "datetime_utc": {
        "type": "datetime",
        "description": "Date and time converted to Coordinated Universal Time (UTC).",
    },
    "datum": {
        "type": "string",
        "description": "Geodetic coordinate system identifier (e.g. NAD27, NAD83, or WGS84).",
    },
    "account_detail": {
        "type": "string",
        "description": "Description of the account number credited from making debit adjustment to other regulatory liabilities.",
    },
    "decrease_in_other_regulatory_liabilities": {
        "type": "number",
        "description": "The decrease during the reporting period of other regulatory liabilities.",
        "unit": "USD",
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
    "demand_adjusted_mwh": {
        "type": "number",
        "description": "Electricity demand adjusted by EIA to reflect non-physical commercial transfers through pseudo-ties and dynamic scheduling.",
        "unit": "MWh",
    },
    # TODO[zaneselvans] 2024-04-20: Is the timestamp when the forecast was made? Or the
    # time at which the forecast is trying to predict demand?
    "demand_forecast_mwh": {
        "type": "number",
        "description": "Day ahead demand forecast.",
        "unit": "MWh",
    },
    "demand_imputed_eia_mwh": {
        "type": "number",
        "description": "Electricity demand calculated by subtracting BA interchange from net generation, with outliers and missing values imputed by EIA.",
        "unit": "MWh",
    },
    "demand_imputed_pudl_mwh": {
        "type": "number",
        "description": "Electricity demand calculated by subtracting BA interchange from net generation, with outliers and missing values imputed in PUDL.",
        "unit": "MWh",
    },
    "demand_imputed_pudl_mwh_imputation_code": {
        "type": "string",
        "description": "Code describing why a demand value was flagged for imputation.",
        "constraints": {"enum": IMPUTATION_CODES},
    },
    "demand_reported_mwh": {
        "type": "number",
        "description": "Originally reported electricity demand, calculated by taking the net generation within the BA and subtracting the interchange with adjacent BAs.",
        "unit": "MWh",
    },
    "demand_annual_mwh": {
        "type": "number",
        "description": "Annual electricity demand in a given report year.",
        "unit": "MWh",
    },
    "demand_annual_per_capita_mwh": {
        "type": "number",
        "description": "Per-capita annual demand, averaged using Census county-level population estimates.",
        "unit": "MWh/person",
    },
    "demand_charges": {
        "type": "number",
        "description": "Demand charges (USD).",
        "unit": "USD",
    },
    "demand_mwh": {
        "type": "number",
        "description": "Electricity demand (energy) within a given timeframe.",
        "unit": "MWh",
    },
    "demand_density_mwh_km2": {
        "type": "number",
        "description": "Annual demand per km2 of a given service territory.",
        "unit": "MWh/km2",
    },
    "has_air_permit_limits": {
        "type": "boolean",
        "description": "Whether air permit limits are a factor that limits the generator's ability to switch between oil and natural gas.",
    },
    "has_demand_side_management": {
        "type": "boolean",
        "description": "Whether there were strategies or measures used to control electricity demand by customers",
    },
    "has_factors_that_limit_switching": {
        "type": "boolean",
        "description": "Whether there are factors that limit the generator's ability to switch between oil and natural gas.",
    },
    "has_other_factors_that_limit_switching": {
        "type": "boolean",
        "description": "Whether there are factors other than air permit limits and storage that limit the generator's ability to switch between oil and natural gas.",
    },
    "has_storage_limits": {
        "type": "boolean",
        "description": "Whether limited on-site fuel storage is a factor that limits the generator's ability to switch between oil and natural gas.",
    },
    "depreciation_type": {
        "type": "string",
        "description": (
            "Type of depreciation provision within FERC Account 108, including cost of"
            "removal, depreciation expenses, salvage, cost of retired plant, etc."
        ),
    },
    "description": {
        "type": "string",
        "description": "Long human-readable description of the meaning of a code/label.",
    },
    "designed_voltage_kv": {
        "type": "number",
        "description": "Manufactured (Designed) voltage, expressed in kilo-volts, for three-phase 60 cycle alternative current transmission lines",
        "unit": "KV",
    },
    "direct_load_control_customers": {
        "type": "integer",
        "description": (
            "Number of customers with direct load control: a A demand response "
            "activity by which the program sponsor remotely shuts down or cycles a "
            "customer’s electrical equipment (e.g. air conditioner, water heater) on "
            "short notice."
        ),
    },
    "distributed_generation": {
        "type": "boolean",
        "description": "Whether the generator is considered distributed generation",
    },
    "distributed_generation_owned_capacity_mw": {
        "type": "number",
        "description": (
            "Amount of distributed generation capacity owned by the respondent."
        ),
        "unit": "MW",
    },
    "distribution_activity": {
        "type": "boolean",
        "description": (
            "Whether a utility engaged in any distribution using owned/leased "
            "electrical wires during the year."
        ),
    },
    "distribution_circuits": {
        "type": "integer",
        "description": "Total number of distribution circuits.",
    },
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
    "doi": {
        "type": "string",
        "description": "Unique digital object identifier of Zenodo archive.",
    },
    "dollar_value": {
        "type": "number",
        "description": "Dollar value of reported income, expense, asset, or liability.",
        "unit": "USD",
    },
    "duct_burners": {
        "type": "boolean",
        "description": "Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas",
    },
    "earnings_type": {
        "type": "string",
        "description": "Label describing types of earnings.",
    },
    "efficiency_100pct_load": {
        "type": "number",
        "description": "Boiler efficiency percentage when burning at 100 percent load to the nearest 0.1 percent.",
    },
    "efficiency_50pct_load": {
        "type": "number",
        "description": "Boiler efficiency percentage when burning at 50 percent load to the nearest 0.1 percent.",
    },
    "eia_code": {
        "type": "integer",
        "description": (
            "EIA utility or balancing area authority ID associated with this FERC Form "
            "714 respondent. Note that many utilities are also balancing authorities "
            "and in many cases EIA uses the same integer ID to identify a utility in "
            "its role as a balancing authority AND as a utility, but there is no "
            "requirement that these IDs be the same, and in a number of cases they are "
            "different."
        ),
    },
    "electricity_market_module_region_eiaaeo": {
        "type": "string",
        "description": "AEO projection region.",
        "constraints": {"enum": ELECTRICITY_MARKET_MODULE_REGIONS},
    },
    "emission_control_id_eia": {
        "type": "string",
        "description": (
            "The emission control ID used to collect SO2, NOx, particulate, "
            "and mercury emissions data. This column should be used in conjunction "
            "with emissions_control_type as it's not guaranteed to be unique."
        ),
    },
    "emission_control_id_pudl": {
        "type": "number",
        "description": "A PUDL-generated ID used to distinguish emission control units in the same report year and plant id. This ID should not be used to track units over time or between plants.",
    },
    "emission_control_id_type": {
        "type": "string",
        "description": "The type of emissions control id: SO2, NOx, particulate, or mercury.",
    },
    "emission_control_equipment_cost": {
        "type": "number",
        "description": "The total cost to install a piece of emission control equipment.",
        "unit": "USD",
    },
    "emission_control_equipment_type_code": {
        "type": "string",
        "description": "Short code indicating the type of emission control equipment installed.",
    },
    "emission_control_operating_date": {
        "type": "date",
        "description": "The date a piece of emissions control equipment began operating. Derived from month and year columns in the raw data.",
    },
    "emission_control_retirement_date": {
        "type": "date",
        "description": "The expected or actual retirement date for a piece of emissions control equipment. Derived from month and year columns in the raw data.",
    },
    "emissions_unit_id_epa": {
        "type": "string",
        "description": "Emissions (smokestack) unit monitored by EPA CEMS.",
    },
    "end_point": {
        "type": "string",
        "description": "The end point of a transmission line.",
    },
    "ending_balance": {
        "type": "number",
        "description": "Account balance at end of year.",
        "unit": "USD",
    },
    "energy_capacity_mwh": {
        "type": "number",
        "description": "The total amount of energy which the system can supply power before recharging is necessary, in megawatt-hours.",
        "unit": "MWh",
    },
    "energy_charges": {
        "type": "number",
        "description": "Energy charges (USD).",
        "unit": "USD",
    },
    "energy_disposition_type": {
        "type": "string",
        "description": "Type of energy disposition reported in the core_ferc1__yearly_energy_dispositions_sched401. Dispositions include sales to customers, re-sales of energy, energy used internally, losses, etc.",
        "constraints": {
            "enum": ENERGY_DISPOSITION_TYPES_FERC1.extend(
                # Add all possible correction records to enum
                [
                    "disposition_of_energy_correction",
                    "megawatt_hours_sold_sales_to_ultimate_consumers_correction",
                    "net_energy_generation_correction",
                    "net_energy_through_power_exchanges_correction",
                    "net_transmission_energy_for_others_electric_power_wheeling_correction",
                    "sources_of_energy_correction",
                ]
            )
        },
    },
    "energy_efficiency_annual_actual_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The peak reduction incurred in a given reporting year by all participants "
            "in efficiency programs."
        ),
        "unit": "MW",
    },
    "energy_efficiency_annual_cost": {
        "type": "number",
        "description": (
            "The sum of actual direct costs, incentive payments, and indirect costs "
            "incurred in a given reporting year from energy efficiency programs."
        ),
        "unit": "USD",
    },
    "energy_efficiency_annual_effects_mwh": {
        "type": "number",
        "description": (
            "The change in energy use incurred in a given reporting year by "
            "all participants in energy efficiency programs."
        ),
        "unit": "MWh",
    },
    "energy_efficiency_annual_incentive_payment": {
        "type": "number",
        "description": (
            "The cost of incentive payments incurred in a given reporting year "
            "from energy efficiency programs. Incentives are the "
            "total financial value provided to a customer for program participation, "
            "whether cash payment, in-kind services (e.g. design work), or other "
            "benefits directly provided customer for their program participation."
        ),
        "unit": "USD",
    },
    "energy_efficiency_incremental_actual_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The peak reduction incurred in a given reporting year by new "
            "participants in existing energy efficiency programs and all participants "
            "in new energy efficiency programs."
        ),
        "unit": "MW",
    },
    "energy_efficiency_incremental_effects_mwh": {
        "type": "number",
        "description": (
            "The change in energy use incurred in a given reporting year by "
            "new participants in existing energy efficiency programs and all "
            "participants in new energy efficiency programs."
        ),
        "unit": "MWh",
    },
    "energy_mwh": {
        "type": "number",
        "unit": "MWh",
        "description": "Sources and uses of energy in MWh.",
    },
    "energy_savings_estimates_independently_verified": {
        "type": "boolean",
        "description": (
            "Whether savings estimates are based on a forecast or the report of one or "
            "more independent evaluators."
        ),
    },
    "energy_savings_independently_verified": {
        "type": "boolean",
        "description": (
            "Whether reported energy savings were verified through an independent "
            "evaluation."
        ),
    },
    "energy_savings_mwh": {
        "type": "number",
        "description": (
            "The energy savings incurred in a given reporting year by participation in "
            "demand response programs."
        ),
        "unit": "MWh",
    },
    "energy_served_ami_mwh": {
        "type": "number",
        "description": (
            "Amount of energy served through AMI meters. AMI meters can transmit data "
            "in both directions, between the delivery entity and the customer."
        ),
        "unit": "MWh",
    },
    "generation_energy_source": {
        "type": "string",
        "description": "High level energy source used to produce electricity.",
        "constraints": {"enum": GENERATION_ENERGY_SOURCES_EIA930},
    },
    "energy_source_code": {
        "type": "string",
        "description": (
            "A 2-3 letter code indicating the energy source (e.g. fuel type) "
            "associated with the record."
        ),
        # Should this have an enum reference to the core_eia__codes_energy_sources table??
    },
    "energy_source_code_num": {
        "type": "string",
        "description": (
            "Name of the energy_source_code_N column that this energy source code was "
            "reported in for the generator referenced in the same record."
        ),
        "constraints": {
            "enum": sorted({f"energy_source_code_{n}" for n in range(1, 9)})
        },
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
    "energy_source_code_1_plant_gen": {
        "type": "string",
        "description": "Code representing the most predominant type of energy that fuels the record_id_eia_plant_gen's generator.",
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
    "energy_source_type": {
        "type": "string",
        "description": "Type of energy source reported in the core_ferc1__yearly_energy_sources_sched401 table. There are three groups of energy sources: generation, power exchanges and transmission.",
        "constraints": {
            "enum": ENERGY_SOURCE_TYPES_FERC1.extend(
                # Add all possible correction records to enum
                [
                    "disposition_of_energy_correction",
                    "megawatt_hours_sold_sales_to_ultimate_consumers_correction",
                    "net_energy_generation_correction",
                    "net_energy_through_power_exchanges_correction",
                    "net_transmission_energy_for_others_electric_power_wheeling_correction",
                    "sources_of_energy_correction",
                ]
            )
        },
    },
    "energy_storage": {
        "type": "boolean",
        "description": "Indicates if the facility has energy storage capabilities.",
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "energy_storage_capacity_mwh": {
        "type": "number",
        "description": "Energy storage capacity in MWh (e.g. for batteries).",
        "unit": "MWh",
    },
    "energy_use_type": {
        "type": "string",
        "description": "Type of energy use, indicating the name of the series from AEO Table 2. Includes fuels, electricity, losses, and various subtotals; consult table documentation for aggregation guidelines.",
        "constraints": {"enum": ENERGY_USE_TYPES_EIAAEO},
    },
    "energy_use_mmbtu": {
        "type": "number",
        "description": "Energy use, in MMBtu; also referred to as energy consumption, energy demand, or delivered energy, depending on type.",
        "unit": "MMBtu",
    },
    "energy_use_sector": {
        "type": "string",
        "description": "Sector for energy use figures in AEO Table 2. Similar to customer class, but with some missing and some extra values.",
        "constraints": {
            "enum": set(CUSTOMER_CLASSES) - {"direct_connection"}
            | {"electric_power", "unspecified"}
        },
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
        "description": "Whether the reported capacity data is estimated or actual.",
        "constraints": {"enum": list(ESTIMATED_OR_ACTUAL.values())},
    },
    "estimated_or_actual_fuel_data": {
        "type": "string",
        "description": "Whether the reported fuel data is estimated or actual.",
        "constraints": {"enum": list(ESTIMATED_OR_ACTUAL.values())},
    },
    "estimated_or_actual_tech_data": {
        "type": "string",
        "description": "Whether the reported technology data is estimated or actual.",
        "constraints": {"enum": list(ESTIMATED_OR_ACTUAL.values())},
    },
    "exchange_energy_delivered_mwh": {
        "type": "number",
        "description": (
            "The amount of exchange energy delivered. Does not include power delivered "
            "as part of a tolling arrangement."
        ),
        "unit": "MWh",
    },
    "exchange_energy_received_mwh": {
        "type": "number",
        "description": (
            "The amount of exchange energy received. Does not include power received "
            "through tolling arrangements."
        ),
        "unit": "MWh",
    },
    "exhibit_21_version": {
        "type": "string",
        "description": "Version of exhibit 21 submitted (if applicable).",
        "constraints": {
            "pattern": r"^21\.*\d*$",
        },
    },
    "expense_type": {"type": "string", "description": "The type of expense."},
    "ferc1_generator_agg_id": {
        "type": "integer",
        "description": "ID dynamically assigned by PUDL to EIA records with multiple matches to a single FERC ID in the FERC-EIA manual matching process.",
    },
    "ferc1_generator_agg_id_plant_gen": {
        "type": "integer",
        "description": "ID dynamically assigned by PUDL to EIA records with multiple matches to a single FERC ID in the FERC-EIA manual matching process. This ID is associated with the record_id_eia_plant_gen record.",
    },
    "ferc_account": {
        "type": "string",
        "description": "Actual FERC Account number (e.g. '359.1') if available, or a PUDL assigned ID when FERC accounts have been split or combined in reporting.",
    },
    "ferc_account_description": {
        "type": "string",
        "description": "Description of the FERC account.",
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
    "ferc_acct_name_plant_gen": {
        "type": "string",
        "description": "Name of FERC account, derived from technology description and prime mover code. This name is associated with the record_id_eia_plant_gen record.",
        "constraints": {"enum": ["Hydraulic", "Nuclear", "Steam", "Other"]},
    },
    "ferc_cogen_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC cogenerator status. See FERC Form 556.",
    },
    "ferc_cogen_status": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility cogenerator status. See FERC Form 556.",
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ferc_exempt_wholesale_generator": {
        "type": "boolean",
        "description": "Indicates whether the plant has FERC qualifying facility exempt wholesale generator status",
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "ferc_exempt_wholesale_generator_docket_no": {
        "type": "string",
        "description": "The docket number relating to the FERC qualifying facility exempt wholesale generator status.",
    },
    "ferc_license_id": {
        "type": "string",
        "description": "The FERC license ID of a project.",
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
        "description": "Indicates whether or not a generator is a qualifying FERC cogeneration facility.",
    },
    "fgd_control_flag": {
        "type": "boolean",
        "description": "Indicates whether or not a plant has a flue gas desulfurization control unit.",
    },
    "fgd_electricity_consumption_mwh": {
        "type": "number",
        "unit": "MWh",
        "description": "Electric power consumed by the flue gas desulfurization unit (in MWh).",
    },
    "fgd_hours_in_service": {
        "type": "integer",
        "unit": "hours",
        "description": "Number of hours the flue gas desulfurization equipment was in operation during the year.",
    },
    "fgd_other_cost": {
        "description": (
            "Other actual installed costs for installation of a flue gas "
            "desulfurization unit or the anticipated other costs pertaining to the "
            "installation of a flue gas desulfurization unit."
        ),
        "type": "number",
        "unit": "USD",
    },
    "fgd_operating_date": {
        "description": "The actual or projected in-service datetime of this flue gas desulfurization system",
        "type": "date",
    },
    "fgd_operational_status_code": {
        "type": "string",
        "description": "Operating status code for flue gas desulfurization equipment.",
    },
    "fgd_manufacturer": {
        "type": "string",
        "description": "Name of flue gas desulfurization equipment manufacturer.",
    },
    "fgd_manufacturer_code": {
        "type": "string",
        "description": "Code corresponding to name of flue gas desulfurization equipment manufacturer.",
    },
    "fgd_sorbent_consumption_1000_tons": {
        "type": "number",
        "unit": "1000_tons",
        "description": "Quantity of flue gas desulfurization sorbent used, to the nearest 0.1 thousand tons.",
    },
    "fgd_structure_cost": {
        "type": "number",
        "unit": "USD",
        "description": "Actual installed costs for the existing systems or the anticipated costs of structures and equipment to bring a planned flue gas desulfurization system into commercial operation.",
    },
    "fgd_trains_100pct": {
        "type": "number",
        "description": "Total number of flue gas desulfurization unit scrubber trains operated at 100 percent load.",
    },
    "fgd_trains_total": {
        "type": "number",
        "description": "Total number of flue gas desulfurization unit scrubber trains.",
    },
    "filename_sec10k": {
        "type": "string",
        "description": (
            "Unique portion of the filename associated with the SEC 10-K filing in the "
            "EDGAR database. The full source URL can be reconstructed by prepending "
            "https://www.sec.gov/Archives/edgar/data/ and adding the .txt file type "
            "extension."
        ),
    },
    "filing_date": {
        "type": "date",
        "description": "Date of the day on which the filing was submitted.",
    },
    "film_number": {
        "type": "string",
        "description": "Document control number used in the SEC EDGAR database. The first four digits can be used to access scans of the document in the SEC's Virtual Private Reference Room.",
    },
    "firing_rate_using_coal_tons_per_hour": {
        "type": "number",
        "unit": "tons_per_hour",
        "description": "Design firing rate at maximum continuous steam flow for coal to the nearest 0.1 ton per hour.",
    },
    "firing_rate_using_gas_mcf_per_hour": {
        "type": "number",
        "unit": "mcf_per_hour",
        "description": "Design firing rate at maximum continuous steam flow for gas to the nearest 0.1 cubic feet per hour.",
    },
    "firing_rate_using_oil_bbls_per_hour": {
        "type": "number",
        "unit": "bbls_per_hour",
        "description": "Design firing rate at maximum continuous steam flow for pet coke to the nearest 0.1 barrels per hour.",
    },
    "firing_rate_using_other_fuels": {
        "type": "number",  # TO DO: unit not in layout files, how to ID?
        "description": "Design firing rate at maximum continuous steam flow for energy sources other than coal, petroleum, or natural gas.",
    },
    "firing_type_1": {
        "type": "string",
        "description": "EIA short code indicating the type of firing used by this boiler.",
    },
    "firing_type_2": {
        "type": "string",
        "description": "EIA short code indicating the type of firing used by this boiler.",
    },
    "firing_type_3": {
        "type": "string",
        "description": "EIA short code indicating the type of firing used by this boiler.",
    },
    "fiscal_year_end": {
        "type": "string",
        "description": "The end date of an SEC filing company's fiscal year, in MMDD format.",
        # This REGEXP constraint was causing issues w/ SQLAlchemy / SQLite.
        # https://github.com/sqlalchemy/sqlalchemy/discussions/12498
        "constraints": {
            "pattern": r"^(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1\d|2\d|3[01])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)$",
        },
    },
    "flow_rate_method": {
        "description": (
            "Calculation method for flow rates (actual or method of estimation)"
        ),
        "type": "string",
    },
    "flue_gas_bypass_fgd": {
        "type": "boolean",
        "description": "Indicates whether flue gas can bypass the flue gas desulfurization unit.",
    },
    "flue_gas_entering_fgd_pct_of_total": {
        "type": "number",
        "description": "Ratio of all flue gas that is entering the flue gas desulfurization unit.",
    },
    "flue_gas_exit_rate_cubic_feet_per_minute": {
        "type": "number",
        "unit": "cfm",
        "description": "Actual flue gas exit rate, in cubic feet per minute.",
    },
    "flue_gas_exit_temperature_fahrenheit": {
        "type": "number",
        "unit": "F",
        "description": "Flue gas exit temperature, in degrees Fahrenheit.",
    },
    "flue_id_eia": {
        "type": "string",
        "description": (
            "The flue identification value reported to EIA. The flue is a duct, pipe, "
            "or opening that transports exhast gases through the stack. This field was "
            "reported in conjunction with stack_id_eia until 2013 when "
            "stack_flue_id_eia took their place."
        ),
    },
    "fluidized_bed_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses fluidized bed technology",
    },
    "fly_ash_reinjection": {
        "type": "boolean",
        "description": "Indicates whether the boiler is capable of re-injecting fly ash.",
    },
    "forecast_year": {
        "type": "integer",
        "description": "Four-digit year that applies to a particular forecasted value.",
    },
    "fraction_owned": {
        "type": "number",
        "description": "Proportion of generator ownership attributable to this utility.",
    },
    "fuel_agg": {
        "type": "string",
        "description": "Category of fuel aggregation in EIA bulk electricity data.",
    },
    "fuel_class": {
        "type": "string",
        "description": f"Fuel types specific to EIA 861 distributed generation table: {FUEL_CLASSES}",
        # TODO: Needs a better name. EIA-861 distributed generation only.
        "constraints": {"enum": FUEL_CLASSES},
    },
    "fuel_consumed_for_electricity_mmbtu": {
        "type": "number",
        "description": "Total consumption of fuel to produce electricity, in physical unit, year to date.",
        "unit": "MMBtu",
    },
    "fuel_consumed_for_electricity_units": {
        "type": "number",
        "description": "Consumption for electric generation of the fuel type in physical unit.",
    },
    "fuel_consumed_mmbtu": {
        "type": "number",
        "description": "Total consumption of fuel in physical unit, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.",
        "unit": "MMBtu",
    },
    "fuel_consumed_total_cost": {
        "type": "number",
        "description": "Total cost of consumed fuel.",
        "unit": "USD",
    },
    "fuel_consumed_units": {
        "type": "number",
        "description": "Consumption of the fuel type in physical unit. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.",
    },
    "fuel_cost": {
        "type": "number",
        "description": "Total fuel cost for plant (in $USD).",
        "unit": "USD",
    },
    "fuel_cost_per_mmbtu_source": {
        "type": "string",
        "description": (
            "Indicates the source of the values in the fuel_cost_per_mmbtu "
            "column. The fuel cost either comes directly from the EIA forms "
            "(original), was filled in from the EIA's API using state-level averages "
            "(eiaapi), was filled in using a rolling average (rolling_avg) or "
            "When the records get aggregated together and contain multiple "
            "sources (mixed)."
        ),
        "constraints": {"enum": ["original", "eiaapi", "rolling_avg", "mixed"]},
    },
    "fuel_mmbtu": {
        "type": "number",
        "description": "Total heat content for plant (in MMBtu).",
        "unit": "MMBtu",
    },
    "fuel_cost_per_mmbtu": {
        "type": "number",
        "description": "Average fuel cost per mmBTU of heat content in nominal USD.",
        "unit": "USD_per_MMBtu",
    },
    "fuel_cost_per_mmbtu_eia": {
        "type": "number",
        "description": "Average fuel cost per mmBTU of heat content in nominal USD.",
        "unit": "USD_per_MMBtu",
    },
    "fuel_cost_per_mmbtu_ferc1": {
        "type": "number",
        "description": "Average fuel cost per mmBTU of heat content in nominal USD.",
        "unit": "USD_per_MMBtu",
    },
    "fuel_cost_per_mwh": {
        "type": "number",
        "description": "Derived from MCOE, a unit level value. Average fuel cost per MWh of heat content in nominal USD.",
        "unit": "USD_per_MWh",
    },
    "fuel_cost_per_mwh_eia": {
        "type": "number",
        "description": "Derived from MCOE, a unit level value. Average fuel cost per MWh of heat content in nominal USD.",
        "unit": "USD_per_MWh",
    },
    "fuel_cost_per_mwh_ferc1": {
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
    "fuel_cost_real_per_mmbtu_eiaaeo": {
        "type": "number",
        "description": (
            "Average fuel cost per mmBTU of heat content in real USD, "
            "standardized to the value of a USD in the year defined by "
            "``real_cost_basis_year``."
        ),
        "unit": "USD_per_MMBtu",
    },
    "fuel_derived_from": {
        "type": "string",
        "description": "Original fuel from which this refined fuel was derived.",
        "constraints": {
            "enum": sorted(
                set(
                    CODE_METADATA["core_eia__codes_energy_sources"]["df"][
                        "fuel_derived_from"
                    ]
                )
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
                set(
                    CODE_METADATA["core_eia__codes_energy_sources"]["df"][
                        "fuel_group_eia"
                    ]
                )
            )
        },
    },
    "fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Heat content of the fuel in millions of Btus per physical unit.",
        "unit": "MMBtu_per_unit",
    },
    "fuel_pct": {
        "type": "number",
        "description": "Percent of fuel",
    },
    "fuel_phase": {
        "type": "string",
        "description": "Physical phase of matter of the fuel.",
        "constraints": {
            "enum": sorted(
                set(
                    CODE_METADATA["core_eia__codes_energy_sources"]["df"][
                        "fuel_phase"
                    ].dropna()
                )
            )
        },
    },
    "fuel_received_mmbtu": {
        "type": "number",
        "description": "Aggregated fuel receipts, in MMBtu, in EIA bulk electricity data.",
        "unit": "MMBtu",
    },
    "fuel_received_units": {
        "type": "number",
        "description": "Quantity of fuel received in tons, barrel, or Mcf.",
    },
    "fuel_switch_energy_source_1": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be able to be used as a sole source of fuel for this unit.",
    },
    "fuel_switch_energy_source_2": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be able to be used as a sole source of fuel for this unit.",
    },
    "fuel_switch_energy_source_3": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be able to be used as a sole source of fuel for this unit.",
    },
    "fuel_switch_energy_source_4": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be able to be used as a sole source of fuel for this unit.",
    },
    "fuel_switch_energy_source_5": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be able to be used as a sole source of fuel for this unit.",
    },
    "fuel_switch_energy_source_6": {
        "type": "string",
        "description": "The codes representing the type of fuel that will be able to be used as a sole source of fuel for this unit.",
    },
    "fuel_type": {
        "type": "string",
        "description": "Type of fuel.",
        # TODO disambiguate column name. This should be just FERC 1 tables, as the EIA
        # fuel types are now all energy_source_code
    },
    "fuel_type_eiaaeo": {
        "type": "string",
        "description": ("Fuel type reported for AEO end-use sector generation data."),
        "constraints": {"enum": FUEL_TYPES_EIAAEO},
    },
    "fuel_type_code_agg": {
        "type": "string",
        "description": (
            "A partial aggregation of the reported fuel type codes into larger "
            "categories used by EIA in, for example, the Annual Energy Review (AER) or "
            "Monthly Energy Review (MER). Two or three letter alphanumeric."
        ),
    },
    "fuel_type_code_pudl": {
        "type": "string",
        "description": "Simplified fuel type code used in PUDL",
        "constraints": {
            "enum": sorted(
                set(
                    CODE_METADATA["core_eia__codes_energy_sources"][
                        "df"
                    ].fuel_type_code_pudl
                )
            )
        },
    },
    "fuel_type_code_pudl_eia": {
        "type": "string",
        "description": "Simplified fuel type code used in PUDL",
        "constraints": {
            "enum": sorted(
                set(
                    CODE_METADATA["core_eia__codes_energy_sources"][
                        "df"
                    ].fuel_type_code_pudl
                )
            )
        },
    },
    "fuel_type_code_pudl_ferc1": {
        "type": "string",
        "description": "Simplified fuel type code used in PUDL",
        "constraints": {
            "enum": sorted(
                set(
                    CODE_METADATA["core_eia__codes_energy_sources"][
                        "df"
                    ].fuel_type_code_pudl
                )
            )
        },
    },
    "fuel_type_count": {
        "type": "integer",
        "description": "A count of how many different simple energy sources there are associated with a generator.",
    },
    "fuel_units": {
        "type": "string",
        "description": "Reported unit of measure for fuel.",
        # Note: Different ENUM constraints are applied below on EIA vs. FERC1
    },
    "furnished_without_charge_mwh": {
        "type": "number",
        "description": (
            "The amount of electricity furnished by the electric utility without "
            "charge, such as to a municipality under a franchise agreement or for "
            "street and highway lighting."
        ),
        "unit": "MWh",
    },
    "gas_fraction_cost": {
        "type": "number",
        "description": "Natural gas cost as a percentage of overall fuel cost.",
    },
    "gas_fraction_mmbtu": {
        "type": "number",
        "description": "Natural gas heat content as a percentage of overall fuel heat content (MMBtu).",
    },
    "generation_activity": {
        "type": "boolean",
        "description": "Whether a utility utilized generation from company owned plant during the year.",
    },
    "generator_id": {
        "type": "string",
        "description": (
            "Generator ID is usually numeric, but sometimes includes letters. Make "
            "sure you treat it as a string!"
        ),
    },
    "generator_id_plant_gen": {
        "type": "string",
        "description": "Generator ID of the record_id_eia_plant_gen record. This is usually numeric, but sometimes includes letters. Make sure you treat it as a string!",
    },
    "generator_id_epa": {
        "type": "string",
        "description": "Generator ID used by the EPA.",
    },
    "generators_num_less_1_mw": {
        "type": "integer",
        "description": "Total number of generators less than 1 MW.",
    },
    "generators_number": {
        "type": "integer",
        "description": "Total number of generators",
    },
    "generator_operating_date": {
        "type": "date",
        "description": "Date the generator began commercial operation. If harvested values are inconsistent, we default to using the most recently reported date.",
    },
    "generator_operating_year": {
        "type": "integer",
        "description": "Year a generator went into service.",
    },
    "generator_operating_year_plant_gen": {
        "type": "integer",
        "description": "The year an associated plant_gen's generator went into service.",
    },
    "generator_retirement_date": {
        "type": "date",
        "description": "Date of the scheduled or effected retirement of the generator.",
    },
    "geo_agg": {
        "type": "string",
        "description": "Category of geographic aggregation in EIA bulk electricity data.",
    },
    "has_green_pricing": {
        "type": "boolean",
        "description": "Whether a green pricing program was associated with this utility during the reporting year.",
    },
    "green_pricing_revenue": {
        "type": "number",
        "description": (
            "The money derived from premium green pricing rate of the respondent's"
            "program."
        ),
        "unit": "USD",
    },
    "grid_voltage_1_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distribution facilities",
        "unit": "kV",
    },
    "grid_voltage_2_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distribution facilities",
        "unit": "kV",
    },
    "grid_voltage_3_kv": {
        "type": "number",
        "description": "Plant's grid voltage at point of interconnection to transmission or distribution facilities",
        "unit": "kV",
    },
    "gross_generation_mwh": {
        "type": "number",
        "description": "Gross electricity generation for the specified period in megawatt-hours (MWh).",
        "unit": "MWh",
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
    "hour_of_year": {
        "type": "integer",
        "description": "Integer between 1 and 8670 representing the hour in a given year.",
    },
    "unit_heat_rate_mmbtu_per_mwh": {
        "type": "number",
        "description": "Fuel content per unit of electricity generated. Coming from MCOE calculation.",
        "unit": "MMBtu_MWh",
    },
    "unit_heat_rate_mmbtu_per_mwh_eia": {
        "type": "number",
        "description": "Fuel content per unit of electricity generated. Coming from MCOE calculation.",
        "unit": "MMBtu_MWh",
    },
    "unit_heat_rate_mmbtu_per_mwh_ferc1": {
        "type": "number",
        "description": "Fuel content per unit of electricity generated. Calculated from FERC reported fuel consumption and net generation.",
        "unit": "MMBtu_MWh",
    },
    "heat_rate_mmbtu_per_mwh": {
        "type": "number",
        "description": "Fuel content per unit of electricity generated.",
        "unit": "MMBtu_MWh",
    },
    "heat_rate_penalty": {
        "type": "number",
        "description": "Heat rate penalty for retrofitting. This column only has contents to retrofit technologies. It seems to be a rate between 0.35 and 0.09",
        "unit": "MMBtu_MWh",
    },
    "highest_distribution_voltage_kv": {
        "type": "number",
        "description": "The highest voltage that's part of the distribution system.",
        "unit": "kV",
    },
    "home_area_network": {
        "type": "integer",
        "description": (
            "Number of AMI meters with home area network (HAN) gateway enabled."
        ),
    },
    "hrsg": {
        "type": "boolean",
        "description": "indicates if the boiler is a heat recovery steam generator (HRSG).",
    },
    "in_rate_base": {
        "type": "boolean",
        "description": (
            "Whether or not a record from the detailed FERC1 accounting tables should "
            "be considered allowable in a utility's rate base based on utility "
            "accounting standards. "
            "This flag was manually compiled by RMI utility accounting experts "
            "based on the xbrl_factoid and sometimes varies based on the utility_type, "
            "plant_status or plant_function."
        ),
    },
    "in_revenue_requirement": {
        "type": "boolean",
        "description": (
            "Whether or not a record from the detailed income statement data is typically "
            "included in a utility's revenue requirement. This flag was manually "
            "compiled by RMI utility accounting experts based on the xbrl_factoid and "
            "sometimes varies based on the utility_type or plant_function."
        ),
    },
    "inactive_accounts_included": {
        "type": "boolean",
        "description": (
            "Whether the respondent includes inactive accounts in its definition of "
            "customers used to determine SAIDI and SAIFI."
        ),
    },
    "include_generator": {
        "type": "boolean",
        "description": (
            "Every row in the aggregation table describes a single generator. Groups "
            "of rows with the same aggregation are combined using a capacity weighted "
            "average to produce an aggregate generation profile. A few generators "
            "are not included in that aggregation process. This column determines "
            "whether a generator is included."
        ),
    },
    "income_type": {
        "type": "string",
        "description": "Type of income reported in core_ferc1__yearly_income_statements_sched114 table.",
        "constraints": {
            "enum": INCOME_TYPES_FERC1.extend(
                # Add all possible correction records to enum
                [
                    "accretion_expense_correction",
                    "accretion_expense_subdimension_correction",
                    "allowance_for_borrowed_funds_used_during_construction_credit_correction",
                    "allowance_for_borrowed_funds_used_during_construction_credit_subdimension_correction",
                    "allowance_for_other_funds_used_during_construction_correction",
                    "allowance_for_other_funds_used_during_construction_subdimension_correction",
                    "amortization_and_depletion_of_utility_plant_correction",
                    "amortization_and_depletion_of_utility_plant_subdimension_correction",
                    "amortization_of_conversion_expenses_correction",
                    "amortization_of_conversion_expenses_subdimension_correction",
                    "amortization_of_debt_discount_and_expense_correction",
                    "amortization_of_debt_discount_and_expense_subdimension_correction",
                    "amortization_of_electric_plant_acquisition_adjustments_correction",
                    "amortization_of_electric_plant_acquisition_adjustments_subdimension_correction",
                    "amortization_of_gain_on_reacquired_debt_credit_correction",
                    "amortization_of_gain_on_reacquired_debt_credit_subdimension_correction",
                    "amortization_of_loss_on_reacquired_debt_correction",
                    "amortization_of_loss_on_reacquired_debt_subdimension_correction",
                    "amortization_of_premium_on_debt_credit_correction",
                    "amortization_of_premium_on_debt_credit_subdimension_correction",
                    "amortization_of_property_losses_unrecovered_plant_and_regulatory_study_costs_correction",
                    "amortization_of_property_losses_unrecovered_plant_and_regulatory_study_costs_subdimension_correction",
                    "costs_and_expenses_of_merchandising_jobbing_and_contract_work_correction",
                    "costs_and_expenses_of_merchandising_jobbing_and_contract_work_subdimension_correction",
                    "depreciation_expense_correction",
                    "depreciation_expense_for_asset_retirement_costs_correction",
                    "depreciation_expense_for_asset_retirement_costs_subdimension_correction",
                    "depreciation_expense_subdimension_correction",
                    "donations_correction",
                    "donations_subdimension_correction",
                    "equity_in_earnings_of_subsidiary_companies_correction",
                    "equity_in_earnings_of_subsidiary_companies_subdimension_correction",
                    "expenditures_for_certain_civic_political_and_related_activities_correction",
                    "expenditures_for_certain_civic_political_and_related_activities_subdimension_correction",
                    "expenses_of_nonutility_operations_correction",
                    "expenses_of_nonutility_operations_subdimension_correction",
                    "extraordinary_deductions_correction",
                    "extraordinary_deductions_subdimension_correction",
                    "extraordinary_income_correction",
                    "extraordinary_income_subdimension_correction",
                    "extraordinary_items_after_taxes_correction",
                    "extraordinary_items_after_taxes_subdimension_correction",
                    "gain_on_disposition_of_property_correction",
                    "gain_on_disposition_of_property_subdimension_correction",
                    "gains_from_disposition_of_allowances_correction",
                    "gains_from_disposition_of_allowances_subdimension_correction",
                    "gains_from_disposition_of_plant_correction",
                    "gains_from_disposition_of_plant_subdimension_correction",
                    "income_before_extraordinary_items_correction",
                    "income_before_extraordinary_items_subdimension_correction",
                    "income_taxes_extraordinary_items_correction",
                    "income_taxes_extraordinary_items_subdimension_correction",
                    "income_taxes_federal_correction",
                    "income_taxes_federal_subdimension_correction",
                    "income_taxes_operating_income_correction",
                    "income_taxes_operating_income_subdimension_correction",
                    "income_taxes_other_correction",
                    "income_taxes_other_subdimension_correction",
                    "income_taxes_utility_operating_income_other_correction",
                    "income_taxes_utility_operating_income_other_subdimension_correction",
                    "interest_and_dividend_income_correction",
                    "interest_and_dividend_income_subdimension_correction",
                    "interest_on_debt_to_associated_companies_correction",
                    "interest_on_debt_to_associated_companies_subdimension_correction",
                    "interest_on_long_term_debt_correction",
                    "interest_on_long_term_debt_subdimension_correction",
                    "investment_tax_credit_adjustments_correction",
                    "investment_tax_credit_adjustments_nonutility_operations_correction",
                    "investment_tax_credit_adjustments_nonutility_operations_subdimension_correction",
                    "investment_tax_credit_adjustments_subdimension_correction",
                    "investment_tax_credits_correction",
                    "investment_tax_credits_subdimension_correction",
                    "life_insurance_correction",
                    "life_insurance_subdimension_correction",
                    "loss_on_disposition_of_property_correction",
                    "loss_on_disposition_of_property_subdimension_correction",
                    "losses_from_disposition_of_allowances_correction",
                    "losses_from_disposition_of_allowances_subdimension_correction",
                    "losses_from_disposition_of_service_company_plant_correction",
                    "losses_from_disposition_of_service_company_plant_subdimension_correction",
                    "maintenance_expense_correction",
                    "maintenance_expense_subdimension_correction",
                    "miscellaneous_amortization_correction",
                    "miscellaneous_amortization_subdimension_correction",
                    "miscellaneous_nonoperating_income_correction",
                    "miscellaneous_nonoperating_income_subdimension_correction",
                    "net_extraordinary_items_correction",
                    "net_extraordinary_items_subdimension_correction",
                    "net_income_loss_correction",
                    "net_income_loss_subdimension_correction",
                    "net_interest_charges_correction",
                    "net_interest_charges_subdimension_correction",
                    "net_other_income_and_deductions_correction",
                    "net_other_income_and_deductions_subdimension_correction",
                    "net_utility_operating_income_correction",
                    "net_utility_operating_income_subdimension_correction",
                    "nonoperating_rental_income_correction",
                    "nonoperating_rental_income_subdimension_correction",
                    "operating_revenues_correction",
                    "operating_revenues_subdimension_correction",
                    "operation_expense_correction",
                    "operation_expense_subdimension_correction",
                    "other_deductions_correction",
                    "other_deductions_subdimension_correction",
                    "other_income_correction",
                    "other_income_deductions_correction",
                    "other_income_deductions_subdimension_correction",
                    "other_income_subdimension_correction",
                    "other_interest_expense_correction",
                    "other_interest_expense_subdimension_correction",
                    "penalties_correction",
                    "penalties_subdimension_correction",
                    "provision_for_deferred_income_taxes_credit_operating_income_correction",
                    "provision_for_deferred_income_taxes_credit_operating_income_subdimension_correction",
                    "provision_for_deferred_income_taxes_credit_other_income_and_deductions_correction",
                    "provision_for_deferred_income_taxes_credit_other_income_and_deductions_subdimension_correction",
                    "provision_for_deferred_income_taxes_other_income_and_deductions_correction",
                    "provision_for_deferred_income_taxes_other_income_and_deductions_subdimension_correction",
                    "provisions_for_deferred_income_taxes_utility_operating_income_correction",
                    "provisions_for_deferred_income_taxes_utility_operating_income_subdimension_correction",
                    "regulatory_credits_correction",
                    "regulatory_credits_subdimension_correction",
                    "regulatory_debits_correction",
                    "regulatory_debits_subdimension_correction",
                    "revenues_from_merchandising_jobbing_and_contract_work_correction",
                    "revenues_from_merchandising_jobbing_and_contract_work_subdimension_correction",
                    "revenues_from_nonutility_operations_correction",
                    "revenues_from_nonutility_operations_subdimension_correction",
                    "taxes_on_other_income_and_deductions_correction",
                    "taxes_on_other_income_and_deductions_subdimension_correction",
                    "taxes_other_than_income_taxes_other_income_and_deductions_correction",
                    "taxes_other_than_income_taxes_other_income_and_deductions_subdimension_correction",
                    "taxes_other_than_income_taxes_utility_operating_income_correction",
                    "taxes_other_than_income_taxes_utility_operating_income_subdimension_correction",
                    "utility_operating_expenses_correction",
                    "utility_operating_expenses_subdimension_correction",
                ]
            )
        },
    },
    "increase_in_other_regulatory_liabilities": {
        "type": "number",
        "description": "The increase during the reporting period of other regulatory liabilities.",
        "unit": "USD",
    },
    "incremental_energy_savings_mwh": {
        "type": "number",
        "description": (
            "energy savings in the given report year resulting from new participants "
            "in existing demand response programs and all participants in new demand "
            "response programs."
        ),
        "unit": "MWh",
    },
    "incremental_life_cycle_energy_savings_mwh": {
        "type": "number",
        "description": (
            "The estimated total changes in energy use for incremental programs and "
            "participants over the life of the programs. DSM programs have a useful "
            "life, and the net effects of these programs will diminish over time. "
            "Considers the useful life of energy efficiency technology by accounting "
            "for building demolition, equipment degradation, and program attrition."
        ),
        "unit": "MWh",
    },
    "incremental_life_cycle_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The estimated total changes in peak load for incremental programs and "
            "participants over the life of the programs. DSM programs have a useful "
            "life, and the net effects of these programs will diminish over time. "
            "Considers the useful life of energy efficiency technology by accounting "
            "for building demolition, equipment degradation, and program attrition."
        ),
        "unit": "MW",
    },
    "incremental_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The peak reduction incurred in a given reporting year by new "
            "participants in existing energy efficiency programs and all participants "
            "in new energy efficiency programs."
        ),
        "unit": "MW",
    },
    "industry_name_sic": {
        "type": "string",
        "description": "Text description of Standard Industrial Classification (SIC)",
    },
    "industry_id_sic": {
        "type": "string",
        "description": (
            "Four-digit Standard Industrial Classification (SIC) code identifying "
            "the company's primary industry. SIC codes have been replaced by NAICS "
            "codes in many applications, but are still used by the SEC. See e.g. "
            "https://www.osha.gov/data/sic-manual for code definitions."
        ),
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "installation_year": {
        "type": "integer",
        "description": "Year the plant's most recently built unit was installed.",
    },
    "installation_year_eia": {
        "type": "integer",
        "description": "Year the plant's most recently built unit was installed.",
    },
    "installation_year_ferc1": {
        "type": "integer",
        "description": "Year the plant's most recently built unit was installed.",
    },
    "intake_distance_shore_feet": {
        "description": "Maximum distance from shore to intake",
        "type": "number",
        "unit": "ft",
    },
    "intake_distance_surface_feet": {
        "description": "Average distance below water surface to intake",
        "type": "number",
        "unit": "ft",
    },
    "intake_rate_100pct_gallons_per_minute": {
        "description": (
            "Design cooling water flow rate at 100 percent load at in-take"
        ),
        "type": "number",
        "unit": "gpm",
    },
    "interchange_adjusted_mwh": {
        "type": "number",
        "description": "Energy interchange between adjacent balancing authorities, adjusted by EIA to reflect non-physical commercial transfers through pseudo-ties and dynamic scheduling.",
        "unit": "MWh",
    },
    "interchange_imputed_eia_mwh": {
        "type": "number",
        "description": "Energy interchange between adjacent balancing authorities, with outliers and missing values imputed by EIA.",
        "unit": "MWh",
    },
    "interchange_reported_mwh": {
        "type": "number",
        "description": "Original reported energy interchange between adjacent balancing authorities.",
        "unit": "MWh",
    },
    "interconnect_code_eia": {
        "type": "string",
        "description": "EIA interconnect code.",
        "constraints": {"enum": {"eastern", "western", "ercot"}},
    },
    "taxpayer_id_irs": {
        "type": "string",
        "description": "Taxpayer ID of the company with the IRS.",
        "constraints": {
            "pattern": r"^\d{2}-\d{7}$",
        },
    },
    "is_epacems_state": {
        "type": "boolean",
        "description": (
            "Indicates whether the associated state reports data within the EPA's "
            "Continuous Emissions Monitoring System."
        ),
    },
    "is_generation_only": {
        "type": "boolean",
        "description": "Indicates whether the balancing authority is generation-only, meaning it does not serve retail customers and thus reports only net generation and interchange, but not demand.",
    },
    "iso_rto_code": {
        "type": "string",
        "description": "The code of the plant's ISO or RTO. NA if not reported in that year.",
    },
    "kwh_per_customer": {"type": "number", "description": "kWh per customer."},
    "label": {
        "type": "string",
        "description": "Longer human-readable code using snake_case",
    },
    "latitude": {
        "type": "number",
        "description": "Latitude of the plant's location, in degrees.",
    },
    "levelized_cost_of_energy_per_mwh": {
        "type": "number",
        "description": "Levelized cost of energy (LCOE) is a summary metric that combines the primary technology cost and performance parameters: capital expenditures, operations expenditures, and capacity factor.",
        "unit": "USD_per_Mwh",
    },
    "liability_type": {
        "type": "string",
        "description": "Type of liability being reported to the core_ferc1__yearly_balance_sheet_liabilities_sched110 table.",
        "constraints": {
            "enum": LIABILITY_TYPES_FERC1.extend(
                # Add all possible correction records to enum
                [
                    "current_and_accrued_liabilities_correction",
                    "deferred_credits_correction",
                    "liabilities_and_other_credits_correction",
                    "long_term_debt_correction",
                    "other_noncurrent_liabilities_correction",
                    "proprietary_capital_correction",
                    "retained_earnings_correction",
                    "unappropriated_undistributed_subsidiary_earnings_correction",
                ]
            )
        },
    },
    "license_id_ferc1": {
        "type": "integer",
        "description": "FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible.",
    },
    "liquefied_natural_gas_storage": {
        "type": "boolean",
        "description": "Indicates if the facility have the capability to store the natural gas in the form of liquefied natural gas.",
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "load_management_annual_actual_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The peak reduction incurred in a given reporting year by all participants "
            "in load management programs."
        ),
        "unit": "MW",
    },
    "load_management_annual_cost": {
        "type": "number",
        "description": (
            "The sum of actual direct costs, incentive payments, and indirect costs "
            "incurred in a given reporting year from load management programs."
        ),
        "unit": "USD",
    },
    "load_management_annual_effects_mwh": {
        "type": "number",
        "description": (
            "The change in energy use incurred in a given reporting year by "
            "all participants in load management programs."
        ),
        "unit": "MWh",
    },
    "load_management_annual_incentive_payment": {
        "type": "number",
        "description": (
            "The cost of incentive payments incurred in a given reporting year "
            "from load management programs. Incentives are the "
            "total financial value provided to a customer for program participation, "
            "whether cash payment, in-kind services (e.g. design work), or other "
            "benefits directly provided customer for their program participation."
        ),
        "unit": "USD",
    },
    "load_management_annual_potential_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The potential amount of peak reduction that could be incurred in a given "
            "reporting year by all participants in load management programs."
        ),
        "unit": "MW",
    },
    "load_management_incremental_actual_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The peak reduction incurred in a given reporting year by new "
            "participants in existing load management programs and all participants "
            "in new load management programs."
        ),
        "unit": "MW",
    },
    "load_management_incremental_effects_mwh": {
        "type": "number",
        "description": (
            "The change in energy use incurred in a given reporting year by "
            "new participants in existing load management programs and all "
            "participants in new load management programs."
        ),
        "unit": "MWh",
    },
    "load_management_incremental_potential_peak_reduction_mw": {
        "type": "number",
        "description": (
            "The potential amount of peak reduction that could be incurred in a given "
            "reporting year by new participants in existing load management programs "
            "and all participants in new load management programs."
        ),
        "unit": "MW",
    },
    "longitude": {
        "type": "number",
        "description": "Longitude of the plant's location, in degrees.",
    },
    "major_program_changes": {
        "type": "boolean",
        "description": (
            "Whether there have been any major changes to the respondent's demand-side "
            "management programs (e.g., terminated programs, new information or "
            "financing programs, or a shift to programs with dual load building "
            "objectives and energy efficiency objectives), program tracking "
            "procedures, or reporting methods that affect the comparison of "
            "demand-side management data reported on this schedule to data from "
            "previous years."
        ),
    },
    "match_type": {
        "type": "string",
        "description": "Indicates the source and validation of the match between EIA and FERC. Match types include matches was generated from the model, verified by the training data, overridden by the training data, etc.",
    },
    "max_charge_rate_mw": {
        "type": "number",
        "description": "Maximum charge rate in MW.",
        "unit": "MW",
    },
    "max_discharge_rate_mw": {
        "type": "number",
        "description": "Maximum discharge rate in MW.",
        "unit": "MW",
    },
    "max_fuel_mmbtu_per_unit": {
        "type": "number",
        "description": "Maximum heat content per physical unit of fuel in MMBtu.",
        "unit": "MMBtu",
    },
    "max_oil_heat_input": {
        "type": "number",
        "description": "The maximum oil heat input (percent of MMBtus) expected for proposed unit when co-firing with natural gas",
        "unit": "% MMBtu",
    },
    "max_oil_output_mw": {
        "type": "number",
        "description": "The maximum output (net MW) expected for proposed unit, when making the maximum use of oil and co-firing natural gas.",
        "unit": "MW",
    },
    "max_steam_flow_1000_lbs_per_hour": {
        "type": "number",
        "unit": "1000_lbs_per_hour",
        "description": "Maximum continuous steam flow at 100 percent load.",
    },
    "mercury_content_ppm": {
        "type": "number",
        "description": "Mercury content in parts per million (ppm) to the nearest 0.001 ppm.",
        "unit": "ppm",
    },
    "mercury_control_existing_strategy_1": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_existing_strategy_2": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_existing_strategy_3": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_existing_strategy_4": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_existing_strategy_5": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_existing_strategy_6": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_id_eia": {
        "type": "string",
        "description": "Mercury control identification number. This ID is not a unique identifier.",
    },
    "mercury_control_proposed_strategy_1": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_proposed_strategy_2": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent mercury regulation.",
    },
    "mercury_control_proposed_strategy_3": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent mercury regulation.",
    },
    "merge_address": {
        "type": "string",
        "description": "Address of new parent company.",
    },
    "merge_city": {
        "type": "string",
        "description": "City of new parent company.",
    },
    "merge_company": {
        "type": "string",
        "description": "Name of the company merged with or acquired.",
    },
    "merge_date": {"type": "date", "description": "Date of merger or acquisition."},
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
    "mine_state": {
        "type": "string",
        "description": "State where the coal mine is located. Two letter abbreviation.",
    },
    "mine_type_code": {
        "type": "string",
        "description": "Type of coal mine.",
    },
    "minimum_load_mw": {
        "type": "number",
        "description": "The minimum load at which the generator can operate at continuosuly.",
        "unit": "MW",
    },
    "model_case_eiaaeo": {
        "type": "string",
        "description": (
            "Factors such as economic growth, future oil prices, the ultimate "
            "size of domestic energy resources, and technological change are "
            "often uncertain. To illustrate some of these uncertainties, EIA "
            "runs side cases to show how the model responds to changes in key "
            "input variables compared with the Reference case. See "
            "https://www.eia.gov/outlooks/aeo/assumptions/case_descriptions.php "
            "for more details."
        ),
        "constraints": {"enum": MODEL_CASES_EIAAEO},
    },
    "moisture_content_pct": {
        "type": "number",
        "description": (
            "For coal only: the moisture content of the fuel in terms of moisture "
            "percentage by weight. Reported to the nearest 0.01 percent."
        ),
    },
    "momentary_interruption_definition": {
        "type": "string",
        "description": (
            "How the respondent defines momentary service interruptions: less than 1 "
            "min, equal to or less than 5 min, or some other way."
        ),
    },
    "monthly_average_consumption_rate_gallons_per_minute": {
        "description": "Monthly average consumption rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "monthly_average_discharge_rate_gallons_per_minute": {
        "description": "Monthly average discharge rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "monthly_average_discharge_temperature_fahrenheit": {
        "description": "Average cooling water temperature at discharge point",
        "type": "number",
        "unit": "F",
    },
    "monthly_average_diversion_rate_gallons_per_minute": {
        "description": "Monthly average diversion rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "monthly_average_intake_temperature_fahrenheit": {
        "description": "Average cooling water temperature at intake point",
        "type": "number",
        "unit": "F",
    },
    "monthly_average_withdrawal_rate_gallons_per_minute": {
        "description": "Monthly average withdrawal rate of cooling water",
        "type": "number",
        "unit": "gpm",
    },
    "monthly_maximum_discharge_temperature_fahrenheit": {
        "description": "Maximum cooling water temperature at discharge",
        "type": "number",
        "unit": "F",
    },
    "monthly_maximum_intake_temperature_fahrenheit": {
        "description": "Maximum cooling water temperature at intake",
        "type": "number",
        "unit": "F",
    },
    "monthly_total_chlorine_lbs": {
        "description": (
            "Amount of elemental chlorine added to cooling water monthly. "
            "May be just the amount of chlorine-containing compound if "
            "schedule 9 is filled out."
        ),
        "type": "number",
        "unit": "lb",
    },
    "monthly_total_consumption_volume_gallons": {
        "description": "Monthly volume of water consumed at consumption point (accurate to 0.1 million gal)",
        "type": "number",
        "unit": "gal",
    },
    "monthly_total_cooling_hours_in_service": {
        "description": "Total hours the system operated during the month",
        "type": "integer",
        "unit": "hr",
    },
    "monthly_total_discharge_volume_gallons": {
        "description": "Monthly volume of water discharged at discharge point (accurate to 0.1 million gal)",
        "type": "number",
        "unit": "gal",
    },
    "monthly_total_diversion_volume_gallons": {
        "description": "Monthly volume of water diverted at diversion point (accurate to 0.1 million gal)",
        "type": "number",
        "unit": "gal",
    },
    "monthly_total_withdrawal_volume_gallons": {
        "description": "Monthly volume of water withdrawn at withdrawal point (accurate to 0.1 million gal)",
        "type": "number",
        "unit": "gal",
    },
    "can_burn_multiple_fuels": {
        "type": "boolean",
        "description": "Whether the generator can burn multiple fuels.",
    },
    "name_change_date": {
        "type": "date",
        "description": "Date of last name change of the company.",
    },
    "nameplate_power_factor": {
        "type": "number",
        "description": "The nameplate power factor of the generator.",
    },
    "natural_gas_delivery_contract_type_code": {
        "type": "string",
        "description": "Contract type for natural gas delivery service:",
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
        "description": "Indicates if the facility have on-site storage of natural gas.",
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
        "description": (
            "All the regional entities within the North American Electric Reliability "
            "Corporation (NERC) in which the respodent conducts operations."
        ),
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
    "net_demand_forecast_mwh": {
        "type": "number",
        "description": "Net forecasted electricity demand for the specific period in megawatt-hours (MWh).",
        "unit": "MWh",
    },
    "net_generation_adjusted_mwh": {
        "type": "number",
        "description": "Reported net generation adjusted by EIA to reflect non-physical commercial transfers through pseudo-ties and dynamic scheduling.",
        "unit": "MWh",
    },
    "net_generation_imputed_eia_mwh": {
        "type": "number",
        "description": "Reported net generation with outlying values removed and missing values imputed by EIA.",
        "unit": "MWh",
    },
    "net_generation_reported_mwh": {
        "type": "number",
        "description": "Unaltered originally reported net generation for the specified period.",
        "unit": "MWh",
    },
    "net_generation_mwh": {
        "type": "number",
        "description": "Net electricity generation for the specified period in megawatt-hours (MWh).",
        "unit": "MWh",
        # TODO: disambiguate as this column means something different in
        # core_eia923__monthly_generation_fuel:
        # "description": "Net generation, year to date in megawatthours (MWh). This is total electrical output net of station service.  In the case of combined heat and power plants, this value is intended to include internal consumption of electricity for the purposes of a production process, as well as power put on the grid.",
    },
    "net_generation_mwh_eia": {
        "type": "number",
        "description": "Net electricity generation for the specified period in megawatt-hours (MWh).",
        "unit": "MWh",
    },
    "net_generation_mwh_ferc1": {
        "type": "number",
        "description": "Net electricity generation for the specified period in megawatt-hours (MWh).",
        "unit": "MWh",
    },
    "net_load_mwh": {
        "type": "number",
        "description": "Net output for load (net generation - energy used for pumping) in megawatt-hours.",
        "unit": "MWh",
    },
    "has_net_metering": {
        "type": "boolean",
        "description": "Whether the plant has a net metering agreement in effect during the reporting year.  (Only displayed for facilities that report the sun or wind as an energy source). This field was only reported up until 2015",
        # TODO: Is this really boolean? Or do we have non-null strings that mean False?
    },
    "net_output_penalty": {
        "type": "number",
        "description": "Penalty for retrofitting for net output.  This column only has contents to retrofit technologies. It seems to be a rate between -0.25 and -0.08",
    },
    "net_power_exchanged_mwh": {
        "type": "number",
        "description": (
            "The net amount of energy exchanged. Net exchange is the difference "
            "between the amount of exchange received and the amount of exchange "
            "delivered. This entry should not include wholesale energy purchased from "
            "or sold to regulated companies or unregulated companies for other systems."
        ),
        "unit": "MWh",
    },
    "net_summer_capacity_natural_gas_mw": {
        "type": "number",
        "description": "The maximum net summer output achievable when running on natural gas.",
        "unit": "MW",
    },
    "net_summer_capacity_oil_mw": {
        "type": "number",
        "description": "The maximum net summer output achievable when running on oil.",
        "unit": "MW",
    },
    "net_winter_capacity_natural_gas_mw": {
        "type": "number",
        "description": "The maximum net winter output achievable when running on natural gas.",
        "unit": "MW",
    },
    "net_winter_capacity_oil_mw": {
        "type": "number",
        "description": "The maximum net summer output achievable when running on oil.",
        "unit": "MW",
    },
    "net_wheeled_power_mwh": {
        "type": "number",
        "description": (
            "The difference between the amount of energy entering the respondent's "
            "system (wheeled received) for transmission through the respondent's "
            "system and the amount of energy leaving the respondent's system (wheeled "
            "delivered). Wheeled net represents the energy losses on the respondent's "
            "system associated with the wheeling of energy for other systems."
        ),
        "unit": "MWh",
    },
    "new_parent": {
        "type": "string",
        "description": "Name of the new parent company post merger.",
    },
    "new_source_review": {
        "type": "boolean",
        "description": "Indicates whether the boiler is subject to New Source Review requirements.",
    },
    "new_source_review_date": {
        "type": "date",
        "description": "Month of issued New Source Review permit.",
    },
    "new_source_review_permit": {
        "type": "string",
        "description": "New Source Review permit number.",
    },
    "non_amr_ami": {
        "type": "integer",
        "description": (
            "Number of non-AMR/AMI meters. Usually electromechanical or solid state "
            "meters measuring aggregated kWh where data are manually retrieved over "
            "monthly billing cycles for billing purposes only. Standard meters may "
            "also include functions to measure time-of-use and/or demand with data "
            "manually retrieved over monthly billing cycles."
        ),
    },
    "non_coincident_peak_demand_mw": {
        "type": "number",
        "description": "Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts.",
        "unit": "MW",
    },
    "not_water_limited_capacity_mw": {
        "type": "number",
        "description": "Plant capacity in MW when not limited by condenser water.",
        "unit": "MW",
    },
    "nox_control_existing_caaa_compliance_strategy_1": {
        "type": "string",
        "description": "Existing strategies to meet the nitrogen oxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "nox_control_existing_caaa_compliance_strategy_2": {
        "type": "string",
        "description": "Existing strategies to meet the nitrogen oxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "nox_control_existing_caaa_compliance_strategy_3": {
        "type": "string",
        "description": "Existing strategies to meet the nitrogen oxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "nox_control_existing_strategy_1": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent nitrogen oxide regulation.",
    },
    "nox_control_existing_strategy_2": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent nitrogen oxide regulation.",
    },
    "nox_control_existing_strategy_3": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent nitrogen oxide regulation.",
    },
    "nox_control_id_eia": {
        "type": "string",
        "description": "Nitrogen oxide control identification number. This ID is not a unique identifier.",
    },
    "nox_control_manufacturer": {
        "type": "string",
        "description": "Name of nitrogen oxide control manufacturer.",
    },
    "nox_control_manufacturer_code": {
        "type": "string",
        "description": "Code indicating the nitrogen oxide control burner manufacturer.",
    },
    "nox_control_out_of_compliance_strategy_1": {
        "type": "string",
        "description": "If boiler is not in compliance with nitrogen oxide regulations, strategy for compliance.",
    },
    "nox_control_out_of_compliance_strategy_2": {
        "type": "string",
        "description": "If boiler is not in compliance with nitrogen oxide regulations, strategy for compliance.",
    },
    "nox_control_out_of_compliance_strategy_3": {
        "type": "string",
        "description": "If boiler is not in compliance with nitrogen oxide regulations, strategy for compliance.",
    },
    "nox_control_planned_caaa_compliance_strategy_1": {
        "type": "string",
        "description": "Planned strategies to meet the nitrogen oxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "nox_control_planned_caaa_compliance_strategy_2": {
        "type": "string",
        "description": "Planned strategies to meet the nitrogen oxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "nox_control_planned_caaa_compliance_strategy_3": {
        "type": "string",
        "description": "Planned strategies to meet the nitrogen oxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "nox_control_proposed_strategy_1": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent nitrogen oxide regulation.",
    },
    "nox_control_proposed_strategy_2": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent nitrogen oxide regulation.",
    },
    "nox_control_proposed_strategy_3": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent nitrogen oxide regulation.",
    },
    "nox_control_status_code": {
        "type": "string",
        "description": "Nitrogen oxide control status code.",
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
    "nuclear_fraction_cost": {
        "type": "number",
        "description": "Nuclear cost as a percentage of overall fuel cost.",
    },
    "nuclear_fraction_mmbtu": {
        "type": "number",
        "description": "Nuclear heat content as a percentage of overall fuel heat content (MMBtu).",
    },
    "nuclear_unit_id": {
        "type": "string",
        "description": "For nuclear plants only, the unit number .One digit numeric. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level.",
    },
    "num_transmission_circuits": {
        "type": "integer",
        "description": "Number of circuits in a transmission line.",
    },
    "oil_fraction_cost": {
        "type": "number",
        "description": "Oil cost as a percentage of overall fuel cost.",
    },
    "oil_fraction_mmbtu": {
        "type": "number",
        "description": "Oil heat content as a percentage of overall fuel heat content (MMBtu).",
    },
    "operates_generating_plant": {
        "type": "boolean",
        "description": (
            "Whether the respondent operated at least one generating plant during the "
            "reporting period."
        ),
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
    "operating_voltage_kv": {
        "type": "number",
        "description": "The operating voltage, expressed kilo-volts, for three-phase 60 cycle alternative current transmission lines.",
        "unit": "KV",
    },
    "operational_status": {
        "type": "string",
        "description": "The operating status of the asset. For generators this is based on which tab the generator was listed in in EIA 860.",
    },
    "operational_status_code": {
        "type": "string",
        "description": "The operating status of the asset.",
    },
    "operational_status_pudl": {
        "type": "string",
        "description": "The operating status of the asset using PUDL categories.",
        "constraints": {"enum": ["operating", "retired", "proposed"]},
    },
    "operational_status_pudl_plant_gen": {
        "type": "string",
        "description": "The operating status of the asset using PUDL categories of the record_id_eia_plant_gen record .",
        "constraints": {"enum": ["operating", "retired", "proposed"]},
    },
    "operator_utility_id_eia": {
        "type": "integer",
        "description": "The EIA utility Identification number for the operator utility.",
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
    "opex_fgd_feed_materials_chemical": {
        "type": "integer",
        "unit": "USD",
        "description": "Annual operation and maintenance expenditures for feed materials and chemicals for flue gas desulfurization equipment, excluding electricity.",
    },
    "opex_fgd_labor_supervision": {
        "type": "integer",
        "unit": "USD",
        "description": "Annual operation and maintenance expenditures for labor and supervision of flue gas desulfurization equipment, excluding electricity.",
    },
    "opex_fgd_land_acquisition": {
        "type": "integer",
        "unit": "USD",
        "description": "Annual operation and maintenance expenditures for land acquisition for flue gas desulfurization equipment, excluding electricity.",
    },
    "opex_fgd_maintenance_material_other": {
        "type": "integer",
        "unit": "USD",
        "description": "Annual operation and maintenance expenditures for maintenance, materials and all other costs of flue gas desulfurization equipment, excluding electricity",
    },
    "opex_fgd_total_cost": {
        "type": "integer",
        "unit": "USD",
        "description": "Annual total cost of operation and maintenance expenditures on flue gas desulfurization equipment, excluding electricity",
    },
    "opex_fgd_waste_disposal": {
        "type": "integer",
        "unit": "USD",
        "description": "Annual operation and maintenance expenditures for waste disposal, excluding electricity.",
    },
    "opex_fixed_per_kw": {
        "type": "number",
        "description": "Fixed operation and maintenance expenses. Annual expenditures to operate and maintain equipment that are not incurred on a per-unit-energy basis.",
        "unit": "USD_per_kw",
    },
    "opex_variable_per_mwh": {
        "type": "number",
        "description": "Operation and maintenance costs incurred on a per-unit-energy basis.",
        "unit": "USD_per_MWh",
    },
    "opex_fuel": {
        "type": "number",
        "description": "Production expenses: fuel (USD).",
        "unit": "USD",
    },
    "opex_fuel_per_mwh": {
        "type": "number",
        "description": "Production expenses: fuel (USD) per megawatt-hour (Mwh).",
        "unit": "USD_per_Mwh",
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
    "opex_nonfuel_per_mwh": {
        "type": "number",
        "description": "Investments in non-fuel production expenses per Mwh.",
        "unit": "USD_per_Mwh",
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
    "opex_total_nonfuel": {  # To do: if identical to `opex_total`, rename column.
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
    "original_planned_generator_operating_date": {
        "type": "date",
        "description": "The date the generator was originally scheduled to be operational",
    },
    "other_charges": {
        "type": "number",
        "description": "Other charges, including out-of-period adjustments (USD).",
        "unit": "USD",
    },
    "other_combustion_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses other combustion technologies",
    },
    "other_costs": {
        "type": "number",
        "description": "Additional costs.",
        "unit": "USD",
    },
    "other_costs_incremental_cost": {
        "type": "number",
        "description": (
            "Costs resulting from new participants in existing energy efficiency "
            "programs and all participants in new energy efficiency programs that "
            "aren't directly associated with customer incentives."
        ),
        "unit": "USD",
    },
    "other_modifications_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete.",
    },
    "other_planned_modifications": {
        "type": "boolean",
        "description": "Indicates whether there are there other modifications planned for the generator.",
    },
    "outages_recorded_automatically": {
        "type": "boolean",
        "description": (
            "Whether the information about customer outages is recorded automatically."
        ),
    },
    "outlet_distance_shore_feet": {
        "description": "Maximum distance from shore to outlet",
        "type": "number",
        "unit": "ft",
    },
    "outlet_distance_surface_feet": {
        "description": "Average distance below water surface to outlet",
        "type": "number",
        "unit": "ft",
    },
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
        "description": "The EIA utility Identification number for the owner company that is responsible for the day-to-day operations of the generator, not the operator utility.",
    },
    "owner_utility_name_eia": {
        "type": "string",
        "description": "The name of the EIA owner utility.",
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
    "parent_company_central_index_key": {
        "type": "string",
        "description": "Central index key (CIK) of the parent company.",
    },
    "parent_company_business_city": {
        "type": "string",
        "description": "City where the parent company's place of business is located.",
    },
    "parent_company_business_state": {
        "type": "string",
        "description": "State where the parent company's place of business is located.",
    },
    "parent_company_business_street_address": {
        "type": "string",
        "description": "Street address of the parent company's place of business.",
    },
    "parent_company_business_street_address_2": {
        "type": "string",
        "description": "Second line of the street address of the parent company's place of business.",
    },
    "parent_company_business_zip_code": {
        "type": "string",
        "description": "Zip code of the parent company's place of business.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "parent_company_business_zip_code_4": {
        "type": "string",
        "description": "Zip code suffix of the company's place of business.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "parent_company_incorporation_state": {
        "type": "string",
        "description": "Two letter state code where parent company is incorporated.",
    },
    "parent_company_industry_name_sic": {
        "type": "string",
        "description": "Text description of the parent company's Standard Industrial Classification (SIC)",
    },
    "parent_company_industry_id_sic": {
        "type": "string",
        "description": "Four-digit Standard Industrial Classification (SIC) code identifying "
        "the parent company's primary industry. SIC codes have been replaced by NAICS "
        "codes in many applications, but are still used by the SEC. See e.g. "
        "https://www.osha.gov/data/sic-manual for code definitions.",
    },
    "parent_company_mail_city": {
        "type": "string",
        "description": "City of the parent company's mailing address.",
    },
    "parent_company_mail_state": {
        "type": "string",
        "description": "State of the parent company's mailing address.",
    },
    "parent_company_mail_street_address": {
        "type": "string",
        "description": "Street portion of the parent company's mailing address.",
    },
    "parent_company_mail_street_address_2": {
        "type": "string",
        "description": "Second line of the street portion of the parent company's mailing address.",
    },
    "parent_company_mail_zip_code": {
        "type": "string",
        "description": "Zip code of the parent company's mailing address.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "parent_company_mail_zip_code_4": {
        "type": "string",
        "description": "Zip code suffix of the parent company's mailing address.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "parent_company_name": {
        "type": "string",
        "description": "Name of the parent company.",
    },
    "parent_company_phone_number": {
        "type": "string",
        "description": "Phone number of the parent company.",
    },
    "parent_company_taxpayer_id_irs": {
        "type": "string",
        "description": "Taxpayer ID of the parent company with the IRS.",
        "constraints": {
            "pattern": r"^\d{2}-\d{7}$",
        },
    },
    "parent_company_utility_id_eia": {
        "type": "integer",
        "description": "The EIA utility ID of the parent company.",
    },
    "parent_company_utility_name_eia": {
        "type": "string",
        "description": "The EIA reported utility name of the parent company.",
    },
    "particulate_control_id_eia": {
        "type": "string",
        "description": "Particulate matter control identification number. This ID is not a unique identifier.",
    },
    "particulate_control_out_of_compliance_strategy_1": {
        "type": "string",
        "description": "If boiler is not in compliance with particulate matter regulations, strategy for compliance.",
    },
    "particulate_control_out_of_compliance_strategy_2": {
        "type": "string",
        "description": "If boiler is not in compliance with particulate matter regulations, strategy for compliance.",
    },
    "particulate_control_out_of_compliance_strategy_3": {
        "type": "string",
        "description": "If boiler is not in compliance with particulate matter regulations, strategy for compliance.",
    },
    "partitions": {
        "type": "string",
        "description": "The data partitions used to generate this instance of the database.",
    },
    "peak_demand_mw": {
        "type": "number",
        "unit": "MW",
        "description": "Net peak demand for 60 minutes. Note: in some cases peak demand for other time periods may have been reported instead, if hourly peak demand was unavailable.",
        # TODO Disambiguate column names. Usually this is over 60 minutes, but in
        # other tables it's not specified.
    },
    "percent_dry_cooling": {
        "description": "Percent of cooling load served by dry cooling components",
        "type": "number",
    },
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
    "place_name": {
        "type": "string",
        "description": (
            "County or lake name, sourced from the latest Census PEP vintage based on "
            "county FIPS ID. Lake names originate from VCE RARE directly, and may also "
            "appear several times--once for each state it touches. FIPS ID values for "
            "lakes have been nulled."
        ),
    },
    "planned_derate_date": {
        "type": "date",
        "description": "Planned effective month that the generator is scheduled to enter operation after the derate modification.",
    },
    "planned_energy_source_code_1": {
        "type": "string",
        "description": "New energy source code for the planned repowered generator.",
    },
    "planned_generator_retirement_date": {
        "type": "date",
        "description": "Planned effective date of the scheduled retirement of the generator.",
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
    "planned_uprate_date": {
        "type": "date",
        "description": "Planned effective date that the generator is scheduled to enter operation after the uprate modification.",
    },
    "plant_capability_mw": {
        "type": "number",
        "description": "Net plant capability in megawatts.",
        "unit": "MW",
    },
    "plant_function": {
        "type": "string",
        "description": "Functional role played by utility plant (steam production, nuclear production, distribution, transmission, etc.).",
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
    "plant_status": {
        "type": "string",
        "description": "Utility plant financial status (in service, future, leased, total).",
        # TODO 2024-05-01: add enum constraint
    },
    "plant_summer_capacity_mw": {
        "description": "The plant summer capacity associated with the operating generators at the plant",
        "type": "number",
        "unit": "MW",
    },
    "plant_type": {
        "type": "string",
        "description": "Type of plant.",
        # TODO: if plant_type is categorized w/ categorize_strings, add enum in FIELD_METADATA_BY_RESOURCE
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
    "pond_cost": {
        "description": (
            "Actual installed cost for the existing cooling ponds or the "
            "anticipated cost to bring the cooling ponds into commercial "
            "operation"
        ),
        "type": "number",
        "unit": "USD",
    },
    "pond_landfill_requirements_acre_foot_per_year": {
        "type": "number",
        "unit": "acre_foot_per_year",
        "description": "Annual pond and land fill requirements for flue gas desulfurization equipment.",
    },
    "pond_operating_date": {
        "description": "Cooling ponds actual or projected in-service date",
        "type": "date",
    },
    "pond_surface_area_acres": {
        "description": "Total surface area of cooling pond",
        "type": "number",
        "unit": "acre",
    },
    "pond_volume_acre_feet": {
        "description": "Total volume of water in cooling pond",
        "type": "number",
        "unit": "acre-feet",
    },
    "population": {
        "type": "number",
        "description": "County population, sourced from Census DP1 data.",
    },
    "population_density_km2": {
        "type": "number",
        "description": "Average population per sq. km area of a service territory.",
    },
    "potential_peak_demand_savings_mw": {
        "type": "number",
        "description": "The total demand savings that could occur at the time of the system peak hour assuming all demand response is called.",
        "unit": "MW",
    },
    "power_requirement_mw": {
        "description": "Maximum power requirement for cooling towers at 100 percent load",
        "type": "number",
        "unit": "MW",
    },
    "previously_canceled": {
        "type": "boolean",
        "description": "Indicates whether the generator was previously reported as indefinitely postponed or canceled",
    },
    "price_responsive_programs": {
        "type": "boolean",
        "description": (
            "Whether the respondent operates any incentive-based demand response "
            "programs (e.g., market incentives, financial incentives, direct load "
            "control, interruptible programs, demand bidding/buyback, emergency demand "
            "response, capacity market programs, and ancillary service market "
            "programs)."
        ),
    },
    "price_responsiveness_customers": {
        "type": "integer",
        "description": (
            "The number of customers participating in the respondent's incentive-based "
            "demand response programs."
        ),
    },
    "primary_fuel_by_cost": {
        "type": "string",
        "description": "Primary fuel for plant as a percentage of cost.",
    },
    "primary_fuel_by_mmbtu": {
        "type": "string",
        "description": "Primary fuel for plant as a percentage of heat content.",
    },
    "primary_purpose_id_naics": {
        "type": "integer",
        "description": "North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant",
    },
    "primary_transportation_mode_code": {
        "type": "string",
        "description": "Transportation mode for the longest distance transported.",
    },
    "prime_mover_code": {
        "type": "string",
        "description": "Code for the type of prime mover (e.g. CT, CG)",
    },
    "prime_mover_code_plant_gen": {
        "type": "string",
        "description": "Code for the type of prime mover (e.g. CT, CG) associated with the record_id_eia_plant_gen.",
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
        "description": "Megawatt-hours shown on bills rendered to the respondent. Includes both electricity purchased for storage and non-storage purposes, which were lumped together prior to 2021.",
        "unit": "MWh",
    },
    "purchased_other_than_storage_mwh": {
        "type": "number",
        "description": "Number of megawatt hours purchased during the period for other than energy storage.",
        "unit": "MWh",
    },
    "purchased_storage_mwh": {
        "type": "number",
        "description": "Number of megawatt hours purchased during the period for energy storage.",
        "unit": "MWh",
    },
    "pv_current_flow_type": {
        "type": "string",
        "description": "Current flow type for photovoltaics: AC or DC",
        "constraints": {"enum": ["AC", "DC"]},
    },
    "rate_schedule_description": {
        "type": "string",
        "description": "Free-form description of what the rate schedule name is. Not standardized. Often a sub-category of rate_schedule_type.",
    },
    "rate_schedule_type": {
        "type": "string",
        "description": "Categorization of rate schedule type.",
    },
    "reactive_power_output_mvar": {
        "type": "number",
        "description": "Reactive Power Output (MVAr)",
        "unit": "MVAr",
    },
    "real_cost_basis_year": {
        "type": "integer",
        "description": "Four-digit year which is the basis for any 'real cost' "
        "monetary values (as opposed to nominal values).",
    },
    "real_time_pricing": {
        "type": "boolean",
        "description": (
            "Whether the respondent has customers participating in a real time pricing "
            "(RTP) program. RTP is a program of rate and price structure in which the "
            "retail price for electricity typically fluctuates hourly or more often, "
            "to reflect changes in the wholesale price of electricity on either a day- "
            "ahead or hour-ahead basis."
        ),
    },
    "rec_revenue": {
        "type": "number",
        "description": (
            "Amount of revenue collected from Renewable Energy Certificates (RECs)."
        ),
        "unit": "USD",
    },
    "rec_sales_mwh": {
        "type": "number",
        "description": (
            "Amount of sales collected from Renewable Energy Certificates (RECs)."
        ),
        "unit": "MWh",
    },
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
        "description": "Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.",
    },
    "record_id_eia": {
        "type": "string",
        "description": "Identifier for EIA plant parts analysis records.",
    },
    "record_id_eia_plant_gen": {
        "type": "string",
        "description": "Identifier for EIA plant parts analysis records which is at the plant_part level of plant_gen - meaning each record pertains to one generator.",
    },
    "record_id_ferc1": {
        "type": "string",
        "description": "Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.",
    },
    "region_name_eiaaeo": {
        "type": "string",
        "description": (
            "EIA AEO region for energy consumption. Includes US Census Divisions plus United States."
        ),
        "constraints": {
            "enum": [
                # 2025-05 kmm: we can't use POLITICAL_SUBDIVISIONS here because
                # it splits Pacific into Contiguous and Noncontiguous.
                "east_north_central",
                "east_south_central",
                "middle_atlantic",
                "mountain",
                "new_england",
                "pacific",
                "south_atlantic",
                "west_north_central",
                "west_south_central",
                "united_states",
            ],
        },
    },
    "region_name_us_census": {
        "type": "string",
        "description": "Human-readable name of a US Census region.",
    },
    "region_type_eiaaeo": {
        "type": "string",
        "description": (
            "Region type for EIA AEO energy consumption, indicating whether region_name_eiaaeo is a US Census Division or country (United States)"
        ),
        "constraints": {
            "enum": [
                "us_census_division",
                "country",
            ],
        },
    },
    "has_regulatory_limits": {
        "type": "boolean",
        "description": "Whether there are factors that limit the operation of the generator when running on 100 percent oil",
    },
    "regulation_mercury": {
        "type": "string",
        "description": "Most stringent type of statute or regulation code under which the boiler is operating for mercury control standards.",
    },
    "regulation_nox": {
        "type": "string",
        "description": "EIA short code for most stringent type of statute or regulation code under which the boiler is operating for nitrogen oxide control standards.",
    },
    "regulation_particulate": {
        "type": "string",
        "description": "EIA short code for most stringent type of statute or regulation code under which the boiler is operating for particulate matter control standards.",
    },
    "regulation_so2": {
        "type": "string",
        "description": "EIA short code for most stringent type of statute or regulation code under which the boiler is operating for sulfur dioxide control standards.",
    },
    "regulatory_status_code": {
        "type": "string",
        "description": "Indicates whether the plant is regulated or non-regulated.",
    },
    "report_date": {"type": "date", "description": "Date reported."},
    "report_timezone": {
        "type": "string",
        "description": "Timezone used by the reporting entity. For use in localizing UTC times.",
        "constraints": {"enum": US_TIMEZONES},
    },
    "report_year": {
        "type": "integer",
        "description": "Four-digit year in which the data was reported.",
    },
    "reported_as_another_company": {
        "type": "string",
        "description": (
            "The name of the company if a respondent's demand-side management "
            "activities are reported on Schedule 6 of another company’s form."
        ),
    },
    "reporting_frequency_code": {
        "type": "string",
        "description": "Code that specifies what time period data has to be reported (i.e. monthly data or annual totals) and how often the power plant reports this data to EIA. See reporting_frequencies_eia for more details.",
        "constraints": {
            "enum": sorted(
                set(
                    CODE_METADATA["core_eia__codes_reporting_frequencies"]["df"]["code"]
                )
            )
        },
    },
    "respondent_id_ferc714": {
        "type": "integer",
        "description": (
            "PUDL-assigned identifying a respondent to FERC Form 714. This ID associates "
            "natively reported respondent IDs from the original CSV and XBRL data sources."
        ),
    },
    "respondent_id_ferc714_csv": {
        "type": "integer",
        "description": (
            "FERC Form 714 respondent ID from CSV reported data - published from years: 2006-2020. "
            "This ID is linked to the newer years of reported XBRL data through the PUDL-assigned "
            "respondent_id_ferc714 ID. "
            "This ID was originally reported as respondent_id. "
            "Note that this ID does not correspond to FERC respondent IDs from other forms."
        ),
    },
    "respondent_id_ferc714_xbrl": {
        "type": "string",
        "description": (
            "FERC Form 714 respondent ID from XBRL reported data - published from years: 2021-present. "
            "This ID is linked to the older years of reported CSV data through the PUDL-assigned "
            "respondent_id_ferc714 ID. "
            "This ID was originally reported as entity_id. "
            "Note that this ID does not correspond to FERC respondent IDs from other forms."
        ),
    },
    "respondent_name_ferc714": {
        "type": "string",
        "description": "Name of the utility, balancing area authority, or planning authority responding to FERC Form 714.",
    },
    "respondent_type": {
        "type": "string",
        "description": (
            "Whether a respondent to the FERC form 714 is a utility or a balancing "
            "authority."
        ),
        "constraints": {"enum": ["utility", "balancing_authority"]},
    },
    "retail_marketing_activity": {
        "type": "boolean",
        "description": "Whether a utility engaged in retail power marketing during the year.",
    },
    "retail_sales_mwh": {
        "type": "number",
        "description": (
            "MWh of sales to end-use customers in areas where the customer has been "
            "given the legal right to select a power supplier other than the "
            "traditional, vertically integrated electric utility."
        ),
        "unit": "MWh",
    },
    "retirements": {
        "type": "number",
        "description": "Cost of disposal of items classified within the account.",
        "unit": "USD",
    },
    "revenue": {"type": "number", "description": "Amount of revenue.", "unit": "USD"},
    "revenue_class": {
        "type": "string",
        "description": f"Source of revenue: {REVENUE_CLASSES}",
        "constraints": {"enum": REVENUE_CLASSES},
    },
    "revenue_per_kwh": {
        "type": "number",
        "description": (
            "The amount of revenue per kWh by rate schedule acquired in the given "
            "report year."
        ),
        "unit": "USD",
    },
    "revenue_type": {
        "type": "string",
        "description": "Label describing types of revenues.",
    },
    "revenue_requirement_category": {
        "type": "string",
        "description": (
            "The category of revenue requirement associated with each component of utility's"
            "income statements. "
            "These categories were manually compiled by RMI utility accounting experts "
            "based on the xbrl_factoid and sometimes vary based on the utility_type or "
            "plant_function. This column is intended to be used to aggregate this "
            "table."
        ),
        "constraints": {
            "enum": [
                "depreciation_amortization_depletion",
                "depreciation_arc",
                "fuel",
                "investment_tax_credit",
                "maintenance",
                "non_fuel_operation",
                "other",
                "purchased_power",
                "regulatory_debits_credits",
                "taxes",
            ]
        },
    },
    "revenue_requirement_technology": {
        "type": "string",
        "description": (
            "The technology type associated with components of a utility's "
            "revenue requirement. "
            "These categories were manually compiled by RMI utility accounting experts "
            "based on the xbrl_factoid and sometimes vary based on the utility_type or "
            "plant_function as well. This column is intended to be used to aggregate this "
            "table."
        ),
        "constraints": {
            "enum": [
                "administrative",
                "common_plant_electric",
                "customer_accounts",
                "customer_service",
                "distribution",
                "general",
                "hydraulic_production",
                "hydraulic_production_conventional",
                "hydraulic_production_pumped_storage",
                "intangible",
                "nuclear_production",
                "other",
                "other_electric_plant",
                "other_power_supply",
                "other_production",
                "purchased_power",
                "regional_transmission_and_market_operation",
                "sales",
                "steam_production",
                "transmission",
            ]
        },
    },
    "row_type_xbrl": {
        "type": "string",
        "description": "Indicates whether the value reported in the row is calculated, or uniquely reported within the table.",
        "constraints": {
            "enum": [
                "calculated_value",
                "reported_value",
                "correction",
                "subdimension_correction",
            ]
        },
    },
    "rto_iso_lmp_node_id": {
        "type": "string",
        "description": "The designation used to identify the price node in RTO/ISO Locational Marginal Price reports",
    },
    "rto_iso_location_wholesale_reporting_id": {
        "type": "string",
        "description": "The designation used to report the specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report",
    },
    "rtos_of_operation": {
        "type": "string",
        "description": ("The ISOs/RTOs, in which the respondent conducts operations."),
        "constraints": {"enum": RTO_CLASSES},
    },
    "saidi_w_major_event_days_minus_loss_of_service_minutes": {
        "type": "number",
        "description": (
            "Cumulative duration (minutes) of interruption for the average customer "
            "during the report year including major event days and excluding "
            "reliability events caused by a loss of supply."
        ),
        "unit": "min",
    },
    "saidi_w_major_event_days_minutes": {
        "type": "number",
        "description": (
            "Cumulative duration (minutes) of interruption for the average customer "
            "during the report year including major event days."
        ),
        "unit": "min",
    },
    "saidi_wo_major_event_days_minutes": {
        "type": "number",
        "description": (
            "Cumulative duration (minutes) of interruption for the average customer "
            "during the report year excluding major event days."
        ),
        "unit": "min",
    },
    "saifi_w_major_event_days_customers": {
        "type": "number",
        "description": (
            "Average number of times a customer experienced a sustained interruption "
            "(over 5 minutes) during the report year including major event days."
        ),
    },
    "saifi_w_major_event_days_minus_loss_of_service_customers": {
        "type": "number",
        "description": (
            "Average number of times a customer experienced a sustained interruption "
            "(over 5 minutes) during the report year including major event days and "
            "excluding reliability events caused by a loss of supply."
        ),
    },
    "saifi_wo_major_event_days_customers": {
        "type": "number",
        "description": (
            "Average number of times a customer experienced a sustained interruption "
            "(over 5 minutes) during the report year excluding major event days."
        ),
    },
    "sales_for_resale_mwh": {
        "type": "number",
        "description": (
            "The amount of electricity sold for resale purposes. This entry should "
            "include sales for resale to power marketers (reported separately in "
            "previous years), full and partial requirements customers, firm power "
            "customers and nonfirm customers."
        ),
        "unit": "MWh",
    },
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
    "sales_to_ultimate_consumers_mwh": {
        "type": "number",
        "description": (
            "The amount of electricity sold to customers purchasing electricity for "
            "their own use and not for resale."
        ),
        "unit": "MWh",
    },
    "scaled_demand_mwh": {
        "type": "number",
        "description": "Estimated electricity demand scaled by the total sales within a state.",
        "unit": "MWh",
    },
    "sec_act": {
        "type": "string",
        "description": "SEC Act through which the form was enacted, e.g. 1934 act.",
        "constraints": {
            "enum": ["1934 act"],
        },
    },
    "filing_number_sec": {
        "type": "string",
        "description": "Filing number used internally by the SEC commission to track filing.",
    },
    "sec10k_type": {
        "type": "string",
        "description": (
            "Specific version of SEC 10-K that was filed. 10-k: the standard annual "
            "report. 10-k/a: an amended version of the annual report. 10-k405: filed "
            "to report insider trading that was not reported in a timely fashion. "
            "10-k405/a: an amended version of the 10-k405. 10-kt: submitted in lieu of "
            "or in addition to a standard 10-K annual report when a company changes "
            "the end of its fiscal year (e.g. due to a merger) leaving the company "
            "with a longer or shorter reporting period. 10-kt/a: an amended version of "
            "the 10-kt. 10-ksb: the annual report for small businesses, also known as "
            "penny stocks. 10-ksb/a: an amended version of the 10-ksb."
        ),
        "constraints": {
            "enum": [
                "10-k",
                "10-k/a",
                "10-k405",
                "10-k405/a",
                "10-kt",
                "10-kt/a",
                "10-ksb",
                "10-ksb/a",
            ]
        },
    },
    "secondary_transportation_mode_code": {
        "type": "string",
        "description": "Transportation mode for the second longest distance transported.",
    },
    "sector_agg": {
        "type": "string",
        "description": "Category of sectoral aggregation in EIA bulk electricity data.",
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
    "served_arbitrage": {
        "type": "boolean",
        "description": "Whether the energy storage device served arbitrage applications during the reporting year",
    },
    "served_backup_power": {
        "type": "boolean",
        "description": "Whether the energy storage device served backup power applications during the reporting year.",
    },
    "served_co_located_renewable_firming": {
        "type": "boolean",
        "description": "Whether the energy storage device served renewable firming applications during the reporting year.",
    },
    "served_frequency_regulation": {
        "type": "boolean",
        "description": "Whether the energy storage device served frequency regulation applications during the reporting year.",
    },
    "served_load_following": {
        "type": "boolean",
        "description": "Whether the energy storage device served load following applications during the reporting year.",
    },
    "served_load_management": {
        "type": "boolean",
        "description": "Whether the energy storage device served load management applications during the reporting year.",
    },
    "served_ramping_spinning_reserve": {
        "type": "boolean",
        "description": "Whether the this energy storage device served ramping / spinning reserve applications during the reporting year.",
    },
    "served_system_peak_shaving": {
        "type": "boolean",
        "description": "Whether the energy storage device served system peak shaving applications during the reporting year.",
    },
    "served_transmission_and_distribution_deferral": {
        "type": "boolean",
        "description": "Whether the energy storage device served renewable firming applications during the reporting year.",
    },
    "served_voltage_or_reactive_power_support": {
        "type": "boolean",
        "description": "Whether the energy storage device served voltage or reactive power support applications during the reporting year.",
    },
    "service_area": {
        "type": "string",
        "description": "Service area in which plant is located; for unregulated companies, it's the electric utility with which plant is interconnected",
    },
    "service_type": {
        "type": "string",
        "description": (
            "The type of service the respondent provides to a given customer class."
            "Bundled: both energy and delivery; energy: just the energy consumed; "
            "delivery: just the billing and energy delivery services."
        ),
        "constraints": {"enum": ["bundled", "energy", "delivery"]},
    },
    "short_form": {
        "type": "boolean",
        "description": (
            "Whether the reported information comes from the short form. In the case "
            "of form EIA 861, a shorter version of the form was created in 2012 to "
            "reduce respondent burden on smaller utilities and increase our processing "
            "efficiency."
        ),
    },
    "sludge_disposal_cost": {
        "type": "number",
        "unit": "USD",
        "description": (
            "Actual installed costs for the existing sludge transport and disposal "
            "systems or the anticipated costs of sludge transport and disposal systems "
            "to bring a planned system into commercial operation."
        ),
    },
    "sludge_pond": {
        "type": "boolean",
        "description": "Indicates if there is a sludge pond associated with this unit.",
    },
    "sludge_pond_lined": {
        "type": "boolean",
        "description": "Indicates whether the sludge pond is lined.",
    },
    "so2_control_existing_caaa_compliance_strategy_1": {
        "type": "string",
        "description": "Existing strategies to meet the sulfur dioxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "so2_control_existing_caaa_compliance_strategy_2": {
        "type": "string",
        "description": "Existing strategies to meet the sulfur dioxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "so2_control_existing_caaa_compliance_strategy_3": {
        "type": "string",
        "description": "Existing strategies to meet the sulfur dioxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "so2_control_existing_strategy_1": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent sulfur dioxide regulation.",
    },
    "so2_control_existing_strategy_2": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent sulfur dioxide regulation.",
    },
    "so2_control_existing_strategy_3": {
        "type": "string",
        "description": "Existing strategy to comply with the most stringent sulfur dioxide regulation.",
    },
    "so2_control_id_eia": {
        "type": "string",
        "description": "Sulfur dioxide control identification number. This ID is not a unique identifier.",
    },
    "so2_control_out_of_compliance_strategy_1": {
        "type": "string",
        "description": "If boiler is not in compliance with sulfur dioxide regulations, strategy for compliance.",
    },
    "so2_control_out_of_compliance_strategy_2": {
        "type": "string",
        "description": "If boiler is not in compliance with sulfur dioxide regulations, strategy for compliance.",
    },
    "so2_control_out_of_compliance_strategy_3": {
        "type": "string",
        "description": "If boiler is not in compliance with sulfur dioxide regulations, strategy for compliance.",
    },
    "so2_control_planned_caaa_compliance_strategy_1": {
        "type": "string",
        "description": "Planned strategies to meet the sulfur dioxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "so2_control_planned_caaa_compliance_strategy_2": {
        "type": "string",
        "description": "Planned strategies to meet the sulfur dioxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "so2_control_planned_caaa_compliance_strategy_3": {
        "type": "string",
        "description": "Planned strategies to meet the sulfur dioxide requirements of Title IV of the Clean Air Act Amendment of 1990.",
    },
    "so2_control_proposed_strategy_1": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent sulfur dioxide regulation.",
    },
    "so2_control_proposed_strategy_2": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent sulfur dioxide regulation.",
    },
    "so2_control_proposed_strategy_3": {
        "type": "string",
        "description": "Proposed strategy to comply with the most stringent sulfur dioxide regulation.",
    },
    "so2_emission_rate_lbs_per_hour": {
        "type": "number",
        "unit": "lbs_per_hour",
        "description": "Sulfur dioxide emission rate when operating at 100 percent load (pounds per hour).",
    },
    "so2_equipment_type_1": {
        "type": "string",
        "description": "Type of sulfur dioxide control equipment.",
    },
    "so2_equipment_type_2": {
        "type": "string",
        "description": "Type of sulfur dioxide control equipment.",
    },
    "so2_equipment_type_3": {
        "type": "string",
        "description": "Type of sulfur dioxide control equipment.",
    },
    "so2_equipment_type_4": {
        "type": "string",
        "description": "Type of sulfur dioxide control equipment.",
    },
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
    "so2_removal_efficiency_design": {
        "type": "number",
        "description": "Designed removal efficiency for sulfur dioxide when operating at 100 percent load. Reported at the nearest 0.1 percent by weight of gases removed from the flue gas.",
    },
    "so2_removal_efficiency_tested": {
        "type": "number",
        "description": "Removal efficiency for sulfur dioxide (to the nearest 0.1 percent by weight) at tested rate at 100 percent load.",
    },
    "so2_removal_efficiency_annual": {
        "type": "number",
        "description": "Removal efficiency for sulfur dioxide (to the nearest 0.1 percent by weight) based on designed firing rate and hours in operation (listed as a percentage).",
    },
    "so2_test_date": {
        "type": "date",
        "description": "Date of most recent test for sulfur dioxide removal efficiency.",
    },
    "sold_to_utility_mwh": {
        "type": "number",
        "description": (
            "The amount of electric energy sold back to the utility through the net "
            "metering application."
        ),
        "unit": "MWh",
    },
    "sorbent_type_1": {
        "type": "string",
        "description": "Type of sorbent used by this sulfur dioxide control equipment.",
    },
    "sorbent_type_2": {
        "type": "string",
        "description": "Type of sorbent used by this sulfur dioxide control equipment.",
    },
    "sorbent_type_3": {
        "type": "string",
        "description": "Type of sorbent used by this sulfur dioxide control equipment.",
    },
    "sorbent_type_4": {
        "type": "string",
        "description": "Type of sorbent used by this sulfur dioxide control equipment.",
    },
    "solid_fuel_gasification": {
        "type": "boolean",
        "description": "Indicates whether the generator is part of a solid fuel gasification system",
    },
    "source_url": {
        "type": "string",
        "description": "URL pointing to the original source of the data in the record.",
        "constraints": {
            "pattern": r"^https?://.+",
        },
    },
    "specifications_of_coal_ash": {
        "type": "number",
        "description": (
            "Design fuel specifications for ash when burning coal or petroleum coke "
            "(nearest 0.1 percent by weight)."
        ),
    },
    "specifications_of_coal_sulfur": {
        "type": "number",
        "description": (
            "Design fuel specifications for sulfur when burning coal or petroleum coke "
            "(nearest 0.1 percent by weight)."
        ),
    },
    "stack_flue_id_eia": {
        "type": "string",
        "description": (
            "The stack or flue identification value reported to EIA. This denotes the "
            "place where emissions from the combustion process are released into the "
            "atmosphere. Prior to 2013, this was reported as `stack_id_eia` and "
            "`flue_id_eia`."
        ),
    },
    "stack_flue_id_pudl": {
        "type": "string",
        "description": (
            "A stack and/or flue identification value created by PUDL for use as part "
            "of the primary key for the stack flue equipment and boiler association "
            "tables. For 2013 and onward, this value is equal to the value for "
            "stack_flue_id_eia. Prior to 2013, this value is equal to the value for "
            "stack_id_eia and the value for flue_id_eia separated by an underscore or "
            "just the stack_flue_eia in cases where flue_id_eia is NA."
        ),
    },
    "stack_id_eia": {
        "type": "string",
        "description": (
            "The stack identification value reported to EIA. Stacks or chimneys are "
            "the place where emissions from the combustion process are released into "
            "the atmosphere. This field was reported in conjunction with flue_id_eia "
            "until 2013 when stack_flue_id_eia took their place."
        ),
    },
    "standard": {
        "type": "string",
        "description": (
            "Whether the respondent calculates SAIDI/SAIFI, and major event days "
            "according to the IEEE or an Other standard."
        ),
        "constraints": {"enum": RELIABILITY_STANDARDS},
        # TODO: Might want to make this column more specific to outages: ex: outage calculation standard.
    },
    "standard_nox_rate": {
        "type": "number",
        "description": "Numeric value for the unit of measurement specified for nitrogen oxide.",
    },
    "standard_particulate_rate": {
        "type": "number",
        "description": "Numeric value for the unit of measurement specified for particulate matter.",
    },
    "standard_so2_rate": {
        "type": "number",
        "description": "Numeric value for the unit of measurement specified for sulfur dioxide.",
    },
    "standard_so2_percent_scrubbed": {
        "type": "number",
        "description": "The percent of sulfur dioxide to be scrubbed specified by the most stringent sulfur dioxide regulation.",
    },
    "start_point": {
        "type": "string",
        "description": "The starting point of a transmission line.",
    },
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
    "incorporation_state": {
        "type": "string",
        "description": "Two letter state code where company is incorporated.",
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
    "storage_enclosure_code": {
        "type": "string",
        "description": "A code representing the enclosure type that best describes where the generator is located.",
    },
    "storage_technology_code_1": {
        "type": "string",
        "description": "The electro-chemical storage technology used for this battery applications.",
    },
    "storage_technology_code_2": {
        "type": "string",
        "description": "The electro-chemical storage technology used for this battery applications.",
    },
    "storage_technology_code_3": {
        "type": "string",
        "description": "The electro-chemical storage technology used for this battery applications.",
    },
    "storage_technology_code_4": {
        "type": "string",
        "description": "The electro-chemical storage technology used for this battery applications.",
    },
    "stored_excess_wind_and_solar_generation": {
        "type": "boolean",
        "description": "Whether the energy storage device was used to store excess wind/solar generation during the reporting year.",
    },
    "street_address": {
        "type": "string",
        # TODO: Disambiguate as this means different things in different tables.
        "description": "Physical street address.",
    },
    "subcritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses subcritical technology",
    },
    "subdivision_code": {
        "type": "string",
        "description": (
            "Two-letter ISO-3166 political subdivision code (e.g. US state "
            "or Canadian province abbreviations like CA or AB)."
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
    "subplant_id": {
        "type": "integer",
        "description": "Sub-plant ID links EPA CEMS emissions units to EIA units.",
    },
    "subsidiary_company_central_index_key": {
        "type": "string",
        "description": "Central index key (CIK) of the subsidiary company.",
    },
    "subsidiary_company_business_city": {
        "type": "string",
        "description": "City where the subsidiary company's place of business is located.",
    },
    "subsidiary_company_business_state": {
        "type": "string",
        "description": "State where the subsidiary company's place of business is located.",
    },
    "subsidiary_company_business_street_address": {
        "type": "string",
        "description": "Street address of the subsidiary company's place of business.",
    },
    "subsidiary_company_business_street_address_2": {
        "type": "string",
        "description": "Second line of the street address of the subsidiary company's place of business.",
    },
    "subsidiary_company_business_zip_code": {
        "type": "string",
        "description": "Zip code of the subsidiary company's place of business.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "subsidiary_company_business_zip_code_4": {
        "type": "string",
        "description": "Zip code suffix of the subsidiary company's place of business.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "subsidiary_company_incorporation_state": {
        "type": "string",
        "description": "Two letter state code where subisidary company is incorporated.",
    },
    "subsidiary_company_id_sec10k": {
        "type": "string",
        "description": (
            "PUDL-assigned ID for subsidiaries found in SEC 10-K Exhibit 21. "
            "The ID is created by concatenating the CIK of the company whose filing the subsidiary "
            "was found in, the subsidiary company's name, and location of incorporation. It is not "
            "guaranteed to be stable across different releases of PUDL and so should never be "
            "hard-coded in analyses."
        ),
    },
    "subsidiary_company_industry_name_sic": {
        "type": "string",
        "description": "Text description of the subsidiary company's Standard Industrial Classification (SIC)",
    },
    "subsidiary_company_industry_id_sic": {
        "type": "string",
        "description": "Four-digit Standard Industrial Classification (SIC) code identifying "
        "the subsidiary company's primary industry. SIC codes have been replaced by NAICS "
        "codes in many applications, but are still used by the SEC. See e.g. "
        "https://www.osha.gov/data/sic-manual for code definitions.",
    },
    "subsidiary_company_location": {
        "type": "string",
        "description": (
            "Location of subsidiary company. This is the full US state name or country name "
            "and occasionally a two digit code that was not mapped to a full name during cleaning."
        ),
    },
    "subsidiary_company_mail_city": {
        "type": "string",
        "description": "City of the subsidiary company's mailing address.",
    },
    "subsidiary_company_mail_state": {
        "type": "string",
        "description": "State of the parent company's mailing address.",
    },
    "subsidiary_company_mail_street_address": {
        "type": "string",
        "description": "Street portion of the subsidiary company's mailing address.",
    },
    "subsidiary_company_mail_street_address_2": {
        "type": "string",
        "description": "Second line of the street portion of the subsidiary company's mailing address.",
    },
    "subsidiary_company_mail_zip_code": {
        "type": "string",
        "description": "Zip code of the subsidiary company's mailing address.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "subsidiary_company_mail_zip_code_4": {
        "type": "string",
        "description": "Zip code suffix of the subsidiary company's mailing address.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "subsidiary_company_name": {
        "type": "string",
        "description": "Name of subsidiary company.",
    },
    "subsidiary_company_phone_number": {
        "type": "string",
        "description": "Phone number of the subsidiary company.",
    },
    "subsidiary_company_taxpayer_id_irs": {
        "type": "string",
        "description": "Taxpayer ID of the subsidiary company with the IRS.",
        "constraints": {
            "pattern": r"^\d{2}-\d{7}$",
        },
    },
    "subsidiary_company_utility_id_eia": {
        "type": "integer",
        "description": "The EIA utility ID of the subsidiary company.",
    },
    "subsidiary_company_utility_name_eia": {
        "type": "string",
        "description": "The EIA reported utility name of the subsidiary company.",
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
    "summer_capacity_planned_additions_mw": {
        "type": "number",
        "description": (
            "The total planned additions to net summer generating capacity."
        ),
        "unit": "mw",
    },
    "summer_capacity_retirements_mw": {
        "type": "number",
        "description": ("The total retirements from net summer generating capacity."),
        "unit": "mw",
    },
    "summer_capacity_unplanned_additions_mw": {
        "type": "number",
        "description": (
            "The total unplanned additions to net summer generating capacity."
        ),
        "unit": "mw",
    },
    "summer_estimated_capability_mw": {
        "type": "number",
        "description": "EIA estimated summer capacity (in MWh).",
        "unit": "MWh",
    },
    "summer_peak_demand_forecast_mw": {
        "type": "number",
        "description": (
            "The maximum forecasted hourly sumemr load (for the months of June through "
            "September)."
        ),
        "unit": "MW",
    },
    "summer_peak_demand_mw": {
        "type": "number",
        "description": (
            "The maximum hourly summer load (for the months of June through September) "
            "based on net energy for the system during the reporting year. Net energy "
            "for the system is the sum of energy an electric utility needs to satisfy "
            "their service area and includes full and partial wholesale requirements "
            "customers, and the losses experienced in delivery. The maximum hourly "
            "load is determined by the interval in which the 60-minute integrated "
            "demand is the greatest."
        ),
        "unit": "MW",
    },
    "supercritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses supercritical technology",
    },
    "supplier_name": {
        "type": "string",
        "description": "Company that sold the fuel to the plant or, in the case of Natural Gas, pipeline owner.",
    },
    "supporting_structure_type": {
        "type": "string",
        "description": "Supporting structure of the transmission line.",
    },
    "can_switch_oil_gas": {
        "type": "boolean",
        "description": "Whether the generator can switch between oil and natural gas.",
    },
    "synchronized_transmission_grid": {
        "type": "boolean",
        "description": "Indicates whether standby generators (SB status) can be synchronized to the grid.",
    },
    "tariff": {
        "type": "string",
        "description": "FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)",
    },
    "tech_class": {
        "type": "string",
        "description": (
            "Type of technology specific to EIA 861 distributed generation and net "
            f"generation tables: {TECH_CLASSES}."
        ),
        "constraints": {"enum": TECH_CLASSES},
    },
    "technology_description": {
        "type": "string",
        "description": "High level description of the technology used by the generator to produce electricity.",
    },
    "technology_description_plant_gen": {
        "type": "string",
        "description": "High level description of the technology used by the record_id_eia_plant_gen's generator to produce electricity.",
    },
    "technology_description_detail_1": {
        "type": "string",
        "description": "Technology details indicate resource levels and specific technology subcategories.",
    },
    "technology_description_detail_2": {
        "type": "string",
        "description": "Technology details indicate resource levels and specific technology subcategories.",
    },
    "technology_description_eiaaeo": {
        "type": "string",
        "description": "Generation technology reported for AEO.",
        "constraints": {
            "enum": TECH_DESCRIPTIONS_EIAAEO,
        },
    },
    "temperature_method": {
        "description": "Method for measurement of temperatures",
        "type": "string",
    },
    "temporal_agg": {
        "type": "string",
        "description": "Category of temporal aggregation in EIA bulk electricity data.",
    },
    "time_cold_shutdown_full_load_code": {
        "type": "string",
        "description": "The minimum amount of time required to bring the unit to full load from shutdown.",
    },
    "time_of_use_pricing": {
        "type": "boolean",
        "description": (
            "Whether the respondent has customers participating in a time-of-use "
            "pricing programs (TOU). TOU is a program in which customers pay different "
            "prices at different times of the day. On-peak prices are higher and "
            "off-peak prices are lower than a “standard” rate. Price schedule is fixed "
            "and predefined, based on season, day of week, and time of day."
        ),
    },
    "time_to_switch_gas_to_oil": {
        "type": "string",
        "description": "The time required to switch the generator from running 100 percent natural gas to running 100 percent oil.",
    },
    "time_to_switch_oil_to_gas": {
        "type": "string",
        "description": "The time required to switch the generator from running 100 percent oil to running 100 percent natural gas.",
    },
    "has_time_responsive_programs": {
        "type": "boolean",
        "description": (
            "Whether the respondent operates any time-based rate programs (e.g., "
            "real-time pricing, critical peak pricing, variable peak pricing and "
            "time-of-use rates administered through a tariff)."
        ),
    },
    "time_responsiveness_customers": {
        "type": "integer",
        "description": (
            "The number of cusomters participating in the respondent's time-based "
            "rate programs."
        ),
    },
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
    "total_capacity_less_1_mw": {
        "type": "number",
        "description": (
            "The total amount of capacity from generators with less than 1 MW of "
            "nameplate capacity."
        ),
        "unit": "MW",
    },
    "total_disposition_mwh": {
        "type": "number",
        "description": (
            "Sum of all disposition of electricity listed. "
            "Includes sales to ultimate customers, sales for resale, energy furnished "
            "without charge, energy consumed by respondent without charge and total "
            "energy losses."
        ),
        "unit": "MWh",
    },
    "total_energy_losses_mwh": {
        "type": "number",
        "description": (
            "The total amount of electricity lost from transmission, distribution, "
            "and/or unaccounted for. Should be expressed as a positive number."
        ),
        "unit": "MWh",
    },
    "total_fgd_equipment_cost": {
        "type": "number",
        "description": (
            "Total actual installed costs for the existing flue gas desulfurization "
            "unit or the anticipated costs to bring a planned flue gas desulfurization "
            "unit into commercial operation."
        ),
    },
    "total_fuel_cost": {
        "type": "number",
        "description": "Total annual reported fuel costs for the plant part. Includes costs from all fuels.",
    },
    "total_fuel_cost_eia": {
        "type": "number",
        "description": "Total annual reported fuel costs for the plant part. Includes costs from all fuels.",
    },
    "total_fuel_cost_ferc1": {
        "type": "number",
        "description": "Total annual reported fuel costs for the plant part. Includes costs from all fuels.",
    },
    "total_mmbtu": {
        "type": "number",
        "description": "Total annual heat content of fuel consumed by a plant part record in the plant parts list.",
    },
    "total_mmbtu_eia": {
        "type": "number",
        "description": "Total annual heat content of fuel consumed by a plant part record in the plant parts list.",
    },
    "total_mmbtu_ferc1": {
        "type": "number",
        "description": "Total annual heat content of fuel consumed by a plant part record in the plant parts list.",
    },
    "total_settlement": {
        "type": "number",
        "description": "Sum of demand, energy, and other charges (USD). For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative.",
        "unit": "USD",
    },
    "total_sources_mwh": {
        "type": "number",
        "description": (
            "Sum of all sources of electricity listed. Includes net generation, "
            "purchases from electricity suppliers, net exchanges (received - "
            "delivered), net wheeled (received - delivered), transmission by others, "
            "and losses."
        ),
        "unit": "MWh",
    },
    "tower_cost": {
        "description": (
            "Actual installed cost for the existing cooling towers or the "
            "anticipated cost to bring the cooling towers into commercial "
            "operation"
        ),
        "type": "number",
        "unit": "USD",
    },
    "tower_operating_date": {
        "description": "Cooling towers actual or projected in-service date",
        "type": "date",
    },
    "tower_type_1": {
        "description": "Types of cooling towers at this plant",
        "type": "string",
    },
    "tower_type_2": {
        "description": "Types of cooling towers at this plant",
        "type": "string",
    },
    "tower_type_3": {
        "description": "Types of cooling towers at this plant",
        "type": "string",
    },
    "tower_type_4": {
        "description": "Types of cooling towers at this plant",
        "type": "string",
    },
    "tower_water_rate_100pct_gallons_per_minute": {
        "description": "Maximum design rate of water flow at 100 percent load for the cooling towers",
        "type": "number",
        "unit": "gpm",
    },
    "transmission_activity": {
        "type": "boolean",
        "description": (
            "Whether a utility engaged in any transmission activities during the year."
        ),
    },
    "transmission_by_other_losses_mwh": {
        "type": "number",
        "description": (
            "The amount of energy losses associated with the wheeling of electricity "
            "provided to the respondent's system by other utilities. Transmission by "
            "others, losses should always be a negative value."
        ),
        "unit": "MWh",
    },
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
    "transmission_line_and_structures_length_miles": {
        "type": "number",
        "description": "Length (in pole miles or circuit miles (if transmission lines are underground)) for lines that are agrregated with other lines / structures (whose cost are aggregated and combined with other structures).",
    },
    "transmission_line_length_miles": {
        "type": "number",
        "description": "Length (in pole miles or circuit miles (if transmission lines are underground)) for lines that are stand alone structures (whose cost are reported on a stand-alone basis).",
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
    "turndown_ratio": {
        "type": "number",
        "description": "The turndown ratio for the boiler.",
    },
    "ultrasupercritical_tech": {
        "type": "boolean",
        "description": "Indicates whether the generator uses ultra-supercritical technology",
    },
    "unit_id_eia": {
        "type": "string",
        "description": "EIA-assigned unit identification code.",
    },
    "unit_id_pudl": {
        "type": "integer",
        "description": "Dynamically assigned PUDL unit id. WARNING: This ID is not guaranteed to be static long term as the input data and algorithm may evolve over time.",
    },
    "unit_id_pudl_plant_gen": {
        "type": "integer",
        "description": "Dynamically assigned PUDL unit id of the record_id_eia_plant_gen. WARNING: This ID is not guaranteed to be static long term as the input data and algorithm may evolve over time.",
    },
    "unit_nox": {
        "type": "string",
        "description": "Numeric value for the unit of measurement specified for nitrogen oxide.",
    },
    "unit_particulate": {
        "type": "string",
        "description": "Numeric value for the unit of measurement specified for particulate matter.",
    },
    "unit_so2": {
        "type": "string",
        "description": "Numeric value for the unit of measurement specified for sulfur dioxide.",
    },
    "uprate_derate_completed_date": {
        "type": "date",
        "description": "The date when the uprate or derate was completed.",
    },
    "uprate_derate_during_year": {
        "type": "boolean",
        "description": "Was an uprate or derate completed on this generator during the reporting year?",
    },
    "utility_id_eia": {
        "type": "integer",
        "description": "The EIA Utility Identification number.",
        # TODO: Disambiguate column name. In some cases this specifically refers to
        # the utility which operates a given plant or generator, but comes from the
        # same set of IDs as all the utility IDs.
        # E.g. in core_eia860__scd_ownership or core_eia860__scd_generators it would be something like:
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
    "utility_owned_capacity_mw": {
        "type": "number",
        "description": "Total non-net-metered capacity owned by the respondent.",
        "unit": "MW",
        # TODO: Make this column name more specific to non-net metered capacity.
    },
    "utility_plant_asset_type": {
        "type": "string",
        "description": "Type of utility plant asset reported in the core_ferc1__yearly_utility_plant_summary_sched200 table. Assets include those leased to others, held for future use, construction work-in-progress and details of accumulated depreciation.",
        "constraints": {
            "enum": UTILITY_PLANT_ASSET_TYPES_FERC1.extend(
                # Add all possible correction records to enum
                [
                    "abandonment_of_leases_correction",
                    "abandonment_of_leases_subdimension_correction",
                    "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility_correction",
                    "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility_subdimension_correction",
                    "amortization_and_depletion_of_producing_natural_gas_land_and_land_rights_utility_plant_in_service_correction",
                    "amortization_and_depletion_of_producing_natural_gas_land_and_land_rights_utility_plant_in_service_subdimension_correction",
                    "amortization_and_depletion_utility_plant_leased_to_others_correction",
                    "amortization_and_depletion_utility_plant_leased_to_others_subdimension_correction",
                    "amortization_of_other_utility_plant_utility_plant_in_service_correction",
                    "amortization_of_other_utility_plant_utility_plant_in_service_subdimension_correction",
                    "amortization_of_plant_acquisition_adjustment_correction",
                    "amortization_of_plant_acquisition_adjustment_subdimension_correction",
                    "amortization_of_underground_storage_land_and_land_rights_utility_plant_in_service_correction",
                    "amortization_of_underground_storage_land_and_land_rights_utility_plant_in_service_subdimension_correction",
                    "amortization_utility_plant_held_for_future_use_correction",
                    "amortization_utility_plant_held_for_future_use_subdimension_correction",
                    "construction_work_in_progress_correction",
                    "construction_work_in_progress_subdimension_correction",
                    "depreciation_amortization_and_depletion_utility_plant_in_service_correction",
                    "depreciation_amortization_and_depletion_utility_plant_in_service_subdimension_correction",
                    "depreciation_amortization_and_depletion_utility_plant_leased_to_others_correction",
                    "depreciation_amortization_and_depletion_utility_plant_leased_to_others_subdimension_correction",
                    "depreciation_and_amortization_utility_plant_held_for_future_use_correction",
                    "depreciation_and_amortization_utility_plant_held_for_future_use_subdimension_correction",
                    "depreciation_utility_plant_held_for_future_use_correction",
                    "depreciation_utility_plant_held_for_future_use_subdimension_correction",
                    "depreciation_utility_plant_in_service_correction",
                    "depreciation_utility_plant_in_service_subdimension_correction",
                    "depreciation_utility_plant_leased_to_others_correction",
                    "depreciation_utility_plant_leased_to_others_subdimension_correction",
                    "utility_plant_acquisition_adjustment_correction",
                    "utility_plant_acquisition_adjustment_subdimension_correction",
                    "utility_plant_and_construction_work_in_progress_correction",
                    "utility_plant_and_construction_work_in_progress_subdimension_correction",
                    "utility_plant_held_for_future_use_correction",
                    "utility_plant_held_for_future_use_subdimension_correction",
                    "utility_plant_in_service_classified_and_property_under_capital_leases_correction",
                    "utility_plant_in_service_classified_and_property_under_capital_leases_subdimension_correction",
                    "utility_plant_in_service_classified_and_unclassified_correction",
                    "utility_plant_in_service_classified_and_unclassified_subdimension_correction",
                    "utility_plant_in_service_classified_correction",
                    "utility_plant_in_service_classified_subdimension_correction",
                    "utility_plant_in_service_completed_construction_not_classified_correction",
                    "utility_plant_in_service_completed_construction_not_classified_subdimension_correction",
                    "utility_plant_in_service_experimental_plant_unclassified_correction",
                    "utility_plant_in_service_experimental_plant_unclassified_subdimension_correction",
                    "utility_plant_in_service_plant_purchased_or_sold_correction",
                    "utility_plant_in_service_plant_purchased_or_sold_subdimension_correction",
                    "utility_plant_in_service_property_under_capital_leases_correction",
                    "utility_plant_in_service_property_under_capital_leases_subdimension_correction",
                    "utility_plant_leased_to_others_correction",
                    "utility_plant_leased_to_others_subdimension_correction",
                    "utility_plant_net_correction",
                    "utility_plant_net_subdimension_correction",
                ]
            )
        },
    },
    "utility_type": {
        "type": "string",
        "description": "Listing of utility plant types. Examples include Electric Utility, Gas Utility, and Other Utility.",
    },
    "utility_type_other": {
        "type": "string",
        "description": "Freeform description of type of utility reported in one of the other three other utility_type sections in the core_ferc1__yearly_utility_plant_summary_sched200 table. This field is reported only in the DBF reporting years (1994-2020).",
    },
    "valid_until_date": {
        "type": "date",
        "description": "The record in the changelog is valid until this date. The record is valid from the report_date up until but not including the valid_until_date.",
    },
    "variable_peak_pricing": {
        "type": "boolean",
        "description": (
            "Whether the respondent has customers participating in a variable peak "
            "pricing program (VPP). VPP is a program in which a form of TOU pricing "
            "allows customers to purchase their generation supply at prices set on a "
            "daily basis with varying on-peak and constant off-peak rates. Under the "
            "VPP program, the on-peak price for each weekday becomes available the "
            "previous day (typically late afternoon) and the customer is billed for "
            "actual consumption during the billing cycle at these prices."
        ),
    },
    "waste_fraction_cost": {
        "type": "number",
        "description": "Waste-heat cost as a percentage of overall fuel cost.",
    },
    "waste_fraction_mmbtu": {
        "type": "number",
        "description": "Waste-heat heat content as a percentage of overall fuel heat content (MMBtu).",
    },
    "waste_heat_input_mmbtu_per_hour": {
        "type": "number",
        "unit": "MMBtu_per_hour",
        "description": "Design waste-heat input rate at maximum continuous steam flow where a waste-heat boiler is a boiler that receives all or a substantial portion of its energy input from the noncumbustible exhaust gases of a separate fuel-burning process (MMBtu per hour).",
    },
    "num_water_heaters": {
        "type": "integer",
        "description": (
            "The number of grid-enabled water heaters added to the respondent's "
            "program this year - if the respondent has DSM program for grid-enabled "
            "water heaters (as defined by DOE’s Office of Energy Efficiency and "
            "Renewable Energy)."
        ),
    },
    "water_limited_capacity_mw": {
        "type": "number",
        "description": "Plant capacity in MW when limited by condenser water.",
        "unit": "MW",
    },
    "water_source": {
        "type": "string",
        "description": "Name of water source associated with the plant.",
    },
    "water_source_code": {
        "description": "Type of cooling water source",
        "type": "string",
    },
    "water_type_code": {
        "description": "Type of cooling water",
        "type": "string",
    },
    "weighted_average_life_years": {
        "type": "number",
        "description": (
            "The weighted average life of the respondent's portfolio of energy "
            "efficiency programs."
        ),
    },
    "wet_dry_bottom": {
        "type": "string",
        "unit": "MMBtu_per_hour",
        "description": "Wet or Dry Bottom where Wet Bottom is defined as slag tanks that are installed at furnace throat to contain and remove molten ash from the furnace, and Dry Bottom is defined as having no slag tanks at furnace throat area, throat area is clear, and bottom ash drops through throat to bottom ash water hoppers.",
    },
    "wheeled_power_delivered_mwh": {
        "type": "number",
        "description": (
            "The total amount of energy leaving the respondent's system that was "
            "transmitted through the respondent's system for delivery to other "
            "systems. If wheeling delivered is not precisely known, the value is an "
            "estimate based on the respondent's system's known percentage of losses "
            "for wheeling transactions."
        ),
        "unit": "MWh",
    },
    "wheeled_power_received_mwh": {
        "type": "number",
        "description": (
            "The total amount of energy entering the respondent's system from other "
            "systems for transmission through the respondent's system (wheeling) for "
            "delivery to other systems. Does not include energy purchased or exchanged "
            "for consumption within the respondent's system, which was wheeled to "
            "the respondent by others."
        ),
        "unit": "MWh",
    },
    "wholesale_marketing_activity": {
        "type": "boolean",
        "description": "Whether a utility engages in wholesale power marketing during the year.",
    },
    "wholesale_power_purchases_mwh": {
        "type": "number",
        "description": "Purchases from electricity suppliers.",
        "unit": "MWh",
    },
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
    "winter_peak_demand_forecast_mw": {
        "type": "number",
        "description": (
            "The maximum forecasted hourly winter load (for the months of January "
            "through March)."
        ),
        "unit": "MW",
    },
    "winter_peak_demand_mw": {
        "type": "number",
        "description": (
            "The maximum hourly winter load (for the months of January through March) "
            "based on net energy for the system during the reporting year. Net energy "
            "for the system is the sum of energy an electric utility needs to satisfy "
            "their service area and includes full and partial wholesale requirements "
            "customers, and the losses experienced in delivery. The maximum hourly "
            "load is determined by the interval in which the 60-minute integrated "
            "demand is the greatest."
        ),
        "unit": "MW",
    },
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
    "design_wind_speed_mph": {
        "type": "number",
        "description": "Average annual wind speed that turbines at this wind site were designed for.",
    },
    "obstacle_id_faa": {
        "type": "string",
        "description": (
            "The Federal Aviation Administration (FAA) obstacle number assigned to this "
            "generator. If more than one obstacle number exists, the one that best "
            "represents the turbines. References the obstacle numbers reported in the "
            "FAA's Digital Obstacle File: "
            "https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dof/ "
            "This field was only reported from 2013 through 2015."
        ),
    },
    "predominant_turbine_manufacturer": {
        "type": "string",
        "description": "Name of predominant manufacturer of turbines at this generator.",
    },
    "predominant_turbine_model": {
        "type": "string",
        "description": "Predominant model number of turbines at this generator.",
    },
    "turbine_hub_height_feet": {
        "type": "number",
        "description": "The hub height of turbines at this generator. If more than one value exists, the one that best represents the turbines.",
        "unit": "ft",
    },
    "wind_quality_class": {
        "type": "integer",
        "description": "The wind quality class for turbines at this generator. See table core_eia__codes_wind_quality_class for specifications about each class.",
        "constraints": {"enum": [1, 2, 3, 4]},
    },
    "wind_speed_avg_ms": {
        "type": "number",
        "description": "Average wind speed in meters per second.",
        "unit": "ms",
    },
    "extreme_fifty_year_gust_ms": {
        "type": "number",
        "description": "The extreme 50-year wind gusts at this generator in meters per hour.",
        "unit": "ms",
    },
    "turbulence_intensity_a": {
        "type": "number",
        "description": "The upper bounds of the turbulence intensity at the wind site (ratio of standard deviation of fluctuating wind velocity to the mean wind speed).",
    },
    "turbulence_intensity_b": {
        "type": "number",
        "description": "The lower bounds of the turbulence intensity at the wind site (ratio of standard deviation of fluctuating wind velocity to the mean wind speed).",
    },
    "azimuth_angle_deg": {
        "type": "number",
        "description": "Indicates the azimuth angle of the unit for fixed tilt or single-axis technologies.",
        "unit": "deg",
    },
    "standard_testing_conditions_capacity_mwdc": {
        "type": "number",
        "description": "The net capacity of this photovoltaic generator in direct current under standard test conditions (STC) of 1000 W/m^2 solar irradiance and 25 degrees Celsius PV module temperature. This was only reported in 2013 and 2014.",
        "unit": "MW",
    },
    "net_metering_capacity_mwdc": {
        "type": "number",
        "description": "The DC megawatt capacity that is part of a net metering agreement.",
        "unit": "MW",
    },
    "tilt_angle_deg": {
        "type": "number",
        "description": "Indicates the tilt angle of the unit for fixed tilt or single-axis technologies.",
        "unit": "deg",
    },
    "uses_material_crystalline_silicon": {
        "type": "boolean",
        "description": "Indicates whether any solar photovoltaic panels at this generator are made of crystalline silicon.",
    },
    "uses_technology_dish_engine": {
        "type": "boolean",
        "description": "Indicates whether dish engines are used at this solar generating unit.",
    },
    "uses_technology_dual_axis_tracking": {
        "type": "boolean",
        "description": "Indicates whether dual-axis tracking technologies are used at this solar generating unit.",
    },
    "uses_technology_east_west_fixed_tilt": {
        "type": "boolean",
        "description": "Indicates whether east west fixed tilt technologies are used at this solar generating unit.",
    },
    "uses_technology_fixed_tilt": {
        "type": "boolean",
        "description": "Indicates whether fixed tilt technologies are used at this solar generating unit.",
    },
    "uses_technology_lenses_mirrors": {
        "type": "boolean",
        "description": "Indicates whether lenses or mirrors are used at this solar generating unit.",
    },
    "uses_technology_linear_fresnel": {
        "type": "boolean",
        "description": "Indicates whether linear fresnel technologies are used at this solar generating unit.",
    },
    "uses_net_metering_agreement": {
        "type": "boolean",
        "description": "Indicates if the output from this generator is part of a net metering agreement.",
    },
    "uses_material_other": {
        "type": "boolean",
        "description": "Indicates whether any solar photovoltaic panels at this generator are made of other materials.",
    },
    "uses_technology_other": {
        "type": "boolean",
        "description": "Indicates whether other solar technologies are used at this solar generating unit.",
    },
    "uses_technology_parabolic_trough": {
        "type": "boolean",
        "description": "Indicates whether parabolic trough technologies s are used at this solar generating unit.",
    },
    "uses_technology_power_tower": {
        "type": "boolean",
        "description": "Indicates whether power towers are used at this solar generating unit.",
    },
    "uses_technology_single_axis_tracking": {
        "type": "boolean",
        "description": "Indicates whether single-axis tracking technologies are used at this solar generating unit.",
    },
    "uses_material_thin_film_a_si": {
        "type": "boolean",
        "description": "Indicates whether any solar photovoltaic panels at this generator are made of thin-film amorphous silicon (A-Si).",
    },
    "uses_material_thin_film_cdte": {
        "type": "boolean",
        "description": "Indicates whether any solar photovoltaic panels at this generator are made of thin-film cadmium telluride (CdTe).",
    },
    "uses_material_thin_film_cigs": {
        "type": "boolean",
        "description": "Indicates whether any solar photovoltaic panels at this generator are made of thin-film copper indium gallium diselenide (CIGS).",
    },
    "uses_material_thin_film_other": {
        "type": "boolean",
        "description": "Indicates whether any solar photovoltaic panels at this generator are made of other thin-film material.",
    },
    "uses_virtual_net_metering_agreement": {
        "type": "boolean",
        "description": "Indicates if the output from this generator is part of a virtual net metering agreement.",
    },
    "virtual_net_metering_capacity_mwdc": {
        "type": "number",
        "description": "The DC capacity in MW that is part of a virtual net metering agreement.",
        "unit": "MW",
    },
    "model_case_nrelatb": {
        "type": "string",
        "description": (
            "NREL's financial assumption cases. There are two cases which effect project financial "
            "assumptions: R&D Only Case and Market + Policies Case. R&D Only includes only projected "
            "R&D improvements while Market + Policy case includes policy and tax incentives. "
            "https://atb.nrel.gov/electricity/2024/financial_cases_&_methods"
        ),
        "constraints": {"enum": ["Market", "R&D"]},
    },
    "model_tax_credit_case_nrelatb": {
        "type": "string",
        "description": (
            "NREL's tax credit assumption cases. There are two types of tax credits: "
            "production tax credit (PTC) and investment tax credit (ITC). For more detail, see: "
            "https://atb.nrel.gov/electricity/2024/financial_cases_&_methods"
        ),
        "constraints": {"enum": ["Market", "R&D"]},
    },
    "projection_year": {
        "type": "integer",
        "description": "The year of the projected value.",
    },
    "cost_recovery_period_years": {
        "type": "integer",
        "description": "The period over which the initial capital investment to build a plant is recovered.",
    },
    "scenario_atb": {
        "type": "string",
        "description": "Technology innovation scenarios. https://atb.nrel.gov/electricity/2023/definitions#scenarios",
        "constraints": {"enum": ["Advanced", "Moderate", "Conservative"]},
    },
    "is_default": {
        "type": "boolean",
        "description": "Indicator of whether the technology is default.",
    },
    "is_technology_mature": {
        "type": "boolean",
        "description": (
            "Indicator of whether the technology is mature. Technologies are defined"
            "as mature if a representative plant is operating or under construction"
            "in the United States in the Base Year."
        ),
    },
    "inflation_rate": {
        "type": "number",
        "description": (
            "Rate of inflation. All dollar values are given in 2021 USD, using the Consumer Price "
            "Index for All Urban Consumers for dollar year conversions where the source "
            "year dollars do not match 2021."
        ),
    },
    "interest_rate_during_construction_nominal": {
        "type": "number",
        "description": (
            "Also referred to as construction finance cost. Portion of all-in capital cost "
            "associated with construction period financing. It is a function of construction "
            "duration, capital fraction during construction, and interest during construction."
        ),
    },
    "interest_rate_calculated_real": {
        "type": "number",
        "description": "Calculated real interest rate.",
    },
    "interest_rate_nominal": {
        "type": "number",
        "description": "Nominal interest rate.",
    },
    "rate_of_return_on_equity_calculated_real": {
        "type": "number",
        "description": "Calculated real rate of return on equity.",
    },
    "rate_of_return_on_equity_nominal": {
        "type": "number",
        "description": "Nomial rate of return on equity.",
    },
    "tax_rate_federal_state": {
        "type": "number",
        "description": (
            "Combined federal and state tax rate. The R&D model_case_nrelatb holds tax and "
            "inflation rates constant at assumed long-term values: 21 percent federal tax rate, "
            "6 percent state tax rate (though actual state tax rates vary), and 2.5 "
            "percent inflation rate excludes effects of tax credits. The Market + Policy "
            "model_case_nrelatb applies federal tax credits and expires them as consistent with "
            "existing law and guidelines."
        ),
    },
    "capital_recovery_factor": {
        "type": "number",
        "description": "Ratio of a constant annuity to the present value of receiving that annuity for a given length of time.",
    },
    "debt_fraction": {
        "type": "number",
        "description": (
            "Fraction of capital financed with debt; Debt fraction is assumed financed with equity; "
            "also referred to as the leverage ratio."
        ),
    },
    "fixed_charge_rate": {
        "type": "number",
        "description": (
            "Amount of revenue per dollar of investment required that must be collected annually "
            "from customers to pay the carrying charges on that investment."
        ),
    },
    "wacc_nominal": {
        "type": "number",
        "description": "Nominal weighted average cost of capital - average expected rate that is paid to finance assets.",
    },
    "wacc_real": {
        "type": "number",
        "description": "Real weighted average cost of capital - average expected rate that is paid to finance assets.",
    },
    "table_name": {
        "type": "string",
        "description": "The name of the PUDL database table where a given record originated from.",
    },
    "xbrl_factoid": {
        "type": "string",  # TODO: this is bad rn... make better
        "description": "The name of type of value which is a derivative of the XBRL fact name.",
    },
    "rate_base_category": {
        "type": "string",
        "description": (
            "A category of asset or liability that RMI compiled to use "
            "as a shorthand for various types of utility assets. "
            "These tags were compiled manually based on the xbrl_factoid and sometimes varies "
            "based on the utility_type, plant_function or plant_status as well."
        ),
        "constraints": {
            "enum": [
                "other_plant",
                "nuclear",
                "transmission",
                "net_nuclear_fuel",
                "distribution",
                "steam",
                "experimental_plant",
                "net_working_capital",
                "general_plant",
                "regional_transmission_and_market_operation",
                "other_production",
                "other_noncurrent_liabilities",
                "hydro",
                "net_utility_plant",
                "intangible_plant",
                "other_deferred_debits_and_credits",
                "net_regulatory_assets",
                "net_ADIT",
                "asset_retirement_costs",
                "utility_plant",
                "electric_plant_leased_to_others",
                "electric_plant_held_for_future_use",
                "non_utility_plant",
                "construction_work_in_progress",
                "AROs",
                "correction",
            ]
        },
    },
    "is_disaggregated_utility_type": {
        "type": "boolean",
        "description": (
            "Indicates whether or not records with null or total values in the "
            "utility_type column were disaggregated. See documentation for process: "
            "pudl.output.ferc1.disaggregate_null_or_total_tag"
        ),
    },
    "is_disaggregated_in_rate_base": {
        "type": "boolean",
        "description": (
            "Indicates whether or not records with null values in the in_rate_base column were "
            "disaggregated. See documentation for process: pudl.output.ferc1.disaggregate_null_or_total_tag"
        ),
    },
    "is_ac_coupled": {
        "type": "boolean",
        "description": (
            "Indicates if this energy storage device is AC-coupled (means the energy storage device "
            "and the PV system are not installed on the same side of an inverter)."
        ),
    },
    "is_dc_coupled": {
        "type": "boolean",
        "description": (
            "Indicates if this energy storage device is DC-coupled (means the energy storage "
            "device and the PV system are on the same side of an inverter and the battery can "
            "still charge from the grid)."
        ),
    },
    "id_dc_coupled_tightly": {
        "type": "boolean",
        "description": (
            "Indicates if this energy storage device is DC tightly coupled (means the energy "
            "storage device and the PV system are on the same side of an inverter and the battery "
            "cannot charge from the grid)."
        ),
    },
    "is_independent": {
        "type": "boolean",
        "description": "Indicates if this energy storage device is independent (not coupled with another generators)",
    },
    "is_direct_support": {
        "type": "boolean",
        "description": (
            "Indicates if this energy storage device is intended for dedicated generator "
            "firming or storing excess generation of other units."
        ),
    },
    "plant_id_eia_direct_support_1": {
        "type": "integer",
        "description": (
            "The EIA Plant ID of the primary unit whose generation this energy storage "
            "device is intended to firm or store."
        ),
    },
    "generator_id_direct_support_1": {
        "type": "string",
        "description": (
            "The EIA Generator ID of the primary unit whose generation this energy "
            "storage device is intended to firm or store."
        ),
    },
    "plant_id_eia_direct_support_2": {
        "type": "integer",
        "description": (
            "The EIA Plant ID of the secondary unit whose generation this energy storage "
            "device is intended to firm or store."
        ),
    },
    "generator_id_direct_support_2": {
        "type": "string",
        "description": (
            "The EIA Generator ID of the secondary unit whose generation this energy "
            "storage device is intended to firm or store."
        ),
    },
    "plant_id_eia_direct_support_3": {
        "type": "integer",
        "description": (
            "The EIA Plant ID of the tertiary unit whose generation this energy storage "
            "device is intended to firm or store."
        ),
    },
    "generator_id_direct_support_3": {
        "type": "string",
        "description": (
            "The EIA Generator ID of the tertiary unit whose generation this energy "
            "storage device is intended to firm or store."
        ),
    },
    "is_transmission_and_distribution_asset_support": {
        "type": "boolean",
        "description": (
            "Indicate if the energy storage system is intended to support a specific substation, "
            "transmission or distribution asset."
        ),
    },
    "uses_bifacial_panels": {
        "type": "boolean",
        "description": (
            "Indicates whether bifacial solar panels are used at this solar generating unit."
        ),
    },
    "business_city": {
        "type": "string",
        "description": "City where the company's place of business is located.",
    },
    "business_state": {
        "type": "string",
        "description": "State where the company's place of business is located.",
    },
    "business_street_address": {
        "type": "string",
        "description": "Street address of the company's place of business.",
    },
    "business_street_address_2": {
        "type": "string",
        "description": "Second line of the street address of the company's place of business.",
    },
    "business_zip_code": {
        "type": "string",
        "description": "Zip code of the company's place of business.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "business_zip_code_4": {
        "type": "string",
        "description": "Zip code suffix of the company's place of business.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "business_postal_code": {
        "type": "string",
        "description": "Non-US postal code of the company's place of business.",
    },
    "filer_count": {
        "type": "integer",
        "description": "A counter indicating which observation of company data within an SEC 10-K filing header the record pertains to.",
    },
    "mail_street_address": {
        "type": "string",
        "description": "Street portion of the company's mailing address.",
    },
    "mail_street_address_2": {
        "type": "string",
        "description": "Second line of the street portion of the company's mailing address.",
    },
    "mail_city": {
        "type": "string",
        "description": "City of the company's mailing address.",
    },
    "mail_state": {
        "type": "string",
        "description": "State of the company's mailing address.",
    },
    "mail_zip_code": {
        "type": "string",
        "description": "Zip code of the company's mailing address.",
        "constraints": {
            "pattern": r"^\d{5}$",
        },
    },
    "mail_zip_code_4": {
        "type": "string",
        "description": "Zip code suffix of the company's mailing address.",
        "constraints": {
            "pattern": r"^\d{4}$",
        },
    },
    "mail_postal_code": {
        "type": "string",
        "description": "Non-US postal code of the company's mailing address.",
    },
}
"""Field attributes by PUDL identifier (`field.name`)."""

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
    "eia": {
        "fuel_units": {"constraints": {"enum": sorted(FUEL_UNITS_EIA.keys())}},
    },
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
    "nrelatb": {
        "technology_description": {"constraints": {"enum": TECH_DESCRIPTIONS_NRELATB}}
    },
    "sec10k": {
        "fraction_owned": {
            "type": "number",
            "description": "Fraction of a subsidiary company owned by the parent.",
        },
    },
    "vcerare": {
        "latitude": {
            "description": "Latitude of the place centroid (e.g., county centroid)."
        },
        "longitude": {
            "description": "Longitude of the place centroid (e.g., county centroid)."
        },
    },
}
"""Field attributes by resource group (`resource.group`) and PUDL identifier.

If a field exists in more than one data group (e.g. both ``eia`` and ``ferc1``) and has
distinct metadata in those groups, this is the place to specify the override. Only those
elements which should be overridden need to be specified.
"""

FIELD_METADATA_BY_RESOURCE: dict[str, dict[str, Any]] = {
    "sector_consolidated_eia": {"code": {"type": "integer"}},
    "core_ferc1__yearly_hydroelectric_plants_sched406": {
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
    "core_ferc1__yearly_steam_plants_sched402": {
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
    "core_ferc1__yearly_plant_in_service_sched204": {
        "ferc_account_label": {
            "type": "string",
            "constraints": {
                "enum": {
                    "accessory_electric_equipment_hydraulic_production",
                    "accessory_electric_equipment_nuclear_production",
                    "accessory_electric_equipment_other_production",
                    "accessory_electric_equipment_steam_production",
                    "asset_retirement_costs_for_distribution_plant_distribution_plant",
                    "asset_retirement_costs_for_general_plant_general_plant",
                    "asset_retirement_costs_for_hydraulic_production_plant_hydraulic_production",
                    "asset_retirement_costs_for_nuclear_production_plant_nuclear_production",
                    "asset_retirement_costs_for_other_production_plant_other_production",
                    "asset_retirement_costs_for_regional_transmission_and_market_operation_plant_regional_transmission_and_market_operation_plant",
                    "asset_retirement_costs_for_steam_production_plant_steam_production",
                    "asset_retirement_costs_for_transmission_plant_transmission_plant",
                    "boiler_plant_equipment_steam_production",
                    "communication_equipment_general_plant",
                    "communication_equipment_regional_transmission_and_market_operation_plant",
                    "computer_hardware_regional_transmission_and_market_operation_plant",
                    "computer_software_regional_transmission_and_market_operation_plant",
                    "distribution_plant",
                    "electric_plant_in_service",
                    "electric_plant_in_service_and_completed_construction_not_classified_electric",
                    "electric_plant_purchased",
                    "electric_plant_sold",
                    "energy_storage_equipment_distribution_plant",
                    "energy_storage_equipment_other_production",
                    "energy_storage_equipment_transmission_plant",
                    "engines_and_engine_driven_generators_steam_production",
                    "experimental_electric_plant_unclassified",
                    "franchises_and_consents",
                    "fuel_holders_products_and_accessories_other_production",
                    "general_plant",
                    "general_plant_excluding_other_tangible_property_and_asset_retirement_costs_for_general_plant",
                    "generators_other_production",
                    "hydraulic_production_plant",
                    "installations_on_customer_premises_distribution_plant",
                    "intangible_plant",
                    "laboratory_equipment_general_plant",
                    "land_and_land_rights_distribution_plant",
                    "land_and_land_rights_general_plant",
                    "land_and_land_rights_hydraulic_production",
                    "land_and_land_rights_nuclear_production",
                    "land_and_land_rights_other_production",
                    "land_and_land_rights_regional_transmission_and_market_operation_plant",
                    "land_and_land_rights_steam_production",
                    "land_and_land_rights_transmission_plant",
                    "leased_property_on_customer_premises_distribution_plant",
                    "line_transformers_distribution_plant",
                    "meters_distribution_plant",
                    "miscellaneous_equipment_general_plant",
                    "miscellaneous_intangible_plant",
                    "miscellaneous_power_plant_equipment_hydraulic_production",
                    "miscellaneous_power_plant_equipment_nuclear_production",
                    "miscellaneous_power_plant_equipment_other_production",
                    "miscellaneous_power_plant_equipment_steam_production",
                    "miscellaneous_regional_transmission_and_market_operation_plant_regional_transmission_and_market_operation_plant",
                    "nuclear_production_plant",
                    "office_furniture_and_equipment_general_plant",
                    "organization",
                    "other_production_plant",
                    "other_tangible_property_general_plant",
                    "overhead_conductors_and_devices_distribution_plant",
                    "overhead_conductors_and_devices_transmission_plant",
                    "poles_and_fixtures_transmission_plant",
                    "poles_towers_and_fixtures_distribution_plant",
                    "power_operated_equipment_general_plant",
                    "prime_movers_other_production",
                    "production_plant",
                    "reactor_plant_equipment_nuclear_production",
                    "reservoirs_dams_and_waterways_hydraulic_production",
                    "roads_and_trails_transmission_plant",
                    "roads_railroads_and_bridges_hydraulic_production",
                    "services_distribution_plant",
                    "station_equipment_distribution_plant",
                    "station_equipment_transmission_plant",
                    "steam_production_plant",
                    "stores_equipment_general_plant",
                    "street_lighting_and_signal_systems_distribution_plant",
                    "structures_and_improvements_distribution_plant",
                    "structures_and_improvements_general_plant",
                    "structures_and_improvements_hydraulic_production",
                    "structures_and_improvements_nuclear_production",
                    "structures_and_improvements_other_production",
                    "structures_and_improvements_regional_transmission_and_market_operation_plant",
                    "structures_and_improvements_steam_production",
                    "structures_and_improvements_transmission_plant",
                    "tools_shop_and_garage_equipment_general_plant",
                    "towers_and_fixtures_transmission_plant",
                    "transmission_and_market_operation_plant_regional_transmission_and_market_operation_plant",
                    "transmission_plant",
                    "transportation_equipment_general_plant",
                    "turbogenerator_units_nuclear_production",
                    "turbogenerator_units_steam_production",
                    "underground_conductors_and_devices_distribution_plant",
                    "underground_conductors_and_devices_transmission_plant",
                    "underground_conduit_distribution_plant",
                    "underground_conduit_transmission_plant",
                    "water_wheels_turbines_and_generators_hydraulic_production",
                    "distribution_plant_correction",
                    "electric_plant_in_service_and_completed_construction_not_classified_electric_correction",
                    "electric_plant_in_service_correction",
                    "general_plant_correction",
                    "general_plant_excluding_other_tangible_property_and_asset_retirement_costs_for_general_plant_correction",
                    "hydraulic_production_plant_correction",
                    "intangible_plant_correction",
                    "nuclear_production_plant_correction",
                    "other_production_plant_correction",
                    "production_plant_correction",
                    "steam_production_plant_correction",
                    "transmission_and_market_operation_plant_regional_transmission_and_market_operation_plant_correction",
                    "transmission_plant_correction",
                }
            },
        }
    },
    "core_ferc1__yearly_depreciation_summary_sched336": {
        "ferc_account_label": {
            "type": "string",
            "constraints": {
                "enum": {
                    "amortization_limited_term_electric_plant",
                    "amortization_limited_term_electric_plant_correction",
                    "amortization_limited_term_electric_plant_subdimension_correction",
                    "amortization_other_electric_plant",
                    "amortization_other_electric_plant_correction",
                    "amortization_other_electric_plant_subdimension_correction",
                    "depreciation_amortization_total",
                    "depreciation_amortization_total_correction",
                    "depreciation_amortization_total_subdimension_correction",
                    "depreciation_expense",
                    "depreciation_expense_asset_retirement",
                    "depreciation_expense_asset_retirement_correction",
                    "depreciation_expense_asset_retirement_subdimension_correction",
                    "depreciation_expense_correction",
                    "depreciation_expense_subdimension_correction",
                }
            },
        }
    },
    "core_ferc1__yearly_depreciation_changes_sched219": {
        "depreciation_type": {
            "type": "string",
            "constraints": {
                "enum": {
                    "book_cost_of_asset_retirement_costs",
                    "book_cost_of_asset_retirement_costs_correction",
                    "book_cost_of_asset_retirement_costs_subdimension_correction",
                    "book_cost_of_retired_plant",
                    "book_cost_of_retired_plant_correction",
                    "book_cost_of_retired_plant_subdimension_correction",
                    "cost_of_removal_of_plant",
                    "cost_of_removal_of_plant_correction",
                    "cost_of_removal_of_plant_subdimension_correction",
                    "depreciation_expense_excluding_adjustments",
                    "depreciation_expense_excluding_adjustments_correction",
                    "depreciation_expense_excluding_adjustments_subdimension_correction",
                    "depreciation_expense_for_asset_retirement_costs",
                    "depreciation_expense_for_asset_retirement_costs_correction",
                    "depreciation_expense_for_asset_retirement_costs_subdimension_correction",
                    "depreciation_provision",
                    "depreciation_provision_correction",
                    "depreciation_provision_subdimension_correction",
                    "ending_balance",
                    "ending_balance_correction",
                    "ending_balance_subdimension_correction",
                    "expenses_of_electric_plant_leased_to_others",
                    "expenses_of_electric_plant_leased_to_others_correction",
                    "expenses_of_electric_plant_leased_to_others_subdimension_correction",
                    "net_charges_for_retired_plant",
                    "net_charges_for_retired_plant_correction",
                    "net_charges_for_retired_plant_subdimension_correction",
                    "other_accounts",
                    "other_accounts_correction",
                    "other_accounts_subdimension_correction",
                    "other_adjustments_to_accumulated_depreciation",
                    "other_adjustments_to_accumulated_depreciation_correction",
                    "other_adjustments_to_accumulated_depreciation_subdimension_correction",
                    "other_clearing_accounts",
                    "other_clearing_accounts_correction",
                    "other_clearing_accounts_subdimension_correction",
                    "salvage_value_of_retired_plant",
                    "salvage_value_of_retired_plant_correction",
                    "salvage_value_of_retired_plant_subdimension_correction",
                    "starting_balance",
                    "starting_balance_correction",
                    "starting_balance_subdimension_correction",
                    "transportation_expenses_clearing",
                    "transportation_expenses_clearing_correction",
                    "transportation_expenses_clearing_subdimension_correction",
                }
            },
        }
    },
    "core_ferc1__yearly_depreciation_by_function_sched219": {
        "depreciation_type": {
            "type": "string",
            "constraints": {
                "enum": {
                    "accumulated_depreciation",
                    "accumulated_depreciation_correction",
                    "accumulated_depreciation_subdimension_correction",
                }
            },
        }
    },
    "core_ferc1__yearly_operating_expenses_sched320": {
        "expense_type": {
            "type": "string",
            "constraints": {
                "enum": {
                    "administrative_and_general_expenses",
                    "administrative_and_general_expenses_correction",
                    "administrative_and_general_operation_expense",
                    "administrative_and_general_operation_expense_correction",
                    "administrative_and_general_salaries",
                    "administrative_expenses_transferred_credit",
                    "advertising_expenses",
                    "allowances",
                    "ancillary_services_market_administration",
                    "capacity_market_administration",
                    "coolants_and_water",
                    "customer_account_expenses",
                    "customer_account_expenses_correction",
                    "customer_assistance_expenses",
                    "customer_installations_expenses",
                    "customer_records_and_collection_expenses",
                    "customer_service_and_information_expenses",
                    "customer_service_and_information_expenses_correction",
                    "day_ahead_and_real_time_market_administration",
                    "demonstrating_and_selling_expenses",
                    "distribution_expenses",
                    "distribution_expenses_correction",
                    "distribution_maintenance_expense_electric",
                    "distribution_maintenance_expense_electric_correction",
                    "distribution_operation_expenses_electric",
                    "distribution_operation_expenses_electric_correction",
                    "duplicate_charges_credit",
                    "electric_expenses_hydraulic_power_generation",
                    "electric_expenses_nuclear_power_generation",
                    "electric_expenses_steam_power_generation",
                    "employee_pensions_and_benefits",
                    "franchise_requirements",
                    "fuel",
                    "fuel_steam_power_generation",
                    "general_advertising_expenses",
                    "generation_expenses",
                    "generation_interconnection_studies",
                    "hydraulic_expenses",
                    "hydraulic_power_generation_maintenance_expense",
                    "hydraulic_power_generation_maintenance_expense_correction",
                    "hydraulic_power_generation_operations_expense",
                    "hydraulic_power_generation_operations_expense_correction",
                    "informational_and_instructional_advertising_expenses",
                    "injuries_and_damages",
                    "load_dispatch_monitor_and_operate_transmission_system",
                    "load_dispatch_reliability",
                    "load_dispatch_transmission_service_and_scheduling",
                    "load_dispatching",
                    "load_dispatching_transmission_expense",
                    "maintenance_of_boiler_plant_steam_power_generation",
                    "maintenance_of_communication_equipment_electric_transmission",
                    "maintenance_of_communication_equipment_regional_market_expenses",
                    "maintenance_of_computer_hardware",
                    "maintenance_of_computer_hardware_transmission",
                    "maintenance_of_computer_software",
                    "maintenance_of_computer_software_transmission",
                    "maintenance_of_electric_plant_hydraulic_power_generation",
                    "maintenance_of_electric_plant_nuclear_power_generation",
                    "maintenance_of_electric_plant_steam_power_generation",
                    "maintenance_of_energy_storage_equipment",
                    "maintenance_of_energy_storage_equipment_other_power_generation",
                    "maintenance_of_energy_storage_equipment_transmission",
                    "maintenance_of_general_plant",
                    "maintenance_of_generating_and_electric_plant",
                    "maintenance_of_line_transformers",
                    "maintenance_of_meters",
                    "maintenance_of_miscellaneous_distribution_plant",
                    "maintenance_of_miscellaneous_hydraulic_plant",
                    "maintenance_of_miscellaneous_market_operation_plant",
                    "maintenance_of_miscellaneous_nuclear_plant",
                    "maintenance_of_miscellaneous_other_power_generation_plant",
                    "maintenance_of_miscellaneous_regional_transmission_plant",
                    "maintenance_of_miscellaneous_steam_plant",
                    "maintenance_of_miscellaneous_transmission_plant",
                    "maintenance_of_overhead_lines",
                    "maintenance_of_overhead_lines_transmission",
                    "maintenance_of_reactor_plant_equipment_nuclear_power_generation",
                    "maintenance_of_reservoirs_dams_and_waterways",
                    "maintenance_of_station_equipment",
                    "maintenance_of_station_equipment_transmission",
                    "maintenance_of_street_lighting_and_signal_systems",
                    "maintenance_of_structures",
                    "maintenance_of_structures_and_improvements_regional_market_expenses",
                    "maintenance_of_structures_distribution_expense",
                    "maintenance_of_structures_hydraulic_power_generation",
                    "maintenance_of_structures_nuclear_power_generation",
                    "maintenance_of_structures_steam_power_generation",
                    "maintenance_of_structures_transmission_expense",
                    "maintenance_of_underground_lines",
                    "maintenance_of_underground_lines_transmission",
                    "maintenance_supervision_and_engineering",
                    "maintenance_supervision_and_engineering_electric_transmission_expenses",
                    "maintenance_supervision_and_engineering_hydraulic_power_generation",
                    "maintenance_supervision_and_engineering_nuclear_power_generation",
                    "maintenance_supervision_and_engineering_other_power_generation",
                    "maintenance_supervision_and_engineering_steam_power_generation",
                    "market_facilitation_monitoring_and_compliance_services",
                    "market_monitoring_and_compliance",
                    "meter_expenses",
                    "meter_reading_expenses",
                    "miscellaneous_customer_accounts_expenses",
                    "miscellaneous_customer_service_and_informational_expenses",
                    "miscellaneous_distribution_expenses",
                    "miscellaneous_general_expenses",
                    "miscellaneous_hydraulic_power_generation_expenses",
                    "miscellaneous_nuclear_power_expenses",
                    "miscellaneous_other_power_generation_expenses",
                    "miscellaneous_sales_expenses",
                    "miscellaneous_steam_power_expenses",
                    "miscellaneous_transmission_expenses",
                    "nuclear_fuel_expense",
                    "nuclear_power_generation_maintenance_expense",
                    "nuclear_power_generation_maintenance_expense_correction",
                    "nuclear_power_generation_operations_expense",
                    "nuclear_power_generation_operations_expense_correction",
                    "office_supplies_and_expenses",
                    "operation_of_energy_storage_equipment",
                    "operation_of_energy_storage_equipment_distribution",
                    "operation_of_energy_storage_equipment_transmission_expense",
                    "operation_supervision",
                    "operation_supervision_and_engineering_distribution_expense",
                    "operation_supervision_and_engineering_electric_transmission_expenses",
                    "operation_supervision_and_engineering_hydraulic_power_generation",
                    "operation_supervision_and_engineering_nuclear_power_generation",
                    "operation_supervision_and_engineering_other_power_generation",
                    "operation_supervision_and_engineering_steam_power_generation",
                    "operations_and_maintenance_expenses_electric",
                    "operations_and_maintenance_expenses_electric_correction",
                    "other_expenses_other_power_supply_expenses",
                    "other_power_generation_maintenance_expense",
                    "other_power_generation_maintenance_expense_correction",
                    "other_power_generation_operations_expense",
                    "other_power_generation_operations_expense_correction",
                    "other_power_supply_expense",
                    "other_power_supply_expense_correction",
                    "outside_services_employed",
                    "overhead_line_expense",
                    "overhead_line_expenses",
                    "power_production_expenses",
                    "power_production_expenses_correction",
                    "power_production_expenses_hydraulic_power",
                    "power_production_expenses_hydraulic_power_correction",
                    "power_production_expenses_nuclear_power",
                    "power_production_expenses_nuclear_power_correction",
                    "power_production_expenses_other_power",
                    "power_production_expenses_other_power_correction",
                    "power_production_expenses_steam_power",
                    "power_production_expenses_steam_power_correction",
                    "power_purchased_for_storage_operations",
                    "property_insurance",
                    "purchased_power",
                    "regional_market_expenses",
                    "regional_market_expenses_correction",
                    "regional_market_maintenance_expense",
                    "regional_market_maintenance_expense_correction",
                    "regional_market_operation_expense",
                    "regional_market_operation_expense_correction",
                    "regulatory_commission_expenses",
                    "reliability_planning_and_standards_development",
                    "reliability_planning_and_standards_development_services",
                    "rents_administrative_and_general_expense",
                    "rents_distribution_expense",
                    "rents_hydraulic_power_generation",
                    "rents_nuclear_power_generation",
                    "rents_other_power_generation",
                    "rents_regional_market_expenses",
                    "rents_steam_power_generation",
                    "rents_transmission_electric_expense",
                    "sales_expenses",
                    "sales_expenses_correction",
                    "scheduling_system_control_and_dispatch_services",
                    "station_expenses_distribution",
                    "station_expenses_transmission_expense",
                    "steam_expenses_nuclear_power_generation",
                    "steam_expenses_steam_power_generation",
                    "steam_from_other_sources",
                    "steam_from_other_sources_nuclear_power_generation",
                    "steam_power_generation_maintenance_expense",
                    "steam_power_generation_maintenance_expense_correction",
                    "steam_power_generation_operations_expense",
                    "steam_power_generation_operations_expense_correction",
                    "steam_transferred_credit",
                    "steam_transferred_credit_nuclear_power_generation",
                    "street_lighting_and_signal_system_expenses",
                    "supervision_customer_account_expenses",
                    "supervision_customer_service_and_information_expenses",
                    "supervision_sales_expense",
                    "system_control_and_load_dispatching_electric",
                    "transmission_expenses",
                    "transmission_expenses_correction",
                    "transmission_maintenance_expense_electric",
                    "transmission_maintenance_expense_electric_correction",
                    "transmission_of_electricity_by_others",
                    "transmission_operation_expense",
                    "transmission_operation_expense_correction",
                    "transmission_rights_market_administration",
                    "transmission_service_studies",
                    "uncollectible_accounts",
                    "underground_line_expenses",
                    "underground_line_expenses_transmission_expense",
                    "water_for_power",
                }
            },
        }
    },
    "core_ferc1__yearly_retained_earnings_sched118": {
        "earnings_type": {
            "type": "string",
            "constraints": {
                "enum": {
                    "adjustments_to_retained_earnings_credit",
                    "adjustments_to_retained_earnings_debit",
                    "appropriated_retained_earnings",
                    "appropriated_retained_earnings_amortization_reserve_federal",
                    "appropriated_retained_earnings_including_reserve_amortization",
                    "appropriated_retained_earnings_including_reserve_amortization_correction",
                    "appropriations_of_retained_earnings",
                    "balance_transferred_from_income",
                    "changes_unappropriated_undistributed_subsidiary_earnings_credits",
                    "dividends_declared_common_stock",
                    "dividends_declared_preferred_stock",
                    "dividends_received",
                    "equity_in_earnings_of_subsidiary_companies",
                    "retained_earnings",
                    "retained_earnings_correction",
                    "transfers_from_unappropriated_undistributed_subsidiary_earnings",
                    "unappropriated_retained_earnings",
                    "unappropriated_retained_earnings_correction",
                    "unappropriated_retained_earnings_previous_year",
                    "unappropriated_undistributed_subsidiary_earnings",
                    "unappropriated_undistributed_subsidiary_earnings_correction",
                    "unappropriated_undistributed_subsidiary_earnings_previous_year",
                }
            },
        }
    },
    "core_ferc1__yearly_operating_revenues_sched300": {
        "revenue_type": {
            "type": "string",
            "constraints": {
                "enum": {
                    "electric_operating_revenues",
                    "electric_operating_revenues_correction",
                    "forfeited_discounts",
                    "interdepartmental_rents",
                    "interdepartmental_sales",
                    "interdepartmental_sales_correction",
                    "large_or_industrial",
                    "large_or_industrial_correction",
                    "miscellaneous_revenue",
                    "miscellaneous_service_revenues",
                    "other_electric_revenue",
                    "other_miscellaneous_operating_revenues",
                    "other_operating_revenues",
                    "other_operating_revenues_correction",
                    "other_sales_to_public_authorities",
                    "other_sales_to_public_authorities_correction",
                    "provision_for_rate_refunds",
                    "public_street_and_highway_lighting",
                    "public_street_and_highway_lighting_correction",
                    "regional_transmission_service_revenues",
                    "rent_from_electric_property",
                    "residential_sales",
                    "residential_sales_correction",
                    "revenues_from_transmission_of_electricity_of_others",
                    "revenues_net_of_provision_for_refunds",
                    "revenues_net_of_provision_for_refunds_correction",
                    "sales_for_resale",
                    "sales_for_resale_correction",
                    "sales_of_electricity",
                    "sales_of_electricity_correction",
                    "sales_of_water_and_water_power",
                    "sales_to_railroads_and_railways",
                    "sales_to_railroads_and_railways_correction",
                    "sales_to_ultimate_consumers",
                    "sales_to_ultimate_consumers_correction",
                    "small_or_commercial",
                    "small_or_commercial_correction",
                }
            },
        }
    },
    "core_ferc1__yearly_cash_flows_sched120": {
        "amount_type": {
            "type": "string",
            "constraints": {
                "enum": {
                    "acquisition_of_other_noncurrent_assets",
                    "allowance_for_other_funds_used_during_construction_investing_activities",
                    "allowance_for_other_funds_used_during_construction_operating_activities",
                    "cash_flows_provided_from_used_in_financing_activities_correction",
                    "cash_flows_provided_from_used_in_investment_activities_correction",
                    "cash_outflows_for_plant",
                    "cash_outflows_for_plant_correction",
                    "cash_provided_by_outside_sources",
                    "cash_provided_by_outside_sources_correction",
                    "collections_on_loans",
                    "contributions_and_advances_from_associated_and_subsidiary_companies",
                    "deferred_income_taxes_net",
                    "depreciation_and_depletion",
                    "disposition_of_investments_in_and_advances_to_associated_and_subsidiary_companies",
                    "dividends_on_common_stock",
                    "dividends_on_preferred_stock",
                    "gross_additions_to_common_utility_plant_investing_activities",
                    "gross_additions_to_nonutility_plant_investing_activities",
                    "gross_additions_to_nuclear_fuel_investing_activities",
                    "gross_additions_to_utility_plant_less_nuclear_fuel_investing_activities",
                    "investment_tax_credit_adjustments_net",
                    "investments_in_and_advances_to_associated_and_subsidiary_companies",
                    "loans_made_or_purchased",
                    "net_cash_flow_from_operating_activities_correction",
                    "net_decrease_in_short_term_debt",
                    "net_income_loss",
                    "net_income_loss_correction",
                    "net_increase_decrease_in_allowances_held_for_speculation_investing_activities",
                    "net_increase_decrease_in_allowances_inventory_operating_activities",
                    "net_increase_decrease_in_inventory_investing_activities",
                    "net_increase_decrease_in_inventory_operating_activities",
                    "net_increase_decrease_in_other_regulatory_assets_operating_activities",
                    "net_increase_decrease_in_other_regulatory_liabilities_operating_activities",
                    "net_increase_decrease_in_payables_and_accrued_expenses_investing_activities",
                    "net_increase_decrease_in_payables_and_accrued_expenses_operating_activities",
                    "net_increase_decrease_in_receivables_investing_activities",
                    "net_increase_decrease_in_receivables_operating_activities",
                    "net_increase_in_short_term_debt",
                    "noncash_adjustments_to_cash_flows_from_operating_activities",
                    "other_adjustments_by_outside_sources_to_cash_flows_from_financing_activities",
                    "other_adjustments_to_cash_flows_from_financing_activities",
                    "other_adjustments_to_cash_flows_from_investment_activities",
                    "other_adjustments_to_cash_flows_from_operating_activities",
                    "other_construction_and_acquisition_of_plant_investment_activities",
                    "other_retirements_of_balances_impacting_cash_flows_from_financing_activities",
                    "payments_for_retirement_of_common_stock_financing_activities",
                    "payments_for_retirement_of_long_term_debt_financing_activities",
                    "payments_for_retirement_of_preferred_stock_financing_activities",
                    "proceeds_from_disposal_of_noncurrent_assets",
                    "proceeds_from_issuance_of_common_stock_financing_activities",
                    "proceeds_from_issuance_of_long_term_debt_financing_activities",
                    "proceeds_from_issuance_of_preferred_stock_financing_activities",
                    "proceeds_from_sales_of_investment_securities",
                    "purchase_of_investment_securities",
                    "undistributed_earnings_from_subsidiary_companies_operating_activities",
                    "net_cash_flow_from_operating_activities",
                    "cash_flows_provided_from_used_in_investment_activities",
                    "cash_flows_provided_from_used_in_financing_activities",
                    "net_increase_decrease_in_cash_and_cash_equivalents",
                    "starting_balance",
                    "ending_balance",
                    "disposition_of_investments_in_and_advances_to_associated_and_subsidiary_companies_abstract",
                    "net_increase_decrease_in_cash_and_cash_equivalents_abstract",
                    "payments_for_retirement_abstract",
                }
            },
        }
    },
    "plant_parts_eia": {
        "energy_source_code_1": {
            "constraints": {
                "enum": set(
                    CODE_METADATA["core_eia__codes_energy_sources"]["df"]["code"]
                )
            }
        },
        "prime_movers_eia": {
            "constraints": {
                "enum": set(CODE_METADATA["core_eia__codes_prime_movers"]["df"]["code"])
            }
        },
        "technology_description": {"constraints": {"enum": set(TECH_DESCRIPTIONS)}},
    },
    "core_ferc1__yearly_transmission_lines_sched422": {
        "capex_land": {
            "description": "Cost of Land and land rights for the transmission line."
        },
        "capex_other": {
            "description": "Construction and other costs for the transmission line."
        },
        "capex_total": {"description": "Total costs for the transmission line."},
        "opex_operations": {
            "description": "Operating expenses for the transmission line."
        },
        "opex_maintenance": {
            "description": "Maintenance expenses for the transmission line."
        },
        "opex_rents": {"description": "Rent expenses for the transmission line."},
        "opex_total": {"description": "Overall expenses for the transmission line."},
    },
    "out_ferc714__hourly_planning_area_demand": {
        "timezone": {"constraints": {"enum": US_TIMEZONES}},
        "report_date": {
            "constraints": {
                "required": True,
            }
        },
    },
    "core_eia930__hourly_net_generation_by_energy_source": {
        "datetime_utc": {
            "description": "Timestamp at the end of the hour for which the data is reported."
        },
    },
    "core_eia930__hourly_operations": {
        "datetime_utc": {
            "description": "Timestamp at the end of the hour for which the data is reported."
        },
    },
    "core_eia930__hourly_subregion_demand": {
        "datetime_utc": {
            "description": "Timestamp at the end of the hour for which the data is reported."
        },
        "demand_reported_mwh": {
            "description": "Originally reported electricity demand for the balancing area subregion. Note that different BAs have different methods of calculating and allocating subregion demand.",
        },
    },
    "core_eia930__hourly_interchange": {
        "datetime_utc": {
            "description": "Timestamp at the end of the hour for which the data is reported."
        },
    },
    "core_nrelatb__yearly_projected_cost_performance": {
        "fuel_cost_per_mwh": {
            "type": "number",
            "description": "Fuel costs in USD$/MWh. NREL-derived values using heat rates.",
            "unit": "USD_per_MWh",
        }
    },
    "core_sec10k__changelog_company_name": {
        "central_index_key": {
            "constraints": {
                "required": True,
            }
        },
        "name_change_date": {
            "constraints": {
                "required": True,
            }
        },
    },
    "out_sec10k__changelog_company_name": {
        "central_index_key": {
            "constraints": {
                "required": True,
            }
        },
        "name_change_date": {
            "constraints": {
                "required": True,
            }
        },
    },
    "out_ferc1__yearly_rate_base": {
        "plant_function": {
            "type": "string",
            "description": "Functional role played by utility plant (steam production, nuclear production, distribution, transmission, etc.).",
            "constraints": {
                "enum": [
                    "distribution",
                    "experimental",
                    "general",
                    "hydraulic_production",
                    "intangible",
                    "nuclear_production",
                    "other_production",
                    "purchased_sold",
                    "regional_transmission_and_market_operation",
                    "steam_production",
                    "transmission",
                    "unclassified",
                ]
            },
        },
        "utility_type": {
            "type": "string",
            "description": "Listing of utility plant types.",
            "constraints": {
                "enum": ["electric", "gas", "common", "other", "other3", "other2"]
            },
        },
    },
    "out_eia__yearly_assn_plant_parts_plant_gen": {
        "generators_number": {
            "description": "The number of generators associated with each ``record_id_eia``."
        }
    },
    "out_eia860__yearly_ownership": {
        "utility_id_pudl": {
            "description": "A manually assigned PUDL utility ID for the owner company that is responsible for the day-to-day operations of the generator, not the operator utility. May not be stable over time."
        }
    },
    "out_eia930__hourly_aggregated_demand": {
        "aggregation_group": {
            "description": "Label identifying a group of balancing authorities to be used in aggregating demand E.g. a region of the US or a whole interconnect."
        }
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
    strict: bool = False,
) -> pd.DataFrame:
    """Apply dtypes to those columns in a dataframe that have PUDL types defined.

    Note that ad-hoc column dtypes can be defined and merged with default PUDL field
    metadata before it's passed in as ``field_meta`` if you have module specific column
    types you need to apply alongside the standard PUDL field types.

    Args:
        df: The dataframe to apply types to. Not all columns need to have types
            defined in the PUDL metadata unless you pass ``strict=True``.
        group: The data group to use for overrides, if any. E.g. "eia", "ferc1".
        field_meta: A dictionary of field metadata, where each key is a field name
            and the values are dictionaries which must have a "type" element. By
            default this is pudl.metadata.fields.FIELD_METADATA.
        field_meta_by_group: A dictionary of field metadata to use as overrides,
            based on the value of `group`, if any. By default it uses the overrides
            defined in pudl.metadata.fields.FIELD_METADATA_BY_GROUP.
        strict: whether or not all columns need a corresponding field.

    Returns:
        The input dataframe, but with standard PUDL types applied.
    """
    unspecified_fields = sorted(
        set(df.columns)
        - set(field_meta.keys())
        - set(field_meta_by_group.get(group, {}).keys())
    )
    if strict and len(unspecified_fields) > 0:
        raise ValueError(f"Found unspecified fields: {unspecified_fields}")
    dtypes = get_pudl_dtypes(
        group=group,
        field_meta=field_meta,
        field_meta_by_group=field_meta_by_group,
        dtype_map=FIELD_DTYPES_PANDAS,
    )

    return df.astype({col: dtypes[col] for col in df.columns if col in dtypes})
