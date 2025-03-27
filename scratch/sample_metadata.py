"""Sample migrated metadata."""

metadata = {
    "_core_eia860__fgd_equipment": {
        "original_description": """
Information about flue gas desulfurization equipment at generation facilities, from EIA-860 Schedule 6E. Note: This table has been cleaned, but not harvested with other EIA 923 or 860 data. The same variables present in this table may show up in other _core tables in other years. Once this table has been harvested, it will be removed from the PUDL database.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
[else] Catalogs available flue gas desulfurization equipment at generation facilities.

Derived from EIA-860 Schedule 6E.

Usage Warnings
^^^^^^^^^^^^^^
This table has been cleaned, but not harvested with other EIA 923 or 860 data. The same variables present in this table may show up in other _core tables in other years. Once this table has been harvested, it will be removed from the PUDL database.
""",
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "so2_control_id_eia",
                "utility_id_eia",
                "utility_name_eia",
                "state",
                "state_id_fips",
                "county",
                "county_id_fips",
                "fgd_operating_date",
                "fgd_operational_status_code",
                "flue_gas_bypass_fgd",
                "byproduct_recovery",
                "sludge_pond",
                "sludge_pond_lined",
                "pond_landfill_requirements_acre_foot_per_year",
                "fgd_structure_cost",
                "fgd_other_cost",
                "sludge_disposal_cost",
                "total_fgd_equipment_cost",
                "fgd_trains_100pct",
                "fgd_trains_total",
                "flue_gas_entering_fgd_pct_of_total",
                "flue_gas_exit_rate_cubic_feet_per_minute",
                "flue_gas_exit_temperature_fahrenheit",
                "so2_emission_rate_lbs_per_hour",
                "so2_equipment_type_1",
                "so2_equipment_type_2",
                "so2_equipment_type_3",
                "so2_equipment_type_4",
                "so2_removal_efficiency_design",
                "specifications_of_coal_ash",
                "specifications_of_coal_sulfur",
                "sorbent_type_1",
                "sorbent_type_2",
                "sorbent_type_3",
                "sorbent_type_4",
                "fgd_manufacturer",
                "fgd_manufacturer_code",
                "steam_plant_type_code",
                "plant_summer_capacity_mw",
                "water_source",
                "data_maturity",
            ],
            "primary_key": ["plant_id_eia", "so2_control_id_eia", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "eia860",
    },
    "_core_eia923__cooling_system_information": {
        "original_description": """
EIA-923 Cooling System Information, from EIA-923 Schedule 8D.

Reports monthly information about cooling systems at generation facilities,
mainly water volumes and temperatures. In 2008 and 2009, EIA only reports
annual averages, but in later years all data is monthly.

Note: This table has been cleaned, but not harvested with other EIA 923 or 860
data. The same variables present in this table may show up in other _core
tables in other years. Once this table has been harvested, it will be removed
from the PUDL database.
""",
        "description": """
[if timeseries] Time series of cooling system statistics at generation facilities, mainly water volumes and temperatures. Reported monthly, except for 2008 and 2009, which report annual averages.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from EIA-923 Schedule 8D.

Usage Warnings
^^^^^^^^^^^^^^
This table has been cleaned, but not harvested with other EIA 923 or 860
data. The same variables present in this table may show up in other _core
tables in other years. Once this table has been harvested, it will be removed
from the PUDL database.
""",
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "cooling_id_eia",
                "cooling_status_code",
                "cooling_type",
                "monthly_total_cooling_hours_in_service",
                "flow_rate_method",
                "temperature_method",
                "annual_maximum_intake_summer_temperature_fahrenheit",
                "annual_maximum_intake_winter_temperature_fahrenheit",
                "monthly_average_intake_temperature_fahrenheit",
                "monthly_maximum_intake_temperature_fahrenheit",
                "annual_maximum_outlet_summer_temperature_fahrenheit",
                "annual_maximum_outlet_winter_temperature_fahrenheit",
                "monthly_average_discharge_temperature_fahrenheit",
                "monthly_maximum_discharge_temperature_fahrenheit",
                "annual_average_consumption_rate_gallons_per_minute",
                "monthly_average_consumption_rate_gallons_per_minute",
                "monthly_total_consumption_volume_gallons",
                "annual_average_discharge_rate_gallons_per_minute",
                "monthly_average_discharge_rate_gallons_per_minute",
                "monthly_total_discharge_volume_gallons",
                "monthly_average_diversion_rate_gallons_per_minute",
                "monthly_total_diversion_volume_gallons",
                "annual_average_withdrawal_rate_gallons_per_minute",
                "monthly_average_withdrawal_rate_gallons_per_minute",
                "monthly_total_withdrawal_volume_gallons",
                "annual_total_chlorine_lbs",
                "monthly_total_chlorine_lbs",
                "data_maturity",
            ],
            "primary_key": ["plant_id_eia", "report_date", "cooling_id_eia"],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "_out_eia__monthly_derived_generator_attributes": {
        "original_description": """
Monthly generator capacity factor, heat rate, fuel cost per MMBTU and fuel cost per MWh. These calculations are based on the allocation of net generation reported on the basis of plant, prime mover and energy source to individual generators. Heat rates by generator-month are estimated by using allocated estimates for per-generator net generation and fuel consumption as well as the :ref:`core_eia923__monthly_boiler_fuel` table, which reports fuel consumed by boiler. Heat rates are necessary to estimate the amount of fuel consumed by a generation unit, and thus the fuel cost per MWh generated. Plant specific fuel prices are taken from the :ref:`core_eia923__monthly_fuel_receipts_costs` table, which only has ~70% coverage, leading to some generators with heat rate estimates still lacking fuel cost estimates.
""",
        "description": """
[if timeseries] Time series of generator capacity factor, heat rate, fuel cost per MMBTU and fuel cost per MWh.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from eia923 and eia860.

Usage Warnings
^^^^^^^^^^^^^^
Plant specific fuel prices are taken from the :ref:`core_eia923__monthly_fuel_receipts_costs` table, which only has ~70% coverage, leading to some generators with heat rate estimates still lacking fuel cost estimates.

Additional Details
^^^^^^^^^^^^^^^^^^
These calculations are based on the allocation of net generation reported on the basis of plant, prime mover and energy source to individual generators. Heat rates by generator-month are estimated by using allocated estimates for per-generator net generation and fuel consumption as well as the :ref:`core_eia923__monthly_boiler_fuel` table, which reports fuel consumed by boiler. Heat rates are necessary to estimate the amount of fuel consumed by a generation unit, and thus the fuel cost per MWh generated.
""",
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "unit_id_pudl",
                "report_date",
                "capacity_factor",
                "fuel_cost_per_mmbtu_source",
                "fuel_cost_per_mmbtu",
                "fuel_cost_per_mwh",
                "unit_heat_rate_mmbtu_per_mwh",
                "net_generation_mwh",
                "total_fuel_cost",
                "total_mmbtu",
            ],
            "primary_key": ["report_date", "plant_id_eia", "generator_id"],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "_out_eia__plants_utilities": {
        "original_description": """
Denormalized table containing all plant and utility IDs and names from EIA.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for all plant and utility IDs and names from EIA.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from eia860 and eia923.
""",
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "plant_name_eia",
                "plant_id_pudl",
                "utility_id_eia",
                "utility_name_eia",
                "utility_id_pudl",
                "data_maturity",
            ],
            "primary_key": ["report_date", "plant_id_eia", "utility_id_eia"],
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
    },
    "_out_eia__yearly_fuel_cost_by_generator": {
        "original_description": """
Yearly estimate of per-generator fuel costs both per MMBTU and per MWh. These calculations are based on the allocation of net generation and fuel consumption as well as plant-level delivered fuel prices reported in the fuel receipts and cost table. The intermediary heat rate calculation depends on having the unit ID filled in, which means fuel cost coverage is low. The fuel costs are also currently aggregated to coarse fuel categories rather than using the more detailed energy source codes.Note that the values in this table are unfiltered and we expect some of the values are unreasonable and out of bounds.This table should not be used without filtering values to within logical boundaries.
""",
        "description": """
[if timeseries] Time series of estimated per-generator fuel costs both per MMBTU and per MWh.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from eia923 and eia860.

Usage Warnings
^^^^^^^^^^^^^^
The intermediary heat rate calculation depends on having the unit ID filled in, which means fuel cost coverage is low.

The fuel costs are currently aggregated to coarse fuel categories rather than using the more detailed energy source codes.

The values in this table are unfiltered and we expect some of the values are unreasonable and out of bounds. **This table should not be used without filtering values to within logical boundaries.**

Additional Details
^^^^^^^^^^^^^^^^^^
These calculations are based on the allocation of net generation and fuel consumption as well as plant-level delivered fuel prices reported in the fuel receipts and cost table.
""",
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "unit_id_pudl",
                "plant_name_eia",
                "plant_id_pudl",
                "utility_id_eia",
                "utility_name_eia",
                "utility_id_pudl",
                "fuel_type_count",
                "fuel_type_code_pudl",
                "fuel_cost_per_mmbtu_source",
                "fuel_cost_per_mmbtu",
                "unit_heat_rate_mmbtu_per_mwh",
                "fuel_cost_per_mwh",
            ],
            "primary_key": ["report_date", "plant_id_eia", "generator_id"],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "_out_ferc1__yearly_plants_utilities": {
        "original_description": """
Denormalized table that contains FERC plant and utility information.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for FERC plants and utilities.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from ferc1.
""",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "plant_name_ferc1",
                "plant_id_pudl",
                "utility_name_ferc1",
                "utility_id_pudl",
            ],
            "primary_key": ["utility_id_ferc1", "plant_name_ferc1"],
        },
        "field_namespace": "ferc1",
        "etl_group": "outputs",
        "sources": ["ferc1"],
    },
    "core_eia861__yearly_utility_data_nerc": {
        "original_description": """
The NERC regions that a utiltiy operates in.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
[if assn] Links utilities to the NERC regions where they operate.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

This table has no primary key. Each row states that a particular utility operated in a particular region at a particular time.

Derived from eia861.
""",
        "schema": {
            "fields": [
                "nerc_region",
                "nerc_regions_of_operation",
                "report_date",
                "state",
                "utility_id_eia",
                "data_maturity",
            ]
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia923__monthly_boiler_fuel": {
        "original_description": """
EIA-923 Monthly Boiler Fuel Consumption and Emissions, from EIA-923 Schedule 3.

Reports the quantity of each type of fuel consumed by each boiler on a monthly basis, as
well as the sulfur and ash content of those fuels. Fuel quantity is reported in standard
EIA fuel units (tons, barrels, Mcf). Heat content per unit of fuel is also reported,
making this table useful for calculating the thermal efficiency (heat rate) of various
generation units.

This table provides better coverage of the entire fleet of generators than the
``core_eia923__monthly_generation_fuel`` table, but the fuel consumption reported here is not directly
associated with a generator. This complicates the heat rate calculation, since the
associations between individual boilers and generators are incomplete and can be
complex.

Note that a small number of respondents only report annual fuel consumption, and all of
it is reported in December.
""",
        "description": """
[if timeseries] Time series of boiler fuel consumption and emissions.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from EIA-923 Schedule 3.

Usage Warnings
^^^^^^^^^^^^^^
This table provides better coverage of the entire fleet of generators than the ``core_eia923__monthly_generation_fuel`` table, but the fuel consumption reported here is not directly associated with a generator. This complicates the heat rate calculation, since the associations between individual boilers and generators are incomplete and can be complex.

A small number of respondents only report annual fuel consumption, and all of
it is reported in December.

Additional Details
^^^^^^^^^^^^^^^^^^
Reports the quantity of each type of fuel consumed by each boiler, as
well as the sulfur and ash content of those fuels. Fuel quantity is reported in standard
EIA fuel units (tons, barrels, Mcf). Heat content per unit of fuel is also reported,
making this table useful for calculating the thermal efficiency (heat rate) of various
generation units.
""",
        "schema": {
            "fields": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
                "prime_mover_code",
                "fuel_type_code_pudl",
                "report_date",
                "fuel_consumed_units",
                "fuel_mmbtu_per_unit",
                "sulfur_content_pct",
                "ash_content_pct",
                "data_maturity",
            ],
            "primary_key": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
                "prime_mover_code",
                "report_date",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "core_eia923__monthly_fuel_receipts_costs": {
        "original_description": """
Data describing fuel deliveries to power plants, reported in EIA-923 Schedule 2, Part A.

Each record describes an individual fuel delivery. There can be multiple deliveries of
the same type of fuel from the same supplier to the same plant in a single month, so the
table has no natural primary key.

There can be a significant delay between the receipt of fuel and its consumption, so
using this table to infer monthly attributes associated with power generation may not be
entirely accurate. However, this is the most granular data we have describing fuel
costs, and we use it in calculating the marginal cost of electricity for individual
generation units.

Under some circumstances utilities are allowed to treat the price of fuel as proprietary
business data, meaning it is redacted from the publicly available spreadsheets. It's
still reported to EIA and influences the aggregated (state, region, annual, etc.) fuel
prices they publish. From 2009-2021 about 1/3 of all prices are redacted. The missing
data is not randomly distributed. Deregulated markets dominated by merchant generators
(independent power producers) redact much more data, and natural gas is by far the most
likely fuel to have its price redacted. This means, for instance, that the entire
Northeastern US reports essentially no fine-grained data about its natural gas prices.

Additional data which we haven't yet integrated is available in a similar format from
2002-2008 via the EIA-423, and going back as far as 1972 from the FERC-423.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
[else] Log of fuel deliveries to power plants.

This table has no primary key. Each row represents an individual fuel delivery. There can be multiple deliveries of the same type of fuel from the same supplier to the same plant in a single month, so the table has no natural primary key.

Derived from EIA-923 Schedule 2, Part A.

Usage Warnings
^^^^^^^^^^^^^^
Under some circumstances utilities are allowed to treat the price of fuel as proprietary
business data, meaning it is redacted from the publicly available spreadsheets. It's
still reported to EIA and influences the aggregated (state, region, annual, etc.) fuel
prices they publish. From 2009-2021 about 1/3 of all prices are redacted. The missing
data is not randomly distributed. Deregulated markets dominated by merchant generators
(independent power producers) redact much more data, and natural gas is by far the most
likely fuel to have its price redacted. This means, for instance, that the entire
Northeastern US reports essentially no fine-grained data about its natural gas prices.

Additional Details
^^^^^^^^^^^^^^^^^^
There can be a significant delay between the receipt of fuel and its consumption, so
using this table to infer monthly attributes associated with power generation may not be
entirely accurate. However, this is the most granular data we have describing fuel
costs, and we use it in calculating the marginal cost of electricity for individual
generation units.

Additional data which we haven't yet integrated is available in a similar format from
2002-2008 via the EIA-423, and going back as far as 1972 from the FERC-423.
""",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "contract_type_code",
                "contract_expiration_date",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_group_code",
                "mine_id_pudl",
                "supplier_name",
                "fuel_received_units",
                "fuel_mmbtu_per_unit",
                "sulfur_content_pct",
                "ash_content_pct",
                "mercury_content_ppm",
                "fuel_cost_per_mmbtu",
                "primary_transportation_mode_code",
                "secondary_transportation_mode_code",
                "natural_gas_transport_code",
                "natural_gas_delivery_contract_type_code",
                "moisture_content_pct",
                "chlorine_content_ppm",
                "data_maturity",
            ]
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "core_ferc1__yearly_plant_in_service_sched204": {
        "original_description": """
Balances and changes to FERC Electric Plant in Service accounts, as reported on FERC Form 1, Schedule 204. Data originally from the f1_plant_in_srvce table in FERC's FoxPro database. Account numbers correspond to the FERC Uniform System of Accounts for Electric Plant, which is defined in Code of Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g. https://www.law.cornell.edu/cfr/text/18/part-101). Each FERC respondent reports starting and ending balances for each account annually. Balances are organization wide, and are not broken down on a per-plant basis. End of year balance should equal beginning year balance plus the sum of additions, retirements, adjustments, and transfers.
""",
        "description": """
[if timeseries] Time series of balances and changes to FERC Electric Plant in Service accounts.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from FERC Form 1, Schedule 204; originally from the f1_plant_in_srvce table in FERC's FoxPro database.

Additional Details
^^^^^^^^^^^^^^^^^^
Account numbers correspond to the FERC Uniform System of Accounts for Electric Plant, which is defined in Code of Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g. https://www.law.cornell.edu/cfr/text/18/part-101). Each FERC respondent reports starting and ending balances for each account annually. Balances are organization wide, and are not broken down on a per-plant basis. End of year balance should equal beginning year balance plus the sum of additions, retirements, adjustments, and transfers.
""",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "ferc_account_label",
                "ferc_account",
                "row_type_xbrl",
                "starting_balance",
                "additions",
                "retirements",
                "adjustments",
                "transfers",
                "ending_balance",
                "record_id",
                "utility_type",
                "plant_status",
            ],
            "primary_key": ["utility_id_ferc1", "report_year", "ferc_account_label"],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_transmission_lines_sched422": {
        "original_description": """
Transmission Line Statistics. Schedule 422 of FERC Form 1. Information describing transmission lines, the cost of lines, annual operating and capital expenses, etc.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for transmission line statistics. Describes transmission lines, the cost of lines, annual operating and capital expenses, etc.
# [else] {Concise verb/noun indicating type} {short_description}.

This table has no primary key. Each row records statistics for one segment of one transmission line in a particular year.

Derived from Schedule 422 of FERC Form 1.
""",
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "start_point",
                "end_point",
                "operating_voltage_kv",
                "designed_voltage_kv",
                "supporting_structure_type",
                "transmission_line_length_miles",
                "transmission_line_and_structures_length_miles",
                "num_transmission_circuits",
                "conductor_size_and_material",
                "capex_land",
                "capex_other",
                "capex_total",
                "opex_operations",
                "opex_maintenance",
                "opex_rents",
                "opex_total",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc714__respondent_id": {
        "original_description": """
Respondent identification. FERC Form 714, Part I, Schedule 1.
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for Ferc Form 714 respondents.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from FERC Form 714, Part I, Schedule 1.
""",
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_id_ferc714_csv",
                "respondent_id_ferc714_xbrl",
                "respondent_name_ferc714",
                "eia_code",
            ],
            "primary_key": ["respondent_id_ferc714"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
    },
    "core_nrelatb__yearly_projected_financial_cases": {
        "original_description": """
Financial assumptions for each model case (model_case_nrelatb), and sub-type of technology (technology_description).
""",
        "description": """
# [if timeseries] Time series of {short_description}.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for the financial assumptions of each model case (model_case_nrelatb) and sub-type of technology (technology_description).
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from nrelatb.
""",
        "schema": {
            "fields": [
                "report_year",
                "model_case_nrelatb",
                "projection_year",
                "technology_description",
                "inflation_rate",
                "interest_rate_during_construction_nominal",
                "interest_rate_calculated_real",
                "interest_rate_nominal",
                "rate_of_return_on_equity_calculated_real",
                "rate_of_return_on_equity_nominal",
                "tax_rate_federal_state",
            ],
            "primary_key": [
                "report_year",
                "model_case_nrelatb",
                "projection_year",
                "technology_description",
            ],
        },
        "sources": ["nrelatb"],
        "etl_group": "nrelatb",
        "field_namespace": "nrelatb",
    },
    "out_eia923__monthly_generation_fuel_by_generator": {
        "original_description": """
Monthly estimated net generation and fuel consumption by generator. Based on allocating net electricity generation and fuel consumption reported in the EIA-923 generation and generation_fuel tables to individual generators.
""",
        "description": """
[if timeseries] Time series of estimated net generation and fuel consumption by generator.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from eia923 and eia860. Allocates net electricity generation and fuel consumption reported in the EIA-923 generation and generation_fuel tables to individual generators.
""",
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "plant_id_pudl",
                "plant_name_eia",
                "utility_id_eia",
                "utility_id_pudl",
                "utility_name_eia",
                "generator_id",
                "unit_id_pudl",
                "fuel_consumed_for_electricity_mmbtu",
                "fuel_consumed_mmbtu",
                "net_generation_mwh",
            ],
            "primary_key": ["report_date", "plant_id_eia", "generator_id"],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "out_eia923__yearly_generation_fuel_by_generator_energy_source": {
        "original_description": """
Yearly estimated net generation and fuel consumption associated with each combination of generator, energy source, and prime mover. First, the net electricity generation and fuel consumption reported in the EIA-923 generation fuel are allocated to individual generators. Then, these allocations are aggregated to unique generator, prime mover, and energy source code combinations. This process does not distinguish between primary and secondary energy_sources for generators. Net generation is allocated equally between energy source codes, so if a plant has multiple generators with the same prime_mover_code but different energy source codes the core_eia923__monthly_generation_fuel records will be associated similarly between these two generators. Allocated net generation will still be proportional to each generator's net generation or capacity.
""",
        "description": """
[if timeseries] Time series of estimated net generation and fuel consumption associated with each combination of generator, energy source, and prime mover.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from eia923 (core_eia923__monthly_generation_fuel) and eia860.

Usage Warnings
^^^^^^^^^^^^^^
This process does not distinguish between primary and secondary energy_sources for generators. Net generation is allocated equally between energy source codes, so if a plant has multiple generators with the same prime_mover_code but different energy source codes the core_eia923__monthly_generation_fuel records will be associated similarly between these two generators. Allocated net generation will still be proportional to each generator's net generation or capacity.

Additional Details
^^^^^^^^^^^^^^^^^^
First, the net electricity generation and fuel consumption reported in the EIA-923 generation fuel are allocated to individual generators. Then, these allocations are aggregated to unique generator, prime mover, and energy source code combinations.
""",
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "energy_source_code_num",
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "out_ferc1__yearly_balance_sheet_assets_sched110": {
        "original_description": """
Denormalized table that contains FERC balance sheet asset information.
""",
        "description": """
[if timeseries] Time series of balance sheet assets.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from ferc1 sched110.
""",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "asset_type",
                "balance",
                "ending_balance",
                "ferc_account",
                "row_type_xbrl",
                "starting_balance",
                "utility_type",
            ],
            "primary_key": ["utility_id_ferc1", "report_year", "asset_type"],
        },
        "field_namespace": "ferc1",
        "etl_group": "outputs",
        "sources": ["ferc1"],
    },
    "out_ferc1__yearly_sales_by_rate_schedules_sched304": {
        "original_description": """
Denormalized table that contains FERC electricity sales by rate schedule information.
""",
        "description": """
[if timeseries] Time series of electricity sales by rate schedule.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

This table has no primary key. Each row represents a sales record for a particular utility.

Derived from ferc1 sched304.
""",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "report_year",
                "rate_schedule_type",
                "billing_status",
                "rate_schedule_description",
                "sales_mwh",
                "dollar_value",
                "avg_customers_per_month",
                "kwh_per_customer",
                "revenue_per_kwh",
                "record_id",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc714__hourly_planning_area_demand": {
        "original_description": """
Hourly electricity demand by planning area. FERC Form 714, Part III, Schedule 2a. This table includes data from the pre-2021 CSV raw source as well as the newer 2021 through present XBRL raw source.

An important caveat to note is that there was some cleaning done to the datetime_utc timestamps. The Form 714 includes sparse documentation for respondents for how to interpret timestamps - the form asks respondents to provide 24 instances of hourly demand for each day. The form is labeled with hour 1-24. There is no indication if hour 1 begins at midnight.

The XBRL data contained several formats of timestamps. Most records corresponding to hour 1 of the Form have a timestamp with hour 1 as T1. About two thirds of the records in the hour 24 location of the form have a timestamp with an hour reported as T24 while the remaining third report this as T00 of the next day. T24 is not a valid format for the hour of a datetime, so we convert these T24 hours into T00 of the next day. A smaller subset of the respondents reports the 24th hour as the last second of the day - we also convert these records to the T00 of the next day.

This table includes three respondent ID columns: one from the CSV raw source, one from the XBRL raw source and another that is PUDL-derived that links those two source ID's together. This table has filled in source IDs for all records so you can select the full timeseries for a given respondent from any of these three IDs.
""",
        "description": """
[if timeseries] Time series of electricity demand by planning area.
# [if assn] Links {source1} to {source2}.
# [if codes] Explains codes for {purpose}.
# [if entity/scd] Entity/SCD table for {short_description}.
# [else] {Concise verb/noun indicating type} {short_description}.

Derived from FERC Form 714, Part III, Schedule 2a.

Usage Warnings
^^^^^^^^^^^^^^
This table includes data from the pre-2021 CSV raw source as well as the newer 2021 through present XBRL raw source.

There was some cleaning done to the datetime_utc timestamps. The Form 714 includes sparse documentation for respondents for how to interpret timestamps. The form asks respondents to provide 24 instances of hourly demand for each day. The form is labeled with hour 1-24. There is no indication if hour 1 begins at midnight.

The XBRL data contained several formats of timestamps. Most records corresponding to hour 1 of the Form have a timestamp with hour 1 as T1. About two thirds of the records in the hour 24 location of the form have a timestamp with an hour reported as T24 while the remaining third report this as T00 of the next day. T24 is not a valid format for the hour of a datetime, so we convert these T24 hours into T00 of the next day. A smaller subset of the respondents reports the 24th hour as the last second of the day - we also convert these records to the T00 of the next day.

Additional Details
^^^^^^^^^^^^^^^^^^
This table includes three respondent ID columns: one from the CSV raw source, one from the XBRL raw source and another that is PUDL-derived that links those two source ID's together. This table has filled in source IDs for all records so you can select the full timeseries for a given respondent from any of these three IDs.
""",
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_id_ferc714_csv",
                "respondent_id_ferc714_xbrl",
                "report_date",
                "datetime_utc",
                "timezone",
                "demand_mwh",
            ],
            "primary_key": ["respondent_id_ferc714", "datetime_utc"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
        "create_database_schema": False,
    },
    "out_ferc714__respondents_with_fips": {
        "original_description": """
Annual respondents with the county FIPS IDs for their service territories.
""",
        "description": """
[if timeseries] Time series of {short_description}.
[if assn] Links {source1} to {source2}.
[if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for FERC 714 respondents, annotated with the county FIPS IDs for their service territories.
[else] {Concise verb/noun indicating type} {short_description}.

This table has no primary key. Each row represents a respondent's information for a particular report date.

Derived from ferc714.
""",
        "schema": {
            "fields": [
                "eia_code",
                "respondent_type",
                "respondent_id_ferc714",
                "respondent_name_ferc714",
                "report_date",
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
                "utility_id_eia",
                "utility_name_eia",
                "state",
                "county",
                "state_id_fips",
                "county_id_fips",
            ]
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "outputs",
    },
}
