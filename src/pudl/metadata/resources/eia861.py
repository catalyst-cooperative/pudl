"""Definitions of data tables primarily coming from EIA-861."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia861__yearly_advanced_metering_infrastructure": {
        "description": {
            "additional_summary_text": "advanced metering infrastructure (AMI) and automated meter reading (AMR) by state, sector, and balancing authority.",
            "additional_details_text": """The energy served (in MWH) for AMI systems is provided. Form EIA-861 respondents also report the number
of standard meters (non AMR/AMI) in their system. Historical Changes: We started collecting the number of standard meters in 2013. The monthly survey collected
these data from January 2011 to January 2017.""",
        },
        "schema": {
            "fields": [
                "advanced_metering_infrastructure",
                "automated_meter_reading",
                "balancing_authority_code_eia",
                "customer_class",
                "daily_digital_access_customers",
                "direct_load_control_customers",
                "energy_served_ami_mwh",
                "entity_type",
                "home_area_network",
                "non_amr_ami",
                "report_date",
                "short_form",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
            "primary_key": [
                "balancing_authority_code_eia",
                "customer_class",
                "report_date",
                "state",
                "utility_id_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__assn_balancing_authority": {
        "description": {
            "additional_summary_text": "state, balancing authority, and utility in a given year."
        },
        "schema": {
            "fields": [
                "report_date",
                "balancing_authority_id_eia",
                "utility_id_eia",
                "state",
            ],
            "primary_key": [
                "report_date",
                "balancing_authority_id_eia",
                "utility_id_eia",
                "state",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_balancing_authority": {
        "description": {
            "additional_summary_text": "balancing authorities.",
        },
        "schema": {
            "fields": [
                "report_date",
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
            ],
            "primary_key": [
                "report_date",
                "balancing_authority_id_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_demand_response": {
        "description": {
            "additional_summary_text": "demand response programs by state, sector, and balancing authority.",
            "additional_details_text": """The EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA.""",
        },
        "schema": {
            "fields": [
                "actual_peak_demand_savings_mw",
                "balancing_authority_code_eia",
                "customer_class",
                "customer_incentives_cost",
                "customers",
                "energy_savings_mwh",
                "other_costs",
                "potential_peak_demand_savings_mw",
                "report_date",
                "short_form",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
            "primary_key": [
                "balancing_authority_code_eia",
                "customer_class",
                "report_date",
                "state",
                "utility_id_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_demand_response_water_heater": {
        "description": {
            "additional_summary_text": "grid-connected water heaters enrolled in demand response programs."
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "report_date",
                "state",
                "utility_id_eia",
                "num_water_heaters",
                "data_maturity",
            ],
            "primary_key": [
                "balancing_authority_code_eia",
                "report_date",
                "state",
                "utility_id_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_demand_side_management_ee_dr": {
        "description": {
            "additional_summary_text": "The impact of energy efficiency and load management programs on total energy sold (MWh) and peak demand (MW) by customer class.",
            "usage_warnings": ["discontinued_data"],
            "additional_details_text": """The raw EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA.""",
        },
        "schema": {
            "fields": [
                "annual_indirect_program_cost",
                "annual_total_cost",
                "customer_class",
                "energy_efficiency_annual_actual_peak_reduction_mw",
                "energy_efficiency_annual_cost",
                "energy_efficiency_annual_effects_mwh",
                "energy_efficiency_annual_incentive_payment",
                "energy_efficiency_incremental_actual_peak_reduction_mw",
                "energy_efficiency_incremental_effects_mwh",
                "load_management_annual_actual_peak_reduction_mw",
                "load_management_annual_cost",
                "load_management_annual_effects_mwh",
                "load_management_annual_incentive_payment",
                "load_management_annual_potential_peak_reduction_mw",
                "load_management_incremental_actual_peak_reduction_mw",
                "load_management_incremental_effects_mwh",
                "load_management_incremental_potential_peak_reduction_mw",
                "nerc_region",
                "price_responsiveness_customers",
                "report_date",
                "state",
                "time_responsiveness_customers",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_demand_side_management_misc": {
        "description": {
            "additional_summary_text": "demand-side management (DSM) program information.",
            "usage_warnings": ["discontinued_data"],
            "additional_details_text": """Includes boolean fields about whether the energy savings estimates/calculations were
independently verified and whether the utility runs time and or price responsive
programs. Also contains information on whether any of the respondent's DSM activities
are reported under another company, and if so which one.

The raw EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA.""",
        },
        "schema": {
            "fields": [
                "energy_savings_estimates_independently_verified",
                "energy_savings_independently_verified",
                "entity_type",
                "major_program_changes",
                "nerc_region",
                "price_responsive_programs",
                "report_date",
                "reported_as_another_company",
                "short_form",
                "state",
                "has_time_responsive_programs",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_demand_side_management_sales": {
        "description": {
            "additional_summary_text": "electricity sales related to demand-side management (DSM).",
            "usage_warnings": ["discontinued_data"],
            "additional_details_text": """The raw EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA.""",
        },
        "schema": {
            "fields": [
                "nerc_region",
                "report_date",
                "sales_for_resale_mwh",
                "sales_to_ultimate_consumers_mwh",
                "state",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_distributed_generation_fuel": {
        "description": {
            "additional_summary_text": "the energy sources used for utility or customer-owned distributed generation capacity.",
            "additional_details_text": """The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units.""",
        },
        "schema": {
            "fields": [
                "estimated_or_actual_fuel_data",
                "fuel_class",
                "fuel_pct",
                "report_date",
                "state",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_distributed_generation_misc": {
        # TODO: might want to rename this table to be _capacity
        "description": {
            "additional_summary_text": "the capacity and quantity of utility or customer-owned distributed generation.",
            "additional_details_text": """The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units.""",
        },
        "schema": {
            "fields": [
                "backup_capacity_mw",
                "distributed_generation_owned_capacity_mw",
                "estimated_or_actual_capacity_data",
                "generators_num_less_1_mw",
                "generators_number",
                "report_date",
                "state",
                "total_capacity_less_1_mw",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_distributed_generation_tech": {
        "description": {
            "additional_summary_text": "the technology used for utility or customer-owned distributed generation.",
            "additional_details_text": """The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units.""",
        },
        "schema": {
            "fields": [
                "capacity_mw",
                "estimated_or_actual_tech_data",
                "report_date",
                "state",
                "tech_class",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_distribution_systems": {
        "description": {
            "additional_summary_text": "distribution circuits and circuits with voltage optimization by state."
        },
        "schema": {
            "fields": [
                "circuits_with_voltage_optimization",
                "distribution_circuits",
                "report_date",
                "short_form",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_dynamic_pricing": {
        "description": {
            "additional_summary_text": " enrollment in dynamic pricing programs by state, sector, and balancing authority.",
            "additional_details_text": """Respondents check if one or more customers are enrolled in time-of-use pricing, real
time pricing, variable peak pricing, critical peak pricing, and critical peak rebates.""",
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "critical_peak_pricing",
                "critical_peak_rebate",
                "customer_class",
                "customers",
                "real_time_pricing",
                "report_date",
                "short_form",
                "state",
                "time_of_use_pricing",
                "utility_id_eia",
                "utility_name_eia",
                "variable_peak_pricing",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_energy_efficiency": {
        "description": {
            "additional_summary_text": """incremental energy savings, peak demand savings, weighted average life
cycle, and associated costs for the reporting year and life cycle of energy efficiency
programs.""",
            "additional_details_text": """The EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA.""",
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "customer_class",
                "customer_incentives_incremental_cost",
                "customer_incentives_incremental_life_cycle_cost",
                "customer_other_costs_incremental_life_cycle_cost",
                "incremental_energy_savings_mwh",
                "incremental_life_cycle_energy_savings_mwh",
                "incremental_life_cycle_peak_reduction_mw",
                "incremental_peak_reduction_mw",
                "other_costs_incremental_cost",
                "report_date",
                "short_form",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "weighted_average_life_years",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_green_pricing": {
        "description": {
            "additional_summary_text": "green pricing program revenue, sales, and customer count by sector and state.",
            "usage_warnings": ["discontinued_data"],
        },
        "schema": {
            "fields": [
                "customer_class",
                "customers",
                "green_pricing_revenue",
                "rec_revenue",
                "rec_sales_mwh",
                "report_date",
                "sales_mwh",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_mergers": {
        "description": {
            "additional_summary_text": "utility mergers and acquisitions.",
        },
        "schema": {
            "fields": [
                "entity_type",
                "merge_address",
                "merge_city",
                "merge_company",
                "merge_date",
                "merge_state",
                "new_parent",
                "report_date",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "zip_code",
                "zip_code_4",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_net_metering_customer_fuel_class": {
        "description": {
            "additional_summary_text": "net metering by customer and fuel class.",
            "usage_warnings": ["irregular_years"],
            "additional_details_text": """The amount of energy sold to back to the grid. From 2007 - 2009 the data
are reported as a lump sum of total energy dispatched by sector. After 2009, the data
are broken down by sector and technology type.""",
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "capacity_mw",
                "energy_capacity_mwh",
                "customer_class",
                "customers",
                "report_date",
                "short_form",
                "sold_to_utility_mwh",
                "state",
                "tech_class",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_net_metering_misc": {
        # TODO: I feel skeptical that the pv_current_flow_type field shouldn't be linked to the other net metering table.
        "description": {
            "additional_summary_text": "PV current flow type for net metered capacity.",
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "pv_current_flow_type",
                "report_date",
                "state",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_non_net_metering_customer_fuel_class": {
        "description": {
            "additional_summary_text": "non-net metered distributed generation by sector and technology type.",
            "additional_details_text": """The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units.""",
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "capacity_mw",
                "energy_capacity_mwh",
                "customer_class",
                "report_date",
                "state",
                "tech_class",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_non_net_metering_misc": {
        "description": {
            "additional_summary_text": "non-net metered distributed generation generators, pv current flow type, backup capacity and utility owned capacity.",
            "additional_details_text": """The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units.""",
        },
        "schema": {
            "fields": [
                "backup_capacity_mw",
                "balancing_authority_code_eia",
                "generators_number",
                "pv_current_flow_type",
                "report_date",
                "state",
                "utility_id_eia",
                "utility_owned_capacity_mw",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_operational_data_misc": {
        # TODO: misc might be a misleading name
        "description": {
            "additional_summary_text": "megawatt hours (MWH) for all a utility's sources of electricity and disposition of electricity listed.",
            "additional_details_text": (
                """Sources include: net generation
purchases from electricity suppliers, exchanges received, exchanges delivered, exchanges
net, wheeled received, wheeled delivered, wheeled net, transmission by others, and
losses."""
            ),
        },
        "schema": {
            "fields": [
                "consumed_by_facility_mwh",
                "consumed_by_respondent_without_charge_mwh",
                "data_observed",
                "entity_type",
                "exchange_energy_delivered_mwh",
                "exchange_energy_received_mwh",
                "furnished_without_charge_mwh",
                "nerc_region",
                "net_generation_mwh",
                "net_power_exchanged_mwh",
                "net_wheeled_power_mwh",
                "report_date",
                "retail_sales_mwh",
                "sales_for_resale_mwh",
                "short_form",
                "state",
                "summer_peak_demand_mw",
                "total_disposition_mwh",
                "total_energy_losses_mwh",
                "total_sources_mwh",
                "transmission_by_other_losses_mwh",
                "utility_id_eia",
                "utility_name_eia",
                "wheeled_power_delivered_mwh",
                "wheeled_power_received_mwh",
                "wholesale_power_purchases_mwh",
                "winter_peak_demand_mw",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_operational_data_revenue": {
        "description": {
            "additional_summary_text": "utility revenue by type of electric operating revenue.",
            "additional_details_text": """A utility's revenue by type of electric operating revenue.
Includes electric operating revenue From sales to ultimate customers, revenue from
unbundled (delivery) customers, revenue from sales for resale, electric credits/other
adjustments, revenue from transmission, other electric operating revenue, and total
electric operating revenue.""",
        },
        "schema": {
            "fields": [
                "nerc_region",
                "report_date",
                "revenue",
                "revenue_class",
                "state",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_reliability": {
        "description": {
            "additional_summary_text": "electricity system reliability and outage impacts.",
            "additional_details_text": """Includes the system average interruption duration index (SAIDI), system average
interruption frequency index (SAIFI), and customer average interruption duration index
(CAIDI) aka SAIDI/SAIFI with and without major event days and loss of service. Includes
the standard (IEEE/other) and other relevant information.""",
        },
        "schema": {
            "fields": [
                "caidi_w_major_event_days_minus_loss_of_service_minutes",
                "caidi_w_major_event_days_minutes",
                "caidi_wo_major_event_days_minutes",
                "customers",
                "entity_type",
                "highest_distribution_voltage_kv",
                "inactive_accounts_included",
                "momentary_interruption_definition",
                "outages_recorded_automatically",
                "report_date",
                "saidi_w_major_event_days_minus_loss_of_service_minutes",
                "saidi_w_major_event_days_minutes",
                "saidi_wo_major_event_days_minutes",
                "saifi_w_major_event_days_customers",
                "saifi_w_major_event_days_minus_loss_of_service_customers",
                "saifi_wo_major_event_days_customers",
                "short_form",
                "standard",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_sales": {
        "description": {
            "additional_summary_text": "electricity sales to ultimate customers by utility, balancing authority, state, and customer class."
        },
        "schema": {
            "fields": [
                "utility_id_eia",
                "state",
                "report_date",
                "balancing_authority_code_eia",
                "customer_class",
                "business_model",
                "data_observed",
                "entity_type",
                "service_type",
                "short_form",
                "utility_name_eia",
                "customers",
                "sales_mwh",
                "sales_revenue",
                "data_maturity",
            ],
            "primary_key": [
                "utility_id_eia",
                "state",
                "report_date",
                "balancing_authority_code_eia",
                "customer_class",
                "business_model",
                "service_type",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_short_form": {
        "description": {
            "additional_summary_text": "data from the short form (EIA-861S).",
            "usage_warnings": ["missing_years"],
            "additional_details_text": """The data started being reported in 2012. However, the 2019 data is not available.
They are expected to submit the completed Form EIA-861S to EIA by April 30th, following the end of the prior calendar year.
Utilities report on Form EIA-861S if they:

- Report less than 200,000 megawatthours on the last previous Form EIA-861.

- Provide only bundled service (generation and distribution).

- Are not needed to ensure acceptable quality of statistical estimates.

- Are not part of the aggregate TVA or WPPI.

- Do not report on Form EIA-861M.""",
        },
        "schema": {
            "fields": [
                "report_date",
                "utility_id_eia",
                "utility_name_eia",
                "entity_type",
                "state",
                "balancing_authority_code_eia",
                "sales_revenue",
                "sales_mwh",
                "customers",
                "has_net_metering",
                "has_demand_side_management",
                "has_time_responsive_programs",
                "has_green_pricing",
                "data_maturity",
            ],
            "primary_key": [
                "utility_id_eia",
                "state",
                "report_date",
                "balancing_authority_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_service_territory": {
        "description": {
            "additional_summary_text": "counties in utility service territories."
        },
        "schema": {
            "fields": [
                "county",
                "short_form",
                "state",
                "utility_id_eia",
                "utility_name_eia",
                "report_date",
                "state_id_fips",
                "county_id_fips",
                "data_maturity",
            ],
            "primary_key": [
                "report_date",
                "utility_id_eia",
                "county_id_fips",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__assn_utility": {
        "description": {
            "additional_summary_text": "utility and state in a given year."
        },
        "schema": {
            "fields": [
                "report_date",
                "utility_id_eia",
                "state",
            ],
            "primary_key": [
                "report_date",
                "utility_id_eia",
                "state",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_utility_data_misc": {
        "description": {
            "additional_summary_text": "utility business activities.",
            "additional_details_text": """This includes whether they operate alternative fuel vehicles, whether they provide
transmission, distribution, or generation services (bundled or unbundled), and whether
they engage in wholesale and/or retail markets.""",
        },
        "schema": {
            "fields": [
                "alternative_fuel_vehicle_2_activity",
                "alternative_fuel_vehicle_activity",
                "bundled_activity",
                "buying_distribution_activity",
                "buying_transmission_activity",
                "distribution_activity",
                "entity_type",
                "generation_activity",
                "nerc_region",
                "operates_generating_plant",
                "report_date",
                "retail_marketing_activity",
                "short_form",
                "state",
                "transmission_activity",
                "utility_id_eia",
                "utility_name_eia",
                "wholesale_marketing_activity",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_utility_data_nerc": {
        "description": {
            "additional_summary_text": "the NERC regions that utilities operate in.",
        },
        "schema": {
            "fields": [
                "nerc_region",
                "nerc_regions_of_operation",
                "report_date",
                "state",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "core_eia861__yearly_utility_data_rto": {
        "description": {
            "additional_summary_text": "the RTOs that utilities operate in.",
        },
        "schema": {
            "fields": [
                "nerc_region",
                "report_date",
                "rtos_of_operation",
                "state",
                "utility_id_eia",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia861"],
        "etl_group": "eia861",
    },
    "out_eia861__yearly_utility_service_territory": {
        "description": {
            "additional_summary_text": "counties in utility service territories.",
            "additional_details_text": "Contains additional information about counties.",
        },
        "schema": {
            "fields": [
                "county_id_fips",
                "county_name_census",
                "population",
                "area_km2",
                "report_date",
                "utility_id_eia",
                "state",
                "county",
                "state_id_fips",
            ],
            "primary_key": ["utility_id_eia", "report_date", "county_id_fips"],
        },
        "sources": ["eia861", "censusdp1"],
        "field_namespace": "eia",
        "etl_group": "service_territories",
    },
    "out_eia861__yearly_balancing_authority_service_territory": {
        "description": {
            "additional_summary_text": "counties in balancing authority service territories.",
        },
        "schema": {
            "fields": [
                "county_id_fips",
                "county_name_census",
                "population",
                "area_km2",
                "report_date",
                "balancing_authority_id_eia",
                "state",
                "county",
                "state_id_fips",
            ],
            "primary_key": [
                "balancing_authority_id_eia",
                "report_date",
                "county_id_fips",
                "county",
            ],
        },
        "sources": ["eia861", "censusdp1"],
        "field_namespace": "eia",
        "etl_group": "service_territories",
    },
}
