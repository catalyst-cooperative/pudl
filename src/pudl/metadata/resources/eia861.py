"""Definitions of data tables primarily coming from EIA-861."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia861__yearly_advanced_metering_infrastructure": {
        "description": "The data contain number of meters from automated meter readings (AMR) and advanced metering infrastructure (AMI) by state, sector, and balancing authority. The energy served (in megawatthours) for AMI systems is provided. Form EIA-861 respondents also report the number of standard meters (non AMR/AMI) in their system. Historical Changes: We started collecting the number of standard meters in 2013. The monthly survey collected these data from January 2011 to January 2017.",
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
        "description": "Association table showing which combinations of state, balancing authority, and utilities were observed in the data each year.",
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
        "description": "Annual entity table for balancing authorities.",
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
        "description": (
            """Energy demand response programs by state, sector, and balancing
authority. We collect data for the number of customers enrolled, energy savings,
potential and actual peak savings, and associated costs.

The EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA."""
        ),
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
        "description": "The number of grid connected water heaters enrolled in demand response programs.",
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
        "description": (
            """The impact of energy efficiency and load management programs on total
energy sold (MWh) and peak demand (MW) by customer class. Includes incremental effects
(from new programs and new participants) as well as total annual effects (all programs
and participants in a given year) and potential effects (anticipated peak reduction for
load management programs). Also includes the cost of DSM programs and the number of
customers enrolled in price-responsive and time-responsive programs.

The raw EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA."""
        ),
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
        "description": (
            """Miscellaneous information from the EIA861 DSM table.
Includes boolean fields about whether the energy savings estimates/calculations were
independently verified and whether the utility runs time and or price responsive
programs. Also contains information on whether any of the respondent's DSM activities
are reported under another company, and if so which one.

The raw EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA."""
        ),
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
        "description": (
            """Electricity sales for resale and to ultimate customer.

The raw EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA."""
        ),
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
        "description": (
            """Information on the energy sources used for utility or customer-owned
distributed generation capacity.

The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units."""
        ),
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
        "description": (
            """Information on the capacity of utility or customer-owned distributed
generation. Includes the number of generators, whether the capacity is estimated or
actual, the amount of backup capacity, and how much capacity is from generators with
less than 1 MW of nameplate capacity.

The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units."""
        ),
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
        "description": (
            """Information on the technology used for utility or customer-owned
distributed generation.

The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units."""
        ),
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
        "description": "The number of distribution circuits and circuits with voltage optimization by state.",
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
        "description": "The number of customers enrolled in dynamic pricing programs by state, sector, and balancing authority. Respondents check if one or more customers are enrolled in time-of-use pricing, real time pricing, variable peak pricing, critical peak pricing, and critical peak rebates.",
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
        "description": (
            """Incremental energy savings, peak demand savings, weighted average life
cycle, and associated costs for the reporting year and life cycle of energy efficiency
programs.

The EIA861 demand-side management (DSM) table (split into three normalized tables in
PUDL) contain data through 2012. The form changed in 2013 and split the contents of the
DSM table into energy efficiency and demand response tables. Though similar, the
information collected before and after 2012 are not comparable enough to combine into a
singular, continuous table. We were discouraged from doing so after contacting a
representative from EIA."""
        ),
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
        "description": "Green pricing program revenue, sales, and customer count by sector and state.",
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
        "description": "Information about utility mergers and acquisitions.",
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
        "description": (
            """The amount of energy sold to back to the grid. From 2007 - 2009 the data
are reported as a lump sum of total energy dispatched by sector. After 2009, the data
are broken down by sector and technology type."""
        ),
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
        "description": "The PV current flow type for net metered capacity.",
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
        "description": (
            """The amount of non-net metered distributed generation by sector and
technology type.

The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units."""
        ),
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
        "description": (
            """Information on the capacity of utility or customer-owned distributed
generation. Includes the number of generators, pv current flow type, backup capacity
and utility owned capacity.

The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units."""
        ),
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
        "description": (
            """The annual megawatt hours (MWH) for all a utility's sources of
electricity and disposition of electricity listed. Sources include: net generation
purchases from electricity suppliers, exchanges received, exchanges delivered, exchanges
net, wheeled received, wheeled delivered, wheeled net, transmission by others, and
losses."""
        ),
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
        "description": (
            """A utility's revenue by type of electric operating revenue.
Includes electric operating revenue From sales to ultimate customers, revenue from
unbundled (delivery) customers, revenue from sales for resale, electric credits/other
adjustments, revenue from transmission, other electric operating revenue, and total
electric operating revenue."""
        ),
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
        "description": (
            """Standardized metrics of electricity system reliability and outage
impacts. Includes the system average interruption duration index (SAIDI), system average
interruption frequency index (SAIFI), and customer average interruption duration index
(CAIDI) aka SAIDI/SAIFI with and without major event days and loss of service. Includes
the standard (IEEE/other) and other relevant information."""
        ),
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
        "description": "Annual electricity sales to ultimate customers broken down by utility, balancing authority, state, and customer class.",
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
        "description": (
            """This is data extracted from Form EIA-861S, a shorter version of Form EIA-861.
The data started being reported in 2012. However, the 2019 data is not available.
They are expected to submit the completed Form EIA-861S to EIA by April 30th, following the end of the prior calendar year.
Utilities report on Form EIA-861S if they:

- Report less than 200,000 megawatthours on the last previous Form EIA-861.

- Provide only bundled service (generation and distribution).

- Are not needed to ensure acceptable quality of statistical estimates.

- Are not part of the aggregate TVA or WPPI.

- Do not report on Form EIA-861M."""
        ),
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
        "description": "County FIPS codes for counties composing utility service territories.",
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
        "description": "Association table indicating which states each utility reported data for by year.",
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
        "description": (
            """A table of boolean values indicating what kind of business activities each utility engages in

This includes whether they operate alternative fuel vehicles, whether they provide
transmission, distribution, or generation services (bundled or unbundled), and whether
they engage in wholesale and/or retail markets."""
        ),
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
        "description": "The NERC regions that a utility operates in.",
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
        "description": "The RTOs that a utility operates in.",
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
        "description": "County-level data about EIA-861 utility service territories.",
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
        "description": "County-level data about EIA-861 balancing authority service territories.",
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
