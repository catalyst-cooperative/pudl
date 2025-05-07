"""Definitions of data tables derived from the EIA-930."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia930__hourly_operations": {
        "description": (
            """Hourly balancing authority net generation, interchange, and demand.

Net generation represents the metered output of electric generating units in a BA's
electric system. This generation only includes generating units that are managed by a BA
or whose operations are visible to a BA.

Generators on the distribution system—both large-scale resources and small-scale
distributed resources, such as rooftop solar photovoltaic (PV) systems—are typically not
included.

In some electricity publications, EIA reports generation from all utility-scale
generating units in the United States. BAs only meter generating units that are from a
subset of all utility-scale generating units. As a result, when hourly generation from
the EIA-930 is aggregated to monthly or annual levels, the results will be lower than
monthly and annual aggregations in other EIA electricity publications.

Interchange is the net metered tie line flow from one BA to another directly
interconnected BA. This table includes the net sum of all interchange occurring between
a BA all of its directly interconnected neighboring BAs. For a detailed breakdown of
interchange between each pair of adjacent BAs see
:ref:`core_eia930__hourly_interchange`.

Negative interchange values indicate net inflows, and positive interchange values
indicate net outflows.

Demand is a calculated value representing the amount of electricity load within a BA's
electric system. A BA derives its demand value by taking the total metered net
electricity generation within its electric system and subtracting the total metered net
electricity interchange occurring between the BA and its neighboring BAs.

Each BA produces a day-ahead electricity demand forecast for every hour of the next day.
These forecasts help BAs plan for and coordinate the reliable operation of their
electric system."""
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "net_generation_reported_mwh",
                "net_generation_adjusted_mwh",
                "net_generation_imputed_eia_mwh",
                "interchange_reported_mwh",
                "interchange_adjusted_mwh",
                "interchange_imputed_eia_mwh",
                "demand_reported_mwh",
                "demand_adjusted_mwh",
                "demand_imputed_eia_mwh",
                "demand_forecast_mwh",
            ],
            "primary_key": [
                "datetime_utc",
                "balancing_authority_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
    "core_eia930__hourly_net_generation_by_energy_source": {
        "description": (
            """EIA-930 hourly balancing authority net generation by energy source.

BAs report generation from dual-fuel (switching from one fuel to another) and
multiple-fuel (using multiple fuels simultaneously) generators under the actual energy
source used, if known, and under the generator's primary energy source, if not known.

To maintain generator confidentiality, generation may sometimes be reported in the Other
category if too few generators are reported for a particular energy source category.

In theory the sum of net generation across all energy sources should equal the total
net generation reported in the balancing authority operations table. In practice, there
are many cases in which these values diverge significantly, which require further
investigation."""
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "generation_energy_source",
                "net_generation_reported_mwh",
                "net_generation_adjusted_mwh",
                "net_generation_imputed_eia_mwh",
            ],
            "primary_key": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "generation_energy_source",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
    "core_eia930__hourly_interchange": {
        "description": """Hourly balancing authority interchange.

Interchange is the net metered tie line flow from one BA to another directly
interconnected BA. Total net interchange is the net sum of all interchange occurring
between a BA and its directly interconnected neighboring BAs.

Negative interchange values indicate net inflows, and positive interchange values
indicate net outflows.""",
        "schema": {
            "fields": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "balancing_authority_code_adjacent_eia",
                "interchange_reported_mwh",
            ],
            "primary_key": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "balancing_authority_code_adjacent_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
    "core_eia930__hourly_subregion_demand": {
        "description": (
            """EIA-930 hourly balancing authority subregion demand.

For select BAs, demand by subregion data provide demand at a further level of geographic
disaggregation (for example, load zones, weather zones, operating company areas, local
BAs, etc.) within a BA's electric system. A BA's reporting demand by subregion section
below provides more information on subregions."""
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "balancing_authority_subregion_code_eia",
                "demand_reported_mwh",
            ],
            "primary_key": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "balancing_authority_subregion_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
    "out_eia930__hourly_operations": {
        "description": (
            """EXPERIMENTAL / WORK-IN-PROGRESS, 2025-04-04.

This adds imputed demand to the ``core_eia930__hourly_operations`` table where the
original data was missing or anomalous.  Codes explaining why values have been imputed
can be found in the ``core_pudl__codes_imputation_reasons`` table.

This table is available in the nightly builds during development, but has not been fully
vetted yet."""
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "net_generation_reported_mwh",
                "net_generation_adjusted_mwh",
                "net_generation_imputed_eia_mwh",
                "interchange_reported_mwh",
                "interchange_adjusted_mwh",
                "interchange_imputed_eia_mwh",
                "demand_reported_mwh",
                "demand_adjusted_mwh",
                "demand_imputed_pudl_mwh",
                "demand_imputed_pudl_mwh_imputation_code",
                "demand_imputed_eia_mwh",
                "demand_forecast_mwh",
            ],
            "primary_key": [
                "datetime_utc",
                "balancing_authority_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
    "out_eia930__hourly_aggregated_demand": {
        "description": (
            """EXPERIMENTAL / WORK-IN-PROGRESS, 2025-04-04.

This aggregates imputed demand from the ``out_eia930__hourly_operations`` table.
It aggregates demand to the level of EIA regions, interconnects, and the contiguous
United States. The spatial granularity of each record is indicated by
``aggregation_level``."""
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "aggregation_level",
                "aggregation_group",
                "demand_imputed_pudl_mwh",
            ],
            "primary_key": [
                "datetime_utc",
                "aggregation_level",
                "aggregation_group",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
    "out_eia930__hourly_subregion_demand": {
        "description": (
            """EXPERIMENTAL / WORK-IN-PROGRESS, 2025-03-31.

This table is based on ``core_eia930__hourly_subregion_demand``, but adds imputed demand
values where the original data was missing or anomalous.  Codes explaining why values
have been imputed can be found in the ``core_pudl__codes_imputation_reasons`` table.

This table is available in the nightly builds during development, but has not been fully
vetted yet."""
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "balancing_authority_subregion_code_eia",
                "demand_reported_mwh",
                "demand_imputed_pudl_mwh",
                "demand_imputed_pudl_mwh_imputation_code",
            ],
            "primary_key": [
                "datetime_utc",
                "balancing_authority_code_eia",
                "balancing_authority_subregion_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
        "create_database_schema": False,
    },
}
