"""Definitions of data tables derived from the EIA-930."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia930__hourly_operations": {
        "description": {
            "additional_summary_text": "balancing authority net generation, interchange, and demand.",
            "additional_details_text": """Net generation represents the metered output of electric generating units in a BA's electric system. This generation only includes generating units that are managed by a BA or whose operations are visible to a BA.

Generators on the distribution system—both large-scale resources and small-scale distributed resources, such as rooftop solar photovoltaic (PV) systems—are typically not included.

In some electricity publications, EIA reports generation from all utility-scale
generating units in the United States. BAs only meter generating units that are from a subset of all utility-scale generating units. As a result, when hourly generation from the EIA-930 is aggregated to monthly or annual levels, the results will be lower than monthly and annual aggregations in other EIA electricity publications.

Interchange is the net metered tie line flow from one BA to another directly
interconnected BA. This table includes the net sum of all interchange occurring between a BA and all of its directly interconnected neighboring BAs. For a detailed breakdown of interchange between each pair of adjacent BAs see
core_eia930__hourly_interchange.

Negative interchange values indicate net inflows, and positive interchange values indicate net outflows.

Demand is a calculated value representing the amount of electricity load within a BA's electric system. A BA derives its demand value by taking the total metered net electricity generation within its electric system and subtracting the total metered net electricity interchange occurring between the BA and its neighboring BAs.

Each BA produces a day-ahead electricity demand forecast for every hour of the next day. These forecasts help BAs plan for and coordinate the reliable operation of their electric system.""",
        },
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
        "description": {
            "additional_summary_text": "balancing authority net generation by energy source.",
            "usage_warnings": ["known_discrepancies"],
            "additional_details_text": """BAs report generation from dual-fuel (switching from one fuel to another) and
multiple-fuel (using multiple fuels simultaneously) generators under the actual energy source used, if known, and under the generator's primary energy source, if not known.

To maintain generator confidentiality, generation may sometimes be reported in the Other category if too few generators are reported for a particular energy source category.

In theory, the sum of net generation across all energy sources should equal the total net generation reported in the balancing authority operations table. In practice,
there are many cases in which these values diverge significantly, which require further investigation.""",
        },
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
        "description": {
            "additional_summary_text": "balancing authority interchange.",
            "additional_details_text": """Interchange is the net metered tie line flow from one BA to another directly
interconnected BA. Total net interchange is the net sum of all interchange occurring between a BA and its directly interconnected neighboring BAs.
Negative interchange values indicate net inflows, and positive interchange values indicate net outflows.""",
        },
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
        "description": {
            "additional_summary_text": "balancing authority subregion demand.",
            "additional_details_text": """For select BAs, balancing authority subregion demand provides a further level of geographic disaggregation (for example, load zones, weather zones, operating company areas, local BAs, etc.) within a BA's electric system.""",
        },
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
        "description": {
            "additional_summary_text": "balancing authority net generation, interchange, and demand with imputed demand.",
            "additional_details_text": """This table is based on ``core_eia930__hourly_operations``,
but adds imputed demand where the original data was missing or anomalous. Codes
explaining why values have been imputed can be found in the
``core_pudl__codes_imputation_reasons`` table.
""",
            "usage_warnings": ["imputed_values", "experimental_wip"],
        },
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
        "description": {
            "additional_summary_text": "aggregated balancing authority demand by EIA region, interconnect, and continental US.",
            "additional_details_text": """The spatial granularity of each record is indicated by `aggregation_level`.""",
            "usage_warnings": ["aggregation_hazard", "experimental_wip"],
        },
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
        "description": {
            "additional_summary_text": "balancing authority subregion demand with imputed demand.",
            "additional_details_text": """This table is based on ``core_eia930__hourly_subregion_demand``, but adds imputed demand where the original data was missing or anomalous. Codes explaining why values have been imputed can be found in the ``core_pudl__codes_imputation_reasons`` table.""",
            "usage_warnings": ["imputed_values", "experimental_wip"],
        },
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
