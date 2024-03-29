"""Table definitions for data coming from the GridPath Resource Adequacy Toolkit."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_gridpathratoolkit__hourly_aggregated_extended_capacity_factors": {
        "description": (
            "Hourly capacity factors defining the generation profiles for solar and "
            "wind. Contains a mix of profiles representing whole regions and "
            "individual plants, where the individual plants are hybrid wind or solar "
            "plus electricity storage facilities. For the hybrid facilities the "
            "capacity factor represents the available output of only the renewable "
            "generators. Estimates of the generation profiles have been extended "
            "across a longer range of dates than the original data. See the Appendix "
            "of the GridPath Resource Adequacy Toolkit report for more details."
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "aggregation_key",
                "capacity_factor",
            ],
            "primary_key": ["datetime_utc", "aggregation_key"],
        },
        "sources": ["gridpathratoolkit"],
        "field_namespace": "gridpathratoolkit",
        "etl_group": "gridpathratoolkit",
    },
    "core_gridpathratoolkit__capacity_factor_aggregations": {
        "description": (
            "This table defines which individual generator profiles are combined when "
            "creating aggregated capacity factors / generation profiles. Generator "
            "capacity is used to weight the contribution of each generator in the "
            "resulting aggregated profiles."
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "aggregation_key",
                "capacity_mw",
                "include_generator",
            ],
            "primary_key": ["plant_id_eia", "generator_id"],
        },
        "sources": ["gridpathratoolkit"],
        "field_namespace": "gridpathratoolkit",
        "etl_group": "gridpathratoolkit",
    },
}
