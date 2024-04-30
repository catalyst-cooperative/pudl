"""Table definitions for data coming from the GridPath Resource Adequacy Toolkit."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "out_gridpathratoolkit__hourly_available_capacity_factor": {
        "description": (
            "Hourly capacity factors defining the capacity available from an "
            "aggregated group of generators, stated as a fraction of the aggregate "
            "nameplate capacity of the group. "
            "This table contains a mix of profiles representing whole regions and "
            "individual plants, where the individual plants are hybrid wind or solar "
            "plus electricity storage facilities. For the hybrid facilities the "
            "capacity factor represents the available output of only the renewable "
            "generators. Estimates of the generation profiles have been extended "
            "across a longer range of dates than the original data. For wind, the "
            "years 2015-2020 are synthesized data and for solar 2020 is synthesized. "
            "See the Appendix of the GridPath Resource Adequacy Toolkit report for "
            "details."
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "aggregation_group",
                "capacity_factor",
            ],
            "primary_key": ["datetime_utc", "aggregation_group"],
        },
        "sources": ["gridpathratoolkit"],
        "field_namespace": "gridpathratoolkit",
        "etl_group": "gridpathratoolkit",
        "create_database_schema": False,
    },
    "core_gridpathratoolkit__assn_generator_aggregation_group": {
        "description": (
            "This table defines which individual generator profiles are combined when "
            "creating aggregated capacity factors / generation profiles. Generator "
            "capacity is used to weight the contribution of each generator in the "
            "resulting aggregated profiles, and is made available in this table for "
            "convenience and legibility. The resulting aggregated profiles are stored "
            "in :ref:`out_gridpathratoolkit__hourly_available_capacity_factor`."
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "aggregation_group",
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
