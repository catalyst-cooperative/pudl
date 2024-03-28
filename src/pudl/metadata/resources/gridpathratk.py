"""Table definitions for data coming from the GridPath Resource Adequacy Toolkit."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_gridpathratk__aggregated_extended_profiles": {
        "description": (
            "Hourly capacity factors defining the generation profiles for solar and "
            "wind. Contains a mix of profiles representing whole regions and "
            "individual plants, where the individual plants are hybrid wind or solar "
            "plus electricity storage facilities. For the hybrid facilities the "
            "capacity factor represents the available output of only the renewable "
            "generators. Estimates of the generation profiles have been extended "
            "across a longer range of dates than the original data. See the Appendix "
            "the GridPath Resource Adequacy Toolkit report for more details."
        ),
        "schema": {
            "fields": [
                "utc_datetime",
                "aggregation_key",
                "capacity_factor",
            ],
            "primary_key": ["utc_datetime", "aggregation_key"],
        },
        "sources": ["gridpathratk"],
        "field_namespace": "gridpathratk",
        "etl_group": "gridpathratk",
    }
}
