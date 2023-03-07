"""Definitions for the glue/crosswalk tables that connect data groups."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "epacamd_eia": {
        "description": """This crosswalk table comes from the
EPA's Github repo camd-eia-crosswalk:
https://github.com/USEPA/camd-eia-crosswalk.
It's purpose is to connect EPA units with EIA plants, boilers, and generators.
The camd-eia-crosswalk README and our Data Source documentation page on
:doc:`../data_sources/epacems` depict the complicated relationship between EIA and EPA
data, specifically the nature of EPA vs. EIA "units" and the level of granularity that
one can connect the two sources.

The crosswalk table is generated using EIA data from 2018 meaning that any plants that
have shifted before or since then aren't accurately reflected in the data. We're hoping
to create a temporal version of the crosswalk at some point.

Our version of the crosswalk clarifies some of the column names and removes unmatched
rows. The :func:`pudl.glue.epacamd_eia.transform` function doc strings explain what
changes are made from the EPA's version.
""",
        "schema": {
            "fields": [
                "plant_id_epa",
                "emissions_unit_id_epa",
                "generator_id_epa",
                "plant_id_eia",
                "boiler_id",
                "generator_id",
            ],
        },
        "field_namespace": "glue",
        "etl_group": "glue",
        "sources": ["epacamd_eia"],
    },
}
"""PUDL-specifiic resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
