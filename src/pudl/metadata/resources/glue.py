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
rows. The :func:`pudl.etl.glue_assets.epacamd_eia` function doc strings explain
what changes are made from the EPA's version.
""",
        "schema": {
            "fields": [
                "report_year",
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
    "epacamd_eia_subplant_ids": {
        "description": """This table is an augmented version of the epacamd_eia
crosswalk table which initally comes from the EPA's Github repo camd-eia-crosswalk:
https://github.com/USEPA/camd-eia-crosswalk.
It's purpose is to connect EPA units with EIA units, and generators.

This table identifies subplants within plant_ids, which are the smallest coherent units
for aggregation. plant_id refers to a legal entity that often contains multiple distinct
power plants, even of different technology or fuel types.

EPA CEMS data combines information from several parts of a power plant:
* emissions from smokestacks
* fuel use from combustors
* electricty production from generators
But smokestacks, combustors, and generators can be connected in complex, many-to-many
relationships. This complexity makes attribution difficult for, as an example,
allocating pollution to energy producers. Furthermore, heterogeneity within plant_ids
make aggregation to the parent entity difficult or inappropriate.

This table inherits from the EPA's crosswalk, the ID's from EPA CAMD
hourly_emissions_epacems table itself, the boiler_generator_assn_eia860 table and the
generators_eia860 table. While the epacamd_eia table is the core backbone of the table,
EPA CAMD id's ensure there is complete coverage of EPA CAMD reporting units. The EIA 860
table addition ensures there is also complete coverage of those units as well.

For more information about the how this subplant_id is made, see the documentation for
pudl.etl.glue_assets.make_subplant_ids and pudl.etl.glue_assets.update_subplant_ids
from: https://catalystcoop-pudl.readthedocs.io/en/latest/autoapi/index.html

But by analyzing the relationships between combustors and generators,
as provided in the epacamd_eia crosswalk, we can identify distinct power plants.
These are the smallest coherent units of aggregation.

This table does not have primary keys because the primary keys would have been:
plant_id_eia, generator_id, subplant_id and emissions_unit_id_epa, but there are some
null records in the generator_id column because ~2 percent of all EPA CAMD records are not
successfully mapped to EIA generators.
""",
        "schema": {
            "fields": [
                "plant_id_eia",
                "plant_id_epa",
                "subplant_id",
                "unit_id_pudl",
                "emissions_unit_id_epa",
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
