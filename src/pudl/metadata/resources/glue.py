"""Definitions for the glue/crosswalk tables that connect data groups."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_epa__assn_eia_epacamd": {
        "description": """This crosswalk table comes from the
PUDL fork of the EPA camd-eia-crosswalk Github repo:
https://github.com/catalyst-cooperative/camd-eia-crosswalk-latest.
It's purpose is to connect EPA units with EIA plants, boilers, and generators.
The camd-eia-crosswalk README and our Data Source documentation page on
:doc:`../data_sources/epacems` depict the complicated relationship between EIA and EPA
data, specifically the nature of EPA vs. EIA "units" and the level of granularity that
one can connect the two sources.

The original EPA crosswalk runs on 2018 EIA data. We adapted the crosswalk code to run on
each new year of EIA data, capturing changes in plant information over time.

Our version of the crosswalk clarifies some of the column names and removes unmatched
rows. The :func:`pudl.etl.glue_assets.core_epa__assn_eia_epacamd` function doc strings explain
what changes are made from the EPA's version.""",
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
    "core_epa__assn_eia_epacamd_subplant_ids": {
        "description": """This table is an augmented version of the core_epa__assn_eia_epacamd
crosswalk table which initially comes from the EPA's Github repo camd-eia-crosswalk:
https://github.com/USEPA/camd-eia-crosswalk.
It's purpose is to connect EPA units with EIA units, and generators.

This table identifies subplants within plant_ids, which are the smallest coherent units
for aggregation. plant_id refers to a legal entity that often contains multiple distinct
power plants, even of different technology or fuel types.

EPA CEMS data combines information from several parts of a power plant:
* emissions from smokestacks
* fuel use from combustors
* electricity production from generators
But smokestacks, combustors, and generators can be connected in complex, many-to-many
relationships. This complexity makes attribution difficult for, as an example,
allocating pollution to energy producers. Furthermore, heterogeneity within plant_ids
make aggregation to the parent entity difficult or inappropriate.

This table inherits from the EPA's crosswalk, the ID's from EPA CAMD
core_epacems__hourly_emissions table itself, the core_eia860__assn_boiler_generator table and the
core_eia860__scd_generators table. While the core_epa__assn_eia_epacamd table is the core backbone of the table,
EPA CAMD id's ensure there is complete coverage of EPA CAMD reporting units. The EIA 860
table addition ensures there is also complete coverage of those units as well.

For more information about the how this subplant_id is made, see the documentation for
pudl.etl.glue_assets.make_subplant_ids and pudl.etl.glue_assets.update_subplant_ids
from: https://catalystcoop-pudl.readthedocs.io/en/nightly/autoapi/index.html

But by analyzing the relationships between combustors and generators,
as provided in the core_epa__assn_eia_epacamd crosswalk, we can identify distinct power plants.
These are the smallest coherent units of aggregation.

This table does not have primary keys because the primary keys would have been:
plant_id_eia, generator_id, subplant_id and emissions_unit_id_epa, but there are some
null records in the generator_id column because ~2 percent of all EPA CAMD records are not
successfully mapped to EIA generators.""",
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
