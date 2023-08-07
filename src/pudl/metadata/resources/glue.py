"""Definitions for the glue/crosswalk tables that connect data groups."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "epacamd_eia_subplant_ids": {
        "description": """This table is an augmented version of the epacamd_eia
crosswalk table, initally published in the EPA's Github repo camd-eia-crosswalk:
https://github.com/USEPA/camd-eia-crosswalk. The purpose of the crosswalk, and this
augmented table, is to connect EPA units with EIA units, and generators at the most
granular level possible.

The epacamd_eia crosswalk table is the backbone of this table. Our first augmentation is
to create a version of that crosswalk with the most recent data from EIA. The crosswalk
was only designed to intake one year's worth of data and is thus presented as dateless
upon publication. We run the crosswalk code with the new year's data and retain all
unique outputs from that previous outputs. The result is a table with no coorespondng
date.

We then merge in the ID's
from EPA CAMD hourly_emissions_epacems table as well as the
boiler_generator_assn_eia860 and generators_eia860 tables to ensure complete covereage
of relevant reporting units from both EPA and EIA.

EPA CEMS data combines information from several parts of a power plant:
* emissions from smokestacks
* fuel use from combustors
* electricty production from generators
But smokestacks, combustors, and generators can be connected in complex, many-to-many
relationships. This complexity makes attribution difficult for, as an example,
allocating pollution to energy producers. Furthermore, heterogeneity within plant_ids
make aggregation to the parent entity difficult or inappropriate. For more information
about the complex relationship between EPA and EIA data, see the EPA's
camd-eia-crosswalk repo README, linked above, or our Data Source documentation page on
:doc:`../data_sources/epacems`.

This table creates a subplant_id to identify the smallest coherent unit of aggregation
between EIA and EPA. Together, the subplant_id and plant_id_eia can be used to
identify, for instance, the smallest aggregation of specific EIA plant parts that can be
associated with reported emissions from CEMS. In cases where there is no
cooresponding EPA unit for a given EIA unit, a result of merging in all relevant EIA ids
during the first round of augmentation, the subplant_id reflects the same grouping as
the unit_id_pudl -- the smallest aggregation of EIA boilers and generators.

For more information about the how this subplant_id is made, see the documentation for
pudl.etl.glue_assets.make_subplant_ids and pudl.etl.glue_assets.update_subplant_ids
from: https://catalystcoop-pudl.readthedocs.io/en/latest/autoapi/index.html

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
                "boiler_id",
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
