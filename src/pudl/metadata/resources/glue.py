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
    "ferc1_eia": {
        "description": "This table connects FERC1 plant tables to EIA's plant-parts via record linkage.",
        "schema": {
            "fields": [
                "record_id_ferc1",
                "record_id_eia",
                "match_type",
                "plant_name_ppe",
                "plant_part",
                "report_year",
                "report_date",
                "ownership_record_type",
                "plant_name_eia",
                "plant_id_eia",
                "generator_id",
                "unit_id_pudl",
                "prime_mover_code",
                "energy_source_code_1",
                "technology_description",
                "ferc_acct_name",
                "generator_operating_year",
                "utility_id_eia",
                "utility_id_pudl",
                "true_gran",
                "appro_part_label",
                "appro_record_id_eia",
                "record_count",
                "fraction_owned",
                "ownership_dupe",
                "operational_status",
                "operational_status_pudl",
                "plant_id_pudl",
                "total_fuel_cost_eia",
                "fuel_cost_per_mmbtu_eia",
                "net_generation_mwh_eia",
                "capacity_mw_eia",
                "capacity_factor_eia",
                "total_mmbtu_eia",
                "heat_rate_mmbtu_mwh_eia",
                "fuel_type_code_pudl_eia",
                "installation_year_eia",
                "plant_part_id_eia",
                "utility_id_ferc1",
                "utility_name_ferc1",
                "plant_id_ferc1",
                "plant_name_ferc1",
                "asset_retirement_cost",
                "avg_num_employees",
                "capacity_factor_ferc1",
                "capacity_mw_ferc1",
                "capex_annual_addition",
                "capex_annual_addition_rolling",
                "capex_annual_per_kw",
                "capex_annual_per_mw",
                "capex_annual_per_mw_rolling",
                "capex_annual_per_mwh",
                "capex_annual_per_mwh_rolling",
                "capex_equipment",
                "capex_land",
                "capex_per_mw",
                "capex_structures",
                "capex_total",
                "capex_wo_retirement_total",
                "construction_type",
                "construction_year",
                "installation_year_ferc1",
                "net_generation_mwh_ferc1",
                "not_water_limited_capacity_mw",
                "opex_allowances",
                "opex_boiler",
                "opex_coolants",
                "opex_electric",
                "opex_engineering",
                "opex_fuel",
                "fuel_cost_per_mwh",
                "opex_misc_power",
                "opex_misc_steam",
                "opex_nonfuel_per_mwh",
                "opex_operations",
                "opex_per_mwh",
                "opex_plant",
                "opex_production_total",
                "opex_rents",
                "opex_steam",
                "opex_steam_other",
                "opex_structures",
                "opex_total_nonfuel",
                "opex_transfer",
                "peak_demand_mw",
                "plant_capability_mw",
                "plant_hours_connected_while_generating",
                "plant_type",
                "water_limited_capacity_mw",
                "fuel_cost_per_mmbtu_ferc1",
                "fuel_type",
                "license_id_ferc1",
                "opex_maintenance",
                "opex_total",
                "capex_facilities",
                "capex_roads",
                "net_capacity_adverse_conditions_mw",
                "net_capacity_favorable_conditions_mw",
                "opex_dams",
                "opex_generation_misc",
                "opex_hydraulic",
                "opex_misc_plant",
                "opex_water_for_power",
                "ferc_license_id",
                "capex_equipment_electric",
                "capex_equipment_misc",
                "capex_wheels_turbines_generators",
                "energy_used_for_pumping_mwh",
                "net_load_mwh",
                "opex_production_before_pumping",
                "opex_pumped_storage",
                "opex_pumping",
                "total_fuel_cost_ferc1",
                "total_mmbtu_ferc1",
                "fuel_type_code_pudl_ferc1",
                "plant_id_report_year",
                "heat_rate_mmbtu_mwh_ferc1",
            ],
            "primary_key": ["record_id_eia", "record_id_ferc1"],
        },
        "field_namespace": "glue",
        "etl_group": "glue",
        "sources": ["eia860", "eia923", "ferc1"],
    },
}
"""PUDL-specifiic resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
