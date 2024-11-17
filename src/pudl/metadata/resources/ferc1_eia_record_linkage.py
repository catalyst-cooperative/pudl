"""Definitions for the connection between FERC1 and EIA."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "out_eia__yearly_plant_parts": {
        "description": """Output table with the aggregation of all EIA plant parts. For use with matching to FERC 1.

Practically speaking, a plant is a collection of generator(s). There are many
attributes of generators (i.e. prime mover, primary fuel source, technology
type). We can use these generator attributes to group generator records into
larger aggregate records which we call "plant parts". A plant part is a record
which corresponds to a particular collection of generators that all share an
identical attribute and utility owner. E.g. all of the generators with unit_id=2, or all
of the generators with coal as their primary fuel source.

The EIA data about power plants (from EIA 923 and 860) is reported in tables
with records that correspond to mostly generators and plants. Other datasets
(cough cough FERC1) are less well organized and include plants, generators and
other plant parts all in the same table without any clear labels. This plant part table
is an attempt to create records corresponding to many different plant parts in order to
connect specific slices of EIA plants to other datasets.

Because generators are often owned by multiple utilities, another dimension of
this plant part table involves generating two records for each owner: one for the
portion of the plant part they own and one for the plant part as a whole. The
portion records are labeled in the ``ownership_record_type`` column as ``owned``
and the total records are labeled as ``total``.

This table includes A LOT of duplicative information about EIA plants. It is primarily
meant for use as an input into the record linkage between FERC1 plants and EIA.""",
        "schema": {
            "fields": [
                "record_id_eia",
                "plant_id_eia",
                "report_date",
                "plant_part",
                "generator_id",
                "unit_id_pudl",
                "prime_mover_code",
                "energy_source_code_1",
                "technology_description",
                "ferc_acct_name",
                "utility_id_eia",
                "true_gran",
                "appro_part_label",
                "appro_record_id_eia",
                "ferc1_generator_agg_id",
                "capacity_eoy_mw",
                "capacity_factor",
                "capacity_mw",
                "construction_year",
                "fraction_owned",
                "fuel_cost_per_mmbtu",
                "fuel_cost_per_mwh",
                "fuel_type_code_pudl",
                "generator_retirement_date",
                "unit_heat_rate_mmbtu_per_mwh",
                "installation_year",
                "net_generation_mwh",
                "generator_operating_year",
                "operational_status",
                "operational_status_pudl",
                "ownership_record_type",
                "ownership_dupe",
                "planned_generator_retirement_date",
                "plant_id_pudl",
                "plant_name_eia",
                "plant_name_ppe",
                "plant_part_id_eia",
                "record_count",
                "total_fuel_cost",
                "total_mmbtu",
                "utility_id_pudl",
                "utility_name_eia",
                "report_year",
                "plant_id_report_year",
            ],
            "primary_key": ["record_id_eia"],
            "foreign_key_rules": {
                "fields": [["record_id_eia"], ["record_id_eia_plant_gen"]],
                # TODO: track down 10 missing record_id_eia's in this table #3818
                "exclude": ["out_pudl__yearly_assn_eia_ferc1_plant_parts"],
            },
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
        "field_namespace": "eia",
    },
    "out_eia__yearly_generators_by_ownership": {
        "description": "A mega table of all EIA generators with ownership integrated.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "report_date",
                "unit_id_pudl",
                "plant_id_pudl",
                "plant_name_eia",
                "utility_id_eia",
                "utility_id_pudl",
                "utility_name_eia",
                "technology_description",
                "energy_source_code_1",
                "prime_mover_code",
                "generator_operating_date",
                "generator_retirement_date",
                "operational_status",
                "capacity_mw",
                "fuel_type_code_pudl",
                "planned_generator_retirement_date",
                "capacity_factor",
                "fuel_cost_per_mmbtu_source",
                "fuel_cost_per_mmbtu",
                "fuel_cost_per_mwh",
                "unit_heat_rate_mmbtu_per_mwh",
                "net_generation_mwh",
                "total_fuel_cost",
                "total_mmbtu",
                "ferc_acct_name",
                "generator_operating_year",
                "operational_status_pudl",
                "capacity_eoy_mw",
                "fraction_owned",
                "ownership_record_type",
            ],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
        "field_namespace": "eia",
    },
    "out_eia__yearly_assn_plant_parts_plant_gen": {
        "description": """In order to easily determine what generator records are associated with every
plant part record, we made this association table. This table associates every plant part
record (identified as ``record_id_eia``) from the ``out_eia__yearly_plant_parts`` table to the associated
'plant_gen' records (identified as ``record_id_eia_plant_gen``) from the same table. The plant part
records have a one to many relationship to the associated 'plant_gen' records. The 'plant_gen' records
are also included in the plant part records, so for these records there is a one to one relationship
and the ``record_id_eia`` and the ``record_id_eia_plant_gen`` are the same.

All of the columns in this table that have a suffix of '_plant_gen' are attributes of the
``record_id_eia_plant_gen`` record.""",
        "schema": {
            "fields": [
                "record_id_eia",
                "record_id_eia_plant_gen",
                "report_date",
                "plant_id_eia",
                "utility_id_eia",
                "ownership_record_type",
                "generator_id_plant_gen",
                "energy_source_code_1_plant_gen",
                "prime_mover_code_plant_gen",
                "unit_id_pudl_plant_gen",
                "technology_description_plant_gen",
                "ferc_acct_name_plant_gen",
                "ferc1_generator_agg_id_plant_gen",
                "generator_operating_year_plant_gen",
                "operational_status_pudl_plant_gen",
                "generators_number",
            ],
            "primary_key": ["record_id_eia", "record_id_eia_plant_gen"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
        "field_namespace": "eia",
    },
    "out_pudl__yearly_assn_eia_ferc1_plant_parts": {
        "description": """This table links power plant data reported in FERC Form 1 to related EIA data. It
answers the question "What EIA data reported about plants or generators should be
associated with a given plant record found in the FERC Form 1."

Each record in this table corresponds to a single FERC Form 1 record reported in one of
several tables describing power plants (large steam, hydro, small, etc.). These FERC
records can correspond to an entire plant, individual generators within a plant, all
generators in a plant with the same prime mover type, or just the respondent's ownership
share of any of those categories (or other categories). Furthermore, the same utility
may report the same plant in different ways in different years.

The EIA data associated with each FERC plant record comes from our Plant Parts EIA
table. The EIA data in each record represents an aggregation of several slices of an EIA
plant, across both physical characteristics and utility ownership.""",
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
                "unit_heat_rate_mmbtu_per_mwh_eia",
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
                "construction_year_eia",
                "construction_year_ferc1",
                "installation_year_ferc1",
                "net_generation_mwh_ferc1",
                "not_water_limited_capacity_mw",
                "opex_allowances",
                "opex_boiler",
                "opex_coolants",
                "opex_electric",
                "opex_engineering",
                "opex_fuel",
                "fuel_cost_per_mwh_eia",
                "fuel_cost_per_mwh_ferc1",
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
                "unit_heat_rate_mmbtu_per_mwh_ferc1",
            ],
            "primary_key": ["record_id_ferc1"],
        },
        "field_namespace": "pudl",
        "etl_group": "outputs",
        "sources": ["eia860", "eia923", "ferc1"],
    },
}
"""Resources definition for granular connection between FERC1 and EIA."""
