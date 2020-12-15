"""Metadata and operational constants."""
from typing import Dict, List, Union

FIELD_DTYPES: Dict[str, str] = {
    "string": "string",
    "number": "float",
    "integer": "Int64",
    "boolean": "boolean",
    "date": "datetime64[ns]",
    "datetime": "datetime64[ns]",
    "year": "Int64",
}
"""
Pandas data type by PUDL field type (Data Package `field.type`).
"""

LICENSES: Dict[str, Dict[str, str]] = {
    "cc-by-4.0": {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0",
        "path": "https://creativecommons.org/licenses/by/4.0",
    },
    "us-govt": {
        "name": "other-pd",
        "title": "U.S. Government Works",
        "path": "https://www.usa.gov/government-works",
    },
}
"""
License attributes by PUDL identifier.
"""

SOURCES: Dict[str, Dict[str, str]] = {
    "eia860": {
        "title": "EIA Form 860",
        "path": "https://www.eia.gov/electricity/data/eia860",
    },
    "eia861": {
        "title": "EIA Form 861: Annual Electric Power Industry Report",
        "path": "https://www.eia.gov/electricity/data/eia861",
    },
    "eia923": {
        "title": "EIA Form 923",
        "path": "https://www.eia.gov/electricity/data/eia923",
    },
    "eiawater": {
        "title": "EIA Thermoelectric cooling water data",
        "path": "https://www.eia.gov/electricity/data/water",
    },
    "epacems": {
        "title": "EPA Air Markets Program Data: Hourly Continuous Emission Monitoring System(CEMS)",
        "path": "https://ampd.epa.gov/ampd",
    },
    "epaipm": {
        "title": "EPA Integrated Planning Model (IPM)",
        "path": "https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6",
    },
    "ferc1": {
        "title": "FERC Form 1: Electric Utility Annual Report",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual",
    },
    "ferc714": {
        "title": "FERC Form 714: Annual Electric Balancing Authority Area and Planning Area Report",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric",
    },
    "ferceqr": {
        "title": "FERC Form 920: Electric Quarterly Report (EQR)",
        "path": "https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr",
    },
    "msha": {
        "title": "Mine Safety and Health Administration (MSHA)",
        "path": "https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp",
    },
    "phmsa": {
        "title": "Pipelines and Hazardous Materials Safety Administration (PHMSA)",
        "path": "https://www.phmsa.dot.gov/data-and-statistics/pipeline/data-and-statistics-overview",
    },
    "pudl": {
        "title": "The Public Utility Data Liberation (PUDL) Project",
        "path": "https://catalyst.coop/pudl",
        "email": "pudl@catalyst.coop",
    },
}
"""
Source attributes by PUDL identifier.
"""

CONTRIBUTORS: Dict[str, Dict[str, str]] = {
    "catalyst-cooperative": {
        "title": "Catalyst Cooperative",
        "email": "pudl@catalyst.coop",
        "path": "https://catalyst.coop",
        "role": "publisher",
        "organization": "Catalyst Cooperative",
    },
    "zane-selvans": {
        "title": "Zane Selvans",
        "email": "zane.selvans@catalyst.coop",
        "path": "https://amateurearthling.org",
        "role": "wrangler",
        "organization": "Catalyst Cooperative",
    },
    "christina-gosnell": {
        "title": "Christina Gosnell",
        "email": "christina.gosnell@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "steven-winter": {
        "title": "Steven Winter",
        "email": "steven.winter@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "alana-wilson": {
        "title": "Alana Wilson",
        "email": "alana.wilson@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "karl-dunkle-werner": {
        "title": "Karl Dunkle Werner",
        "email": "karldw@berkeley.edu",
        "path": "https://karldw.org",
        "role": "contributor",
        "organization": "UC Berkeley",
    },
    "greg-schivley": {
        "title": "Greg Schivley",
        "path": "https://gschivley.github.io",
        "role": "contributor",
        "organization": "Carbon Impact Consulting",
    },
}
"""
Contributor attributes by PUDL identifier.
"""

CONTRIBUTORS_BY_SOURCE: Dict[str, List[str]] = {
    "pudl": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
        "karl-dunkle-werner",
    ],
    "eia923": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
    ],
    "eia860": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
    ],
    "ferc1": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
    ],
    "epacems": [
        "catalyst-cooperative",
        "karl-dunkle-werner",
        "zane-selvans",
    ],
    "epaipm": [
        "greg-schivley",
    ],
}
"""
Contributors (PUDL identifiers) by source (PUDL identifier).
"""

KEYWORDS_BY_SOURCE: Dict[str, List[str]] = {
    "pudl": ["us", "electricity"],
    "eia860": [
        "electricity",
        "electric",
        "boiler",
        "generator",
        "plant",
        "utility",
        "fuel",
        "coal",
        "natural gas",
        "prime mover",
        "eia860",
        "retirement",
        "capacity",
        "planned",
        "proposed",
        "energy",
        "hydro",
        "solar",
        "wind",
        "nuclear",
        "form 860",
        "eia",
        "annual",
        "gas",
        "ownership",
        "steam",
        "turbine",
        "combustion",
        "combined cycle",
        "eia",
        "energy information administration",
    ],
    "eia923": [
        "fuel",
        "boiler",
        "generator",
        "plant",
        "utility",
        "cost",
        "price",
        "natural gas",
        "coal",
        "eia923",
        "energy",
        "electricity",
        "form 923",
        "receipts",
        "generation",
        "net generation",
        "monthly",
        "annual",
        "gas",
        "fuel consumption",
        "MWh",
        "energy information administration",
        "eia",
        "mercury",
        "sulfur",
        "ash",
        "lignite",
        "bituminous",
        "subbituminous",
        "heat content",
    ],
    "epacems": [
        "epa",
        "us",
        "emissions",
        "pollution",
        "ghg",
        "so2",
        "co2",
        "sox",
        "nox",
        "load",
        "utility",
        "electricity",
        "plant",
        "generator",
        "unit",
        "generation",
        "capacity",
        "output",
        "power",
        "heat content",
        "mmbtu",
        "steam",
        "cems",
        "continuous emissions monitoring system",
        "hourly" "environmental protection agency",
        "ampd",
        "air markets program data",
    ],
    "ferc1": [
        "electricity",
        "electric",
        "utility",
        "plant",
        "steam",
        "generation",
        "cost",
        "expense",
        "price",
        "heat content",
        "ferc",
        "form 1",
        "federal energy regulatory commission",
        "capital",
        "accounting",
        "depreciation",
        "finance",
        "plant in service",
        "hydro",
        "coal",
        "natural gas",
        "gas",
        "opex",
        "capex",
        "accounts",
        "investment",
        "capacity",
    ],
    "epaipm": ["epaipm", "integrated planning"],
}
"""
Keywords by source (PUDL identifier).
"""

RESOURCES_BY_SOURCE: Dict[str, List[str]] = {
    "eia860": [
        "boiler_generator_assn_eia860",
        "utilities_eia860",
        "plants_eia860",
        "generators_eia860",
        "ownership_eia860",
    ],
    "eia861": [
        "service_territory_eia861",
        "balancing_authority_eia861",
        "sales_eia861",
        "advanced_metering_infrastructure_eia861",
        "demand_response_eia861",
        "demand_side_management_eia861",
        "distributed_generation_eia861",
        "distribution_systems_eia861",
        "dynamic_pricing_eia861",
        "energy_efficiency_eia861",
        "green_pricing_eia861",
        "mergers_eia861",
        "net_metering_eia861",
        "non_net_metering_eia861",
        "operational_data_eia861",
        "reliability_eia861",
        "utility_data_eia861",
    ],
    "eia923": [
        "generation_fuel_eia923",
        "boiler_fuel_eia923",
        "generation_eia923",
        "coalmine_eia923",
        "fuel_receipts_costs_eia923",
    ],
    "epacems": [
        "hourly_emissions_epacems",
    ],
    "epaipm": [
        "transmission_single_epaipm",
        "transmission_joint_epaipm",
        "load_curves_epaipm",
        "plant_region_map_epaipm",
    ],
    "ferc1": [
        "fuel_ferc1",  # Plant-level data, linked to plants_steam_ferc1
        "plants_steam_ferc1",  # Plant-level data
        "plants_small_ferc1",  # Plant-level data
        "plants_hydro_ferc1",  # Plant-level data
        "plants_pumped_storage_ferc1",  # Plant-level data
        "purchased_power_ferc1",  # Inter-utility electricity transactions
        "plant_in_service_ferc1",  # Row-mapped plant accounting data.
        # 'accumulated_depreciation_ferc1',  # Requires row-mapping to be useful.
    ],
    "ferc714": [
        "respondent_id_ferc714",
        "id_certification_ferc714",
        "gen_plants_ba_ferc714",
        "demand_monthly_ba_ferc714",
        "net_energy_load_ba_ferc714",
        "adjacency_ba_ferc714",
        "interchange_ba_ferc714",
        "lambda_hourly_ba_ferc714",
        "lambda_description_ferc714",
        "description_pa_ferc714",
        "demand_forecast_pa_ferc714",
        "demand_hourly_pa_ferc714",
    ],
    "glue": [
        "plants",
        "plants_eia",
        "plants_ferc",
        "utilities",
        "utilities_eia",
        "utilities_ferc",
        "utility_plant_assn",
    ],
}
"""
Resources (PUDL identifiers) by source (PUDL identifier).
"""

FOREIGN_KEY_RULES: Dict[tuple, List[Union[str, tuple]]] = {
    ("energy_source_code", ): ["energy_source_eia923", ("abbr", )],
    ("fuel_type", ): ["fuel_type_eia923", ("abbr", )],
    ("fuel_type_code", ): ["fuel_type_eia923", ("abbr", )],
    ("fuel_type_code_aer", ): ["fuel_type_aer_eia923", ("abbr", )],
    ("line_id", ): ["ferc_depreciation_lines"],
    ("mine_id_pudl", ): ["coalmine_eia923"],
    ("plant_id_eia", ): ["plants_entity_eia"],
    ("plant_id_eia", "generator_id"): ["generators_entity_eia"],
    ("plant_id_pudl", ): ["plants_pudl"],
    ("plant_name_ferc1", "utility_id_ferc1"): ["plants_ferc1"],
    ("plant_name_original", "utility_id_ferc1"): [
        "plants_ferc1", ("plant_name_ferc1", "utility_id_ferc1")
    ],
    ("primary_transportation_mode_code", ): ["transport_modes_eia923", ("abbr", )],
    ("prime_mover_code", ): ["prime_movers_eia923", ("abbr", )],
    ("region", ): ["regions_entity_epaipm", ("region_id_epaipm", )],
    ("region_from", ): ["regions_entity_epaipm", ("region_id_epaipm", )],
    ("region_id_epaipm", ): ["regions_entity_epaipm"],
    ("region_to", ): ["regions_entity_epaipm", ("region_id_epaipm", )],
    ("secondary_transportation_mode_code", ): ["transport_modes_eia923", ("abbr", )],
    ("utility_id_eia", ): ["utilities_entity_eia"],
    ("utility_id_ferc1", ): ["utilities_ferc1"],
    ("utility_id_pudl", ): ["utilities_pudl"],
}
"""
Rules for foreign key constraints between resources.

Each tuple of local field names points to the name of the reference resource and,
if different from the local field names, the reference's field names.
"""
