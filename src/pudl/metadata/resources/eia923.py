"""Definitions of data tables primarily coming from EIA-923."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "boiler_fuel_eia923": {
        "description": (
            """EIA-923 Monthly Boiler Fuel Consumption and Emissions, from EIA-923 Schedule 3.

Reports the quantity of each type of fuel consumed by each boiler on a monthly basis, as
well as the sulfur and ash content of those fuels. Fuel quantity is reported in standard
EIA fuel units (tons, barrels, Mcf). Heat content per unit of fuel is also reported,
making this table useful for calculating the thermal efficiency (heat rate) of various
generation units.

This table provides better coverage of the entire fleet of generators than the
``generation_fuel_eia923`` table, but the fuel consumption reported here is not directly
associated with a generator. This complicates the heat rate calculation, since the
associations between individual boilers and generators are incomplete and can be
complex.

Note that a small number of respondents only report annual fuel consumption, and all of
it is reported in December.
"""
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
                "prime_mover_code",
                "fuel_type_code_pudl",
                "report_date",
                "fuel_consumed_units",
                "fuel_mmbtu_per_unit",
                "sulfur_content_pct",
                "ash_content_pct",
            ],
            "primary_key": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
                "prime_mover_code",
                "report_date",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "coalmine_eia923": {
        "description": (
            """Attributes of coal mines reporting deliveries in the Fuel Receipts and Costs table, via
EIA-923 Schedule 2, Part C.

This table is produced during the transformation of fuel delivery data, in order to
produce a better normalized database. The same coalmines report many individual
deliveries, and repeating their attributes many times in the fuel receipts and costs
table is duplicative. Unfortunately the coalmine attributes do not generally use a
controlled vocabulary or well defined IDs and so in practice there are many distinct
records in this table that correspond to the same mines in reality.

We have not yet taken the time to rigorously clean this data, but it could be linked
with both Mining Safety and Health Administration (MSHA) and USGS data to provide more
insight into where coal is coming from, and what the employment and geological context
is for those supplies.
"""
        ),
        "schema": {
            "fields": [
                "mine_id_pudl",
                "mine_name",
                "mine_type_code",
                "state",
                "county_id_fips",
                "mine_id_msha",
                "data_maturity",
            ],
            "primary_key": ["mine_id_pudl"],
            "foreign_key_rules": {"fields": [["mine_id_pudl"]]},
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "fuel_receipts_costs_eia923": {
        "description": (
            """Data describing fuel deliveries to power plants, reported in EIA-923 Schedule 2, Part A.

Each record describes an individual fuel delivery. There can be multiple deliveries of
the same type of fuel from the same supplier to the same plant in a single month, so the
table has no natural primary key.

There can be a significant delay between the receipt of fuel and its consumption, so
using this table to infer monthly attributes associated with power generation may not be
entirely accurate. However, this is the most granular data we have describing fuel
costs, and we use it in calculating the marginal cost of electricity for individual
generation units.

Under some circumstances utilities are allowed to treat the price of fuel as proprietary
business data, meaning it is redacted from the publicly available spreadsheets. It's
still reported to EIA and influences the aggregated (state, region, annual, etc.) fuel
prices they publish. From 2009-2021 about 1/3 of all prices are redacted. The missing
data is not randomly distributed. Deregulated markets dominated by merchant generators
(independent power producers) redact much more data, and natural gas is by far the most
likely fuel to have its price redacted. This means, for instance, that the entire
Northeastern US reports essentially no fine-grained data about its natural gas prices.

Additional data which we haven't yet integrated is available in a similar format from
2002-2008 via the EIA-423, and going back as far as 1972 from the FERC-423.
"""
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "contract_type_code",
                "contract_expiration_date",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_group_code",
                "mine_id_pudl",
                "supplier_name",
                "fuel_received_units",
                "fuel_mmbtu_per_unit",
                "sulfur_content_pct",
                "ash_content_pct",
                "mercury_content_ppm",
                "fuel_cost_per_mmbtu",
                "primary_transportation_mode_code",
                "secondary_transportation_mode_code",
                "natural_gas_transport_code",
                "natural_gas_delivery_contract_type_code",
                "moisture_content_pct",
                "chlorine_content_ppm",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "generation_eia923": {
        "description": (
            """EIA-923 Monthly Generating Unit Net Generation. From EIA-923 Schedule 3.

Reports the net electricity generated by each reporting generator on a monthly basis.
This is the most granular information we have about how much electricity individual
generators are producing, but only about half of all the generation reported in the
``generation_fuel_eia923`` appears in this table due to the different reporting
requirements imposed on different types and sizes of generators.

Whenever possible, we use this generator-level net generation to estimate the heat rates
of generation units and the marginal cost of electricity on a per-generator basis, but
those calculations depend on plant-level fuel costs and sometimes uncertain or
incomplete boiler-generator associations.

Note that a small number of respondents only report annual net generation, and all of
it is reported in December.
"""
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "report_date",
                "net_generation_mwh",
                "data_maturity",
            ],
            "primary_key": ["plant_id_eia", "generator_id", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "generation_fuel_eia923": {
        "description": (
            """EIA-923 Monthly Generation and Fuel Consumption Time Series. From EIA-923 Schedule 3.

Monthly electricity generation and fuel consumption reported for each combination of
fuel and prime mover within a plant. This data can't be easily linked to individual
boilers, generators, and generation units, but it is provides the most complete coverage
of fuel consumption and electricity generation for the entire generation fleet. We use
the primary fuels and prime movers reported for each generator along with their
capacities to attribute fuel consumption and generation when it isn't directly reported
in the ``generation_eia923`` and ``boiler_fuel_eia923`` tables in order to calculate
capacity factors, heat rates, and the marginal cost of electricity.

The table makes a distinction between all fuel consumed and fuel consumed for
electricity generation because some units are also combined heat and power (CHP) units,
and also provide high temperature process heat at the expense of net electricity
generation.

Note that a small number of respondents only report annual fuel consumption and net
generation, and all of it is reported in December.

Note that this table does not include data from nuclear plants as they report at the
generation unit level, rather than the plant level. See the
``generation_fuel_nuclear_eia923`` table for nuclear electricity generation and fuel
consumption.
"""
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_type_code_aer",
                "prime_mover_code",
                "fuel_consumed_units",
                "fuel_consumed_for_electricity_units",
                "fuel_mmbtu_per_unit",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
                "net_generation_mwh",
                "data_maturity",
            ],
            "primary_key": [
                "plant_id_eia",
                "report_date",
                "energy_source_code",
                "prime_mover_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "generation_fuel_nuclear_eia923": {
        "description": (
            """EIA-923 Monthly Generation and Fuel Consumption Time Series. From EIA-923 Schedule 3.

Monthly electricity generation and fuel consumption reported for each combination of
fuel and prime mover within a nuclear generation unit. This data is originally reported
alongside similar information for fossil fuel plants, but the nuclear data is reported
by (nuclear) generation unit rather than fuel type and prime mover, and so has a
different primary key.
"""
        ),
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "nuclear_unit_id",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_type_code_aer",
                "prime_mover_code",
                "fuel_consumed_units",
                "fuel_consumed_for_electricity_units",
                "fuel_mmbtu_per_unit",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
                "net_generation_mwh",
                "data_maturity",
            ],
            "primary_key": [
                "plant_id_eia",
                "report_date",
                "nuclear_unit_id",
                "energy_source_code",
                "prime_mover_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
}
"""EIA-923 resource attributes organized by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
