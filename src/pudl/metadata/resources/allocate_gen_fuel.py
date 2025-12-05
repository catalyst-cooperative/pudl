"""Resource metadata for the allocate_gen_fuel tables."""

from typing import Any

from pudl.metadata.resource_helpers import inherits_harvested_values_details

AGG_FREQS = ["yearly", "monthly"]

USAGE_WARNING_DRAWBACK = {
    "type": "custom",
    "description": "This downscaling process used to create this table does not distinguish between primary and secondary energy_sources for generators (see below for implications).",
}
UPSTREAM_ALLOCATION_CONTEXT = """The net generation and fuel consumption allocation
method PUDL employs begins with the following context of the originally reported
EIA-860 and EIA-923 data:
* The :ref:`core_eia923__monthly_generation_fuel` table is the authoritative source of
  information about how much generation and fuel consumption is attributable to an
  entire plant. This table has the most complete data coverage, but it is not the most
  granular data reported.
* The :ref:`core_eia923__monthly_generation` table contains the most granular net
  generation data. It is reported at the ``plant_id_eia``, ``generator_id`` and
  ``report_date`` level. This table includes only ~40% of the total MWhs reported in
  the :ref:`core_eia923__monthly_generation_fuel` table.
* The :ref:`core_eia923__monthly_boiler_fuel` table contains the most granular fuel
  consumption data.  It is reported at the boiler/prime mover/energy source level. This
  table includes only ~40% of the total MMBTUs reported in the
  :ref:`core_eia923__monthly_generation_fuel` table.
* The :ref:`core_eia860__scd_generators` table provides an exhaustive list of all
  generators whose generation is being reported in the
  :ref:`core_eia923__monthly_generation_fuel` table."""
KNOWN_DRAWBACKS_DESCRIPTION = (
    "This process does not distinguish between primary and secondary energy_sources for generators. "
    "Net generation is allocated equally between energy source codes, so if a "
    "plant has multiple generators with the same prime_mover_code but different "
    "energy source codes the core_eia923__monthly_generation_fuel records will be associated "
    "similarly between these two generators. Allocated net generation will still "
    "be proportional to each generator's net generation or capacity."
)

RESOURCE_METADATA: dict[str, dict[str, Any]] = (
    {
        f"out_eia923__{freq}_generation_fuel_by_generator_energy_source": {
            "description": {
                "additional_summary_text": "of estimated net generation and fuel consumption associated with each combination of generator, energy source, and prime mover.",
                "layer_code": "out_narrow",
                "additional_source_text": "(Schedule 3)",
                "usage_warnings": [
                    "estimated_values",
                    USAGE_WARNING_DRAWBACK,
                    "month_as_date",
                    {
                        "type": "custom",
                        "description": "A small number of respondents only report annual fuel consumption, and all of it is reported in December.",
                    },
                ],
                "additional_details_text": (
                    f"{UPSTREAM_ALLOCATION_CONTEXT}\n\n"
                    "In this table, PUDL has allocated the net electricity generation "
                    "and fuel consumption from "
                    ":ref:`core_eia923__monthly_generation_fuel` to the ``generator_id``/"
                    "``energy_source_code``/``prime_mover_code`` level.\n\n"
                    "The allocation process entails generating a fraction for each "
                    "record based on the net generation in the "
                    ":ref:`core_eia923__monthly_generation` table and the capacity "
                    "from the :ref:`core_eia860__scd_generators` table. "
                    f"{KNOWN_DRAWBACKS_DESCRIPTION}"
                ),
            },
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "prime_mover_code",
                    "energy_source_code",
                    "energy_source_code_num",
                    "net_generation_mwh",
                    "fuel_consumed_mmbtu",
                    "fuel_consumed_for_electricity_mmbtu",
                ],
                "primary_key": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "prime_mover_code",
                    "energy_source_code",
                ],
            },
            "field_namespace": "eia",
            "sources": ["eia923", "eia860"],
            "etl_group": "outputs",
        }
        for freq in AGG_FREQS
    }
    | {
        f"out_eia923__{freq}_generation_fuel_by_generator": {
            "description": {
                "additional_summary_text": "of estimated net generation and fuel consumption by generator.",
                "additional_source_text": "(Schedule 3)",
                "usage_warnings": [
                    "estimated_values",
                    USAGE_WARNING_DRAWBACK,
                    "month_as_date",
                    {
                        "type": "custom",
                        "description": "A small number of respondents only report annual fuel consumption, and all of it is reported in December.",
                    },
                    "harvested",
                ],
                "additional_details_text": (
                    "Based on allocating net electricity generation and fuel consumption reported "
                    "in the EIA-923 generation and generation_fuel tables to individual generators.\n\n"
                    f"{UPSTREAM_ALLOCATION_CONTEXT}\n\n"
                    "In this table, PUDL aggregates the net generation and fuel consumption "
                    "that has been allocated to the ``generator_id``/"
                    "``energy_source_code``/``prime_mover_code`` level in the"
                    f":ref:`out_eia923__{freq}_generation_fuel_by_generator_energy_source` "
                    "to the generator level."
                    f"{KNOWN_DRAWBACKS_DESCRIPTION}"
                    f"\n\n{inherits_harvested_values_details('generators, plants, and utilities')}"
                ),
            },
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "plant_id_pudl",
                    "plant_name_eia",
                    "utility_id_eia",
                    "utility_id_pudl",
                    "utility_name_eia",
                    "generator_id",
                    "unit_id_pudl",
                    "fuel_consumed_for_electricity_mmbtu",
                    "fuel_consumed_mmbtu",
                    "net_generation_mwh",
                ],
                "primary_key": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                ],
            },
            "field_namespace": "eia",
            "sources": ["eia923", "eia860"],
            "etl_group": "outputs",
        }
        for freq in AGG_FREQS
    }
    | {
        "out_eia923__yearly_generation_fuel_by_generator_energy_source_owner": {
            "description": {
                "additional_summary_text": (
                    "of estimated net generation and fuel consumption for each generator, "
                    "associated with each combination of generator, energy source, prime mover, and owner."
                ),
                "layer_code": "out_narrow",
                "additional_source_text": "(Schedule 3)",
                "usage_warnings": [
                    "estimated_values",
                    USAGE_WARNING_DRAWBACK,
                    "month_as_date",
                    "harvested",
                ],
                "additional_details_text": (
                    "First, the net electricity generation and fuel consumption "
                    "reported in the EIA-923 generation fuel are allocated to individual "
                    "generators. Then, these allocations are aggregated to unique generator, "
                    "prime mover, energy source code, and owner combinations. Note that the "
                    "utility_id_eia in this table refers to the OWNER of the generator, not the "
                    f"operator.\n\n{KNOWN_DRAWBACKS_DESCRIPTION}"
                    f"\n\n{inherits_harvested_values_details('generators and plants')}"
                ),
            },
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "prime_mover_code",
                    "energy_source_code",
                    "utility_id_eia",
                    "ownership_record_type",
                    "fraction_owned",
                    "capacity_mw",
                    "energy_source_code_num",
                    "net_generation_mwh",
                    "fuel_consumed_mmbtu",
                    "fuel_consumed_for_electricity_mmbtu",
                ],
                "primary_key": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "prime_mover_code",
                    "energy_source_code",
                    "utility_id_eia",
                    "ownership_record_type",
                ],
            },
            "field_namespace": "eia",
            "sources": ["eia923", "eia860"],
            "etl_group": "outputs",
        },
    }
)
