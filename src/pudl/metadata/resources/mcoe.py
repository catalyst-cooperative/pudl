"""Resource metadata for the MCOE (marginal cost of electricity) tables."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    f"mcoe_{freq}": {
        "description": (
            f"{freq.title()} generator capacity factor, heat rate, fuel cost per MMBTU and fuel cost "
            "per MWh. These calculations are based on the allocation of net generation reported on "
            "the basis of plant, prime mover and energy source to individual generators. However, "
            "fuel consumed is taken only from the :ref:`boiler_fuel_eia923` table, which reports fuel "
            "consumed by boiler. This means that only generators that have been assigned a "
            "``unit_id_pudl`` in :ref:`boiler_generator_assn_eia860` will have heat rates. Heat rates "
            "are necessary to estimate the amount of fuel consumed by a generation unit, and thus the "
            "fuel cost per MWh generated. Plant specific fuel prices are taken from the "
            ":ref:`fuel_receipts_costs_eia923` table, which only has ~70% coverage, leading to some "
            "generators with heat rate estimates still lacking fuel cost estimates."
        ),
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
                "fuel_cost_from_eiaapi",
                "fuel_cost_per_mmbtu",
                "fuel_cost_per_mwh",
                "heat_rate_mmbtu_mwh",
                "net_generation_mwh",
                "total_fuel_cost",
                "total_mmbtu",
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
    for freq in ["yearly", "monthly"]
}
