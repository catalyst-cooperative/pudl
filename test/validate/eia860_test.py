"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

import pandas as pd
import pytest

logger = logging.getLogger(__name__)


def test_nuclear_units_are_generators(pudl_out_eia, live_dbs):
    """Validate that all nuclear Unit IDs correspond to generator IDs."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip()

    all_nuke_gens = (
        pudl_out_eia.gens_eia860()[
            [
                "plant_id_eia",
                "generator_id",
                "energy_source_code_1",
            ]
        ]
        .query("energy_source_code_1 == 'NUC'")
        .set_index(["plant_id_eia", "generator_id"])
    )

    # Drop all nuclear units that don't have a nuclear_unit_id.
    all_nuke_gf = (
        pd.read_sql(
            "core_eia923__monthly_generation_fuel_nuclear",
            pudl_out_eia.pudl_engine,
            columns=[
                "plant_id_eia",
                "nuclear_unit_id",
                "energy_source_code",
            ],
        )
        .query("nuclear_unit_id != 'UNK'")
        .assign(generator_id=lambda x: x.nuclear_unit_id)
        .set_index(["plant_id_eia", "generator_id"])
    )
    assert set(all_nuke_gf.index).issubset(all_nuke_gens.index)
