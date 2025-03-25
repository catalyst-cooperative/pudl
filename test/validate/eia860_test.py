"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

import pandas as pd
import pytest

logger = logging.getLogger(__name__)


@pytest.mark.xfail(reason="There are 40 known inconsistent generator IDs.")
def test_generator_id_consistency(pudl_out_eia, live_dbs):
    """Check if there are any plants that report inconsistent generator IDs.

    There are some instances in which a plant will report generator IDs differently in
    different years, such that the IDs differ only in terms of the case of letters, or
    non-alphanumeric characters. This test identifies them. We haven't fixed them yet.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip()

    # All unique plant+generator ID combinations:
    gen_ids = pudl_out_eia.gens_eia860()[
        ["plant_id_eia", "generator_id"]
    ].drop_duplicates()
    # A simple generator_id w/o non-alphanumeric characters or lower-case letters:
    gen_ids["simple_id"] = gen_ids.generator_id.str.upper().str.replace(
        r"[^a-zA-Z0-9]", "", regex=True
    )

    # Identify the set of simple IDs which map to multiple original generator IDs,
    # meaning that within a single plant, there are generators whose original IDs
    # only differ by non-alphanumeric characters, or by the case of the letters:
    multiple_ids = (
        gen_ids.groupby(["plant_id_eia", "simple_id"])["generator_id"].nunique().gt(1)
    )

    # Select only those generator IDs that have multiple values:
    problem_generators = (
        gen_ids.set_index(["plant_id_eia", "simple_id"]).loc[multiple_ids].sort_index()
    )
    if not problem_generators.empty:
        errstr = f"Found {len(problem_generators)} ambiguous generator IDs."
        raise ValueError(errstr)


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
