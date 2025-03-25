"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

import pandas as pd
import pytest

logger = logging.getLogger(__name__)


@pytest.mark.xfail(reason="There are 730 generators that report multiple operators")
def test_unique_operator_id(pudl_out_eia, live_dbs):
    """Test that each generator in the ownership table has a unique operator ID.

    The ``utility_id_eia`` column is supposed to be the operator, which should only be
    one utility for each generator in each report year, while ``owner_utility_id_eia``
    is supposed to represent the owner, of which there can be several for each
    generator.  We have a known issue with 2010 data. Many generators are being reported
    with multiple ``utility_id_eia`` values, which appear to actually be the
    ``owner_utility_id_eia``.

    Raises:
        AssertionError: If there are generators with multiple reported operators
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip()

    own_out = pudl_out_eia.own_eia860()
    operator_check = own_out.groupby(
        ["report_date", "plant_id_eia", "generator_id"], dropna=True
    )[["utility_id_eia"]].nunique()
    multi_operator = operator_check[operator_check.utility_id_eia > 1]
    years = multi_operator.report_date.dt.year.unique()
    if not multi_operator.empty:
        raise AssertionError(
            f"There are {len(multi_operator)} generator records across "
            f"{list(years)} that are being reported as having multiple "
            "operators."
        )


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
