"""Validate post-ETL EIA 860 data and the associated derived outputs."""
import logging

import pytest
from scipy import stats

from pudl import helpers

logger = logging.getLogger(__name__)


def test_own_eia860(pudl_out_eia, live_dbs):
    """Sanity checks for EIA 860 generator ownership data."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip()

    own_out = pudl_out_eia.own_eia860()

    if (own_out.fraction_owned > 1.0).any():
        raise AssertionError(
            "Generators with ownership fractions > 1.0 found. Bad data?"
        )

    if (own_out.fraction_owned < 0.0).any():
        raise AssertionError(
            "Generators with ownership fractions < 0.0 found. Bad data?"
        )

    # Verify that the reported ownership fractions add up to something very
    # close to 1.0 (i.e. that the full ownership of each generator is
    # specified by the EIA860 data)
    own_gb = own_out.groupby(['report_date', 'plant_id_eia', 'generator_id'])
    own_sum = own_gb['fraction_owned'].agg(helpers.sum_na).reset_index()
    logger.info(
        f"{len(own_sum[own_sum.fraction_owned.isnull()])} generator-years have no ownership data.")

    own_sum = own_sum.dropna()
    pct_missing = stats.percentileofscore(own_sum.fraction_owned, 0.98)
    logger.info(
        f"{len(own_sum[own_sum.fraction_owned < 0.98])} ({pct_missing}%) "
        f"generator-years have incomplete ownership data.")
    if not max(own_sum['fraction_owned'] < 1.02):
        raise ValueError("Plants with more than 100% ownership found...")
    # There might be a few generators with incomplete ownership but virtually
    # everything should be pretty fully described. If not, let us know. The
    # 0.5 threshold means 0.5% -- i.e. less than 1 in 200 is partial.
    if pct_missing >= 0.5:
        raise ValueError(
            f"{pct_missing}% of generators lack complete ownership data."
        )


@pytest.mark.xfail(reason="There are 730 generators that report multiple operators")
def test_operator_id(pudl_out_eia, live_dbs):
    """
    Test if the operator id in ownership table is consistent across generators.

    The ``utility_id_eia`` column is supposed to be the operator, which should
    only be one utility for each generator. The ``owner_utility_id_eia`` is
    suppose to be the owner, which can be multiple utilities for each generator.
    We have a known issue with 2010 data. Many generators are being reported
    with the ``utility_id_eia`` with what appears to be the
    ``owner_utility_id_eia`` instead.

    Raises:
        AssertionError: If there are generators with multiple reported operators

    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip()

    own_out = pudl_out_eia.own_eia860()
    operator_check = (
        own_out.groupby(
            ['report_date', 'plant_id_eia', 'generator_id'],
            dropna=True)
        [['utility_id_eia']].nunique()
    )
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
    """
    Check if there are any plants that report inconsistent generator IDs.

    There are some instances in which a plant will report generator IDs
    differently in different years, such that the IDs differ only in terms of
    the case of letters, or non-alphanumeric characters. This test identifies
    them. We haven't fixed them yet.

    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip()

    # All unique plant+generator ID combinations:
    gen_ids = (
        pudl_out_eia.gens_eia860()[["plant_id_eia", "generator_id"]]
        .drop_duplicates()
    )
    # A simple generator_id w/o non-alphanumeric characters or lower-case letters:
    gen_ids["simple_id"] = (
        gen_ids.generator_id.str.upper()
        .str.replace(r"[^a-zA-Z0-9]", "", regex=True)
    )

    # Identify the set of simple IDs which map to multiple original generator IDs,
    # meaning that within a single plant, there are generators whose original IDs
    # only differ by non-alphanumeric characters, or by the case of the letters:
    multiple_ids = (
        gen_ids.groupby(["plant_id_eia", "simple_id"])["generator_id"]
        .nunique() > 1
    )

    # Select only those generator IDs that have multiple values:
    problem_generators = (
        gen_ids.set_index(["plant_id_eia", "simple_id"])
        .loc[multiple_ids]
        .sort_index()
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
        pudl_out_eia.gens_eia860()[[
            "plant_id_eia",
            "generator_id",
            "energy_source_code_1",
        ]]
        .query("energy_source_code_1 == 'NUC'")
        .set_index(["plant_id_eia", "generator_id"])
    )

    all_nuke_gf = (
        pudl_out_eia.gf_eia923()[[
            "plant_id_eia",
            "nuclear_unit_id",
            "fuel_type",
        ]]
        .query("fuel_type == 'NUC'")
        .assign(generator_id=lambda x: x.nuclear_unit_id.astype(int).astype(str))
        .set_index(["plant_id_eia", "generator_id"])
    )
    assert set(all_nuke_gf.index).issubset(all_nuke_gens.index)
