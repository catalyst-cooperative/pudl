"""PyTest cases related to generating dervied outputs."""

import logging

import pytest

import pudl
import pudl.validate as pv

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def fast_out(pudl_engine):
    """A PUDL output object for use in CI."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        freq="MS",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
        fill_tech_desc=True,
    )


def nuke_gen_fraction(df):
    """Calculate the nuclear fraction of net generation."""
    total_gen = df.net_generation_mwh.sum()
    nuke_gen = df[df.fuel_type_code_pudl == "nuclear"].net_generation_mwh.sum()
    return nuke_gen / total_gen


@pytest.mark.parametrize(
    "df_name,expected_nuke_fraction,tolerance",
    [
        ("gf_eia923", 0.2, 0.02),
        ("mcoe_generators", 0.2, 0.02),
    ],
)
def test_nuclear_fraction(fast_out, df_name, expected_nuke_fraction, tolerance):
    """Ensure that overall nuclear generation fractions are as expected."""
    actual_nuke_fraction = nuke_gen_fraction(fast_out.__getattribute__(df_name)())
    assert abs(actual_nuke_fraction - expected_nuke_fraction) <= tolerance


@pytest.mark.parametrize(
    "df1_name,df2_name,mult,kwargs",
    [
        ("gens_eia860", "bga_eia860", 1 / 1, {}),
        ("gens_eia860", "gens_eia860", 1 / 1, {}),
        ("gens_eia860", "own_eia860", 1 / 1, {}),
        ("gens_eia860", "plants_eia860", 1 / 1, {}),
        ("gens_eia860", "boil_eia860", 1 / 1, {}),
        ("gens_eia860", "pu_eia860", 1 / 1, {}),
        ("gens_eia860", "utils_eia860", 1 / 1, {}),
        ("gens_eia860", "bf_eia923", 12 / 1, {}),
        ("gens_eia860", "frc_eia923", 12 / 1, {}),
        ("gens_eia860", "gen_eia923", 12 / 1, {}),
        ("gens_eia860", "gen_fuel_by_generator_energy_source_eia923", 12 / 1, {}),
        ("gens_eia860", "gen_fuel_by_generator_eia923", 12 / 1, {}),
        ("gens_eia860", "gf_eia923", 12 / 1, {}),
        ("gens_eia860", "hr_by_unit", 12 / 1, {}),
        ("gens_eia860", "hr_by_gen", 12 / 1, {}),
        ("gens_eia860", "fuel_cost", 12 / 1, {}),
        ("gens_eia860", "capacity_factor", 12 / 1, {}),
        pytest.param(
            "gens_eia860",
            "mcoe_generators",
            12 / 1,
            {"all_gens": False},
            marks=pytest.mark.xfail(reason="MCOE has time coverage issues."),
        ),
    ],
)
def test_eia_outputs(fast_out, df1_name, df2_name, mult, kwargs):
    """Check EIA output functions and date frequencies of output dataframes."""
    pytest.skip(reason="Memory intensive, GHA CI failing. Migrate to dbt ASAP.")
    df1 = fast_out.__getattribute__(df1_name)()
    logger.info(f"Running fast_out.{df2_name}() with freq={fast_out.freq}.")
    df2 = fast_out.__getattribute__(df2_name)(**kwargs)
    logger.info(f"Found {len(df2)} rows in {df2_name}")
    logger.info(f"Checking {df2_name} date frequency relative to {df1_name}.")
    pv.check_date_freq(df1, df2, mult)


@pytest.mark.parametrize(
    "df_name,thresh",
    [
        ("mcoe_generators", 0.9),
    ],
)
def test_null_rows(fast_out, df_name, thresh):
    """Check MCOE output for null rows resulting from bad merges."""
    # These are columns that only exist in earlier years
    pv.no_null_rows(
        df=fast_out.__getattribute__(df_name)(),
        df_name=df_name,
        thresh=thresh,
    )
