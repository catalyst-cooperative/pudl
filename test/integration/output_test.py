"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging

import pandas as pd
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
    "df_name",
    [
        "pu_ferc1",
        "fuel_ferc1",
        "plants_steam_ferc1",
        "fbp_ferc1",
        "plants_all_ferc1",
        "plants_hydro_ferc1",
        "plants_pumped_storage_ferc1",
        "plants_small_ferc1",
        "purchased_power_ferc1",
        "plant_in_service_ferc1",
    ],
)
def test_ferc1_outputs(fast_out, df_name):
    """Check that FERC 1 output functions work."""
    logger.info(f"Running fast_out.{df_name}()")
    df = fast_out.__getattribute__(df_name)()
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty


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
            marks=pytest.mark.xfail(
                reason="MCOE has time coverage issues that will be resolved in Dagster migration."
            ),
        ),
    ],
)
def test_eia_outputs(fast_out, df1_name, df2_name, mult, kwargs):
    """Check EIA output functions and date frequencies of output dataframes."""
    df1 = fast_out.__getattribute__(df1_name)()
    logger.info(f"Running fast_out.{df2_name}() with freq={fast_out.freq}.")
    df2 = fast_out.__getattribute__(df2_name)(**kwargs)
    logger.info(f"Found {len(df2)} rows in {df2_name}")
    logger.info(f"Checking {df2_name} date frequency relative to {df1_name}.")
    pv.check_date_freq(df1, df2, mult)


@pytest.mark.parametrize(
    "df_name",
    [
        "plant_parts_eia",
        "ferc1_eia",
        "gen_fuel_by_generator_energy_source_eia923",
        "gen_fuel_by_generator_eia923",
        "gen_fuel_by_generator_energy_source_owner_eia923",
    ],
)
def test_annual_eia_outputs(fast_out_annual, df_name):
    """Test some output methods with frequency ``AS``."""
    logger.info(f"Running fast_out_annual.{df_name}()")
    df = fast_out_annual.__getattribute__(df_name)()
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty


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


@pytest.mark.parametrize(
    "table_suffix",
    ["eia861", "ferc714"],
)
def test_outputs_by_table_suffix(fast_out, table_suffix):
    """Check that all EIA-861 & FERC-714 output tables are present and non-empty."""
    tables = [t for t in fast_out.__dir__() if t.endswith(table_suffix)]
    for table in tables:
        logger.info(f"Checking that {table} is a DataFrame with no null columns.")
        df = fast_out.__getattribute__(table)()

        assert isinstance(df, pd.DataFrame), f"{table } is {type(df)}, not DataFrame!"
        assert not df.empty, f"{table} is empty!"
        for col in df.columns:
            if df[col].isna().all():
                raise ValueError(f"Found null column: {table}.{col}")


@pytest.mark.parametrize(
    "df_name",
    [
        "summarized_demand_ferc714",
        "fipsified_respondents_ferc714",
    ],
)
def test_ferc714_outputs(pudl_engine, df_name):
    """Test FERC 714 derived output methods."""
    df = pd.read_sql(df_name, pudl_engine)
    assert isinstance(df, pd.DataFrame), f"{df_name} is {type(df)} not DataFrame!"
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty, f"{df_name} is empty!"


@pytest.mark.parametrize(
    "df_name",
    [
        "compiled_geometry_balancing_authority_eia861",
        "compiled_geometry_utility_eia861",
    ],
)
def test_service_territory_outputs(pudl_engine, df_name):
    """Test FERC 714 derived output methods."""
    df = pd.read_sql(df_name, pudl_engine)
    assert isinstance(df, pd.DataFrame), f"{df_name} is {type(df)} not DataFrame!"
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty, f"{df_name} is empty!"


@pytest.mark.parametrize(
    "df_name",
    [
        "predicted_state_hourly_demand",
    ],
)
def test_state_demand_outputs(pudl_engine, df_name):
    """Test state demand analysis methods."""
    df = pd.read_sql(df_name, pudl_engine)
    assert isinstance(df, pd.DataFrame), f"{df_name} is {type(df)} not DataFrame!"
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty, f"{df_name} is empty!"
