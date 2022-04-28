"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging
import os
import sys

import geopandas as gpd
import pandas as pd
import pytest

import pudl
import pudl.validate as pv

logger = logging.getLogger(__name__)

# This avoids trying to use the EIA API key when CI is run by a bot that doesn't
# have access to our GitHub secrets
API_KEY_EIA = os.environ.get("API_KEY_EIA", False)
if API_KEY_EIA:
    logger.info("Found an API_KEY_EIA in the environment.")
else:
    logger.warning("API_KEY_EIA was not available from the environment.")
FILL_FUEL_COST = bool(API_KEY_EIA)


@pytest.fixture(scope="module")
def fast_out(pudl_engine, pudl_datastore_fixture):
    """A PUDL output object for use in CI."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        ds=pudl_datastore_fixture,
        freq="MS",
        fill_fuel_cost=FILL_FUEL_COST,
        roll_fuel_cost=True,
        fill_net_gen=False,
        fill_tech_desc=True,
    )


@pytest.fixture(scope="module")
def fast_out_annual(pudl_engine, pudl_datastore_fixture):
    """A PUDL output object for use in CI."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        ds=pudl_datastore_fixture,
        freq="AS",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
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
        ("gf_nonuclear_eia923", 0.0, 0.0),
        ("gf_nuclear_eia923", 1.0, 0.001),
    ],
)
def test_nuclear_fraction(fast_out, df_name, expected_nuke_fraction, tolerance):
    """Ensure that overall nuclear generation fractions are as expected."""
    actual_nuke_fraction = nuke_gen_fraction(fast_out.__getattribute__(df_name)())
    assert abs(actual_nuke_fraction - expected_nuke_fraction) <= tolerance


@pytest.mark.parametrize(
    "df_name",
    [
        "all_plants_ferc1",
        "fbp_ferc1",
        "fuel_ferc1",
        "plant_in_service_ferc1",
        "plants_hydro_ferc1",
        "plants_pumped_storage_ferc1",
        "plants_small_ferc1",
        "plants_steam_ferc1",
        "pu_ferc1",
        "purchased_power_ferc1",
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
        ("gens_eia860", "pu_eia860", 1 / 1, {}),
        ("gens_eia860", "utils_eia860", 1 / 1, {}),
        ("gens_eia860", "bf_eia923", 12 / 1, {}),
        ("gens_eia860", "frc_eia923", 12 / 1, {}),
        ("gens_eia860", "gen_eia923", 12 / 1, {}),
        # gen_fuel_by_generator_eia923 currently only produces annual results.
        ("gens_eia860", "gen_fuel_by_generator_eia923", 1 / 1, {}),
        ("gens_eia860", "gf_eia923", 12 / 1, {}),
        ("gens_eia860", "gf_nonuclear_eia923", 12 / 1, {}),
        ("gens_eia860", "gf_nuclear_eia923", 12 / 1, {}),
        ("gens_eia860", "hr_by_unit", 12 / 1, {}),
        ("gens_eia860", "hr_by_gen", 12 / 1, {}),
        ("gens_eia860", "fuel_cost", 12 / 1, {}),
        ("gens_eia860", "capacity_factor", 12 / 1, {}),
        ("gens_eia860", "mcoe", 12 / 1, {"all_gens": False}),
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
        "gen_fuel_by_generator_energy_source_eia923",
        "gen_fuel_by_generator_eia923",
        "gen_fuel_by_generator_energy_source_owner_eia923",
    ],
)
def test_annual_eia_outputs(fast_out, df_name):
    """Check that the annual EIA 1 output functions work."""
    logger.info(f"Running fast_out.{df_name}()")
    df = fast_out.__getattribute__(df_name)()
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty


@pytest.mark.parametrize(
    "df_name,thresh",
    [
        ("mcoe", 0.9),
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


def test_eia861_etl(fast_out):
    """Make sure that the EIA 861 Extract-Transform steps work."""
    fast_out.etl_eia861()
    eia861_tables = [tbl for tbl in fast_out._dfs if "_eia861" in tbl]
    for df_name in eia861_tables:
        logger.info(f"Checking that {df_name} is a non-empty DataFrame")
        df = fast_out.__getattribute__(df_name)()
        assert isinstance(df, pd.DataFrame), f"{df_name} is {type(df)}, not DataFrame!"
        assert not df.empty, f"{df_name} is empty!"


def test_ferc714_etl(fast_out):
    """Make sure that the FERC 714 Extract-Transform steps work."""
    fast_out.etl_ferc714()
    ferc714_tables = [tbl for tbl in fast_out._dfs if "_ferc714" in tbl]
    for df_name in ferc714_tables:
        logger.info(f"Checking that {df_name} is a non-empty DataFrame")
        df = fast_out.__getattribute__(df_name)()
        assert isinstance(df, pd.DataFrame), f"{df_name} is {type(df)} not DataFrame!"
        assert not df.empty, f"{df_name} is empty!"


@pytest.fixture(scope="module")
def ferc714_out(fast_out, pudl_settings_fixture):
    """A FERC 714 Respondents output object for use in CI."""
    return pudl.output.ferc714.Respondents(
        fast_out,
        pudl_settings=pudl_settings_fixture,
    )


@pytest.mark.parametrize(
    "df_name",
    [
        "annualize",
        "categorize",
        "summarize_demand",
        "fipsify",
    ],
)
def test_ferc714_outputs(ferc714_out, df_name):
    """Test FERC 714 derived output methods."""
    logger.info(f"Running ferc714_out.{df_name}()")
    df = ferc714_out.__getattribute__(df_name)()
    assert isinstance(df, pd.DataFrame), f"{df_name} is {type(df)} not DataFrame!"
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty, f"{df_name} is empty!"


@pytest.mark.xfail(
    (sys.platform != "linux") & (not os.environ.get("CONDA_PREFIX", False)),
    reason="Test relies on ogr2ogr being installed via GDAL.",
)
def test_ferc714_respondents_georef_counties(ferc714_out):
    """Test FERC 714 respondent county FIPS associations.

    This test works with the Census DP1 data, which is converted into
    SQLite using the GDAL command line tool ogr2ogr. That tools is easy
    to install via conda or on Linux, but is more challenging on Windows
    and MacOS, so this test is marked xfail conditionally if the user is
    neither using conda, nor is on Linux.

    """
    ferc714_gdf = ferc714_out.georef_counties()
    assert isinstance(ferc714_gdf, gpd.GeoDataFrame), "ferc714_gdf not a GeoDataFrame!"
    assert not ferc714_gdf.empty, "ferc714_gdf is empty!"


def test_plant_parts_eia_filled(fast_out_annual):
    """Ensure the EIA plant-parts list can be generated."""
    fast_out_annual.plant_parts_eia()


@pytest.fixture(scope="module")
def fast_out_filled(pudl_engine, pudl_datastore_fixture):
    """A PUDL output object for use in CI with net generation filled."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        ds=pudl_datastore_fixture,
        freq="MS",
        fill_fuel_cost=FILL_FUEL_COST,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )


@pytest.mark.parametrize(
    "df_name,expected_nuke_fraction,tolerance",
    [
        ("gf_nuclear_eia923", 1.0, 0.001),
        ("gf_nonuclear_eia923", 0.0, 0.0),
        ("gf_eia923", 0.2, 0.02),
        ("mcoe", 0.2, 0.02),
    ],
)
def test_mcoe_filled(fast_out_filled, df_name, expected_nuke_fraction, tolerance):
    """Test that the net generation allocation process is working.

    In addition to running the allocation itself, make sure that the nuclear and
    non-nuclear generation fractions are as we would expect after the net generation has
    been allocated.
    """
    actual_nuke_fraction = nuke_gen_fraction(
        fast_out_filled.__getattribute__(df_name)()
    )
    assert abs(actual_nuke_fraction - expected_nuke_fraction) <= tolerance
