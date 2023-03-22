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


@pytest.fixture(scope="module")
def fast_out_annual(pudl_engine):
    """A PUDL output object for use in CI."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
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
        ("gens_eia860", "gen_fuel_by_generator_eia923", 12 / 1, {}),
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
    """Check that the EIA 1 output functions work."""
    logger.info(f"Running fast_out.{df_name}()")
    df = fast_out.__getattribute__(df_name)()
    logger.info(f"Found {len(df)} rows in {df_name}")
    assert not df.empty


@pytest.mark.parametrize(
    "df_name",
    ["plant_parts_eia", "ferc1_eia"],
)
def test_annual_only_outputs(fast_out_annual, df_name):
    """Check that output methods that only operate with an ``AS`` frequency."""
    logger.info(f"Running fast_out_annual.{df_name}()")
    df = fast_out_annual.__getattribute__(df_name)()
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


@pytest.fixture(scope="module")
def ferc714_out(fast_out, pudl_settings_fixture, pudl_datastore_fixture):
    """A FERC 714 Respondents output object for use in CI."""
    return pudl.output.ferc714.Respondents(
        fast_out, pudl_settings=pudl_settings_fixture, ds=pudl_datastore_fixture
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

    This test works with the Census DP1 data, which is converted into SQLite using the
    GDAL command line tool ogr2ogr. That tools is easy to install via conda or on Linux,
    but is more challenging on Windows and MacOS, so this test is marked xfail
    conditionally if the user is neither using conda, nor is on Linux.
    """
    ferc714_gdf = ferc714_out.georef_counties()
    assert isinstance(ferc714_gdf, gpd.GeoDataFrame), "ferc714_gdf not a GeoDataFrame!"
    assert not ferc714_gdf.empty, "ferc714_gdf is empty!"


@pytest.fixture(scope="module")
def fast_out_filled(pudl_engine):
    """A PUDL output object for use in CI with net generation filled."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        freq="MS",
        fill_fuel_cost=True,
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

    In addition to running the allocation itself, make sure that the nuclear and non-
    nuclear generation fractions are as we would expect after the net generation has
    been allocated.
    """
    actual_nuke_fraction = nuke_gen_fraction(
        fast_out_filled.__getattribute__(df_name)()
    )
    assert abs(actual_nuke_fraction - expected_nuke_fraction) <= tolerance


@pytest.mark.parametrize(
    "variation, check",
    [
        ("test_local", "df_equal"),
        ("test_invalid_engine_url", "df_equal"),
        ("test_local", "same_keys"),
        ("test_invalid_engine_url", "same_keys"),
        ("test_local", "valid_db"),
        ("test_invalid_engine_url", "valid_db"),
    ],
)
def test_pudltabl_pickle(
    pudl_engine,
    pudl_settings_fixture,
    monkeypatch,
    variation,
    check,
):
    """Test that PudlTabl is serializable with pickle.

    Because pickle is insecure, bandit must be quieted for some lines of this test. This
    test attempts to simulate the situation where PudlTabl is restored in the same
    environment that created it 'test_local' and in a different environment from the one
    that created it 'test_invalid_engine_url'.
    """
    import pickle  # nosec
    from io import BytesIO

    pudl_out_pickle = pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        freq="AS",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )

    # need to monkeypatch `get_defaults` because it is used in PudlTabl.__setstate__
    # and the real one does not work in GitHub actions because the settings file it
    # uses does not exist
    monkeypatch.setattr(
        "pudl.workspace.setup.get_defaults", lambda: pudl_settings_fixture
    )
    # make sure there's a df to pickle
    plants = pudl_out_pickle.plants_eia860()
    if variation == "test_invalid_engine_url":
        import sqlalchemy as sa

        # need to change the pudl_engine to one with an invalid URL so that
        # `PudlTabl.__setstate__` has to fall back on the local default
        pudl_out_pickle.pudl_engine = sa.create_engine("sqlite:////wrong/url")

        # confirm this engine won't work
        with pytest.raises(sa.exc.OperationalError):
            pudl_out_pickle.pudl_engine.connect()

    # just to make sure we keep all the parts
    keys = set(pudl_out_pickle.__dict__.keys())
    # dump the object into a pickle stored in a buffer
    pickle.dump(pudl_out_pickle, buffer := BytesIO())
    # restore the object from the pickle in the buffer
    restored = pickle.loads(buffer.getvalue())  # nosec

    if check == "df_equal":
        # make sure the df was properly restored
        pd.testing.assert_frame_equal(restored._dfs["plants_eia860"], plants)
    elif check == "same_keys":
        # check that the restored version has all the correct attributes
        assert set(restored.__dict__.keys()) == keys
    elif check == "valid_db":
        # check that the restored DB is valid
        assert bool(restored.pudl_engine.connect())
