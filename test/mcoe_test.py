"""
Test MCOE (marginal cost of electricity) module functionality.

This set of test attempts to exercise all of the functions which are used in
the calculation of the marginal cost of electricity (MCOE), based on fuel
costs reported to EIA, and non-fuel operating costs reported to FERC.  Much
of what these functions do is attempt to correctly attribute data reported on a
per plant basis to individual generators.

This test module assumes that you have a working and up-to-date NON TEST PUDL
database.
"""
import pytest
from pudl import pudl, mcoe, analysis


@pytest.mark.eia860
@pytest.mark.post_etl
def test_mcoe_pulls_eia860(pudl_engine,
                           generators_pull_eia860,
                           boiler_generator_pull_eia860):
    """Test MCOE data pull functions that use EIA860 data."""


@pytest.mark.eia923
@pytest.mark.post_etl
def test_mcoe_pulls_eia923(pudl_engine,
                           generation_pull_eia923,
                           fuel_receipts_costs_pull_eia923,
                           boiler_fuel_pull_eia923):
    """Test MCOE data pull functions that use EIA923 data."""


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
def test_mcoe_calcs(pudl_engine,
                    generation_pull_eia923,
                    fuel_receipts_costs_pull_eia923,
                    boiler_fuel_pull_eia923,
                    boiler_generator_pull_eia860,
                    generators_pull_eia860):
    """Test MCOE data pull functions that use EIA923 data."""
    # We need to split these into individual values to pass them on
    (frc9_summed, frc9_summed_plant) = fuel_receipts_costs_pull_eia923
    (bf9_summed, bf9_plant_summed) = boiler_fuel_pull_eia923

    print("Calculating per-generator heat rates for MCOE...")
    heat_rate = analysis.heat_rate(boiler_generator_pull_eia860,
                                   generation_pull_eia923,
                                   bf9_summed, bf9_plant_summed,
                                   pudl_engine)

    print("Calculating per-generator fuel costs for MCOE...")
    fuel_cost = mcoe.fuel_cost(generators_pull_eia860,
                               generation_pull_eia923,
                               frc9_summed, frc9_summed_plant,
                               heat_rate)


@pytest.fixture(scope='module')
def boiler_generator_pull_eia860(pudl_engine):
    """Fixture for pulling boiler generator EIA860 dataframe for MCOE."""
    print("Pulling EIA860 boiler generator associations for MCOE...")
    return(mcoe.boiler_generator_pull_eia860(pudl_engine))


@pytest.fixture(scope='module')
def generators_pull_eia860(pudl_engine):
    """Fixture for pulling generator info from EIA860 dataframe for MCOE."""
    print("Pulling EIA860 generator data for MCOE...")
    return(mcoe.generators_pull_eia860(pudl_engine))


@pytest.fixture(scope='module')
def generation_pull_eia923(pudl_engine):
    """Fixture for pulling annualized EIA923 generation dataframe for MCOE."""
    print("Pulling annualized EI923 net generation data...")
    return(mcoe.generation_pull_eia923(pudl_engine))


@pytest.fixture(scope='module')
def fuel_receipts_costs_pull_eia923(pudl_engine):
    """Fixture for pulling annual EIA923 fuel receipts dataframe for MCOE."""
    print("Pulling EIA923 fuel receipts & costs data for MCOE...")
    return(mcoe.fuel_reciepts_costs_pull_eia923(pudl_engine))


@pytest.fixture(scope='module')
def boiler_fuel_pull_eia923(pudl_engine):
    """Fixture for pulling EIA923 boiler fuel consumed dataframe for MCOE."""
    print("Pulling EIA923 boiler fuel data for MCOE...")
    return(mcoe.boiler_fuel_pull_eia923(pudl_engine))
