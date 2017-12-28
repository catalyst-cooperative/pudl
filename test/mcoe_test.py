"""
Test MCOE (marginal cost of electricity) module functionality.

This set of test attempts to exercise all of the functions which are used in
the calculation of the marginal cost of electricity (MCOE), based on fuel
costs reported to EIA, and non-fuel operating costs reported to FERC.  Much
of what these functions do is attempt to correctly attribute data reported on a
per plant basis to individual generators.

For now, these calculations are only using the EIA fuel cost data. FERC Form 1
non-fuel production costs have yet to be integrated.
"""
import pytest
from pudl import init, mcoe, outputs


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_capacity_factor(generators_eia860,
                         generation_eia923_as,
                         generation_eia923_ms):
    """Run capacity factor calculation."""
    print("Calculating annual capacity factors...")
    cap_fact_as = mcoe.capacity_factor(generators_eia860,
                                       generation_eia923_as,
                                       min_cap_fact=0,
                                       max_cap_fact=1.5)
    print("    capacity_factor: {} records found".format(len(cap_fact_as)))

    print("Calculating monthly capacity factors...")
    cap_fact_ms = mcoe.capacity_factor(generators_eia860,
                                       generation_eia923_ms,
                                       min_cap_fact=0,
                                       max_cap_fact=1.5)
    print("    capacity_factor: {} records found".format(len(cap_fact_ms)))

    assert len(cap_fact_ms) / len(cap_fact_as) == 12, \
        'Did not find 12x as many monthly as annual capacity factor records.'


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_heat_rate(heat_rate_as, heat_rate_ms):
    """Run heat rate calculation."""
    print("    heat_rate: {} annual records found".format(len(heat_rate_as)))
    assert mcoe.single_gens(heat_rate_as),\
        "Found non-unique annual generator heat rates!"
    print("    heat_rate: {} monthly records found".format(len(heat_rate_ms)))
    assert mcoe.single_gens(heat_rate_ms),\
        "Found non-unique monthly generator heat rates!"


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_fuel_cost(heat_rate_as,
                   heat_rate_ms,
                   fuel_receipts_costs_eia923_as,
                   fuel_receipts_costs_eia923_ms,
                   generation_eia923_as,
                   generation_eia923_ms):
    """Run fuel cost calculation."""
    print("Calculating annual fuel costs...")
    fuel_cost_as = mcoe.fuel_cost(heat_rate_as,
                                  fuel_receipts_costs_eia923_as,
                                  generation_eia923_as)
    print("    fuel_cost: {} annual records found".format(len(fuel_cost_as)))
    assert mcoe.single_gens(fuel_cost_as),\
        "Non-unique annual generator fuel cost records found."

    print("Calculating monthly fuel costs...")
    fuel_cost_ms = mcoe.fuel_cost(heat_rate_ms,
                                  fuel_receipts_costs_eia923_ms,
                                  generation_eia923_ms)
    print("    fuel_cost: {} monthly records found".format(len(fuel_cost_ms)))
    assert mcoe.single_gens(fuel_cost_ms),\
        "Non-unique monthly generator fuel cost records found."


@pytest.fixture(scope='module')
def boiler_generator_assn_eia860(live_pudl_db,
                                 start_date_eia923,
                                 end_date_eia923):
    """Fixture for pulling boiler generator associations from EIA860."""
    testing = (not live_pudl_db)
    print("Pulling EIA boiler generator associations for MCOE...")
    return(outputs.boiler_generator_assn_eia860(start_date=start_date_eia923,
                                                end_date=end_date_eia923,
                                                testing=testing))


@pytest.fixture(scope='module')
def generators_eia860(live_pudl_db, start_date_eia923, end_date_eia923):
    """Fixture for pulling generator info from EIA860 dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling EIA860 generator data for MCOE...")
    return(outputs.generators_eia860(start_date=start_date_eia923,
                                     end_date=end_date_eia923,
                                     testing=testing))


@pytest.fixture(scope='module')
def generation_eia923_as(live_pudl_db, start_date_eia923, end_date_eia923):
    """Fixture for pulling annualized EIA923 generation dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling annualized EI923 net generation data...")
    return(outputs.generation_eia923(freq='AS',
                                     start_date=start_date_eia923,
                                     end_date=end_date_eia923,
                                     testing=testing))


@pytest.fixture(scope='module')
def generation_eia923_ms(live_pudl_db, start_date_eia923, end_date_eia923):
    """Fixture for pulling monthly EIA923 generation dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling monthly EI923 net generation data...")
    return(outputs.generation_eia923(freq='MS',
                                     start_date=start_date_eia923,
                                     end_date=end_date_eia923,
                                     testing=testing))


@pytest.fixture(scope='module')
def fuel_receipts_costs_eia923_as(live_pudl_db,
                                  start_date_eia923,
                                  end_date_eia923):
    """Fixture for pulling annual EIA923 fuel receipts dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling annual EIA923 fuel receipts & costs data for MCOE...")
    return(outputs.fuel_receipts_costs_eia923(freq='AS',
                                              start_date=start_date_eia923,
                                              end_date=end_date_eia923,
                                              testing=testing))


@pytest.fixture(scope='module')
def fuel_receipts_costs_eia923_ms(live_pudl_db,
                                  start_date_eia923,
                                  end_date_eia923):
    """Fixture for pulling annual EIA923 fuel receipts dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling monthly EIA923 fuel receipts & costs data for MCOE...")
    return(outputs.fuel_receipts_costs_eia923(freq='MS',
                                              start_date=start_date_eia923,
                                              end_date=end_date_eia923,
                                              testing=testing))


@pytest.fixture(scope='module')
def boiler_fuel_eia923_as(live_pudl_db, start_date_eia923, end_date_eia923):
    """Fixture for pulling annual EIA923 boiler fuel dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling annual EIA923 boiler fuel data for MCOE...")
    return(outputs.boiler_fuel_eia923(freq='AS',
                                      start_date=start_date_eia923,
                                      end_date=end_date_eia923,
                                      testing=testing))


@pytest.fixture(scope='module')
def boiler_fuel_eia923_ms(live_pudl_db, start_date_eia923, end_date_eia923):
    """Fixture for pulling annual EIA923 boiler fuel dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling monthly EIA923 boiler fuel data for MCOE...")
    return(outputs.boiler_fuel_eia923(freq='MS',
                                      start_date=start_date_eia923,
                                      end_date=end_date_eia923,
                                      testing=testing))


@pytest.fixture(scope='module')
def boiler_generator_assn_eia(boiler_generator_assn_eia860,
                              generators_eia860,
                              generation_eia923_as, boiler_fuel_eia923_as,
                              live_pudl_db,
                              start_date_eia923,
                              end_date_eia923):
    """Fixture for pulling boiler generator associations dataframe for MCOE."""
    testing = (not live_pudl_db)
    print("Pulling EIA boiler generator associations for MCOE...")
    return(mcoe.boiler_generator_association(boiler_generator_assn_eia860,
                                             generators_eia860,
                                             generation_eia923_as,
                                             boiler_fuel_eia923_as,
                                             start_date=start_date_eia923,
                                             end_date=end_date_eia923,
                                             testing=testing))


@pytest.fixture(scope='module')
def heat_rate_as(boiler_generator_assn_eia,
                 generation_eia923_as,
                 boiler_fuel_eia923_as,
                 generators_eia860):
    """Fixture for annual heat rate dataframe."""
    print("Calculating annual heat rates...")
    # Remove all associations tagged as bad for one reason or another
    bga_good = boiler_generator_assn_eia[
        ~boiler_generator_assn_eia.missing_from_923 &
        ~boiler_generator_assn_eia.plant_w_bad_generator &
        ~boiler_generator_assn_eia.unmapped_but_in_923 &
        ~boiler_generator_assn_eia.unmapped
    ]
    bga_good = bga_good.drop(['missing_from_923',
                              'plant_w_bad_generator',
                              'unmapped_but_in_923',
                              'unmapped'], axis=1)
    bga_good = bga_good.drop_duplicates(subset=['report_date', 'plant_id_eia',
                                                'boiler_id', 'generator_id'])
    hr_annual = mcoe.heat_rate(bga_good,
                               generation_eia923_as,
                               boiler_fuel_eia923_as,
                               generators_eia860,
                               min_heat_rate=5.5)
    return(hr_annual)


@pytest.fixture(scope='module')
def heat_rate_ms(boiler_generator_assn_eia,
                 generation_eia923_ms,
                 boiler_fuel_eia923_ms,
                 generators_eia860):
    """Fixture for monthly heat rate dataframe."""
    print("Calculating monthly heat rates...")
    # Remove all associations tagged as bad for one reason or another
    bga_good = boiler_generator_assn_eia[
        ~boiler_generator_assn_eia.missing_from_923 &
        ~boiler_generator_assn_eia.plant_w_bad_generator &
        ~boiler_generator_assn_eia.unmapped_but_in_923 &
        ~boiler_generator_assn_eia.unmapped
    ]
    bga_good = bga_good.drop(['missing_from_923',
                              'plant_w_bad_generator',
                              'unmapped_but_in_923',
                              'unmapped'], axis=1)
    bga_good = bga_good.drop_duplicates(subset=['report_date', 'plant_id_eia',
                                                'boiler_id', 'generator_id'])
    hr_monthly = mcoe.heat_rate(bga_good,
                                generation_eia923_ms,
                                boiler_fuel_eia923_ms,
                                generators_eia860,
                                min_heat_rate=5.5)
    return(hr_monthly)
