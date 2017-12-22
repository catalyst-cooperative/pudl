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
                                       freq='AS',
                                       min_cap_fact=0,
                                       max_cap_fact=1.5)
    print("    capacity_factor: {} records found".format(len(cap_fact_as)))

    print("Calculating monthly capacity factors...")
    cap_fact_ms = mcoe.capacity_factor(generators_eia860,
                                       generation_eia923_ms,
                                       freq='MS',
                                       min_cap_fact=0,
                                       max_cap_fact=1.5)
    print("    capacity_factor: {} records found".format(len(cap_fact_ms)))

    assert len(cap_fact_ms) / len(cap_fact_as) == 12, \
        'Annual records are not 1/12 of monthly records'


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_heat_rate(heat_rate_as):
    """Run heat rate calculation."""
    pass


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_fuel_cost(heat_rate_as,
                   fuel_receipts_costs_eia923_as,
                   generation_eia923_as):
    """Run fuel cost calculation."""
    print("Calculating annual fuel cost...")
    fuel_cost_as = mcoe.fuel_cost(heat_rate_as,
                                  fuel_receipts_costs_eia923_as,
                                  generation_eia923_as)
    print("    fuel_cost: {} records found".format(len(fuel_cost_as)))

    pass


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_mcoe_calcs(pudl_engine,
                    generation_pull_eia923,
                    fuel_receipts_costs_pull_eia923,
                    boiler_fuel_pull_eia923,
                    boiler_generator_pull_eia860,
                    generators_pull_eia860):
    """Run the MCOE fuel cost and heat rate calculations."""
    # We need to split these into individual values to pass them on
    (frc9_summed, frc9_summed_plant) = fuel_receipts_costs_pull_eia923
    (bf9_summed, bf9_plant_summed) = boiler_fuel_pull_eia923
    (g8, g8_es) = generators_pull_eia860

    gens = mcoe.gens_with_bga(boiler_generator_pull_eia860,
                              generation_pull_eia923)

    # Spot check a few plants to ensure that their generators have been
    # assigned the expected association status. These dictionaries are
    # plant_id_eia: ['list','of','generator','ids']
    complete = {
        470: ['1', '2', '3'],
    }
    incomplete = {
        470: [],
    }
    for plant_id, gen_ids in complete.items():
        complete_mask = (gens.plant_id_eia == plant_id) & \
                        (gens.generator_id.isin(gen_ids))
        assert gens[complete_mask].complete_assn.all()

    for plant_id, gen_ids in incomplete.items():
        incomplete_mask = (gens.plant_id_eia == plant_id) & \
                          (gens.generator_id.isin(gen_ids))
        assert (~gens[incomplete_mask].complete_assn).all()

    print("Calculating per-generator heat rates for MCOE...")
    heat_rate = mcoe.heat_rate(g8_es, boiler_generator_pull_eia860,
                               generation_pull_eia923,
                               bf9_summed, bf9_plant_summed,
                               pudl_engine)

    print("Calculating per-generator fuel costs for MCOE...")
    fuel_cost = mcoe.fuel_cost(g8_es, generation_pull_eia923, frc9_summed,
                               frc9_summed_plant, heat_rate)


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
                 boiler_fuel_eia923_as):
    """Fixture for heat rate dataframe."""
    print("Calculating heat rate...")
    return(mcoe.heat_rate(boiler_generator_assn_eia,
                          generation_eia923_as,
                          boiler_fuel_eia923_as,
                          min_heat_rate=5.5))
