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
import pandas as pd
from pudl import init, mcoe, outputs
from pudl import constants as pc

start_date = pd.to_datetime(str(min(pc.working_years['eia923'])))
end_date = pd.to_datetime('{}-12-31'.format(max(pc.working_years['eia923'])))


@pytest.fixture(scope='module', params=['MS', 'AS'])
def output_byfreq(live_pudl_db, request):
    pudl_out = outputs.PudlOutput(
        freq=request.param, testing=(not live_pudl_db),
        start_date=start_date, end_date=end_date
    )
    return(pudl_out)


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_capacity_factor(output_byfreq):
    """Test the capacity factor calculation."""
    cf = mcoe.capacity_factor(output_byfreq)
    print("capacity_factor: {} records found".format(len(cf)))


@pytest.mark.eia860
@pytest.mark.mcoe
@pytest.mark.post_etl
def test_bga(output_byfreq):
    """Test the boiler generator associations."""
    bga = output_byfreq.bga()
    gens_simple = output_byfreq.gens_eia860()[['report_date',
                                               'plant_id_eia',
                                               'generator_id',
                                               'fuel_type_pudl']]
    bga_gens = bga[['report_date', 'plant_id_eia',
                    'unit_id_pudl', 'generator_id']].drop_duplicates()

    gens_simple = pd.merge(gens_simple, bga_gens,
                           on=['report_date', 'plant_id_eia', 'generator_id'],
                           validate='one_to_one')
    units_simple = gens_simple.drop('generator_id', axis=1).drop_duplicates()
    units_fuel_count = \
        units_simple.groupby(
            ['report_date',
             'plant_id_eia',
             'unit_id_pudl'])['fuel_type_pudl'].count().reset_index()
    units_fuel_count.rename(
        columns={'fuel_type_pudl': 'fuel_type_count'}, inplace=True)
    units_simple = pd.merge(units_simple, units_fuel_count,
                            on=['report_date', 'plant_id_eia', 'unit_id_pudl'])
    num_multi_fuel_units = len(units_simple[units_simple.fuel_type_count > 1])
    multi_fuel_unit_fraction = num_multi_fuel_units / len(units_simple)
    print('''NOTE: {:.0%} of generation units contain generators with\
differing primary fuels.'''.format(multi_fuel_unit_fraction))


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_heat_rate(output_byfreq):
    """Run heat rate calculation."""
    print("Calculating heat rates by generation unit...")
    hr_by_unit = mcoe.heat_rate_by_unit(output_byfreq)
    print("    heat_rate: {} unit records found".format(len(hr_by_unit)))
    assert single_records(
        hr_by_unit,
        key_cols=['report_date', 'plant_id_eia', 'unit_id_pudl']),\
        "Found non-unique unit heat rates!"

    print("Re-calculating heat rates by individual generator...")
    hr_by_gen = mcoe.heat_rate_by_gen(output_byfreq)
    print("    heat_rate: {} generator records found".format(len(hr_by_gen)))
    assert single_records(hr_by_gen),\
        "Found non-unique generator heat rates!"


@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.mcoe
def test_fuel_cost(output_byfreq):
    """Run fuel cost calculation."""
    print("Calculating fuel costs by individual generator...")
    fc = output_byfreq.fuel_cost()
    print("    fuel_cost: {} records found".format(len(fc)))
    assert single_records(fc),\
        "Found non-unique generator fuel cost records!"


def single_records(df,
                   key_cols=['report_date', 'plant_id_eia', 'generator_id']):
    """Test whether dataframe has a single record per generator."""
    len_1 = len(df)
    len_2 = len(df.drop_duplicates(subset=key_cols))
    if len_1 == len_2:
        return True
    else:
        return False


def nonunique_gens(df,
                   key_cols=['plant_id_eia', 'generator_id', 'report_date']):
    """Generate a list of all the non-unique generator records for testing."""
    unique_gens = df.drop_duplicates(subset=key_cols)
    dupes = df[~df.isin(unique_gens)].dropna()
    dupes = dupes.sort_values(by=key_cols)
    return(dupes)
