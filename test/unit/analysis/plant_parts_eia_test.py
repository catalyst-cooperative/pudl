"""Tests for timeseries anomalies detection and imputation."""
import pandas as pd

import pudl.analysis.plant_parts_eia


def test_plant_part():
    """Test the different aggregations of the plant-part part list.

    The only data col we are testing here is capacity_mw.

    The below tests exclude the duplication of input records based on
    ownership.
    """
    gens_mega = pd.DataFrame({
        'plant_id_eia': [1, 1, 1, 1],
        'report_date': ['2020-01-01', '2020-01-01', '2020-01-01', '2020-01-01'],
        'utility_id_eia': [111, 111, 111, 111],
        'generator_id': ['a', 'b', 'c', 'd'],
        'prime_mover_code': ['ST', 'GT', 'CT', 'CA'],
        'energy_source_code_1': ['BIT', 'NG', 'NG', 'NG'],
        'ownership': ['total', 'total', 'total', 'total', ],
        'operational_status_pudl': ['operating', 'operating', 'operating', 'operating'],
        'capacity_mw': [400, 50, 125, 75],
    }).astype({'report_date': 'datetime64[ns]'})

    # test aggregation by plant
    plant_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name='plant')
        .ag_part_by_own_slice(
            gens_mega, sum_cols=['capacity_mw'], wtavg_dict={})
        .convert_dtypes()
    )

    plant_ag_expected = pd.DataFrame({
        'plant_id_eia': [1],
        'report_date': ['2020-01-01'],
        'operational_status_pudl': ['operating'],
        'utility_id_eia': [111],
        'ownership': ['total'],
        'capacity_mw': [650.0]
    }).astype({'report_date': 'datetime64[ns]'}).convert_dtypes()

    pd.testing.assert_frame_equal(plant_ag_out, plant_ag_expected)

    # test aggregation by plant prime fuel
    plant_primary_fuel_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name='plant_prime_fuel')
        .ag_part_by_own_slice(
            gens_mega, sum_cols=['capacity_mw'], wtavg_dict={})
        .convert_dtypes()
    )

    plant_primary_fuel_ag_expected = pd.DataFrame({
        'plant_id_eia': 1,
        'energy_source_code_1': ['BIT', 'NG'],
        'report_date': '2020-01-01',
        'operational_status_pudl': 'operating',
        'utility_id_eia': 111,
        'ownership': 'total',
        'capacity_mw': [400.0, 250.0]
    }).astype({'report_date': 'datetime64[ns]'}).convert_dtypes()

    pd.testing.assert_frame_equal(
        plant_primary_fuel_ag_out, plant_primary_fuel_ag_expected)

    # test aggregation by plant prime mover
    plant_prime_mover_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name='plant_prime_mover')
        .ag_part_by_own_slice(
            gens_mega, sum_cols=['capacity_mw'], wtavg_dict={})
        .convert_dtypes()
    )

    plant_prime_mover_ag_expected = pd.DataFrame({
        'plant_id_eia': 1,
        'prime_mover_code': ['CA', 'CT', 'GT', 'ST'],
        'report_date': '2020-01-01',
        'operational_status_pudl': 'operating',
        'utility_id_eia': 111,
        'ownership': 'total',
        'capacity_mw': [75.0, 125.0, 50.0, 400.0]
    }).astype({'report_date': 'datetime64[ns]'}).convert_dtypes()

    pd.testing.assert_frame_equal(
        plant_prime_mover_ag_out, plant_prime_mover_ag_expected)

    plant_gen_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name='plant_gen')
        .ag_part_by_own_slice(
            gens_mega, sum_cols=['capacity_mw'], wtavg_dict={})
        .convert_dtypes()
    )

    plant_gen_ag_expected = pd.DataFrame({
        'plant_id_eia': 1,
        'generator_id': ['a', 'b', 'c', 'd'],
        'report_date': '2020-01-01',
        'operational_status_pudl': 'operating',
        'utility_id_eia': 111,
        'ownership': 'total',
        'capacity_mw': [400.0, 50.0, 125.0, 75.0]
    }).astype({'report_date': 'datetime64[ns]'}).convert_dtypes()

    pd.testing.assert_frame_equal(plant_gen_ag_out, plant_gen_ag_expected)


def test_make_mega_gen_tbl():
    """Test the creation of the mega generator table.

    Integrates ownership with generators.
    """
    # one plant with three generators
    mcoe = pd.DataFrame({
        'plant_id_eia': 1,
        'report_date': '2020-01-01',
        'generator_id': ['a', 'b', 'c'],
        'utility_id_eia': [111, 111, 111],
        'unit_id_pudl': 1,
        'prime_mover_code': ['CT', 'CT', 'CA'],
        'technology_description': 'Natural Gas Fired Combined Cycle',
        'operational_status': 'existing',
        'retirement_date': pd.NA,
        'capacity_mw': [50, 50, 100],
    }).astype({
        'retirement_date': "datetime64[ns]",
        'report_date': "datetime64[ns]",
    })
    # one record for every owner of each generator
    df_own_eia860 = pd.DataFrame({
        'plant_id_eia': 1,
        'report_date': '2020-01-01',
        'generator_id': ['a', 'b', 'c', 'c'],
        'utility_id_eia': 111,
        'owner_utility_id_eia': [111, 111, 111, 888],
        'fraction_owned': [1, 1, .75, .25]
    }).astype({'report_date': "datetime64[ns]"})

    out = pudl.analysis.plant_parts_eia.MakeMegaGenTbl().execute(
        mcoe, df_own_eia860, slice_cols=['capacity_mw'])

    out_expected = pd.DataFrame({
        'plant_id_eia': 1,
        'report_date': '2020-01-01',
        'generator_id': ['a', 'b', 'c', 'c', 'a', 'b', 'c', 'c'],
        'unit_id_pudl': 1,
        'prime_mover_code': ['CT', 'CT', 'CA', 'CA', 'CT', 'CT', 'CA', 'CA'],
        'technology_description': 'Natural Gas Fired Combined Cycle',
        'operational_status': 'existing',
        'retirement_date': pd.NaT,
        'capacity_mw': [50.0, 50.0, 75.0, 25.0, 50.0, 50.0, 100.0, 100.0],
        'ferc_acct_name': 'Other',
        'operational_status_pudl': 'operating',
        'capacity_eoy_mw': [50, 50, 100, 100, 50, 50, 100, 100],
        'fraction_owned': [1.00, 1.00, .75, .25, 1.00, 1.00, 1.00, 1.00],
        'utility_id_eia': [111, 111, 111, 888, 111, 111, 111, 888],
        'ownership': ['owned', 'owned', 'owned', 'owned', 'total', 'total', 'total', 'total']
    }).astype({
        'retirement_date': "datetime64[ns]",
        'report_date': "datetime64[ns]",
        'utility_id_eia': "Int64"  # convert to pandas Int64 instead of numpy int64
    }).set_index([[0, 1, 2, 3, 0, 1, 2, 3]])

    pd.testing.assert_frame_equal(out, out_expected)


def test_slice_by_ownership():
    """Test the slice_by_ownership method."""
    dtypes = {
        'report_date': 'datetime64[ns]',
        'utility_id_eia': pd.Int64Dtype()
    }
    own_ex1 = pd.DataFrame(
        {'plant_id_eia': [1, 1, 1, 1],
         'report_date': ['2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01'],
         'generator_id': ['a', 'a', 'b', 'b'],
         'utility_id_eia': [3, 3, 3, 3],
         'owner_utility_id_eia': [3, 4, 3, 4],
         'fraction_owned': [.7, .3, .1, .9]
         },

    ).astype(dtypes)

    gens_mega_ex1 = pd.DataFrame(
        {'plant_id_eia':
         [1, 1],
         'report_date':
             ['2019-01-01', '2019-01-01', ],
         'generator_id':
             ['a', 'b', ],
         'utility_id_eia':
             [3, 3, ],
         'total_fuel_cost':
             [4500, 1250],
         'net_generation_mwh':
             [10000, 5000],
         'capacity_mw':
             [100, 50],
         'capacity_eoy_mw':
             [100, 50],
         'total_mmbtu':
             [9000, 7800],
         },
    ).astype(dtypes)

    out_ex1 = pd.DataFrame(
        {'plant_id_eia':
         [1, 1, 1, 1, 1, 1, 1, 1, ],
         'report_date':
             ['2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01',
                 '2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01', ],
         'generator_id':
             ['a', 'a', 'b', 'b', 'a', 'a', 'b', 'b', ],
         'total_fuel_cost':
             [4500 * .7, 4500 * .3, 1250 * .1, 1250 * .9,
              4500, 4500, 1250, 1250, ],
         'net_generation_mwh':
             [10000 * .7, 10000 * .3, 5000 * .1, 5000 * .9,
              10000, 10000, 5000, 5000, ],
         'capacity_mw':
             [100 * .7, 100 * .3, 50 * .1, 50 * .9,
              100, 100, 50, 50, ],
         'capacity_eoy_mw':
             [100 * .7, 100 * .3, 50 * .1, 50 * .9,
              100, 100, 50, 50, ],
         'total_mmbtu':
             [9000 * .7, 9000 * .3, 7800 * .1, 7800 * .9,
              9000, 9000, 7800, 7800, ],
         'fraction_owned':
             [.7, .3, .1, .9,
              1, 1, 1, 1],
         'utility_id_eia':
             [3, 4, 3, 4,
              3, 4, 3, 4],
         'ownership':
             ['owned', 'owned', 'owned', 'owned',
                 'total', 'total', 'total', 'total']
         },
    ).astype(dtypes)

    out = (
        pudl.analysis.plant_parts_eia.MakeMegaGenTbl()
        .slice_by_ownership(
            gens_mega=gens_mega_ex1,
            own_eia860=own_ex1
        )
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        out_ex1, out,
    )
