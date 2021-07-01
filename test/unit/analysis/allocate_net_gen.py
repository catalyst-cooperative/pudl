"""Unit tests for allocation of net generation."""

import numpy as np
import pandas as pd

from pudl import helpers
from pudl.analysis import allocate_net_gen

# Reusable input files... (not sure if this is the right way to store them)

# inputs for example 1:
#  multi-generator-plant with one primary fuel type that fully reports to the
#  generation_eia923 table
gen_1 = pd.DataFrame(
    [
        [50307, 'GEN1', '2018-01-01', 14.0],
        [50307, 'GEN2', '2018-01-01', 1.0],
        [50307, 'GEN3', '2018-01-01', 0.0],
        [50307, 'GEN4', '2018-01-01', 0.0],
    ],
    columns=[
        'plant_id_eia', 'generator_id', 'report_date', 'net_generation_mwh']
)

gf_1 = pd.DataFrame(
    [
        [50307, 'ST', 'NG', '2018-01-01', 15.0, 117559.0],
        [50307, 'IC', 'DFO', '2018-01-01', 0.0, 0.0],
        [50307, 'IC', 'RFO', '2018-01-01', pd.NA, pd.NA],
        [50307, 'ST', 'RFO', '2018-01-01', pd.NA, pd.NA],
    ],
    columns=['plant_id_eia', 'prime_mover_code', 'fuel_type',
             'report_date', 'net_generation_mwh', 'fuel_consumed_mmbtu']
)

gens_1 = pd.DataFrame(
    [
        [50307, 'GEN1', '2018-01-01', 'ST', 7.5, 2,
            'NG', None, None, None, None, None],
        [50307, 'GEN2', '2018-01-01', 'ST', 2.5, 2,
            'NG', None, None, None, None, None],
        [50307, 'GEN3', '2018-01-01', 'ST', 2.5, 2,
            'NG', None, None, None, None, None],
        [50307, 'GEN4', '2018-01-01', 'ST', 4.3, 2,
            'NG', None, None, None, None, None],
        [50307, 'GEN5', '2018-01-01', 'IC', 1.8, 2,
            'DFO', None, None, None, None, None],
    ],
    columns=[
        'plant_id_eia', 'generator_id', 'report_date', 'prime_mover_code',
        'capacity_mw', 'fuel_type_count', 'energy_source_code_1',
        'energy_source_code_2', 'energy_source_code_3', 'energy_source_code_4',
        'energy_source_code_5', 'energy_source_code_6']
)


def test__associate_generator_tables_1():
    """Test associate_generator_tables function with example 1."""
    gen_assoc_1_expected = pd.DataFrame(
        [
            [50307, 'GEN1', '2018-01-01', 'ST', 7.5, 2.0,
                'energy_source_code_1', 'NG', 14.0, 15.0, 117559.0, 16.8, 15.0],
            [50307, 'GEN2', '2018-01-01', 'ST', 2.5, 2.0,
                'energy_source_code_1', 'NG', 1.0, 15.0, 117559.0, 16.8, 15.0],
            [50307, 'GEN3', '2018-01-01', 'ST', 2.5, 2.0,
                'energy_source_code_1', 'NG', 0.0, 15.0, 117559.0, 16.8, 15.0],
            [50307, 'GEN4', '2018-01-01', 'ST', 4.3, 2.0,
                'energy_source_code_1', 'NG', 0.0, 15.0, 117559.0, 16.8, 15.0],
            [50307, 'GEN5', '2018-01-01', 'IC', 1.8, 2.0,
                'energy_source_code_1', 'DFO', np.nan, 0.0, 0.0, 1.8, np.nan],
            [50307, np.nan, '2018-01-01', 'IC', np.nan, np.nan,
                np.nan, 'RFO', np.nan, np.nan, np.nan, np.nan, np.nan],
            [50307, np.nan, '2018-01-01', 'ST', np.nan, np.nan,
                np.nan, 'RFO', np.nan, np.nan, np.nan, np.nan, np.nan],
        ],
        columns=['plant_id_eia', 'generator_id', 'report_date', 'prime_mover_code', 'capacity_mw', 'fuel_type_count', 'energy_source_code_num',
                 'fuel_type', 'net_generation_mwh_g_tbl', 'net_generation_mwh_gf_tbl', 'fuel_consumed_mmbtu', 'capacity_mw_fuel', 'net_generation_mwh_g_tbl_fuel']


    ).pipe(helpers.convert_cols_dtypes, 'eia')

    gen_assoc_1_actual = (
        allocate_net_gen.associate_generator_tables(
            gf=gf_1, gen=gen_1, gens=gens_1)
        .pipe(helpers.convert_cols_dtypes, 'eia')
    )

    pd.testing.assert_frame_equal(gen_assoc_1_expected, gen_assoc_1_actual)


def test__allocate_gen_fuel_by_gen_pm_fuel_1():
    """Test allocate_gen_fuel_by_gen_pm_fuel function with example 1."""
    gen_pm_fuel_1_expected = pd.DataFrame(
        [
            [50307, 'ST', 'NG', '2018-01-01', 'GEN1', 14.0, 109721.73],
            [50307, 'ST', 'NG', '2018-01-01', 'GEN2', 1.0, 7837.26],
            [50307, 'ST', 'NG', '2018-01-01', 'GEN3', 0.0, 0.0],
            [50307, 'ST', 'NG', '2018-01-01', 'GEN4', 0.0, 0.0],
            [50307, 'IC', 'DFO', '2018-01-01', 'GEN5', 0.0, 0.0],
        ],
        columns=[
            'plant_id_eia', 'prime_mover_code', 'fuel_type', 'report_date', 'generator_id',
            'net_generation_mwh', 'fuel_consumed_mmbtu', ]
    ).pipe(helpers.convert_cols_dtypes, 'eia')

    gen_pm_fuel_1_actual = allocate_net_gen.allocate_gen_fuel_by_gen_pm_fuel(
        gf=gf_1, gen=gen_1, gens=gens_1
    )

    pd.testing.assert_frame_equal(gen_pm_fuel_1_expected, gen_pm_fuel_1_actual)

    # gen_pm_fuel_1_expected is an inputs into agg_by_generator().. so should I
    # test this here??
    #
    # testing the aggregation to the generator level for example 1.
    # in this case, each generator has one prime mover and one fuel source so
    # they are effectively the same.
    gen_out_1_expected = pd.DataFrame(
        [
            [50307, 'GEN1', '2018-01-01', 14.0, 109721.73],
            [50307, 'GEN2', '2018-01-01', 1.0, 7837.26],
            [50307, 'GEN3', '2018-01-01', 0.0, 0.0],
            [50307, 'GEN4', '2018-01-01', 0.0, 0.0],
            [50307, 'GEN5', '2018-01-01', 0.0, 0.0],
        ],
        columns=['plant_id_eia', 'generator_id', 'report_date',
                 'net_generation_mwh', 'fuel_consumed_mmbtu']
    )
    gen_out_1_actual = allocate_net_gen.agg_by_generator(gen_pm_fuel_1_actual)
    pd.testing.assert_frame_equal(gen_out_1_expected, gen_out_1_actual)
