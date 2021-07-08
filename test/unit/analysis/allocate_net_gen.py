"""Unit tests for allocation of net generation."""

import numpy as np
import pandas as pd

from pudl import helpers
from pudl.analysis import allocate_net_gen

# Reusable input files...

# inputs for example 1:
#  multi-generator-plant with one primary fuel type that fully reports to the
#  generation_eia923 table
GEN_1 = pd.DataFrame({
    'plant_id_eia': [50307, 50307, 50307, 50307],
    'generator_id': ['GEN1', 'GEN2', 'GEN3', 'GEN4'],
    'report_date': ['2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', ],
    'net_generation_mwh': [14.0, 1.0, 0.0, 0.0],
}
)

GF_1 = pd.DataFrame({
    'plant_id_eia': [50307, 50307, 50307, 50307],
    'prime_mover_code': ['ST', 'IC', 'IC', 'ST'],
    'fuel_type': ['NG', 'DFO', 'RFO', 'RFO'],
    'report_date': ['2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', ],
    'net_generation_mwh': [15.0, 0.0, np.nan, np.nan],
    'fuel_consumed_mmbtu': [100000.0, 0.0, np.nan, np.nan],
},
)

GENS_1 = pd.DataFrame({
    'plant_id_eia': [50307, 50307, 50307, 50307, 50307],
    'generator_id': ['GEN1', 'GEN2', 'GEN3', 'GEN4', 'GEN5'],
    'report_date': ['2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', ],
    'prime_mover_code': ['ST', 'ST', 'ST', 'ST', 'IC'],
    'capacity_mw': [7.5, 2.5, 2.5, 4.3, 1.8],
    'fuel_type_count': [2, 2, 2, 2, 2],
    'energy_source_code_1': ['NG', 'NG', 'NG', 'NG', 'DFO'],
    'energy_source_code_2': [None, None, None, None, None],
    'energy_source_code_3': [None, None, None, None, None],
    'energy_source_code_4': [None, None, None, None, None],
    'energy_source_code_5': [None, None, None, None, None],
    'energy_source_code_6': [None, None, None, None, None],
    'planned_energy_source_code_1': [None, None, None, None, None],
},
)


def test__associate_generator_tables_1():
    """Test associate_generator_tables function with example 1."""
    gen_assoc_1_expected = pd.DataFrame({
        'plant_id_eia': [50307, 50307, 50307, 50307, 50307, 50307, 50307],
        'generator_id': ['GEN1', 'GEN2', 'GEN3', 'GEN4', 'GEN5', np.nan, np.nan],
        'report_date': ['2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', ],
        'prime_mover_code': ['ST', 'ST', 'ST', 'ST', 'IC', 'IC', 'ST'],
        'capacity_mw': [7.5, 2.5, 2.5, 4.3, 1.8, np.nan, np.nan],
        'fuel_type_count': [2.0, 2.0, 2.0, 2.0, 2.0, np.nan, np.nan],
        'energy_source_code_num': ['energy_source_code_1', 'energy_source_code_1', 'energy_source_code_1', 'energy_source_code_1', 'energy_source_code_1', np.nan, np.nan],
        'fuel_type': ['NG', 'NG', 'NG', 'NG', 'DFO', 'RFO', 'RFO'],
        'net_generation_mwh_g_tbl': [14.0, 1.0, 0.0, 0.0, np.nan, np.nan, np.nan],
        'net_generation_mwh_gf_tbl': [15.0, 15.0, 15.0, 15.0, 0.0, np.nan, np.nan],
        'fuel_consumed_mmbtu': [100000.0, 100000.0, 100000.0, 100000.0, 0.0, np.nan, np.nan],
        'capacity_mw_fuel': [16.8, 16.8, 16.8, 16.8, 1.8, np.nan, np.nan],
        'net_generation_mwh_g_tbl_fuel': [15.0, 15.0, 15.0, 15.0, np.nan, np.nan, np.nan],
    },
    ).pipe(helpers.convert_cols_dtypes, 'eia')

    gen_assoc_1_actual = (
        allocate_net_gen.associate_generator_tables(
            gf=GF_1, gen=GEN_1, gens=GENS_1)
        .pipe(helpers.convert_cols_dtypes, 'eia')
    )

    pd.testing.assert_frame_equal(gen_assoc_1_expected, gen_assoc_1_actual)


def test__allocate_gen_fuel_by_gen_pm_fuel_1():
    """Test allocate_gen_fuel_by_gen_pm_fuel function with example 1."""
    gen_pm_fuel_1_expected = pd.DataFrame(
        {
            'plant_id_eia': [50307, 50307, 50307, 50307, 50307],
            'prime_mover_code': ['ST', 'ST', 'ST', 'ST', 'IC'],
            'fuel_type': ['NG', 'NG', 'NG', 'NG', 'DFO'],
            'report_date': ['2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', ],
            'generator_id': ['GEN1', 'GEN2', 'GEN3', 'GEN4', 'GEN5'],
            'frac': [0.93, 0.066, 0.0, 0.0, 1.0],
            'net_generation_mwh_gf_tbl': [15.0, 15.0, 15.0, 15.0, 0.0],
            'net_generation_mwh_g_tbl': [14.0, 1.0, 0.0, 0.0, 0.0],
            'capacity_mw': [7.5, 2.5, 2.5, 4.3, 1.8],
            'fuel_consumed_mmbtu': [93333.33, 6666.66, 0.0, 0.0, 0.0],
            'net_generation_mwh': [14.0, 1.0, 0.0, 0.0, 0.0],
            'fuel_consumed_mmbtu_gf_tbl': [100000.0, 100000.0, 100000.0, 100000.0, 0.0],
        },

    ).pipe(helpers.convert_cols_dtypes, 'eia')

    gen_pm_fuel_1_actual = allocate_net_gen.allocate_gen_fuel_by_gen_pm_fuel(
        gf=GF_1, gen=GEN_1, gens=GENS_1
    )

    pd.testing.assert_frame_equal(gen_pm_fuel_1_expected, gen_pm_fuel_1_actual)

    # gen_pm_fuel_1_expected is an inputs into agg_by_generator().. so should I
    # test this here??
    #
    # testing the aggregation to the generator level for example 1.
    # in this case, each generator has one prime mover and one fuel source so
    # they are effectively the same.
    gen_out_1_expected = pd.DataFrame({
        'plant_id_eia': [50307, 50307, 50307, 50307, 50307],
        'generator_id': ['GEN1', 'GEN2', 'GEN3', 'GEN4', 'GEN5'],
        'report_date': ['2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', ],
        'net_generation_mwh': [14.0, 1.0, 0.0, 0.0, 0.0],
        'fuel_consumed_mmbtu': [93333.33, 6666.66, 0.0, 0.0, 0.0],
    },
    )
    gen_out_1_actual = allocate_net_gen.agg_by_generator(gen_pm_fuel_1_actual)
    pd.testing.assert_frame_equal(gen_out_1_expected, gen_out_1_actual)
