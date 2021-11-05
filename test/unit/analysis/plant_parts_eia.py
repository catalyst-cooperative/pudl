"""Tests for timeseries anomalies detection and imputation."""
import pandas as pd

import pudl.analysis.plant_parts_eia


class PudlTablFake:
    """Fake PudlTabl class."""

    freq = 'AS'


def test_slice_by_ownership():
    """Test the slice_by_ownership method."""
    pu_fake = PudlTablFake()
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
         'capacity_mw_eoy':
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
         'capacity_mw_eoy':
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
        pudl.analysis.plant_parts_eia.MakeMegaGenTbl(pu_fake)
        .slice_by_ownership(
            gens_mega=gens_mega_ex1,
            own_eia860=own_ex1).reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        out_ex1, out,
    )
