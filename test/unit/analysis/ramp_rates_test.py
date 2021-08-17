"""Unit tests for the :mod:`pudl.analysis.ramp_rates` module."""
from typing import Dict, Sequence

import pandas as pd
import pytest

# import pudl.analysis.ramp_rates as rr


def df_from_product(inputs: Dict[str, Sequence], as_index=True) -> pd.DataFrame:
    """Make a dataframe from cartesian product of input sequences.

    Args:
        inputs (Dict[str, Sequence]): dataframe column names mapped
            to their unique values.
        as_index (bool): whether to set the product as the index

    Return:
        df (pd.DataFrame): cartesian product dataframe
    """
    names, iterables = zip(*inputs.items())
    df = pd.MultiIndex.from_product(
        iterables, names=names).to_frame(index=as_index)

    return df


@pytest.fixture()
def dummy_cems():
    """
    EPA CEMS.

    idx unit_id_epa     operating_datetime_utc  plant_id  unitid  gross_load_mw
    0             0  2019-12-31 22:00:00+00:00        10       a              0
    1             0  2019-12-31 23:00:00+00:00        10       a              1
    2             0  2020-01-01 00:00:00+00:00        10       a              2
    3             0  2020-01-01 01:00:00+00:00        10       a              3
    4             1  2019-12-31 22:00:00+00:00        11       a              4
    5             1  2019-12-31 23:00:00+00:00        11       a              5
    6             1  2020-01-01 00:00:00+00:00        11       a              6
    7             1  2020-01-01 01:00:00+00:00        11       a              7
    8             2  2019-12-31 22:00:00+00:00        11       b              8
    9             2  2019-12-31 23:00:00+00:00        11       b              9
    10            2  2020-01-01 00:00:00+00:00        11       b             10
    11            2  2020-01-01 01:00:00+00:00        11       b             11
    """
    inputs = dict(
        unit_id_epa=[0, 1, 2],
        operating_datetime_utc=pd.date_range(
            start="2019-12-31 22:00",
            end="2020-01-01 01:00",
            freq="h",
            tz='UTC'
        ),
    )
    cems = df_from_product(inputs, as_index=False)
    # add composite keys
    cems['plant_id'] = cems['unit_id_epa'].map({0: 10, 1: 11, 2: 11})
    cems['unitid'] = cems['unit_id_epa'].map({0: 'a', 1: 'a', 2: 'b'})
    # add values
    cems['gross_load_mw'] = range(len(cems))

    return cems


@pytest.fixture()
def dummy_crosswalk():
    """EPA Crosswalk."""
    raise NotImplementedError
