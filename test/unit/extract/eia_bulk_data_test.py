"""Tests for eia_bulk_data module."""
from io import BytesIO

import numpy as np
import pandas as pd
import pytest

import pudl.extract.eia_bulk_data as bulk


@pytest.fixture()
def elec_txt_dataframe() -> pd.DataFrame:
    """Simulate raw ELEC.txt."""
    # actual subset of the bulk data file, but with data values truncated to only 2020 and 2021
    test_file = b"""
    {"series_id":"ELEC.RECEIPTS.SUB-KY-94.Q","name":"Receipts of fossil fuels by electricity plants : subbituminous coal : Kentucky : independent power producers (total) : quarterly","units":"thousand tons","f":"Q","description":"Subbituminous Coal; Power plants owned by unregulated power companies (also called merchant generators); ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA-KY","geography":"USA-KY","start":"2015Q1","end":"2015Q2","last_updated":"2015-08-25T15:52:58-04:00","geoset_id":"ELEC.RECEIPTS.SUB-94.Q","data":[["2015Q2",31.691],["2015Q1",44.397]]}
    {"series_id":"ELEC.COST_BTU.NG-US-2.Q","name":"Average cost of fossil fuels for electricity generation (per Btu) : natural gas : United States : electric utility non-cogen : quarterly","units":"dollars per million Btu","f":"Q","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008Q1","end":"2022Q1","last_updated":"2022-05-24T10:42:22-04:00","geoset_id":"ELEC.COST_BTU.NG-2.Q","data":[["2021Q4",4.995772961839175],["2021Q3",4.151767110444698],["2021Q2",2.8827941976335474],["2021Q1",7.802182875710838],["2020Q4",2.4484731967412308],["2020Q3",2.0102033299161364],["2020Q2",1.7616810963957992],["2020Q1",2.105433009656698]]}
    {"series_id":"ELEC.COST_BTU.NG-US-2.A","name":"Average cost of fossil fuels for electricity generation (per Btu) : natural gas : United States : electric utility non-cogen : annual","units":"dollars per million Btu","f":"A","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008","end":"2021","last_updated":"2022-02-25T15:25:17-05:00","geoset_id":"ELEC.COST_BTU.NG-2.A","data":[["2021",null],["2020",null]]}
    {"series_id":"ELEC.RECEIPTS_BTU.NG-US-2.Q","name":"Receipts of fossil fuels by electricity plants (Btu) : natural gas : United States : electric utility non-cogen : quarterly","units":"billion Btu","f":"Q","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008Q1","end":"2022Q1","last_updated":"2022-05-24T10:42:22-04:00","geoset_id":"ELEC.RECEIPTS_BTU.NG-2.Q","data":[["2021Q4",971009.16934],["2021Q3",1247751.19006],["2021Q2",924073.00164],["2021Q1",844665.11442],["2020Q4",972282.3858],["2020Q3",1439140.59549],["2020Q2",959842.7605],["2020Q1",1000417.64012]]}
    {"series_id":"ELEC.RECEIPTS_BTU.NG-US-2.A","name":"Receipts of fossil fuels by electricity plants (Btu) : natural gas : United States : electric utility non-cogen : annual","units":"billion Btu","f":"A","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008","end":"2021","last_updated":"2022-02-25T15:25:17-05:00","geoset_id":"ELEC.RECEIPTS_BTU.NG-2.A","data":[["2021",3987498.47545],["2020",4371683.38189]]}
    """
    buffer = BytesIO(test_file)
    df = pd.read_json(buffer, lines=True)
    return df


def test__filter_df(elec_txt_dataframe):
    """Filter for only the desired data series."""
    input_ = elec_txt_dataframe
    expected = input_.iloc[1:, :]
    actual = bulk._filter_df(input_)
    pd.testing.assert_frame_equal(actual, expected)


def test__extract_keys_from_series_id(elec_txt_dataframe):
    """Parse keys from EIA series_id string."""
    input_ = elec_txt_dataframe.iloc[[3], :]
    expected = pd.DataFrame(
        {
            "series": ["RECEIPTS_BTU"],
            "fuel": ["NG"],
            "region": ["US"],
            "sector": ["2"],
            "frequency": ["Q"],
        },
        index=[3],
    )
    actual = bulk._extract_keys_from_series_id(input_)
    pd.testing.assert_frame_equal(actual, expected)


def test__extract_keys_from_name(elec_txt_dataframe):
    """Parse keys from EIA name string."""
    input_ = elec_txt_dataframe.iloc[[3], :]
    expected = pd.DataFrame(
        {
            "series": ["Receipts of fossil fuels by electricity plants (Btu)"],
            "fuel": ["natural gas"],
            "region": ["United States"],
            "sector": ["electric utility non-cogen"],
            "frequency": ["quarterly"],
        },
        index=[3],
    )
    actual = bulk._extract_keys_from_name(input_)
    pd.testing.assert_frame_equal(actual, expected)


def test__parse_data_column(elec_txt_dataframe):
    """Convert a pd.Series of python lists to a dataframe."""
    input_ = elec_txt_dataframe.iloc[[2, 4], :]  # only annual series for easier testing
    expected = pd.DataFrame(
        {
            "series_id": [
                "ELEC.COST_BTU.NG-US-2.A",
                "ELEC.COST_BTU.NG-US-2.A",
                "ELEC.RECEIPTS_BTU.NG-US-2.A",
                "ELEC.RECEIPTS_BTU.NG-US-2.A",
            ],
            "date": [
                pd.Timestamp("2021"),
                pd.Timestamp("2020"),
                pd.Timestamp("2021"),
                pd.Timestamp("2020"),
            ],
            "value": [
                np.nan,
                np.nan,
                3987498.47545,
                4371683.38189,
            ],
        },
    )
    expected.loc[:, "series_id"] = expected.loc[:, "series_id"].astype(
        "category", copy=False
    )

    actual = bulk._parse_data_column(input_)
    pd.testing.assert_frame_equal(actual, expected)
