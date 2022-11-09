"""Tests for pudl.transform.eia_bulk_elec functions."""
from io import BytesIO

import pandas as pd
import pytest

import pudl.transform.eia_bulk_elec as bulk


@pytest.fixture()
def test_file_bytes() -> bytes:
    """Simulate raw ELEC.txt."""
    # actual subset of the bulk data file, but with data values truncated to only 2020 and 2021
    test_file = b"""
    {"series_id":"ELEC.COST_BTU.NG-US-2.Q","name":"Average cost of fossil fuels for electricity generation (per Btu) : natural gas : United States : electric utility non-cogen : quarterly","units":"dollars per million Btu","f":"Q","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008Q1","end":"2022Q1","last_updated":"2022-05-24T10:42:22-04:00","geoset_id":"ELEC.COST_BTU.NG-2.Q","data":[["2021Q4",4.995772961839175],["2021Q3",4.151767110444698],["2021Q2",2.8827941976335474],["2021Q1",7.802182875710838],["2020Q4",2.4484731967412308],["2020Q3",2.0102033299161364],["2020Q2",1.7616810963957992],["2020Q1",2.105433009656698]]}
    {"series_id":"ELEC.COST_BTU.NG-US-2.A","name":"Average cost of fossil fuels for electricity generation (per Btu) : natural gas : United States : electric utility non-cogen : annual","units":"dollars per million Btu","f":"A","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008","end":"2021","last_updated":"2022-02-25T15:25:17-05:00","geoset_id":"ELEC.COST_BTU.NG-2.A","data":[["2021",null],["2020",null]]}
    {"series_id":"ELEC.RECEIPTS_BTU.NG-US-2.Q","name":"Receipts of fossil fuels by electricity plants (Btu) : natural gas : United States : electric utility non-cogen : quarterly","units":"billion Btu","f":"Q","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008Q1","end":"2022Q1","last_updated":"2022-05-24T10:42:22-04:00","geoset_id":"ELEC.RECEIPTS_BTU.NG-2.Q","data":[["2021Q4",971009.16934],["2021Q3",1247751.19006],["2021Q2",924073.00164],["2021Q1",844665.11442],["2020Q4",972282.3858],["2020Q3",1439140.59549],["2020Q2",959842.7605],["2020Q1",1000417.64012]]}
    {"series_id":"ELEC.RECEIPTS_BTU.NG-US-2.A","name":"Receipts of fossil fuels by electricity plants (Btu) : natural gas : United States : electric utility non-cogen : annual","units":"billion Btu","f":"A","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008","end":"2021","last_updated":"2022-02-25T15:25:17-05:00","geoset_id":"ELEC.RECEIPTS_BTU.NG-2.A","data":[["2021",3987498.47545],["2020",4371683.38189]]}
    {"series_id":"ELEC.RECEIPTS_BTU.NG-US-2.M","name":"Receipts of fossil fuels by electricity plants (Btu) : natural gas : United States : electric utility non-cogen : monthly","units":"billion Btu","f":"M","description":"Natural Gas; Power plants owned by regulated electric utilties that produce electricity only; ","copyright":"None","source":"EIA, U.S. Energy Information Administration","iso3166":"USA","geography":"USA","start":"2008","end":"2021","last_updated":"2022-05-24T10:42:22-04:00","geoset_id":"ELEC.RECEIPTS_BTU.NG-2.M","data":[["202112",315651.25091],["202111",308818.09240],["202110",346539.82603],["202109",363177.75426],["202108",449157.70593],["202107",435415.72987],["202106",392843.02136],["202105",279308.50414],["202104",251921.47614],["202103",252818.90965],["202102",284471.48196],["202101",307374.72281],["202012",326252.51506],["202011",285763.24174],["202010",360266.62900],["202009",395627.81647],["202008",506306.27518],["202007",537206.50384],["202006",393583.82122],["202005",291905.33591],["202004",274353.60337],["202003",316916.39687],["202002",331497.59236],["202001",352003.65089]]}
    """
    return test_file


@pytest.fixture()
def elec_txt_dataframe(test_file_bytes) -> pd.DataFrame:
    """Simulate raw pd.read_json('ELEC.txt')."""
    buffer = BytesIO(test_file_bytes)
    df = pd.read_json(buffer, lines=True)
    return df


def test__extract_keys_from_series_id(elec_txt_dataframe):
    """Parse keys from EIA series_id string."""
    input_ = elec_txt_dataframe.iloc[[2], :]
    expected = pd.DataFrame(
        {
            "series_code": ["RECEIPTS_BTU"],
            "fuel_agg": ["NG"],
            "geo_agg": ["US"],
            "sector_agg": ["2"],
            "temporal_agg": ["Q"],
        },
        index=[2],
    )
    actual = bulk._extract_keys_from_series_id(input_)
    pd.testing.assert_frame_equal(actual, expected)
