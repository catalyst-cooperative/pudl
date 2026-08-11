"""Unit tests for denormalized EIA-923 outputs."""

import pandas as pd

from pudl.metadata.classes import Resource
from pudl.output.eia923 import out_eia923__monthly_energy_storage


def test_out_eia923__monthly_energy_storage():
    core = pd.DataFrame(
        {
            "plant_id_eia": [1, 2],
            "report_date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            "prime_mover_code": ["BA", "PS"],
            "energy_source_code": ["MWH", "WAT"],
            "data_maturity": ["final", "final"],
            "fuel_units": ["mwh", "mwh"],
            "fuel_consumed_for_electricity_units": [12.0, 23.0],
            "fuel_consumed_units": [12.0, 23.0],
            "gross_generation_mwh": [9.0, 19.0],
            "net_generation_mwh": [-3.0, -4.0],
        }
    )
    plants_utilities = pd.DataFrame(
        {
            "report_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "plant_id_eia": [1, 2],
            "plant_id_pudl": [101, 102],
            "plant_name_eia": ["Battery Plant", "Pumped Storage Plant"],
            "utility_id_eia": [11, 22],
            "utility_id_pudl": [201, 202],
            "utility_name_eia": ["Battery Utility", "Hydro Utility"],
            "data_maturity": ["final", "final"],
        }
    )
    resource = Resource.from_id("out_eia923__monthly_energy_storage")

    actual = out_eia923__monthly_energy_storage(core, plants_utilities)
    assert isinstance(actual, pd.DataFrame)
    actual = resource.enforce_schema(actual)
    expected = resource.enforce_schema(
        pd.DataFrame(
            {
                "report_date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
                "plant_id_eia": [1, 2],
                "plant_id_pudl": [101, 102],
                "plant_name_eia": ["Battery Plant", "Pumped Storage Plant"],
                "utility_id_eia": [11, 22],
                "utility_id_pudl": [201, 202],
                "utility_name_eia": ["Battery Utility", "Hydro Utility"],
                "prime_mover_code": ["BA", "PS"],
                "energy_source_code": ["MWH", "WAT"],
                "fuel_units": ["mwh", "mwh"],
                "fuel_consumed_for_electricity_units": [12.0, 23.0],
                "fuel_consumed_units": [12.0, 23.0],
                "gross_generation_mwh": [9.0, 19.0],
                "net_generation_mwh": [-3.0, -4.0],
                "data_maturity": ["final", "final"],
            }
        )
    )

    pd.testing.assert_frame_equal(actual, expected)
