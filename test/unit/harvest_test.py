"""Tests for Resource harvesting methods."""

from typing import Any

import numpy as np
import pandas as pd
import pytest

from pudl.metadata.classes import Resource
from pudl.metadata.helpers import most_frequent

# ---- Helpers ---- #


def _assert_frame_equal(a: pd.DataFrame, b: pd.DataFrame, **kwargs: Any) -> None:
    """Assert dataframes are equal, printing a useful error if not."""
    try:
        pd.testing.assert_frame_equal(a, b, **kwargs)
    except AssertionError as error:
        msg = "\n\n".join(["Dataframes are not equal.", str(error), str(a), str(b)])
        raise AssertionError(msg) from error


# ---- Unit tests ---- #


STANDARD: dict[str, Any] = {
    "name": "r",
    "harvest": {"harvest": False},
    "schema": {
        "fields": [
            {
                "name": "i",
                "type": "integer",
                "harvest": {"aggregate": most_frequent},
                "description": "letter i",
            },
            {
                "name": "j",
                "type": "integer",
                "harvest": {"aggregate": most_frequent},
                "description": "letter j",
            },
            {
                "name": "x",
                "type": "integer",
                "harvest": {"aggregate": most_frequent},
                "description": "letter x",
            },
            {
                "name": "y",
                "type": "integer",
                "harvest": {"aggregate": most_frequent},
                "description": "letter y",
            },
        ],
        "primary_key": ["i", "j"],
    },
    "description": "letter r",
}

HARVEST: dict[str, Any] = {**STANDARD, "harvest": {"harvest": True}}


def test_resource_ignores_input_with_different_name() -> None:
    """Standard resources ignore input dataframes not named the same as themselves."""
    dfs = {0: pd.DataFrame([{"i": 1, "j": 1, "x": 1, "y": 1}])}
    result, _ = Resource(**STANDARD).harvest_dfs(dfs)
    assert result.empty


@pytest.mark.parametrize(
    "df",
    [
        pd.DataFrame([{"x": 1, "y": 1}]),  # no key fields
        pd.DataFrame([{"i": 1, "x": 1, "y": 1}]),  # missing key fields
    ],
)
def test_resource_ignores_input_with_missing_key_fields(df: pd.DataFrame) -> None:
    """Harvest resources ignore inputs with missing primary key fields."""
    dfs = {0: df}
    result, _ = Resource(**HARVEST).harvest_dfs(dfs)
    assert result.empty


def test_resource_harvests_input_with_only_key_fields() -> None:
    """Harvest resources harvests inputs with only primary key fields."""
    dfs = {0: pd.DataFrame([{"i": 1, "j": 2}])}
    result, _ = Resource(**HARVEST).harvest_dfs(dfs)
    assert not result.empty
    assert result.index.tolist() == [(1, 2)]


@pytest.mark.parametrize(
    "dfs",
    [
        {0: pd.DataFrame([{"i": 1, "j": 2, "x": 10, "y": 100}])},
        {
            0: pd.DataFrame([{"i": 1, "j": 2, "x": 10}]),
            1: pd.DataFrame([{"i": 1, "j": 2, "y": 100}]),
        },
        {
            0: pd.DataFrame([{"i": 1, "j": 2}]),
            1: pd.DataFrame([{"i": 1, "j": 2, "x": 10, "y": 100}]),
        },
    ],
)
def test_resource_harvests_inputs(dfs: dict[Any, pd.DataFrame]) -> None:
    """Resource harvests inputs."""
    resource = Resource(**HARVEST)
    expected = (
        pd.DataFrame([{"i": 1, "j": 2, "x": 10, "y": 100}])
        .astype(resource.to_pandas_dtypes())
        .set_index(["i", "j"])
    )
    result, _ = resource.harvest_dfs(dfs)
    _assert_frame_equal(result, expected)


def test_resource_with_only_key_fields_harvests() -> None:
    """Harvest resource with only key fields harvests inputs."""
    resource = Resource(**HARVEST)
    resource.schema.fields = resource.schema.fields[:2]
    expected = (
        pd.DataFrame([{"i": 1, "j": 2}])
        .astype(resource.to_pandas_dtypes())
        .set_index(["i", "j"])
    )
    dfs = {0: pd.DataFrame([{"i": 1, "j": 2, "x": 10, "y": 100}])}
    result, _ = resource.harvest_dfs(dfs)
    _assert_frame_equal(result, expected)


# ---- EIA example ---- #

INPUT_DFS: dict[str, pd.DataFrame] = {
    "core_eia861__yearly_service_territory": pd.DataFrame(
        columns=[
            "utility_id_eia",
            "utility_name_eia",
            "report_year",
            "county",
            "state",
        ],
        data=[
            (3989, "City of Colorado Springs - CO", "2017-01-01", "El Paso", "CO"),
            (3989, "City of Colorado Springs - CO", "2017-01-01", "Teller", "CO"),
            (3989, "City of Colorado Springs - (CO)", "2018-01-01", "El Paso", "CO"),
            (3989, "City of Colorado Springs - (CO)", "2018-01-01", "Teller", "CO"),
        ],
    ),
    "core_eia861__yearly_sales": pd.DataFrame(
        columns=[
            "utility_id_eia",
            "utility_name_eia",
            "report_year",
            "county",
            "state",
            "sales",
        ],
        data=[
            (
                3989,
                "City of Colorado Springs - (CO)",
                "2017-01-01",
                "El Paso",
                "CO",
                127682,
            ),
            (
                3989,
                "City of Colorado Springs - (CO)",
                "2017-01-01",
                "Teller",
                "CO",
                733947,
            ),
            (
                3989,
                "City of Colorado Springs - (CO)",
                "2018-01-01",
                "El Paso",
                "CO",
                87729,
            ),
            (
                3989,
                "City of Colorado Springs - (CO)",
                "2018-01-01",
                "Teller",
                "CO",
                992734,
            ),
            (
                3989,
                "City of Colorado Springs - (CO)",
                "2018-01-01",
                "Boulder",
                "CO",
                66666,
            ),
        ],
    ),
    "core_eia923__monthly_generation": pd.DataFrame(
        columns=[
            "plant_id_eia",
            "generator_id",
            "report_month",
            "net_generation_mwh",
            "prime_mover_code",
            "topping_bottoming_code",
            "balancing_authority_code_eia",
        ],
        data=[
            (3, "1", "2018-01-01", 10738.0, "ST", "T", "SOCO"),
            (3, "1", "2018-02-01", -348.0, "ST", "T", "SOCO"),
            (3, "1", "2018-03-01", -414.0, "ST", "T", "SOCO"),
            (3, "1", "2018-04-01", -411.0, "ST", "T", "SOCO"),
            (3, "1", "2018-05-01", np.nan, "CT", "T", "SOCO"),
            (3, "1", "2018-06-01", -607.0, "ST", "Top", "SOCO"),
            (3, "1", "2018-07-01", 5022.0, "ST", "Top", "SOCO"),
            (3, "1", "2018-08-01", -689.0, "ST", "T", "SOCO"),
            (3, "1", "2018-09-01", 6718.0, "ST", "T", "SOCO"),
            (3, "1", "2018-10-01", 3877.0, "ST", "T", "SOCO"),
            (3, "1", "2018-11-01", np.nan, "ST", "T", "SOCO"),
            (3, "1", "2018-12-01", -494.0, "ST", "T", "SOCO"),
        ],
    ),
    "core_eia860__scd_generators": pd.DataFrame(
        columns=[
            "plant_id_eia",
            "generator_id",
            "report_year",
            "capacity_mw",
            "prime_mover_code",
            "utility_id_eia",
            "utility_name_eia",
            "topping_bottoming_code",
            "state",
        ],
        data=[
            (3, "1", "2018-01-01", 153.1, "ST", 195, "Alabama Power Co", "T", "AL"),
            (3, "1", "2017-01-01", 153.1, "ST", 195, "Alabama Power Co", "T", "AL"),
            (3, "2", "2017-01-01", 50, "ST", 195, "Alabama Power Co", "B", "ALL"),
            (3, "2", "2018-01-01", 50, "ST", 195, "Alabama Power Co", "B", "AL"),
        ],
    ),
    "core_eia860__assn_boiler_generator": pd.DataFrame(
        columns=["plant_id_eia", "generator_id", "report_year", "boiler_id"],
        data=[
            (3, "1", "2018-01-01", "1ST"),
            (3, "1", "2017-01-01", "1ST"),
            (3, "2", "2017-01-01", "2ST"),
            (3, "2", "2018-01-01", "2ST"),
            (4, "a", "2017-01-01", "a1"),
            (4, "a", "2017-01-01", "a2"),
            (4, "a", "2017-01-01", "a2"),
            (4, "b", "2017-01-01", "b1"),
        ],
    ),
}

FIELD_DTYPES: dict[str, str] = {
    "balancing_authority_code_eia": "string",
    "utility_id_eia": "integer",
    "plant_id_eia": "integer",
    "generator_id": "string",
    "boiler_id": "string",
    "utility_name_eia": "string",
    "state": "string",
    "county": "string",
    "prime_mover_code": "string",
    "topping_bottoming_code": "string",
    "report_year": "year",
    "report_month": "date",
    "capacity_mw": "number",
    "sales": "number",
    "net_generation_mwh": "number",
}

RESOURCES: list[dict[str, Any]] = [
    {
        "name": "test_eia860__entity_plants",
        "harvest": {"harvest": True},
        "schema": {
            "fields": ["plant_id_eia", "state", "balancing_authority_code_eia"],
            "primary_key": ["plant_id_eia"],
        },
    },
    {
        "name": "test_eia860__entity_generators",
        "harvest": {"harvest": True},
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "topping_bottoming_code",
            ],
            "primary_key": ["plant_id_eia", "generator_id"],
        },
    },
    {
        "name": "core_eia860__scd_generators",
        "harvest": {"harvest": True},
        "schema": {
            "fields": ["plant_id_eia", "generator_id", "report_year", "capacity_mw"],
            "primary_key": ["plant_id_eia", "generator_id", "report_year"],
        },
    },
    {
        "name": "utility_entity_eia",
        "harvest": {"harvest": True},
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia"],
            "primary_key": ["utility_id_eia"],
        },
    },
    {
        "name": "utility_assn_eia",
        "harvest": {"harvest": True},
        "schema": {
            "fields": ["utility_id_eia", "report_year", "state", "county"],
            "primary_key": ["utility_id_eia", "report_year", "state", "county"],
        },
    },
    {
        "name": "core_eia923__monthly_generation",
        "harvest": {"harvest": False},
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "report_month",
                "net_generation_mwh",
            ],
            "primary_key": ["plant_id_eia", "generator_id", "report_month"],
        },
    },
    {
        "name": "core_eia861__yearly_sales",
        "harvest": {"harvest": False},
        "schema": {
            "fields": ["utility_id_eia", "report_year", "state", "county", "sales"],
            "primary_key": ["utility_id_eia", "report_year", "state", "county"],
        },
    },
    {
        "name": "core_eia860__assn_boiler_generator",
        "harvest": {"harvest": True},
        "schema": {
            "fields": ["plant_id_eia", "generator_id", "report_year", "boiler_id"],
            "primary_key": ["plant_id_eia", "generator_id", "report_year", "boiler_id"],
        },
    },
]

# Build resource models
for i, d in enumerate(RESOURCES):
    d["schema"]["fields"] = [
        {"name": name, "type": FIELD_DTYPES[name], "description": name}
        for name in d["schema"]["fields"]
    ]
    d["description"] = "Test table."
    RESOURCES[i] = Resource(**d)

EXPECTED_DFS: dict[str, pd.DataFrame] = {
    "test_eia860__entity_plants": pd.DataFrame(
        columns=["plant_id_eia", "state", "balancing_authority_code_eia"],
        data=[(3, "AL", "SOCO"), (4, np.nan, np.nan)],
    ),
    "test_eia860__entity_generators": pd.DataFrame(
        columns=[
            "plant_id_eia",
            "generator_id",
            "prime_mover_code",
            "topping_bottoming_code",
        ],
        data=[
            (3, "1", "ST", "T"),
            (3, "2", "ST", "B"),
            (4, "a", np.nan, np.nan),
            (4, "b", np.nan, np.nan),
        ],
    ),
    "core_eia860__scd_generators": pd.DataFrame(
        columns=["plant_id_eia", "generator_id", "report_year", "capacity_mw"],
        data=[
            (3, "1", "2017-01-01", 153.1),
            (3, "1", "2018-01-01", 153.1),
            (3, "2", "2017-01-01", 50),
            (3, "2", "2018-01-01", 50),
            (4, "a", "2017-01-01", np.nan),
            (4, "b", "2017-01-01", np.nan),
        ],
    ),
    "utility_entity_eia": pd.DataFrame(
        columns=["utility_id_eia", "utility_name_eia"],
        data=[
            (195, "Alabama Power Co"),
            (3989, "City of Colorado Springs - (CO)"),
        ],
    ),
    "utility_assn_eia": pd.DataFrame(
        columns=["utility_id_eia", "report_year", "state", "county"],
        data=[
            (3989, "2017-01-01", "CO", "El Paso"),
            (3989, "2017-01-01", "CO", "Teller"),
            (3989, "2018-01-01", "CO", "El Paso"),
            (3989, "2018-01-01", "CO", "Teller"),
            (3989, "2018-01-01", "CO", "Boulder"),
        ],
    ),
    "core_eia923__monthly_generation": pd.DataFrame(
        columns=["plant_id_eia", "generator_id", "report_month", "net_generation_mwh"],
        data=[
            (3, "1", "2018-01-01", 10738.0),
            (3, "1", "2018-02-01", -348.0),
            (3, "1", "2018-03-01", -414.0),
            (3, "1", "2018-04-01", -411.0),
            (3, "1", "2018-05-01", np.nan),
            (3, "1", "2018-06-01", -607.0),
            (3, "1", "2018-07-01", 5022.0),
            (3, "1", "2018-08-01", -689.0),
            (3, "1", "2018-09-01", 6718.0),
            (3, "1", "2018-10-01", 3877.0),
            (3, "1", "2018-11-01", np.nan),
            (3, "1", "2018-12-01", -494.0),
        ],
    ),
    "core_eia861__yearly_sales": pd.DataFrame(
        columns=["utility_id_eia", "report_year", "state", "county", "sales"],
        data=[
            (3989, "2017-01-01", "CO", "El Paso", 127682),
            (3989, "2017-01-01", "CO", "Teller", 733947),
            (3989, "2018-01-01", "CO", "El Paso", 87729),
            (3989, "2018-01-01", "CO", "Teller", 992734),
            (3989, "2018-01-01", "CO", "Boulder", 66666),
        ],
    ),
    "core_eia860__assn_boiler_generator": pd.DataFrame(
        columns=["plant_id_eia", "generator_id", "report_year", "boiler_id"],
        data=[
            (3, "1", "2018-01-01", "1ST"),
            (3, "1", "2017-01-01", "1ST"),
            (3, "2", "2017-01-01", "2ST"),
            (3, "2", "2018-01-01", "2ST"),
            (4, "a", "2017-01-01", "a1"),
            (4, "a", "2017-01-01", "a2"),
            (4, "b", "2017-01-01", "b1"),
        ],
    ),
}

# Format expected dataframes
rnames = [r.name for r in RESOURCES]
for name, df in EXPECTED_DFS.items():
    resource = RESOURCES[rnames.index(name)]
    df = df.astype(resource.to_pandas_dtypes())
    if resource.harvest.harvest:
        df = df.set_index(resource.schema.primary_key)
    else:
        df.index = pd.Index([name] * len(df), name="df")
    EXPECTED_DFS[name] = df


@pytest.mark.parametrize("resource", RESOURCES)
def test_eia_example(resource: Resource) -> None:
    """Resources harvest the expected result for Christina's EIA example."""
    result, _ = resource.harvest_dfs(INPUT_DFS)
    _assert_frame_equal(result, EXPECTED_DFS[resource.name])
