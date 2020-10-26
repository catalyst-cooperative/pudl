"""Tests for the harvest module."""
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import pytest

from pudl.harvest.harvest import (ResourceBuilder, expand_resource_fields,
                                  most_frequent)

# ---- Helpers ---- #


def _compare_dataframes(
    a: pd.DataFrame,
    b: pd.DataFrame,
    check_index=True,
    check_col_order=False,
    check_row_order=False,
    **kwargs: Any,
) -> Tuple[str, pd.DataFrame, pd.DataFrame]:
    if not a.columns.empty and (set(a.columns) == set(b.columns)):
        columns = list(b.columns)
        if not check_col_order:
            a = a[columns]
        if not check_row_order:
            a = a.sort_index().sort_values(columns)
            b = b.sort_index().sort_values(columns)
    if not check_index:
        a = a.reset_index(drop=True)
        b = b.reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(a, b, **kwargs)
        return "", a, b
    except AssertionError as error:
        return str(error), a, b


def _assert_expected(
    result: Dict[str, pd.DataFrame], expected: Dict[str, pd.DataFrame], **kwargs: Any
) -> None:
    errors = []
    for k in result:
        error, a, b = _compare_dataframes(result[k], expected[k], **kwargs)
        if error:
            errors += [f"* {k}", error, str(a), str(b)]
    if errors:
        raise AssertionError("\n\n".join(["Dataframes are not equal."] + errors))


# ---- Unit tests ---- #

standard = [
    {
        "name": "r",
        "harvest": False,
        "schema": {"fields": ["i", "j", "x", "y"], "primaryKey": ["i", "j"]},
    }
]
expand_resource_fields(standard, default={"aggregate": most_frequent})

harvest = [
    {
        "name": "r",
        "harvest": True,
        "schema": {"fields": ["i", "j", "x", "y"], "primaryKey": ["i", "j"]},
    }
]
expand_resource_fields(harvest, default={"aggregate": most_frequent})


def test_prunes_standard_resource_with_missing_name() -> None:
    """It prunes a standard resource not named the same as an input."""
    dfs = {"other": pd.DataFrame([{"i": 1, "j": 1, "x": 1, "y": 1}])}
    assert not ResourceBuilder(dfs).build(standard)


def test_prunes_harvest_resource_with_no_key_fields() -> None:
    """It prunes a harvest resource with no key fields in the inputs."""
    dfs = [pd.DataFrame([{"x": 1, "y": 1}])]
    assert not ResourceBuilder(dfs).build(harvest)


def test_prunes_harvest_resource_with_no_data_fields() -> None:
    """It prunes a harvest resource with no data fields in the inputs."""
    dfs = [pd.DataFrame([{"i": 1, "j": 1}])]
    assert not ResourceBuilder(dfs).build(harvest)


@pytest.mark.parametrize(
    "dfs",
    [
        {"r": pd.DataFrame([{"x": 1, "y": 1}])},  # no key fields
        {"r": pd.DataFrame([{"i": 1, "j": 1}])},  # no data fields
        {"r": pd.DataFrame([{"i": 1, "j": 1, "x": 1}])},  # incomplete data fields
        {"r": pd.DataFrame([{"i": 1, "x": 1, "y": 1}])},  # incomplete key fields
    ],
)
def test_errors_extracting_missing_fields(dfs) -> None:
    """It rejects a standard resource with fields missing in the input."""
    with pytest.raises(KeyError):
        ResourceBuilder(dfs).build(standard)


@pytest.mark.parametrize(
    "dfs",
    [
        [pd.DataFrame([{"i": 1, "x": 1, "y": 1}])],  # incomplete
        [
            pd.DataFrame([{"i": 1, "x": 1}]),
            pd.DataFrame([{"j": 1, "y": 1}]),
        ],  # disjoint
        [
            pd.DataFrame([{"i": 1, "j": 1}]),
            pd.DataFrame([{"x": 1, "y": 1}]),
        ],  # unlinked
    ],
)
def test_errors_harvesting_incomplete_key_fields(dfs) -> None:
    """It rejects a harvest resource with key fields missing in the inputs."""
    with pytest.raises(KeyError):
        ResourceBuilder(dfs).build(harvest)


@pytest.mark.parametrize(
    "dfs",
    [
        [pd.DataFrame([{"i": 1, "j": 1, "x": 10, "y": 100}])],
        [
            pd.DataFrame([{"i": 1, "j": 1, "x": 10}]),
            pd.DataFrame([{"i": 1, "j": 1, "y": 100}]),
        ],
        [
            pd.DataFrame([{"i": 1, "j": 1}]),
            pd.DataFrame([{"i": 1, "j": 1, "x": 10, "y": 100}]),
        ],
    ],
)
def test_harvests_fields(dfs) -> None:
    """It harvests a resource."""
    expected = {
        "r": pd.DataFrame([{"i": 1, "j": 1, "x": 10, "y": 100}]).set_index(["i", "j"])
    }
    result = ResourceBuilder(dfs).build(harvest)
    _assert_expected(result, expected, check_dtype=False)


def test_harvests_resource_with_no_data_fields() -> None:
    """It harvests a resource with no data fields (only key fields)."""
    resources = [
        {
            "name": "r",
            "harvest": True,
            "schema": {"fields": ["i", "j"], "primaryKey": ["i", "j"]},
        }
    ]
    expand_resource_fields(resources, default={"aggregate": most_frequent})
    dfs = [pd.DataFrame([{"i": 1, "j": 1, "x": 10, "y": 100}])]
    expected = {"r": pd.DataFrame([{"i": 1, "j": 1}]).set_index(["i", "j"])}
    result = ResourceBuilder(dfs).build(resources)
    _assert_expected(result, expected)


# ---- EIA example ---- #

dfs = dict(
    service_territory_eia861=pd.DataFrame(
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
    sales_eia861=pd.DataFrame(
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
    generation_eia923=pd.DataFrame(
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
    generators_eia860=pd.DataFrame(
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
    boiler_generator_assn_eia860=pd.DataFrame(
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
)

resources = [
    {
        "name": "plant_entity_eia860",
        "harvest": True,
        "schema": {
            "fields": ["plant_id_eia", "state", "balancing_authority_code_eia"],
            "primaryKey": ["plant_id_eia"],
        },
    },
    {
        "name": "generator_entity_eia860",
        "harvest": True,
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "topping_bottoming_code",
            ],
            "primaryKey": ["plant_id_eia", "generator_id"],
        },
    },
    {
        "name": "generators_eia860",
        "harvest": True,
        "schema": {
            "fields": ["plant_id_eia", "generator_id", "report_year", "capacity_mw"],
            "primaryKey": ["plant_id_eia", "generator_id", "report_year"],
        },
    },
    {
        "name": "utility_entity_eia",
        "harvest": True,
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia"],
            "primaryKey": ["utility_id_eia"],
        },
    },
    {
        "name": "utility_assn_eia",
        "harvest": True,
        "schema": {
            "fields": ["utility_id_eia", "report_year", "state", "county"],
            "primaryKey": ["utility_id_eia", "report_year", "state", "county"],
        },
    },
    {
        "name": "generation_eia923",
        "harvest": False,
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "report_month",
                "net_generation_mwh",
            ],
            "primaryKey": ["plant_id_eia", "generator_id", "report_month"],
        },
    },
    {
        "name": "sales_eia861",
        "harvest": False,
        "schema": {
            "fields": ["utility_id_eia", "report_year", "state", "county", "sales"],
            "primaryKey": ["utility_id_eia", "report_year", "state", "county"],
        },
    },
    {
        "name": "boiler_generator_assn_eia860",
        "harvest": True,
        "schema": {
            "fields": ["plant_id_eia", "generator_id", "report_year", "boiler_id"],
            "primaryKey": ["plant_id_eia", "generator_id", "report_year", "boiler_id"],
        },
    },
]
expand_resource_fields(resources, default={"aggregate": most_frequent})

expected = dict(
    plant_entity_eia860=pd.DataFrame(
        columns=["plant_id_eia", "state", "balancing_authority_code_eia"],
        data=[(3, "AL", "SOCO"), (4, np.nan, np.nan)],
    ),
    generator_entity_eia860=pd.DataFrame(
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
    generators_eia860=pd.DataFrame(
        columns=["plant_id_eia", "generator_id", "report_year", "capacity_mw"],
        data=[
            (3, "1", "2018-01-01", 153.1),
            (3, "1", "2017-01-01", 153.1),
            (3, "2", "2018-01-01", 50),
            (3, "2", "2017-01-01", 50),
            (4, "a", "2017-01-01", np.nan),
            (4, "b", "2017-01-01", np.nan),
        ],
    ),
    utility_entity_eia=pd.DataFrame(
        columns=["utility_id_eia", "utility_name_eia"],
        data=[(3989, "City of Colorado Springs - (CO)"), (195, "Alabama Power Co")],
    ),
    utility_assn_eia=pd.DataFrame(
        columns=["utility_id_eia", "report_year", "state", "county"],
        data=[
            (3989, "2017-01-01", "CO", "El Paso"),
            (3989, "2017-01-01", "CO", "Teller"),
            (3989, "2018-01-01", "CO", "El Paso"),
            (3989, "2018-01-01", "CO", "Teller"),
            (3989, "2018-01-01", "CO", "Boulder"),
        ],
    ),
    generation_eia923=pd.DataFrame(
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
    sales_eia861=pd.DataFrame(
        columns=["utility_id_eia", "report_year", "state", "county", "sales"],
        data=[
            (3989, "2017-01-01", "CO", "El Paso", 127682),
            (3989, "2017-01-01", "CO", "Teller", 733947),
            (3989, "2018-01-01", "CO", "El Paso", 87729),
            (3989, "2018-01-01", "CO", "Teller", 992734),
            (3989, "2018-01-01", "CO", "Boulder", 66666),
        ],
    ),
    boiler_generator_assn_eia860=pd.DataFrame(
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
)

rnames = [r["name"] for r in resources]
for name, df in expected.items():
    ri = rnames.index(name)
    # Expect all harvested periodic keys as datetime64[ns]
    if resources[ri]["harvest"]:
        for col in df:
            if "report_" in col:
                df[col] = df[col].astype("datetime64[ns]")
    # Expect primary key as index
    key = resources[ri]["schema"]["primaryKey"]
    df = df.set_index(key)
    expected[name] = df


def test_eia_example() -> None:
    """It builds the expected result for Christina's EIA example."""
    builder = ResourceBuilder(dfs)
    result = builder.build(resources)
    _assert_expected(result, expected)
