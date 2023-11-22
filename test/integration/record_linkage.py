"""Test core record linkage functionality."""
# ruff: noqa: S311

import random
import string

import numpy as np
import pandas as pd
import pytest

from pudl.analysis.record_linkage.classify_plants_ferc1 import (
    construct_ferc1_plant_matching_model,
)
from pudl.transform.params.ferc1 import (
    CONSTRUCTION_TYPE_CATEGORIES,
    PLANT_TYPE_CATEGORIES,
    VALID_PLANT_YEARS,
)

_FUEL_COLS = [
    "coal_fraction_mmbtu",
    "gas_fraction_mmbtu",
    "hydro_fraction_mmbtu",
    "nuclear_fraction_mmbtu",
    "oil_fraction_mmbtu",
    "other_fraction_mmbtu",
    "solar_fraction_mmbtu",
    "waste_fraction_mmbtu",
    "wind_fraction_mmbtu",
]

_RANDOM_GENERATOR = np.random.default_rng(12335)


def _randomly_modify_string(input_str: str, k: int = 5) -> str:
    """Generate up to k random edits of input string."""
    input_list = list(input_str)

    # Possible characters to select from when performing "add" or "substitute"
    # Letters are included twice to increase odds of selecting a letter
    characters = (
        string.digits + string.ascii_letters + string.ascii_letters + string.punctuation
    )
    edit_types = ["add", "delete", "substitute"]
    num_edits = min(random.randrange(k) for i in range(10))

    for _ in range(num_edits):
        edit = edit_types[random.randrange(3)]
        position = random.randrange(len(input_str) - 1)

        if edit == "add":
            input_list.insert(position, random.choice(characters))
        elif edit == "delete":
            input_list.pop(position)
        else:
            input_list[position] = random.choice(characters)

    return "".join(input_list)


def _noisify(col: pd.Series, sigma: float = 0.01, probability: float = 1) -> pd.Series:
    """Add random noise to a column."""
    noisy_rows = _RANDOM_GENERATOR.random(len(col)) > (1 - probability)
    modifier_array = np.zeros(len(col))
    modifier_array[noisy_rows] += _RANDOM_GENERATOR.normal(
        scale=sigma, size=sum(noisy_rows)
    )
    return col + modifier_array


def _modify_categorical(
    df: pd.DataFrame, col: str, categories: list, probability: float = 0.01
) -> pd.DataFrame:
    """Randomly modify categorical column using given probability."""
    modify_elements = _RANDOM_GENERATOR.random(len(df)) > (1 - probability)
    df.loc[modify_elements, col] = _RANDOM_GENERATOR.choice(categories)
    return df


def _generate_random_test_df(
    default_plant_name: str,
    size: int = 2022 - 1994,
    plant_name_max_edits: int = 5,
    plant_type_error_prob: float = 0.01,
    construction_type_error_prob: float = 0.01,
    construction_year_error_prob: float = 0.01,
    capacity_mean: float = 500.0,
    capacity_sigma: float = 10.0,
    capacity_change_prob: float = 0.01,
    utility_id_error_prob: float = 0.01,
    utility_id: int = random.randrange(1000),
):
    """Generate a random input DataFrame for testing record linkage."""

    generated_df = pd.DataFrame(
        {
            "base_plant_name": [default_plant_name] * size,
            "plant_type": [
                random.choice(list(PLANT_TYPE_CATEGORIES["categories"].keys()))
            ]
            * size,
            "report_year": list(range(1994, 1994 + size)),
            "construction_type": [
                random.choice(list(CONSTRUCTION_TYPE_CATEGORIES["categories"].keys()))
            ]
            * size,
            "capacity_mw": [capacity_mean] * size,
            "construction_year": [
                random.randrange(
                    VALID_PLANT_YEARS["lower_bound"], VALID_PLANT_YEARS["upper_bound"]
                )
            ]
            * size,
            "utility_id_ferc1": [utility_id] * size,
        }
    )

    # Add random edits to plant name
    generated_df["plant_name_ferc1"] = generated_df["base_plant_name"].apply(
        _randomly_modify_string
    )

    # Modify capacity rows based on probability and sigma
    generated_df["capacity_mw"] = _noisify(
        generated_df["capacity_mw"],
        sigma=capacity_sigma,
        probability=capacity_change_prob,
    )

    # Modify categorical columns
    generated_df = _modify_categorical(
        generated_df,
        "construction_type",
        list(CONSTRUCTION_TYPE_CATEGORIES["categories"]),
        construction_type_error_prob,
    )
    generated_df = _modify_categorical(
        generated_df,
        "construction_year",
        list(range(VALID_PLANT_YEARS["lower_bound"], VALID_PLANT_YEARS["upper_bound"])),
        construction_year_error_prob,
    )
    generated_df = _modify_categorical(
        generated_df,
        "plant_type",
        list(PLANT_TYPE_CATEGORIES["categories"]),
        plant_type_error_prob,
    )

    # Generate l1 unit vector to represent fuel fractions
    fuel_frac_vec = abs(_RANDOM_GENERATOR.normal(size=len(_FUEL_COLS)))
    fuel_frac_vec = fuel_frac_vec / np.linalg.norm(fuel_frac_vec, ord=1)
    generated_df[_FUEL_COLS] = fuel_frac_vec

    # Add minor noise to fuel fractions
    for col in _FUEL_COLS:
        generated_df[col] = _noisify(generated_df[col])

    return generated_df


@pytest.fixture
def mock_ferc1_plants_df():
    """Returns a test DataFrame for use in generic record linkage testing."""
    return pd.concat(
        [
            _generate_random_test_df("fox lake, mn"),
            _generate_random_test_df("maalaea", capacity_mean=50.0),
            _generate_random_test_df("colstrip 1 & 2", capacity_mean=700.0),
            _generate_random_test_df("wyman 4", capacity_mean=600.0, size=5),
            _generate_random_test_df("mcintosh", capacity_mean=300.0, size=6),
            _generate_random_test_df("boulevard", capacity_mean=40.0, size=12),
            _generate_random_test_df("eagle mountain", capacity_mean=400.0, size=11),
            _generate_random_test_df("eagle", capacity_mean=150.0, size=14),
            _generate_random_test_df("permian basin", capacity_mean=340.0),
            _generate_random_test_df("lake hubbard", capacity_mean=450.0),
            _generate_random_test_df("north lake", capacity_mean=800.0),
            _generate_random_test_df("stryker creek", capacity_mean=850.0),
            _generate_random_test_df("sewell creek", capacity_mean=900.0),
            _generate_random_test_df("southeast chicago", capacity_mean=400.0, size=3),
            _generate_random_test_df("mohave", capacity_mean=500.0, size=10),
            _generate_random_test_df("el segundo", capacity_mean=600.0, size=13),
            _generate_random_test_df("highgrove", capacity_mean=300.0, size=23),
            _generate_random_test_df("cool water"),
            _generate_random_test_df("huntington beach"),
            _generate_random_test_df("long beach"),
            _generate_random_test_df("san onofre 2&3"),
        ]
    )


def test_classify_plants_ferc1(mock_ferc1_plants_df):
    """Test the FERC inter-year plant linking model."""
    linker = construct_ferc1_plant_matching_model(_FUEL_COLS)
    df = linker.fit_predict(mock_ferc1_plants_df)

    # Compute percent of records assigned correctly
    correctly_matched = (
        df.groupby("base_plant_name")["plant_id_ferc1"]
        .apply(lambda plant_ids: plant_ids.value_counts().iloc[0])
        .sum()
    )
    print(correctly_matched / len(df))

    assert (
        correctly_matched / len(df) > 0.85
    ), "Percent of correctly matched FERC records below 85%."
