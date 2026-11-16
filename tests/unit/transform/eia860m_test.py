"""Unit tests for the EIA-860M transform module."""

import inspect

import pandas as pd

from pudl.transform import eia860m


def test_append_puerto_rico_generators_concats_rows():
    """Puerto Rico generator rows should be appended after the mainland rows."""
    mainland = pd.DataFrame(
        {
            "plant_id_eia": [1, 2],
            "generator_id": ["a", "b"],
            "report_date": ["2024-01-01", "2024-01-01"],
        }
    )
    puerto_rico = pd.DataFrame(
        {
            "plant_id_eia": [3],
            "generator_id": ["c"],
            "report_date": ["2024-01-01"],
        }
    )

    out = eia860m._append_puerto_rico_generators(mainland, puerto_rico)

    assert out["plant_id_eia"].tolist() == [1, 2, 3]
    assert out["generator_id"].tolist() == ["a", "b", "c"]
    assert out.index.tolist() == [0, 1, 2]


def test_append_puerto_rico_generators_keeps_all_columns():
    """Columns found on only one of the two pages should be kept, filled with nulls."""
    mainland = pd.DataFrame({"plant_id_eia": [1], "balancing_authority_code": ["X"]})
    puerto_rico = pd.DataFrame({"plant_id_eia": [3], "county": ["Ponce"]})

    out = eia860m._append_puerto_rico_generators(mainland, puerto_rico)

    assert set(out.columns) == {"plant_id_eia", "balancing_authority_code", "county"}
    assert out["plant_id_eia"].tolist() == [1, 3]
    assert out["county"].isna().tolist() == [True, False]
    assert out["balancing_authority_code"].isna().tolist() == [False, True]


def test_changelog_generators_depends_on_puerto_rico_raw_assets():
    """The EIA-860M generator changelog should take the Puerto Rico raw pages."""
    changelog_fn = eia860m.core_eia860m__changelog_generators.op.compute_fn.decorated_fn
    params = set(inspect.signature(changelog_fn).parameters)
    assert params == {
        "raw_eia860m__generator_proposed",
        "raw_eia860m__generator_existing",
        "raw_eia860m__generator_retired",
        "raw_eia860m__puerto_rico_generator_proposed",
        "raw_eia860m__puerto_rico_generator_existing",
        "raw_eia860m__puerto_rico_generator_retired",
    }
