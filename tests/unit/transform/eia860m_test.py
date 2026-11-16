"""Unit tests for the EIA-860M transform module."""

import pandas as pd
import pytest

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
    upstream = {
        key.to_user_string()
        for key in eia860m.core_eia860m__changelog_generators.dependency_keys
    }
    assert upstream == {
        "raw_eia860m__generator_proposed",
        "raw_eia860m__generator_existing",
        "raw_eia860m__generator_retired",
        "raw_eia860m__puerto_rico_generator_proposed",
        "raw_eia860m__puerto_rico_generator_existing",
        "raw_eia860m__puerto_rico_generator_retired",
    }


class _StopAfterGeneratorsCallError(Exception):
    """Raised by the mocked EIA-860 generators transform to end the asset early."""


def test_changelog_generators_pairs_puerto_rico_pages_with_mainland(mocker):
    """Each Puerto Rico page should be appended to the matching mainland page."""
    mocked_generators = mocker.patch.object(
        eia860m,
        "_core_eia860__generators",
        side_effect=_StopAfterGeneratorsCallError,
    )
    pages = {
        f"raw_eia860m__{region}generator_{status}": pd.DataFrame(
            {"source_page": [f"{region}{status}"]}
        )
        for region in ["", "puerto_rico_"]
        for status in ["proposed", "existing", "retired"]
    }

    with pytest.raises(_StopAfterGeneratorsCallError):
        eia860m.core_eia860m__changelog_generators(**pages)

    passed = mocked_generators.call_args.kwargs
    for status in ["proposed", "existing", "retired"]:
        assert passed[f"raw_eia860__generator_{status}"]["source_page"].tolist() == [
            status,
            f"puerto_rico_{status}",
        ]
    # The retired tabs have no operational status code, for Puerto Rico as well.
    assert (
        passed["raw_eia860__generator_retired"]["operational_status_code"].isna().all()
    )
