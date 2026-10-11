"""Unit tests for :mod:`pudl.analysis.record_linkage.eia_ferc1_record_linkage`."""

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pudl.analysis.ml_tools.experiment_tracking import ExperimentTracker
from pudl.analysis.record_linkage import eia_ferc1_record_linkage
from pudl.analysis.record_linkage.eia_ferc1_record_linkage import (
    U_ESTIMATION_SEED,
    add_null_overrides,
    get_best_matches,
    get_model_predictions,
)
from pudl.metadata.classes import Resource

# A real record_id_ferc1 pulled from src/pudl/package_data/glue/eia_ferc1_null.csv
NULL_OVERRIDE_RECORD_ID_FERC1 = "f1_gnrt_plant_2008_12_108_0_5"


def test_add_null_overrides_preserves_condensed_columns():
    """Nulling EIA columns shouldn't wipe out condensed FERC1-derived columns.

    Regression test: previously these shared columns were included in
    ``eia_cols_to_null`` and got wiped out for every record in ``eia_ferc1_null.csv``,
    producing spurious NULL report dates/years. ``report_date``, ``report_year``,
    ``plant_id_pudl``, and ``utility_id_pudl`` are condensed in
    :func:`prettyify_best_matches` to hold a FERC1-derived value even for records with
    no EIA match.
    """
    eia_field_names = Resource.from_id("out_eia__yearly_plant_parts").get_field_names()
    eia_only_col = next(
        col
        for col in eia_field_names
        if col not in {"report_year", "report_date", "plant_id_pudl", "utility_id_pudl"}
    )

    connects_ferc1_eia = pd.DataFrame(
        {
            "record_id_ferc1": [NULL_OVERRIDE_RECORD_ID_FERC1, "some_other_record"],
            "record_id_eia": [pd.NA, "some_eia_record"],
            "report_date": pd.to_datetime(["2008-01-01", "2009-01-01"]),
            "report_year": [2008, 2009],
            "plant_id_pudl": [123, 456],
            "utility_id_pudl": [789, 1011],
            "match_type": ["prediction; not in training data", "correct match"],
            eia_only_col: ["some eia value", "another eia value"],
        }
    )

    result = add_null_overrides(connects_ferc1_eia)

    overridden = result.loc[
        result.record_id_ferc1 == NULL_OVERRIDE_RECORD_ID_FERC1
    ].iloc[0]
    assert overridden.match_type == "overridden"
    assert pd.notna(overridden.report_date)
    assert pd.notna(overridden.report_year)
    assert pd.notna(overridden.plant_id_pudl)
    assert pd.notna(overridden.utility_id_pudl)
    assert pd.isna(overridden[eia_only_col])


def _predictions() -> pd.DataFrame:
    """Candidate matches, with a tie for FERC record f2 listed highest-ID first."""
    return pd.DataFrame(
        {
            "record_id_l": ["e1", "e2", "e4", "e3", "e5"],
            "record_id_r": ["f1", "f1", "f2", "f2", "f3"],
            "match_probability": [0.91, 0.99, 0.95, 0.95, 0.93],
        }
    )


def _best_matches(preds: pd.DataFrame, mocker) -> pd.DataFrame:
    inputs = mocker.MagicMock()
    inputs.get_train_df.return_value = pd.DataFrame(
        {"record_id_ferc1": ["f1"], "record_id_eia": ["e2"]}
    ).set_index(["record_id_ferc1", "record_id_eia"])
    return get_best_matches(preds, inputs, mocker.MagicMock(spec=ExperimentTracker))


def test_get_best_matches_picks_highest_probability_with_tie_break(mocker):
    """One match per FERC record: highest probability, ties go to the lowest EIA ID."""
    best = _best_matches(_predictions(), mocker)
    assert best["record_id_ferc1"].tolist() == ["f1", "f2", "f3"]
    assert best["record_id_eia"].tolist() == ["e2", "e3", "e5"]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("seed", range(5))
def test_get_best_matches_ignores_row_order(seed: int, reverse: bool, mocker):
    """Reordering the model output must not change which match is chosen."""
    expected = _best_matches(_predictions(), mocker)
    reordered = _predictions().sample(frac=1, random_state=seed)
    if reverse:
        reordered = reordered.iloc[::-1]
    assert_frame_equal(_best_matches(reordered, mocker), expected)


def test_get_model_predictions_seeds_u_estimation(mocker):
    """The random sampling used to estimate u probabilities must be seeded."""
    linker_cls = mocker.patch.object(eia_ferc1_record_linkage, "Linker")
    mocker.patch.object(eia_ferc1_record_linkage, "SettingsCreator")
    get_model_predictions(
        eia_df=pd.DataFrame({"record_id": ["e1", "e2"]}),
        ferc_df=pd.DataFrame({"record_id": ["f1"]}),
        train_df=mocker.MagicMock(),
        experiment_tracker=mocker.MagicMock(spec=ExperimentTracker),
    )
    training = linker_cls.return_value.training
    training.estimate_u_using_random_sampling.assert_called_once_with(
        max_pairs=1e7, seed=U_ESTIMATION_SEED
    )
