"""Unit tests for :mod:`pudl.analysis.record_linkage.eia_ferc1_record_linkage`."""

import jellyfish
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from splink import DuckDBAPI

from pudl.analysis.ml_tools.experiment_tracking import ExperimentTracker
from pudl.analysis.record_linkage import eia_ferc1_record_linkage
from pudl.analysis.record_linkage.eia_ferc1_model_config import get_comparisons
from pudl.analysis.record_linkage.eia_ferc1_record_linkage import (
    U_ESTIMATION_SEED,
    ModelPredictionsConfig,
    add_null_overrides,
    get_best_matches,
    get_model_predictions,
    prepare_for_matching,
    select_best_matches,
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


def _select_best(preds: pd.DataFrame) -> pd.DataFrame:
    """Run :func:`select_best_matches` against a DuckDB table of predictions."""
    predictions = DuckDBAPI().register(
        preds.reset_index(drop=True), table_name="predictions"
    )
    return select_best_matches(predictions)


def test_select_best_matches_picks_highest_probability_with_tie_break():
    """One match per FERC record: highest probability, ties go to the lowest EIA ID."""
    best = _select_best(_predictions())
    assert best["record_id_ferc1"].tolist() == ["f1", "f2", "f3"]
    assert best["record_id_eia"].tolist() == ["e2", "e3", "e5"]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("seed", range(5))
def test_select_best_matches_ignores_row_order(seed: int, reverse: bool):
    """Reordering the model output must not change which match is chosen."""
    expected = _select_best(_predictions())
    reordered = _predictions().sample(frac=1, random_state=seed)
    if reverse:
        reordered = reordered.iloc[::-1]
    assert_frame_equal(_select_best(reordered), expected)


def test_get_best_matches_reports_metrics(mocker):
    """Metrics are computed from the best matches and returned for asset metadata."""
    inputs = mocker.MagicMock()
    inputs.get_train_df.return_value = pd.DataFrame(
        {"record_id_ferc1": ["f1", "f2"], "record_id_eia": ["e2", "e9"]}
    ).set_index(["record_id_ferc1", "record_id_eia"])
    best = pd.DataFrame(
        {"record_id_ferc1": ["f1", "f2"], "record_id_eia": ["e2", "e3"]}
    )
    _, metrics = get_best_matches(
        best, inputs, mocker.MagicMock(spec=ExperimentTracker)
    )
    assert metrics == {"precision": 0.5, "recall": 1.0, "accuracy": 0.5}


def test_prepare_metaphone_matches_rowwise_encoding():
    """Encoding unique names once must match encoding each row, and keep nulls null."""
    names = pd.Series(["Smith Creek", None, "Smith Creek", "Barry", pd.NA])
    df = pd.DataFrame(dict.fromkeys(["plant_name", "utility_name"], names)).assign(
        record_id="x",
        fuel_type_code_pudl="gas",
        installation_year=2000,
        construction_year=2000,
        capacity_mw=1.0,
        net_generation_mwh=1.0,
        report_year=2000,
        plant_id_pudl=1,
        utility_id_pudl=1,
    )
    out = prepare_for_matching.compute_fn.decorated_fn(df, pd.DataFrame())
    expected = [None if pd.isnull(n) else jellyfish.metaphone(n) for n in names]
    assert out["plant_name_mphone"].tolist() == expected


def test_get_comparisons_returns_fresh_objects():
    """Comparisons are configured in place, so they must not be shared."""
    assert get_comparisons()[1] is not get_comparisons()[1]


def test_get_model_predictions_seeds_u_estimation(mocker):
    """The random sampling used to estimate u probabilities must be seeded."""
    linker_cls = mocker.patch.object(eia_ferc1_record_linkage, "Linker")
    mocker.patch.object(eia_ferc1_record_linkage, "SettingsCreator")
    mocker.patch.object(eia_ferc1_record_linkage, "DuckDBAPI")
    mocker.patch.object(eia_ferc1_record_linkage, "select_best_matches")
    get_model_predictions(
        eia_df=pd.DataFrame({"record_id": ["e1", "e2"]}),
        ferc_df=pd.DataFrame({"record_id": ["f1"]}),
        train_df=mocker.MagicMock(),
        experiment_tracker=mocker.MagicMock(spec=ExperimentTracker),
        config=ModelPredictionsConfig(),
    )
    training = linker_cls.return_value.training
    training.estimate_u_using_random_sampling.assert_called_once_with(
        max_pairs=1e7, seed=U_ESTIMATION_SEED
    )
