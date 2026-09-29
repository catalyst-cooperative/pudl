"""Unit tests for :mod:`pudl.analysis.record_linkage.eia_ferc1_record_linkage`."""

import duckdb
import jellyfish
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from splink import DuckDBAPI
from splink.blocking_analysis import count_comparisons_from_blocking_rules

from pudl.analysis.ml_tools.experiment_tracking import ExperimentTracker
from pudl.analysis.record_linkage import eia_ferc1_record_linkage
from pudl.analysis.record_linkage.eia_ferc1_model_config import (
    blocking_rule_7,
    blocking_rule_10,
    get_comparisons,
    get_year_comparison,
)
from pudl.analysis.record_linkage.eia_ferc1_record_linkage import (
    U_ESTIMATION_SEED,
    ModelPredictionsConfig,
    add_null_overrides,
    get_best_matches,
    get_model_predictions,
    override_bad_predictions,
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


def test_override_bad_predictions_labels_and_logs_overridden_matches(caplog):
    """Wrong predictions are labeled ``overridden`` and counted in the log."""
    matches = pd.DataFrame(
        {
            "record_id_ferc1": ["f1", "f2", "f3"],
            "record_id_eia": ["e1", "e2", "e3"],
        }
    )
    train = pd.DataFrame(
        {
            "record_id_ferc1": ["f1", "f2", "f4"],
            "record_id_eia": ["e1", "e9", "e4"],
        }
    ).set_index(["record_id_ferc1", "record_id_eia"])

    with caplog.at_level("INFO"):
        result = override_bad_predictions(matches, train).set_index("record_id_ferc1")

    assert result["match_type"].to_dict() == {
        "f1": "correct match",
        "f2": "incorrect prediction; overridden",
        "f3": "prediction; not in training data",
        "f4": "incorrect prediction; no predicted match",
    }
    assert result.loc["f2", "record_id_eia"] == "e9"
    assert "Percent of training data overridden in matches: 0.33" in caplog.text


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
    """Metrics are computed from the best matches and returned for asset metadata.

    Of four training records, f1 is predicted correctly, f2 is predicted wrongly, f3 is
    correct, and f4 gets no prediction.
    """
    inputs = mocker.MagicMock()
    inputs.get_train_df.return_value = pd.DataFrame(
        {
            "record_id_ferc1": ["f1", "f2", "f3", "f4"],
            "record_id_eia": ["e1", "e2", "e3", "e4"],
        }
    ).set_index(["record_id_ferc1", "record_id_eia"])
    best = pd.DataFrame(
        {
            "record_id_ferc1": ["f1", "f2", "f3", "f5"],
            "record_id_eia": ["e1", "e9", "e3", "e5"],
        }
    )
    _, metrics = get_best_matches(
        best, inputs, mocker.MagicMock(spec=ExperimentTracker)
    )
    # precision: 2 of the 3 predictions on training records were right;
    # recall: 2 of the 4 training records' true matches were found;
    # coverage: 3 of the 4 training records got a prediction;
    # accuracy: 2 of the 4 training records were predicted correctly.
    assert metrics == {
        "precision": 0.667,
        "recall": 0.5,
        "coverage": 0.75,
        "accuracy": 0.5,
    }


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


@pytest.mark.parametrize(
    ("rule", "column", "blocked_pairs"),
    [
        # 4.2 and 4.4 round to 4, 4.6 rounds to 5, and the null never blocks
        (blocking_rule_7, "capacity_mw", 1),
        (blocking_rule_10, "net_generation_mwh", 1),
    ],
)
def test_numeric_blocking_rules_block_on_rounded_values(rule, column, blocked_pairs):
    """Values that round to the same integer are compared; exact equality isn't needed."""
    db_api = DuckDBAPI()
    eia = pd.DataFrame(
        {
            "record_id": ["e1", "e2", "e3"],
            "report_year": [2020, 2020, 2020],
            column: [4.2, 4.6, None],
            "plant_name_mphone": ["AB", "AB", "AB"],
        }
    )
    ferc = pd.DataFrame(
        {
            "record_id": ["f1"],
            "report_year": [2020],
            column: [4.4],
            "plant_name_mphone": ["AB"],
        }
    )
    counts = count_comparisons_from_blocking_rules(
        [
            db_api.register(eia, dataset_display_name="eia_df"),
            db_api.register(ferc, dataset_display_name="ferc_df"),
        ],
        blocking_rules=[rule],
        link_type="link_only",
        unique_id_column_name="record_id",
        record_sample_proportion=1.0,
    )
    assert counts[0]["marginal_comparison_count"] == blocked_pairs


@pytest.mark.parametrize(
    ("year_l", "year_r", "level"),
    [
        (None, 2000, 0),
        (2000, None, 0),
        (2000, 2000, 1),
        # A one year difference is a level regardless of leap days
        (2000, 2001, 2),
        (2001, 2000, 2),
        (2000, 2002, 3),
        # Years that differ by a single digit are no longer treated as similar
        (2001, 2011, 4),
        (1991, 2001, 4),
        (2000, 2003, 4),
    ],
)
def test_year_comparison_levels(year_l, year_r, level):
    """Years are compared by their numeric difference."""
    comparison = get_year_comparison("year").get_comparison("duckdb")
    conditions = [lvl.sql_condition for lvl in comparison.comparison_levels]
    con = duckdb.connect()
    con.register(
        "pair",
        pd.DataFrame({"year_l": [year_l], "year_r": [year_r]}, dtype=pd.Int64Dtype()),
    )

    def _applies(condition: str) -> bool:
        if condition == "ELSE":
            return True
        query = "SELECT " + condition.replace('"', "") + " FROM pair"  # noqa: S608
        return bool(con.sql(query).fetchone()[0])

    # Levels are ordered from most to least specific; the first true one applies.
    assert next(i for i, cond in enumerate(conditions) if _applies(cond)) == level


def test_prepare_for_matching_uses_integer_years():
    """Installation and construction years are nullable integers, not datetimes."""
    df = pd.DataFrame(
        {
            "record_id": ["x", "y"],
            "plant_name": ["a", "b"],
            "utility_name": ["a", "b"],
            "fuel_type_code_pudl": ["gas", "gas"],
            "installation_year": [2000.0, None],
            "construction_year": [1999, 2001],
            "capacity_mw": [1.0, 1.0],
            "net_generation_mwh": [1.0, 1.0],
            "report_year": [2000, 2000],
            "plant_id_pudl": [1, 1],
            "utility_id_pudl": [1, 1],
        }
    )
    out = prepare_for_matching.compute_fn.decorated_fn(df, pd.DataFrame())
    assert out["installation_year"].dtype == pd.Int64Dtype()
    assert out["installation_year"].tolist() == [2000, pd.NA]
    assert out["construction_year"].dtype == pd.Int64Dtype()


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
