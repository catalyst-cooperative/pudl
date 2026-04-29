"""Test record linkage models and utilities."""

import mlflow
import pandas as pd
import pytest

from pudl.analysis.ml_tools import experiment_tracking


@pytest.mark.parametrize(
    "input_dict,flattened_dict",
    [
        (
            {
                "flatten": {"these": {"levels": 0}, "and_this": 1},
                "also_this_level": 5,
            },
            {
                "flatten.these.levels": 0,
                "flatten.and_this": 1,
                "also_this_level": 5,
            },
        ),
        (
            {
                "flatten": {"some_other": {"levels": 0, "more": {"levels": 6}}},
                "even": {"more": {"levels": "value"}, "and_some": {"more": 9}},
            },
            {
                "flatten.some_other.levels": 0,
                "flatten.some_other.more.levels": 6,
                "even.more.levels": "value",
                "even.and_some.more": 9,
            },
        ),
    ],
)
def test_flatten_model_config(input_dict: dict, flattened_dict: dict):
    """
    Test flatten_model_config function.

    Should take nested dict and return dict with only one level and nested keys
    seperated by periods.
    """
    assert experiment_tracking._flatten_model_config(input_dict) == flattened_dict


@pytest.fixture(scope="function")
def experiment_tracker_config(tmp_path) -> dict:
    """Return experiment tracker config with tracking uri pointing to temp dir."""
    return {
        "tracking_uri": f"sqlite:///{tmp_path}/experiments.sqlite",
        "log_yaml": False,
        "run_context": "testing",
        "artifact_location": str(tmp_path),
    }


@pytest.mark.parametrize(
    "tracking_enabled,experiment_name",
    [(True, "test_run"), (False, "test_run_disabled")],
)
def test_create_experiment_tracker(
    experiment_tracker_config: dict, tracking_enabled: bool, experiment_name: str
):
    """Test that ExperimentTracker properly configures mlflow and executes logging statements."""
    experiment_tracker_config = experiment_tracking.ExperimentTrackerConfig(
        **experiment_tracker_config,
        tracking_enabled=tracking_enabled,
    )
    experiment_tracker = experiment_tracking.experiment_tracker_factory(
        experiment_name,
        {},
    )(experiment_tracker_config)
    experiment_tracker.execute_logging(
        lambda: mlflow.log_param("test_param", "param_value")
    )
    experiment_tracker.execute_logging(lambda: mlflow.log_metric("test_metric", 5.0))
    runs_df = mlflow.search_runs(
        output_format="pandas",
        experiment_names=[experiment_name],
    )

    if tracking_enabled:
        pd.testing.assert_frame_equal(
            runs_df[["params.test_param", "metrics.test_metric", "tags.run_context"]],
            pd.DataFrame(
                {
                    "params.test_param": ["param_value"],
                    "metrics.test_metric": [5.0],
                    "tags.run_context": ["testing"],
                }
            ),
        )
    else:
        assert runs_df.empty
