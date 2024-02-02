"""Test record linkage models and utilities."""
import mlflow
import pandas as pd
import pytest

from pudl.analysis.record_linkage import model_helpers


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
    assert model_helpers.flatten_model_config(input_dict) == flattened_dict


@pytest.fixture
def experiment_tracker_config(tmp_path) -> model_helpers.ExperimentTrackerConfig:
    """Return experiment tracker config with tracking uri pointing to temp dir."""
    return model_helpers.ExperimentTrackerConfig(
        tracking_uri=f"sqlite:///{tmp_path}/experiments.sqlite",
        tracking_enabled=True,
        experiment_name="test_experiment",
        log_yaml=False,
        run_context="testing",
    )


def test_create_experiment_tracker(experiment_tracker_config):
    """Test that ExperimentTracker properly configures mlflow and executes logging statements."""
    experiment_tracker = model_helpers.create_experiment_tracker(
        experiment_tracker_config
    )
    experiment_tracker.execute_logging(
        lambda: mlflow.log_param("test_param", "param_value")
    )
    experiment_tracker.execute_logging(lambda: mlflow.log_metric("test_metric", 5.0))
    runs_df = mlflow.search_runs(
        output_format="pandas",
        experiment_names=[experiment_tracker_config.experiment_name],
    )

    pd.testing.assert_frame_equal(
        runs_df[["params.test_param", "metrics.test_metric", "tags.run_context"]],
        pd.DataFrame(
            {
                "params.test_param": ["param_value"],
                "metrics.test_metric": [5.0],
                "tags.run_context": [experiment_tracker_config.run_context],
            }
        ),
    )
