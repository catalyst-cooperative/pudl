"""This module provides shared tooling that can be used by all record linkage models."""
import importlib
from contextlib import contextmanager

import mlflow
import yaml
from dagster import Config, op
from pydantic import BaseModel

import pudl
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


def get_model_config(experiment_name: str) -> dict:
    """Load model configuration from yaml file."""
    config_file = (
        importlib.resources.files("pudl.package_data.settings")
        / "record_linkage_model_config.yml"
    )
    config = yaml.safe_load(config_file.open("r"))

    if not (model_config := config.get(experiment_name)):
        raise RuntimeError(f"No {experiment_name} entry in {config_file}")

    return {experiment_name: model_config}


def flatten_model_config(model_config: dict) -> dict:
    """Take nested dictionary defining model config and flatten for logging purposes."""

    def _flatten_level(config_level: dict, param_name: str):
        flattened_dict = {}
        for key, val in config_level.items():
            flattened_param = f"{param_name}.{key}"
            if isinstance(val, dict):
                flattened_dict |= _flatten_level(val, param_name=flattened_param)
            else:
                flattened_dict[flattened_param[1:]] = val
        return flattened_dict

    return _flatten_level(model_config, "")


class ExperimentTrackerConfig(Config):
    """Dagster config to setup experiment tracking with mlflow."""

    tracking_uri: str = f"sqlite:///{PudlPaths().output_dir}/experiments.sqlite"
    tracking_enabled: bool = True
    experiment_name: str
    log_yaml: bool
    run_context: str


class ExperimentTracker(BaseModel):
    """Class to manage tracking a single model."""

    tracker_config: ExperimentTrackerConfig
    run_id: str

    @classmethod
    def create_experiment_tracker(
        cls, experiment_config: ExperimentTrackerConfig
    ) -> "ExperimentTracker":
        """Create experiment tracker for specified experiment."""
        mlflow.set_tracking_uri(experiment_config.tracking_uri)
        with mlflow.start_run(
            experiment_id=cls.get_or_create_experiment(
                experiment_config.experiment_name
            ),
            tags={"run_context": experiment_config.run_context},
        ) as run:
            if experiment_config.log_yaml:
                config = flatten_model_config(
                    get_model_config(experiment_config.experiment_name)
                )
                mlflow.log_params(config)

            return cls(tracker_config=experiment_config, run_id=run.info.run_id)

    @contextmanager
    def start_run(self):
        """Creates context manager for a run associated with a specified experiment."""
        mlflow.set_tracking_uri(self.tracker_config.tracking_uri)
        with mlflow.start_run(
            run_id=self.run_id,
            experiment_id=self.get_or_create_experiment(
                self.tracker_config.experiment_name
            ),
        ) as run:
            yield run

    @staticmethod
    def get_or_create_experiment(experiment_name: str):
        """Retrieve the ID of an existing MLflow experiment or create a new one if it doesn't exist.

        This function checks if an experiment with the given name exists within MLflow.
        If it does, the function returns its ID. If not, it creates a new experiment
        with the provided name and returns its ID.

        Returns:
        - str: ID of the existing or newly created MLflow experiment.
        """
        if experiment := mlflow.get_experiment_by_name(experiment_name):
            experiment_id = experiment.experiment_id
        else:
            experiment_id = mlflow.create_experiment(experiment_name)

        return experiment_id


@op
def create_experiment_tracker(config: ExperimentTrackerConfig):
    """Use config to create an experiment tracker."""
    return ExperimentTracker.create_experiment_tracker(config)
