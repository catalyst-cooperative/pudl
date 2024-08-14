"""This module implements experiment tracking tooling using mlflow as a backend.

:class:`ExperimentTracker`'s are created using an op factory :func:`experiment_tracker_factory`
and can be passed around to op's which make up a PUDL model. This class will maintain
state between ops, ensuring that all parameters and metrics are logged to the appropriate
mlflow run. The following command will launch the mlflow UI to view model results:
`mlflow ui --backend-store-uri {tracking_uri}`. `tracking_uri` by default will point
to a file named 'experiments.sqlite' in the base directory of your PUDL repo, but
this is a configurable value, which can be found in the dagster UI.
"""

from collections.abc import Callable
from pathlib import Path

import mlflow
from dagster import Config, op
from pydantic import BaseModel

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


def _flatten_model_config(model_config: dict) -> dict:
    """Take nested dictionary defining model config and flatten for logging purposes.

    This is essentially a translation layer between Dagster configuration and mlflow,
    which does not support displaying nested parameters in the UI.

    Examples:
        >>> _flatten_model_config(
        ...     {
        ...         'ferc_to_ferc': {
        ...             'link_ids_cross_year': {
        ...                 'compute_distance_matrix': {
        ...                     'distance_threshold': .5,
        ...                      'metric': 'euclidean',
        ...                 },
        ...                 'match_orphaned_records': {'distance_threshold': 0.5},
        ...             }
        ...         }
        ...     }
        ... ) == {
        ...     'ferc_to_ferc.link_ids_cross_year.compute_distance_matrix.distance_threshold': 0.5,
        ...     'ferc_to_ferc.link_ids_cross_year.compute_distance_matrix.metric': 'euclidean',
        ...     'ferc_to_ferc.link_ids_cross_year.match_orphaned_records.distance_threshold': 0.5
        ... }
        True
    """

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

    tracking_uri: str = f"sqlite:///{Path('./').absolute()}/experiments.sqlite"
    tracking_enabled: bool = False
    run_context: str = "production"
    #: Location to store artifacts. Artifact storage not currently used.
    artifact_location: str = str(Path("./").absolute())


class ExperimentTracker(BaseModel):
    """Class to manage tracking a machine learning model using MLflow.

    The following command will launch the mlflow UI to view model results:
    `mlflow ui --backend-store-uri {tracking_uri}`. From here, you can compare metrics
    from multiple runs, and track performance.

    This class is designed to be created using the `op` :func:`create_experiment_tracker`.
    This allows the `ExperimentTracker` to be passed around within a Dagster `graph`,
    and be used for mlflow logging in any of the `op`'s that make up the `graph`. This
    is useful because Dagster executes `op`'s in separate processes, while mlflow does
    not maintain state between processes. This design also allows configuration of
    the ExperimentTracker to be set from the Dagster UI.

    Currently, we are only doing experiment tracking in a local context, but if we were
    to setup a tracking server, we could point the `tracking_uri` at this remote server
    without having to modify the models. Experiment tracking can also be done outside
    of the PUDL context. If doing exploratory work in a notebook, you can use mlflow
    directly in a notebook with the same experiment name used here, and mlflow will
    seamlessly integrate the results with those from PUDL runs.
    """

    tracker_config: ExperimentTrackerConfig
    run_id: str
    experiment_name: str

    @classmethod
    def create_experiment_tracker(
        cls,
        experiment_config: ExperimentTrackerConfig,
        experiment_name: str,
        model_config: dict,
    ) -> "ExperimentTracker":
        """Create experiment tracker for specified experiment."""
        run_id = ""
        if experiment_config.tracking_enabled:
            logger.info(
                f"Experiment tracker is enabled for {experiment_name}. "
                "To view results in the mlflow ui, execute the command: "
                f"`mlflow ui --backend-store-uri {experiment_config.tracking_uri}`"
            )
            mlflow.set_tracking_uri(experiment_config.tracking_uri)
            # Create new run under specified experiment
            with mlflow.start_run(
                experiment_id=cls.get_or_create_experiment(
                    experiment_name=experiment_name,
                    artifact_location=experiment_config.artifact_location,
                ),
                tags={"run_context": experiment_config.run_context},
            ) as run:
                # Log model configuration
                mlflow.log_params(model_config)

                # Get run_id from new run so it can be restarted in other processes
                run_id = run.info.run_id

        return cls(
            tracker_config=experiment_config,
            run_id=run_id,
            experiment_name=experiment_name,
        )

    def execute_logging(self, logging_func: Callable):
        """Perform MLflow logging statement inside ExperimentTracker run.

        Args:
            logging_func: Callable which should perform an mlflow logging statement
                inside context manager for run. Passing in callable allows
                ExperimentTracker to only execute logging if tracking is enabled
                in configuration.
        """
        if self.tracker_config.tracking_enabled:
            mlflow.set_tracking_uri(self.tracker_config.tracking_uri)
            # Calling `start_run` with `run_id` will tell mlflow to start the run again
            with mlflow.start_run(
                run_id=self.run_id,
                experiment_id=self.get_or_create_experiment(self.experiment_name),
            ):
                logging_func()

    @staticmethod
    def get_or_create_experiment(
        experiment_name: str, artifact_location: str = ""
    ) -> str:
        """Retrieve the ID of an existing MLflow experiment or create a new one if it doesn't exist.

        This function checks if an experiment with the given name exists within MLflow.
        If it does, the function returns its ID. If not, it creates a new experiment
        with the provided name and returns its ID.

        Returns:
            ID of the existing or newly created MLflow experiment.
        """
        if experiment := mlflow.get_experiment_by_name(experiment_name):
            experiment_id = experiment.experiment_id
        else:
            experiment_id = mlflow.create_experiment(
                experiment_name, artifact_location=artifact_location
            )

        return experiment_id


def experiment_tracker_factory(
    experiment_name: str,
    model_config: dict,
) -> ExperimentTracker:
    """Use config to create an experiment tracker."""

    @op(name=f"{experiment_name}_tracker")
    def create_experiment_tracker(config: ExperimentTrackerConfig):
        return ExperimentTracker.create_experiment_tracker(
            config,
            experiment_name,
            model_config,
        )

    return create_experiment_tracker
