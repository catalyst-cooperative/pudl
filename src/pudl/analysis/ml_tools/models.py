"""Provides tooling for developing/tracking ml models within PUDL."""
import importlib

import yaml
from dagster import (
    AssetIn,
    AssetsDefinition,
    GraphDefinition,
    OpDefinition,
    graph_asset,
)

import pudl

from . import experiment_tracking

logger = pudl.logging_helpers.get_logger(__name__)
MODEL_CONFIGURATION = {}


def get_yml_config(experiment_name: str) -> dict:
    """Load model configuration from yaml file."""
    config_file = (
        importlib.resources.files("pudl.package_data.settings")
        / "record_linkage_model_config.yml"
    )
    config = yaml.safe_load(config_file.open("r"))

    if not (model_config := config.get(experiment_name)):
        raise RuntimeError(f"No {experiment_name} entry in {config_file}")

    return {experiment_name: model_config}


def get_default_config(model_graph: GraphDefinition) -> dict:
    """Get default config values for model."""

    def _get_default_from_ops(node: OpDefinition | GraphDefinition):
        config = {}
        if isinstance(node, GraphDefinition):
            config = {
                "ops": {
                    child_node.name: _get_default_from_ops(child_node)
                    for child_node in node.node_defs
                }
            }
        else:
            if node.config_schema.default_provided:
                config = {"config": node.config_schema.default_value}
            else:
                config = {"config": None}

        return config

    config = {model_graph.name: _get_default_from_ops(model_graph)}
    config[f"{model_graph.name}_tracker"] = {
        "config": experiment_tracking.ExperimentTrackerConfig().model_dump()
    }
    return config


def pudl_model(asset_name: str, config_from_yaml: bool = False) -> AssetsDefinition:
    """Decorator for an ML model that will handle providing configuration to dagster."""

    def _decorator(model_graph: GraphDefinition):
        model_config = get_default_config(model_graph)
        if config_from_yaml:
            model_config |= get_yml_config(model_graph.name)

        MODEL_CONFIGURATION[asset_name] = {"ops": model_config}

        # Inputs should come from assets except experiment tracker
        ins = {
            key: AssetIn(key)
            for key in model_graph.input_dict
            if key != "experiment_tracker"
        }

        @graph_asset(name=asset_name, ins=ins)
        def model_asset(**kwargs):
            experiment_tracker = experiment_tracking.experiment_tracker_factory(
                experiment_name=model_graph.name,
                model_config=model_config,
                run_context="production",
            )()
            return model_graph(experiment_tracker, **kwargs)

        return model_asset

    return _decorator
