"""Provides tooling for developing/tracking ML models within PUDL.

The ML pipelines here use Dagster's ``@op`` and ``@graph`` primitives rather than
``@asset``. Each pipeline (e.g. ``ferc_to_ferc``, ``ferc_to_eia``) is a multi-step
computation — embedding, clustering, matching — where the intermediate outputs (distance
matrices, cluster assignments, etc.) are not meaningful PUDL data products. They are
implementation details of the model.  Converting each ``@op`` to an ``@asset`` would
pollute the asset catalog with tables that have no meaning outside the model.

``graph_asset`` is the Dagster idiom for exactly this use case: a complex computation
with internal steps that nevertheless produces a single named asset visible in the
catalog. Do not refactor these to chains of ``@asset``.

The ``@pudl_model`` decorator
------------------------------
:func:`pudl_model` is a decorator factory that wraps a Dagster ``@graph`` and
converts it into a ``graph_asset``. Applying it to a ``@graph`` function does
three things:

1. **Collects configuration.** It walks the graph's op tree, harvesting default
   config values from each op's :class:`~dagster.Config` subclass. If
   ``config_from_yaml=True``, it also merges overrides from
   ``pudl.package_data.settings.pudl_models.yml``. The merged config is stored
   in the module-level ``MODEL_CONFIGURATION`` dict, which
   :func:`~pudl.dagster.config.get_ml_models_config` later folds into the
   default job config so Dagster knows the defaults at launch time.

2. **Injects an ExperimentTracker.** An :class:`~pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker`
   op is synthesized and called first inside the ``graph_asset``, then passed as
   the first argument to the wrapped graph. Ops that want to log metrics receive
   it as an input parameter named ``experiment_tracker``. The tracker input is
   excluded from the asset's ``ins`` mapping so Dagster does not treat it as a
   dependency on an upstream asset.

3. **Returns a graph_asset.** The decorated function is replaced by a
   ``graph_asset`` whose name is ``asset_name`` and whose upstream asset
   dependencies are inferred from the graph's remaining inputs.

Configuration precedence (lowest → highest):

* Default values on each op's ``Config`` subclass (code)
* Entries in ``pudl_models.yml`` (repo-level YAML, only when ``config_from_yaml=True``)
* Values entered in the Dagster UI Launchpad (single-run override)
"""

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
        importlib.resources.files("pudl.package_data.settings") / "pudl_models.yml"
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
            )()
            return model_graph(experiment_tracker, **kwargs)

        return model_asset

    return _decorator
