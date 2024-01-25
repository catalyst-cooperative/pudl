"""This module provides shared tooling that can be used by all record linkage models."""
import importlib

import yaml


def get_model_config(model_key: str) -> dict:
    """Load model configuration from yaml file."""
    config_file = (
        importlib.resources.files("pudl.package_data.settings")
        / "record_linkage_model_config.yml"
    )
    config = yaml.safe_load(config_file.open("r"))

    if not (model_config := config.get(model_key)):
        raise RuntimeError(f"No {model_key} entry in {config_file}")

    return model_config
