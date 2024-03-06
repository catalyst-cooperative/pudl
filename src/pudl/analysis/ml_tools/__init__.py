"""Implements shared tooling for machine learning models in PUDL."""

from . import models


def get_ml_models_config():
    """Return default configuration for all PUDL models."""
    return {"ops": models.MODEL_CONFIGURATION}
