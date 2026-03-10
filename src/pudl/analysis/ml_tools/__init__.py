"""Implements shared tooling for machine learning models in PUDL."""


def get_ml_models_config():
    """Return default configuration for all PUDL models."""
    from pudl.analysis.ml_tools import models

    return {"ops": models.MODEL_CONFIGURATION}
