"""Test to see if our environment (PUDL_INPUT/OUTPUT, pudl_settings) is set up properly
in a variety of situations."""

import os

import pytest

from pudl.workspace.setup import get_defaults


def setup():
    if (old_output := os.getenv("PUDL_OUTPUT")) is not None:
        os.environ["PUDL_OUTPUT_OLD"] = old_output
    if (old_input := os.getenv("PUDL_INPUT")) is not None:
        os.environ["PUDL_INPUT_OLD"] = old_input


def test_get_defaults_in_test_environment_no_env_vars_no_config():
    if os.getenv("PUDL_OUTPUT"):
        del os.environ["PUDL_OUTPUT"]
    if os.getenv("PUDL_INPUT"):
        del os.environ["PUDL_INPUT"]

    with pytest.raises(RuntimeError):
        get_defaults()


def teardown():
    if (old_output := os.getenv("PUDL_OUTPUT_OLD")) is not None:
        os.environ["PUDL_OUTPUT"] = old_output
        del os.environ["PUDL_OUTPUT_OLD"]
    if (old_input := os.getenv("PUDL_INPUT_OLD")) is not None:
        os.environ["PUDL_INPUT"] = old_input
        del os.environ["PUDL_INPUT_OLD"]
