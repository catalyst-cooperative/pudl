"""Test to see if our environment (PUDL_INPUT/OUTPUT, pudl_settings) is set up properly
in a variety of situations."""

import os
from io import StringIO

import pytest
import yaml

from pudl.workspace.setup import get_settings


def test_get_settings_in_test_environment_no_env_vars():
    if os.getenv("PUDL_OUTPUT"):
        del os.environ["PUDL_OUTPUT"]
    if os.getenv("PUDL_INPUT"):
        del os.environ["PUDL_INPUT"]

    workspace = "/test/whatever"
    default_settings = {
        "pudl_in": workspace,
        "pudl_out": workspace,
    }

    settings_yaml = StringIO(yaml.dump(default_settings))

    settings = get_settings(env="test", yaml_file=settings_yaml, live_dbs=False)

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": f"{workspace}/output",
        "data_dir": f"{workspace}/data",
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == f"{default_settings['pudl_in']}/output"
    assert os.getenv("PUDL_INPUT") == f"{default_settings['pudl_in']}/data"


def test_get_settings_in_test_environment_use_env_vars():
    workspace = "/test/whatever/from/env"
    os.environ |= {
        "PUDL_OUTPUT": f"{workspace}/output",
        "PUDL_INPUT": f"{workspace}/data",
    }

    default_settings = {
        "pudl_in": "/test/workspace",
        "pudl_out": "/test/workspace",
    }

    settings_yaml = StringIO(yaml.dump(default_settings))

    settings = get_settings(env="test", yaml_file=settings_yaml, live_dbs=False)

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": f"{workspace}/output",
        "data_dir": f"{workspace}/data",
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == f"{workspace}/output"
    assert os.getenv("PUDL_INPUT") == f"{workspace}/data"


def test_get_settings_in_test_environment_no_env_vars_no_config():
    if os.getenv("PUDL_OUTPUT"):
        del os.environ["PUDL_OUTPUT"]
    if os.getenv("PUDL_INPUT"):
        del os.environ["PUDL_INPUT"]

    with pytest.raises(RuntimeError):
        get_settings(env="test", yaml_file=None, live_dbs=True)
