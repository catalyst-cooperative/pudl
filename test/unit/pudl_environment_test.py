"""Test to see if our environment (PUDL_INPUT/OUTPUT, pudl_settings) is set up properly
in a variety of situations."""

import os
from io import StringIO

import pytest
import yaml

from pudl.workspace.setup import get_defaults


def setup():
    if os.getenv("PUDL_OUTPUT"):
        os.environ["PUDL_OUTPUT_OLD"] = os.getenv("PUDL_OUTPUT")
    if os.getenv("PUDL_INPUT"):
        os.environ["PUDL_INPUT_OLD"] = os.getenv("PUDL_INPUT")


def test_get_defaults_in_test_environment_no_env_vars():
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

    settings = get_defaults(yaml_file=settings_yaml)

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": f"{workspace}/output",
        "data_dir": f"{workspace}/data",
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == f"{default_settings['pudl_out']}/output"
    assert os.getenv("PUDL_INPUT") == f"{default_settings['pudl_in']}/data"


def test_get_defaults_in_test_environment_no_env_vars_tmpdir(pudl_output_tmpdir):
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

    settings = get_defaults(
        yaml_file=settings_yaml, output_dir=pudl_output_tmpdir / "output"
    )

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": f"{pudl_output_tmpdir}/output",
        "data_dir": f"{workspace}/data",
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == f"{pudl_output_tmpdir}/output"
    assert os.getenv("PUDL_INPUT") == f"{default_settings['pudl_in']}/data"


@pytest.mark.parametrize(
    "settings_yaml",
    [
        None,
        StringIO(
            yaml.dump(
                {
                    "pudl_in": "/test/workspace",
                    "pudl_out": "/test/workspace",
                }
            )
        ),
    ],
)
def test_get_defaults_in_test_environment_use_env_vars(settings_yaml):
    workspace = "/test/whatever/from/env"
    os.environ |= {
        "PUDL_OUTPUT": f"{workspace}/output",
        "PUDL_INPUT": f"{workspace}/data",
    }

    settings = get_defaults(yaml_file=settings_yaml)

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": f"{workspace}/output",
        "data_dir": f"{workspace}/data",
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == f"{workspace}/output"
    assert os.getenv("PUDL_INPUT") == f"{workspace}/data"


@pytest.mark.parametrize(
    "settings_yaml",
    [
        None,
        StringIO(
            yaml.dump(
                {
                    "pudl_in": "/test/workspace",
                    "pudl_out": "/test/workspace",
                }
            )
        ),
    ],
)
def test_get_defaults_in_test_environment_use_env_vars_tmpdir(
    settings_yaml, pudl_output_tmpdir
):
    workspace = "/test/whatever/from/env"
    os.environ |= {
        "PUDL_OUTPUT": f"{workspace}/output",
        "PUDL_INPUT": f"{workspace}/data",
    }

    settings = get_defaults(
        yaml_file=settings_yaml, output_dir=pudl_output_tmpdir / "output"
    )

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": f"{pudl_output_tmpdir}/output",
        "data_dir": f"{workspace}/data",
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == f"{pudl_output_tmpdir}/output"
    assert os.getenv("PUDL_INPUT") == f"{workspace}/data"


def test_get_defaults_in_test_environment_no_env_vars_no_config():
    if os.getenv("PUDL_OUTPUT"):
        del os.environ["PUDL_OUTPUT"]
    if os.getenv("PUDL_INPUT"):
        del os.environ["PUDL_INPUT"]

    with pytest.raises(RuntimeError):
        get_defaults(yaml_file=None, default_pudl_yaml=None)


def teardown():
    if os.getenv("PUDL_OUTPUT_OLD"):
        os.environ["PUDL_OUTPUT"] = os.getenv("PUDL_OUTPUT_OLD")
        del os.environ["PUDL_OUTPUT_OLD"]
    if os.getenv("PUDL_INPUT_OLD"):
        os.environ["PUDL_INPUT"] = os.getenv("PUDL_INPUT_OLD")
        del os.environ["PUDL_INPUT_OLD"]
