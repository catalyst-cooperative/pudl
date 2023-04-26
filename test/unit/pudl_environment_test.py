"""Test to see if our environment (PUDL_INPUT/OUTPUT, pudl_settings) is set up properly
in a variety of situations."""

import os
import pathlib
from io import StringIO

import pytest
import yaml

from pudl.workspace.setup import get_defaults


def setup():
    if (old_output := os.getenv("PUDL_OUTPUT")) is not None:
        os.environ["PUDL_OUTPUT_OLD"] = old_output
    if (old_input := os.getenv("PUDL_INPUT")) is not None:
        os.environ["PUDL_INPUT_OLD"] = old_input


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
    ["settings_yaml", "env_vars"],
    [
        (
            None,
            {
                "PUDL_OUTPUT": "/test/whatever/from/env/output",
                "PUDL_INPUT": "/test/whatever/from/env/input",
            },
        ),
        (
            StringIO(
                yaml.dump(
                    {
                        "pudl_in": "/test/workspace",
                        "pudl_out": "/test/workspace",
                    }
                )
            ),
            {
                "PUDL_OUTPUT": "/test/whatever/from/env/output",
                "PUDL_INPUT": "/test/whatever/from/env/input",
            },
        ),
        (
            StringIO(
                yaml.dump(
                    {
                        "pudl_in": "/test/workspace",
                        "pudl_out": "/test/workspace",
                    }
                )
            ),
            {
                "PUDL_OUTPUT": "/test/whatever/from/env/different_output",
                "PUDL_INPUT": "/test/whatever/from/env/different_input",
            },
        ),
    ],
)
def test_get_defaults_in_test_environment_use_env_vars(settings_yaml, env_vars):
    workspace = pathlib.Path(env_vars["PUDL_OUTPUT"]).parent
    os.environ |= env_vars

    settings = get_defaults(yaml_file=settings_yaml)

    expected_values = {
        "pudl_in": f"{workspace}",
        "pudl_out": env_vars["PUDL_OUTPUT"],
        "data_dir": env_vars["PUDL_INPUT"],
    }

    for key, value in expected_values.items():
        assert (key, settings[key]) == (key, value)

    assert os.getenv("PUDL_OUTPUT") == env_vars["PUDL_OUTPUT"]
    assert os.getenv("PUDL_INPUT") == env_vars["PUDL_INPUT"]


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
    if (old_output := os.getenv("PUDL_OUTPUT_OLD")) is not None:
        os.environ["PUDL_OUTPUT"] = old_output
        del os.environ["PUDL_OUTPUT_OLD"]
    if (old_input := os.getenv("PUDL_INPUT_OLD")) is not None:
        os.environ["PUDL_INPUT"] = old_input
        del os.environ["PUDL_INPUT_OLD"]
