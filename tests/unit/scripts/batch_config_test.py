"""Tests for :mod:`pudl.scripts.batch_config`."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pudl.scripts import batch_config


class TestParseContainerEnv:
    """Tests for :func:`batch_config._parse_container_env`."""

    def test_value_may_contain_equals(self):
        """Only the first ``=`` splits the pair; later ones stay in the value."""
        result = batch_config._parse_container_env(("URL=https://x/?a=b&c=d",))
        assert result == {"URL": "https://x/?a=b&c=d"}

    def test_strips_surrounding_double_quotes(self):
        """A value wrapped in double quotes is unwrapped."""
        result = batch_config._parse_container_env(('NAME="quoted value"',))
        assert result == {"NAME": "quoted value"}

    def test_duplicate_key_raises(self):
        """A repeated key is a caller bug, not a silent last-wins override."""
        with pytest.raises(ValueError, match="Duplicate --container-env key: 'FOO'"):
            batch_config._parse_container_env(("FOO=1", "FOO=2"))


DEFAULT_BATCH_CONFIG = {
    "container_image": "docker.io/catalystcoop/pudl-etl@sha256:abc",
    "container_env": (),
    "container_command": "pixi",
    "container_arg": (),
    "machine_type": "c4d-highmem-16",
    "cpu_milli": 16000,
    "memory_mib": 129024,
    "disk_gb": 1000,
    "disk_type": "hyperdisk-balanced",
    "batch_job_id": "nightly-2026-09-02-abc123",
    "pipeline": "build-pudl",
}
"""Valid ``to_config`` arguments; merge per-test overrides in at the call site."""


class TestToConfigValidation:
    """The argument guards in :func:`batch_config.to_config` that we own.

    The generated dict's shape is dictated by the external Batch API and isn't
    asserted here; :class:`TestMain` exercises it end to end instead.
    """

    def test_missing_container_image_raises(self):
        with pytest.raises(ValueError, match="container_image is required"):
            batch_config.to_config(**(DEFAULT_BATCH_CONFIG | {"container_image": ""}))

    def test_missing_container_command_raises(self):
        with pytest.raises(ValueError, match="container_command is required"):
            batch_config.to_config(**(DEFAULT_BATCH_CONFIG | {"container_command": ""}))


class TestMain:
    """End-to-end tests for the ``batch_config`` CLI."""

    def test_writes_usable_config_file(self, mocker, tmp_path: Path):
        """A full invocation writes JSON carrying the values we're responsible for.

        ``_lookup_machine_spec`` shells out to ``gcloud`` so it's stubbed; the
        rest runs for real.
        """
        mocker.patch.object(
            batch_config, "_lookup_machine_spec", return_value=(8000, 62464)
        )
        output = tmp_path / "batch_job.json"

        result = CliRunner().invoke(
            batch_config.main,
            [
                "--container-image",
                "docker.io/catalystcoop/pudl-etl@sha256:abc",
                "--container-command",
                "pixi",
                "--container-arg=run",
                "--container-arg=pudl_deploy",
                "--container-env",
                "GIT_TAG=nightly-2026-09-02",
                "--machine-type",
                "c4d-standard-8",
                "--disk-gb",
                "500",
                "--disk-type",
                "hyperdisk-balanced",
                "--batch-job-id",
                "deploy-2026-09-02-abc",
                "--pipeline",
                "deploy-pudl",
                "--output",
                str(output),
            ],
        )
        assert result.exit_code == 0, result.output
        config = json.loads(output.read_text())

        # The stubbed machine-type lookup is wired into computeResource.
        compute = config["taskGroups"][0]["taskSpec"]["computeResource"]
        assert compute["cpuMilli"] == 8000
        assert compute["memoryMib"] == 62464
        assert compute["bootDiskMib"] == 500 * 1024

        # Command + args are concatenated in order; env pairs are parsed.
        runnable = config["taskGroups"][0]["taskSpec"]["runnables"][0]
        assert runnable["container"]["commands"] == ["pixi", "run", "pudl_deploy"]
        assert runnable["environment"]["variables"] == {"GIT_TAG": "nightly-2026-09-02"}

        # The pipeline label goes on both the instances (VM metrics) and the job
        # (task logs) so the monitoring dashboard can filter every widget on it.
        assert config["allocationPolicy"]["labels"]["pipeline"] == "deploy-pudl"
        assert config["labels"]["pipeline"] == "deploy-pudl"
