"""Tests for :mod:`pudl.scripts.batch_config`."""

import json
from pathlib import Path
from typing import NotRequired, TypedDict

import pytest
from click.testing import CliRunner

from pudl.scripts import batch_config


class _BatchConfigKwargs(TypedDict):
    """Mirrors the keyword-only parameters of :func:`batch_config.to_config`."""

    container_image: str
    container_env: tuple[str, ...]
    container_command: str
    container_arg: tuple[str, ...]
    machine_type: str
    cpu_milli: int
    memory_mib: int
    disk_gb: int
    disk_type: str
    batch_job_id: str
    pipeline: str
    local_ssd_gb: NotRequired[int]


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


DEFAULT_BATCH_CONFIG: _BatchConfigKwargs = {
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
"""Valid ``to_config`` arguments; copy and override per-test at the call site."""


class TestToConfigValidation:
    """The argument guards in :func:`batch_config.to_config` that we own.

    The generated dict's shape is dictated by the external Batch API and isn't
    asserted here; :class:`TestMain` exercises it end to end instead.
    """

    def test_missing_container_image_raises(self):
        config = DEFAULT_BATCH_CONFIG.copy()
        config["container_image"] = ""
        with pytest.raises(ValueError, match="container_image is required"):
            batch_config.to_config(**config)

    def test_missing_container_command_raises(self):
        config = DEFAULT_BATCH_CONFIG.copy()
        config["container_command"] = ""
        with pytest.raises(ValueError, match="container_command is required"):
            batch_config.to_config(**config)

    def test_local_ssd_gb_must_be_multiple_of_375(self):
        config = DEFAULT_BATCH_CONFIG.copy()
        config["local_ssd_gb"] = 500
        with pytest.raises(ValueError, match="multiple of 375"):
            batch_config.to_config(**config)

    def test_no_local_ssd_by_default(self):
        """Omitting ``local_ssd_gb`` leaves the config free of Local SSD wiring."""
        result = batch_config.to_config(**DEFAULT_BATCH_CONFIG)
        task_spec = result["taskGroups"][0]["taskSpec"]
        policy = result["allocationPolicy"]["instances"][0]["policy"]
        assert "volumes" not in task_spec
        assert "disks" not in policy
        assert len(task_spec["runnables"]) == 1
        assert (
            "PUDL_OUTPUT" not in task_spec["runnables"][0]["environment"]["variables"]
        )

    def test_local_ssd_wires_disk_volume_env_and_chmod(self):
        """A Local SSD size attaches the array, mounts it, redirects scratch dirs.

        A pre-runnable makes the root-owned mount writable by the non-root
        container user.
        """
        config = DEFAULT_BATCH_CONFIG.copy()
        config["local_ssd_gb"] = 750
        result = batch_config.to_config(**config)

        task_spec = result["taskGroups"][0]["taskSpec"]
        policy = result["allocationPolicy"]["instances"][0]["policy"]
        mount = batch_config.LOCAL_SSD_MOUNT_PATH

        disk = policy["disks"][0]
        assert disk["newDisk"] == {"sizeGb": "750", "type": "local-ssd"}
        assert disk["deviceName"] == batch_config.LOCAL_SSD_DEVICE_NAME

        volume = task_spec["volumes"][0]
        assert volume["deviceName"] == batch_config.LOCAL_SSD_DEVICE_NAME
        assert volume["mountPath"] == mount

        runnables = task_spec["runnables"]
        assert f"chmod 0777 {mount}" in runnables[0]["script"]["text"]
        container = runnables[1]["container"]
        assert container["volumes"] == [f"{mount}:{mount}:rw"]

        env = runnables[1]["environment"]["variables"]
        assert env["PUDL_OUTPUT"] == f"{mount}/output"
        assert env["PUDL_INPUT"] == f"{mount}/input"
        assert env["DAGSTER_HOME"] == f"{mount}/dagster_home"


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
