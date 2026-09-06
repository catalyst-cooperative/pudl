#! /usr/bin/env python
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "click>=8.4",
# ]
# ///
"""Generate a Google Batch Job configuration file.

This runs on bare GitHub Actions runners, without the full pixi/pudl environment -- the
inline script metadata above lets ``uv run`` install just the handful of dependencies
this script actually needs (stdlib plus ``click``) into an ephemeral environment, rather
than requiring a full pudl install first.

The ``--container-*`` flags are named after their equivalents in ``gcloud compute
instances update-container``.
"""

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

import click

logging.basicConfig()
logger = logging.getLogger(__name__)

DEFAULT_MACHINE_TYPE = "c4d-standard-8"
DEFAULT_DISK_GB = 250
DEFAULT_DISK_TYPE = "hyperdisk-balanced"

# When --local-ssd-gb is set, the ETL's scratch directories are redirected off the
# (throughput-capped) Hyperdisk boot disk onto a Local SSD RAID array. Batch mounts
# the array on the host at /mnt/disks/<deviceName> and bind-mounts it into the
# container at the same path; a pre-runnable makes it writable by the non-root
# container user. The subdirectories mirror CONTAINER_PUDL_WORKSPACE in
# builds/Dockerfile.
LOCAL_SSD_DEVICE_NAME = "pudl-scratch"
LOCAL_SSD_MOUNT_PATH = f"/mnt/disks/{LOCAL_SSD_DEVICE_NAME}"
LOCAL_SSD_GB_INCREMENT = 375
_LOCAL_SSD_SCRATCH_ENV = {
    "PUDL_INPUT": f"{LOCAL_SSD_MOUNT_PATH}/input",
    "PUDL_OUTPUT": f"{LOCAL_SSD_MOUNT_PATH}/output",
    "DAGSTER_HOME": f"{LOCAL_SSD_MOUNT_PATH}/dagster_home",
}


def _parse_container_env(container_env: tuple[str, ...]) -> dict[str, str]:
    """Parse --container-env KEY=VALUE pairs into a dict.

    Raises if the same key is given more than once.
    """
    env_dict: dict[str, str] = {}
    for pair in sorted(container_env):
        name, value = pair.split("=", maxsplit=1)
        if name in env_dict:
            raise ValueError(f"Duplicate --container-env key: {name!r}")
        env_dict[name] = value.strip('"')
    return env_dict


def _lookup_machine_spec(machine_type: str) -> tuple[int, int]:
    """Return ``(cpuMilli, memoryMib)`` for a real GCE machine type, via ``gcloud``.

    Batch's ``computeResource.cpuMilli``/``memoryMib`` default to 2000/2000 (2 vCPU, 2
    GB) if left unset -- regardless of the machine type pinned in ``allocationPolicy``.
    This looks up the real values and fills them in so the job doesn't lie about the
    resources it has available.
    """
    gcloud_path = shutil.which("gcloud")
    if gcloud_path is None:
        raise click.ClickException(
            "gcloud CLI not found -- looking up machine type resources requires an "
            "authenticated gcloud (see google-github-actions/setup-gcloud)."
        )

    try:
        result = subprocess.run(  # noqa: S603
            [
                gcloud_path,
                "compute",
                "machine-types",
                "list",
                "--filter",
                f"name={machine_type}",
                "--limit",
                "1",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"gcloud lookup for machine type {machine_type!r} failed: {exc.stderr}"
        ) from exc

    matches = json.loads(result.stdout)
    if not matches:
        raise click.ClickException(
            f"No machine type found matching {machine_type!r}. Check the spelling, "
            "or that it's offered in at least one zone."
        )
    return matches[0]["guestCpus"] * 1000, matches[0]["memoryMb"]


def to_config(
    *,
    container_image: str,
    container_env: tuple[str, ...],
    container_command: str,
    container_arg: tuple[str, ...],
    machine_type: str,
    cpu_milli: int,
    memory_mib: int,
    disk_gb: int,
    disk_type: str,
    batch_job_id: str,
    pipeline: str,
    local_ssd_gb: int = 0,
) -> dict[str, Any]:
    """Munge arguments into a configuration dictionary."""
    if not container_image:
        raise ValueError("container_image is required")
    if not container_command:
        raise ValueError("container_command is required")
    if local_ssd_gb and local_ssd_gb % LOCAL_SSD_GB_INCREMENT != 0:
        raise ValueError(
            f"--local-ssd-gb must be a multiple of {LOCAL_SSD_GB_INCREMENT} "
            f"(each Local SSD partition is {LOCAL_SSD_GB_INCREMENT} GB); got {local_ssd_gb}"
        )

    env_dict = _parse_container_env(container_env)

    container: dict[str, Any] = {
        "imageUri": container_image,
        "commands": [container_command, *container_arg],
    }
    runnables: list[dict[str, Any]] = [
        {"container": container, "environment": {"variables": env_dict}},
    ]
    instance_disks: list[dict[str, Any]] = []
    task_volumes: list[dict[str, Any]] = []

    if local_ssd_gb:
        # Redirect the ETL's scratch dirs onto the Local SSD. These override the
        # defaults baked into builds/Dockerfile.
        env_dict.update(_LOCAL_SSD_SCRATCH_ENV)
        # Even for machine types with bundled Local SSDs (e.g. *-lssd), Batch
        # requires the array to be declared explicitly, sized to the bundled
        # total.
        instance_disks = [
            {
                "newDisk": {"sizeGb": str(local_ssd_gb), "type": "local-ssd"},
                "deviceName": LOCAL_SSD_DEVICE_NAME,
            }
        ]
        task_volumes = [
            {
                "deviceName": LOCAL_SSD_DEVICE_NAME,
                "mountPath": LOCAL_SSD_MOUNT_PATH,
                "mountOptions": "rw,async",
            }
        ]
        container["volumes"] = [f"{LOCAL_SSD_MOUNT_PATH}:{LOCAL_SSD_MOUNT_PATH}:rw"]
        # Batch mounts a freshly-formatted Local SSD root-owned; the container
        # runs as a non-root user, so open it up before the main runnable.
        runnables.insert(
            0,
            {
                "script": {"text": f"#!/bin/bash\nchmod 0777 {LOCAL_SSD_MOUNT_PATH}"},
            },
        )

    task_spec: dict[str, Any] = {
        "runnables": runnables,
        "computeResource": {
            "cpuMilli": cpu_milli,
            "memoryMib": memory_mib,
            "bootDiskMib": disk_gb * 1024,
        },
        "maxRunDuration": f"{60 * 60 * 12}s",
    }
    if task_volumes:
        task_spec["volumes"] = task_volumes

    policy: dict[str, Any] = {
        "machineType": machine_type,
        # Batch's default boot image is Container-Optimized OS, but Google's own
        # installOpsAgent bootstrap script only supports Debian/CentOS/Rocky (it
        # shells out to apt/yum, neither of which exist on COS) Pin the Debian
        # image explicitly so installOpsAgent actually works.
        "bootDisk": {
            "image": "batch-debian",
            "type": disk_type,
            "sizeGb": str(disk_gb),
        },
    }
    if instance_disks:
        policy["disks"] = instance_disks

    # NOTE (daz): the best documentation of the actual data structure I've found is at
    # https://cloud.google.com/python/docs/reference/batch/latest/google.cloud.batch_v1.types.Job
    return {
        "taskGroups": [{"taskSpec": task_spec}],
        "allocationPolicy": {
            "serviceAccount": {
                "email": "deploy-pudl-vm-service-account@catalyst-cooperative-pudl.iam.gserviceaccount.com"
            },
            # Explicitly set rather than relying on Batch to auto-label VM instances
            # with the job ID. Allows dashboards to group per-VM metrics by job.
            # "pipeline" is (by convention) the name of the GitHub Actions workflow that
            # launched the job, e.g. "build-pudl", "deploy-pudl",
            # "build-deploy-ferceqr"). This lets a single dashboard switch between
            # pipelines via a template variable presented in a dropdown menu.
            "labels": {"batch-job-id": batch_job_id, "pipeline": pipeline},
            "instances": [{"installOpsAgent": True, "policy": policy}],
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        # Batch copies these job-level labels onto every `batch_task_logs` entry (as
        # `labels.<key>`), unlike the `allocationPolicy` instance labels above which
        # only surface on VM metrics. Repeating `pipeline` here lets the dashboard's
        # Logs widget filter by pipeline via the `${pipeline}` template variable,
        # matching the behavior of the metric widgets.
        "labels": {
            "component": "build",
            "pipeline": pipeline,
        },
    }


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--container-image", required=True)
@click.option("--container-command", required=True)
@click.option(
    "--container-env",
    multiple=True,
    default=(),
    help="A KEY=VALUE container environment variable. Repeat for multiple.",
)
@click.option(
    "--container-arg",
    multiple=True,
    default=(),
    help="A container command argument. Repeat, in order, for multiple.",
)
@click.option(
    "--machine-type",
    default=DEFAULT_MACHINE_TYPE,
    show_default=True,
    help="GCE machine type to run the job on (e.g. c2d-highmem-16).",
)
@click.option(
    "--disk-gb",
    default=DEFAULT_DISK_GB,
    show_default=True,
    type=int,
    help="Size of the boot disk, in GB, to attach to the VM.",
)
@click.option(
    "--disk-type",
    default=DEFAULT_DISK_TYPE,
    show_default=True,
    help="Boot disk type (e.g. pd-ssd, pd-balanced, hyperdisk-balanced).",
)
@click.option(
    "--local-ssd-gb",
    default=0,
    show_default=True,
    type=int,
    help=(
        "Total Local SSD size in GB (a multiple of 375; must match the bundled "
        "total for *-lssd machine types). 0 disables Local SSD. When set, "
        "PUDL_INPUT/PUDL_OUTPUT/DAGSTER_HOME are redirected onto the Local SSD "
        "instead of the boot disk."
    ),
)
@click.option(
    "--batch-job-id",
    required=True,
    help=(
        "Value for the batch-job-id label attached to created VM instances, used "
        "to group per-VM Cloud Monitoring metrics by job. Should match the job "
        "name passed to `gcloud batch jobs submit`."
    ),
)
@click.option(
    "--pipeline",
    required=True,
    help=(
        "Value for the pipeline label attached to created VM instances and task "
        "logs. Use the name of the launching GitHub Actions workflow (e.g. "
        "build-pudl, deploy-pudl, build-deploy-ferceqr), used to switch the "
        "resource-usage dashboard between pipelines via a template variable."
    ),
)
@click.option(
    "--output",
    required=True,
    type=click.Path(path_type=Path),
    help="Path to write the generated Batch job JSON config to.",
)
def main(
    container_image: str,
    container_command: str,
    container_env: tuple[str, ...],
    container_arg: tuple[str, ...],
    machine_type: str,
    disk_gb: int,
    disk_type: str,
    local_ssd_gb: int,
    batch_job_id: str,
    pipeline: str,
    output: Path,
) -> None:
    """Generate a Batch configuration file."""
    cpu_milli, memory_mib = _lookup_machine_spec(machine_type)
    config = to_config(
        container_image=container_image,
        container_command=container_command,
        container_env=container_env,
        container_arg=container_arg,
        machine_type=machine_type,
        cpu_milli=cpu_milli,
        memory_mib=memory_mib,
        disk_gb=disk_gb,
        disk_type=disk_type,
        local_ssd_gb=local_ssd_gb,
        batch_job_id=batch_job_id,
        pipeline=pipeline,
    )

    logger.info(f"Writing to {output}")
    with output.open("w") as f:
        f.write(json.dumps(config, indent=2))


if __name__ == "__main__":
    main()
