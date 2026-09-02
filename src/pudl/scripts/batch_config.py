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
from collections import OrderedDict
from pathlib import Path
from typing import Any

import click

logging.basicConfig()
logger = logging.getLogger(__name__)

DEFAULT_MACHINE_TYPE = "e2-highmem-8"
DEFAULT_DISK_GB = 250
DEFAULT_DISK_TYPE = "hyperdisk-balanced"


def _parse_container_env(container_env: tuple[str, ...]) -> "OrderedDict[str, str]":
    """Parse --container-env KEY=VALUE pairs into an ordered dict.

    Raises if the same key is given more than once. A repeated key almost always
    means a bug in the calling workflow (e.g. two steps setting the same envvar)
    rather than an intentional override -- silently keeping only the last value
    previously made that kind of bug invisible.
    """
    env_dict: OrderedDict[str, str] = OrderedDict()
    for pair in sorted(container_env):
        name, value = pair.split("=", maxsplit=1)
        if name in env_dict:
            raise ValueError(f"Duplicate --container-env key: {name!r}")
        env_dict[name] = value.strip('"')
    return env_dict


def _lookup_machine_spec(machine_type: str) -> tuple[int, int]:
    """Return ``(cpuMilli, memoryMib)`` for a real GCE machine type, via ``gcloud``.

    Batch's ``computeResource.cpuMilli``/``memoryMib`` default to 2000/2000 (2 vCPU,
    2 GB) if left unset -- regardless of the machine type pinned in
    ``allocationPolicy``. That mismatch is exactly what shows up as an
    apparently-tiny job in the Cloud Console's job list and Batch API, even though
    the VM Batch actually provisions has the pinned machine type's real resources.

    Newer machine families don't use clean per-vCPU memory ratios (e.g.
    c4d-highmem-16 is 126 GiB for 16 vCPU -- 7.875 GiB/vCPU, not 8), so hardcoding a
    ratio table here would silently drift wrong as new families launch. A machine
    type's vCPU/memory shape is identical in every zone that offers it, so a single
    global lookup (no zone/region needed) via the ``gcloud`` CLI -- already
    authenticated in every workflow that calls this script -- gets the real,
    always-current numbers directly from GCE itself.
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
) -> dict[str, Any]:
    """Munge arguments into a configuration dictionary."""
    if not container_image:
        raise ValueError("container_image is required")
    if not container_command:
        raise ValueError("container_command is required")

    env_dict = _parse_container_env(container_env)

    # NOTE (daz): the best documentation of the actual data structure I've found is at
    # https://cloud.google.com/python/docs/reference/batch/latest/google.cloud.batch_v1.types.Job
    return {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": container_image,
                                "commands": [container_command, *container_arg],
                            },
                            "environment": {"variables": env_dict},
                        },
                    ],
                    "computeResource": {
                        "cpuMilli": cpu_milli,
                        "memoryMib": memory_mib,
                        "bootDiskMib": disk_gb * 1024,
                    },
                    "maxRunDuration": f"{60 * 60 * 12}s",
                }
            }
        ],
        "allocationPolicy": {
            "serviceAccount": {
                "email": "deploy-pudl-vm-service-account@catalyst-cooperative-pudl.iam.gserviceaccount.com"
            },
            # Explicitly set rather than relying on Batch to auto-label VM instances
            # with the job ID: it used to (older jobs' VMs carried a `batch-job-id`
            # user label Cloud Monitoring dashboards group by), but recent VMs no
            # longer do -- Batch's behavior here apparently changed. Setting it
            # ourselves is the only way to guarantee dashboards can group per-VM
            # metrics by job regardless of what Batch does internally. `pipeline`
            # (e.g. "ferceqr", "pudl", "pudl-deploy") lets a single dashboard switch
            # between pipelines via a template variable, instead of needing separate
            # per-pipeline dashboards or widgets.
            "labels": {"batch-job-id": batch_job_id, "pipeline": pipeline},
            "instances": [
                {
                    "installOpsAgent": True,
                    "policy": {
                        "machineType": machine_type,
                        # Batch's default boot image is Container-Optimized OS, but
                        # Google's own installOpsAgent bootstrap script only supports
                        # Debian/CentOS/Rocky (it shells out to apt/yum, neither of
                        # which exist on COS) -- confirmed via job logs showing the
                        # agent install silently doing nothing on COS. Pin the
                        # Debian image explicitly so installOpsAgent actually works.
                        "bootDisk": {
                            "image": "batch-debian",
                            "type": disk_type,
                            "sizeGb": str(disk_gb),
                        },
                    },
                }
            ],
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        # Batch copies these job-level labels onto every `batch_task_logs` entry
        # (as `labels.<key>`), unlike the `allocationPolicy` instance labels above
        # which only surface on VM metrics. Repeating `pipeline` here lets the
        # dashboard's Logs widget filter by pipeline via the `${pipeline}`
        # template variable, matching the behavior of the metric widgets.
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
        "Value for the pipeline label attached to created VM instances (e.g. "
        "ferceqr, pudl, pudl-deploy), used to switch the resource-usage dashboard "
        "between pipelines via a template variable."
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
        batch_job_id=batch_job_id,
        pipeline=pipeline,
    )

    logger.info(f"Writing to {output}")
    with output.open("w") as f:
        f.write(json.dumps(config, indent=2))


if __name__ == "__main__":
    main()
