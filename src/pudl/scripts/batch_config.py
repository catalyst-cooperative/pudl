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
from collections import OrderedDict
from pathlib import Path
from typing import Any

import click

logging.basicConfig()
logger = logging.getLogger(__name__)

DEFAULT_MACHINE_TYPE = "e2-highmem-8"
DEFAULT_DISK_GB = 250
DEFAULT_DISK_TYPE = "pd-balanced"


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


def to_config(
    *,
    container_image: str,
    container_env: tuple[str, ...],
    container_command: str,
    container_arg: tuple[str, ...],
    machine_type: str,
    disk_gb: int,
    disk_type: str,
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
            "instances": [
                {
                    "policy": {
                        "machineType": machine_type,
                        "bootDisk": {"type": disk_type, "sizeGb": str(disk_gb)},
                    }
                }
            ],
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        "labels": {
            "component": "build",
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
    help="Boot disk type (e.g. pd-ssd, pd-balanced, pd-standard).",
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
    output: Path,
) -> None:
    """Generate a Batch configuration file."""
    config = to_config(
        container_image=container_image,
        container_command=container_command,
        container_env=container_env,
        container_arg=container_arg,
        machine_type=machine_type,
        disk_gb=disk_gb,
        disk_type=disk_type,
    )

    logger.info(f"Writing to {output}")
    with output.open("w") as f:
        f.write(json.dumps(config, indent=2))


if __name__ == "__main__":
    main()
