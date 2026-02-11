#! /usr/bin/env python

"""Generate a Google Batch Job configuration file.

Since we're running this from a GHA runner that doesn't have our whole
environment installed, just uses stdlib.

Current shape is intended as a drop-in replacement for `gcloud compute
instances update-container`.
"""

import argparse
import itertools
import json
import logging
from collections import OrderedDict
from pathlib import Path
from typing import TypeVar

logging.basicConfig()
logger = logging.getLogger()

MIB_PER_GB = 1e9 / 2**20


T = TypeVar("T")


def _flat[T](ls: list[list[T]]) -> list[T]:
    return list(itertools.chain.from_iterable(ls))


def to_config(
    *,
    container_image: str,
    container_env: list[list[str]],
    container_command: str,
    container_arg: str,
    vcpu: int,
    mem_gb: int,
    disk_gb: int,
) -> dict:
    """Munge arguments into a configuration dictionary."""
    complete_env = sorted(_flat(container_env))
    env_dict = OrderedDict(
        (name, value.strip('"'))
        for name, value in (pair.split("=", maxsplit=1) for pair in complete_env)
    )

    # NOTE (daz): the best documentation of the actual data structure I've found is at
    # https://cloud.google.com/python/docs/reference/batch/latest/google.cloud.batch_v1.types.Job
    task_spec = {
        "runnables": [
            {
                "container": {
                    "imageUri": container_image,
                    "commands": [container_command] + _flat(container_arg),
                },
                "environment": {"variables": env_dict},
            },
        ],
        "computeResource": {
            "cpuMilli": vcpu * 1000,
            "memoryMib": int(mem_gb * MIB_PER_GB),
            "bootDiskMib": disk_gb * 1024,
        },
        "maxRunDuration": f"{60 * 60 * 1}s",  # 1 hour
    }
    config = {
        "taskGroups": [{"taskSpec": task_spec}],
        "allocationPolicy": {
            "serviceAccount": {
                "email": "deploy-pudl-vm-service-account@catalyst-cooperative-pudl.iam.gserviceaccount.com"
            },
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        "labels": {
            "component": "build",
        },
    }
    return config


def generate_batch_config():
    """Generate a Batch configuration file.

    Take almost all the same arguments as `gcloud compute instances
    update-container`, but output a Batch configuration json instead.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--container-image")
    parser.add_argument("--container-command")
    parser.add_argument("--container-env", action="append", nargs="*", default=[])
    parser.add_argument("--container-arg", action="append", nargs="*", default=[])
    parser.add_argument(
        "--vcpu", default=8, type=int, help="Number of vCPUs to give VM"
    )
    parser.add_argument(
        "--mem-gb", default=63, type=int, help="Total RAM in GB to give VM"
    )
    parser.add_argument(
        "--disk-gb", default=200, type=int, help="Size of disk in GB to attach to VM"
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    config = to_config(
        container_image=args.container_image,
        container_command=args.container_command,
        container_arg=args.container_arg,
        container_env=args.container_env,
        vcpu=args.vcpu,
        mem_gb=args.mem_gb,
        disk_gb=args.disk_gb,
    )

    logger.info(f"Writing to {args.output}")
    with args.output.open("w") as f:
        f.write(json.dumps(config, indent=2))


if __name__ == "__main__":
    generate_batch_config()
