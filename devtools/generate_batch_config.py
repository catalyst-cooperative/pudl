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

logging.basicConfig()
logger = logging.getLogger()

MIB_PER_GB = 1e9 / 2**20


def _flat(ls: list[list]) -> list:
    return list(itertools.chain.from_iterable(ls))


def to_config(
    *,
    container_image: str,
    container_env: list[list[str]],
    container_command: str,
    container_arg: str,
) -> dict:
    """Munge arguments into a configuration dictionary."""
    complete_env = sorted(_flat(container_env))
    env_dict = OrderedDict(
        (name, value.strip('"'))
        for name, value in (pair.split("=", maxsplit=1) for pair in complete_env)
    )

    # NOTE (daz): the best documentation of the actual data structure I've found is at
    # https://cloud.google.com/python/docs/reference/batch/latest/google.cloud.batch_v1.types.Job
    config = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": container_image,
                                "commands": [container_command] + _flat(container_arg),
                            },
                            "environment": {"variables": env_dict},
                        }
                    ],
                    "computeResource": {
                        "cpuMilli": 16000,
                        "memoryMib": int(127 * MIB_PER_GB),
                        "bootDiskMib": 200 * 1024,
                    },
                    "maxRunDuration": f"{60 * 60 * 12}s",
                }
            }
        ],
        "allocationPolicy": {
            "serviceAccount": {
                "email": "deploy-pudl-vm-service-account@catalyst-cooperative-pudl.iam.gserviceaccount.com"
            },
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
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
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    config = to_config(
        container_image=args.container_image,
        container_command=args.container_command,
        container_arg=args.container_arg,
        container_env=args.container_env,
    )

    logger.info(f"Writing to {args.output}")
    with args.output.open("w") as f:
        f.write(json.dumps(config, indent=2))


if __name__ == "__main__":
    generate_batch_config()
