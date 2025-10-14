"""Setup so that we can profile memory usage of Dagster runs.

**DO NOT IMPORT MANUALLY, ANYWHERE!**
"""

import datetime
import os

import psutil
from memray import Tracker

if os.getenv("MEMRAY_PROFILE", False):
    us = psutil.Process()
    forkserver_spawner = us.parent()
    if forkserver_spawner is None:
        raise RuntimeError("Somehow there was no spawner for this forkserver.")
    api_server = forkserver_spawner.parent()
    if api_server is None:
        raise RuntimeError(
            "Somehow there was no api_server that made the forkserver spawner."
        )
    args = api_server.cmdline()
    location_name_index = args.index("--location-name") + 1
    location_name = args[location_name_index]
    utcnow = datetime.datetime.now(datetime.UTC)
    filename = f"{location_name}-{utcnow:%Y_%m_%dT%H_%M_%S}-{us.pid}.bin"
    memray_tracker = Tracker(file_name=filename, follow_fork=True)
    memray_tracker.__enter__()
