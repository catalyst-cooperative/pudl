"""Check that the partitions that we process and map are in the raw archives.

The working partitions of a data source and the partitions in the ``file_map.csv`` of
its dataset must all be partitions of the archive on Zenodo that the dataset is pinned
to in ``zenodo_dois.yml``, as described by its ``datapackage.json``. The archive can
have more partitions than these, which we have not mapped or processed yet.
"""

import pandas as pd
import pytest
from requests.exceptions import ConnectionError, RetryError  # noqa: A004
from urllib3.exceptions import MaxRetryError, ResponseError

from pudl import PUDL_PACKAGE_DATA_PATH
from pudl.extract.extractor import TIME_PARTITION_KEYS, working_time_partitions
from pudl.metadata.sources import SOURCES
from pudl.workspace.datastore import Datastore


def _time_key(dataset: str) -> str:
    """Name the time partition of a dataset's archive, like ``year`` or ``year_month``.

    The working partitions of a data source are named in the plural (``years``), and
    the partitions of an archive in the singular.
    """
    working_partitions = SOURCES[dataset]["working_partitions"]
    (key,) = (k for k in TIME_PARTITION_KEYS if working_partitions.get(k))
    return key.removesuffix("s")


def _mapped_partitions(dataset: str) -> set[str]:
    """List the partitions in the ``file_map.csv`` of a dataset, if it has one."""
    path = PUDL_PACKAGE_DATA_PATH / dataset / "file_map.csv"
    if not path.exists():
        return set()
    return set(pd.read_csv(path, usecols=[0], comment="#", dtype=str).iloc[:, 0])


NOT_PARTITIONED_BY_TIME = {
    "ferc714": "2006-2020 are in one CSV archive, ferc714.zip, which has no partitions.",
    "sec10k": "The archive is partitioned by table, not by time.",
}
"""Datasets whose archive descriptors do not list their working partitions."""

DATASETS = sorted(
    name
    for name in SOURCES
    if (working_time_partitions(name) or _mapped_partitions(name))
    and name not in NOT_PARTITIONED_BY_TIME
)


@pytest.mark.parametrize("dataset", DATASETS)
def test_processed_and_mapped_partitions_are_in_the_archive(
    dataset: str, zenodo_datastore: Datastore
):
    """The partitions that are processed or mapped are all in the raw archive."""
    try:
        descriptor = zenodo_datastore.get_datapackage_descriptor(dataset)
    except (MaxRetryError, ConnectionError, RetryError, ResponseError) as error:
        pytest.xfail(f"Could not fetch the datapackage of {dataset}: {error}")
    in_the_archive = {
        str(partition)
        for partition in descriptor.get_partitions().get(_time_key(dataset), set())
    }
    working = set(working_time_partitions(dataset))
    mapped = _mapped_partitions(dataset)
    assert in_the_archive, (
        f"The {dataset} archive has no {_time_key(dataset)} partitions."
    )
    assert working <= in_the_archive, (
        f"Working partitions of {dataset} not in the archive: "
        f"{sorted(working - in_the_archive)}"
    )
    assert mapped <= in_the_archive, (
        f"Partitions in the file map of {dataset} not in the archive: "
        f"{sorted(mapped - in_the_archive)}"
    )
