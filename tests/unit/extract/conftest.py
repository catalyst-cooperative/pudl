"""Fixtures for the unit tests of the extractors and their metadata."""

from collections.abc import Callable, Sequence
from pathlib import Path

import pytest


@pytest.fixture
def fake_package_data(
    mocker, tmp_path: Path
) -> Callable[[str, Sequence[str], Path | None], Path]:
    """Make a dataset that the metadata classes load as if it were in the package data.

    The metadata classes find the files of a dataset in the package data, and its
    working partitions in the data sources. This fixture points both of those at a
    fake dataset for the duration of a test, so the tests don't need any test-only
    arguments in the metadata classes.

    Returns a function that takes the name of the dataset, its working partitions
    (as years) and optionally the directory that holds the dataset, which is a new
    temporary directory by default. It returns the directory of the dataset, which
    exists, for the test to write the metadata files to.
    """

    def _fake_package_data(
        dataset_name: str,
        working_partitions: Sequence[str] = (),
        root: Path | None = None,
    ) -> Path:
        root = tmp_path if root is None else root
        mocker.patch("pudl.extract.extractor.PUDL_PACKAGE_DATA_PATH", root)
        if working_partitions:
            mocker.patch.dict(
                "pudl.extract.extractor.SOURCES",
                {
                    dataset_name: {
                        "working_partitions": {"years": list(working_partitions)}
                    }
                },
            )
        dataset_path = root / dataset_name
        dataset_path.mkdir(exist_ok=True)
        return dataset_path

    return _fake_package_data
