"""Unit tests for loading the ``file_map.csv`` metadata of the extractors."""

from pathlib import Path

import pandas as pd
import pytest

from pudl import PUDL_PACKAGE_DATA_PATH
from pudl.extract.extractor import GenericMetadata

FAKE_FILE_MAP_PATH = Path(__file__).parents[2] / "data/extract/test/file_map.csv"
ALL_FILE_MAP_PATHS = [
    *sorted(PUDL_PACKAGE_DATA_PATH.glob("*/file_map.csv")),
    FAKE_FILE_MAP_PATH,
]


def _read_packaged_file_map(path) -> pd.DataFrame:
    """Read a file map as it is loaded by the extractors, with all cells as strings."""
    return pd.read_csv(path, index_col=0, comment="#", dtype=str)


def _never_observed_pages(file_map: pd.DataFrame) -> list[str]:
    """Return the pages that have no file (``-1``) in every partition."""
    return [page for page in file_map.columns if (file_map[page] == "-1").all()]


def test_file_map_with_empty_cells_is_rejected(tmp_path):
    """A file map with an empty cell is an error rather than a missing value."""
    (tmp_path / "file_map.csv").write_text("page,books\n2010,a.csv\n2011,\n")
    (tmp_path / "column_maps").mkdir()
    metadata = GenericMetadata("test", path=tmp_path)
    with pytest.raises(AssertionError, match="empty cells"):
        metadata._load_file_map()


def test_file_map_keeps_no_file_marker_as_string(tmp_path):
    """A column holding only ``-1`` stays the string "-1", not the integer -1."""
    (tmp_path / "file_map.csv").write_text("page,books\n2010,-1\n2011,-1\n")
    (tmp_path / "column_maps").mkdir()
    metadata = GenericMetadata("test", path=tmp_path)
    assert metadata._load_file_map().loc["2010", "books"] == "-1"


def test_never_observed_pages_finds_columns_that_are_all_no_file():
    """Only the pages with no file for any partition are identified."""
    file_map = pd.DataFrame(
        {"seen": ["a.csv", "-1"], "sometimes": ["-1", "b.csv"], "never": ["-1", "-1"]},
        index=["2010", "2011"],
    )
    assert _never_observed_pages(file_map) == ["never"]


@pytest.mark.parametrize("path", ALL_FILE_MAP_PATHS, ids=lambda p: p.parent.name)
def test_packaged_file_maps_have_no_empty_cells(path):
    """Every page and partition of a packaged file map has a file name, or ``-1``."""
    file_map = _read_packaged_file_map(path)
    empty_cells = [
        (partition, page)
        for page in file_map.columns
        for partition in file_map.index[file_map[page].isna()]
    ]
    assert not empty_cells, (
        f"{path.parent.name}/file_map.csv has empty cells at {empty_cells}. "
        "Use -1 for a page with no file."
    )


@pytest.mark.parametrize("path", ALL_FILE_MAP_PATHS, ids=lambda p: p.parent.name)
def test_packaged_file_maps_have_no_never_observed_pages(path):
    """A page with ``-1`` for every partition is never observed, so is not mapped."""
    never_observed = _never_observed_pages(_read_packaged_file_map(path))
    assert not never_observed, (
        f"{path.parent.name}/file_map.csv has no file in any partition for pages "
        f"{never_observed}. Remove these columns, since the pages are never observed."
    )
