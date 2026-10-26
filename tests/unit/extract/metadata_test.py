"""Unit tests for the metadata files that only have rows for the partitions that change.

The files that describe the structure of a dataset do not need a row for every
partition, only for those where the structure is different from the partition before.
"""

from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from pudl import PUDL_PACKAGE_DATA_PATH
from pudl.extract.excel import ExcelMetadata
from pudl.extract.extractor import (
    GenericMetadata,
    expand_partitions,
    working_time_partitions,
)

CHANGE_LOG_PATHS = sorted(
    [
        *PUDL_PACKAGE_DATA_PATH.glob("*/column_maps/*.csv"),
        *PUDL_PACKAGE_DATA_PATH.glob("*/page_map.csv"),
        *PUDL_PACKAGE_DATA_PATH.glob("*/skiprows.csv"),
        *PUDL_PACKAGE_DATA_PATH.glob("*/skipfooter.csv"),
    ]
)
"""Packaged files that only have rows for the partitions that change."""


def _partitioned(rows: dict[str, list]) -> pd.DataFrame:
    """Make a frame that is indexed by string partitions, as the files are loaded."""
    df = pd.DataFrame(rows)
    df.index = pd.Index(df.pop("partition"), dtype=pd.StringDtype())
    return df


def _drop_unchanged_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the rows that differ from the row before, as in a change log.

    Empty cells are the same as each other, which is not true of pandas' equality. The
    first row is always kept, even if it is empty, as it is what the rest follow from.
    """
    previous = df.shift()
    same_as_previous = ((df == previous) | (df.isna() & previous.isna())).all(axis=1)
    same_as_previous.iloc[0] = False
    return df[~same_as_previous]


def test_expand_partitions_carries_rows_forward():
    """A row describes the partitions after it, until the next row."""
    changes = _partitioned({"partition": ["2010", "2013"], "skiprows": [1, 4]})
    expanded = expand_partitions(changes, ["2010", "2011", "2012", "2013", "2014"])
    expected = _partitioned(
        {
            "partition": ["2010", "2011", "2012", "2013", "2014"],
            "skiprows": [1, 1, 1, 4, 4],
        }
    )
    pdt.assert_frame_equal(expanded, expected)


def test_expand_partitions_keeps_dtypes():
    """Integers stay integers, even though some rows are filled in."""
    changes = _partitioned(
        {"partition": ["2010", "2012"], "rows": [1, 2], "name": ["a", "b"]}
    )
    expanded = expand_partitions(changes, ["2010", "2011", "2012", "2013"])
    assert expanded["rows"].dtype == changes["rows"].dtype
    assert expanded["rows"].tolist() == [1, 1, 2, 2]
    assert expanded["name"].tolist() == ["a", "a", "b", "b"]


def test_expand_partitions_carries_whole_rows():
    """An empty cell means the data is not there, not that it is the same as before."""
    changes = _partitioned(
        {"partition": ["2010", "2012"], "a": ["x", None], "b": ["y", "z"]}
    )
    expanded = expand_partitions(changes, ["2010", "2011", "2012", "2013"])
    assert expanded["a"].tolist()[:2] == ["x", "x"]
    assert expanded["a"].isna().tolist() == [False, False, True, True]
    assert expanded["b"].tolist() == ["y", "y", "z", "z"]


def test_expand_partitions_does_not_describe_partitions_before_the_first_row():
    """Nothing is carried backward, so there is no data to infer for earlier years."""
    changes = _partitioned({"partition": ["2012"], "skiprows": [3]})
    expanded = expand_partitions(changes, ["2010", "2011", "2012", "2013"])
    assert list(expanded.index) == ["2012", "2013"]
    assert expanded["skiprows"].dtype == changes["skiprows"].dtype


def test_expand_partitions_keeps_rows_for_partitions_that_are_not_listed():
    """The partitions of the rows are described whether or not they are listed."""
    changes = _partitioned({"partition": ["2010", "2011"], "skiprows": [1, 2]})
    expanded = expand_partitions(changes, ["2011", "2012"])
    assert list(expanded.index) == ["2010", "2011", "2012"]
    assert expanded["skiprows"].tolist() == [1, 2, 2]


def test_expand_partitions_leaves_a_row_for_every_partition_as_it_is():
    """A file that was not converted to the change log format is still valid."""
    every_row = _partitioned(
        {"partition": ["2010", "2011", "2012"], "skiprows": [1, 1, 2]}
    )
    pdt.assert_frame_equal(
        expand_partitions(every_row, ["2010", "2011", "2012"]), every_row
    )
    pdt.assert_frame_equal(expand_partitions(every_row, []), every_row)


def test_expand_partitions_orders_months_and_half_years():
    """Partitions that are not years sort as the dates that they are."""
    months = _partitioned({"partition": ["2024-11"], "skiprows": [1]})
    expanded = expand_partitions(months, ["2024-12", "2025-01", "2025-02"])
    assert list(expanded.index) == ["2024-11", "2024-12", "2025-01", "2025-02"]
    half_years = _partitioned({"partition": ["2015half2"], "skiprows": [1]})
    expanded = expand_partitions(half_years, ["2016half1", "2016half2"])
    assert list(expanded.index) == ["2015half2", "2016half1", "2016half2"]


@pytest.mark.parametrize(
    "partitions",
    [["2011", "2010"], ["2010", "2010"]],
    ids=["unordered", "duplicated"],
)
def test_expand_partitions_requires_unique_ordered_partitions(partitions):
    """The closest row before a partition is only defined for ordered partitions."""
    changes = _partitioned({"partition": partitions, "skiprows": [1, 2]})
    with pytest.raises(AssertionError, match="unique and in ascending order"):
        expand_partitions(changes, ["2010", "2011"])


def test_expand_partitions_of_an_empty_frame_is_empty():
    """There is nothing to carry forward."""
    empty = _partitioned({"partition": [], "skiprows": []})
    assert expand_partitions(empty, ["2010"]).empty


def test_working_time_partitions():
    """The partitions are those of the data source, as strings in order."""
    assert working_time_partitions("eia860")[:2] == ["2001", "2002"]
    assert working_time_partitions("eia860m")[:2] == ["2015-07", "2015-08"]
    assert working_time_partitions("eia930")[:2] == ["2015half2", "2016half1"]
    assert working_time_partitions("not_a_data_source") == []


def test_change_log_files_describe_every_working_partition(tmp_path: Path):
    """Change log files are expanded to the partitions that the metadata is given."""
    (tmp_path / "column_maps").mkdir()
    (tmp_path / "column_maps" / "page.csv").write_text(
        "year_index,col\n2010,old\n2013,new\n"
    )
    (tmp_path / "skiprows.csv").write_text("year_index,page\n2010,0\n2012,5\n")
    (tmp_path / "skipfooter.csv").write_text("year_index,page\n2010,0\n")
    (tmp_path / "page_map.csv").write_text("year_index,page\n2010,0\n")
    (tmp_path / "file_map.csv").write_text(
        "year_index,page\n2010,a.xlsx\n2011,b.xlsx\n2012,c.xlsx\n2013,d.xlsx\n"
    )
    metadata = ExcelMetadata(
        "fake",
        path=tmp_path,
        partitions=["2010", "2011", "2012", "2013", "2014"],
    )
    assert metadata.get_column_map("page", year=2012) == {"old": "col"}
    assert metadata.get_column_map("page", year=2013) == {"new": "col"}
    assert metadata.get_column_map("page", year=2014) == {"new": "col"}
    assert [metadata.get_skiprows("page", year=y) for y in range(2010, 2015)] == [
        0,
        0,
        5,
        5,
        5,
    ]
    assert metadata.get_skipfooter("page", year=2014) == 0
    assert metadata.get_sheet_name("page", year=2014) == 0


def test_change_log_files_describe_the_partitions_of_the_file_map(tmp_path: Path):
    """The file map lists partitions that are not working partitions of the source."""
    (tmp_path / "column_maps").mkdir()
    (tmp_path / "column_maps" / "page.csv").write_text("year_index,col\n2010,old\n")
    (tmp_path / "file_map.csv").write_text(
        "year_index,page\n2010,a.csv\n2011,b.csv\n2012,-1\n"
    )
    metadata = GenericMetadata("fake", path=tmp_path, partitions=[])
    assert list(metadata._column_map["page"].index) == ["2010", "2011", "2012"]
    assert metadata.get_column_map("page", year=2012) == {"old": "col"}


def test_file_map_is_not_a_change_log(tmp_path: Path):
    """The names of the source files differ by partition, so each needs a row."""
    (tmp_path / "column_maps").mkdir()
    (tmp_path / "file_map.csv").write_text("year_index,page\n2010,a.xlsx\n")
    metadata = GenericMetadata("fake", path=tmp_path, partitions=["2010", "2011"])
    assert list(metadata._load_file_map().index) == ["2010"]


def test_metadata_of_a_fake_dataset_has_the_partitions_of_its_file_map():
    """The fake datasets are not data sources, so only their file maps list partitions."""
    metadata = ExcelMetadata(
        "test", path=Path(__file__).parents[2] / "data/extract/test"
    )
    assert working_time_partitions("test") == []
    assert metadata._partitions == ["2010", "2011", "2012", "2013", "2014"]
    assert list(metadata._column_map["books"].index) == metadata._partitions


def _path_id(path: Path) -> str:
    """Name a packaged file by its dataset and file name."""
    return f"{path.relative_to(PUDL_PACKAGE_DATA_PATH)}"


@pytest.mark.parametrize("path", CHANGE_LOG_PATHS, ids=_path_id)
def test_packaged_change_log_files_have_no_repeated_rows(path: Path):
    """A row that is the same as the row before it should not be in the file."""
    df = pd.read_csv(path, index_col=0, dtype=str, keep_default_na=False)
    repeated = list(df.index.difference(_drop_unchanged_rows(df).index))
    assert not repeated, (
        f"{_path_id(path)} has rows for the partitions {repeated} that are the same "
        "as the row before. Only the partitions that change need a row."
    )


@pytest.mark.parametrize("path", CHANGE_LOG_PATHS, ids=_path_id)
def test_packaged_change_log_files_use_minus_one_for_absent_data(path: Path):
    """Data that does not exist in a partition is -1, never an empty cell."""
    df = pd.read_csv(path, index_col=0, dtype=str, keep_default_na=False)
    empty = [
        (partition, column)
        for column in df.columns
        for partition in df.index[df[column] == ""]
    ]
    assert not empty, f"{_path_id(path)} has empty cells at {empty}. Use -1."
