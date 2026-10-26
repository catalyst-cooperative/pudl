"""Check that the hand-compiled metadata files of each dataset agree with each other.

The files in ``src/pudl/package_data/${dataset}`` describe the structure of the raw
inputs of a dataset: which file holds each page in each partition (``file_map.csv``),
which sheet, how many rows to skip (``page_map.csv``, ``skiprows.csv`` and
``skipfooter.csv``) and how the columns are named (``column_maps/${page}.csv``). They
are compiled by hand, so it is easy for them to disagree about, for example, which
pages or partitions exist. The checks here do not look at the raw data, only at
whether the files are consistent with each other. Mistakes in the structure of the
raw data are found when it is extracted, and the extracted columns are compared with
the column maps.

``file_map.csv`` is the source of truth for whether a page exists in a partition: a
file name of ``-1`` means that it does not. Every other file uses ``-1`` in the same
cases, and for columns of a column map that do not exist.

Known violations are listed in :data:`KNOWN_VIOLATIONS` with how many there are, so
that new ones are noticed, as are old ones that have been fixed. Each is either an
``error``, which is wrong and should be fixed, or ``unfinished``, which is correct but
not yet completely mapped. Mapping all of the data in the spreadsheets and CSV files
is a goal, so the ``unfinished`` ones are a list of the mapping work that is left.
"""

import csv
import io
import re
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache, cached_property
from pathlib import Path
from typing import Literal, NamedTuple

import pandas as pd
import pytest

from pudl import PUDL_PACKAGE_DATA_PATH
from pudl.extract.excel import ExcelMetadata
from pudl.extract.extractor import GenericMetadata, working_time_partitions

ABSENT = "-1"
"""The value of data that does not exist in a partition."""

PARTITION_FORMATS = {
    "years": r"\d{4}",
    "year_months": r"\d{4}-\d{2}",
    "half_years": r"\d{4}half[12]",
}
"""Regular expressions of the labels of each kind of time partition."""

NO_PARTITION = "any_year"
"""The label of the one partition of datasets that are not split by time."""


def _strings(df: pd.DataFrame) -> pd.DataFrame:
    """Compare the cells of frames as the text of the files, whatever their dtypes."""
    return df.astype(str)


def _mapped(row: pd.Series) -> pd.Series:
    """Select the columns of a row of a column map that are in the partition."""
    return row[row != ABSENT]


@dataclass
class MetadataView:
    """The metadata of a dataset, and the files that it was loaded from."""

    name: str
    path: Path
    metadata: GenericMetadata
    working_partitions: list[str]

    @classmethod
    def of_packaged_dataset(cls, name: str) -> MetadataView:
        """Load the metadata of a dataset in the package data, as the extractors do."""
        path = PUDL_PACKAGE_DATA_PATH / name
        metadata_class = ExcelMetadata if (path / "skiprows.csv").exists() else None
        metadata = (metadata_class or GenericMetadata)(name)
        return cls(name, path, metadata, working_time_partitions(name))

    @classmethod
    def of_fake_dataset(cls, path: Path, working_partitions: list[str]) -> MetadataView:
        """Load the metadata of a dataset made to test the checks."""
        metadata = ExcelMetadata("fake", path=path, partitions=working_partitions)
        return cls("fake", path, metadata, working_partitions)

    @property
    def is_excel(self) -> bool:
        """Whether the dataset has the page, skiprows and skipfooter files."""
        return isinstance(self.metadata, ExcelMetadata)

    @property
    def has_file_map(self) -> bool:
        """Whether the dataset has a file map to say which pages exist."""
        return (self.path / "file_map.csv").exists()

    @cached_property
    def column_maps(self) -> dict[str, pd.DataFrame]:
        """The column map of each page, with a row for every partition."""
        return {page: _strings(df) for page, df in self.metadata._column_map.items()}

    @cached_property
    def file_map(self) -> pd.DataFrame:
        """The names of the source files of each page (columns) and partition (rows)."""
        assert isinstance(self.metadata, ExcelMetadata) or self.has_file_map
        if isinstance(self.metadata, ExcelMetadata):
            return _strings(self.metadata._file_name)
        return _strings(self.metadata._load_file_map())

    @cached_property
    def page_files(self) -> dict[str, pd.DataFrame]:
        """The files with a column for each page and a row for each partition."""
        assert isinstance(self.metadata, ExcelMetadata)
        return {
            "page_map": _strings(self.metadata._sheet_name),
            "skiprows": _strings(self.metadata._skiprows),
            "skipfooter": _strings(self.metadata._skipfooter),
        }

    def has_file(self, page: str, partition: str) -> bool:
        """Whether the file map has a source file for a page and partition."""
        return self.file_map.loc[partition, page] != ABSENT

    def listed_partitions(self) -> dict[str, list[str]]:
        """List the partitions that each file has rows for, before they are expanded."""
        names = [f"column_maps/{p.name}" for p in (self.path / "column_maps").glob("*")]
        if self.is_excel:
            names += ["page_map.csv", "skiprows.csv", "skipfooter.csv"]
        return {
            name: [
                str(p)
                for p in pd.read_csv(self.path / name, index_col=0, dtype=str).index
            ]
            for name in sorted(names)
        }

    def header(self, name: str) -> list[str]:
        """List the names of the columns of a file, as they are written in it."""
        text = (self.path / name).read_text()
        return next(csv.reader(io.StringIO(text)))[1:]


def _partition_labels(view: MetadataView) -> list | None:
    """Find the partition labels of files that are not of the kind of the source."""
    kind = next(
        (
            k
            for k, pattern in PARTITION_FORMATS.items()
            if view.working_partitions
            and re.fullmatch(pattern, view.working_partitions[0])
        ),
        None,
    )
    if kind is None:
        return None
    return [
        (name, partition)
        for name, partitions in view.listed_partitions().items()
        for partition in partitions
        if partition != NO_PARTITION
        and not re.fullmatch(PARTITION_FORMATS[kind], partition)
    ]


def _raw_columns_mapped_twice(view: MetadataView) -> list:
    """Find the rows of column maps in which a raw column is given to two columns.

    A column map is used as a dictionary from the raw columns to the columns, so all of
    the columns with the same raw column but the last would be lost.
    """
    found = []
    for page, column_map in view.column_maps.items():
        for partition, row in column_map.iterrows():
            mapped = _mapped(row)
            repeated = mapped[mapped.duplicated(keep=False)]
            if len(repeated):
                found.append((page, partition, sorted(set(repeated))[0]))
    return found


def _repeated_column_headers(view: MetadataView) -> list:
    """Find the files in which two columns have the same name.

    Pandas gives the second of them a different name, so the metadata has a column that
    no one meant.
    """
    names = [f"column_maps/{p.name}" for p in (view.path / "column_maps").glob("*")]
    if view.is_excel:
        names += ["page_map.csv", "skiprows.csv", "skipfooter.csv"]
    return [
        (name, column)
        for name in sorted(names)
        for column in sorted(set(view.header(name)))
        if view.header(name).count(column) > 1
    ]


def _page_files_with_different_pages(view: MetadataView) -> list:
    """Find the pages that some of the page map, skiprows and skipfooter lack."""
    page_sets = [set(df.columns) for df in view.page_files.values()]
    return sorted(set.union(*page_sets) - set.intersection(*page_sets))


def _pages_not_those_of_the_file_map(view: MetadataView) -> list:
    """Find the pages that are in the page files or the file map, but not both."""
    page_files = set.union(*(set(df.columns) for df in view.page_files.values()))
    return sorted(set(view.file_map.columns) ^ page_files)


def _column_maps_not_those_of_the_file_map(view: MetadataView) -> list:
    """Find the pages that have a column map but no file map column, or reverse."""
    return sorted(set(view.file_map.columns) ^ set(view.column_maps))


def _file_map_partitions_not_in_page_files(view: MetadataView) -> list:
    """Find the partitions of the file map that the page files have no row for."""
    return [
        (name, partition)
        for name, df in view.page_files.items()
        for partition in sorted(set(view.file_map.index) - set(df.index))
    ]


def _working_partitions_not_in_file_map(view: MetadataView) -> list:
    """Find the working partitions of the source that the file map has no row for."""
    return sorted(set(view.working_partitions) - set(view.file_map.index))


def _pages_with_files(view: MetadataView):
    """Generate the pages and partitions of the file map that have a source file."""
    for page in view.file_map.columns:
        for partition in view.file_map.index:
            if view.has_file(page, partition):
                yield page, partition


def _pages_without_files(view: MetadataView):
    """Generate the pages and partitions of the file map that have no source file."""
    for page in view.file_map.columns:
        for partition in view.file_map.index:
            if not view.has_file(page, partition):
                yield page, partition


def _value(df: pd.DataFrame, partition: str, page: str) -> str | None:
    """Read a cell of a file with a column for each page, or None if it has none."""
    if page in df.columns and partition in df.index:
        return str(df.loc[partition, page])
    return None


def _rows_to_skip_not_integers(view: MetadataView) -> list:
    """Find the pages with a file that skip a number of rows that is not an integer."""
    return [
        (name, page, partition, _value(view.page_files[name], partition, page))
        for page, partition in _pages_with_files(view)
        for name in ("skiprows", "skipfooter")
        if not str(_value(view.page_files[name], partition, page)).isdigit()
    ]


def _sheets_missing_for_files(view: MetadataView) -> list:
    """Find the pages with a file that have no sheet in the page map."""
    return [
        (page, partition, _value(view.page_files["page_map"], partition, page))
        for page, partition in _pages_with_files(view)
        if _value(view.page_files["page_map"], partition, page) in (None, ABSENT)
    ]


def _files_without_columns(view: MetadataView) -> list:
    """Find the pages with a file whose column map has no columns in that partition."""
    return [
        (page, partition)
        for page, partition in _pages_with_files(view)
        if page in view.column_maps
        and (
            partition not in view.column_maps[page].index
            or len(_mapped(view.column_maps[page].loc[partition])) == 0
        )
    ]


def _columns_without_files(view: MetadataView) -> list:
    """Find the pages without a file whose column map has columns in that partition."""
    return [
        (page, partition)
        for page, partition in _pages_without_files(view)
        if page in view.column_maps
        and partition in view.column_maps[page].index
        and len(_mapped(view.column_maps[page].loc[partition])) > 0
    ]


def _page_files_without_files(view: MetadataView) -> list:
    """Find the pages without a file that have a value in the page files."""
    return [
        (name, page, partition, _value(df, partition, page))
        for page, partition in _pages_without_files(view)
        for name, df in view.page_files.items()
        if _value(df, partition, page) not in (None, ABSENT)
    ]


@dataclass(frozen=True)
class Check:
    """A way for the metadata files of a dataset to be inconsistent."""

    id: str
    description: str
    find: Callable[[MetadataView], list | None]
    applies_to: Callable[[MetadataView], bool]


def _all(view: MetadataView) -> bool:
    return True


def _excel(view: MetadataView) -> bool:
    return view.is_excel


def _with_file_map(view: MetadataView) -> bool:
    return view.has_file_map


CHECKS = [
    Check(
        "K1",
        "Every partition of a file is a label of the kind that the source has",
        _partition_labels,
        _all,
    ),
    Check(
        "K2",
        "No raw column is mapped to two columns in a row of a column map",
        _raw_columns_mapped_twice,
        _all,
    ),
    Check(
        "K4",
        "No file has two columns with the same name",
        _repeated_column_headers,
        _all,
    ),
    Check(
        "P1",
        "The page map, skiprows and skipfooter files have the same pages",
        _page_files_with_different_pages,
        _excel,
    ),
    Check(
        "P2",
        "The pages of those files are the pages of the file map",
        _pages_not_those_of_the_file_map,
        _excel,
    ),
    Check(
        "P3",
        "The pages with a column map are the pages of the file map",
        _column_maps_not_those_of_the_file_map,
        _with_file_map,
    ),
    Check(
        "P4",
        "Every partition of the file map is described by the page files",
        _file_map_partitions_not_in_page_files,
        _excel,
    ),
    Check(
        "P5",
        "Every working partition of the data source has a row in the file map",
        _working_partitions_not_in_file_map,
        _with_file_map,
    ),
    Check(
        "V1",
        "Where a page has a file, skiprows and skipfooter are integers",
        _rows_to_skip_not_integers,
        _excel,
    ),
    Check(
        "V2",
        "Where a page has a file, the page map has a sheet",
        _sheets_missing_for_files,
        _excel,
    ),
    Check(
        "V3",
        "Where a page has a file, its column map has columns",
        _files_without_columns,
        _with_file_map,
    ),
    Check(
        "V4",
        "Where a page has no file, its column map has no columns",
        _columns_without_files,
        _with_file_map,
    ),
    Check(
        "V5",
        "Where a page has no file, the page files have -1",
        _page_files_without_files,
        _excel,
    ),
]
CHECKS_BY_ID = {check.id: check for check in CHECKS}


class Known(NamedTuple):
    """Violations of a check in a dataset that are known."""

    count: int
    kind: Literal["error", "unfinished"]
    """Whether the metadata is wrong, or correct but not yet completely mapped."""
    note: str


KNOWN_VIOLATIONS: dict[tuple[str, str], Known] = {
    # --- Errors: the metadata is wrong ---------------------------------------
    # --- Unfinished: correct, but not yet completely mapped ------------------
    ("P1", "eia861"): Known(
        19,
        "unfinished",
        "Sheets that the page map locates, but that are not otherwise mapped: the "
        "territories sheets (index 1), the TPO and Decoupled sheets (index 2), and the "
        "sheets of the older dispersed_generation workbooks. They need a file map "
        "entry, skiprows, skipfooter and a column map to be extracted.",
    ),
    ("P2", "eia861"): Known(
        20,
        "unfinished",
        "The 19 sheets that are only in the page map (see P1), and "
        "sales_customer_sited_eia861, which is only in the file map.",
    ),
    ("P3", "eia861"): Known(
        1,
        "unfinished",
        "sales_customer_sited_eia861 has files in the file map for 2012 to 2025 "
        "(Sales_Ult_Cust_CS_*), but no sheet, skiprows, skipfooter or column map.",
    ),
    ("P3", "eia923"): Known(
        3,
        "unfinished",
        "coal_stocks, oil_stocks and petcoke_stocks have files and sheets, but the "
        "extractor lists them as pages that the metadata does not support.",
    ),
    ("P5", "phmsagas"): Known(
        16,
        "unfinished",
        "The working years 1971 to 1979, 1981, 1983 and 1985 to 1989 have no row in "
        "the file map. The file annual_gas_distribution_1970_1989.xls has all of "
        "them, and the file map lists it for only 1970, 1980, 1982 and 1984. It is "
        "not clear whether the rest are meant to be listed.",
    ),
    ("V1", "eia861"): Known(
        28,
        "unfinished",
        "sales_customer_sited_eia861 has files from 2012, but no skiprows or "
        "skipfooter. See P3.",
    ),
    ("V2", "eia861"): Known(
        14,
        "unfinished",
        "sales_customer_sited_eia861 has files from 2012, but no sheet in the page "
        "map. See P3.",
    ),
    ("V3", "eia861"): Known(
        1,
        "unfinished",
        "demand_side_management_eia861 has a file for 1999, but its layout, with 62 "
        "columns instead of the 50 of 2000, has not been mapped. 1999 is not a working "
        "partition of the data source.",
    ),
}
"""Violations that are known: how many, whether they are wrong, and what they are.

When one is fixed, remove it. When there are more, or fewer, the test says so.
Once the errors are fixed, what is left is the list of what has yet to be mapped."""


@cache
def _packaged_view(name: str) -> MetadataView:
    return MetadataView.of_packaged_dataset(name)


PACKAGED_DATASETS = sorted(
    path.parent.name for path in PUDL_PACKAGE_DATA_PATH.glob("*/column_maps")
)


def _applicable_pairs() -> list[tuple[str, str]]:
    """Pair the id of each check with the datasets that it applies to."""
    return [
        (check.id, name)
        for name in PACKAGED_DATASETS
        for check in CHECKS
        if check.applies_to(_packaged_view(name))
    ]


@pytest.mark.parametrize(
    ("check_id", "dataset"),
    [pytest.param(*pair, id=f"{pair[0]}-{pair[1]}") for pair in _applicable_pairs()],
)
def test_packaged_metadata_is_consistent(check_id: str, dataset: str):
    """The packaged metadata has no violations, except the known ones."""
    check = CHECKS_BY_ID[check_id]
    violations = check.find(_packaged_view(dataset))
    assert violations is not None
    if (check_id, dataset) in KNOWN_VIOLATIONS:
        known = KNOWN_VIOLATIONS[(check_id, dataset)]
        assert len(violations) == known.count, (
            f"{check_id} ({check.description}) has {len(violations)} violations in "
            f"{dataset} when {known.count} are known ({known.kind}: {known.note}). If "
            f"they have been fixed, update or remove the known violations. They are: "
            f"{violations}"
        )
    else:
        assert not violations, (
            f"{check_id}: {check.description}. {len(violations)} violations in "
            f"{dataset}, such as {violations[:5]}"
        )


def test_known_violations_are_of_checks_that_apply():
    """A known violation of a check that is not run would never be noticed."""
    assert set(KNOWN_VIOLATIONS) <= set(_applicable_pairs())


# --- Check that the checks can find the violations that they are for -------------


def _write_fake_dataset(path: Path, **changes: str) -> None:
    """Write a dataset that is consistent, except for the files in ``changes``.

    The dataset has pages ``a``, which has a file in every partition, and ``b``, which
    has one from 2011.
    """
    files = {
        "file_map.csv": "page,a,b\n2010,a1.xlsx,-1\n2011,a2.xlsx,b2.xlsx\n"
        "2012,a3.xlsx,b3.xlsx\n",
        "page_map.csv": "page,a,b\n2010,0,-1\n2011,0,1\n",
        "skiprows.csv": "page,a,b\n2010,1,-1\n2011,1,2\n",
        "skipfooter.csv": "page,a,b\n2010,0,-1\n2011,0,0\n",
        "column_maps/a.csv": "page,x,y\n2010,xx,yy\n",
        "column_maps/b.csv": "page,x\n2010,-1\n2011,bx\n",
    }
    files.update(changes)
    for name, text in files.items():
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(text)


FAKE_PARTITIONS = ["2010", "2011", "2012"]

BROKEN_FAKE_DATASETS = {
    "K1": {"column_maps/a.csv": "page,x,y\n2010a,xx,yy\n"},
    "K2": {"column_maps/a.csv": "page,x,y\n2010,xx,xx\n"},
    "K4": {"column_maps/a.csv": "page,x,x\n2010,xx,yy\n"},
    "P1": {"skipfooter.csv": "page,a\n2010,0\n"},
    "P2": {
        "page_map.csv": "page,a,b,c\n2010,0,-1,0\n2011,0,1,0\n",
        "skiprows.csv": "page,a,b,c\n2010,1,-1,0\n2011,1,2,0\n",
        "skipfooter.csv": "page,a,b,c\n2010,0,-1,0\n2011,0,0,0\n",
    },
    "P3": {"column_maps/c.csv": "page,x\n2010,xx\n"},
    "P4": {"page_map.csv": "page,a,b\n2011,0,1\n"},
    "V1": {"skiprows.csv": "page,a,b\n2010,-1,-1\n2011,1,2\n"},
    "V2": {"page_map.csv": "page,a,b\n2010,-1,-1\n2011,0,1\n"},
    "V3": {"column_maps/a.csv": "page,x,y\n2010,-1,-1\n"},
    "V4": {"column_maps/b.csv": "page,x\n2010,bx\n"},
    "V5": {"skiprows.csv": "page,a,b\n2010,1,0\n2011,1,2\n"},
}
"""The files to change in the fake dataset to violate each check."""


def test_fake_dataset_is_consistent(tmp_path: Path):
    """The fake dataset has no violations, so the ones below are from the changes."""
    _write_fake_dataset(tmp_path)
    view = MetadataView.of_fake_dataset(tmp_path, FAKE_PARTITIONS)
    for check in CHECKS:
        assert check.find(view) == [], check.id


@pytest.mark.parametrize("check_id", sorted(BROKEN_FAKE_DATASETS))
def test_check_finds_its_violation(check_id: str, tmp_path: Path):
    """Each check finds the violation that it is for."""
    _write_fake_dataset(tmp_path, **BROKEN_FAKE_DATASETS[check_id])
    view = MetadataView.of_fake_dataset(tmp_path, FAKE_PARTITIONS)
    assert CHECKS_BY_ID[check_id].find(view), check_id


def test_working_partitions_without_a_file_map_row_are_found(tmp_path: Path):
    """P5 finds a working partition that the file map has no row for."""
    _write_fake_dataset(tmp_path)
    view = MetadataView.of_fake_dataset(tmp_path, [*FAKE_PARTITIONS, "2013"])
    assert CHECKS_BY_ID["P5"].find(view) == ["2013"]


def test_every_check_has_a_broken_fake_dataset():
    """A check that is not tried on a violation might never find one."""
    assert {check.id for check in CHECKS} - {"P5"} == set(BROKEN_FAKE_DATASETS)
