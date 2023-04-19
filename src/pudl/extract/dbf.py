"""Generalized DBF extractor for FERC data."""
from ast import Tuple
from collections import defaultdict
import csv
from functools import lru_cache
import importlib
from pathlib import Path
from pickle import MEMOIZE
from typing import Any, Dict, Iterator, List, Optional

import pudl
import pandas as pd
from pudl.workspace.datastore import Datastore
from dbfread import DBF, FieldParser
import sqlalchemy as sa
from pudl.metadata.classes import DataSource
from typing import Protocol


class TableSchema:
    """Simple data-wrapper for the fox-pro table schema."""
    def __init__(self, table_name: str):
        self.name = table_name
        self._columns = []
        self._column_types = {}
        self._short_name_map = {} # short_name_map[short_name] -> long_name

    def add_column(self, col_name: str, col_type: sa.types.TypeEngine, short_name: Optional[str] = None):
        assert col_name not in self._columns
        self._columns.append(col_name)
        self._column_types[col_name] = col_type
        if short_name is not None:
            self._short_name_map[short_name] = col_name

    def get_columns(self) -> Iterator[tuple[str, sa.types.TypeEngine]]:
        for col_name in self._columns:
            yield (col_name, self._column_types[col_name])

    def get_column_rename_map(self) -> Dict[str, str]:
        return dict(self._short_name_map)

    def to_sqlite_table(self, sqlite_meta: sa.MetaData) -> sa.Table:
        table = sa.Table(self.name,  sqlite_meta)
        for col_name, col_type in self.get_columns():
            table.append_column(sa.Column(col_name, col_type))
        return table


class AbstractFoxProDatastore(Protocol):
    """This is the interface definition for dealing with fox-pro datastores."""
    def get_dataset(self) -> str:
        """Returns name of the dataset that this datastore provides access to."""
        ...

    def get_table_names(self) -> List[str]:
        """Returns list of all available table names."""
        ...

    def get_table_schema(self, table_name: str, year: int) -> TableSchema:
        """Returns schema for a given table and a given year."""
        ...

    def load_table_dfs(self, table_name: str, years: List[int]) -> Optional[pd.DataFrame]:
        """Returns dataframe that contains data for a given table across given years."""
        ...


class FercFieldParser(FieldParser):
    """A custom DBF parser to deal with bad FERC data types."""
    def parseN(self, field, data: bytes) -> int | float | None:  # noqa: N802
        """Augments the Numeric DBF parser to account for bad FERC data.

        There are a small number of bad entries in the backlog of FERC Form 1
        data. They take the form of leading/trailing zeroes or null characters
        in supposedly numeric fields, and occasionally a naked '.'

        Accordingly, this custom parser strips leading and trailing zeros and
        null characters, and replaces a bare '.' character with zero, allowing
        all these fields to be cast to numeric values.

        Args:
            field: The DBF field being parsed.
            data: Binary data (bytes) read from the DBF file.
        """  # noqa: D417
        # Strip whitespace, null characters, and zeroes
        data = data.strip().strip(b"*\x00").lstrip(b"0")
        # Replace bare periods (which are non-numeric) with zero.
        if data == b".":
            data = b"0"
        return super().parseN(field, data)


DBF_TYPES = {
    "C": sa.String,
    "D": sa.Date,
    "F": sa.Float,
    "I": sa.Integer,
    "L": sa.Boolean,
    "M": sa.Text,  # 10 digit .DBT block number, stored as a string...
    "N": sa.Float,
    "T": sa.DateTime,
    "0": sa.Integer,  # based on dbf2sqlite mapping
    "B": "XXX",  # .DBT block number, binary string
    "@": "XXX",  # Timestamp... Date = Julian Day, Time is in milliseconds?
    "+": "XXX",  # Autoincrement (e.g. for IDs)
    "O": "XXX",  # Double, 8 bytes
    "G": "XXX",  # OLE 10 digit/byte number of a .DBT block, stored as string
}
"""dict: A mapping of DBF field types to SQLAlchemy Column types.

This dictionary maps the strings which are used to denote field types in the DBF objects
to the corresponding generic SQLAlchemy Column types: These definitions come from a
combination of the dbfread example program dbf2sqlite and this DBF file format
documentation page: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm

Unmapped types left as 'XXX' which should result in an error if encountered.
"""

# TODO(rousik): circular dependency between FercFoxProDatastore and FoxProTable,
# could be addressed through interfaces/ABCs


# TODO(rousik): instead of using class-level constants, we could pass the params in the constructor, which should 
# allow us to instantiate these dataset-specific datastores in the extractor code.
# That may make the manipulations little easier.
class FercFoxProDatastore:
    """Wrapper to provide standardized access to FoxPro FERC databases."""
    def __init__(self, datastore: Datastore, dataset:str, dbc_filename:str, field_parser:FieldParser=FercFieldParser):
        """
        Args:
            datastore: provides access to raw files on disk
        """
        self._cache = {}
        self.datastore = datastore
        self.dataset = dataset
        self.dbc_filename = dbc_filename
        self.field_parser = field_parser

        # file_map contains root-path where all DBC and DBF files are stored.
        # This can vary over the years.
        self._root_path = {}
        for row in self._open_csv_resource("file_map.csv"):
            self._root_path[int(row["year"])] = Path(row["path"])

        # table_file_map holds mapping between tables and their corresponding
        # DBF files.
        self._table_file_map = {}
        for row in self._open_csv_resource("table_file_map.csv"):
            self._table_file_map[row["table"]] = row["filename"]

    def get_dataset(self):
        return self.dataset

    def _open_csv_resource(self, base_filename:str) -> csv.DictReader:
        """Opens the given resource file as csv.DictReader."""
        pkg_path = f"pudl.package_data.{self.dataset}"
        return csv.DictReader(importlib.resources.open_text(pkg_path, base_filename))

    def get_dir(self, year: int) -> Path:
        try:
            return self._root_path[year]
        except KeyError:
            raise ValueError(f"No {self.dataset} data for year {year}")

    def get_file(self, year: int, filename: str) -> Any:
        if year not in self._cache:
            self._cache[year] = self.datastore.get_zipfile_resource(
                self.dataset, year=year, data_format="dbf"
            )
        archive = self._cache[year]
        try:
            return archive.open((self.get_dir(year) / filename).as_posix())
        except KeyError:
            raise KeyError(f"{filename} not available for year {year} in {self.dataset}.")
        
    def get_table_dbf(self, table_name: str, year: int) -> DBF:
        fname = self._table_file_map[table_name]
        fd = self.get_file(year, fname)
        return DBF(
            fname,
            encoding="latin1",
            parserclass=self.field_parser,
            ignore_missing_memofile=True,
            filedata=fd,
        )
    
    def get_table_names(self) -> List[str]:
        return list(self._table_file_map)
    
    @lru_cache
    def get_db_schema(self, year: int) -> Dict[str, List[str]]:
        """Returns dict with table names as keys, and list of column names as values."""
        dbf = DBF(
            "", 
            ignore_missing_memofile=True,
            filedata=self.get_file(year, self.dbc_filename)
        )
        table_names : Dict[Any, str] = {}
        table_columns = defaultdict(list)
        for row in dbf:
            obj_id = row.get("OBJECTID")
            obj_name = row.get("OBJECTNAME")
            obj_type = row.get("OBJECTTYPE", None)
            if obj_type == "Table":
                table_names[obj_id] = obj_name
            elif obj_type == "Field":
                parent_id = row.get("PARENTID")
                table_columns[parent_id].append(obj_name)
        # Remap table ids to table names.
        return {table_names[tid]: cols for tid, cols in table_columns.items()} 

    # TODO(rousik): table column map should be remapping short names (in dbf) to long names found in db schema (DBC).
    # This is kind of annoying transformation but we can't do without it.

    @lru_cache
    def get_table_schema(self, table_name: str, year: int) -> TableSchema:
        table_columns = self.get_db_schema(year)[table_name]
        dbf = self.get_table_dbf(table_name, year)
        dbf_fields = [field for field in dbf.fields if field.name != "_NullFlags"]
        if len(table_columns) != len(table_columns):
            return ValueError(                
                f"Number of DBF fields in {table_name} does not match what was "
                f"found in the DBC index file for {year}."
            )
        schema = TableSchema(table_name)
        for long_name, dbf_col in zip(table_columns, dbf_fields):
            if long_name[:8] != dbf_col.name.lower()[:8]:
                raise ValueError(
                    f"DBF field name mismatch: {dbf_col.name} != {long_name}"
                )
            col_type = DBF_TYPES[dbf_col.type]
            if col_type == sa.String:
                col_type = sa.String(length=dbf_col.length)
            schema.add_column(long_name, col_type, short_name=dbf_col.name)
        return schema  

    def _load_single_year(self, table_name: str, year: int) -> pd.DataFrame:
        sch = self.get_table_schema(table_name, year)
        df = pd.DataFrame(iter(self.get_table_dbf(table_name, year)))
        df = df.drop("_NullFlags", axis=1, errors="ignore").rename(
            sch.get_column_rename_map(),
            axis=1
        )
        return df

    def load_table_dfs(self, table_name: str, years: List[int]) -> Optional[pd.DataFrame]:
        yearly_dfs = []
        for yr in years:
            try:
                yearly_dfs.append(self._load_single_year(table_name, yr))
            except KeyError:
                continue
        if yearly_dfs:
            return pd.concat(yearly_dfs, sort=True)
        return None
