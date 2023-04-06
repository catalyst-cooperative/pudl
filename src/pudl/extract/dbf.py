"""Generalized DBF extractor for FERC data."""
from ast import Tuple
from collections import defaultdict
import csv
import importlib
from pathlib import Path
from typing import Iterator, List

import pandas as pd
from pudl.workspace.datastore import Datastore
from dbfread import DBF, FieldParser
import sqlalchemy as sa
from pudl.metadata.classes import DataSource
from typing import Protocol


class AbstractFoxProDatastore(Protocol):
    def get_table_dbf(self, table_name: str, year: int) -> DBF:
        ...

    def get_dataset(self) -> str:
        ...

    def get_all_schemas(self, year: int) -> dict[str, list[str]]:
        ...

    def get_table_schema(self, table: str, year: int) -> list[str]:
        ...

    def get_table_column_map(self, table: str, year: int) -> dict[str, str]:
        """Returns mapping from short-column names to long column names."""

# class TableSchema:
#     def __init__(self, name: str):
#         self.name = name
#         self.columns = {}  # maps long names to truncated names

#     def add_column(self, long_name: str, short_name: str):
#         self.columns[long_name] = short_name


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
class FoxProTable:
    def __init__(
            self,
            datastore: AbstractFoxProDatastore,
            table_name: str):
        self.table_name = table_name
        self.datastore = datastore

    # This is an abstraction for dealing with fox-pro tables. Each table
    # is derived from FercFoxProDatastore, and has name and methods for:
    # 1. loading dataframes for a year (or collection of years)
    # 2. exporting table to Sqlite alchemy tables
    def to_sqlite_schema(self, refyear: int | None = None) -> sa.Table:
        if refyear is None:
            refyear = max(
                DataSource.from_id(self.datstore.get_dataset()).working_partitions["years"]
            )
        dbf = self.datastore.get_table_dbf(self.table_name, refyear)
        
        new_table = sa.Table(self.table_name)
        col_map = self.datastore.get_table_column_map(self.table_name, refyear)

        for field in dbf.fields:
            if field.name == "_NullFlags":
                continue
            # TODO(rousik): remap column to long-form name
            col_name = col_map[field.name]
            col_type = DBF_TYPES[field.type]
            new_table.append_column(sa.Column(col_name, col_type))
        # TODO: respondent_id PK/FK constraints are dataset specific business logic
        return new_table       
    
    def load_single_df(self, year: int) -> pd.DataFrame:
        """Loads single year worth of data into pd.DataFrame."""
        df = pd.DataFrame(iter(self.datastore.get_table_dbf(self.table_name, year)))
        df.drop("_NullFlags", axis=1, errors="ignore").rename(
            self.datastore.get_table_column_map(self.table_name, year),
            axis=1,
        )
        return df
    
    def load_df(self, years: List[int]):
        """Loads table into single pd.DataFrame containing data from specified years."""
        yearly_dfs = []
        for yr in years:
            try:
                yearly_dfs.append(self.load_single_df(yr))
            except KeyError:
                pass
        if yearly_dfs:
            return pd.concat(yearly_dfs, sort=True)
        return None

class FercFoxProDatastore:
    """Wrapper to provide standardized access to FoxPro FERC databases."""

    # Name of this dataset, e.g. ferc1
    DATASET = None

    # Name of the dbc file that contains the schema, e.g. F1_PUB.DBC
    DBC_FILENAME = None

    FIELD_PARSER = FercFieldParser

    def __init__(self, datastore: Datastore):
        """
        Args:
            datastore: provides access to raw files on disk
        """
        self.datastore = datastore
        assert(self.DATASET is not None)
        assert(self.DBF_FILENAME is not None)

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

    def _open_csv_resource(self, base_filename:str) -> csv.DictReader:
        """Opens the given resource file as csv.DictReader."""
        pkg_path = "pudl.package_data.{self.DATASET}"
        return csv.DictReader(importlib.resources.open_text(pkg_path, base_filename))

    def get_dir(self, year: int) -> Path:
        try:
            return self._root_path[year]
        except KeyError:
            raise ValueError(f"No {self.DATASET} data for year {year}")

    def get_file(self, year:int, filename: str):
        if year not in self._cache:
            self._cache[year] = self.datastore.get_zipfile_resource(
                self.DATASET, year=year, data_format="dbf"
            )
        archive = self._cache[year]
        try:
            return archive.open((self.get_dir(year) / filename).as_posix())
        except KeyError:
            raise KeyError(f"{filename} not available for year {year} in {self.DATASET}.")
        
    def get_table_dbf(self, table_name: str, year: int):
        fname = self.get_table_file(table_name)
        fd = self.get_file(year, fname)
        return DBF(
            fname,
            encoding="latin1",
            parserclass=self.FIELD_PARSER,
            ignore_missing_memofile=True,
            filedata=fd,
        )

    @Memoize
    def get_schema(self, year: int) -> dict[str, list[str]]:
        """Returns dict mapping from table name to list of columns/fields."""
        dbf = DBF(
            "", 
            ignore_missing_memofile=True,
            filedata=self.get_file(year, self.DBC_FILENAME)
        )
        table_names = {}
        table_cols = defaultdict(list)
        for row in dbf:
            if row.get("OBJECTTYPE", None) == "Table":
                table_names[row["OBJECTID"]] = row["OBJECTNAME"]
            elif row.get("OBJECTTYPE", None) == "Field":
                table_cols[row["PARENTID"]].append(row["OBJECTNAME"])

        return {
            tname: table_cols[tid]
            for tid, tname in table_names.items()
        }

    def get_table_file(self, table_name: str) -> str:
        """Returns the base filename for a given table."""
        return self._table_file_map[table_name]
        # TODO: do we need to map KeyErrors to something?

    def get_table_names(self): -> list[str]:
        return list(self._table_file_map)

    def get_all_tables(self, year: int) -> Iterator[Tuple[str, DBF]]:
        """Iterate over all tables available for a given year."""
        for table_name in self._table_file_map:
            try:
                yield (table_name, self.get_table_dbf(table_name, year))
            except KeyError:
                pass        
            
    def get_column_map(self, table_name: str, year: int) -> dict[str, dict[str, str]]:
        """Returns mapping from short column names to long column names for all tables.
        
        The result is two-tier dictionary:
           result[table_name][short_col_name] -> long_col_name
        """
        # TODO: ideally, we would verify that column map remains stable YoY. This is
        # assumed, but probably not necessarily guaranteed. Order also matters.
        column_map = defaultdict(dict)
        schema = self.get_schema(year)
        for table_name, table_dbf in self.get_all_tables(year):
            table_schema = schema[table_name]
            fields = [col for col in table_dbf.field_names if col != "_NullFlags"]
            if len(fields) != len(table_schema):
                return ValueError(                
                    f"Number of DBF fields in {table_name} does not match what was "
                    f"found in the DBC index file for {year}."
                )
            column_map[table_name] = dict(zip(fields, table_schema))
        
        # Validate that column prefixes match!
        for tname, col_map in column_map:
            for sn, ln in col_map.items():
                if ln[:8] != sn.lower()[:8]:
                    raise ValueError(
                        f"DBF field name mismatch '{ln}' != '{sn}' for table {tname}"
                    )              
        return column_map
    
    def load_table_df(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Retrieve DataFrame for a given table and given list of years."""
        yearly_dfs = []
        for yr in years:
            try:
                yearly_dfs.append(
                    pd.DataFrame(iter(self.get_table_dbf(table_name)))
                )
            except KeyError:
                # TODO: should we emit warning here (?)
                pass
        if yearly_dfs: 
            return (
                pd.concat(yearly_dfs, sort=True)
                .drop("_NullFlags", axis=1, errors="ignore")
                .rename(self.get_column_map(table_name), axis=1)
            )    

    # TODO: replace pd.DataFrame with thin wrapper that will also hold metadata such as 
    # year and table_name.
    # Note that in the original approach, the files are processed in slightly different
    # order. Rather than loading everything for a single year, all years for a single 
    # table are concatenanted into data-frame that is fed into the sqlite.
    # We should figure out if this is necessary, or whether we could feed data into sqlite
    # in an incremental fashion (in one year increments).
    def load_tables(self, year: int) -> Iterator[pd.DataFrame]:
        """Loads all tables available for a given year."""
        table_schema = self.get_schema(year)
        for dbf_path in self.get_dir(year).glob("*.DBF"):
            dbf = DBF(
                dbf_path.name,
                encoding="latin1",
                # TODO: add parserclass=FERC1FieldParser generalization here 
                ignore_missing_memofile=True,
                filedata=self.get_file(year, dbf_path.name)
            )
            # TODO: extract type information