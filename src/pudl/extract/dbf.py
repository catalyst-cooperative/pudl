"""Generalized DBF extractor for FERC data."""
from collections import defaultdict
import csv
import importlib
from pathlib import Path
from typing import Iterator

import pandas as pd
from pudl.workspace.datastore import Datastore
from dbfread import DBF, FieldParser

class TableSchema:
    def __init__(self, name: str):
        self.name = name
        self.columns = {}  # maps long names to truncated names

    def add_column(self, long_name: str, short_name: str):
        self.columns[long_name] = short_name

class FercFoxProDatastore:
    """Wrapper to provide standardized access to FoxPro FERC databases."""

    # Name of this dataset, e.g. ferc1
    DATASET = None

    # Name of the dbc file that contains the schema, e.g. F1_PUB.DBC
    DBC_FILENAME = None

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

    def get_schema(self, year: int) -> dict[str, list[str]]:
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
        return self._table_file_map[table_name]
        # TODO: do we need to map KeyErrors to something?

    def get_all_tables(self) -> map[str, str]:
        """Return copy of the dictionary that maps table names to files."""
        return dict(self._table_file_map)

    def get_column_map(self, table_name: str) -> str:
        # TODO: implement this by combinging get_schema with the fields present
        # in the individual DBF tables.
        # It is unclear whether the columns, over the years, can shift/change order
        # or whether the logic implemented as-is is the best approach to this.
        pass

    def load_table_df(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Retrieve DataFrame for a given year and table."""
        yearly_dfs = []
        for yr in years:
            table_filename = self.get_table_file(table_name)
            try:
                fd = self.get_file(yr, table_filename)
            except KeyError:
                # TODO: log warning about missing file
                continue
            yearly_dfs.append(
                pd.DataFrame(
                    iter(
                        DBF(
                            table_filename,
                            encoding="latin1",
                            parserclass=FERC1FieldParser,
                            ignore_missing_memofile=True,
                            filedata=fd,
                        )
                    ) 
                )
            )
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


        # Iterate over all DBF files in the directory



class Ferc1FoxProDatastore(FercFoxProDatastore):
    DBC_FILENAME = "F1_PUB.DBC"

    def __init__(self, datastore: Datastore):
        super().__init__(self, datastore, "F1_PUB.DBC")