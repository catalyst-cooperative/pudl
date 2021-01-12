"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import logging
from pathlib import Path
from typing import Dict, NamedTuple, Tuple
from zipfile import ZipFile

import pandas as pd
from prefect import task

from pudl import constants as pc
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)


class EpaCemsPartition(NamedTuple):
    """Represents EpaCems partition identifying unique resource file."""

    year: str
    state: str

    def get_key(self):
        """Returns hashable key for use with EpaCemsDatastore."""
        return (self.year, self.state.lower())

    def get_filters(self):
        """Returns filters for retrieving given partition resource from Datastore."""
        return dict(year=self.year, state=self.state.lower())

    def get_monthly_file(self, month: int) -> Path:
        """Returns path to the file that contains data for a given month.

        This file is stored inside the zip file resource for this partition.
        """
        return Path(f"{self.year}{self.state.lower()}{month:02}")


class EpaCemsDatastore:
    """Helper class to extract EpaCems resources from datastore.

    EpaCems resources are identified by a year and a state. Each of these zip files
    contain monthly zip files that in turn contain csv files. This class implements
    method open_csv that reads the monthly csv and returrns it as a pandas.DataFrame.

    Because multiple months of data are usually read in sequence, this class also
    implements caching of the open archives. These persist available during the lifetime
    of this instance.
    """

    def __init__(self, datastore: Datastore):
        """Create new EpaCemsDatastore wrapper."""
        self.datastore = datastore
        self._open_archives = {}  # type: Dict[Tuple(str, str), ZipFile]

    def open_csv(self, partition: EpaCemsPartition, month: int) -> pd.DataFrame:
        """Returns dataframe containing data for a given EpaCems partition and month."""
        archive_key = partition.get_key()
        if archive_key not in self._open_archives:
            self._open_archives[archive_key] = self.datastore.get_zipfile_resource(
                "epacems", **partition.get_filters())
        archive = self._open_archives[archive_key]
        # Access the csv file within the embedded zip file
        mf = partition.get_monthly_file(month)
        with archive.open(str(mf.with_suffix(".zip")), "r") as mzip:
            with ZipFile(mzip, "r").open(str(mf.with_suffix(".csv")), "r") as csv_file:
                return self.csv_to_dataframe(csv_file)

    def csv_to_dataframe(self, csv_file) -> pd.DataFrame:
        """
        Convert a CEMS csv file into a :class:`pandas.DataFrame`.

        Note that some columns are not read. See
        :mod:`pudl.constants.epacems_columns_to_ignore`. Data types for the columns
        are specified in :mod:`pudl.constants.epacems_csv_dtypes` and names of the
        output columns are set by :mod:`pudl.constants.epacems_rename_dict`.

        Args:
            csv (file-like object): data to be read

        Returns:
            pandas.DataFrame: A DataFrame containing the contents of the
            CSV file.

        """
        return pd.read_csv(
            csv_file,
            index_col=False,
            usecols=lambda col: col not in pc.epacems_columns_to_ignore,
            dtype=pc.epacems_csv_dtypes,
        ).rename(columns=pc.epacems_rename_dict)


@task
def extract_fragment(partition: EpaCemsPartition):
    """Extracts epacems dataframe for given year and state.

    Args:
        partition (EpacemsPartition): defines which partition (year, state)
        should be loaded.

    Returns:
        {fragment_name: pandas.DataFrame}
    """
    dfs = []
    ds = EpaCemsDatastore(Datastore.from_prefect_context())
    for month in range(1, 13):
        dfs.append(ds.open_csv(partition, month=month))

    final_df = pd.concat(dfs, sort=True, copy=False, ignore_index=True)
    key = f'hourly_emissions_epacems_{partition.year}_{partition.state.lower()}'
    return {key: final_df}
