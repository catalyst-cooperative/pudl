"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import io
import logging
from zipfile import ZipFile
from pathlib import Path

import pandas as pd

from pudl import constants as pc
from pudl.workspace.datastore import Datastore
from typing import NamedTuple

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
        self.datastore = datastore
        self._open_archives = {}  # type: Dict[Tuple(str, str), zipfile.ZipFile]

    def open_csv(self, partition: EpaCemsPartition, month: int) -> pd.DataFrame:
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


def extract(epacems_years, states, ds: EpaCemsDatastore):
    """
    Coordinate the extraction of EPA CEMS hourly DataFrames.

    Args:
        epacems_years (list): The years of CEMS data to extract, as 4-digit
            integers.
        states (list): The states whose CEMS data we want to extract, indicated
            by 2-letter US state codes.
        ds (:class:`EpaCemsDatastore`): Initialized datastore
        testing (boolean): use Zenodo sandbox if True

    Yields:
        dict: a dictionary with a single EPA CEMS tabular data resource name as
        the key, having the form "hourly_emissions_epacems_YEAR_STATE" where
        YEAR is a 4 digit number and STATE is a lower case 2-letter code for a
        US state. The value is a :class:`pandas.DataFrame` containing all the
        raw EPA CEMS hourly emissions data for the indicated state and year.
    """
    for year in epacems_years:
        # The keys of the us_states dictionary are the state abbrevs
        for state in states:
            partition = EpaCemsPartition(state=state, year=year)
            dfs = []
            logger.info(f"Performing ETL for EPA CEMS hourly {state}-{year}")

            for month in range(1, 13):
                dfs.append(ds.open_csv(partition, month=month))

            # Return a dictionary where the key identifies this dataset
            # (just like the other extract functions), but unlike the
            # others, this is yielded as a generator (and it's a one-item
            # dictionary).
            yield {
                ("hourly_emissions_epacems_" + str(year) + "_" + state.lower()):
                    pd.concat(dfs, sort=True, copy=False, ignore_index=True)
            }
