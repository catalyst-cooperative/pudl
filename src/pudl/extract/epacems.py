"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import io
import logging
from collections import namedtuple
from zipfile import ZipFile

import pandas as pd
from prefect import task
from prefect.engine.results import LocalResult

from pudl import constants as pc
from pudl.workspace import datastore as datastore

logger = logging.getLogger(__name__)


class EpaCemsDatastore(datastore.Datastore):
    """Provide a thin datastore wrapper to ease access to epacems."""

    def open_csv(self, state, year, month):
        """
        Open the csv file for the given state / year / month.

        Args:
            state (str): 2 character state abbreviation such as "ca" or "ny"
            year (int): integer of the desired year
            month (int): integer of the desired month

        Returns:
            CSV file stream, or raises an error on invalid input
        """
        state = state.lower()

        monthly_zip = f"{year}{state}{month:02}.zip"
        monthly_csv = f"{year}{state}{month:02}.csv"

        try:
            resource = next(self.get_resources(
                "epacems", **{"year": year, "state": state}))
        except StopIteration:
            raise ValueError(f"No epacems data for {state} in {year}")

        logger.debug(f"epacems resource found at {resource['path']}")

        with ZipFile(resource["path"], "r").open(monthly_zip, "r") as mz:
            with ZipFile(mz, "r").open(monthly_csv, "r") as csv:
                logger.debug(f"epacepms csv {monthly_csv} opened")
                csv = io.BytesIO(csv.read())

        return csv


def csv_to_dataframe(csv):
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
    df = pd.read_csv(
        csv,
        index_col=False,
        usecols=lambda col: col not in pc.epacems_columns_to_ignore,
        dtype=pc.epacems_csv_dtypes,
    ).rename(columns=pc.epacems_rename_dict)
    return df


EpacemsPartition = namedtuple('EpacemsPartition', 'year state')


@task(result=LocalResult(), target="epacems-extract-{year}-{state}")  # noqa: FS003
def extract_fragment(datastore, partition):
    """Extracts epacems dataframe for given year and state.

    Args:
        partition (EpacemsPartition): defines which partition (year, state)
        should be loaded.

    Returns:
        {fragment_name: pandas.DataFrame}
    """
    dfs = []
    for month in range(1, 13):
        csv = datastore.open_csv(partition.state, partition.year, month)
        dfs.append(csv_to_dataframe(csv))

    final_df = pd.concat(dfs, sort=True, copy=False, ignore_index=True)
    key = f'hourly_emissions_epacems_{partition.year}_{partition.state.lower()}'
    return {key: final_df}
