"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import io
import logging
from zipfile import ZipFile

import pandas as pd

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


def extract(epacems_years, states, ds):
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
            dfs = []
            logger.info(f"Performing ETL for EPA CEMS hourly {state}-{year}")

            for month in range(1, 13):
                csv = ds.open_csv(state, year, month)
                dfs.append(csv_to_dataframe(csv))

            # Return a dictionary where the key identifies this dataset
            # (just like the other extract functions), but unlike the
            # others, this is yielded as a generator (and it's a one-item
            # dictionary).
            yield {
                ("hourly_emissions_epacems_" + str(year) + "_" + state.lower()):
                    pd.concat(dfs, sort=True, copy=False, ignore_index=True)
            }
