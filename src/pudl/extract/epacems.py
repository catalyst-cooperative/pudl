"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import logging

import pandas as pd

import pudl.constants as pc
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)


def read_cems_csv(filename):
    """Reads one CEMS CSV file.

    Note that some columns are not read. See epacems_columns_to_ignores.

    Args:
        filename (str): The name of the file to be read

    Returns:
        pandas.DataFrame: A DataFrame containing the contents of the CSV file.

    """
    df = pd.read_csv(
        filename,
        index_col=False,
        usecols=lambda col: col not in pc.epacems_columns_to_ignore,
        dtype=pc.epacems_csv_dtypes,
    ).rename(columns=pc.epacems_rename_dict)
    return df


def extract(epacems_years, states, data_dir):
    """
    Coordinate the extraction of EPA CEMS hourly DataFrames.

    Args:
        epacems_years (list): list of years from which we are trying to read
            CEMS data
        states (list): list of states from which we are trying to read CEMS
            data
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Yields:
        dict: a dictionary of States (keys) and DataFrames of CEMS data (values)

    Todo:
        This is really slow. Can we do some parallel processing?

    """
    for year in epacems_years:
        # The keys of the us_states dictionary are the state abbrevs
        for state in states:
            dfs = []
            for month in range(1, 13):
                filename = datastore.path('epacems',
                                          year=year, month=month, state=state,
                                          data_dir=data_dir)
                logger.info(
                    f"Performing ETL for EPA CEMS hourly "
                    f"{state}-{year}-{month:02}")
                dfs.append(read_cems_csv(filename))
            # Return a dictionary where the key identifies this dataset
            # (just like the other extract functions), but unlike the
            # others, this is yielded as a generator (and it's a one-item
            # dictionary).
            yield {
                (year, state):
                    pd.concat(dfs, sort=True, copy=False, ignore_index=True)
            }
