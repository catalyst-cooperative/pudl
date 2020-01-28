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
    """
    Read a CEMS CSV file, compressed or not, into a :class:`pandas.DataFrame`.

    Note that some columns are not read. See
    :mod:`pudl.constants.epacems_columns_to_ignore`. Data types for the columns
    are specified in :mod:`pudl.constants.epacems_csv_dtypes` and names of the
    output columns are set by :mod:`pudl.constants.epacems_rename_dict`.

    Args:
        filename (str): The name of the file to be read

    Returns:
        pandas.DataFrame: A DataFrame containing the contents of the
        CSV file.

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
        epacems_years (list): The years of CEMS data to extract, as 4-digit
            integers.
        states (list): The states whose CEMS data we want to extract, indicated
            by 2-letter US state codes.
        data_dir (path-like): Path to the top directory of the PUDL datastore.

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
                filename = datastore.path('epacems',
                                          year=year, month=month, state=state,
                                          data_dir=data_dir)
                dfs.append(read_cems_csv(filename))
            # Return a dictionary where the key identifies this dataset
            # (just like the other extract functions), but unlike the
            # others, this is yielded as a generator (and it's a one-item
            # dictionary).
            yield {
                ("hourly_emissions_epacems_" + str(year) + "_" + state.lower()):
                    pd.concat(dfs, sort=True, copy=False, ignore_index=True)
            }
