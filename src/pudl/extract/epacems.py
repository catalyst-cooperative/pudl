"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import logging
import os
import pandas as pd
from pudl.settings import SETTINGS
import pudl.constants as pc

logger = logging.getLogger(__name__)


def get_epacems_dir(year):
    """
    Data directory search for EPA CEMS hourly

    Args:
        year (int): The year that we're trying to read data for.
    Returns:
        path to appropriate EPA CEMS data directory.
    """
    # These are the only years we've got...
    assert year in range(min(pc.data_years['epacems']),
                         max(pc.data_years['epacems']) + 1)

    return os.path.join(SETTINGS['epacems_data_dir'], 'epacems{}'.format(year))


def get_epacems_file(year, month, state):
    """
    Given a year, month, and state, return the appropriate EPA CEMS zipfile.

    Args:
        year (int): The year that we're trying to read data for.
        month (int): The month we're trying to read data for.
        state (str): The state we're trying to read data for.
    Returns:
        path to EPA CEMS zipfiles for that year, month, and state.
    """
    state = state.lower()
    month = str(month).zfill(2)
    filename = f'epacems{year}{state}{month}.zip'
    full_path = os.path.join(get_epacems_dir(year), filename)
    assert os.path.isfile(full_path), (
        f"ERROR: Failed to find EPA CEMS file for {state}, {year}-{month}.\n"
        + f"Expected it here: {full_path}")
    return full_path


def read_cems_csv(filename):
    """Read one CEMS CSV file
    Note that some columns are not read. See epacems_columns_to_ignores.
    """
    df = pd.read_csv(
        filename,
        index_col=False,
        usecols=lambda col: col not in pc.epacems_columns_to_ignore,
        dtype=pc.epacems_csv_dtypes,
    ).rename(columns=pc.epacems_rename_dict)
    return df


def extract(epacems_years, states):
    """
    Extract the EPA CEMS hourly data.

    This function is the main function of this file. It returns a generator
    for extracted DataFrames.
    """
    # TODO: this is really slow. Can we do some parallel processing?
    for year in epacems_years:
        # The keys of the us_states dictionary are the state abbrevs
        for state in states:
            dfs = []
            for month in range(1, 13):
                filename = get_epacems_file(year, month, state)
                logger.info(
                    f"Performing ETL for EPA CEMS hourly "
                    f"{state}-{year}-{month:02}")
                dfs.append(read_cems_csv(filename))
            # Return a dictionary where the key identifies this dataset
            # (just like the other extract functions), but unlike the
            # others, this is yielded as a generator (and it's a one-item
            # dictionary).
            yield {
                (year, state): pd.concat(dfs, sort=True, copy=False, ignore_index=True)
            }
