"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import os
import pandas as pd
from pudl.settings import SETTINGS
import pudl.constants as pc


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
        f"ERROR: Failed to find EPA CEMS file for {state}, {year}-{month}.\n" +
        f"Expected it here: {full_path}")
    return full_path


def extract(epacems_years, states, verbose=True):
    """
    Extract the EPA CEMS hourly data.

    This function is the main function of this file. It returns a generator
    for extracted DataFrames.
    """
    # TODO: this is really slow. Can we do some parallel processing?
    if verbose:
        print("Extracting EPA CEMS data...", flush=True)
    for year in epacems_years:
        if verbose:
            print(f"    {year}:", flush=True)
        # The keys of the us_states dictionary are the state abbrevs
        for state in states:
            if verbose:
                print(f"        {state}:", end=" ", flush=True)
            dfs = []
            for month in range(1, 13):
                filename = get_epacems_file(year, month, state)

                if verbose:
                    print(f"{month}", end=" ", flush=True)
            # Return a dictionary where the key identifies this dataset
            # (just like the other extract functions), but unlike the
            # others, this is yielded as a generator (and it's a one-item
            # dictionary).
            # TODO: set types explicitly
                df = (
                    pd.read_csv(filename, low_memory=False)
                    .rename(columns=pc.epacems_rename_dict)
                )
                dfs.append(df)
            print(" ", flush=True)  # newline...
            yield {(year, state): pd.concat(dfs, sort=True)}
