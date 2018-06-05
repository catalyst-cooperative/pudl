"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import glob
import os
import pandas as pd
from pudl import settings
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

    return os.path.join(settings.EPACEMS_DATA_DIR, 'epacems{}'.format(year))


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


def fix_names(df):
    rename_dict = {
        'STATE': 'state',
        'FACILITY_NAME': 'facility_name',
        'ORISPL_CODE': 'orispl_code',
        'UNITID': 'unitid',
        'OP_DATE': 'op_date',
        'OP_HOUR': 'op_hour',
        'OP_TIME': 'op_time',
        'GLOAD (MW)': 'gload',
        'GLOAD': 'gload',
        'SLOAD (1000 lbs)': 'sload',
        'SLOAD': 'sload',
        'SO2_MASS (lbs)': 'so2_mass',
        'SO2_MASS': 'so2_mass',
        'SO2_MASS_MEASURE_FLG': 'so2_mass_measure_flg',
        'SO2_RATE (lbs/mmBtu)': 'so2_rate',
        'SO2_RATE': 'so2_rate',
        'SO2_RATE_MEASURE_FLG': 'so2_rate_measure_flg',
        'NOX_RATE (lbs/mmBtu)': 'nox_rate',
        'NOX_RATE': 'nox_rate',
        'NOX_RATE_MEASURE_FLG': 'nox_rate_measure_flg',
        'NOX_MASS (lbs)': 'nox_mass',
        'NOX_MASS': 'nox_mass',
        'NOX_MASS_MEASURE_FLG': 'nox_mass_measure_flg',
        'CO2_MASS (tons)': 'co2_mass',
        'CO2_MASS': 'co2_mass',
        'CO2_MASS_MEASURE_FLG': 'co2_mass_measure_flg',
        'CO2_RATE (tons/mmBtu)': 'co2_rate',
        'CO2_RATE': 'co2_rate',
        'CO2_RATE_MEASURE_FLG': 'co2_rate_measure_flg',
        'HEAT_INPUT (mmBtu)': 'heat_input',
        'HEAT_INPUT': 'heat_input',
        }
    return df.rename(columns=rename_dict)

def extract(epacems_years, verbose):
    """
    Extract the EPA CEMS hourly data.

    This function is the main function of this file. It returns a generator
    for extracted DataFrames.
    """

    print("Reading EPA CEMS {} data...")
    for year in epacems_years:
        if verbose:
            print("    {}...".format(year))
        # The keys of the us_states dictionary are the state abbrevs
        for state in pc.cems_states.keys():
            for month in range(1, 13):
                filename = get_epacems_file(year, month, state)
                if verbose:
                    print(f"Extracting: {filename}")
                # Return a dictionary where the key identifies this dataset
                # (just like the other extract functions), but unlike the
                # others, this is yielded as a generator (and it's a one-item
                # dictionary).
                try:
                    df = pd.read_csv(filename).pipe(fix_names)
                # TODO: remove this try/except once all files can be read.
                except:
                    print(f"ERROR: Failed to extract EPA CEMs data for {state}, {year}-{month}.")
                    df = None
                yield {(year, month, state): df}
