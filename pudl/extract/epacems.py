"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import os
import pandas as pd
import numpy as np
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


def add_facility_id_unit_id_epa(df):
    """Harmonize columns that are added later

    The load into Postgres checks for consistent column names, and these
    two columns aren't present before August 2008, so add them in.

    Args:
        df (pd.DataFrame): A CEMS dataframe
    Returns:
        The same DataFrame, guaranteed to have facility_id and unit_id_epa cols.
    """
    if "facility_id" not in df.columns:
        df["facility_id"] = np.NaN
    if "unit_id_epa" not in df.columns:
        df["unit_id_epa"] = np.NaN
    return df


def _all_na_or_values(series, values):
    """Test whether every element in the series is either missing or in values

    This is fiddly because isin() changes behavior if the series is totally NaN
    (because of type issues)
    Demo: x = pd.DataFrame({'a': ['x', np.NaN], 'b': [np.NaN, np.NaN]})
    x.isin({'x', np.NaN})

    Args:
        series (pd.Series): A data column
        values (set): A set of values
    Returns:
        True or False
    """
    series_excl_na = series[series.notna()]
    if not len(series_excl_na):
        out = True
    elif series_excl_na.isin(values).all():
        out = True
    else:
        out = False
    return out


def drop_calculated_rates(df):
    """Drop these calculated rates because they don't provide any information.

    If you want these, you can just use a view.

    It's always true that so2_rate_lbs_mmbtu == so2_mass_lbs / heat_content_mmbtu
    and co2_rate_tons_mmbtu == co2_mass_tons / heat_content_mmbtu,
    but the same does not hold for NOx.

    Args:
        df (pd.DataFrame): A CEMS dataframe
    Returns:
        The same DataFrame, without the variables so2_rate_measure_flg,
        so2_rate_lbs_mmbtu, co2_rate_measure_flg, or co2_rate_tons_mmbtu
    """

    assert _all_na_or_values(df["so2_rate_measure_flg"], {"Calculated"})
    assert _all_na_or_values(df["co2_rate_measure_flg"], {"Calculated"})
    del df["so2_rate_measure_flg"], df["so2_rate_lbs_mmbtu"]
    del df["co2_rate_measure_flg"], df["co2_rate_tons_mmbtu"]
    return df


def extract(epacems_years, states, verbose):
    """
    Extract the EPA CEMS hourly data.

    This function is the main function of this file. It returns a generator
    for extracted DataFrames.
    """
    # TODO: this is really slow. Can we do some parallel processing?
    if verbose:
        print("Reading EPA CEMS data...")
    for year in epacems_years:
        if verbose:
            print("    {}...".format(year))
        # The keys of the us_states dictionary are the state abbrevs
        for state in states:
            for month in range(1, 13):
                filename = get_epacems_file(year, month, state)

                if verbose:
                    print(f"        Extracting: {filename}")
                # Return a dictionary where the key identifies this dataset
                # (just like the other extract functions), but unlike the
                # others, this is yielded as a generator (and it's a one-item
                # dictionary).
                # TODO: set types explicitly
                df = (
                    pd.read_csv(filename, low_memory=False)
                    .rename(columns=pc.epacems_rename_dict)
                    .pipe(add_facility_id_unit_id_epa)
                    .pipe(drop_calculated_rates)
                )
                yield {(year, month, state): df}
