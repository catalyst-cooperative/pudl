"""
Shared routines used for transforming data before loading into the PUDL DB.

This module contains functions that are more generally useful in the cleaning
and transformation of data that is being loaded into the PUDL database, rather
than being specific to a particular data source.
"""

import pandas as pd
import numpy as np


def cleanstrings(field, stringmap, unmapped=None):
    """
    Consolidate freeform strings in dataframe column to canonical codes.

    This function maps many different strings meant to represent the same value
    or category to a single value. In addition, white space is stripped and
    values are translated to lower case.  Optionally replace all unmapped
    values in the original field with a value (like NaN) to indicate data which
    is uncategorized or confusing.

    Args:
        field (pandas.DataFrame column): A pandas DataFrame column
            (e.g. f1_fuel["FUEL"]) whose strings will be matched, where
            possible, to categorical values from the stringmap dictionary.

        stringmap (dict): A dictionary whose keys are the strings we're mapping
            to, and whose values are the strings that get mapped.

        unmapped (str, None, NaN) is the value which strings not found in the
            stringmap dictionary should be replaced by.

    Returns:
        pandas.Series: The function returns a new pandas series/column that can
            be used to set the values of the original data.
    """
    from numpy import setdiff1d

    # Simplify the strings we're working with, to reduce the number of strings
    # we need to enumerate in the maps

    # Transform the strings to lower case, strip leading/trailing whitespace
    field = field.str.lower().str.strip()
    # remove duplicate internal whitespace
    field = field.replace('[\s+]', ' ', regex=True)

    for k in stringmap.keys():
        field = field.replace(stringmap[k], k)

    if unmapped is not None:
        badstrings = setdiff1d(field.unique(), list(stringmap.keys()))
        field = field.replace(badstrings, unmapped)

    return field


def fix_int_na(col, float_na=np.nan, int_na=-1, str_na=''):
    """
    Convert a dataframe column from float to string for CSV export.

    Numpy doesn't have a real NA value for integers. When pandas stores integer
    data which has NA values, it thus upcasts integers to floating point
    values, using np.nan values for NA. However, in order to dump some of our
    dataframes to CSV files that are suitable for loading into postgres
    directly, we need to write out integer formatted numbers, with empty
    strings as the NA value. This function replaces np.nan values with a
    sentiel value, converts the column to integers, and then to strings,
    finally replacing the sentinel value with the desired NA string.

    Args:
        col (pandas.Series): The DataFrame column that needs to be
            reformatted for output.
        float_na (float): The floating point value to be interpreted as NA and
            replaced in col.
        int_na (int): Sentinel value to substitute for float_na prior to
            conversion of the column to integers.
        str_na (str): sa.String value to substitute for int_na after the column
            has been converted to strings.

    Returns:
        str_col (pandas.Series): a column containing the same values and lack
            of values as the col argument, but stored as strings that are
            compatible with the postgresql COPY FROM command.
    """
    return(col.replace(float_na, int_na).
           astype(int).
           astype(str).
           replace(str(int_na), str_na))


def month_year_to_date(df, month_regex="(_month$|^month_)",
                       year_regex="(_year$|^year_)"):
    """Convert pairs of year/month fields in a dataframe into Date fields."""
    import re
    df = df.copy()
    # Columns that match our month or year patterns.
    month_cols = list(df.filter(regex=month_regex).columns)
    year_cols = list(df.filter(regex=year_regex).columns)

    # Base column names that don't include the month or year pattern
    months_base = [re.sub(month_regex, '', m) for m in month_cols]
    years_base = [re.sub(year_regex, '', y) for y in year_cols]

    # We only want to retain columns that have BOTH month and year
    # matches -- otherwise there's no point in creating a Date.
    date_base = [base for base in months_base if base in years_base]

    # For each base column that DOES have both a month and year,
    # We need to grab the real column names corresponding to each,
    # so we can access the values in the data frame, and use them
    # to create a corresponding Date column named [BASE]_date
    month_year_base = []
    for base in date_base:
        cols = list(df.filter(regex=base).columns)
        # the one field that matches col & also month_regex
        for col in cols:
            if re.search(month_regex, col) is not None:
                month_col = col
            elif re.search(year_regex, col) is not None:
                year_col = col
        month_year_base.append((month_col, year_col, base))

    for m, y, b in month_year_base:
        df[y] = fix_int_na(df[y])
        df[m] = fix_int_na(df[m])

        date_mask = (df[y] != '') & (df[m] != '')
        years = df.loc[date_mask, y]
        months = df.loc[date_mask, m]

        df.loc[date_mask, '{}_date'.format(b)] = pd.to_datetime({
            'year': years,
            'month': months,
            'day': 1}, errors='coerce')

        # Now that we've replaced these fields with a date, we drop them.
        df = df.drop([m, y], axis=1)

    return(df)
