"""
Shared routines used for transforming data before loading into the PUDL DB.

This module contains functions that are more generally useful in the cleaning
and transformation of data that is being loaded into the PUDL database, rather
than being specific to a particular data source.
"""

import pandas as pd
import numpy as np


def cleanstrings(field, stringmap, unmapped=None, simplify=True):
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

        simplify (bool): If true, strip whitespace, remove duplicate
            whitespace, and force lower-case on both the string map and the
            field values.

    Returns:
        pandas.Series: The function returns a new pandas series/column that can
            be used to set the values of the original data.
    """
    from numpy import setdiff1d
    import re

    # Simplify the strings we're working with, to reduce the number of strings
    # we need to enumerate in the maps

    if simplify:
        # Transform strings to lower case, strip leading/trailing whitespace
        field = field.str.lower().str.strip()
        # remove duplicate internal whitespace
        field = field.replace('[\s+]', ' ', regex=True)
        for k, v in stringmap.items():
            stringmap[k] = [re.sub('\s+', ' ', s.lower().strip()) for s in v]

    for k in stringmap.keys():
        if len(stringmap[k]) > 0:
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
    return (col.replace(float_na, int_na).
           astype(int).
           astype(str).
           replace(str(int_na), str_na))


def month_year_to_date(df):
    """Convert all pairs of year/month fields in a dataframe into Date fields.

    This function finds all column names within a dataframe that match the
    regular expression '_month$' and '_year$', and looks for pairs that have
    identical prefixes before the underscore. These fields are assumed to
    describe a date, accurate to the month.  The two fields are used to
    construct a new _date column (having the same prefix) and the month/year
    columns are then dropped.

    This function needs to be combined with convert_to_date, and improved:
     - find and use a _day$ column as well
     - allow specification of default month & day values, if none are found.
     - allow specification of lists of year, month, and day columns to be
       combined, rather than automataically finding all the matching ones.
     - Do the Right Thing when invalid or NA values are encountered.
    """
    import re
    df = df.copy()
    month_regex = "_month$"
    year_regex = "_year$"
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
    month_year_date = []
    for base in date_base:
        base_month_regex = '^{}{}'.format(base, month_regex)
        month_col = list(df.filter(regex=base_month_regex).columns)
        assert(len(month_col) == 1)
        month_col = month_col[0]
        base_year_regex = '^{}{}'.format(base, year_regex)
        year_col = list(df.filter(regex=base_year_regex).columns)
        assert(len(year_col) == 1)
        year_col = year_col[0]
        date_col = '{}_date'.format(base)
        month_year_date.append((month_col, year_col, date_col))

    for month_col, year_col, date_col in month_year_date:
        df[year_col] = fix_int_na(df[year_col])
        df[month_col] = fix_int_na(df[month_col])

        date_mask = (df[year_col] != '') & (df[month_col] != '')
        years = df.loc[date_mask, year_col]
        months = df.loc[date_mask, month_col]

        df.loc[date_mask, date_col] = pd.to_datetime({
            'year': years,
            'month': months,
            'day': 1}, errors='coerce')

        # Now that we've replaced these fields with a date, we drop them.
        df = df.drop([month_col, year_col], axis=1)

    return df


def convert_to_date(df,
                    date_col='report_date',
                    year_col='report_year',
                    month_col='report_month',
                    day_col='report_day',
                    month_value=1,
                    day_value=1):
    """
    Convert specified year, month or day columns into a datetime object.

    Args:
        df: dataframe to convert
        date_col: the name of the column you want in the output.
        year_col: the name of the year column in the original table.
        month_col: the name of the month column in the original table.
        day_col: the name of the day column in the original table.
        month_value: generated month if no month exists.
        day_value: generated day if no month exists.
    """
    df = df.copy()
    if date_col in df.columns:
        return df

    year = df[year_col]

    if month_col not in df.columns:
        month = month_value
    else:
        month = df[month_col]

    if day_col not in df.columns:
        day = day_value
    else:
        day = df[day_col]

    df[date_col] = \
        pd.to_datetime({'year': year,
                        'month': month,
                        'day': day})
    cols_to_drop = [x for x in [
        day_col, year_col, month_col] if x in df.columns]
    df.drop(cols_to_drop, axis=1, inplace=True)

    return df
