"""
General utility functions that are used in a variety of contexts.

The functions in this module are used in various stages of the ETL and post-etl
processes. They are usually not dataset specific, but not always. If a function
is designed to be used as a general purpose tool, applicable in multiple
scenarios, it should probably live here. There are lost of transform type
functions in here that help with cleaning and restructing dataframes.
"""

import logging
import os.path
import re
from functools import partial

import numpy as np
import pandas as pd
import sqlalchemy as sa
import timezonefinder

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)

# This is a little abbreviated function that allows us to propagate the NA
# values through groupby aggregations, rather than using inefficient lambda
# functions in each one.
sum_na = partial(pd.Series.sum, skipna=False)

# Initializing this TimezoneFinder opens a bunch of geography files and holds
# them open for efficiency. I want to avoid doing that for every call to find
# the timezone, so this is global.
tz_finder = timezonefinder.TimezoneFinder()


def is_annual(df_year, year_col='report_date'):
    """Determine whether dataframe is consistent with yearly reporting.

    Some processes will only work with consistent yearly reporting. This means
    if you have two non-contiguous years of data or the datetime reporting is
    inconsistent, the process will break.

    Args:
        df_year ():
        year_col (str): The column of the DataFrame in which the year is
            reported.

    Returns:
        bool:

    Todo:
        Return to for df_year, Returns, assert statements

    """
    year_index = pd.DatetimeIndex(df_year[year_col].unique()).sort_values()
    if len(year_index) >= 3:
        date_freq = pd.infer_freq(year_index)
        assert date_freq == 'AS-JAN', "infer_freq() not AS-JAN"
    elif len(year_index) == 2:
        min_year = year_index.min()
        max_year = year_index.max()
        assert year_index.min().month == 1, "min year not Jan"
        assert year_index.min().day == 1, "min day not 1st"
        assert year_index.max().month == 1, "max year not Jan"
        assert year_index.max().day == 1, "max day not 1st"
        delta_year = pd.Timedelta(max_year - min_year)
        assert delta_year / pd.Timedelta(days=1) >= 365.0
        assert delta_year / pd.Timedelta(days=1) <= 366.0
    elif len(year_index) == 1:
        assert year_index.min().month == 1, "only month not Jan"
        assert year_index.min().day == 1, "only day not 1st"
    else:
        assert False, "Zero dates found!"

    return True


def merge_on_date_year(df_date, df_year, on=(), how='inner',
                       date_col='report_date',
                       year_col='report_date'):
    """Merge two dataframes based on a shared year.

    Some of our data is annual, and has an integer year column (e.g. FERC 1).
    Some of our data is annual, and uses a Date column (e.g. EIA 860), and
    some of our data has other temporal resolutions, and uses date columns
    (e.g. EIA 923 fuel receipts are monthly, EPA CEMS data is hourly). This
    function takes two data frames and merges them based on the year that the
    data pertains to.  It requires one of the dataframes to have annual
    resolution, and allows the annual time to be described as either an integer
    year or a Date. The non-annual dataframe must have a Date column.

    By default, it is assumed that both the date and annual columns to be
    merged on are called 'report_date' since that's the common case when
    bringing together EIA860 and EIA923 data.

    Args:
        df_date: the dataframe with a more granular date column, the label of
            which is specified by date_col (report_date by default)
        df_year: the dataframe with a column containing annual dates, the label
            of which is specified by year_col (report_date by default)
        on: The list of columns to merge on, other than the year and date
            columns.
        date_col: name of the date column to use to find the year to merge on.
            Must be a Date.
        year_col: name of the year column to merge on. Must be a Date
            column with annual resolution.

    Returns:
        :mod:`pandas.DataFrame`: a dataframe with a date column, but no year
        columns, and only one copy of any shared columns that were not part of
        the list of columns to be merged on.  The values from df1 are the ones
        which are retained for any shared, non-merging columns

    """
    assert date_col in df_date.columns.tolist()
    assert year_col in df_year.columns.tolist()
    # assert that the annual data is in fact annual:
    assert is_annual(df_year, year_col=year_col)

    # assert that df_date has annual or finer time resolution.
    first_date = pd.to_datetime(df_date[date_col].min())
    all_dates = pd.DatetimeIndex(df_date[date_col]).unique().sort_values()
    assert len(all_dates) > 0
    if len(all_dates) > 1:
        if len(all_dates) == 2:
            second_date = all_dates.max()
        elif len(all_dates) > 2:
            date_freq = pd.infer_freq(all_dates)
            rng = pd.date_range(start=first_date, periods=2, freq=date_freq)
            second_date = rng[1]
            assert (second_date - first_date) / pd.Timedelta(days=366) <= 1.0

    # Create a temporary column in each dataframe with the year
    df_year = df_year.copy()
    df_date = df_date.copy()
    df_year['year_temp'] = pd.to_datetime(df_year[year_col]).dt.year
    # Drop the yearly report_date column: this way there won't be duplicates
    # and the final df will have the more granular report_date.
    df_year = df_year.drop([year_col], axis=1)
    df_date['year_temp'] = pd.to_datetime(df_date[date_col]).dt.year

    full_on = on + ['year_temp']
    unshared_cols = [col for col in df_year.columns.tolist()
                     if col not in df_date.columns.tolist()]
    cols_to_use = unshared_cols + full_on

    # Merge and drop the temp
    merged = pd.merge(df_date, df_year[cols_to_use], how=how, on=full_on)
    merged = merged.drop(['year_temp'], axis=1)

    return merged


def organize_cols(df, cols):
    """
    Organize columns into key ID & name fields & alphabetical data columns.

    For readability, it's nice to group a few key columns at the beginning
    of the dataframe (e.g. report_year or report_date, plant_id...) and then
    put all the rest of the data columns in alphabetical order.

    Args:
        df: The DataFrame to be re-organized.
        cols: The columns to put first, in their desired output ordering.

    Returns:
        pandas.DataFrame: A dataframe with the same columns as the input
        DataFrame df, but with cols first, in the same order as they
        were passed in, and the remaining columns sorted alphabetically.

    Todo:
        Update docstring.

    """
    # Generate a list of all the columns in the dataframe that are not
    # included in cols
    data_cols = [c for c in df.columns.tolist() if c not in cols]
    data_cols.sort()
    organized_cols = cols + data_cols
    return df[organized_cols]


def extend_annual(df, date_col='report_date', start_date=None, end_date=None):
    """Extend time range in a DataFrame by duplicating first and last years.

    Takes the earliest year's worth of annual data and uses it to create
    earlier years by duplicating it, and changing the year.  Similarly,
    extends a dataset into the future by duplicating the last year's records.

    This is primarily used to extend the EIA860 data about utilities, plants,
    and generators, so that we can analyze a larger set of EIA923 data. EIA923
    data has been integrated a bit further back, and the EIA860 data has a year
    long lag in being released.

    Args:
        df (pandas.DataFrame):
        date_col (str):
        start_date (date):
        end_date (date):

    Returns:
        pandas.DataFrame:

    Todo:
        Return to

    """
    # assert that df time resolution really is annual
    assert is_annual(df, year_col=date_col)

    earliest_date = pd.to_datetime(df[date_col].min())
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        while start_date < earliest_date:
            prev_year = \
                df[pd.to_datetime(df[date_col]) == earliest_date].copy()
            prev_year[date_col] = earliest_date - pd.DateOffset(years=1)
            df = df.append(prev_year)
            df[date_col] = pd.to_datetime(df[date_col])
            earliest_date = pd.to_datetime(df[date_col].min())

    latest_date = pd.to_datetime(df[date_col].max())
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        while end_date >= latest_date + pd.DateOffset(years=1):
            next_year = df[pd.to_datetime(df[date_col]) == latest_date].copy()
            next_year[date_col] = latest_date + pd.DateOffset(years=1)
            df = df.append(next_year)
            df[date_col] = pd.to_datetime(df[date_col])
            latest_date = pd.to_datetime(df[date_col].max())

    df[date_col] = pd.to_datetime(df[date_col])
    return df


def strip_lower(df, columns=None):
    """Strip and compact whitespace, lowercase listed DataFrame columns.

    First converts all listed columns (if present in df) to string type, then
    applies the str.strip() and str.lower() methods to them, and replaces all
    internal whitespace to a single space. The columns are assigned in place.

    Args:
        df (pandas.DataFrame): DataFrame whose columns are being cleaned up.
        columns (iterable): The labels of the columns to be stripped and
            converted to lowercase.

    Returns:
        :mod:`pandas.DataFrame`: The whole DataFrame that was passed in, with
        the columns cleaned up in place, allowing method chaining.

    """
    out_df = df.copy()
    for col in columns:
        if col in out_df.columns:
            out_df.loc[:, col] = (
                out_df[col].astype(str).
                str.strip().
                str.lower().
                str.replace(r'\s+', ' ')
            )

    return out_df


def cleanstrings_series(col, str_map, unmapped=None, simplify=True):
    """Clean up the strings in a single column/Series.

    Args:
        col (pd.Series): A pandas Series, typically a single column of a
            dataframe, containing the freeform strings that are to be cleaned.
        map (dict): A dictionary of lists of strings, in which the keys are the
            simplified canonical strings, witch which each string found in the
            corresponding list will be replaced.
        unmapped (str): A value with which to replace any string found in col
            that is not found in one of the lists of strings in map. Typically
            the null string ''. If None, these strings will not be replaced.
        simplify (bool): If True, strip and compact whitespace, and lowercase
            all strings in both the list of values to be replaced, and the
            values found in col. This can reduce the number of strings that
            need to be kept track of.

    Returns:
        :mod:`pandas.Series`: The cleaned up Series / column, suitable for
        replacng the original messy column in a pd.Dataframe.

    """
    if simplify:
        col = (
            col.astype(str).
            str.strip().
            str.lower().
            str.replace(r'\s+', ' ')
        )
        for k in str_map:
            str_map[k] = [re.sub(r'\s+', ' ', s.lower().strip())
                          for s in str_map[k]]

    for k in str_map:
        if str_map[k]:
            col = col.replace(str_map[k], k)

    if unmapped is not None:
        badstrings = np.setdiff1d(col.unique(), list(str_map.keys()))
        # This call to replace can only work if there are actually some
        # leftover strings to fix -- otherwise it runs forever because we
        # are replacing nothing with nothing.
        if len(badstrings) > 0:
            col = col.replace(badstrings, unmapped)

    return col


def cleanstrings(df, columns, stringmaps, unmapped=None, simplify=True):
    """Consolidate freeform strings in several dataframe columns.

    This function will consolidate freeform strings found in `columns` into
    simplified categories, as defined by `stringmaps`. This is useful when
    a field contains many different strings that are really meant to represent
    a finite number of categories, e.g. a type of fuel. It can also be used to
    create simplified categories that apply to similar attributes that are
    reported in various data sources from different agencies that use their own
    taxonomies.

    The function takes and returns a pandas.DataFrame, making it suitable for
    use with the pd.DataFrame.pipe() method in a chain.

    Args:
        df (pd.DataFrame): the DataFrame containing the string columns to be
            cleaned up.
        columns (list): a list of string column labels found in the column
            index of df. These are the columns that will be cleaned.
        stringmaps (list): a list of dictionaries. The keys of these
            dictionaries are strings, and the values are lists of strings. Each
            dictionary in the list corresponds to a column in columns. The
            keys of the dictionaries are the values with which every string in
            the list of values will be replaced.
        unmapped (str, None): the value with which strings not found in the
            stringmap dictionary will be replaced. Typically the null string
            ''. If None, then strings found in the columns but not in the
            stringmap will be left unchanged.
        simplify (bool): If true, strip whitespace, remove duplicate
            whitespace, and force lower-case on both the string map and the
            values found in the columns to be cleaned. This can reduce the
            overall number of string values that need to be tracked.

    Returns:
        pandas.Series: The function returns a new pandas series/column that can
        be used to set the values of the original data.

    Todo:
        Update docstring.

    """
    out_df = df.copy()
    for col, str_map in zip(columns, stringmaps):
        out_df[col] = cleanstrings_series(
            out_df[col], str_map, unmapped=unmapped, simplify=simplify)

    return out_df


def fix_int_na(df, columns, float_na=np.nan, int_na=-1, str_na=''):
    """Convert NA containing integer columns from float to string.

    Numpy doesn't have a real NA value for integers. When pandas stores integer
    data which has NA values, it thus upcasts integers to floating point
    values, using np.nan values for NA. However, in order to dump some of our
    dataframes to CSV files that are suitable for loading into postgres
    directly, we need to write out integer formatted numbers, with empty
    strings as the NA value. This function replaces np.nan values with a
    sentinel value, converts the column to integers, and then to strings,
    finally replacing the sentinel value with the desired NA string.

    Args:
        df (pandas.DataFrame): The dataframe to be fixed. This argument allows
            method chaining with the pipe() method.
        columns (iterable of strings): A list of DataFrame column labels
            indicating which columns need to be reformatted for output.
        float_na (float): The floating point value to be interpreted as NA and
            replaced in col.
        int_na (int): Sentinel value to substitute for float_na prior to
            conversion of the column to integers.
        str_na (str): sa.String value to substitute for int_na after the column
            has been converted to strings.

    Returns:
        df (pandas.DataFrame): a new DataFrame, with the selected columns
        converted to strings that look like integers, compatible with
        the postgresql COPY FROM command.

    Todo:
        Update docstring.

    """
    return (
        df.replace({c: float_na for c in columns}, int_na)
          .astype({c: int for c in columns})
          .astype({c: str for c in columns})
          .replace({c: str(int_na) for c in columns}, str_na)
    )


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

    Args:
        df (pandas.DataFrame): The DataFrame in which to convert year/months
            fields to Date fields.

    Returns:
        pandas.DataFrame: A DataFrame in which the year/month fields have been
        converted into Date fields.

    Todo:
        Update docstring.

    """
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
        if not len(month_col) == 1:
            raise AssertionError()
        month_col = month_col[0]
        base_year_regex = '^{}{}'.format(base, year_regex)
        year_col = list(df.filter(regex=base_year_regex).columns)
        if not len(year_col) == 1:
            raise AssertionError()
        year_col = year_col[0]
        date_col = '{}_date'.format(base)
        month_year_date.append((month_col, year_col, date_col))

    for month_col, year_col, date_col in month_year_date:
        df = fix_int_na(df, columns=[year_col, month_col])

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
    """Convert specified year, month or day columns into a datetime object.

    Args:
        df (pandas.DataFrame): dataframe to convert
        date_col (str): the name of the column you want in the output.
        year_col (str): the name of the year column in the original table.
        month_col (str): the name of the month column in the original table.
        day_col: the name of the day column in the original table.
        month_value (int): generated month if no month exists.
        day_value (int): generated day if no month exists.

    Returns:
        pandas.DataFrame: A DataFrame in which the year, month, day columns
        values have been converted into datetime objects.

    Todo:
        Update docstring.

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


def fix_eia_na(df):
    """Replace common ill-posed EIA NA spreadsheet values with np.nan.

    Args:
        df (pandas.DataFrame): The DataFrame to clean.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.

    Todo:
        Update docstring.

    """
    return df.replace(to_replace=[r'^\.$', r'^\s$', r'^$'],
                      value=np.nan, regex=True)


def simplify_columns(df):
    """Simplify column labels for use as database fields.

    This transformation includes:
     - Replacing all non-alphanumeric characters with spaces.
     - Forcing all letters to be lower case.
     - Compacting internal whitespace.
     - Stripping leading and trailing whitespace.

    Args:
        df (pandas.DataFrame): The DataFrame to clean.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.

    Todo:
        Update docstring.

    """
    df.columns = (
        df.columns.
        str.replace('[^0-9a-zA-Z]+', ' ').
        str.strip().
        str.lower().
        str.replace(r'\s+', ' ').
        str.replace(' ', '_')
    )
    return df


def find_timezone(*, lng=None, lat=None, state=None, strict=True):
    """Find the timezone associated with the a specified input location.

    Note that this function requires named arguments. The names are lng, lat,
    and state.  lng and lat must be provided, but they may be NA. state isn't
    required, and isn't used unless lng/lat are NA or timezonefinder can't find
    a corresponding timezone.

    Timezones based on states are imprecise, so it's far better to use lng/lat
    if possible. If `strict` is True, state will not be used.
    More on state-to-timezone conversion here:
    https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory

    Args:
        lng (int or float in [-180,180]): Longitude, in decimal degrees
        lat (int or float in [-90, 90]): Latitude, in decimal degrees
        state (str): Abbreviation for US state or Canadian province
        strict (bool): Raise an error if no timezone is found?

    Returns:
        str: The timezone (as an IANA string) for that location.

    Todo:
        Update docstring.

    """
    try:
        tz = tz_finder.timezone_at(lng=lng, lat=lat)
        if tz is None:  # Try harder
            # Could change the search radius as well
            tz = tz_finder.closest_timezone_at(lng=lng, lat=lat)
    # For some reason w/ Python 3.6 we get a ValueError here, but with
    # Python 3.7 we get an OverflowError...
    except (OverflowError, ValueError):
        # If we're being strict, only use lng/lat, not state
        if strict:
            raise ValueError(
                f"Can't find timezone for: lng={lng}, lat={lat}, state={state}"
            )
        # If, e.g., the coordinates are missing, try looking in the
        # state_tz_approx dictionary.
        try:
            tz = pudl.constants.state_tz_approx[state]
        except KeyError:
            tz = None
    return tz


def verify_input_files(ferc1_years,  # noqa: C901
                       eia923_years,
                       eia860_years,
                       epacems_years,
                       epacems_states,
                       pudl_settings):
    """Verify that all required data files exist prior to the ETL process.

    Args:
        ferc1_years (iterable): Years of FERC1 data we're going to import.
        eia923_years (iterable): Years of EIA923 data we're going to import.
        eia860_years (iterable): Years of EIA860 data we're going to import.
        epacems_years (iterable): Years of CEMS data we're going to import.
        epacems_states (iterable): States of CEMS data we're going to import.
        data_dir (path-like): Path to the top level of the PUDL datastore.

    Raises:
        FileNotFoundError: If any of the requested data is missing.

    Todo:
        Check Docstring.

    """
    data_dir = pudl_settings['data_dir']

    missing_ferc1_years = {
        str(y) for y in ferc1_years if not os.path.isfile(
            pudl.extract.ferc1.dbc_filename(y, data_dir=data_dir))
    }

    missing_eia860_years = set()
    for y in eia860_years:
        for pattern in pc.files_eia860:
            f = pc.files_dict_eia860[pattern]
            try:
                # This function already looks for the file, and raises an
                # IndexError if missing
                pudl.extract.eia860.get_eia860_file(y, f, data_dir=data_dir)
            except IndexError:
                missing_eia860_years.add(str(y))

    missing_eia923_years = set()
    for y in eia923_years:
        try:
            f = pudl.extract.eia923.get_eia923_file(y, data_dir=data_dir)
        except AssertionError:
            missing_eia923_years.add(str(y))
        if not os.path.isfile(f):
            missing_eia923_years.add(str(y))

    if epacems_states and list(epacems_states)[0].lower() == 'all':
        epacems_states = list(pc.cems_states.keys())
    missing_epacems_year_states = set()
    for y in epacems_years:
        for s in epacems_states:
            for m in range(1, 13):
                try:
                    p = pudl.workspace.datastore.path(
                        source='epacems',
                        year=y,
                        month=m,
                        state=s,
                        data_dir=data_dir
                    )
                except AssertionError:
                    missing_epacems_year_states.add((str(y), s))
                    continue
                if not os.path.isfile(p):
                    missing_epacems_year_states.add((str(y), s))

    any_missing = (missing_eia860_years or missing_eia923_years
                   or missing_ferc1_years or missing_epacems_year_states)
    if any_missing:
        err_msg = ["Missing data files for the following sources and years:"]
        if missing_ferc1_years:
            err_msg += ["  FERC 1:  " + ", ".join(missing_ferc1_years)]
        if missing_eia860_years:
            err_msg += ["  EIA 860: " + ", ".join(missing_eia860_years)]
        if missing_eia923_years:
            err_msg += ["  EIA 923: " + ", ".join(missing_eia923_years)]
        if missing_epacems_year_states:
            missing_yr_str = ", ".join(
                {yr_st[0] for yr_st in missing_epacems_year_states})
            missing_st_str = ", ".join(
                {yr_st[1] for yr_st in missing_epacems_year_states})
            err_msg += ["  EPA CEMS:"]
            err_msg += ["    Years:  " + missing_yr_str]
            err_msg += ["    States: " + missing_st_str]
        raise FileNotFoundError("\n".join(err_msg))


def drop_tables(engine):
    """Drops all tables from a SQLite database.

    Creates an sa.schema.MetaData object reflecting the structure of the
    database that the passed in ``engine`` refers to, and uses that schema to
    drop all existing tables.

    Todo:
        Treat DB connection as a context manager (with/as).

    Args:
        engine (sa.engine.Engine): An SQL Alchemy SQLite database Engine
            pointing at an exising SQLite database to be deleted.

    Returns:
        None
    """
    md = sa.MetaData()
    md.reflect(engine)
    md.drop_all(engine)
    conn = engine.connect()
    conn.execute("VACUUM")
    conn.close()


def merge_dicts(list_of_dicts):
    """
    Merge multipe dictionaries together.

    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    Args:
        dict_args (list): a list of dictionaries.
    Returns:
        dictionary
    """
    merge_dict = {}
    for dictionary in list_of_dicts:
        merge_dict.update(dictionary)
    return merge_dict
