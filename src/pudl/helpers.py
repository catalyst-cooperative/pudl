"""
General utility functions that are used in a variety of contexts.

The functions in this module are used in various stages of the ETL and post-etl
processes. They are usually not dataset specific, but not always. If a function
is designed to be used as a general purpose tool, applicable in multiple
scenarios, it should probably live here. There are lost of transform type
functions in here that help with cleaning and restructing dataframes.
"""
import logging
import pathlib
import re
import shutil
from functools import partial

import addfips
import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa
import timezonefinder
from sqlalchemy.engine import reflection

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)

# This is a little abbreviated function that allows us to propagate the NA
# values through groupby aggregations, rather than using inefficient lambda
# functions in each one.
sum_na = partial(pd.Series.sum, skipna=False)

# Initializing this TimezoneFinder opens a bunch of geography files and holds
# them open for efficiency. I want to avoid doing that for every call to find
# the timezone, so this is global.
tz_finder = timezonefinder.TimezoneFinder()


def download_zip_url(url, save_path, chunk_size=128):
    """
    Download and save a Zipfile locally.

    Useful for acquiring and storing non-PUDL data locally.

    Args:
        url (str): The URL from which to download the Zipfile
        save_path (pathlib.Path): The location to save the file.
        chunk_size (int): Data chunk in bytes to use while downloading.

    Returns:
        None

    """
    # This is a temporary hack to avoid being filtered as a bot:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    r = requests.get(url, stream=True, headers=headers)
    with save_path.open(mode='wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def add_fips_ids(df, state_col="state", county_col="county", vintage=2015):
    """Add State and County FIPS IDs to a dataframe."""
    # force the columns to be the nullable string types so we have a consistent
    # null value to filter out before feeding to addfips
    df = df.astype({
        state_col: pd.StringDtype(),
        county_col: pd.StringDtype(),

    })
    af = addfips.AddFIPS(vintage=vintage)
    # Lookup the state and county FIPS IDs and add them to the dataframe:
    df["state_id_fips"] = df.apply(
        lambda x: (af.get_state_fips(state=x[state_col])
                   if pd.notnull(x[state_col]) else pd.NA),
        axis=1)

    logger.info(
        f"Assigned state FIPS codes for "
        f"{len(df[df.state_id_fips.notnull()])/len(df):.2%} of records."
    )
    df["county_id_fips"] = df.apply(
        lambda x: (af.get_county_fips(state=x[state_col], county=x[county_col])
                   if pd.notnull(x[county_col]) else pd.NA),
        axis=1)
    # force the code columns to be nullable strings - the leading zeros are
    # important
    df = df.astype({
        "county_id_fips": pd.StringDtype(),
        "state_id_fips": pd.StringDtype(),
    })
    logger.info(
        f"Assigned county FIPS codes for "
        f"{len(df[df.county_id_fips.notnull()])/len(df):.2%} of records."
    )
    return df


def clean_eia_counties(df, fixes, state_col="state", county_col="county"):
    """Replace non-standard county names with county nmes from US Census."""
    df = df.copy()
    df[county_col] = (
        df[county_col].str.strip()
        .str.replace(r"\s+", " ")  # Condense multiple whitespace chars.
        .str.replace(r"^St ", "St. ")  # Standardize abbreviation.
        .str.replace(r"^Ste ", "Ste. ")  # Standardize abbreviation.
        .str.replace("Kent & New Castle", "Kent, New Castle")  # Two counties
        # Fix ordering, remove comma
        .str.replace("Borough, Kodiak Island", "Kodiak Island Borough")
        # Turn comma-separated counties into lists
        .str.replace(",$", "").str.split(',')
    )
    # Create new records for each county in a multi-valued record
    df = df.explode(county_col)
    df[county_col] = df[county_col].str.strip()
    # Yellowstone county is in MT, not WY
    df.loc[(df[state_col] == "WY") &
           (df[county_col] == "Yellowstone"), state_col] = "MT"
    # Replace individual bad county names with identified correct names in fixes:
    for fix in fixes.itertuples():
        state_mask = df[state_col] == fix.state
        county_mask = df[county_col] == fix.eia_county
        df.loc[state_mask & county_mask, county_col] = fix.fips_county
    return df


def oob_to_nan(df, cols, lb=None, ub=None):
    """
    Set non-numeric values and those outside of a given rage to NaN.

    Args:
        df (pandas.DataFrame): The dataframe containing values to be altered.
        cols (iterable): Labels of the columns whose values are to be changed.
        lb: (number): Lower bound, below which values are set to NaN. If None,
            don't use a lower bound.
        ub: (number): Upper bound, below which values are set to NaN. If None,
            don't use an upper bound.

    Returns:
        pandas.DataFrame: The altered DataFrame.

    """
    out_df = df.copy()
    for col in cols:
        # Force column to be numeric if possible, NaN otherwise:
        out_df.loc[:, col] = pd.to_numeric(out_df[col], errors="coerce")
        if lb is not None:
            out_df.loc[out_df[col] < lb, col] = np.nan
        if ub is not None:
            out_df.loc[out_df[col] > ub, col] = np.nan

    return out_df


def prep_dir(dir_path, clobber=False):
    """
    Create (or delete and recreate) a directory.

    Args:
        dir_path (path-like): path to the directory that you are trying to
            clean and prepare.
        clobber (bool): If True and dir_path exists, it will be removed and
            replaced with a new, empty directory.

    Raises:
        FileExistsError: if a file or directory already exists at dir_path.

    Returns:
        pathlib.Path: Path to the created directory.

    """
    dir_path = pathlib.Path(dir_path)
    if dir_path.exists():
        if clobber:
            shutil.rmtree(dir_path)
        else:
            raise FileExistsError(f'{dir_path} exists and clobber is {clobber}')
    dir_path.mkdir(parents=True)
    return dir_path


def is_doi(doi):
    """
    Determine if a string is a valid digital object identifier (DOI).

    Function simply checks whether the offered string matches a regular
    expresssion -- it doesn't check whether the DOI is actually registered
    with the relevant authority.

    Args:
        doi (str): String to validate.

    Returns:
        bool: True if doi matches the regex for valid DOIs, False otherwise.

    """
    doi_regex = re.compile(
        r'(doi:\s*|(?:https?://)?(?:dx\.)?doi\.org/)?(10\.\d+(.\d+)*/.+)$',
        re.IGNORECASE | re.UNICODE)

    return bool(re.match(doi_regex, doi))


def is_annual(df_year, year_col='report_date'):
    """
    Determine whether a DataFrame contains consistent annual time-series data.

    Some processes will only work with consistent yearly reporting. This means
    if you have two non-contiguous years of data or the datetime reporting is
    inconsistent, the process will break. This function attempts to infer the
    temporal frequency of the dataframe, or if that is impossible, to at least
    see whether the data would be consistent with annual reporting -- e.g. if
    there is only a single year of data, it should all have the same date, and
    that date should correspond to January 1st of a given year.

    This function is known to be flaky and needs to be re-written to deal with
    the edge cases better.

    Args:
        df_year (:class:`pandas.DataFrame`): A pandas DataFrame that might
            contain time-series data at annual resolution.
        year_col (str): The column of the DataFrame in which the year is
            reported.

    Returns:
        bool: True if df_year is found to be consistent with continuous annual
        time resolution, False otherwise.

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
        pandas.DataFrame: a dataframe with a date column, but no year
        columns, and only one copy of any shared columns that were not part of
        the list of columns to be merged on.  The values from df1 are the ones
        which are retained for any shared, non-merging columns

    Raises:
        ValueError: if the date or year columns are not found, or if the year
            column is found to be inconsistent with annual reporting.

    TODO: Right mergers will result in null values in the resulting date
        column. The final output includes the date_col from the date_df and thus
        if there are any entity records (records being merged on) in the
        year_df but not in the date_df, a right merge will result in nulls in
        the date_col. And when we drop the 'year_temp' column, the year from
        the year_df will be gone. Need to determine how to deal with this.
        Should we generate a montly record in each year? Should we generate
        full time serires? Should we restrict right merges in this function?

    """
    if date_col not in df_date.columns.tolist():
        raise ValueError(f"Date column {date_col} not found in df_date.")
    if year_col not in df_year.columns.tolist():
        raise ValueError(f"Year column {year_col} not found in df_year.")
    if not is_annual(df_year, year_col=year_col):
        raise ValueError(f"df_year is not annual, based on column {year_col}.")

    first_date = pd.to_datetime(df_date[date_col].min())
    all_dates = pd.DatetimeIndex(df_date[date_col]).unique().sort_values()
    if not len(all_dates) > 0:
        raise ValueError("Didn't find any dates in DatetimeIndex.")
    if len(all_dates) > 1:
        if len(all_dates) == 2:
            second_date = all_dates.max()
        elif len(all_dates) > 2:
            date_freq = pd.infer_freq(all_dates)
            rng = pd.date_range(start=first_date, periods=2, freq=date_freq)
            second_date = rng[1]
            if (second_date - first_date) / pd.Timedelta(days=366) > 1.0:
                raise ValueError("Consecutive annual dates >1 year apart.")

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

    """
    # Generate a list of all the columns in the dataframe that are not
    # included in cols
    data_cols = [c for c in df.columns.tolist() if c not in cols]
    data_cols.sort()
    organized_cols = cols + data_cols
    return df[organized_cols]


def strip_lower(df, columns):
    """Strip and compact whitespace, lowercase listed DataFrame columns.

    First converts all listed columns (if present in df) to string type, then
    applies the str.strip() and str.lower() methods to them, and replaces all
    internal whitespace to a single space. The columns are assigned in place.

    Args:
        df (pandas.DataFrame): DataFrame whose columns are being cleaned up.
        columns (iterable): The labels of the columns to be stripped and
            converted to lowercase.

    Returns:
        pandas.DataFrame: The whole DataFrame that was passed in, with
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
        col (pandas.Series): A pandas Series, typically a single column of a
            dataframe, containing the freeform strings that are to be cleaned.
        str_map (dict): A dictionary of lists of strings, in which the keys are
            the simplified canonical strings, witch which each string found in
            the corresponding list will be replaced.
        unmapped (str): A value with which to replace any string found in col
            that is not found in one of the lists of strings in map. Typically
            the null string ''. If None, these strings will not be replaced.
        simplify (bool): If True, strip and compact whitespace, and lowercase
            all strings in both the list of values to be replaced, and the
            values found in col. This can reduce the number of strings that
            need to be kept track of.

    Returns:
        pandas.Series: The cleaned up Series / column, suitable for
        replacing the original messy column in a :class:`pandas.DataFrame`.

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
    use with the :func:`pandas.DataFrame.pipe` method in a chain.

    Args:
        df (pandas.DataFrame): the DataFrame containing the string columns to
            be cleaned up.
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
        pandas.DataFrame: The function returns a new DataFrame containing the
        cleaned strings.

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
    dataframes to CSV files for use in data packages, we need to write out
    integer formatted numbers, with empty strings as the NA value. This
    function replaces np.nan values with a sentinel value, converts the column
    to integers, and then to strings, finally replacing the sentinel value with
    the desired NA string.

    This is an interim solution -- now that pandas extension arrays have been
    implemented, we need to go back through and convert all of these integer
    columns that contain NA values to Nullable Integer types like Int64.

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

    Todo:
        This function needs to be combined with convert_to_date, and improved:
        * find and use a _day$ column as well
        * allow specification of default month & day values, if none are found.
        * allow specification of lists of year, month, and day columns to be
        combined, rather than automataically finding all the matching ones.
        * Do the Right Thing when invalid or NA values are encountered.

    Args:
        df (pandas.DataFrame): The DataFrame in which to convert year/months
            fields to Date fields.

    Returns:
        pandas.DataFrame: A DataFrame in which the year/month fields have been
        converted into Date fields.

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
                    date_col="report_date",
                    year_col="report_year",
                    month_col="report_month",
                    day_col="report_day",
                    month_value=1,
                    day_value=1):
    """
    Convert specified year, month or day columns into a datetime object.

    If the input ``date_col`` already exists in the input dataframe, then no
    conversion is applied, and the original dataframe is returned unchanged.
    Otherwise the constructed date is placed in that column, and the columns
    which were used to create the date are dropped.

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
    df.drop(cols_to_drop, axis="columns", inplace=True)

    return df


def fix_eia_na(df):
    """
    Replace common ill-posed EIA NA spreadsheet values with np.nan.

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
    """
    Simplify column labels for use as snake_case database fields.

    All columns will be re-labeled by:
    * Replacing all non-alphanumeric characters with spaces.
    * Forcing all letters to be lower case.
    * Compacting internal whitespace to a single " ".
    * Stripping leading and trailing whitespace.
    * Replacing all remaining whitespace with underscores.

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


def drop_tables(engine,
                clobber=False):
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
    insp = reflection.Inspector.from_engine(engine)
    if len(insp.get_table_names()) > 0 and not clobber:
        raise AssertionError(
            f'You are attempting to drop your database without setting clobber to {clobber}')
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
        dict

    """
    merge_dict = {}
    for dictionary in list_of_dicts:
        merge_dict.update(dictionary)
    return merge_dict


def convert_cols_dtypes(df, data_source, name=None):
    """
    Convert the data types for a dataframe.

    This function will convert a PUDL dataframe's columns to the correct data
    type. It uses a dictionary in constants.py called column_dtypes to assign
    the right type. Within a given data source (e.g. eia923, ferc1) each column
    name is assumed to *always* have the same data type whenever it is found.

    Boolean type conversions created a special problem, because null values in
    boolean columns get converted to True (which is bonkers!)... we generally
    want to preserve the null values and definitely don't want them to be True,
    so we are keeping those columns as objects and preforming a simple mask for
    the boolean columns.

    The other exception in here is with the `utility_id_eia` column. It is
    often an object column of strings. All of the strings are numbers, so it
    should be possible to convert to :func:`pandas.Int32Dtype` directly, but it
    is requiring us to convert to int first. There will probably be other
    columns that have this problem... and hopefully pandas just enables this
    direct conversion.

    Args:
        df (pandas.DataFrame): dataframe with columns that appear in the PUDL
            tables.
        data_source (str): the name of the datasource (eia, ferc1, etc.)
        name (str): name of the table (for logging only!)

    Returns:
        pandas.DataFrame: a dataframe with columns as specified by the
        :mod:`pudl.constants` ``column_dtypes`` dictionary.

    """
    # get me all of the columns for the table in the constants dtype dict
    col_dtypes = {col: col_dtype for col, col_dtype
                  in pc.column_dtypes[data_source].items()
                  if col in list(df.columns)}

    # grab only the boolean columns (we only need their names)
    bool_cols = {col: col_dtype for col, col_dtype
                 in col_dtypes.items()
                 if col_dtype == pd.BooleanDtype()}
    # Grab only the string columns...
    string_cols = {col: col_dtype for col, col_dtype
                   in col_dtypes.items()
                   if col_dtype == pd.StringDtype()}

    # grab all of the non boolean columns
    non_bool_cols = {col: col_dtype for col, col_dtype
                     in col_dtypes.items()
                     if col_dtype != pd.BooleanDtype()}

    # If/when we have the columns exhaustively typed, we can do it like this,
    # but right now we don't have the FERC columns done, so we can't:
    # get me all of the columns for the table in the constants dtype dict
    # col_types = {
    #    col: pc.column_dtypes[data_source][col] for col in df.columns
    # }
    # grab only the boolean columns (we only need their names)
    # bool_cols = {col for col in col_types if col_types[col] is bool}
    # grab all of the non boolean columns
    # non_bool_cols = {
    #    col: col_types[col] for col in col_types if col_types[col] is not bool
    # }

    for col in bool_cols:
        # Bc the og bool values were sometimes coming across as actual bools or
        # strings, for some reason we need to map both types (I'm not sure
        # why!). We use na_action to preserve the og NaN's. I've also added in
        # the string version of a null value bc I am sure it will exist.
        df[col] = df[col].map({'False': False,
                               'True': True,
                               False: False,
                               True: True,
                               'nan': pd.NA})

    if name:
        logger.debug(f'Converting the dtypes of: {name}')
    # unfortunately, the pd.Int32Dtype() doesn't allow a conversion from object
    # columns to this nullable int type column. `utility_id_eia` shows up as a
    # column of strings (!) of numbers so it is an object column, and therefor
    # needs to be converted beforehand.
    if 'utility_id_eia' in df.columns:
        # we want to be able to use this dtype cleaning at many stages, and
        # sometimes this column has been converted to a float and therefor
        # we need to skip this conversion
        if df.utility_id_eia.dtypes is np.dtype('object'):
            df = df.astype({'utility_id_eia': 'float'})
    df = (
        df.replace(to_replace="<NA>", value={col: pd.NA for col in string_cols})
        .replace(to_replace="nan", value={col: pd.NA for col in string_cols})
        .astype(non_bool_cols)
        .astype(bool_cols)
    )
    return df


def convert_dfs_dict_dtypes(dfs_dict, data_source):
    """Convert the data types of a dictionary of dataframes.

    This is a wrapper for :func:`pudl.helpers.convert_cols_dtypes` which loops
    over an entire dictionary of dataframes, assuming they are all from the
    specified data source, and appropriately assigning data types to each
    column based on the data source specific type map stored in pudl.constants

    """
    cleaned_dfs_dict = {}
    for name, df in dfs_dict.items():
        cleaned_dfs_dict[name] = convert_cols_dtypes(df, data_source, name)
    return cleaned_dfs_dict


def generate_rolling_avg(df, group_cols, data_col, window, **kwargs):
    """
    Generate a rolling average.

    For a given dataframe with a `report_date` column, generate a monthly
    rolling average and use this rolling average to impute missing values.

    Args:
        df (pandas.DataFrame): Original dataframe. Must have group_cols
            column, a data_col column and a 'report_date' column.
        group_cols (iterable): a list of columns to groupby.
        data_col (str): the name of the data column.
        window (int): window from pandas.Series.rolling
        **kwargs : Additional arguments to pass to
            :class:`pandas.Series.rolling`.


    Returns:
        pandas.DataFrame

    """
    df = df.astype({'report_date': 'datetime64[ns]'})
    # create a full date range for this df
    date_range = (pd.DataFrame(pd.date_range(
        start=min(df['report_date']),
        end=max(df['report_date']), freq='MS',
        name='report_date')).
        # assiging a temp column to merge on
        assign(tmp=1))
    groups = (df[group_cols + ['report_date']].
              drop_duplicates().
              # assiging a temp column to merge on
              assign(tmp=1))
    # merge the date range and the groups together
    # to get the backbone/complete date range/groups
    bones = (date_range.merge(groups).
             # drop the temp column
             drop('tmp', axis=1).
             # then merge the actual data onto the
             merge(df, on=group_cols + ['report_date']).
             set_index(group_cols + ['report_date']).
             groupby(by=group_cols + ['report_date']).
             mean())
    # with the aggregated data, get a rolling average
    roll = (bones.rolling(window=window, center=True, **kwargs).
            agg({data_col: 'mean'})
            )
    # return the merged
    return bones.merge(roll,
                       on=group_cols + ['report_date'],
                       suffixes=('', '_rolling')).reset_index()


def fillna_w_rolling_avg(df_og, group_cols, data_col, window=12, **kwargs):
    """
    Filling NaNs with a rolling average.

    Imputes null values from a dataframe on a rolling monthly average. To note,
    this was designed to work with the PudlTabl object's tables.

    Args:
        df_og (pandas.DataFrame): Original dataframe. Must have group_cols
            column, a data_col column and a 'report_date' column.
        group_cols (iterable): a list of columns to groupby.
        data_col (str): the name of the data column.
        window (int): window from pandas.Series.rolling
        **kwargs : Additional arguments to pass to
            :class:`pandas.Series.rolling`.

    Returns:
        pandas.DataFrame: dataframe with nulls filled in.

    """
    df_og = df_og.astype({'report_date': 'datetime64[ns]'})
    df_roll = generate_rolling_avg(df_og, group_cols, data_col,
                                   window, **kwargs)
    df_roll[data_col] = df_roll[data_col].fillna(
        df_roll[f'{data_col}_rolling'])
    df_new = df_og.merge(df_roll,
                         how='left',
                         on=group_cols + ['report_date'],
                         suffixes=('', '_rollfilled'))
    df_new[data_col] = df_new[data_col].fillna(
        df_new[f'{data_col}_rollfilled'])
    return df_new.drop(columns=[f'{data_col}_rollfilled', f'{data_col}_rolling'])


def count_records(df, cols, new_count_col_name):
    """
    Count the number of unique records in group in a dataframe.

    Args:
        df (panda.DataFrame) : dataframe you would like to groupby and count.
        cols (iterable) : list of columns to group and count by.
        new_count_col_name (string) : the name that will be assigned to the
            column that will contain the count.
    Returns:
        pandas.DataFrame: dataframe with only the `cols` definted and the
        `new_count_col_name`.
    """
    return (df.assign(count_me=1).
            groupby(cols).
            agg({'count_me': 'count'}).
            reset_index().
            rename(columns={'count_me': new_count_col_name}))


def cleanstrings_snake(df, cols):
    """
    Clean the strings in a columns in a dataframe with snake case.

    Args:
        df (panda.DataFrame) : original dataframe.
        cols (list): list of columns in `df` to apply snake case to.

    """
    for col in cols:
        df.loc[:, col] = (
            df[col].astype(str).
            str.strip().
            str.lower().
            str.replace(r'\s+', '_')
        )
    return df


def zero_pad_zips(zip_series, n_digits):
    """
    Retain prefix zeros in zipcodes.

    Args:
        zip_series (pd.Series) : series containing the zipcode values.
        n_digits(int) : zipcode length (likely 4 or 5 digits).
    Returns:
        pd.Series: a series containing zipcodes with their prefix zeros intact and invalid zipcodes rendered as na.
    """
    # Add preceeding zeros where necessary and get rid of decimal zeros
    zip_series = (
        pd.to_numeric(zip_series)  # Make sure it's all numerical values
        .astype(pd.Int64Dtype())  # Make it a (nullable) Integer
        .fillna(0)  # fill the NA
        .astype(str).str.zfill(n_digits)  # Make it a string and zero pad it.
        .astype(pd.StringDtype())  # Make it nullable
        .replace({n_digits * "0": pd.NA})  # All-zero Zip codes aren't valid.
    )
    return zip_series
