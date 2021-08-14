"""
General utility functions that are used in a variety of contexts.

The functions in this module are used in various stages of the ETL and post-etl
processes. They are usually not dataset specific, but not always. If a function
is designed to be used as a general purpose tool, applicable in multiple
scenarios, it should probably live here. There are lost of transform type
functions in here that help with cleaning and restructing dataframes.

"""
import itertools
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
        # Condense multiple whitespace chars.
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"^St ", "St. ", regex=True)  # Standardize abbreviation.
        # Standardize abbreviation.
        .str.replace(r"^Ste ", "Ste. ", regex=True)
        .str.replace("Kent & New Castle", "Kent, New Castle")  # Two counties
        # Fix ordering, remove comma
        .str.replace("Borough, Kodiak Island", "Kodiak Island Borough")
        # Turn comma-separated counties into lists
        .str.replace(r",$", "", regex=True).str.split(',')
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
            raise FileExistsError(
                f'{dir_path} exists and clobber is {clobber}')
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


def clean_merge_asof(
    left,
    right,
    left_on="report_date",
    right_on="report_date",
    by={},
):
    """
    Merge two dataframes having different time report_date frequencies.

    We often need to bring together data which is reported on a monthly basis,
    and entity attributes that are reported on an annual basis.  The
    :func:`pandas.merge_asof` is designed to do this, but requires that
    dataframes are sorted by the merge keys (``left_on``, ``right_on``, and
    ``by.keys()`` here). We also need to make sure that all merge keys have
    identical data types in the two dataframes (e.g. ``plant_id_eia`` needs to
    be a nullable integer in both dataframes, not a python int in one, and a
    nullable :func:`pandas.Int64Dtype` in the other).  Note that
    :func:`pandas.merge_asof` performs a left merge, so the higher frequency
    dataframe **must** be the left dataframe.

    We also force both ``left_on`` and ``right_on`` to be a Datetime using
    :func:`pandas.to_datetime` to allow merging dataframes having integer years
    with those having datetime columns.

    Because :func:`pandas.merge_asof` searches backwards for the first matching
    date, this function only works if the less granular dataframe uses the
    convention of reporting the first date in the time period for which it
    reports. E.g. annual dataframes need to have January 1st as the date. This
    is what happens by defualt if only a year or year-month are provided to
    :func:`pandas.to_datetime` as strings.

    Args:
        left (pandas.DataFrame): The higher frequency "data" dataframe.
            Typically monthly in our use cases. E.g. ``generation_eia923``. Must
            contain ``report_date`` and any columns specified in the ``by``
            argument.
        right (pandas.DataFrame): The lower frequency "attribute" dataframe.
            Typically annual in our uses cases. E.g. ``generators_eia860``. Must
            contain ``report_date`` and any columns specified in the ``by``
            argument.
        left_on (str): Column in ``left`` to merge on using merge_asof. Default
            is ``report_date``. Must be convertible to a Datetime using
            :func:`pandas.to_datetime`
        right_on (str): Column in ``right`` to merge on using merge_asof.
            Default is ``report_date``. Must be convertible to a Datetime using
            :func:`pandas.to_datetime`
        by (dict): A dictionary enumerating any columns to merge on other than
            ``report_date``. Typically ID columns like ``plant_id_eia``,
            ``generator_id`` or ``boiler_id``. The keys of the dictionary are
            the names of the columns, and the values are their data source, as
            defined in :mod:`pudl.constants` (e.g. ``ferc1`` or ``eia``). The
            data source is used to look up the column's canonical data type.

    Returns:
        pandas.DataFrame: Merged contents of left and right input dataframes.
        Will be sorted by ``left_on`` and any columns specified in ``by``. See
        documentation for :func:`pandas.merge_asof` to understand how this kind
        of merge works.

    Raises:
        ValueError: if ``left_on`` or ``right_on`` columns are missing from
            their respective input dataframes.
        ValueError: if any of the labels referenced in ``by`` are missing from
            either the left or right dataframes.

    """
    # Make sure we've got all the required inputs...
    if left_on not in left.columns:
        raise ValueError(f"Left dataframe has no column {left_on}.")
    if right_on not in right.columns:
        raise ValueError(f"Right dataframe has no {right_on}.")
    missing_left_cols = [col for col in by if col not in left.columns]
    if missing_left_cols:
        raise ValueError(f"Left dataframe is missing {missing_left_cols}.")
    missing_right_cols = [col for col in by if col not in right.columns]
    if missing_right_cols:
        raise ValueError(f"Left dataframe is missing {missing_right_cols}.")

    def cleanup(df, on, by):
        df = df.astype(get_pudl_dtypes(by))
        df.loc[:, on] = pd.to_datetime(df[on])
        df = df.sort_values([on] + list(by.keys()))
        return df

    return pd.merge_asof(
        cleanup(df=left, on=left_on, by=by),
        cleanup(df=right, on=right_on, by=by),
        left_on=left_on,
        right_on=right_on,
        by=list(by.keys()),
        tolerance=pd.Timedelta("365 days")  # Should never match across years.
    )


def get_pudl_dtype(col, data_source):
    """Look up a column's canonical data type based on its PUDL data source."""
    return pudl.constants.column_dtypes[data_source][col]


def get_pudl_dtypes(col_source_dict):
    """Look up canonical PUDL data types for columns based on data sources."""
    return {
        col: get_pudl_dtype(col, col_source_dict[col])
        for col in col_source_dict
    }


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


def simplify_strings(df, columns):
    """
    Simplify the strings contained in a set of dataframe columns.

    Performs several operations to simplify strings for comparison and parsing purposes.
    These include removing Unicode control characters, stripping leading and trailing
    whitespace, using lowercase characters, and compacting all internal whitespace to a
    single space.

    Leaves null values unaltered. Casts other values with astype(str).

    Args:
        df (pandas.DataFrame): DataFrame whose columns are being cleaned up.
        columns (iterable): The labels of the string columns to be simplified.

    Returns:
        pandas.DataFrame: The whole DataFrame that was passed in, with
        the string columns cleaned up.

    """
    out_df = df.copy()
    for col in columns:
        if col in out_df.columns:
            out_df.loc[out_df[col].notnull(), col] = (
                out_df.loc[out_df[col].notnull(), col]
                .astype(str)
                .str.replace(r"[\x00-\x1f\x7f-\x9f]", "", regex=True)
                .str.strip()
                .str.lower()
                .str.replace(r'\s+', ' ', regex=True)
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
            str.replace(r'\s+', ' ', regex=True)
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
        base_month_regex = f'^{base}{month_regex}'
        month_col = list(df.filter(regex=base_month_regex).columns)
        if not len(month_col) == 1:
            raise AssertionError()
        month_col = month_col[0]
        base_year_regex = f'^{base}{year_regex}'
        year_col = list(df.filter(regex=base_year_regex).columns)
        if not len(year_col) == 1:
            raise AssertionError()
        year_col = year_col[0]
        date_col = f'{base}_date'
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


def fix_leading_zero_gen_ids(df):
    """
    Remove leading zeros from EIA generator IDs which are numeric strings.

    If the DataFrame contains a column named ``generator_id`` then that column
    will be cast to a string, and any all numeric value with leading zeroes
    will have the leading zeroes removed. This is necessary because in some
    but not all years of data, some of the generator IDs are treated as integers
    in the Excel spreadsheets published by EIA, so the same generator may show
    up with the ID "0001" and "1" in different years.

    Alphanumeric generator IDs with leadings zeroes are not affected, as we
    found no instances in which an alphanumeric generator ID appeared both with
    and without leading zeroes.

    Args:
        df (pandas.DataFrame): DataFrame, presumably containing a column named
            generator_id (otherwise no action will be taken.)

    Returns:
        pandas.DataFrame

    """
    if "generator_id" in df.columns:
        fixed_generator_id = (
            df["generator_id"]
            .astype(str)
            .apply(lambda x: re.sub(r'^0+(\d+$)', r'\1', x))
        )
        num_fixes = len(
            df.loc[df["generator_id"].astype(str) != fixed_generator_id])
        logger.debug("Fixed %s EIA generator IDs with leading zeros.", num_fixes)
        df = (
            df.drop("generator_id", axis="columns")
            .assign(generator_id=fixed_generator_id)
        )
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

    df[date_col] = pd.to_datetime({'year': year,
                                   'month': month,
                                   'day': day})
    cols_to_drop = [x for x in [
        day_col, year_col, month_col] if x in df.columns]
    df.drop(cols_to_drop, axis="columns", inplace=True)

    return df


def fix_eia_na(df):
    """
    Replace common ill-posed EIA NA spreadsheet values with np.nan.

    Currently replaces empty string, single decimal points with no numbers,
    and any single whitespace character with np.nan.

    Args:
        df (pandas.DataFrame): The DataFrame to clean.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.

    """
    bad_na_regexes = [
        r'^\.$',  # Nothing but a decimal point
        r'^\s$',  # A single whitespace character
        r'^$',    # The empty string
    ]
    return df.replace(
        to_replace=bad_na_regexes,
        value=np.nan,
        regex=True
    )


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
        str.replace(r'[^0-9a-zA-Z]+', ' ', regex=True).
        str.strip().
        str.lower().
        str.replace(r'\s+', ' ', regex=True).
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
    # grab all of the non boolean columns
    non_bool_cols = {col: col_dtype for col, col_dtype
                     in col_dtypes.items()
                     if col_dtype != pd.BooleanDtype()}
    # Grab only the string columns...
    string_cols = {col: col_dtype for col, col_dtype
                   in col_dtypes.items()
                   if col_dtype == pd.StringDtype()}

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
        df.astype(non_bool_cols)
        .astype(bool_cols)
        .replace(to_replace="nan", value={col: pd.NA for col in string_cols})
        .replace(to_replace="<NA>", value={col: pd.NA for col in string_cols})
    )

    # Zip codes are highly coorelated with datatype. If they datatype gets
    # converted at any point it may mess up the accuracy of the data. For
    # example: 08401.0 or 8401 are both incorrect versions of 080401 that a
    # simple datatype conversion cannot fix. For this reason, we use the
    # zero_pad_zips function.
    if any('zip_code' for col in df.columns):
        zip_cols = [col for col in df.columns if 'zip_code' in col]
        for col in zip_cols:
            if '4' in col:
                df.loc[:, col] = zero_pad_zips(df[col], 4)
            else:
                df.loc[:, col] = zero_pad_zips(df[col], 5)

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

    For a given dataframe with a ``report_date`` column, generate a monthly
    rolling average and use this rolling average to impute missing values.

    Args:
        df (pandas.DataFrame): Original dataframe. Must have group_cols
            column, a data_col column and a ``report_date`` column.
        group_cols (iterable): a list of columns to groupby.
        data_col (str): the name of the data column.
        window (int): window from :func:`pandas.Series.rolling`.
        kwargs : Additional arguments to pass to
            :func:`pandas.Series.rolling`.

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
        kwargs : Additional arguments to pass to
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
        pandas.DataFrame: dataframe containing only ``cols`` and
        ``new_count_col_name``.

    """
    return (
        df.assign(count_me=1)
        .groupby(cols)
        .count_me.count()
        .reset_index()
        .rename(columns={'count_me': new_count_col_name})
    )


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
            str.replace(r'\s+', '_', regex=True)
        )
    return df


def zero_pad_zips(zip_series, n_digits):
    """
    Retain prefix zeros in zipcodes.

    Args:
        zip_series (pd.Series) : series containing the zipcode values.
        n_digits(int) : zipcode length (likely 4 or 5 digits).

    Returns:
        pandas.Series: a series containing zipcodes with their prefix zeros
        intact and invalid zipcodes rendered as na.

    """
    # Add preceeding zeros where necessary and get rid of decimal zeros
    def get_rid_of_decimal(series):
        return series.str.replace(r'[\.]+\d*', '', regex=True)

    zip_series = (
        zip_series
        .astype(pd.StringDtype())
        .replace('nan', np.nan)
        .fillna("0")
        .pipe(get_rid_of_decimal)
        .str.zfill(n_digits)
        .replace({n_digits * "0": pd.NA})  # All-zero Zip codes aren't valid.
    )
    return zip_series


def iterate_multivalue_dict(**kwargs):
    """Make dicts from dict with main dict key and one value of main dict."""
    single_valued = {k: v for k,
                     v in kwargs.items()
                     if not (isinstance(v, list) or isinstance(v, tuple))}

    # Transform multi-valued {k: vlist} into {k1: [{k1: v1}, {k1: v2}, ...], k2: [...], ...}
    multi_valued = {k: [{k: v} for v in vlist]
                    for k, vlist in kwargs.items()
                    if (isinstance(vlist, list) or isinstance(vlist, tuple))}

    for value_assignments in itertools.product(*multi_valued.values()):
        result = dict(single_valued)
        for k_v in value_assignments:
            result.update(k_v)
        yield result


def get_working_eia_dates():
    """Get all working EIA dates as a DatetimeIndex."""
    dates = pd.DatetimeIndex([])
    for dataset_name, dataset in pc.working_partitions.items():
        if 'eia' in dataset_name:
            for name, partition in dataset.items():
                if name == 'years':
                    dates = dates.append(
                        pd.to_datetime(partition, format='%Y'))
                if name == 'year_month':
                    dates = dates.append(pd.DatetimeIndex(
                        [pd.to_datetime(partition)]))
    return dates
