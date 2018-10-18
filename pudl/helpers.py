"""General utility functions that are used in a variety of contexts."""

from functools import partial

import pandas as pd

# This is a little abbreviated function that allows us to propagate the NA
# values through groupby aggregations, rather than using inefficient lambda
# functions in each one.
sum_na = partial(pd.Series.sum, skipna=False)


def is_annual(df_year, year_col='report_date'):
    """Determine whether dataframe is consistent with yearly reporting."""
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


def merge_on_date_year(df_date, df_year, on=[], how='inner',
                       date_col='report_date',
                       year_col='report_date'):
    """
    Merge two dataframes based on a shared year.

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
    -----
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
    --------
        merged: a dataframe with a date column, but no year columns, and only
            one copy of any shared columns that were not part of the list of
            columns to be merged on.  The values from df1 are the ones which
            are retained for any shared, non-merging columns.

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
    """
    # Generate a list of all the columns in the dataframe that are not
    # included in cols
    data_cols = [c for c in df.columns.tolist() if c not in cols]
    data_cols.sort()
    organized_cols = cols + data_cols
    return df[organized_cols]


def extend_annual(df, date_col='report_date', start_date=None, end_date=None):
    """
    Extend the time range in a dataframe by duplicating first and last years.

    Takes the earliest year's worth of annual data and uses it to create
    earlier years by duplicating it, and changing the year.  Similarly,
    extends a dataset into the future by duplicating the last year's records.

    This is primarily used to extend the EIA860 data about utilities, plants,
    and generators, so that we can analyze a larger set of EIA923 data. EIA923
    data has been integrated a bit further back, and the EIA860 data has a year
    long lag in being released.
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
    """Strip & compact whitespace, lowercase listed DataFrame columns."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.strip().str.lower().str.replace(r'\s+', ' ')

    return df
