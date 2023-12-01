"""General utility functions that are used in a variety of contexts.

The functions in this module are used in various stages of the ETL and post-etl
processes. They are usually not dataset specific, but not always. If a function is
designed to be used as a general purpose tool, applicable in multiple scenarios, it
should probably live here. There are lost of transform type functions in here that help
with cleaning and restructing dataframes.
"""
import importlib.resources
import itertools
import pathlib
import re
import shutil
from collections import defaultdict
from collections.abc import Generator, Iterable
from functools import partial
from io import BytesIO
from typing import Any, Literal

import addfips
import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa
from dagster import AssetKey, AssetsDefinition, AssetSelection, SourceAsset
from pandas._libs.missing import NAType

import pudl.logging_helpers
from pudl.metadata.fields import apply_pudl_dtypes, get_pudl_dtypes

sum_na = partial(pd.Series.sum, skipna=False)
"""A sum function that returns NA if the Series includes any NA values.

In many of our aggregations we need to override the default behavior of treating NA
values as if they were zero. E.g. when calculating the heat rates of generation units,
if there are some months where fuel consumption is reported as NA, but electricity
generation is reported normally, then the fuel consumption for the year needs to be NA,
otherwise we'll get unrealistic heat rates.
"""

logger = pudl.logging_helpers.get_logger(__name__)


def label_map(
    df: pd.DataFrame,
    from_col: str = "code",
    to_col: str = "label",
    null_value: str | NAType = pd.NA,
) -> defaultdict[str, str | NAType]:
    """Build a mapping dictionary from two columns of a labeling / coding dataframe.

    These dataframes document the meanings of the codes that show up in much of the
    originally reported data. They're defined in :mod:`pudl.metadata.codes`.  This
    function is mostly used to build maps that can translate the hard to understand
    short codes into longer human-readable codes.

    Args:
        df: The coding / labeling dataframe. Must contain columns ``from_col``
            and ``to_col``.
        from_col: Label of column containing the existing codes to be replaced.
        to_col: Label of column containing the new codes to be swapped in.
        null_value: Defualt (Null) value to map to when a value which doesn't
            appear in ``from_col`` is encountered.


    Returns:
        A mapping dictionary suitable for use with :meth:`pandas.Series.map`.
    """
    return defaultdict(
        lambda: null_value,
        df.loc[:, [from_col, to_col]]
        .drop_duplicates(subset=[from_col])
        .to_records(index=False),
    )


def find_new_ferc1_strings(
    table: str,
    field: str,
    strdict: dict[str, list[str]],
    ferc1_engine: sa.Engine,
) -> set[str]:
    """Identify as-of-yet uncategorized freeform strings in FERC Form 1.

    Args:
        table: Name of the FERC Form 1 DB to search.
        field: Name of the column in that table to search.
        strdict: A string cleaning dictionary. See
            e.g. `pudl.transform.ferc1.FUEL_UNIT_STRINGS`
        ferc1_engine: SQL Alchemy DB connection engine for the FERC Form 1 DB.

    Returns:
        Any string found in the searched table + field that was not part of any of
        categories enumerated in strdict.
    """
    all_strings = set(
        pd.read_sql(f"SELECT {field} FROM {table};", ferc1_engine).pipe(  # noqa: S608
            simplify_strings, columns=[field]
        )[field]
    )
    old_strings = set.union(*[set(strings) for strings in strdict.values()])
    return all_strings.difference(old_strings)


def find_foreign_key_errors(dfs: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    """Report foreign key violations from a dictionary of dataframes.

    The database schema to check against is generated based on the names of the
    dataframes (keys of the dictionary) and the PUDL metadata structures.

    Args:
        dfs: Keys are table names, and values are dataframes ready for loading
            into the SQLite database.

    Returns:
        A list of dictionaries, each one pertains to a single database table
        in which a foreign key constraint violation was found, and it includes
        the table name, foreign key definition, and the elements of the
        dataframe that violated the foreign key constraint.
    """
    import pudl.metadata.classes

    package = pudl.metadata.classes.Package.from_resource_ids(
        resource_ids=tuple(sorted(dfs))
    )
    errors = []
    for resource in package.resources:
        for foreign_key in resource.schema.foreign_keys:
            x = dfs[resource.name][foreign_key.fields]
            y = dfs[foreign_key.reference.resource][foreign_key.reference.fields]
            ncols = x.shape[1]
            idx = range(ncols)
            xx, yy = x.set_axis(idx, axis=1), y.set_axis(idx, axis=1)
            if ncols == 1:
                # Faster check for single-field foreign key
                invalid = ~(xx[0].isin(yy[0]) | xx[0].isna())
            else:
                invalid = ~(
                    pd.concat([yy, xx]).duplicated().iloc[len(yy) :]
                    | xx.isna().any(axis=1)
                )
            if invalid.any():
                errors.append(
                    {
                        "resource": resource.name,
                        "foreign_key": foreign_key,
                        "invalid": x[invalid],
                    }
                )
    return errors


def download_zip_url(
    url: str, save_path: pathlib.Path, chunk_size: int = 128, timeout: float = 9.05
) -> None:
    """Download and save a Zipfile locally.

    Useful for acquiring and storing non-PUDL data locally.

    Args:
        url: The URL from which to download the Zipfile
        save_path: The location to save the file.
        chunk_size: Data chunk in bytes to use while downloading.
        timeout: Time to wait for the server to accept a connection.
            See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts

    Returns:
        None
    """
    # This is a temporary hack to avoid being filtered as a bot:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    r = requests.get(url, stream=True, headers=headers, timeout=timeout)
    with save_path.open(mode="wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def add_fips_ids(
    df: pd.DataFrame,
    state_col: str = "state",
    county_col: str = "county",
    vintage: int = 2015,
) -> pd.DataFrame:
    """Add State and County FIPS IDs to a dataframe.

    To just add State FIPS IDs, make county_col = None.
    """
    # force the columns to be the nullable string types so we have a consistent
    # null value to filter out before feeding to addfips
    df = df.astype({state_col: pd.StringDtype()})
    if county_col:
        df = df.astype({county_col: pd.StringDtype()})
    af = addfips.AddFIPS(vintage=vintage)
    # Lookup the state and county FIPS IDs and add them to the dataframe:
    df["state_id_fips"] = df.apply(
        lambda x: (
            af.get_state_fips(state=x[state_col]) if pd.notnull(x[state_col]) else pd.NA
        ),
        axis=1,
    )

    # force the code columns to be nullable strings - the leading zeros are
    # important
    df = df.astype({"state_id_fips": pd.StringDtype()})

    logger.info(
        f"Assigned state FIPS codes for "
        f"{len(df[df.state_id_fips.notnull()])/len(df):.2%} of records."
    )
    if county_col:
        df["county_id_fips"] = df.apply(
            lambda x: (
                af.get_county_fips(state=x[state_col], county=x[county_col])
                if pd.notnull(x[county_col]) and pd.notnull(x[state_col])
                else pd.NA
            ),
            axis=1,
        )
        # force the code columns to be nullable strings - the leading zeros are
        # important
        df = df.astype({"county_id_fips": pd.StringDtype()})
        logger.info(
            f"Assigned county FIPS codes for "
            f"{len(df[df.county_id_fips.notnull()])/len(df):.2%} of records."
        )
    return df


def clean_eia_counties(
    df: pd.DataFrame,
    fixes: pd.DataFrame,
    state_col: str = "state",
    county_col: str = "county",
) -> pd.DataFrame:
    """Replace non-standard county names with county nmes from US Census."""
    df = df.copy()
    df[county_col] = (
        df[county_col]
        .str.strip()
        # Condense multiple whitespace chars.
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"^St ", "St. ", regex=True)  # Standardize abbreviation.
        # Standardize abbreviation.
        .str.replace(r"^Ste ", "Ste. ", regex=True)
        .str.replace("Kent & New Castle", "Kent, New Castle")  # Two counties
        # Fix ordering, remove comma
        .str.replace("Borough, Kodiak Island", "Kodiak Island Borough")
        # Turn comma-separated counties into lists
        .str.replace(r",$", "", regex=True)
        .str.split(",")
    )
    # Create new records for each county in a multi-valued record
    df = df.explode(county_col)
    df[county_col] = df[county_col].str.strip()
    # Yellowstone county is in MT, not WY
    df.loc[
        (df[state_col] == "WY") & (df[county_col] == "Yellowstone"), state_col
    ] = "MT"
    # Replace individual bad county names with identified correct names in fixes:
    for fix in fixes.itertuples():
        state_mask = df[state_col] == fix.state
        county_mask = df[county_col] == fix.eia_county
        df.loc[state_mask & county_mask, county_col] = fix.fips_county
    return df


def oob_to_nan(
    df: pd.DataFrame,
    cols: list[str],
    lb: float | None = None,
    ub: float | None = None,
) -> pd.DataFrame:
    """Set non-numeric values and those outside of a given range to NaN.

    Args:
        df: The dataframe containing values to be altered.
        cols: Labels of the columns whose values are to be changed.
        lb: Lower bound, below which values are set to NaN. If None, don't use a lower
            bound.
        ub: Upper bound, below which values are set to NaN. If None, don't use an upper
            bound.

    Returns:
        The altered DataFrame.
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


def oob_to_nan_with_dependent_cols(
    df: pd.DataFrame,
    cols: list[str],
    dependent_cols: list[str],
    lb: float | None = None,
    ub: float | None = None,
) -> pd.DataFrame:
    """Call :func:`oob_to_nan` and additionally nullify any derived columns.

    Set values in ``cols`` to NaN if values are non-numeric or outside of a
    given range. The corresponding values in ``dependent_cols`` are then set
    to NaN. ``dependent_cols`` should be columns derived from one or multiple
    of the columns in ``cols``.

    Args:
        df: The dataframe containing values to be altered.
        cols: Labels of the columns whose values are to be changed.
        dependent_cols: Labels of the columns whose corresponding values should also be
            nullified. Columns are derived from one or multiple of the columns in
            ``cols``.
        lb: Lower bound, below which values are set to NaN. If None, don't use a lower
            bound.
        ub: Upper bound, below which values are set to NaN. If None, don't use an upper
            bound.

    Returns:
        The altered DataFrame.
    """
    out_df = oob_to_nan(df, cols, lb, ub)
    out_df.loc[out_df[cols].isnull().any(axis=1), dependent_cols] = np.nan
    return out_df


def prep_dir(dir_path: str | pathlib.Path, clobber: bool = False) -> pathlib.Path:
    """Create (or delete and recreate) a directory.

    Args:
        dir_path: path to the directory that you are trying to clean and prepare.
        clobber: If True and dir_path exists, it will be removed and replaced with a
            new, empty directory.

    Raises:
        FileExistsError: if a file or directory already exists at dir_path.

    Returns:
        Path to the created directory.
    """
    dir_path = pathlib.Path(dir_path)
    if dir_path.exists():
        if clobber:
            shutil.rmtree(dir_path)
        else:
            raise FileExistsError(f"{dir_path} exists and clobber is {clobber}")
    dir_path.mkdir(parents=True)
    return dir_path


def is_doi(doi: str) -> bool:
    """Determine if a string is a valid digital object identifier (DOI).

    Function simply checks whether the offered string matches a regular
    expresssion -- it doesn't check whether the DOI is actually registered
    with the relevant authority.

    Args:
        doi: String to validate.

    Returns:
        True if doi matches the regex for valid DOIs, False otherwise.
    """
    doi_regex = re.compile(
        r"(doi:\s*|(?:https?://)?(?:dx\.)?doi\.org/)?(10\.\d+(.\d+)*/.+)$",
        re.IGNORECASE | re.UNICODE,
    )

    return bool(re.match(doi_regex, doi))


def convert_col_to_datetime(df: pd.DataFrame, date_col_name: str) -> pd.DataFrame:
    """Convert a non-datetime column in a dataframe to a datetime64[s].

    If the column isn't a datetime, it needs to be converted to a string type
    first so that integer years are formatted correctly.

    Args:
        df: Dataframe with column to convert.
        date_col_name: name of the datetime column to convert.

    Returns:
        Dataframe with the converted datetime column.
    """
    if not pd.api.types.is_datetime64_dtype(df[date_col_name]):
        logger.warning(
            f"{date_col_name} is {df[date_col_name].dtype} column. "
            "Converting to datetime64[ns]."
        )
        df[date_col_name] = pd.to_datetime(df[date_col_name].astype("string"))
    return df


def full_timeseries_date_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: list[str],
    left_date_col: str = "report_date",
    right_date_col: str = "report_date",
    new_date_col: str = "report_date",
    date_on: list[str] = ["year"],
    how: Literal["inner", "outer", "left", "right", "cross"] = "inner",
    report_at_start: bool = True,
    freq: str = "MS",
    **kwargs,
) -> pd.DataFrame:
    """Merge dataframes with different date frequencies and expand to a full timeseries.

    Arguments: see arguments for ``date_merge`` and ``expand_timeseries``
    """
    out = date_merge(
        left=left,
        right=right,
        left_date_col=left_date_col,
        right_date_col=right_date_col,
        new_date_col=new_date_col,
        on=on,
        date_on=date_on,
        how=how,
        report_at_start=report_at_start,
        **kwargs,
    )
    out = expand_timeseries(
        df=out,
        date_col=new_date_col,
        freq=freq,
        key_cols=on,
    )
    return out


def _add_suffix_to_date_on(date_on):
    """Check date_on list is valid and add _temp_for_merge suffix."""
    if date_on is None:
        date_on = ["year"]
    date_on_suffix = []
    for col in date_on:
        if col not in ["year", "month", "quarter", "day"]:
            raise AssertionError(
                logger.error(f"{col} is not a valid string in date_on column list.")
            )
        date_on_suffix.append(col + "_temp_for_merge")
    return date_on_suffix


def date_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: list[str],
    left_date_col: str = "report_date",
    right_date_col: str = "report_date",
    new_date_col: str = "report_date",
    date_on: list[str] = None,
    how: Literal["inner", "outer", "left", "right", "cross"] = "inner",
    report_at_start: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """Merge two dataframes that have different report date frequencies.

    We often need to bring together data that is reported at different
    temporal granularities e.g. monthly basis versus annual basis. This function
    acts as a wrapper on a pandas merge to allow merging at different temporal
    granularities. The date columns of both dataframes are separated into
    year, quarter, month, and day columns. Then, the dataframes are merged according
    to ``how`` on the columns specified by the ``on`` and ``date_on`` argument,
    which list the new temporal columns to merge on as well any additional shared columns.
    Finally, the datetime column is reconstructed in the output dataframe and
    named according to the ``new_date_col`` parameter.

    Args:
        left: The left dataframe in the merge. Typically monthly in our use
            cases if doing a left merge E.g. ``generation_eia923``.
            Must contain columns specified by ``left_date_col`` and
            ``on`` argument.
        right: The right dataframe in the merge. Typically annual in our uses
            cases if doing a left merge E.g. ``generators_eia860``.
            Must contain columns specified by ``right_date_col`` and ``on`` argument.
        on: The columns to merge on that are shared between both
            dataframes. Typically ID columns like ``plant_id_eia``, ``generator_id``
            or ``boiler_id``.
        left_date_col: Column in ``left`` containing datetime like data. Default is
            ``report_date``. Must be a Datetime or convertible to a Datetime using
            :func:`pandas.to_datetime`
        right_date_col: Column in ``right`` containing datetime like data. Default is
            ``report_date``. Must be a Datetime or convertible to a Datetime using
            :func:`pandas.to_datetime`.
        new_date_col: Name of the reconstructed datetime column in the output dataframe.
        date_on: The temporal columns to merge on. Values in this list
            of columns must be [``year``, ``quarter``, ``month``, ``day``].
            E.g. if a monthly reported dataframe is being merged onto a daily reported
            dataframe, then the merge would be performed on ``["year", "month"]``.
            If one of these temporal columns already exists in the dataframe it will not
            be clobbered by the merge, as the suffix "_temp_for_merge" is added when
            expanding the datetime column into year, quarter, month, and day. By default,
            `date_on` will just include year.
        how: How the dataframes should be merged. See :func:`pandas.DataFrame.merge`.
        report_at_start: Whether the data in the dataframe whose report date is not being
            kept in the merged output (in most cases the less frequently reported dataframe)
            is reported at the start or end of the time period e.g. January 1st
            for annual data.
        kwargs : Additional arguments to pass to :func:`pandas.DataFrame.merge`.


    Returns:
        Merged contents of left and right input dataframes.

    Raises:
        ValueError: if ``left_date_col`` or ``right_date_col`` columns are missing from their
            respective input dataframes.
        ValueError: if any of the labels referenced in ``on`` are missing from either
            the left or right dataframes.
    """

    def separate_date_cols(df, date_col_name, date_on):
        out_df = df.copy()
        out_df.loc[:, date_col_name] = pd.to_datetime(out_df[date_col_name])
        if "year_temp_for_merge" in date_on:
            out_df.loc[:, "year_temp_for_merge"] = out_df[date_col_name].dt.year
        if "quarter_temp_for_merge" in date_on:
            out_df.loc[:, "quarter_temp_for_merge"] = out_df[date_col_name].dt.quarter
        if "month_temp_for_merge" in date_on:
            out_df.loc[:, "month_temp_for_merge"] = out_df[date_col_name].dt.month
        if "day_temp_for_merge" in date_on:
            out_df.loc[:, "day_temp_for_merge"] = out_df[date_col_name].dt.day
        return out_df

    right = convert_col_to_datetime(right, right_date_col)
    left = convert_col_to_datetime(left, left_date_col)
    date_on = _add_suffix_to_date_on(date_on)
    right = separate_date_cols(right, right_date_col, date_on)
    left = separate_date_cols(left, left_date_col, date_on)
    merge_cols = date_on + on
    out = pd.merge(left, right, on=merge_cols, how=how, **kwargs)

    suffixes = ["", ""]
    if left_date_col == right_date_col:
        suffixes = kwargs.get("suffixes", ["_x", "_y"])
    # reconstruct the new report date column and clean up columns
    left_right_date_col = [left_date_col + suffixes[0], right_date_col + suffixes[1]]
    if report_at_start:
        # keep the later of the two report dates when determining
        # the new report date for each row
        reconstructed_date = out[left_right_date_col].max(axis=1)
    else:
        # keep the earlier of the two report dates
        reconstructed_date = out[left_right_date_col].min(axis=1)
    out = out.drop(left_right_date_col + date_on, axis=1)
    out.insert(loc=0, column=new_date_col, value=reconstructed_date)
    return out


def expand_timeseries(
    df: pd.DataFrame,
    key_cols: list[str],
    date_col: str = "report_date",
    freq: str = "MS",
    fill_through_freq: Literal["year", "month", "day"] = "year",
) -> pd.DataFrame:
    """Expand a dataframe to a include a full time series at a given frequency.

    This function adds a full timeseries to the given dataframe for each group
    of columns specified by ``key_cols``. The data in the timeseries will be filled
    with the next previous chronological observation for a group of primary key columns
    specified by ``key_cols``.

    Args:
        df: The dataframe to expand. Must have ``date_col`` in columns.
        key_cols: Column names of the non-date primary key columns in the dataframe.
            The resulting dataframe will have a full timeseries expanded for each
            unique group of these ID columns that are present in the dataframe.
        date_col: Name of the datetime column being expanded into a full timeseries.
        freq: The frequency of the time series to expand the data to.
            See :ref:`here <timeseries.offset_aliases>` for a list of
            frequency aliases.
        fill_through_freq: The frequency in which to fill in the data through. For
            example, if equal to "year" the data will be filled in through the end of
            the last reported year for each grouping of `key_cols`. Valid frequencies
            are only "year", "month", or "day".

    Raises:
        ValueError: if ``fill_through_freq`` is not one of "year", "month" or "day".
    """
    try:
        pd.tseries.frequencies.to_offset(freq)
    except ValueError:
        logger.exception(
            f"Frequency string {freq} is not valid. \
            See Pandas Timeseries Offset Aliases docs for valid strings."
        )

    # For each group of ID columns add a dummy record with the date column
    # equal to one increment higher than the last record in the group for the
    # desired fill_through_freq.
    # This allows records to be filled through the end of the last reported period
    # and then this dummy record is dropped
    df = convert_col_to_datetime(df, date_col)
    end_dates = df.groupby(key_cols).agg({date_col: "max"})
    if fill_through_freq == "year":
        end_dates.loc[:, date_col] = pd.to_datetime(
            {
                "year": end_dates[date_col].dt.year + 1,
                "month": 1,
                "day": 1,
            }
        )
    elif fill_through_freq == "month":
        end_dates.loc[:, date_col] = pd.to_datetime(
            {
                "year": end_dates[date_col].dt.year,
                "month": end_dates[date_col].dt.month + 1,
                "day": 1,
            }
        )
    elif fill_through_freq == "day":
        end_dates.loc[:, date_col] = pd.to_datetime(
            {
                "year": end_dates[date_col].dt.year,
                "month": end_dates[date_col].dt.month,
                "day": end_dates[date_col].dt.day + 1,
            }
        )
    else:
        raise ValueError(
            f"{fill_through_freq} is not a valid frequency to fill through."
        )
    end_dates["drop_row"] = True
    df = (
        pd.concat([df, end_dates.reset_index()])
        .set_index(date_col)
        .groupby(key_cols)
        .resample(freq)
        .ffill()
        .drop(key_cols, axis=1)
        .reset_index()
    )
    return (
        df[df.drop_row.isnull()]
        .drop(columns="drop_row")
        .reset_index(drop=True)
        .pipe(apply_pudl_dtypes)
    )


def organize_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Organize columns into key ID & name fields & alphabetical data columns.

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
    # Generate a list of all the columns in the dataframe that are not included in cols
    data_cols = sorted(c for c in df.columns.tolist() if c not in cols)
    organized_cols = cols + data_cols
    return df[organized_cols]


def simplify_strings(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Simplify the strings contained in a set of dataframe columns.

    Performs several operations to simplify strings for comparison and parsing purposes.
    These include removing Unicode control characters, stripping leading and trailing
    whitespace, using lowercase characters, and compacting all internal whitespace to a
    single space.

    Leaves null values unaltered. Casts other values with astype(str).

    Args:
        df: DataFrame whose columns are being cleaned up.
        columns: The labels of the string columns to be simplified.

    Returns:
        The whole DataFrame that was passed in, with the string columns cleaned up.
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
                .str.replace(r"\s+", " ", regex=True)
            )
    return out_df


def cleanstrings_series(
    col: pd.Series,
    str_map: dict[str, list[str]],
    unmapped: str | None = None,
    simplify: bool = True,
) -> pd.Series:
    """Clean up the strings in a single column/Series.

    Args:
        col: A pandas Series, typically a single column of a
            dataframe, containing the freeform strings that are to be cleaned.
        str_map: A dictionary of lists of strings, in which the keys are
            the simplified canonical strings, with which each string found in
            the corresponding list will be replaced.
        unmapped: A value with which to replace any string found in col
            that is not found in one of the lists of strings in map. Typically
            the null string ''. If None, these strings will not be replaced.
        simplify: If True, strip and compact whitespace, and lowercase
            all strings in both the list of values to be replaced, and the
            values found in col. This can reduce the number of strings that
            need to be kept track of.

    Returns:
        The cleaned up Series / column, suitable for replacing the original messy column
        in a :class:`pandas.DataFrame`.
    """
    if simplify:
        col = (
            col.astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
        )
        for k in str_map:
            str_map[k] = [re.sub(r"\s+", " ", s.lower().strip()) for s in str_map[k]]

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


def cleanstrings(
    df: pd.DataFrame,
    columns: list[str],
    stringmaps: list[dict[str, list[str]]],
    unmapped: str | None = None,
    simplify: bool = True,
) -> pd.DataFrame:
    """Consolidate freeform strings in several dataframe columns.

    This function will consolidate freeform strings found in `columns` into simplified
    categories, as defined by `stringmaps`. This is useful when a field contains many
    different strings that are really meant to represent a finite number of categories,
    e.g. a type of fuel. It can also be used to create simplified categories that apply
    to similar attributes that are reported in various data sources from different
    agencies that use their own taxonomies.

    The function takes and returns a pandas.DataFrame, making it suitable for use with
    the :func:`pandas.DataFrame.pipe` method in a chain.

    Args:
        df: the DataFrame containing the string columns to be cleaned up.
        columns: a list of string column labels found in the column index of df. These
            are the columns that will be cleaned.
        stringmaps: a list of dictionaries. The keys of these dictionaries are strings,
            and the values are lists of strings. Each dictionary in the list corresponds
            to a column in columns. The keys of the dictionaries are the values with
            which every string in the list of values will be replaced.
        unmapped: the value with which strings not found in the stringmap dictionary
            will be replaced. Typically the null string ''. If None, then strings found
            in the columns but not in the stringmap will be left unchanged.
        simplify: If true, strip whitespace, remove duplicate whitespace, and force
            lower-case on both the string map and the values found in the columns to be
            cleaned. This can reduce the overall number of string values that need to be
            tracked.

    Returns:
        The function returns a new DataFrame containing the cleaned strings.
    """
    out_df = df.copy()
    for col, str_map in zip(columns, stringmaps):
        out_df[col] = cleanstrings_series(
            out_df[col], str_map, unmapped=unmapped, simplify=simplify
        )

    return out_df


def fix_int_na(
    df: pd.DataFrame,
    columns: list[str],
    float_na: float = np.nan,
    int_na: int = -1,
    str_na: str = "",
) -> pd.DataFrame:
    """Convert NA containing integer columns from float to string.

    Numpy doesn't have a real NA value for integers. When pandas stores integer data
    which has NA values, it thus upcasts integers to floating point values, using np.nan
    values for NA. However, in order to dump some of our dataframes to CSV files for use
    in data packages, we need to write out integer formatted numbers, with empty strings
    as the NA value. This function replaces np.nan values with a sentinel value,
    converts the column to integers, and then to strings, finally replacing the sentinel
    value with the desired NA string.

    This is an interim solution -- now that pandas extension arrays have been
    implemented, we need to go back through and convert all of these integer columns
    that contain NA values to Nullable Integer types like Int64.

    Args:
        df: The dataframe to be fixed. This argument allows method chaining with the
            pipe() method.
        columns: A list of DataFrame column labels indicating which columns need to be
            reformatted for output.
        float_na: The floating point value to be interpreted as NA and replaced in col.
        int_na: Sentinel value to substitute for float_na prior to conversion of the
            column to integers.
        str_na: String value to substitute for int_na after the column has been
            converted to strings.

    Returns:
        A new DataFrame, with the selected columns converted to strings that look like
        integers, compatible with the postgresql COPY FROM command.

    """
    return (
        df.replace({c: float_na for c in columns}, int_na)
        .astype({c: int for c in columns})
        .astype({c: str for c in columns})
        .replace({c: str(int_na) for c in columns}, str_na)
    )


def month_year_to_date(df: pd.DataFrame) -> pd.DataFrame:
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
        The DataFrame in which to convert year/months fields to Date fields.

    Returns:
        A DataFrame in which the year/month fields have been converted into Date fields.
    """
    df = df.copy()
    month_regex = "_month$"
    year_regex = "_year$"
    # Columns that match our month or year patterns.
    month_cols = list(df.filter(regex=month_regex).columns)
    year_cols = list(df.filter(regex=year_regex).columns)

    # Base column names that don't include the month or year pattern
    months_base = [re.sub(month_regex, "", m) for m in month_cols]
    years_base = [re.sub(year_regex, "", y) for y in year_cols]

    # We only want to retain columns that have BOTH month and year
    # matches -- otherwise there's no point in creating a Date.
    date_base = [base for base in months_base if base in years_base]

    # For each base column that DOES have both a month and year,
    # We need to grab the real column names corresponding to each,
    # so we can access the values in the data frame, and use them
    # to create a corresponding Date column named [BASE]_date
    month_year_date = []
    for base in date_base:
        base_month_regex = f"^{base}{month_regex}"
        month_col = list(df.filter(regex=base_month_regex).columns)
        if not len(month_col) == 1:
            raise AssertionError
        month_col = month_col[0]
        base_year_regex = f"^{base}{year_regex}"
        year_col = list(df.filter(regex=base_year_regex).columns)
        if not len(year_col) == 1:
            raise AssertionError
        year_col = year_col[0]
        date_col = f"{base}_date"
        month_year_date.append((month_col, year_col, date_col))

    for month_col, year_col, date_col in month_year_date:
        df = fix_int_na(df, columns=[year_col, month_col])

        date_mask = (df[year_col] != "") & (df[month_col] != "")
        years = df.loc[date_mask, year_col]
        months = df.loc[date_mask, month_col]

        df.loc[date_mask, date_col] = pd.to_datetime(
            {"year": years, "month": months, "day": 1}, errors="coerce"
        )

        # Now that we've replaced these fields with a date, we drop them.
        df = df.drop([month_col, year_col], axis=1)

    return df


def remove_leading_zeros_from_numeric_strings(
    df: pd.DataFrame, col_name: str
) -> pd.DataFrame:
    """Remove leading zeros frame column values that are numeric strings.

    Sometimes an ID column (like generator_id or unit_id) will be reported with leading
    zeros and sometimes it won't. For example, in the Excel spreadsheets published by
    EIA, the same generator may show up with the ID "0001" and "1" in different years
    This function strips the leading zeros from those numeric strings so the data can
    be mapped accross years and datasets more reliably.

    Alphanumeric generator IDs with leadings zeroes are not affected, as we
    found no instances in which an alphanumeric ID appeared both with
    and without leading zeroes. The ID "0A1" will stay "0A1".

    Args:
        df: A DataFrame containing the column you'd like to remove numeric leading zeros
            from.
        col_name: The name of the column you'd like to remove numeric leading zeros
            from.

    Returns:
        A DataFrame without leading zeros for numeric string values in the desired
        column.
    """
    leading_zeros = df[col_name].str.contains(r"^0+\d+$").fillna(False)
    if leading_zeros.any():
        logger.debug(f"Fixing leading zeros in {col_name} column")
        df.loc[leading_zeros, col_name] = df[col_name].str.replace(
            r"^0+", "", regex=True
        )
    else:
        logger.debug(f"Found no numeric leading zeros in {col_name}")
    return df


def convert_to_date(
    df: pd.DataFrame,
    date_col: str = "report_date",
    year_col: str = "report_year",
    month_col: str = "report_month",
    day_col: str = "report_day",
    month_value: int = 1,
    day_value: int = 1,
) -> pd.DataFrame:
    """Convert specified year, month or day columns into a datetime object.

    If the input ``date_col`` already exists in the input dataframe, then no
    conversion is applied, and the original dataframe is returned unchanged.
    Otherwise the constructed date is placed in that column, and the columns
    which were used to create the date are dropped.

    Args:
        df: dataframe to convert
        date_col: the name of the column you want in the output.
        year_col: the name of the year column in the original table.
        month_col: the name of the month column in the original table.
        day_col: the name of the day column in the original table.
        month_value: generated month if no month exists.
        day_value: generated day if no day exists.

    Returns:
        A DataFrame in which the year, month, day columns values have been converted
        into datetime objects.
    """
    df = df.copy()
    if date_col in df.columns:
        return df

    year = df[year_col]

    month = month_value if month_col not in df.columns else df[month_col]

    day = day_value if day_col not in df.columns else df[day_col]

    df[date_col] = pd.to_datetime({"year": year, "month": month, "day": day})
    cols_to_drop = [x for x in [day_col, year_col, month_col] if x in df.columns]
    df = df.drop(cols_to_drop, axis="columns")

    return df


def fix_eia_na(df: pd.DataFrame) -> pd.DataFrame:
    """Replace common ill-posed EIA NA spreadsheet values with np.nan.

    Currently replaces empty string, single decimal points with no numbers,
    and any single whitespace character with np.nan.

    Args:
        df: The DataFrame to clean.

    Returns:
        DataFrame with regularized NA values.
    """
    return df.replace(regex=r"(^\.$|^\s*$)", value=np.nan)


def simplify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Simplify column labels for use as snake_case database fields.

    All column labels will be simplified by:

    * Replacing all non-alphanumeric characters with spaces.
    * Forcing all letters to be lower case.
    * Compacting internal whitespace to a single " ".
    * Stripping leading and trailing whitespace.
    * Replacing all remaining whitespace with underscores.

    Args:
        df: The DataFrame whose column labels to simplify.

    Returns:
        A dataframe with simplified column names.
    """
    # Do nothing, if empty dataframe (e.g. mocked for tests)
    if df.shape[0] == 0:
        return df
    df.columns = (
        df.columns.str.replace(r"[^0-9a-zA-Z]+", " ", regex=True)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(" ", "_")
    )
    return df


def drop_tables(engine: sa.Engine, clobber: bool = False) -> None:
    """Drops all tables from a SQLite database.

    Creates an sa.schema.MetaData object reflecting the structure of the
    database that the passed in ``engine`` refers to, and uses that schema to
    drop all existing tables.

    Args:
        engine: An SQL Alchemy SQLite database Engine pointing at an exising SQLite
            database to be deleted.
        clobber: Whether or not to allow a non-empty DB to be removed.

    Raises:
        AssertionError: if clobber is False and there are any tables in the database.

    Returns:
        None
    """
    md = sa.MetaData()
    with engine.connect() as conn:
        md.reflect(conn)
        insp = sa.inspect(conn)
        if len(insp.get_table_names()) > 0 and not clobber:
            raise AssertionError(
                f"Attempting to drop database at {engine} while clobber is {clobber}."
            )
        md.drop_all(conn)
        conn.exec_driver_sql("VACUUM")


def merge_dicts(lods: list[dict[Any, Any]]) -> dict[Any, Any]:
    """Merge multipe dictionaries together.

    Given any number of dicts, shallow copy and merge into a new dict, precedence goes
    to key value pairs in latter dicts within the input list.

    Args:
        lods: a list of dictionaries.

    Returns:
        A single merged dictionary.

    """
    merged = {}
    for d in lods:
        merged |= d
    return merged


def convert_cols_dtypes(
    df: pd.DataFrame,
    data_source: str | None = None,
    name: str | None = None,
) -> pd.DataFrame:
    """Convert a PUDL dataframe's columns to the correct data type.

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
        df: dataframe with columns that appear in the PUDL tables.
        data_source: the name of the datasource (eia, ferc1, etc.)
        name: name of the table (for logging only!)

    Returns:
        Input dataframe, but with column types as specified by
        :py:const:`pudl.metadata.fields.FIELD_METADATA`
    """
    # get me all of the columns for the table in the constants dtype dict
    dtypes = {
        col: dtype
        for col, dtype in get_pudl_dtypes(group=data_source).items()
        if col in df.columns
    }

    # grab only the boolean columns (we only need their names)
    bool_cols = [col for col in dtypes if dtypes[col] == "boolean"]
    # grab all of the non boolean columns
    non_bool_cols = {col: dtypes[col] for col in dtypes if col not in bool_cols}
    # Grab only the string columns...
    string_cols = [col for col in dtypes if dtypes[col] == "string"]

    for col in bool_cols:
        # Bc the og bool values were sometimes coming across as actual bools or
        # strings, for some reason we need to map both types (I'm not sure
        # why!). We use na_action to preserve the og NaN's. I've also added in
        # the string version of a null value bc I am sure it will exist.
        df[col] = df[col].map(
            {
                "False": False,
                "True": True,
                False: False,
                True: True,
                "nan": pd.NA,
            }
        )

    if name:
        logger.debug(f"Converting the dtypes of: {name}")
    # unfortunately, the pd.Int32Dtype() doesn't allow a conversion from object
    # columns to this nullable int type column. `utility_id_eia` shows up as a
    # column of strings (!) of numbers so it is an object column, and therefor
    # needs to be converted beforehand.
    # we want to be able to use this dtype cleaning at many stages, and
    # sometimes this column has been converted to a float and therefore
    # we need to skip this conversion
    if "utility_id_eia" in df.columns and df.utility_id_eia.dtypes is np.dtype(
        "object"
    ):
        df = df.astype({"utility_id_eia": "float"})
    df = (
        df.astype(non_bool_cols)
        .astype({col: "boolean" for col in bool_cols})
        .replace(to_replace="nan", value={col: pd.NA for col in string_cols})
        .replace(to_replace="<NA>", value={col: pd.NA for col in string_cols})
    )

    # Zip codes are highly correlated with datatype. If they datatype gets
    # converted at any point it may mess up the accuracy of the data. For
    # example: 08401.0 or 8401 are both incorrect versions of 08401 that a
    # simple datatype conversion cannot fix. For this reason, we use the
    # zero_pad_numeric_string function.
    if any("zip_code" for col in df.columns):
        zip_cols = [col for col in df.columns if "zip_code" in col]
        for col in zip_cols:
            if "4" in col:
                df.loc[:, col] = zero_pad_numeric_string(df[col], n_digits=4)
            else:
                df.loc[:, col] = zero_pad_numeric_string(df[col], n_digits=5)

    return df


def generate_rolling_avg(
    df: pd.DataFrame, group_cols: list[str], data_col: str, window: int, **kwargs
) -> pd.DataFrame:
    """Generate a rolling average.

    For a given dataframe with a ``report_date`` column, generate a monthly
    rolling average and use this rolling average to impute missing values.

    Args:
        df: Original dataframe. Must have group_cols column, a data_col column and a
            ``report_date`` column.
        group_cols: a list of columns to groupby.
        data_col: the name of the data column.
        window: rolling window argument to pass to :meth:`pandas.Series.rolling`.
        kwargs: Additional arguments to pass to :meth:`pandas.Series.rolling`.

    Returns:
        DataFrame with an additional rolling average column.
    """
    df = df.astype({"report_date": "datetime64[ns]"})
    # create a full date range for this df
    date_range = pd.DataFrame(
        pd.date_range(
            start=min(df["report_date"]),
            end=max(df["report_date"]),
            freq="MS",
            name="report_date",
        )
    ).assign(tmp=1)  # assiging a temp column to merge on
    groups = (
        df[group_cols + ["report_date"]]
        .drop_duplicates()
        .assign(tmp=1)  # assiging a temp column to merge on
    )
    # merge the date range and the groups together
    # to get the backbone/complete date range/groups
    bones = (
        date_range.merge(groups)
        .drop(columns="tmp")  # drop the temp column
        .merge(df, on=group_cols + ["report_date"])
        .set_index(group_cols + ["report_date"])
        .groupby(by=group_cols + ["report_date"])
        .mean(numeric_only=True)
    )
    # with the aggregated data, get a rolling average
    roll = bones.rolling(window=window, center=True, **kwargs).agg({data_col: "mean"})
    # return the merged
    return bones.merge(
        roll, on=group_cols + ["report_date"], suffixes=("", "_rolling")
    ).reset_index()


def fillna_w_rolling_avg(
    df_og: pd.DataFrame,
    group_cols: list[str],
    data_col: str,
    window: int = 12,
    **kwargs,
) -> pd.DataFrame:
    """Filling NaNs with a rolling average.

    Imputes null values from a dataframe on a rolling monthly average. To note,
    this was designed to work with the PudlTabl object's tables.

    Args:
        df_og: Original dataframe. Must have ``group_cols`` columns, a ``data_col``
            column and a ``report_date`` column.
        group_cols: a list of columns to groupby.
        data_col: the name of the data column we're trying to fill.
        window: rolling window to pass to :meth:`pandas.Series.rolling`.
        kwargs: Additional arguments to pass to :meth:`pandas.Series.rolling`.

    Returns:
        pandas.DataFrame: dataframe with nulls filled in.
    """
    df_og = df_og.astype({"report_date": "datetime64[ns]"})
    df_roll = generate_rolling_avg(df_og, group_cols, data_col, window, **kwargs)
    df_roll[data_col] = df_roll[data_col].fillna(df_roll[f"{data_col}_rolling"])
    df_new = df_og.merge(
        df_roll[group_cols + ["report_date", data_col]],
        how="left",
        on=group_cols + ["report_date"],
        suffixes=("", "_rollfilled"),
    )
    df_new[data_col] = df_new[data_col].fillna(df_new[f"{data_col}_rollfilled"])
    return df_new.drop(columns=f"{data_col}_rollfilled")


def count_records(
    df: pd.DataFrame, cols: list[str], new_count_col_name: str
) -> pd.DataFrame:
    """Count the number of unique records in group in a dataframe.

    Args:
        df: dataframe you would like to groupby and count.
        cols: list of columns to group and count by.
        new_count_col_name: the name that will be assigned to the column that will
            contain the count.

    Returns:
        DataFrame containing only ``cols`` and ``new_count_col_name``.
    """
    return (
        df.assign(count_me=1)
        .groupby(cols, observed=True)
        .count_me.count()
        .reset_index()
        .rename(columns={"count_me": new_count_col_name})
    )


def cleanstrings_snake(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Clean the strings in a columns in a dataframe with snake case.

    Args:
        df: original dataframe.
        cols: list of columns in to apply snake case to.
    """
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
        )
    return df


def zero_pad_numeric_string(col: pd.Series, n_digits: int) -> pd.Series:
    """Clean up fixed-width leading zero padded numeric (e.g. ZIP, FIPS) codes.

    Often values like ZIP and FIPS codes are stored as integers, or get
    converted to floating point numbers because there are NA values in the
    column. Sometimes other non-digit strings are included like Canadian
    postal codes mixed in with ZIP codes, or IMP (imported) instead of a
    FIPS county code. This function attempts to manage these irregularities
    and produce either fixed-width leading zero padded strings of digits
    having a specified length (n_digits) or NA.

    * Convert the Series to a nullable string.
    * Remove any decimal point and all digits following it.
    * Remove any non-digit characters.
    * Replace any empty strings with NA.
    * Replace any strings longer than n_digits with NA.
    * Pad remaining digit-only strings to n_digits length.
    * Replace (invalid) all-zero codes with NA.

    Args:
        col: The Series to clean. May be numeric, string, object, etc.
        n_digits: the desired length of the output strings.

    Returns:
        A Series of nullable strings, containing only all-numeric strings
        having length n_digits, padded with leading zeroes if necessary.
    """
    out_col = (
        col.astype("string")
        # Remove decimal points and any digits following them.
        # This turns floating point strings into integer strings
        .replace(r"[\.]+\d*", "", regex=True)
        # Remove any whitespace
        .replace(r"\s+", "", regex=True)
        # Replace anything that's not entirely digits with NA
        .replace(r"[^\d]+", pd.NA, regex=True)
        # Set any string longer than n_digits to NA
        .replace(f"[\\d]{{{n_digits+1},}}", pd.NA, regex=True)
        # Pad the numeric string with leading zeroes to n_digits length
        .str.zfill(n_digits)
        # All-zero ZIP & FIPS codes are invalid.
        # Also catches empty strings that were zero padded.
        .replace({n_digits * "0": pd.NA})
    )
    if not out_col.str.match(f"^[\\d]{{{n_digits}}}$").all():
        raise ValueError(
            f"Failed to generate zero-padded numeric strings of length {n_digits}."
        )
    return out_col


def iterate_multivalue_dict(**kwargs):
    """Make dicts from dict with main dict key and one value of main dict."""
    single_valued = {
        k: v for k, v in kwargs.items() if not (isinstance(v, list | tuple))
    }

    # Transform multi-valued {k: vlist} into {k1: [{k1: v1}, {k1: v2}, ...], k2: [...], ...}
    multi_valued = {
        k: [{k: v} for v in vlist]
        for k, vlist in kwargs.items()
        if (isinstance(vlist, list | tuple))
    }

    for value_assignments in itertools.product(*multi_valued.values()):
        result = dict(single_valued)
        for k_v in value_assignments:
            result.update(k_v)
        yield result


def get_working_dates_by_datasource(datasource: str) -> pd.DatetimeIndex:
    """Get all working dates of a datasource as a DatetimeIndex."""
    import pudl.metadata.classes

    dates = pd.DatetimeIndex([])
    for data_source in pudl.metadata.classes.DataSource.from_field_namespace(
        datasource
    ):
        working_partitions = data_source.working_partitions
        if "years" in working_partitions:
            dates = dates.append(
                pd.to_datetime(working_partitions["years"], format="%Y")
            )
        if "year_month" in working_partitions:
            dates = dates.append(
                pd.DatetimeIndex([pd.to_datetime(working_partitions["year_month"])])
            )
    return dates


def dedupe_on_category(
    dedup_df: pd.DataFrame, base_cols: list[str], category_name: str, sorter: list[str]
) -> pd.DataFrame:
    """Deduplicate a df using a sorted category to retain prefered values.

    Use a sorted category column to retain your prefered values when a
    dataframe is deduplicated.

    Args:
        dedup_df: the dataframe with the records to deduplicate.
        base_cols: list of columns which must not be duplicated.
        category_name: name of the categorical column to order values for deduplication.
        sorter: sorted list of categorical values found in the ``category_name`` column.

    Returns:
        The deduplicated dataframe.
    """
    dedup_df[category_name] = dedup_df[category_name].astype(
        pd.CategoricalDtype(categories=sorter, ordered=True)
    )

    return dedup_df.drop_duplicates(subset=base_cols, keep="first")


def calc_capacity_factor(
    df: pd.DataFrame,
    freq: Literal["AS", "MS"],
    min_cap_fact: float | None = None,
    max_cap_fact: float | None = None,
) -> pd.DataFrame:
    """Calculate capacity factor.

    Capacity factor is calcuated from the capcity, the net generation over a
    time period and the hours in that same time period. The dates from that
    dataframe are pulled out to determine the hours in each period based on
    the frequency. The number of hours is used in calculating the capacity
    factor. Then records with capacity factors outside the range specified by
    `min_cap_fact` and `max_cap_fact` are dropped.

    Args:
        df: table with required inputs for capacity factor (``report_date``,
            ``net_generation_mwh`` and ``capacity_mw``).
        freq: String describing time frequency at which to aggregate the reported data,
            such as ``MS`` (month start) or ``AS`` (annual start).
        min_cap_fact: Lower bound, below which values are set to NaN. If None, don't use
            a lower bound. Default is None.
        max_cap_fact: Upper bound, below which values are set to NaN.  If None, don't
            use an upper bound. Default is None.

    Returns:
        Modified version of the input DataFrame with an additional ``capacity_factor``
        column.
    """
    # get a unique set of dates to generate the number of hours
    dates = df["report_date"].drop_duplicates()
    dates_to_hours = pd.DataFrame(
        data={
            "report_date": dates,
            "hours": dates.apply(
                lambda d: (
                    pd.date_range(d, periods=2, freq=freq)[1]
                    - pd.date_range(d, periods=2, freq=freq)[0]
                )
                / pd.Timedelta(hours=1)
            ),
        }
    )

    df = (
        # merge in the hours for the calculation
        df.merge(dates_to_hours, on=["report_date"])
        # actually calculate capacity factor wooo!
        .assign(
            capacity_factor=lambda x: x.net_generation_mwh / (x.capacity_mw * x.hours)
        )
        # Replace unrealistic capacity factors with NaN
        .pipe(oob_to_nan, ["capacity_factor"], lb=min_cap_fact, ub=max_cap_fact)
        .drop(["hours"], axis=1)
    )
    return df


def weighted_average(
    df: pd.DataFrame, data_col: str, weight_col: str, by: list[str]
) -> pd.DataFrame:
    """Generate a weighted average.

    Args:
        df: A DataFrame containing, at minimum, the columns specified in the other
            parameters data_col and weight_col.
        data_col: column name of data column to average
        weight_col: column name to weight on
        by: List of columns to group by when calcuating the weighted average value.

    Returns:
        A table with ``by`` columns as the index and the weighted ``data_col``.
    """
    df["_data_times_weight"] = df[data_col] * df[weight_col]
    df["_weight_where_notnull"] = df.loc[df[data_col].notnull(), weight_col]
    g = df.groupby(by, observed=True)
    result = g["_data_times_weight"].sum(min_count=1) / g["_weight_where_notnull"].sum(
        min_count=1
    )
    del df["_data_times_weight"], df["_weight_where_notnull"]
    return result.to_frame(name=data_col)  # .reset_index()


def sum_and_weighted_average_agg(
    df_in: pd.DataFrame,
    by: list,
    sum_cols: list,
    wtavg_dict: dict[str, str],
) -> pd.DataFrame:
    """Aggregate dataframe by summing and using weighted averages.

    Many times we want to aggreate a data table using the same groupby columns
    but with different aggregation methods. This function combines two of our
    most common aggregation methods (summing and applying a weighted average)
    into one function. Because pandas does not have a built-in weighted average
    method for groupby we use :func:``weighted_average``.

    Args:
        df_in (pandas.DataFrame): input table to aggregate. Must have columns
            in ``id_cols``, ``sum_cols`` and keys from ``wtavg_dict``.
        by (list): columns to group/aggregate based on. These columns
            will be passed as an argument into grouby as ``by`` arg.
        sum_cols (list): columns to sum.
        wtavg_dict (dictionary): dictionary of columns to average (keys) and
            columns to weight by (values).

    Returns:
        table with join of columns from ``by``, ``sum_cols`` and keys of
        ``wtavg_dict``. Primary key of table will be ``by``.
    """
    logger.debug(f"grouping by {by}")
    # we are keeping the index here for easy merging of the weighted cols below
    df_out = df_in.groupby(by=by, as_index=True, observed=True)[sum_cols].sum(
        min_count=1
    )
    for data_col, weight_col in wtavg_dict.items():
        df_out.loc[:, data_col] = weighted_average(
            df_in, data_col=data_col, weight_col=weight_col, by=by
        )[data_col]
    return df_out.reset_index()


def get_eia_ferc_acct_map() -> pd.DataFrame:
    """Get map of EIA technology_description/pm codes <> ferc accounts.

    Returns:
        pandas.DataFrame: table which maps the combination of EIA's technology
            description and prime mover code to FERC Uniform System of Accounts
            (USOA) accouting names. Read more about USOA
            `here
            <https://www.ferc.gov/enforcement-legal/enforcement/accounting-matters>`__
            The output table has the following columns: `['technology_description',
            'prime_mover_code', 'ferc_acct_name']`
    """
    eia_ferc_acct_map = pd.read_csv(
        importlib.resources.files("pudl.package_data.glue")
        / "ferc_acct_to_pm_tech_map.csv"
    )
    return eia_ferc_acct_map


def dedupe_n_flatten_list_of_lists(mega_list: list) -> list:
    """Flatten a list of lists and remove duplicates."""
    return list({item for sublist in mega_list for item in sublist})


def flatten_list(xs: Iterable) -> Generator:
    """Flatten an irregular (arbitrarily nested) list of lists (or sets).

    Inspiration from
    `here <https://stackoverflow.com/questions/2158395/flatten-an-irregular-arbitrarily-nested-list-of-lists>`__
    """
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, str | bytes):
            yield from flatten_list(x)
        else:
            yield x


def convert_df_to_excel_file(df: pd.DataFrame, **kwargs) -> pd.ExcelFile:
    """Convert a :class:`pandas.DataFrame` into a :class:`pandas.ExcelFile`.

    Args:
        df: The DataFrame to convert.
        kwargs: Additional arguments to pass into :meth:`pandas.to_excel`.

    Returns:
        The contents of the input DataFrame, represented as an ExcelFile.
    """
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, **kwargs)
    bio.seek(0)
    return pd.ExcelFile(bio)


def get_asset_keys(
    assets: list[AssetsDefinition], exclude_source_assets: bool = True
) -> set[AssetKey]:
    """Get a set of asset keys from a list of asset definitions.

    Args:
        assets: list of asset definitions.
        exclude_source_assets: exclude SourceAssets in the returned list.
            Some selection operations don't allow SourceAsset keys.

    Returns:
        A set of asset keys.
    """
    asset_keys = set()
    for asset in assets:
        if isinstance(asset, SourceAsset):
            if not exclude_source_assets:
                asset_keys = asset_keys.union(asset.key)
        else:
            asset_keys = asset_keys.union(asset.keys)
    return asset_keys


def get_asset_group_keys(
    asset_group: str, all_assets: list[AssetsDefinition]
) -> list[str]:
    """Get a list of asset names in a given asset group.

    Args:
        asset_group: the name of the asset group.
        all_assets: the collection of assets to select the group from.

    Return:
        A list of asset names in the asset_group.
    """
    asset_keys = AssetSelection.groups(asset_group).resolve(all_assets)
    return [asset.to_python_identifier() for asset in list(asset_keys)]


def convert_col_to_bool(
    df: pd.DataFrame, col_name: str, true_values: list, false_values: list
) -> pd.DataFrame:
    """Turn a column into a boolean while preserving NA values.

    You don't have to specify NA as true or false - it will preserve it's NA-ness unless
    you add it to one of the input true/false lists.

    Args:
        df: The dataframe containing the column you want to change.
        col_name: The name of the column you want to turn into a boolean (must be an
            existing column, not a new column name).
        true_values: The list of values in col_name that you want to be marked as True.
        false_values: The list of values appearing in col_name that you want to be
            False.

    Raises:
        AssertionError: if there are non-NA values in col_name that aren't specified in
            true_values or false_values.
        AssertionError: if there are values that appear in both true_values and
            false_values.

    Returns:
        pd.DataFrame: The original dataframe with col_name as a boolean column.
    """
    # Make sure inputs are valid
    if unspecified_values := [
        x for x in df[col_name].dropna().unique() if x not in true_values + false_values
    ]:
        raise AssertionError(
            "Found values besides NA that are not categoriezed as True or False: "
            f"{unspecified_values}"
        )
    if [x for x in true_values if x in false_values]:
        raise AssertionError(
            "Duplicate values in true and false! You can only pick one."
        )
    # Set values as true or value. Astype boolean should preserve NA values.
    # This is easier than building an input dictionary for pandas map or replace
    # functions.
    df = df.copy()
    df.loc[df[col_name].isin(true_values), col_name] = True
    df.loc[df[col_name].isin(false_values), col_name] = False
    df[col_name] = df[col_name].astype("boolean")

    return df


def scale_by_ownership(
    gens: pd.DataFrame,
    own_eia860: pd.DataFrame,
    scale_cols: list,
    validate: str = "1:m",
) -> pd.DataFrame:
    """Generate proportional data by ownership %s.

    Why do we have to do this at all? Sometimes generators are owned by
    many different utility owners that own slices of that generator. EIA
    reports which portion of each generator is owned by which utility
    relatively clearly in their ownership table. On the other hand, in
    FERC1, sometimes a partial owner reports the full plant-part, sometimes
    they report only their ownership portion of the plant-part. And of
    course it is not labeld in FERC1. Because of this, we need to compile
    all of the possible ownership slices of the EIA generators.

    In order to accumulate every possible version of how a generator could
    be reported, this method generates two records for each generator's
    reported owners: one of the portion of the plant part they own and one
    for the plant-part as a whole. The portion records are labeled in the
    ``ownership_record_type`` column as "owned" and the total records are labeled as
    "total".

    In this function we merge in the ownership table so that generators
    with multiple owners then have one record per owner with the
    ownership fraction (in column ``fraction_owned``). Because the ownership
    table only contains records for generators that have multiple owners,
    we assume that all other generators are owned 100% by their operator.
    Then we generate the "total" records by duplicating the "owned" records
    but assigning the ``fraction_owned`` to be 1 (i.e. 100%).

    Arguments:
        gens: table with records at the generator level and generator attributes
            to be scaled by ownership, must have columns ``plant_id_eia``,
            ``generator_id``, and ``report_date``
        own_eia860: the ``ownership_eia860`` table
        scale_cols: a list of columns in the generator table to slice by ownership
            fraction
        validate: how to validate merging the ownership table onto the
            generators table
    Returns:
        Table of generator records with ``scale_cols`` sliced by ownership fraction
        such that there is a "total" and "owned" record for each generator owner.
        The "owned" records have the generator's data scaled to the ownership
        percentage (e.g. if a 200 MW generator has a 75% stake owner and a 25%
        stake owner, this will result in two "owned" records with 150 MW and 50 MW).
        The "total" records correspond to the full plant for every owner (e.g. using
        the same 2-owner 200 MW generator as above, each owner will have a
        records with 200 MW).
    """
    # grab the ownership table, and reduce it to only the columns we need
    own860 = own_eia860[
        [
            "plant_id_eia",
            "generator_id",
            "report_date",
            "fraction_owned",
            "owner_utility_id_eia",
        ]
    ].pipe(pudl.helpers.convert_cols_dtypes, "eia")
    # we're left merging BC we've removed the retired gens, which are
    # reported in the ownership table
    gens = (
        gens.merge(
            own860,
            how="left",
            on=["plant_id_eia", "generator_id", "report_date"],
            validate=validate,
        )
        .assign(  # assume gens that don't show up in the own table have one 100% owner
            fraction_owned=lambda x: x.fraction_owned.fillna(value=1),
            # assign the operator id as the owner if null bc if a gen isn't
            # reported in the own_eia860 table we can assume the operator
            # is the owner
            owner_utility_id_eia=lambda x: x.owner_utility_id_eia.fillna(
                x.utility_id_eia
            ),
            ownership_record_type="owned",
        )  # swap in the owner as the utility
        .drop(columns=["utility_id_eia"])
        .rename(columns={"owner_utility_id_eia": "utility_id_eia"})
    )

    # duplicate all of these "owned" records, asign 1 to all of the
    # fraction_owned column to indicate 100% ownership, and add these new
    # "total" records to the "owned"
    gens = pd.concat(
        [
            gens,
            gens.copy().assign(fraction_owned=1, ownership_record_type="total"),
        ]
    )
    gens.loc[:, scale_cols] = gens.loc[:, scale_cols].multiply(
        gens["fraction_owned"], axis="index"
    )
    return gens


def assert_cols_areclose(
    df: pd.DataFrame,
    a_cols: list[str],
    b_cols: list[str],
    mismatch_threshold: float,
    message: str,
):
    """Check if two column sets of a dataframe are close to each other.

    Ignores NANs and raises if there are too many mismatches.
    """
    # we use df.loc, so if we use a debugger in here we can see the actual data
    # instead of just whether or not there are matches.
    mismatch = df.loc[
        ~np.isclose(
            np.ma.masked_where(np.isnan(df[a_cols]), df[a_cols]),
            np.ma.masked_where(np.isnan(df[b_cols]), df[b_cols]),
            equal_nan=True,
        ).filled()
    ]
    mismatch_ratio = len(mismatch) / len(df)
    if mismatch_ratio > mismatch_threshold:
        raise AssertionError(
            f"{message} Mismatch ratio {mismatch_ratio:.01%} > "
            f"threshold {mismatch_threshold:.01%}."
        )
