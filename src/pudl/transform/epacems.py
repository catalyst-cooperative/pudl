"""Module to perform data cleaning functions on EPA CEMS data tables."""

import datetime
import logging

import numpy as np
import pandas as pd
import pytz
import sqlalchemy as sa

from pudl.helpers import remove_leading_zeros_from_numeric_strings
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)
###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################


def harmonize_eia_epa_orispl(
    df: pd.DataFrame,
    pudl_engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Harmonize the ORISPL code to match the EIA data.

    The EIA plant IDs and CEMS ORISPL codes almost match, but not quite. EPA has
    compiled a crosswalk that maps one set of IDs to the other. The crosswalk is
    integrated into the PUDL db.

    EIA IDs are more correct so use the crosswalk to fix any erronious EPA IDs and get
    rid of that column to avoid confusion.

    https://github.com/USEPA/camd-eia-crosswalk

    Note that this transformation needs to be run *before* convert_to_utc, because
    convert_to_utc uses the plant ID to look up timezones.

    Args:
        pudl_engine: SQLAlchemy connection engine for connecting to an existing PUDL DB.
            This is used to access the crosswalk file for conversion. The crosswalk must
            be processed prior to running this function or it won't work.
        df: A CEMS hourly dataframe for one year-month-state.

    Returns:
        The same data, with the ORISPL plant codes corrected to match the EIA plant IDs.

    """
    # Already ran a test to make sure this works. When you group the crosswalk by
    # plant_id_epa and emissions_unit_id_epa then calculate .nunique() for plant_id_eia,
    # none of the values are greater than one meaning that this drop/merge is ok. Might
    # want to make that an official test somewhere.

    crosswalk_df = pd.read_sql(
        "epacamd_eia_crosswalk",
        con=pudl_engine,
        columns=["plant_id_eia", "plant_id_epa", "emissions_unit_id_epa"],
    ).drop_duplicates()

    # I wonder if there is a faster way to do this by checking if the id needs to be
    # fixed rather than just merging it all together (as done below).

    # Merge CEMS with Crosswalk to get correct EIA ORISPL code.
    df_merged = pd.merge(
        df, crosswalk_df, on=["plant_id_epa", "emissions_unit_id_epa"], how="left"
    )

    # Because the crosswalk isn't complete, there are some instances where the
    # plant_id_eia value will be NA. This isn't great when it goes to grouping or
    # merging data together. Specifically for the convert_to_utc() function below.
    # This creates a column based on the plant_id_eia but backfills NA with
    # plant_id_epa so it can be used to merge on.
    df_merged["plant_id_combined"] = df_merged.plant_id_eia.fillna(
        df_merged.plant_id_epa
    )
    # assert (
    #     ~df_merged.plant_id_combined.isna().any()
    # ), "There shouldn't be any NA vales in the combined plant id column"

    assert len(df_merged) == len(df)
    return df_merged


def convert_to_utc(df: pd.DataFrame, plant_utc_offset: pd.DataFrame) -> pd.DataFrame:
    """Convert CEMS datetime data to UTC timezones.

    Transformations include:

    * Account for timezone differences with offset from UTC.

    Args:
        df: A CEMS hourly dataframe for one year-state.
        plant_utc_offset: A dataframe association plant_id_combined with timezones.

    Returns:
        The same data, with an op_datetime_utc column added and the op_date and op_hour
        columns removed.

    """
    df = df.assign(
        # Convert op_date and op_hour from string and integer to datetime:
        # Note that doing this conversion, rather than reading the CSV with
        # `parse_dates=True`, is >10x faster.
        # Read the date as a datetime, so all the dates are midnight
        op_datetime_naive=lambda x: pd.to_datetime(
            x.op_date, format=r"%m-%d-%Y", exact=True, cache=True
        )
        + pd.to_timedelta(x.op_hour, unit="h")  # Add the hour
    ).merge(
        plant_utc_offset.rename(columns={"plant_id_eia": "plant_id_combined"}),
        how="left",
        on="plant_id_combined",
    )

    # Some of the timezones in the plants_entity_eia table may be missing,
    # but none of the CEMS plants should be.
    if df["utc_offset"].isna().any():
        missing_plants = df.loc[df["utc_offset"].isna(), "plant_id_combined"].unique()
        raise ValueError(
            f"utc_offset should never be missing for CEMS plants, but was "
            f"missing for these: {str(list(missing_plants))}"
        )
    # Add the offset from UTC. CEMS data don't have DST, so the offset is always the
    # same for a given plant. The result is a timezone naive datetime column that
    # contains values in UTC. Storing timezone info in Numpy datetime64 objects is
    # deprecated, but the PyArrow schema stores this data as UTC. See:
    # https://numpy.org/devdocs/reference/arrays.datetime.html#basic-datetimes
    df["operating_datetime_utc"] = df["op_datetime_naive"] - df["utc_offset"]
    del (
        df["op_date"],
        df["op_hour"],
        df["op_datetime_naive"],
        df["utc_offset"],
        df["plant_id_combined"],
    )
    return df


def _load_plant_utc_offset(pudl_engine):
    """Load the UTC offset each EIA plant.

    CEMS times don't change for DST, so we get get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): A database connection engine for
            an existing PUDL DB.

    Returns:
        pandas.DataFrame: With columns plant_id_combined and utc_offset.

    """
    # Verify that we have a PUDL DB with plant attributes:
    inspector = sa.inspect(pudl_engine)
    if "plants_entity_eia" not in inspector.get_table_names():
        raise RuntimeError(
            "No plants_entity_eia available in the PUDL DB! Have you run the ETL? "
            f"Trying to access PUDL DB: {pudl_engine}"
        )
    timezones = pd.read_sql(
        sql="SELECT plant_id_eia, timezone FROM plants_entity_eia", con=pudl_engine
    ).dropna()
    jan1 = datetime.datetime(2011, 1, 1)  # year doesn't matter
    timezones["utc_offset"] = timezones["timezone"].apply(
        lambda tz: pytz.timezone(tz).localize(jan1).utcoffset()
    )
    del timezones["timezone"]
    return timezones


def add_facility_id_unit_id_epa(df):
    """Harmonize columns that are added later.

    The Parquet schema requires consistent column names across all partitions and
    ``facility_id`` and ``unit_id_epa`` aren't present before August 2008, so this
    function adds them in.

    Args:
        df (pandas.DataFrame): A CEMS dataframe

    Returns:
        pandas.Dataframe: The same DataFrame guaranteed to have int facility_id and
        unit_id_epa cols.

    """
    if ("facility_id" not in df.columns) or ("unit_id_epa" not in df.columns):
        # Can't just assign np.NaN and get an integer NaN, so make a new array
        # with the right shape:
        na_col = pd.array(np.full(df.shape[0], np.NaN), dtype="Int64")
        if "facility_id" not in df.columns:
            df["facility_id"] = na_col
        if "unit_id_epa" not in df.columns:
            df["unit_id_epa"] = na_col
    return df


def _all_na_or_values(series, values):
    """Test whether every element in the series is either missing or in values.

    This is fiddly because isin() changes behavior if the series is totally NaN (because
    of type issues).

    Example: x = pd.DataFrame({'a': ['x', np.NaN], 'b': [np.NaN, np.NaN]})
        x.isin({'x', np.NaN})

    Args:
        series (pd.Series): A data column values (set): A set of values

    Returns:
        bool: True or False, whether the elements are missing or in values

    """
    series_excl_na = series[series.notna()]
    if not len(series_excl_na):
        out = True
    elif series_excl_na.isin(values).all():
        out = True
    else:
        out = False
    return out


def correct_gross_load_mw(df: pd.DataFrame) -> pd.DataFrame:
    """Fix values of gross load that are wrong by orders of magnitude.

    Args:
        df: A CEMS dataframe

    Returns:
        The same DataFrame with corrected gross load values.

    """
    # Largest fossil plant is something like 3500 MW, and the largest unit
    # in the EIA 860 is less than 1500. Therefore, assume they've done it
    # wrong (by writing KWh) if they report more.
    # (There is a cogen unit, 54634 unit 1, that regularly reports around
    # 1700 MW. I'm assuming they're correct.)
    # This is rare, so don't bother most of the time.
    bad = df["gross_load_mw"] > 2000
    if bad.any():
        df.loc[bad, "gross_load_mw"] = df.gross_load_mw / 1000
    return df


def transform(
    raw_df: pd.DataFrame,
    pudl_engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Transform EPA CEMS hourly data and ready it for export to Parquet.

    Args:
        raw_df: An extracted by not yet transformed state-year of EPA CEMS data.
        pudl_engine: SQLAlchemy connection engine for connecting to an existing PUDL DB.

    Returns:
        A single year-state of EPA CEMS data

    """
    return (
        raw_df.pipe(remove_leading_zeros_from_numeric_strings, "emissions_unit_id_epa")
        .pipe(harmonize_eia_epa_orispl, pudl_engine)
        .pipe(convert_to_utc, plant_utc_offset=_load_plant_utc_offset(pudl_engine))
        .pipe(correct_gross_load_mw)
        .pipe(apply_pudl_dtypes, group="epacems")
    )
