"""Module to perform data cleaning functions on EPA CEMS data tables."""

import datetime

import pandas as pd
import pytz

import pudl.logging_helpers
from pudl.helpers import remove_leading_zeros_from_numeric_strings
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)


###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################
def harmonize_eia_epa_orispl(
    df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """Harmonize the ORISPL code to match the EIA data.

    The EIA plant IDs and CEMS ORISPL codes almost match, but not quite. EPA has
    compiled a crosswalk that maps one set of IDs to the other. The crosswalk is
    integrated into the PUDL db.

    This function merges the crosswalk with the cems data thus adding the official
    plant_id_eia column to CEMS. In cases where there is no plant_id_eia value for a
    given plant_id_epa (i.e., this plant isn't in the crosswalk yet), we use
    fillna() to add the plant_id_epa value to the plant_id_eia column. Because the
    plant_id_epa is almost always correct this is reasonable.

    EIA IDs are more correct so use the crosswalk to fix any erronious EPA IDs and get
    rid of that column to avoid confusion.

    https://github.com/USEPA/camd-eia-crosswalk

    Note that this transformation needs to be run *before* convert_to_utc, because
    convert_to_utc uses the plant ID to look up timezones.

    Args:
        df: A CEMS hourly dataframe for one year-month-state.
        crosswalk_df: The epacamd_eia dataframe from the database.

    Returns:
        The same data, with the ORISPL plant codes corrected to match the EIA plant IDs.
    """
    # Make sure the crosswalk does not have multiple plant_id_eia values for each
    # plant_id_epa and emissions_unit_id_epa value before reassigning IDs.
    one_to_many = crosswalk_df.groupby(
        ["plant_id_epa", "emissions_unit_id_epa"]
    ).filter(lambda x: x.plant_id_eia.nunique() > 1)

    if not one_to_many.empty:
        raise AssertionError(
            "The epacamd_eia crosswalk has more than one plant_id_eia value per "
            "plant_id_epa and emissions_unit_id_epa group"
        )
    crosswalk_df = crosswalk_df[
        ["plant_id_eia", "plant_id_epa", "emissions_unit_id_epa"]
    ].drop_duplicates()

    # Merge CEMS with Crosswalk to get correct EIA ORISPL code and fill in all unmapped
    # values with old plant_id_epa value.
    df_merged = pd.merge(
        df,
        crosswalk_df,
        on=["plant_id_epa", "emissions_unit_id_epa"],
        how="left",
    ).assign(plant_id_eia=lambda x: x.plant_id_eia.fillna(x.plant_id_epa))

    return df_merged


def convert_to_utc(df: pd.DataFrame, plant_utc_offset: pd.DataFrame) -> pd.DataFrame:
    """Convert CEMS datetime data to UTC timezones.

    Transformations include:

    * Account for timezone differences with offset from UTC.

    Args:
        df: A CEMS hourly dataframe for one year-state.
        plant_utc_offset: A dataframe association with timezones.

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
        plant_utc_offset,
        how="left",
        on="plant_id_eia",
    )

    # Some of the timezones in the plants_entity_eia table may be missing,
    # but none of the CEMS plants should be.
    if df["utc_offset"].isna().any():
        missing_plants = df.loc[df["utc_offset"].isna(), "plant_id_eia"].unique()
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
    )
    return df


def _load_plant_utc_offset(plants_entity_eia: pd.DataFrame) -> pd.DataFrame:
    """Load the UTC offset each EIA plant.

    CEMS times don't change for DST, so we get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        pudl_engine: A database connection engine for
            an existing PUDL DB.

    Returns:
        Dataframe of applicable timezones taken from the plants_entity_eia table.
    """
    timezones = plants_entity_eia[["plant_id_eia", "timezone"]].copy().dropna()
    jan1 = datetime.datetime(2011, 1, 1)  # year doesn't matter
    timezones["utc_offset"] = timezones["timezone"].apply(
        lambda tz: pytz.timezone(tz).localize(jan1).utcoffset()
    )
    del timezones["timezone"]
    return timezones


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
    epacamd_eia: pd.DataFrame,
    plants_entity_eia: pd.DataFrame,
) -> pd.DataFrame:
    """Transform EPA CEMS hourly data and ready it for export to Parquet.

    Args:
        raw_df: An extracted by not yet transformed state-year of EPA CEMS data.
        pudl_engine: SQLAlchemy connection engine for connecting to an existing PUDL DB.

    Returns:
        A single year-state of EPA CEMS data
    """
    # Create all the table inputs used for the subtransform functions below

    return (
        raw_df.pipe(apply_pudl_dtypes, group="epacems")
        .pipe(remove_leading_zeros_from_numeric_strings, "emissions_unit_id_epa")
        .pipe(harmonize_eia_epa_orispl, epacamd_eia)
        .pipe(
            convert_to_utc, plant_utc_offset=_load_plant_utc_offset(plants_entity_eia)
        )
        .pipe(correct_gross_load_mw)
        .pipe(apply_pudl_dtypes, group="epacems")
    )
