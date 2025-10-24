"""Module to perform data cleaning functions on EPA CEMS data tables."""

import datetime

import pandas as pd
import polars as pl
import pytz

import pudl.logging_helpers
from pudl.metadata.fields import apply_pudl_dtypes_polars

logger = pudl.logging_helpers.get_logger(__name__)


###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################
def harmonize_eia_epa_orispl(
    lf: pl.LazyFrame,
    crosswalk_df: pd.DataFrame,
) -> pl.LazyFrame:
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
        lf: A CEMS hourly LazyFrame for one year-month-state.
        crosswalk_df: The core_epa__assn_eia_epacamd dataframe from the database.

    Returns:
        The same data, with the ORISPL plant codes corrected to match the EIA plant IDs.
    """
    # Make sure the crosswalk does not have multiple plant_id_eia values for each
    # plant_id_epa and emissions_unit_id_epa value before reassigning IDs.
    one_to_many = crosswalk_df.groupby(
        ["plant_id_epa", "emissions_unit_id_epa"]
    ).filter(
        lambda x: x.plant_id_eia.nunique() > 1  # noqa: PD101
    )
    if not one_to_many.empty:
        raise AssertionError(
            "The core_epa__assn_eia_epacamd crosswalk has more than one plant_id_eia value per "
            "plant_id_epa and emissions_unit_id_epa group"
        )
    crosswalk_df = crosswalk_df[
        ["plant_id_eia", "plant_id_epa", "emissions_unit_id_epa"]
    ].drop_duplicates()

    # Merge CEMS with Crosswalk to get correct EIA ORISPL code and fill in all unmapped
    # values with old plant_id_epa value.
    return lf.join(
        pl.from_pandas(crosswalk_df).lazy(),
        on=["plant_id_epa", "emissions_unit_id_epa"],
        how="left",
    ).with_columns(pl.col("plant_id_eia").fill_null(pl.col("plant_id_epa")))


def convert_to_utc(lf: pl.LazyFrame, plant_utc_offset: pd.DataFrame) -> pl.LazyFrame:
    """Convert CEMS datetime data to UTC timezones.

    Transformations include:

    * Account for timezone differences with offset from UTC.

    Args:
        lf: A CEMS hourly LazyFrame for one year-state.
        plant_utc_offset: A dataframe association with timezones.

    Returns:
        The same data, with an op_datetime_utc column added and the op_date and op_hour
        columns removed.
    """
    lf = lf.with_columns(
        op_datetime_naive=pl.col("op_date").dt.combine(pl.time(hour=pl.col("op_hour")))
    ).join(
        pl.from_pandas(plant_utc_offset).lazy(),
        how="left",
        on="plant_id_eia",
    )

    # Add the offset from UTC. CEMS data don't have DST, so the offset is always the
    # same for a given plant. The result is a timezone naive datetime column that
    # contains values in UTC. Storing timezone info in Numpy datetime64 objects is
    # deprecated, but the PyArrow schema stores this data as UTC. See:
    # https://numpy.org/devdocs/reference/arrays.datetime.html#basic-datetimes
    return lf.with_columns(
        operating_datetime_utc=pl.col("op_datetime_naive") - pl.col("utc_offset")
    ).drop(["op_date", "op_hour", "op_datetime_naive", "utc_offset"])


def _load_plant_utc_offset(core_eia__entity_plants: pd.DataFrame) -> pd.DataFrame:
    """Load the UTC offset each EIA plant.

    CEMS times don't change for DST, so we get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        pudl_engine: A database connection engine for
            an existing PUDL DB.

    Returns:
        Dataframe of applicable timezones taken from the core_eia__entity_plants table.
    """
    timezones = core_eia__entity_plants[["plant_id_eia", "timezone"]].copy().dropna()
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
    raw_lf: pl.LazyFrame,
    core_epa__assn_eia_epacamd: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> pl.LazyFrame:
    """Transform EPA CEMS hourly data and ready it for export to Parquet.

    Args:
        raw_df: An extracted by not yet transformed year_quarter of EPA CEMS data.
        pudl_engine: SQLAlchemy connection engine for connecting to an existing PUDL DB.

    Returns:
        A single year_quarter of EPA CEMS data
    """
    # Create all the table inputs used for the subtransform functions below

    return (
        raw_lf.pipe(apply_pudl_dtypes_polars, group="epacems")
        .with_columns(
            emissions_unit_id_epa=pl.when(
                pl.col("emissions_unit_id_epa").str.contains(r"^\d+$")
            )
            .then(pl.col("emissions_unit_id_epa").str.strip_chars_start("0"))
            .otherwise(pl.col("emissions_unit_id_epa"))
        )
        .pipe(harmonize_eia_epa_orispl, core_epa__assn_eia_epacamd)
        .pipe(
            convert_to_utc,
            plant_utc_offset=_load_plant_utc_offset(core_eia__entity_plants),
        )
        .with_columns(
            gross_load_mw=pl.when(pl.col("gross_load_mw") > 2000)
            .then(pl.col("gross_load_mw") / 1000)
            .otherwise(pl.col("gross_load_mw"))
        )
        .pipe(apply_pudl_dtypes_polars, group="epacems")
    )
