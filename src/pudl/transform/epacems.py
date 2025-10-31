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
    crosswalk_lf: pl.LazyFrame,
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
    crosswalk_lf = (
        crosswalk_lf.select(
            [
                "plant_id_eia",
                "plant_id_epa",
                "emissions_unit_id_epa",
            ]
        )
        .unique()
        # Sorty by join key to improve join performance
        .sort(["plant_id_eia", "emissions_unit_id_epa"])
    )

    # Merge CEMS with Crosswalk to get correct EIA ORISPL code and fill in all unmapped
    # values with old plant_id_epa value.
    return lf.join(
        crosswalk_lf,
        on=["plant_id_epa", "emissions_unit_id_epa"],
        how="left",
        coalesce=True,
    ).with_columns(pl.col("plant_id_eia").fill_null(pl.col("plant_id_epa")))


def convert_to_utc(lf: pl.LazyFrame, plant_utc_offset: pl.LazyFrame) -> pl.LazyFrame:
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
    return (
        lf.with_columns(
            op_datetime_naive=pl.col("op_date").dt.combine(
                pl.time(hour=pl.col("op_hour"))
            )
        )
        .join(
            plant_utc_offset.sort("plant_id_eia"),
            how="left",
            on="plant_id_eia",
            coalesce=True,
        )
        # Add the offset from UTC. CEMS data don't have DST, so the offset is always the
        # same for a given plant. The result is a timezone naive datetime column that
        # contains values in UTC. Storing timezone info in Numpy datetime64 objects is
        # deprecated, but the PyArrow schema stores this data as UTC. See:
        # https://numpy.org/devdocs/reference/arrays.datetime.html#basic-datetimes
        .with_columns(
            operating_datetime_utc=pl.col("op_datetime_naive") - pl.col("utc_offset")
        )
        .drop(["op_date", "op_hour", "op_datetime_naive", "utc_offset"])
    )


def _load_plant_utc_offset(core_eia__entity_plants: pd.DataFrame) -> pl.LazyFrame:
    """Load the UTC offset each EIA plant.

    CEMS times don't change for DST, so we get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        core_eia__entity_plants: EIA plants DataFrame.

    Returns:
        Polars LazyFrame of applicable timezones taken from the core_eia__entity_plants
        table.
    """
    logger.debug("Creating plant UTC offset LazyFrame")

    # Process timezone data efficiently
    timezones_df = (
        core_eia__entity_plants[["plant_id_eia", "timezone"]]
        .dropna()
        .drop_duplicates()  # Remove duplicates to minimize processing
    )

    # Calculate UTC offset for January (no DST) - vectorized approach
    jan1 = datetime.datetime(2011, 1, 1)

    # Create a mapping of unique timezones to offsets to avoid repeated calculations
    unique_timezones = timezones_df["timezone"].unique()
    timezone_offset_map = {
        tz: pytz.timezone(tz).localize(jan1).utcoffset() for tz in unique_timezones
    }

    timezones_df["utc_offset"] = timezones_df["timezone"].map(timezone_offset_map)

    return pl.from_pandas(timezones_df[["plant_id_eia", "utc_offset"]]).lazy()


def _validate_crosswalk_uniqueness(crosswalk_lf: pl.LazyFrame) -> None:
    """Validate that crosswalk has unique plant_id_eia values per EPA plant/unit.

    This validation is done separately to avoid materializing the LazyFrame during
    transformation.

    Args:
        crosswalk_lf: A polars LazyFrame of the core_epa__assn_eia_epacamd table.

    Raises:
        AssertionError: If crosswalk has multiple plant_id_eia values for a single EPA
        identifier.
    """
    logger.debug("Validating crosswalk uniqueness")

    # Check for one-to-many relationships between EPA and EIA plant IDs
    one_to_many_df = (
        crosswalk_lf.group_by(["plant_id_epa", "emissions_unit_id_epa"])
        .agg(pl.col("plant_id_eia").n_unique().alias("unique_eia_plants"))
        .filter(pl.col("unique_eia_plants") > 1)
        .collect()  # Materialize only the validation results
    )

    if len(one_to_many_df) > 0:
        logger.error(
            f"Found {len(one_to_many_df)} EPA plant/unit combinations with multiple EIA plant IDs"
        )
        raise AssertionError(
            "The core_epa__assn_eia_epacamd crosswalk has more than one plant_id_eia "
            "value per plant_id_epa and emissions_unit_id_epa group"
        )


def transform_epacems(
    raw_lf: pl.LazyFrame,
    core_epa__assn_eia_epacamd: pl.LazyFrame,
    plant_utc_offset: pl.LazyFrame,
) -> pl.LazyFrame:
    """Transform EPA CEMS hourly data and ready it for export to Parquet.

    Args:
        raw_lf: LazyFrame pointing to raw EPA CEMS data.
        core_epa__assn_eia_epacamd: EPA-EIA crosswalk DataFrame.
        core_eia__entity_plants: EIA plants DataFrame.

    Returns:
        A single year_quarter of EPA CEMS data
    """
    _validate_crosswalk_uniqueness(core_epa__assn_eia_epacamd)

    return (
        raw_lf.pipe(apply_pudl_dtypes_polars, group="epacems")
        .with_columns(
            # Strip leading zeros from strings
            # TODO: Update method in helpers.py with polars implementation from here.
            emissions_unit_id_epa=pl.when(
                pl.col("emissions_unit_id_epa").str.contains(r"^\d+$")
            )
            .then(pl.col("emissions_unit_id_epa").str.strip_chars_start("0"))
            .otherwise(pl.col("emissions_unit_id_epa"))
        )
        .pipe(harmonize_eia_epa_orispl, core_epa__assn_eia_epacamd)
        .pipe(convert_to_utc, plant_utc_offset=plant_utc_offset)
        # Fix gross load that is orders of magnitude too high
        .with_columns(
            gross_load_mw=pl.when(pl.col("gross_load_mw") > 2000)
            .then(pl.col("gross_load_mw") / 1000)
            .otherwise(pl.col("gross_load_mw"))
        )
        .pipe(apply_pudl_dtypes_polars, group="epacems")
    )
