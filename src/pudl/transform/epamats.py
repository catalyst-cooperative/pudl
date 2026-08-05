"""Module to perform data cleaning functions on EPA MATS data tables."""

import datetime

import dagster as dg
import pandas as pd
import polars as pl
import pytz

import pudl.logging_helpers
from pudl.metadata.dtypes import apply_pudl_dtypes_polars

logger = pudl.logging_helpers.get_logger(__name__)

MATS_MEASUREMENT_CODES: dict[str, str] = {
    "MEASURE": "Measured",
    "UNAVAIL": "Unavailable",
    "UPDOWN": "Startup or Shutdown",
}
"""Mapping from raw MATS measurement codes to canonical values.

RAW values seen in the data: ``Measured``, ``Startup or Shutdown``,
``Unavailable``, ``Manually Calculated``, ``MEASURE``, ``UNAVAIL``,
``UPDOWN``, or empty string.
"""

MEASUREMENT_CODE_COLS: list[str] = [
    "hg_mass_measurement_code",
    "hcl_mass_measurement_code",
    "hf_mass_measurement_code",
]


def _map_measurement_codes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Map non-canonical measurement codes to canonical values.

    Codes not in the mapping (e.g. ``Measured``, ``Manually Calculated``) are
    left unchanged. Empty strings are set to null.

    Args:
        lf: MATS hourly data as a Polars LazyFrame.

    Returns:
        The same data with measurement codes normalized.
    """
    for col_name in MEASUREMENT_CODE_COLS:
        col = pl.col(col_name)
        expr = pl.when(col == "").then(None)
        for old_val, new_val in MATS_MEASUREMENT_CODES.items():
            expr = expr.when(col == old_val).then(pl.lit(new_val))
        lf = lf.with_columns(expr.otherwise(col).alias(col_name))
    return lf


def harmonize_eia_epa_orispl(
    lf: pl.LazyFrame,
    crosswalk_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Harmonize the ORISPL code to match the EIA data.

    The EIA plant IDs and EPA ORISPL codes almost match, but not quite. EPA has
    compiled a crosswalk that maps one set of IDs to the other. The crosswalk is
    integrated into the PUDL db.

    This function merges the crosswalk with the MATS data thus adding the official
    plant_id_eia column. In cases where there is no plant_id_eia value for a
    given plant_id_epa (i.e., this plant isn't in the crosswalk yet), we use
    fill_null() to add the plant_id_epa value to the plant_id_eia column. Because the
    plant_id_epa is almost always correct this is reasonable.

    EIA IDs are more correct so use the crosswalk to fix any erroneous EPA IDs and get
    rid of that column to avoid confusion.

    Note that this transformation needs to be run *before* convert_to_utc, because
    convert_to_utc uses the plant ID to look up timezones.

    Args:
        lf: A MATS hourly LazyFrame.
        crosswalk_lf: The core_epa__assn_eia_epacamd table as a Polars LazyFrame.

    Returns:
        The same data, with the ORISPL plant codes corrected to match the EIA plant IDs.
    """
    return lf.join(
        crosswalk_lf.select(
            [
                "plant_id_eia",
                "plant_id_epa",
                "emissions_unit_id_epa",
            ]
        )
        .unique()
        .sort(["plant_id_eia", "emissions_unit_id_epa"]),
        on=["plant_id_epa", "emissions_unit_id_epa"],
        how="left",
        coalesce=True,
    ).with_columns(pl.col("plant_id_eia").fill_null(pl.col("plant_id_epa")))


def convert_to_utc(lf: pl.LazyFrame, plant_utc_offset: pl.LazyFrame) -> pl.LazyFrame:
    """Convert MATS datetime data to UTC timezones.

    Transformations include:

    * Account for timezone differences with offset from UTC.

    Args:
        lf: MATS hourly data as a Polars LazyFrame.
        plant_utc_offset: Associated plant UTC offsets as a Polars LazyFrame.

    Returns:
        The same data, with an operating_datetime_utc column added and the op_date and
        op_hour columns removed.
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
        .with_columns(
            operating_datetime_utc=pl.col("op_datetime_naive") - pl.col("utc_offset")
        )
        .drop(["op_date", "op_hour", "op_datetime_naive", "utc_offset"])
    )


def _load_plant_utc_offset(core_eia__entity_plants: pl.DataFrame) -> pl.DataFrame:
    """Load the UTC offset for each EIA plant.

    MATS times don't change for DST, so we get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        core_eia__entity_plants: EIA plants DataFrame.

    Returns:
        Polars DataFrame of applicable timezones taken from the core_eia__entity_plants
        table.
    """
    logger.debug("Creating plant UTC offset DataFrame")

    jan1 = datetime.datetime(2011, 1, 1)

    timezone_offset_map = {
        tz: pytz.timezone(tz).localize(jan1).utcoffset()
        for tz in core_eia__entity_plants.get_column("timezone")
        .drop_nulls()
        .unique()
        .to_list()
    }

    return (
        core_eia__entity_plants.select(["plant_id_eia", "timezone"])
        .drop_nulls()
        .unique()
        .with_columns(
            utc_offset=pl.col("timezone").replace(timezone_offset_map, default=None)
        )
        .select(["plant_id_eia", "utc_offset"])
    )


def _validate_crosswalk_uniqueness(crosswalk_df: pl.DataFrame) -> None:
    """Validate that crosswalk has unique plant_id_eia values per EPA plant/unit.

    This validation is done separately to avoid materializing the LazyFrame during
    transformation.

    Args:
        crosswalk_df: A polars DataFrame of the core_epa__assn_eia_epacamd table.

    Raises:
        AssertionError: If crosswalk has multiple plant_id_eia values for a single EPA
        identifier.
    """
    logger.debug("Validating crosswalk uniqueness")

    num_violations = (
        crosswalk_df.group_by(["plant_id_epa", "emissions_unit_id_epa"])
        .agg(pl.col("plant_id_eia").n_unique().alias("unique_eia_plants"))
        .filter(pl.col("unique_eia_plants") > 1)
        .height
    )

    if num_violations > 0:
        logger.error(
            f"Found {num_violations} EPA plant/unit combinations with multiple EIA plant IDs"
        )
        raise AssertionError(
            "The core_epa__assn_eia_epacamd crosswalk has more than one plant_id_eia "
            "value per plant_id_epa and emissions_unit_id_epa group"
        )


def transform_epamats(
    raw_lf: pl.LazyFrame,
    core_epa__assn_eia_epacamd: pl.DataFrame,
    plant_utc_offset: pl.DataFrame,
) -> pl.LazyFrame:
    """Transform EPA MATS hourly data and ready it for export to Parquet.

    Args:
        raw_lf: LazyFrame pointing to raw EPA MATS data.
        core_epa__assn_eia_epacamd: EPA-EIA crosswalk DataFrame.
        plant_utc_offset: Plant UTC offset DataFrame.

    Returns:
        A transformed LazyFrame of EPA MATS data.
    """
    return (
        raw_lf.pipe(_map_measurement_codes)
        .pipe(apply_pudl_dtypes_polars, resource="core_epamats__hourly_emissions")
        .with_columns(
            emissions_unit_id_epa=pl.when(
                pl.col("emissions_unit_id_epa").str.contains(r"^\d+$")
            )
            .then(pl.col("emissions_unit_id_epa").str.strip_chars_start("0"))
            .otherwise(pl.col("emissions_unit_id_epa"))
        )
        .pipe(harmonize_eia_epa_orispl, core_epa__assn_eia_epacamd.lazy())
        .pipe(convert_to_utc, plant_utc_offset=plant_utc_offset.lazy())
        .with_columns(
            steam_load_lbs=(pl.col("steam_load_1000_lbs") * 1000),
        )
        .drop("steam_load_1000_lbs")
        .pipe(apply_pudl_dtypes_polars, resource="core_epamats__hourly_emissions")
    )


@dg.asset(
    required_resource_keys={"global_data_config", "pudl_paths"},
    io_manager_key="parquet_io_manager",
)
def core_epamats__hourly_emissions(
    context,
    raw_epamats__hourly_emissions: pd.DataFrame,
    _core_epa__assn_eia_epacamd_unique: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> pl.LazyFrame:
    """Transform raw EPA MATS hourly emissions data and write to Parquet."""
    unique_crosswalk = pl.DataFrame(_core_epa__assn_eia_epacamd_unique)
    _validate_crosswalk_uniqueness(unique_crosswalk)
    plant_utc_offset = _load_plant_utc_offset(pl.DataFrame(core_eia__entity_plants))

    return transform_epamats(
        pl.DataFrame(raw_epamats__hourly_emissions).lazy(),
        core_epa__assn_eia_epacamd=unique_crosswalk,
        plant_utc_offset=plant_utc_offset,
    )
