"""Module to perform data cleaning functions on EPA CEMS data tables."""

import datetime
from pathlib import Path

import dagster as dg
import pandas as pd
import polars as pl
import pytz

import pudl.logging_helpers
from pudl.extract.epacems import extract_quarter
from pudl.metadata.fields import apply_pudl_dtypes_polars
from pudl.workspace.setup import PudlPaths

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
        crosswalk_lf: The core_epa__assn_eia_epacamd table as a Polars LazyFrame.

    Returns:
        The same data, with the ORISPL plant codes corrected to match the EIA plant IDs.
    """
    # Merge CEMS with Crosswalk to get correct EIA ORISPL code and fill in all unmapped
    # values with old plant_id_epa value.
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
    """Convert CEMS datetime data to UTC timezones.

    Transformations include:

    * Account for timezone differences with offset from UTC.

    Args:
        lf: CEMS hourly data for one year-state as a Polars LazyFrame.
        plant_utc_offset: Associated plant UTC offsets as a Polars LazyFrame.

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


def _load_plant_utc_offset(core_eia__entity_plants: pl.DataFrame) -> pl.DataFrame:
    """Load the UTC offset for each EIA plant.

    CEMS times don't change for DST, so we get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        core_eia__entity_plants: EIA plants DataFrame.

    Returns:
        Polars DataFrame of applicable timezones taken from the core_eia__entity_plants
        table.
    """
    logger.debug("Creating plant UTC offset DataFrame")

    # Create a mapping of unique timezones to offsets to avoid repeated calculations
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

    # Count distinct EIA plant IDs per EPA plant/unit combination
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


def transform_epacems(
    raw_lf: pl.LazyFrame,
    core_epa__assn_eia_epacamd: pl.DataFrame,
    plant_utc_offset: pl.DataFrame,
) -> pl.LazyFrame:
    """Transform EPA CEMS hourly data and ready it for export to Parquet.

    Args:
        raw_lf: LazyFrame pointing to raw EPA CEMS data.
        core_epa__assn_eia_epacamd: EPA-EIA crosswalk DataFrame.
        core_eia__entity_plants: EIA plants DataFrame.

    Returns:
        A single year_quarter of EPA CEMS data
    """
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
        .pipe(harmonize_eia_epa_orispl, core_epa__assn_eia_epacamd.lazy())
        .pipe(convert_to_utc, plant_utc_offset=plant_utc_offset.lazy())
        # Fix gross load that is orders of magnitude too high
        .with_columns(
            gross_load_mw=pl.when(pl.col("gross_load_mw") > 2000)
            .then(pl.col("gross_load_mw") / 1000)
            .otherwise(pl.col("gross_load_mw"))
        )
        .pipe(apply_pudl_dtypes_polars, group="epacems")
    )


def _partitioned_path() -> Path:
    partitioned_path = (
        PudlPaths().output_dir / "parquet" / "raw_epacems__hourly_emissions"
    )
    partitioned_path.mkdir(exist_ok=True)
    return partitioned_path


@dg.asset(
    required_resource_keys={"datastore", "dataset_settings"},
    io_manager_key="parquet_io_manager",
    op_tags={"memory-use": "high"},
)
def core_epacems__hourly_emissions(
    context,
    _core_epa__assn_eia_epacamd_unique: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> pl.LazyFrame:
    """Extract EPA CEMS data by quarter and write to partitioned Parquet files."""
    # Validate uniqueness once before processing all partitions
    unique_crosswalk = pl.DataFrame(_core_epa__assn_eia_epacamd_unique)
    _validate_crosswalk_uniqueness(unique_crosswalk)
    plant_utc_offset = _load_plant_utc_offset(pl.DataFrame(core_eia__entity_plants))
    partitioned_path = _partitioned_path()

    # Iterate over all the partitions we are processing, since polars is parallelized
    # internally and this will save us significant dagster process startup overhead and
    # avoid CPU resource contention.
    output_paths = []
    for year_quarter in context.resources.dataset_settings.epacems.year_quarters:
        output_path = partitioned_path / f"{year_quarter}.parquet"
        logger.info(f"Processing EPA CEMS {year_quarter}")

        extract_quarter(context, year_quarter).pipe(
            transform_epacems,
            core_epa__assn_eia_epacamd=unique_crosswalk,
            plant_utc_offset=plant_utc_offset,
        ).sink_parquet(
            output_path,
            engine="streaming",
            row_group_size=100_000,
        )

        output_paths.append(output_path)

    return pl.scan_parquet(output_paths, low_memory=True)


@dg.asset(
    ins={
        "core_epacems__hourly_emissions": dg.AssetIn(
            input_manager_key="pudl_io_manager"
        ),
    },
)
def _core_epacems__emissions_unit_ids(
    core_epacems__hourly_emissions: pl.LazyFrame,
) -> pd.DataFrame:
    """Make unique annual plant_id_eia and emissions_unit_id_epa.

    Returns:
        dataframe with unique set of: "plant_id_eia", "year" and "emissions_unit_id_epa"
    """
    return (
        core_epacems__hourly_emissions.select(
            ["plant_id_eia", "year", "emissions_unit_id_epa"]
        )
        .unique()
        .collect(engine="streaming")
    ).to_pandas()
