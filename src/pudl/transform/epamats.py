"""Module to perform data cleaning functions on EPA MATS data tables.

Several transformation steps are identical to those used for EPA CEMS data
(crosswalk-based plant ID harmonization, UTC conversion, plant UTC offset
loading, and crosswalk validation), so they are imported directly from
``pudl.transform.epacems`` rather than duplicated here.
"""

import dagster as dg
import pandas as pd
import polars as pl

import pudl.logging_helpers
from pudl.metadata.dtypes import apply_pudl_dtypes_polars
from pudl.transform.epacems import (
    _load_plant_utc_offset,
    _validate_crosswalk_uniqueness,
    convert_to_utc,
    harmonize_eia_epa_orispl,
)

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

HF_COLUMNS: list[str] = [
    "hf_output_rate_lb_per_mwh",
    "hf_input_rate_lb_per_mmbtu",
    "hf_mass_lbs",
    "hf_mass_measurement_code",
]
"""HF emissions columns in the raw MATS data.

MATS does not require reporting of hydrogen fluoride (HF) emissions, so these
columns are expected to be entirely null. They are dropped from the core table;
see :func:`_validate_and_drop_hf_columns`.
"""


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


def _validate_and_drop_hf_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Assert all HF columns are null, then drop them from the data.

    TODO: MATS does not require reporting of hydrogen fluoride (HF) emissions,
    so all four HF columns are entirely null in the raw data. Rather than
    carrying them through the core table, we assert that they're empty and then
    drop them. If EPA ever starts reporting HF emissions, this assertion will
    fail loudly and we can decide whether to keep the columns.

    Args:
        lf: MATS hourly data as a Polars LazyFrame.

    Returns:
        The same data, without the four all-null HF columns.
    """
    # Count the non-null values in each HF column. This materializes only the
    # four HF columns, which is cheap relative to the rest of the transform.
    non_null_counts = (
        lf.select([pl.col(col).count().alias(col) for col in HF_COLUMNS])
        .collect()
        .row(0)
    )
    assert all(count == 0 for count in non_null_counts), (
        "Expected all HF columns to be null, but found non-null values: "
        f"{dict(zip(HF_COLUMNS, non_null_counts, strict=True))}"
    )
    return lf.drop(HF_COLUMNS)


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
        .pipe(_validate_and_drop_hf_columns)
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
