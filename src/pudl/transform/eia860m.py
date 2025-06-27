"""Module to perform data cleaning functions on EIA860m data tables."""

import pandas as pd
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(
    io_manager_key="pudl_io_manager",
    compute_kind="pandas",
    op_tags={"memory-use": "high"},
)
def core_eia860m__changelog_generators(
    raw_eia860m__generator_proposed: pd.DataFrame,
    raw_eia860m__generator_existing: pd.DataFrame,
    raw_eia860m__generator_retired: pd.DataFrame,
) -> pd.DataFrame:
    """Changelog of EIA-860M Generators based on operating status.

    The monthly reported EIA-860M tables includes existing, proposed and retired
    generators. This table combines all monthly reported data and preserves the first
    reported record when any new information about the generator was reported.

    We are not putting this table through PUDL's standard normalization process for EIA
    tables (see :func:`pudl.transform.eia.harvest_entity_tables`). EIA-860M includes
    provisional data reported monthly so it changes frequently compared to the more
    stable annually reported EIA data. If we fed all of the EIA-860M data into the
    harvesting process, we would get failures because the records from EIA-80m are too
    inconsistent for our thresholds for harvesting canonical values for entities. A
    ramification of this table not being harvested is that if there are any entities
    (generators, plants, utilities) that were only ever reported in an older EIA-860M
    file, there will be no record of it in the PUDL entity or SCD tables. Therefore,
    this asset cannot have foreign key relationships with the rest of the core EIA
    tables.

    """
    # compile all of the columns so these 860m bbs have everything for the transform
    eia860_columns = pudl.helpers.dedupe_n_flatten_list_of_lists(
        [
            pudl.extract.excel.ExcelMetadata("eia860").get_all_columns(gen_table)
            for gen_table in [
                "generator_proposed",
                "generator_existing",
                "generator_retired",
                "generator",
            ]
        ]
    )
    eia860m_all = pudl.transform.eia860._core_eia860__generators(
        raw_eia860__generator_proposed=raw_eia860m__generator_proposed,
        raw_eia860__generator_existing=raw_eia860m__generator_existing,
        raw_eia860__generator_retired=raw_eia860m__generator_retired.assign(
            operational_status_code=pd.NA
        ),
        # pass an empty generator df here. 860 old years had one big gens tab
        # but 860m doesn't. we do this just to enable us to run the 860 transform
        # function. We add all of the columns to it so we don't have any errors
        # from missing columns
        raw_eia860__generator=pd.DataFrame(
            columns=list(eia860_columns)
        ).convert_dtypes(),
    ).assign(
        # In order to be able to compare the values in this table to those reported
        # elsewhere, we need to translate these categories to the associated codes, as
        # the strings associated with the codes vary from table to table. See the
        # core_eia__codes_sector_consolidated table for the canonical definition of the
        # codes.
        sector_id_eia=lambda df: df["sector_name_eia"].map(
            {
                "Electric Utility": 1,
                "IPP Non-CHP": 2,
                "IPP CHP": 3,
                "Commercial Non-CHP": 4,
                "Commercial CHP": 5,
                "Industrial Non-CHP": 6,
                "Industrial CHP": 7,
            }
        )
    )

    # Drop all columns that aren't part of EIA-860M prior to deduplication.
    eia860m_all = eia860m_all[
        [
            field.name
            for field in pudl.metadata.classes.Resource.from_id(
                "core_eia860m__changelog_generators"
            ).schema.fields
            if field.name != "valid_until_date"
        ]
    ]
    # there is one plant/gen that has duplicate values
    gens_idx = ["plant_id_eia", "generator_id", "report_date"]
    dupe_mask = (eia860m_all.plant_id_eia == 56032) & (eia860m_all.generator_id == "1")
    deduped = eia860m_all[dupe_mask].drop_duplicates(subset=gens_idx, keep="first")
    without_known_dupes = eia860m_all[~dupe_mask]
    eia860m_deduped = pd.concat([without_known_dupes, deduped])

    # Check whether we have truly deduplicated the dataframe.
    remaining_dupes = eia860m_deduped[
        eia860m_deduped.duplicated(subset=gens_idx, keep=False)
    ]
    if not remaining_dupes.empty:
        raise ValueError(
            f"Duplicate ownership slices found in 860m table: {remaining_dupes}"
        )

    gen_idx_no_date = [c for c in gens_idx if c != "report_date"]
    eia860m_all = pudl.helpers.expand_timeseries(
        df=eia860m_deduped,
        key_cols=gen_idx_no_date,
        date_col="report_date",
        freq="MS",
        fill_through_freq="month",
    )

    # assign a max report_date column for use in the valid_until_date column
    eia860m_all["report_date_max"] = eia860m_all.groupby(gen_idx_no_date)[
        "report_date"
    ].transform("max")
    # drop duplicates after sorting by date so we get the first appearance
    eia860m_changelog = eia860m_all.sort_values(
        by=["report_date"], ascending=True
    ).drop_duplicates(
        subset=[c for c in eia860m_all if c != "report_date"],
        keep="first",
    )

    report_date_max_mask = (
        eia860m_changelog["report_date"] == eia860m_changelog["report_date_max"]
    )
    eia860m_changelog.loc[~report_date_max_mask, "valid_until_date"] = (
        eia860m_changelog.sort_values(gens_idx, ascending=False)
        .groupby(gen_idx_no_date)["report_date"]
        .transform("shift")
        .fillna(eia860m_changelog.report_date_max)
    )
    # for all of the last month records, use the next month as the valid until date
    eia860m_changelog.loc[report_date_max_mask, "valid_until_date"] = (
        eia860m_changelog.report_date + pd.DateOffset(months=1)
    )
    return eia860m_changelog
