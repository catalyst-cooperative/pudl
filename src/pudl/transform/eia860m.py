"""Module to perform data cleaning functions on EIA860m data tables."""

import pandas as pd
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset
def core_eia860m__yearly_generators_changelog_eia860m(
    raw_eia860m__generator_proposed,
    raw_eia860m__generator_existing,
    raw_eia860m__generator_retired,
):
    """Changelog of EIA860m Generators based on operating status.

    The monthly reported EIA80m tables includes existing, proposed and retired
    generators. This table combines all monthly reported data and preserves the first
    reported record when a generator's was reported with a new operational status.
    """
    # compile all of the columns so these 860m bbs have everything for the transform
    eia860_columns = pudl.helpers.dedupe_n_flatten_list_of_lists(
        [
            pudl.extract.excel.Metadata("eia860").get_all_columns(gen_table)
            for gen_table in [
                "generator_proposed",
                "generator_existing",
                "generator_retired",
                "generator",
            ]
        ]
    )

    eia860m_all = (
        pudl.transform.eia860._core_eia860__generators(
            raw_eia860__generator_proposed=raw_eia860m__generator_proposed,
            raw_eia860__generator_existing=raw_eia860m__generator_existing,
            raw_eia860__generator_retired=raw_eia860m__generator_retired.assign(
                operational_status_code=pd.NA
            ),
            raw_eia860__generator=pd.DataFrame(columns=list(eia860_columns)),
        )
        # drop all the non 86om cols
        .dropna(how="all", axis="columns")
    )
    # TODO: make changelog re : operational_status_code
    return eia860m_all
