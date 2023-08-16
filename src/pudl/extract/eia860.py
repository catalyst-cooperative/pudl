"""Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""
from collections import defaultdict

import pandas as pd
from dagster import (
    AssetOut,
    DynamicOut,
    DynamicOutput,
    Output,
    graph_asset,
    multi_asset,
    op,
)

import pudl
import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings
from pudl.metadata.classes import DataSource

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA860."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("eia860")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe.

        * Rename columns based on our compiled spreadsheet metadata
        * Add report_year if it is missing
        * Add a flag indicating if record came from EIA 860, or EIA 860M
        * Fix any generator_id values with leading zeroes.
        """
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
        self.cols_added = ["report_year"]
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }


# TODO (bendnorman): Add this information to the metadata
raw_table_names = (
    "raw_boiler_cooling_eia860",
    "raw_boiler_generator_assn_eia860",
    "raw_boiler_info_eia860",
    "raw_boiler_mercury_eia860",
    "raw_boiler_nox_eia860",
    "raw_boiler_particulate_eia860",
    "raw_boiler_so2_eia860",
    "raw_boiler_stack_flue_eia860",
    "raw_cooling_equipment_eia860",
    "raw_emission_control_strategies_eia860",
    "raw_emissions_control_equipment_eia860",
    "raw_fgd_equipment_eia860",
    "raw_fgp_equipment_eia860",
    "raw_generator_eia860",
    "raw_generator_existing_eia860",
    "raw_generator_proposed_eia860",
    "raw_generator_retired_eia860",
    "raw_multifuel_existing_eia860",
    "raw_multifuel_retired_eia860",
    "raw_ownership_eia860",
    "raw_plant_eia860",
    "raw_stack_flue_equipment_eia860",
    "raw_utility_eia860",
)


@op(
    out=DynamicOut(),
    required_resource_keys={"dataset_settings"},
)
def eia_years_from_settings(context):
    """Return set of years for EIA in settings.

    These will be used to kick off worker processes to load each year of data in
    parallel.
    """
    eia_settings = context.resources.dataset_settings.eia
    for year in eia_settings.eia860.years:
        yield DynamicOutput(year, mapping_key=str(year))


@op(
    required_resource_keys={"datastore", "dataset_settings"},
)
def load_single_year(context, year: int) -> dict[str, pd.DataFrame]:
    """Load a single year of EIA data from file.

    Args:
        context:
            context: dagster keyword that provides access to resources and config.
        year:
            Year to load.

    Returns:
        Loaded data in a dataframe.
    """
    ds = context.resources.datastore
    return Extractor(ds).extract(year=[year])


@op
def merge_eia860_years(
    yearly_dfs: list[dict[str, pd.DataFrame]]
) -> dict[str, pd.DataFrame]:
    """Merge yearly EIA-860 dataframes."""
    merged = defaultdict(list)
    for dfs in yearly_dfs:
        for page in dfs:
            merged[page].append(dfs[page])

    for page in merged:
        merged[page] = pd.concat(merged[page])

    return merged


@graph_asset
def eia860_raw_dfs() -> dict[str, pd.DataFrame]:
    """All loaded EIA860 dataframes.

    This asset creates a dynamic graph of ops to load EIA860 data in parallel.
    """
    years = eia_years_from_settings()
    dfs = years.map(lambda year: load_single_year(year))
    return merge_eia860_years(dfs.collect())


# TODO (bendnorman): Figure out type hint for context keyword and mutli_asset return
@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia860(context, eia860_raw_dfs):
    """Extract raw EIA data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    eia_settings = context.resources.dataset_settings.eia
    ds = context.resources.datastore

    if eia_settings.eia860.eia860m:
        eia860m_data_source = DataSource.from_id("eia860m")
        eia860m_date = eia860m_data_source.working_partitions["year_month"]
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            year_month=eia860m_date
        )
        eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
            eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs
        )

    # create descriptive table_names
    eia860_raw_dfs = {
        "raw_" + table_name + "_eia860": df for table_name, df in eia860_raw_dfs.items()
    }
    eia860_raw_dfs = dict(sorted(eia860_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in eia860_raw_dfs.items()
    )
