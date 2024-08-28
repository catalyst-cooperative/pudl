"""Retrieves data from PHMSA natural gas spreadsheets for analysis.

This modules pulls data from PHMSA's published Excel spreadsheets.
"""

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl.logging_helpers
from pudl.extract import excel
from pudl.extract.extractor import raw_df_factory

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.ExcelExtractor):
    """Extractor for the excel dataset PHMSA."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.ExcelMetadata("phmsagas")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_renamed(self, newdata: pd.DataFrame, page: str, **partition):
        """Drop columns that get mapped to other assets and columns with unstructured data.

        Older years of PHMSA data have one Excel tab in the raw data, while newer data
        has multiple tabs. To extract data into tables that follow the newer data format
        without duplicating the older data, we need to split older pages into multiple
        tables by column. To prevent each table from containing all columns from these
        older years, filter by the list of columns specified for the page, with a
        warning.
        """
        if (int(partition["year"]) < 2010) and (
            self._metadata.get_form(page) == "gas_transmission_gathering"
        ):
            to_drop = [
                c
                for c in newdata.columns
                if c not in self._metadata.get_all_columns(page)
                and c not in self.cols_added
            ]
            str_part = str(list(partition.values())[0])
            if to_drop:
                logger.info(
                    f"{page}/{str_part}: Dropping columns that are not mapped to this asset:"
                    f"\n{to_drop}"
                )
                newdata = newdata.drop(columns=to_drop, errors="ignore")
        # there is an annoying middling number of columns in phmsa raw data that are unnamed
        # and have a smattering of random values. we want to drop these guys. but we are going
        # to enumerate what we expect to need to drop so if lots of new unmapped columns happen
        # unexpectedly a warning will happen here (and a extraction validation error will happen)
        # FYI: In 2009 there were a ton of extra columns seemingly from two records that had a
        # multi-line comment that shifted the rest of the cells over. Presumably we could map
        # them all and do a manual shift of the data. But its only 2 records so rn we are just
        # droppign them.
        unnamed_page_years = {
            "yearly_distribution": [
                2000,
                2001,
                2002,
                2003,
                2004,
                2005,
                2006,
                2007,
                2009,
            ]
        }
        unnamed_columns = newdata.filter(like="unnamed").columns
        if (page in unnamed_page_years) and (
            int(partition["year"]) in unnamed_page_years[page]
        ):
            newdata = newdata.drop(columns=unnamed_columns)
        elif not unnamed_columns.empty:
            logger.warning(
                "We found some unnamed columns that are probably not expected. "
                f"Consider dropping them. Columns found: {unnamed_columns}"
            )
        return newdata


raw_phmsagas__all_dfs = raw_df_factory(Extractor, name="phmsagas")


@multi_asset(
    outs={
        table_name: AssetOut()
        for table_name in sorted(
            (
                "raw_phmsagas__yearly_distribution",
                "raw_phmsagas__yearly_gathering_pipe_miles_by_nps",
                "raw_phmsagas__yearly_transmission_gathering_summary_by_commodity",
                "raw_phmsagas__yearly_transmission_pipe_miles_by_nps",
                "raw_phmsagas__yearly_transmission_gathering_inspections_assessments",
                "raw_phmsagas__yearly_transmission_gathering_pipe_miles_by_class_location",
                "raw_phmsagas__yearly_transmission_material_verification",
                "raw_phmsagas__yearly_transmission_hca_miles_by_determination_method_and_risk_model",
                "raw_phmsagas__yearly_transmission_miles_by_pressure_test_range_and_internal_inspection",
                "raw_phmsagas__yearly_transmission_gathering_preparer_certification",
                "raw_phmsagas__yearly_transmission_pipe_miles_by_smys",
                "raw_phmsagas__yearly_transmission_gathering_failures_leaks_repairs",
                "raw_phmsagas__yearly_transmission_miles_by_maop",
                "raw_phmsagas__yearly_transmission_gathering_pipe_miles_by_decade_installed",
                "raw_phmsagas__yearly_transmission_gathering_pipe_miles_by_material",
            )
        )
    }
)
def extract_phmsagas(raw_phmsagas__all_dfs):
    """Extract raw PHMSA gas data from excel sheets into dataframes."""
    # create descriptive table_names
    raw_phmsagas__all_dfs = {
        "raw_phmsagas__" + table_name: df
        for table_name, df in raw_phmsagas__all_dfs.items()
    }
    raw_phmsagas__all_dfs = dict(sorted(raw_phmsagas__all_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in raw_phmsagas__all_dfs.items()
    )
