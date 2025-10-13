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

    def load_source(self, page: str, **partition) -> pd.DataFrame:
        """Run same load_source and then replace all periods w/ n's.

        There are a ton of identical column names in the raw dataset for 1984.
        Typically these get processed in load_source via pd.read_excel which adds
        a period and then an auto-incremented number as a suffix. Then this
        data gets run through :func:`pudl.helpers.simplify_columns` which converts
        all non-alphanumeric (aka periods) into spaces and then condenses any multiple
        spaces into one space. This would all be fine and good except for the fact that
        there are 22 column names that are identical expect for trailing spaces in the
        raw source. These trailing spaces effectively get removed in
        :func:`pudl.helpers.simplify_columns` and then we have duplicate column names.
        This method runs the parent adds _n#'s to these trailing space column names.
        """
        df = super().load_source(page, **partition)
        if (int(partition["year"]) == 1984) and (page == "yearly_distribution"):
            df.columns = df.columns.str.replace(" .", "n")
            # the first two iterations of these columns that have trailing spaces
            # don't get the .# at the end and such need this special treatment
            ends_w_space_cols = [col for col in df.columns if col.endswith(" ")]
            assert len(ends_w_space_cols) == 2
            df = df.rename(
                columns={raw_col: f"{raw_col}_n0" for raw_col in ends_w_space_cols}
            )
        return df

    def process_renamed(self, newdata: pd.DataFrame, page: str, **partition):
        """Drop columns that get mapped to other assets and columns with unstructured data.

        Old-ish years (1990-2009) of PHMSA data have one Excel tab in the raw data, while
        newer data has multiple tabs. To extract data into tables that follow the newer
        data format without duplicating the older data, we need to split older pages into
        multiple tables by column. To prevent each table from containing all columns from
        these older years, filter by the list of columns specified for the page, with a
        warning.

        The oldest years (before 1990) contain multiple years in one tab. The records
        contain a report_year column but some of them are reported at a two digit year
        (ex: 87 for 1987). We convert these into four digit years.
        """
        if (int(partition["year"]) <= 1998) and (page == "yearly_distribution"):
            newdata.report_year = newdata.report_year.astype(pd.Int64Dtype())
            double_digit_year_mask = (
                newdata["report_year"].astype("str").str.contains(r"^[0-9]{2}$")
            )
            newdata.loc[double_digit_year_mask, "report_year"] = (
                newdata.loc[double_digit_year_mask, "report_year"] + 1900
            )
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
        # dropping them.
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
