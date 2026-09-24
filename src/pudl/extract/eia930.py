"""Extract EIA Form 930 data from CSVs.

EIA Form 930 is reported in half-year increments. Each half-year has three
separate pages, which are stored as separate CSVs:
"balance", "interchange", and "subregion." See
https://docs.catalyst.coop/pudl/en/latest/data_sources/eia930.html for
more information.

We extract these CSVs into DuckDB, rename the columns as per the column map, and
dump out the concatenated pages to Parquet.

EIA Form 930 also includes an Excel spreadsheet containing reference tables with
information about the BAs, sub-BAs, and codes used in the EIA 930 data. We
process these using the standard Excel extractor.
"""

import re
from io import BytesIO

import duckdb
import pandas as pd
from dagster import AssetOut, Output, asset, multi_asset

import pudl.logging_helpers
from pudl.extract import excel
from pudl.extract.extractor import (
    GenericMetadata,
    raw_df_factory,
)
from pudl.helpers import ParquetData, persist_table_as_parquet
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


@asset(required_resource_keys={"datastore", "global_data_config"})
def raw_eia930__balance(context) -> ParquetData:
    """Raw balance page."""
    return extract_page(
        datastore=context.resources.datastore,
        page="balance",
        half_years=context.resources.global_data_config.pudl.eia.eia930.half_years,
    )


@asset(required_resource_keys={"datastore", "global_data_config"})
def raw_eia930__interchange(context) -> ParquetData:
    """Raw interchange page."""
    return extract_page(
        datastore=context.resources.datastore,
        page="interchange",
        half_years=context.resources.global_data_config.pudl.eia.eia930.half_years,
    )


@asset(required_resource_keys={"datastore", "global_data_config"})
def raw_eia930__subregion(context) -> ParquetData:
    """Raw subregion page - only exists after 2018h2."""
    return extract_page(
        datastore=context.resources.datastore,
        page="subregion",
        half_years=[
            h
            for h in context.resources.global_data_config.pudl.eia.eia930.half_years
            if h >= "2018half2"
        ],
    )


def extract_page(
    datastore: Datastore,
    page: str,
    half_years: list[str],
) -> ParquetData:
    """Pull data for a page across many half-years into a Parquet file.

    This involves reading each half-year, of course, but also concatenating them
    together and expanding the schema to fit all the columns we see.

    Args:
        datastore: the Datastore we use to actually access the raw data.
        page: the name of the page we're extracting.
        half_years: the set of half-year segments we're extracting.

    Returns:
        ParquetData pointing to parquet file with raw table data.
    """
    with duckdb.connect() as con:
        individual_views = [
            extract_half_year_page(
                con,
                datastore=datastore,
                half_year=half_year,
                page=page,
            )
            for half_year in half_years
        ]
        union_query = " UNION ALL BY NAME ".join(
            f"SELECT * FROM {view_name}"  # noqa: S608 (we trust this view name)
            for view_name in individual_views
        )
        all_partitions = con.query(union_query)
        return persist_table_as_parquet(
            all_partitions,
            table_name=f"raw_eia930__{page}",
            use_native_duckdb_writer=True,
        )


def extract_half_year_page(
    con: duckdb.DuckDBPyConnection,
    datastore: Datastore,
    half_year: str,
    page: str,
) -> str:
    """Extract data from a single CSV.

    Reads into DuckDB for speed and memory use. To avoid reading the whole CSV
    into memory, we're extracting directly to a temporary directory.

    Args:
        con: DuckDB connection.
        datastore: the Datastore we use to actually access the input data.
        half_year: the half-year we're reading in.
        page: the name of the page we're reading.

    Returns:
        view_name: name of DuckDB view that represents the read & renamed CSV.
    """
    dataset_name = "eia930"
    filename = f"eia930-{half_year}-{page}.csv"
    metadata = GenericMetadata(dataset_name)
    column_map = metadata.get_column_map(page=page, half_year=half_year)

    with datastore.get_zipfile_resource(dataset_name, half_year=half_year) as zf:
        unzipped = zf.extract(filename, datastore.temporary_extraction_dir.name)
    csv_rel = con.read_csv(unzipped)

    def clean_name(name):
        return re.sub(r"\W+", "_", name.lower()).strip("_")

    cleaned_column_names = {clean_name(col_name) for col_name in csv_rel.columns}
    assert cleaned_column_names == set(column_map.keys())

    select_existing_columns = [
        f'"{col_name}" as "{column_map[clean_name(col_name)]}"'
        for col_name in csv_rel.columns
    ]

    expected_null_cols = set(metadata.get_all_columns(page)) - set(column_map.values())
    select_null_columns = [f"NULL as {col_name}" for col_name in expected_null_cols]

    view_name = f"v_{clean_name(filename)}"
    csv_rel.select(", ".join(select_existing_columns + select_null_columns)).to_view(
        view_name
    )
    return view_name


class Extractor(excel.ExcelExtractor):
    """Extractor for EIA form 930 reference tables."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.ExcelMetadata("eia930")
        self.cols_added = []
        self.BLACKLISTED_PAGES = [
            "balance",
            "interchange",
            "subregion",
        ]  # Actually CSVs
        super().__init__(*args, **kwargs)

    def load_source(self, page: str, **partition: dict) -> pd.DataFrame:
        """Override this method to expect 'all' partition, grab unzipped file directly."""
        # Only valid when the override is something like {"half_year": "all"}
        part = next(iter(partition.values()))
        if part != "all":
            raise ValueError(f"Unsupported EIA930 custom partition: {partition}")

        excel_file = pd.ExcelFile(
            BytesIO(self.ds.get_unique_resource(self._dataset_name, **partition)),
            engine="calamine",
        )

        return pd.read_excel(
            excel_file,
            sheet_name=self._metadata.get_sheet_name(page, **partition),
            skiprows=self._metadata.get_skiprows(page, **partition),
            skipfooter=self._metadata.get_skipfooter(page, **partition),
            dtype=self.get_dtypes(page, **partition),
        )


raw_eia930__all_dfs = raw_df_factory(
    Extractor, name="eia930", partition_override={"half_year": "all"}
)


@multi_asset(
    outs={
        table_name: AssetOut(is_required=False)
        for table_name in sorted(
            (
                "raw_eia930__codes_balancing_authorities",
                "raw_eia930__codes_regions",
                "raw_eia930__codes_balancing_authority_subregions",
                # "raw_eia930__codes_connections",
                # "raw_eia930__codes_energy_sources",
            )
        )
    },
    can_subset=True,
)
def extract_eia930(context, raw_eia930__all_dfs):
    """Extract raw EIA-930 data from excel sheets into dataframes."""
    # create descriptive table_names
    raw_eia930__all_dfs = {
        "raw_eia930__codes_" + table_name: df
        for table_name, df in raw_eia930__all_dfs.items()
    }

    raw_eia930__all_dfs = dict(sorted(raw_eia930__all_dfs.items()))
    selected_outputs = set(context.selected_output_names)

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in raw_eia930__all_dfs.items()
        if table_name in selected_outputs
    )
