"""Extract EIA Form 930 data from CSVs.

EIA Form 930 is reported in half-year increments. Each half-year has three
separate pages, which are stored as separate CSVs:
"balance", "interchange", and "subregion." See
https://catalystcoop-pudl.readthedocs.io/en/latest/data_sources/eia930.html for
more information.

We extract these CSVs into DuckDB, rename the columns as per the column map, and
dump out the concatenated pages to Parquet.
"""

import re
from pathlib import Path

import duckdb
from dagster import asset

import pudl.logging_helpers
from pudl.extract.extractor import (
    GenericMetadata,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


@asset(required_resource_keys={"datastore", "dataset_settings"})
def raw_eia930__balance(context) -> Path:
    """Raw balance page."""
    return extract_page(
        datastore=context.resources.datastore,
        page="balance",
        half_years=context.resources.dataset_settings.eia.eia930.half_years,
    )


@asset(required_resource_keys={"datastore", "dataset_settings"})
def raw_eia930__interchange(context) -> Path:
    """Raw interchange page."""
    return extract_page(
        datastore=context.resources.datastore,
        page="interchange",
        half_years=context.resources.dataset_settings.eia.eia930.half_years,
    )


@asset(required_resource_keys={"datastore", "dataset_settings"})
def raw_eia930__subregion(context) -> Path:
    """Raw subregion page - only exists after 2018h2."""
    return extract_page(
        datastore=context.resources.datastore,
        page="subregion",
        half_years=[
            h
            for h in context.resources.dataset_settings.eia.eia930.half_years
            if h >= "2018half2"
        ],
    )


def extract_page(
    datastore: Datastore,
    page: str,
    half_years: list[str],
) -> Path:
    """Pull data for a page across many half-years into a Parquet file.

    This involves reading each half-year, of course, but also concatenating them
    together and expanding the schema to fit all the columns we see.

    If we were to return the `con.query()` and use an IOManager to manage
    the Parquet IO, we would have to manage the DuckDB connection lifetime
    to avoid trying to write out from a closed DuckDB connection. So, we just
    write out directly in this asset and return a Path that we can pass to
    ``pd.read_parquet``, ``pl.scan_parquet``, or any other Parquet reading
    strategy.

    Args:
        datastore: the Datastore we use to actually access the raw data.
        page: the name of the page we're extracting.
        half_years: the set of half-year segments we're extracting.

    Returns:
        The path to the resulting Parquet file.
    """
    con = duckdb.connect()
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
    output_path = PudlPaths().parquet_path(f"raw_eia930__{page}")
    all_partitions = con.query(union_query)
    all_partitions.to_parquet(str(output_path))
    return output_path


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
