"""Module to extract aggregate data from the EIA bulk electricity download.

EIA's bulk electricity data contains 680,000 objects, most of which are timeseries.
These timeseries contain a variety of measures (fuel amount and cost are just two)
across multiple levels of aggregation from individual plants to national averages.

The data is formatted as a single 1.1GB text file of line-delimited JSON with one line
per object. Each JSON structure has two nested levels: the top level contains metadata
describing the series and the second level (under the "data" heading) contains an array
of timestamp/value pairs. This structure leads to a natural normalization into two
tables: one of metadata and one of timeseries. That is the format delivered by this
module.
"""
from io import BytesIO
from pathlib import Path

import pandas as pd

from pudl.workspace.datastore import Datastore


def _filter_for_fuel_receipts_costs_series(df: pd.DataFrame) -> pd.DataFrame:
    """Pick out the desired data series.

    Fuel receipts and costs are about 1% of the total lines. This function filters for
    series that contain the name "RECEIPTS_BTU" or "COST_BTU" in their ``series_id``.

    Of the approximately 680,000 objects in the dataset, about 19,000 represent things
    other than data series (such as category definitions or plot axes). Those
    non-series objects do not have a field called ``series_id``.
    The ``except KeyError:`` clause handles that situation.
    """
    series_to_extract = ["RECEIPTS_BTU", "COST_BTU"]
    series_pattern = "|".join(series_to_extract)
    pattern = rf"^ELEC\.(?:{series_pattern})"
    try:
        _filter = df.loc[:, "series_id"].str.match(pattern, na=False)
        return df.loc[_filter, :]
    except KeyError:
        return pd.DataFrame()  # empty


def _filter_and_read_to_dataframe(raw_zipfile: Path) -> pd.DataFrame:
    """Decompress and filter the 1100 MB file down to the 16 MB we actually want.

    This produces a dataframe with all text fields. The timeseries data is left as JSON
    strings in the 'data' column. The other columns are metadata.
    """
    filtered = []
    # Use chunksize arg to reduce peak memory usage when reading in 1.1 GB file
    # For reference, the file has ~680k lines and we want around 8.5k
    with pd.read_json(
        raw_zipfile, compression="zip", lines=True, chunksize=10_000
    ) as reader:
        for chunk in reader:
            filtered.append(_filter_for_fuel_receipts_costs_series(chunk))
    out = pd.concat(filtered, ignore_index=True)
    return out


def _parse_data_column(elec_df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for idx in elec_df.index:
        data_df = pd.DataFrame(elec_df.at[idx, "data"], columns=["date", "value"])
        # three possible date formats, only annual/quarterly handled by pd.to_datetime() automatically
        #   annual data as "YYYY" eg "2020"
        #   quarterly data as "YYYYQQ" eg "2020Q2"
        #   monthly data as "YYYYMM" eg "202004"
        is_monthly = (
            data_df.iloc[0:5, data_df.columns.get_loc("date")].str.match(r"\d{6}").all()
        )
        if is_monthly:
            data_df.loc[:, "date"] = pd.to_datetime(
                data_df.loc[:, "date"], format="%Y%m", errors="raise"
            )
        else:
            data_df.loc[:, "date"] = pd.to_datetime(
                data_df.loc[:, "date"], infer_datetime_format=True, errors="raise"
            )
        data_df["series_id"] = elec_df.at[idx, "series_id"]
        out.append(data_df)
    out = pd.concat(out, ignore_index=True, axis=0)
    out.loc[:, "series_id"] = out.loc[:, "series_id"].astype("category", copy=False)
    return out.loc[:, ["series_id", "date", "value"]]  # reorder cols


def _extract(raw_zipfile) -> dict[str, pd.DataFrame]:
    """Extract metadata and timeseries from raw EIA bulk electricity data.

    Args:
        raw_zipfile: Path or other file-like object that can be read by pd.read_json()

    Returns:
        Dictionary of dataframes with keys 'metadata' and 'timeseries'
    """
    filtered = _filter_and_read_to_dataframe(raw_zipfile)
    timeseries = _parse_data_column(filtered)
    metadata = filtered.drop(columns="data")
    return {"metadata": metadata, "timeseries": timeseries}


def extract(ds: Datastore) -> dict[str, pd.DataFrame]:
    """Extract metadata and timeseries from raw EIA bulk electricity data.

    Args:
        ds: Datastore object

    Returns:
        Dictionary of dataframes with keys 'metadata' and 'timeseries'
    """
    raw_zipfile = ds.get_unique_resource("eia_bulk_elec")
    dfs = _extract(BytesIO(raw_zipfile))
    return dfs
