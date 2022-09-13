"""Module to extract aggregate data from the EIA bulk electricity download.

The file structure is a .zip archive with one large (1.1 GB).txt file containing
line-delimited JSON (each line is a separate JSON string). Each line
contains a single data series, such as average cost of fossil fuels for electricity generation
"""
from pathlib import Path

import pandas as pd


def _filter_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pick out the desired data series."""
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

    This produces a dataframe with all text fields. The timeseries data is
    left as JSON strings in the 'data' column. The other columns are metadata.
    """
    filtered = []
    # Use chunksize arg to reduce peak memory usage when reading in 1.1 GB file
    # For reference, the file has ~680k lines and we want around 8.5k
    with pd.read_json(
        raw_zipfile, compression="zip", lines=True, chunksize=10_000
    ) as reader:
        for chunk in reader:
            filtered.append(_filter_df(chunk))
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


def extract(raw_zipfile) -> dict[str, pd.DataFrame]:
    """Extract metadata and timeseries from raw EIA bulk electricity data.

    Args:
        raw_zipfile (file-like): Path or other file-like object that can be read by pd.read_json()

    Returns:
        Dict[str, pd.DataFrame]: dictionary of dataframes with keys 'metadata' and 'timeseries'
    """
    filtered = _filter_and_read_to_dataframe(raw_zipfile)
    timeseries = _parse_data_column(filtered)
    metadata = filtered.drop(columns="data")
    return {"metadata": metadata, "timeseries": timeseries}
