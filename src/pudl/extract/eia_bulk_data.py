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


def _read_to_dataframe(raw_zipfile: Path) -> pd.DataFrame:
    """Decompress and filter the 1100 MB file down to the 16 MB we actually want."""
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


def _extract_keys_from_series_id():
    raise NotImplementedError
