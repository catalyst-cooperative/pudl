"""Clean and normalize EIA bulk electricity data."""
from functools import reduce

import pandas as pd


def _get_empty_col_names(metadata: pd.DataFrame) -> set[str]:
    all_nan = metadata.isna().all()
    all_none = metadata.eq("None").all()
    to_drop = all_nan | all_none
    dropped_col_names = metadata.columns[to_drop]
    expected_to_drop = {
        "category_id",
        "childseries",
        "copyright",
        "lat",
        "latlon",
        "lon",
        "notes",
        "parent_category_id",
    }
    diff = set(dropped_col_names).symmetric_difference(expected_to_drop)
    assert diff == set(), f"Unexpected dropped columns: {diff}"
    return set(dropped_col_names)


def _get_redundant_frequency_col_names(metadata: pd.DataFrame) -> set[str]:
    freq_map = {
        "monthly": "M",
        "quarterly": "Q",
        "annual": "A",
    }
    assert (
        metadata["frequency_code"].eq(metadata["frequency"].map(freq_map)).all()
    ), "Conflicting information between 'frequency_code' and 'frequency'."
    assert (
        metadata["frequency_code"].eq(metadata["f"]).all()
    ), "Conflicting information between 'frequency_code' and 'f'."
    # keep frequency_code for reference
    return {
        "f",
    }


def _get_constant_col_names(metadata: pd.DataFrame) -> set[str]:
    zero_info = metadata.nunique() == 1
    expected = {"copyright", "source"}
    zero_info_cols = set(metadata.columns[zero_info])
    diff = zero_info_cols.symmetric_difference(expected)
    assert len(diff) == 0, f"Unexpected constant column: {diff}"
    return zero_info_cols


def _get_redundant_id_col_names(metadata: pd.DataFrame) -> set[str]:
    geo_parts = metadata["geoset_id"].str.split("-", expand=True)
    reconstructed_series_id = geo_parts[0].str.cat(
        [metadata["region_code"].values, geo_parts[1].values], sep="-"
    )
    assert reconstructed_series_id.eq(
        metadata["series_id"]
    ).all(), "Unexpected information in 'geoset_id'."

    reconstructed_name = metadata["series"].str.cat(
        metadata[["fuel", "region", "sector", "frequency"]].values, sep=" : "
    )
    assert reconstructed_name.eq(
        metadata["name"]
    ).all(), "Unexpected information in 'name'."
    return {"geoset_id", "name"}


def _get_col_names_to_drop(metadata: pd.DataFrame) -> set[str]:
    checks = [
        _get_empty_col_names,
        _get_redundant_frequency_col_names,
        _get_constant_col_names,
        _get_redundant_id_col_names,
    ]
    cols_to_drop = reduce(set.union, (check(metadata) for check in checks))
    return cols_to_drop


def _extract_keys_from_series_id(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Parse EIA series_id to key categories.

    Redundant information with 'name' field but with abbreviated codes instead of descriptive names.
    """
    # drop first one (constant value of "ELEC")
    keys = (
        raw_df.loc[:, "series_id"]
        .str.split(r"[\.-]", expand=True, regex=True)
        .drop(columns=0)
    )
    keys.columns = pd.Index(
        ["series_code", "fuel_code", "region_code", "sector_code", "frequency_code"]
    )
    return keys


def _extract_keys_from_name(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Parse EIA series name to key categories.

    Redundant information with series_id but with descriptive names instead of codes.
    """
    keys = raw_df.loc[:, "name"].str.split(" : ", expand=True)
    keys.columns = pd.Index(["series", "fuel", "region", "sector", "frequency"])
    return keys
