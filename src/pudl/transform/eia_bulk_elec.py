"""Clean and normalize EIA bulk electricity data."""
from functools import reduce
from io import StringIO

import pandas as pd

# modified from:
# meta.groupby(["sector", "sector_code"])['description'].first().str.strip('; ').str.split(';', expand=True)
EIA_SECTOR_AGGREGATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
sector,is_cogen,aggregate,sector_code_eia,aggregate_description_eia
commercial,True,all sectors,99,All sectors
commercial,False,all sectors,99,All sectors
industrial,True,all sectors,99,All sectors
industrial,False,all sectors,99,All sectors
electric utility,True,all sectors,99,All sectors
electric utility,False,all sectors,99,All sectors
independent power producers,True,all sectors,99,All sectors
independent power producers,False,all sectors,99,All sectors
industrial,True,all industrial (total),97,Power plants in the industrial sector
industrial,False,all industrial (total),97,Power plants in the industrial sector
commercial,True,all commercial (total),96,Power plants in the commercial sector
commercial,False,all commercial (total),96,Power plants in the commercial sector
electric utility,True,electric utility,1,Power plants owned by regulated electric utilties
electric utility,False,electric utility,1,Power plants owned by regulated electric utilties
independent power producers,True,independent power producers (total),94,Power plants owned by unregulated power companies (also called merchant generators)
independent power producers,False,independent power producers (total),94,Power plants owned by unregulated power companies (also called merchant generators)
electric utility,True,electric power (total),98,Power plants owned by companies whose primary purpose is to produce power
electric utility,False,electric power (total),98,Power plants owned by companies whose primary purpose is to produce power
independent power producers,True,electric power (total),98,Power plants owned by companies whose primary purpose is to produce power
independent power producers,False,electric power (total),98,Power plants owned by companies whose primary purpose is to produce power
"""
    ),
)
"""Association table describing the many-to-many relationships between plant sectors and various levels of EIA's aggregates."""


# modified from:
# meta.groupby(["fuel", "sector_code"])['description'].first().str.strip('; ').str.split(';', expand=True)
# PEL is defined as "Summation of all petroleum liquids (distallte fuel oil, jet fuel, residual fuel oil, kerosense waste oil and other petroleum liquids)"
# Missing from this list are all the "other" categories of gases: OG, BFG, SGP, SC, PG
# Those gases combine for about the same total MMBTU as DFO, about 0.2% of all reported fuel receipts
EIA_FUEL_AGGREGATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
aggregate_fuel,aggregate_fuel_code,energy_source_code_eia
bituminous coal,BIT,BIT
subbituminous coal,SUB,SUB
lignite coal,LIG,LIG
coal,COW,BIT
coal,COW,SUB
coal,COW,LIG
coal,COW,WC
natural gas,NG,NG
petroleum coke,PC,PC
petroleum liquids,PEL,DFO
petroleum liquids,PEL,RFO
petroleum liquids,PEL,JF
petroleum liquids,PEL,KER
petroleum liquids,PEL,WO
    """
    ),
)
"""Association table describing the many-to-many relationships between fuel types and various levels of EIA's aggregates."""


# modified from:
# meta.groupby(["region", "region_code"])['description'].first().str.strip('; ').str.split(';', expand=True)[[2]]
# The raw data was missing Connecticut
CENSUS_REGION_STATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
region,region_code,state_name,state_abbrev
East North Central,ENC,Illinois,IL
East North Central,ENC,Michigan,MI
East North Central,ENC,Wisconsin,WI
East North Central,ENC,Indiana,IN
East North Central,ENC,Ohio,OH
East South Central,ESC,Mississippi,MS
East South Central,ESC,Alabama,AL
East South Central,ESC,Tennessee,TN
East South Central,ESC,Kentucky,KY
Middle Atlantic,MAT,New York,NY
Middle Atlantic,MAT,Pennsylvania,PA
Middle Atlantic,MAT,New Jersey,NJ
Mountain,MTN,Idaho,ID
Mountain,MTN,Montana,MT
Mountain,MTN,Wyoming,WY
Mountain,MTN,Colorado,CO
Mountain,MTN,Arizona,AZ
Mountain,MTN,New Mexico,SN
Mountain,MTN,Utah,UT
Mountain,MTN,Nevada,NV
New England,NEW,Maine,ME
New England,NEW,Rhode Island,RI
New England,NEW,New Hampshire,NH
New England,NEW,Vermont,VT
New England,NEW,Connecticut,CT
New England,NEW,Massaschusetts,MA
Pacific Contiguous,PCC,Washington,WA
Pacific Contiguous,PCC,California,CA
Pacific Contiguous,PCC,Oregon,OR
Pacific Noncontiguous,PCN,Alaska,AK
Pacific Noncontiguous,PCN,Hawaii,HI
South Atlantic,SAT,West Virginia,WV
South Atlantic,SAT,Virginia,VA
South Atlantic,SAT,South Carolina,SC
South Atlantic,SAT,District of Columbia,DC
South Atlantic,SAT,Georgia,GA
South Atlantic,SAT,Florida,FL
South Atlantic,SAT,Maryland,MD
South Atlantic,SAT,North Carolina,NC
South Atlantic,SAT,Delaware,DE
West North Central,WNC,South Dakota,SD
West North Central,WNC,North Dakota,ND
West North Central,WNC,Minnesota,MN
West North Central,WNC,Nebraska,NE
West North Central,WNC,Missouri,MO
West North Central,WNC,Kansas,KS
West North Central,WNC,Iowa,IA
West South Central,WSC,Texas,TX
West South Central,WSC,Louisiana,LA
West South Central,WSC,Oklahoma,OK
West South Central,WSC,Arkansas,AR
    """
    ),
)
"""Association table describing the many-to-one relationships between states and Census Regions used in EIA's aggregates."""


def _get_empty_col_names(metadata: pd.DataFrame) -> set[str]:
    all_nan = metadata.isna().all(axis=0)
    all_none = metadata.eq("None").all(axis=0)
    to_drop = all_nan | all_none
    dropped_col_names = set(metadata.columns[to_drop])
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
    diff = dropped_col_names.symmetric_difference(expected_to_drop)
    assert diff == set(), f"Unexpected dropped columns: {diff}"
    return dropped_col_names


def _get_redundant_frequency_col_names(metadata: pd.DataFrame) -> set[str]:
    freq_map = {
        "monthly": "M",
        "quarterly": "Q",
        "annual": "A",
    }
    assert (
        metadata["frequency_code"].eq(metadata["frequency"].map(freq_map)).all(axis=0)
    ), "Conflicting information between 'frequency_code' and 'frequency'."
    assert (
        metadata["frequency_code"].eq(metadata["f"]).all(axis=0)
    ), "Conflicting information between 'frequency_code' and 'f'."
    # keep frequency_code for reference
    return {
        "f",
    }


def _get_constant_col_names(metadata: pd.DataFrame) -> set[str]:
    """Check for constant values."""
    zero_info = metadata.nunique() == 1
    expected = {"copyright", "source"}
    zero_info_cols = set(metadata.columns[zero_info])
    diff = zero_info_cols.symmetric_difference(expected)
    assert len(diff) == 0, f"Unexpected constant column: {diff}"
    return zero_info_cols


def _get_redundant_id_col_names(metadata: pd.DataFrame) -> set[str]:
    """Check for ID columns with redundant information."""
    geo_parts = metadata["geoset_id"].str.split("-", expand=True)
    reconstructed_geoset_id = geo_parts[0].str.cat(
        [metadata["region_code"].values, geo_parts[1].values], sep="-"
    )
    assert reconstructed_geoset_id.eq(
        metadata["series_id"]
    ).all(), "Unexpected information in 'geoset_id'."

    return {
        "geoset_id",
    }


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


def _transform_metadata(metadata: pd.DataFrame) -> pd.DataFrame:
    metadata = metadata.copy()
    key_codes = _extract_keys_from_series_id(metadata)
    key_names = _extract_keys_from_name(metadata)
    combined = pd.concat([metadata, key_names, key_codes], axis=1, copy=False)
    combined.loc[:, "last_updated"] = pd.to_datetime(combined.loc[:, "last_updated"])
    # name and description were parsed into other fields/tables so can be dropped
    useless_cols = list(_get_col_names_to_drop(combined)) + ["name", "description"]
    low_value_cols = [
        "start",  # better to re-calculate
        "end",  # better to re-calculate
        "iso3166",  # duplicate info
        "geography",  # duplicate info
    ]
    cols_to_drop = useless_cols + low_value_cols
    meta = combined.drop(columns=cols_to_drop)
    return meta


def _transform_timeseries(
    raw_ts: pd.DataFrame, transformed_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw timeseries to tidy format with full keys instead of an opaque ID.

    Keys: ("fuel", "region", "sector", "frequency", "timestamp")
    Values: "receipts_mmbtu", "cost_dollars_per_mmbtu"
    """
    metadata_keys = transformed_metadata.loc[
        :, ["series_id", "units", "fuel", "region", "sector", "frequency"]
    ]
    ts = raw_ts.merge(metadata_keys, on="series_id").drop(columns="series_id")
    ts = ts.set_index(
        ["units", "fuel", "region", "sector", "frequency", "date"]
    ).unstack("units")
    ts.columns = ts.columns.droplevel(level=None)

    # convert units from billion BTU to MMBTU for consistency with other PUDL tables
    ts.loc[:, "receipts_mmbtu"] = ts.loc[:, "billion Btu"] * 1000
    ts.drop(columns="billion Btu", inplace=True)

    ts.rename(
        columns={
            "dollars per million Btu": "cost_dollars_per_mmbtu",
        },
        inplace=True,
    )
    ts.columns.name = None

    return ts


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform raw EIA bulk electricity aggregates.

    Args:
        raw_dfs (dict[str, pd.DataFrame]): raw metadata and timeseries dataframes

    Returns:
        dict[str, pd.DataFrame]: transformed metadata and timeseries dataframes
    """
    meta = _transform_metadata(raw_dfs["metadata"])
    ts = _transform_timeseries(raw_dfs["timeseries"], meta)
    return {
        "metadata": meta,
        "timeseries": ts,
    }
