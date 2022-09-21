"""Clean and normalize EIA bulk electricity data."""
from functools import reduce
from io import StringIO

import pandas as pd

# modified from:
# meta.groupby(["sector", "sector_code"])['description'].first().str.strip('; ').str.split(';', expand=True)
EIA_SECTOR_AGGREGATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
sector_code_eia,sector_aggregate_code
5,99
4,99
7,99
6,99
1,99
3,99
2,99
7,97
6,97
5,96
4,96
1,1
3,94
2,94
1,98
3,98
2,98
"""
    ),
)
"""Association table describing the many-to-many relationships between plant sectors and various levels of EIA's aggregates."""


BULK_EIA_SECTOR_AGGREGATES = pd.read_csv(
    StringIO(
        """
sector_aggregate_code,aggregate_name,aggregate_description
99,all_sectors,All power plants
98,electric_power,Power plants owned by companies whose primary purpose is to produce power; both regulated and unregulated
97,all_industrial,Power plants in the industrial sector; both cogen and non-cogen
96,all_commercial,Power plants in the commercial sector; both cogen and non-cogen
94,all_ipp,Power plants owned by unregulated power companies (also called merchant generators); both cogen and non-cogen
1,electric_utility,Power plants owned by regulated electric utilties
"""
    ),
)
"""Names and descriptions of EIA sectoral aggregates."""


# modified from:
# meta.groupby(["fuel", "sector_code"])['description'].first().str.strip('; ').str.split(';', expand=True)
# PEL is defined as "Summation of all petroleum liquids (distallte fuel oil, jet fuel, residual fuel oil, kerosense waste oil and other petroleum liquids)"
# Missing from this list are all the "other" categories of gases: OG, BFG, SGP, SC, PG
# Those gases combine for about the same total MMBTU as DFO, about 0.2% of all reported fuel receipts
EIA_FUEL_AGGREGATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
fuel_aggregate_code,energy_source_code_eia
BIT,BIT
SUB,SUB
LIG,LIG
COW,BIT
COW,SUB
COW,LIG
COW,WC
NG,NG
PC,PC
PEL,DFO
PEL,RFO
PEL,JF
PEL,KER
PEL,WO
    """
    ),
)
"""Association table describing the many-to-many relationships between fuel types and various levels of EIA's aggregates."""


EIA_BULK_FUEL_AGGREGATES = pd.read_csv(
    StringIO(
        """
fuel_aggregate_code,fuel_aggregate_name
BIT,bituminous_coal
SUB,sub-bituminous_coal
LIG,lignite_coal
COW,all_coal
NG,natural_gas
PC,petroleum_coke
PEL,petroleum_liquids
"""
    ),
)
"""Names of EIA bulk fuel aggregates.

These codes and categories collide with, but are distinct from, the fuel codes found in EIA 860 or 923.
"""

# modified from:
# meta.groupby(["region", "region_code"])['description'].first().str.strip('; ').str.split(';', expand=True)[[2]]
# The raw data was missing Connecticut
# This is NOT an association table! The bulk data has a region 'USA' that aggregates all states (not sure about territories)
STATES = pd.read_csv(
    StringIO(
        """
state_id_fips,state_name,state_abbrev,census_region,census_region_code
1,Alabama,AL,East South Central,ESC
2,Alaska,AK,Pacific Noncontiguous,PCN
4,Arizona,AZ,Mountain,MTN
5,Arkansas,AR,West South Central,WSC
6,California,CA,Pacific Contiguous,PCC
8,Colorado,CO,Mountain,MTN
9,Connecticut,CT,New England,NEW
10,Delaware,DE,South Atlantic,SAT
11,D.C.,DC,South Atlantic,SAT
12,Florida,FL,South Atlantic,SAT
13,Georgia,GA,South Atlantic,SAT
15,Hawaii,HI,Pacific Noncontiguous,PCN
16,Idaho,ID,Mountain,MTN
17,Illinois,IL,East North Central,ENC
18,Indiana,IN,East North Central,ENC
19,Iowa,IA,West North Central,WNC
20,Kansas,KS,West North Central,WNC
21,Kentucky,KY,East South Central,ESC
22,Louisiana,LA,West South Central,WSC
23,Maine,ME,New England,NEW
24,Maryland,MD,South Atlantic,SAT
25,Massachusetts,MA,New England,NEW
26,Michigan,MI,East North Central,ENC
27,Minnesota,MN,West North Central,WNC
28,Mississippi,MS,East South Central,ESC
29,Missouri,MO,West North Central,WNC
30,Montana,MT,Mountain,MTN
31,Nebraska,NE,West North Central,WNC
32,Nevada,NV,Mountain,MTN
33,New Hampshire,NH,New England,NEW
34,New Jersey,NJ,Middle Atlantic,MAT
35,New Mexico,NM,Mountain,MTN
36,New York,NY,Middle Atlantic,MAT
37,North Carolina,NC,South Atlantic,SAT
38,North Dakota,ND,West North Central,WNC
39,Ohio,OH,East North Central,ENC
40,Oklahoma,OK,West South Central,WSC
41,Oregon,OR,Pacific Contiguous,PCC
42,Pennsylvania,PA,Middle Atlantic,MAT
44,Rhode Island,RI,New England,NEW
45,South Carolina,SC,South Atlantic,SAT
46,South Dakota,SD,West North Central,WNC
47,Tennessee,TN,East South Central,ESC
48,Texas,TX,West South Central,WSC
49,Utah,UT,Mountain,MTN
50,Vermont,VT,New England,NEW
51,Virginia,VA,South Atlantic,SAT
53,Washington,WA,Pacific Contiguous,PCC
54,West Virginia,WV,South Atlantic,SAT
55,Wisconsin,WI,East North Central,ENC
56,Wyoming,WY,Mountain,MTN
60,American Samoa,AS,,
66,Guam,GU,,
69,Northern Mariana Islands,MP,,
72,Puerto Rico,PR,,
78,Virgin Islands,VI,,
    """
    ),
)
"""Table of state information, including the census regions used in EIA bulk aggregates."""


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
