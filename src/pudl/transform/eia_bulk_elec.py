"""Clean and normalize EIA bulk electricity data.

EIA's bulk electricity data contains 680,000 timeseries. These timeseries contain a variety of measures (fuel amount and cost are just two) across multiple levels of aggregation, from individual plants to national averages.

The data is formatted as a single 1.1GB text file of line-delimited JSON with one line per timeseries. Each JSON structure has two nested levels: the top level contains metadata describing the series and the second level (under the "data" heading) contains an array of timestamp/value pairs. This structure leads to a natural normalization into two tables: one of metadata and one of timeseries. That is the format delivered by the extract module.

The transform module parses a compound key out of long string IDs and sets it as the index of the timeseries data. The rest of the metadata is not very valuable so is not returned.

The EIA aggregates are related to their component categories via a set of association tables defined here. For example,
"""
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


BULK_EIA_FUEL_AGGREGATES = pd.read_csv(
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
"01",Alabama,AL,East South Central,ESC
"02",Alaska,AK,Pacific Noncontiguous,PCN
"04",Arizona,AZ,Mountain,MTN
"05",Arkansas,AR,West South Central,WSC
"06",California,CA,Pacific Contiguous,PCC
"08",Colorado,CO,Mountain,MTN
"09",Connecticut,CT,New England,NEW
"10",Delaware,DE,South Atlantic,SAT
"11",D.C.,DC,South Atlantic,SAT
"12",Florida,FL,South Atlantic,SAT
"13",Georgia,GA,South Atlantic,SAT
"15",Hawaii,HI,Pacific Noncontiguous,PCN
"16",Idaho,ID,Mountain,MTN
"17",Illinois,IL,East North Central,ENC
"18",Indiana,IN,East North Central,ENC
"19",Iowa,IA,West North Central,WNC
"20",Kansas,KS,West North Central,WNC
"21",Kentucky,KY,East South Central,ESC
"22",Louisiana,LA,West South Central,WSC
"23",Maine,ME,New England,NEW
"24",Maryland,MD,South Atlantic,SAT
"25",Massachusetts,MA,New England,NEW
"26",Michigan,MI,East North Central,ENC
"27",Minnesota,MN,West North Central,WNC
"28",Mississippi,MS,East South Central,ESC
"29",Missouri,MO,West North Central,WNC
"30",Montana,MT,Mountain,MTN
"31",Nebraska,NE,West North Central,WNC
"32",Nevada,NV,Mountain,MTN
"33",New Hampshire,NH,New England,NEW
"34",New Jersey,NJ,Middle Atlantic,MAT
"35",New Mexico,NM,Mountain,MTN
"36",New York,NY,Middle Atlantic,MAT
"37",North Carolina,NC,South Atlantic,SAT
"38",North Dakota,ND,West North Central,WNC
"39",Ohio,OH,East North Central,ENC
"40",Oklahoma,OK,West South Central,WSC
"41",Oregon,OR,Pacific Contiguous,PCC
"42",Pennsylvania,PA,Middle Atlantic,MAT
"44",Rhode Island,RI,New England,NEW
"45",South Carolina,SC,South Atlantic,SAT
"46",South Dakota,SD,West North Central,WNC
"47",Tennessee,TN,East South Central,ESC
"48",Texas,TX,West South Central,WSC
"49",Utah,UT,Mountain,MTN
"50",Vermont,VT,New England,NEW
"51",Virginia,VA,South Atlantic,SAT
"53",Washington,WA,Pacific Contiguous,PCC
"54",West Virginia,WV,South Atlantic,SAT
"55",Wisconsin,WI,East North Central,ENC
"56",Wyoming,WY,Mountain,MTN
"60",American Samoa,AS,,
"66",Guam,GU,,
"69",Northern Mariana Islands,MP,,
"72",Puerto Rico,PR,,
"78",Virgin Islands,VI,,
    """
    ),
)
"""Table of state information, including the census regions used in EIA bulk aggregates."""


EIA_BULK_FREQUENCY_CODES = pd.DataFrame(
    {"frequency_code": ["M", "Q", "A"], "frequency": ["monthly", "quarterly", "annual"]}
)


def _extract_keys_from_series_id(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Parse key codes from EIA series_id.

    These keys comprise the compound key that uniquely identifies a data series: (metric, fuel, region, sector, frequency).
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


def _transform_timeseries(raw_ts: pd.DataFrame) -> pd.DataFrame:
    """Transform raw timeseries to tidy format with full keys instead of an opaque ID.

    Keys: ("fuel", "region", "sector", "frequency", "timestamp")
    Values: "receipts_mmbtu", "cost_dollars_per_mmbtu"
    """
    compound_key = pd.MultiIndex.from_frame(_extract_keys_from_series_id(raw_ts))
    ts = raw_ts.drop(columns="series_id")
    ts.index = compound_key
    ts = ts.set_index("date", append=True).unstack("series_code")
    ts.columns = ts.columns.droplevel(level=None)

    # convert units from billion BTU to MMBTU for consistency with other PUDL tables
    ts.loc[:, "receipts_mmbtu"] = ts.loc[:, "RECEIPTS_BTU"] * 1000
    ts.drop(columns="RECEIPTS_BTU", inplace=True)

    ts.rename(
        columns={
            "COST_BTU": "cost_dollars_per_mmbtu",
        },
        inplace=True,
    )
    ts.columns.name = None  # remove "series_code" as name; no longer appropriate

    return ts


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform raw EIA bulk electricity aggregates.

    Args:
        raw_dfs (dict[str, pd.DataFrame]): raw metadata and timeseries dataframes

    Returns:
        dict[str, pd.DataFrame]: transformed metadata and timeseries dataframes
    """
    ts = _transform_timeseries(raw_dfs["timeseries"])
    # the metadata table is mostly useless after joining the keys into the timeseries, so don't return it
    return {
        "fuel_receipt_aggs_eia": ts,
    }
