"""Clean and normalize EIA bulk electricity data.

EIA's bulk electricity data contains 680,000 timeseries. These timeseries contain a
variety of measures (fuel amount and cost are just two) across multiple levels of
aggregation, from individual plants to national averages.

The data is formatted as a single 1.1GB text file of line-delimited JSON with one line
per timeseries. Each JSON structure has two nested levels: the top level contains
metadata describing the series and the second level (under the "data" heading)
contains an array of timestamp/value pairs. This structure leads to a natural
normalization into two tables: one of metadata and one of timeseries. That is the
format delivered by the extract module.

The transform module parses a compound key out of long string IDs ("series_id") and
sets it as the index of the timeseries data. The rest of the metadata is not very
valuable so is not transformed or returned.

The EIA aggregates are related to their component categories via a set of association
tables defined in pudl.metadata.dfs. For example, the "all_coal" fuel aggregate is
linked to all the coal-related energy_source_code values: BIT, SUB, LIG, and WC.
Similar relationships are defined for aggregates over fuel, sector, geography, and
time.
"""
from io import StringIO

import pandas as pd

# modified from:
# meta.groupby(["sector", "sector_code"])['description'].first().str.strip('; ')
#   .str.split(';', expand=True)
EIA_SECTOR_AGGREGATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
sector_agg,sector_id_eia
electric_utility,1
ipp_non_cogen,2
ipp_cogen,3
commercial_non_cogen,4
commercial_cogen,5
industrial_non_cogen,6
industrial_cogen,7
all_sectors,1
all_sectors,2
all_sectors,3
all_sectors,4
all_sectors,5
all_sectors,6
all_sectors,7
all_ipp,2
all_ipp,3
all_commercial,4
all_commercial,5
all_industrial,6
all_industrial,7
electric_power,1
electric_power,2
electric_power,3
"""
    ),
)
"""Association table describing the many-to-many relationships between plant sectors
and various levels of EIA's aggregates.
"""


# modified from:
# meta.groupby(["fuel", "sector_code"])['description'].first().str.strip('; ')
#   .str.split(';', expand=True)
# PEL is defined as "Summation of all petroleum liquids (distallte fuel oil, jet fuel,
# residual fuel oil, kerosense waste oil and other petroleum liquids)"
# Missing from this list are all the "other" categories of gases: OG, BFG, SGP, SC, PG
# Those gases combine for about the same total MMBTU as DFO, about 0.2% of all reported
# fuel receipts
EIA_FUEL_AGGREGATE_ASSOCIATION = pd.read_csv(
    StringIO(
        """
fuel_agg,energy_source_code_eia
bituminous_coal,BIT
sub_bituminous_coal,SUB
lignite_coal,LIG
all_coal,BIT
all_coal,SUB
all_coal,LIG
all_coal,WC
natural_gas,NG
petroleum_coke,PC
petroleum_liquids,DFO
petroleum_liquids,RFO
petroleum_liquids,JF
petroleum_liquids,KER
petroleum_liquids,WO
    """
    ),
)
"""Association table describing the many-to-many relationships between fuel types
and various levels of EIA's aggregates."""


# modified from:
# meta.groupby(["region", "region_code"])['description'].first().str.strip('; ')
# .str.split(';', expand=True)[[2]]
# The raw data was missing Connecticut
# This is NOT an association table! The bulk data has a region 'USA' that aggregates
# all states (not sure about territories)
STATES = pd.read_csv(
    StringIO(
        """
state_id_fips,state_name,state_abbrev,census_region,census_region_abbrev
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
"""Table of state information, including the census regions used in EIA bulk
aggregates."""


def _extract_keys_from_series_id(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Parse key codes from EIA series_id.

    These keys comprise the compound key that uniquely identifies a data series:
    (metric, fuel, region, sector, frequency).
    """
    # drop first one (constant value of "ELEC")
    keys = (
        raw_df.loc[:, "series_id"]
        .str.split(r"[\.-]", expand=True, regex=True)
        .drop(columns=0)
    )
    keys.columns = pd.Index(
        ["series_code", "fuel_agg", "geo_agg", "sector_agg", "temporal_agg"]
    )
    return keys


def _map_key_codes_to_readable_values(compound_keys: pd.DataFrame) -> pd.DataFrame:
    keys = compound_keys.copy()
    mappings = {
        "fuel_agg": {
            "BIT": "bituminous_coal",
            "SUB": "sub-bituminous_coal",
            "LIG": "lignite_coal",
            "COW": "all_coal",
            "NG": "natural_gas",
            "PC": "petroleum_coke",
            "PEL": "petroleum_liquids",
        },
        "sector_agg": {
            "1": "electric_utility",
            "2": "ipp_non_cogen",
            "3": "ipp_cogen",
            "4": "commercial_non_cogen",
            "5": "commercial_cogen",
            "6": "industrial_non_cogen",
            "7": "industrial_cogen",
            "94": "all_ipp",
            "96": "all_commercial",
            "97": "all_industrial",
            "98": "electric_power",  # IPP + regulated utilities
            "99": "all_sectors",
        },
        "temporal_agg": {
            "M": "monthly",
            "Q": "quarterly",
            "A": "annual",
        },
    }
    for col_name, mapping in mappings.items():
        keys.loc[:, col_name] = keys.loc[:, col_name].map(mapping)
    keys = keys.astype("category")
    return keys


def _transform_timeseries(raw_ts: pd.DataFrame) -> pd.DataFrame:
    """Transform raw timeseries to tidy format. Replace ID string with readable keys.

    Keys: ("fuel_agg", "geo_agg", "sector_agg", "temporal_agg", "report_date")
    Values: "receipts_mmbtu", "fuel_cost_per_mmbtu"
    """
    compound_key = _map_key_codes_to_readable_values(
        _extract_keys_from_series_id(raw_ts)
    )
    ts = pd.concat([compound_key, raw_ts.drop(columns="series_id")], axis=1)
    ts = ts.pivot(
        index=["fuel_agg", "geo_agg", "sector_agg", "temporal_agg", "date"],
        columns="series_code",
    )
    ts.columns = ts.columns.droplevel(level=None)
    ts.columns.name = None  # remove "series_code" as name; no longer appropriate
    ts.reset_index(drop=False, inplace=True)

    # convert units from billion BTU to MMBTU for consistency with other PUDL tables
    ts.loc[:, "RECEIPTS_BTU"] *= 1000

    ts.rename(
        columns={
            "RECEIPTS_BTU": "receipts_mmbtu",
            "COST_BTU": "fuel_cost_per_mmbtu",
            "date": "report_date",
        },
        inplace=True,
    )

    return ts


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform raw EIA bulk electricity aggregates.

    Args:
        raw_dfs (dict[str, pd.DataFrame]): raw timeseries dataframe

    Returns:
        dict[str, pd.DataFrame]: transformed timeseries dataframe
    """
    ts = _transform_timeseries(raw_dfs["timeseries"])
    # the metadata table is mostly useless after joining the keys into the timeseries,
    # so don't return it
    return {
        "fuel_receipts_costs_aggs_eia": ts,
    }
