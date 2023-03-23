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

The transform module parses a compound primary key out of long string IDs
("series_id"). The rest of the metadata is not very valuable so is not transformed
or returned.

The EIA aggregates are related to their component categories via a set of association
tables defined in pudl.metadata.dfs. For example, the "all_coal" fuel aggregate is
linked to all the coal-related energy_source_code values: BIT, SUB, LIG, and WC.
Similar relationships are defined for aggregates over fuel, sector, geography, and
time.
"""

import pandas as pd


def _extract_keys_from_series_id(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Parse primary key codes from EIA series_id.

    These codes comprise the compound primary key that uniquely identifies a data
    series: (metric, fuel, region, sector, frequency).
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
            # match values in pudl.metadata.dfs.py:EIA_FUEL_AGGREGATE_ASSN
            "BIT": "bituminous_coal",
            "SUB": "sub_bituminous_coal",
            "LIG": "lignite_coal",
            "COW": "all_coal",
            "NG": "natural_gas",
            "PC": "petroleum_coke",
            "PEL": "petroleum_liquids",
        },
        "sector_agg": {
            # match values in pudl.metadata.dfs.py:EIA_SECTOR_AGGREGATE_ASSN
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
            "98": "all_electric_power",  # all_IPP + regulated utilities
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
        assert (
            keys.loc[:, col_name].notnull().all()
        ), f"{col_name} contains an unmapped category."

    keys = keys.astype("category")
    return keys


def _transform_timeseries(raw_ts: pd.DataFrame) -> pd.DataFrame:
    """Transform raw timeseries.

    Transform to tidy format and replace the obscure series_id with a readable
    compound primary key.

    Returns:
        A dataframe with compound key ("fuel_agg", "geo_agg", "sector_agg",
        "temporal_agg", "report_date") and two value columns: "fuel_received_mmbtu",
        "fuel_cost_per_mmbtu"
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
    ts.columns.name = None  # remove "series_code" as name - no longer appropriate
    ts.reset_index(drop=False, inplace=True)

    # convert units from billion BTU to MMBTU for consistency with other PUDL tables
    ts.loc[:, "RECEIPTS_BTU"] *= 1000

    ts.rename(
        columns={
            "RECEIPTS_BTU": "fuel_received_mmbtu",
            "COST_BTU": "fuel_cost_per_mmbtu",
            "date": "report_date",
        },
        inplace=True,
    )

    return ts


# TODO (bendnorman): Are we planning on extracting multiple dataframes from the EIA API?
def transform(raw_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transform raw EIA bulk electricity aggregates.

    Args:
        raw_dfs: raw timeseries dataframe

    Returns:
        Transformed timeseries dataframe with compound key:
        ("fuel_agg", "geo_agg", "sector_agg", "temporal_agg", "report_date")
        and two value columns: "fuel_received_mmbtu", "fuel_cost_per_mmbtu"
    """
    ts = _transform_timeseries(raw_dfs["timeseries"])
    # raw_dfs["metadata"] is mostly useless after joining the keys into the timeseries,
    # so don't return it
    return ts
