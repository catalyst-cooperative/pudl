"""Empirical estimation of fossil plant ramp rates via hourly EPA CEMS data."""
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

import dask
import dask.dataframe as dd
import networkx as nx
import numpy as np
import pandas as pd
from dask.delayed import Delayed  # for type annotation

from pudl.output.epacems import epa_crosswalk, epacems

idx = pd.IndexSlice

CAMD_FUEL_MAP = {
    "Pipeline Natural Gas": "gas",
    "Coal": "coal",
    "Diesel Oil": "oil",
    "Natural Gas": "gas",
    "Process Gas": "gas",
    "Residual Oil": "oil",
    "Other Gas": "gas",
    "Wood": "other",
    "Other Oil": "oil",
    "Coal Refuse": "coal",
    "Petroleum Coke": "oil",
    "Tire Derived Fuel": "other",
    "Other Solid Fuel": "other",
}
EIA_FUEL_MAP = {
    "AB": "other",
    "ANT": "coal",
    "BFG": "gas",
    "BIT": "coal",
    "BLQ": "other",
    "CBL": "Coal",
    "DFO": "oil",
    "JF": "oil",
    "KER": "oil",
    "LFG": "gas",
    "LIG": "coal",
    "MSB": "other",
    "MSN": "other",
    "MSW": "other",
    "MWH": "other",
    "NG": "gas",
    "OBG": "gas",
    "OBL": "other",
    "OBS": "other",
    "OG": "gas",
    "OTH": "other",
    "PC": "oil",
    "PG": "gas",
    "PUR": "other",
    "RC": "coal",
    "RFO": "oil",
    "SC": "coal",
    "SGC": "gas",
    "SGP": "gas",
    "SLW": "other",
    "SUB": "coal",
    "SUN": "gas",  # mis-categorized gas plants with 'solar' in the name
    "TDF": "other",
    "WC": "coal",
    "WDL": "other",
    "WDS": "other",
    "WH": "other",
    "WO": "oil",
}
TECH_TYPE_MAP = {
    frozenset({"ST"}): "steam_turbine",
    frozenset({"GT"}): "gas_turbine",
    # in 2019 about half of solo CTs might be GTs
    frozenset({"CT"}): "combined_cycle",
    # Could classify by operational characteristics but there were only 20 total so didn't bother
    frozenset({"CA"}): "combined_cycle",
    frozenset({"CS"}): "combined_cycle",
    frozenset({"IC"}): "internal_combustion",
    frozenset({"CT", "CA"}): "combined_cycle",
    # I think industrial cogen or mistaken
    frozenset({"ST", "GT"}): "combined_cycle",
    frozenset({"CA", "GT"}): "combined_cycle",  # most look mistaken
    frozenset({"CT", "CA", "ST"}): "combined_cycle",  # most look mistaken
}


# duration of exclusion zone around startup/shutdown
# values are based on plots in cell 78 of notebook 5.0
# https://github.com/catalyst-cooperative/epacems_ramp_rates/blob/main/notebooks/5.0-tb-one_to_one_ramp_rates_by_plant_type.ipynb
EXCLUSION_SIZE_HOURS = {
    "steam_turbine": 5,
    "combined_cycle": 7,
    "gas_turbine": -1,  # no exclusions
    "internal_combustion": -1,  # no exclusions
}


def _classify_on_off(ser: pd.Series) -> pd.Series:
    """Classify generation values into running / not running."""
    # modularize this in case I want to add smoothing or other methods
    return ser.gt(0).astype(np.int8)


def _sorted_groupby_diff(diff_col: pd.Series, group_col: pd.Series) -> pd.Series:
    """Take a first difference within groups, assuming group_col is grouped.

    This is a much faster equivalent to df.groupby(my_group)[my_data].transform(lambda x: x.diff())
    but only if group_col is sorted (or actually just grouped, macro order doesn't matter)
    """
    # the where clause prevents diffs across groups (sets to NaN)
    return diff_col.diff().where(group_col.diff().eq(0))


def add_startup_shutdown_timestamps(cems: pd.DataFrame) -> None:
    """Find timestamps of startups and shutdowns based on transitions between generator on and off states."""
    # for each unit, find change points from zero to non-zero production
    cems["binarized"] = _classify_on_off(cems["gross_load_mw"])
    cems["binary_diffs"] = _sorted_groupby_diff(cems['binarized'], cems['unit_id_epa'])
    # extract changepoint times
    cems["startups"] = cems["operating_datetime_utc"].where(
        cems["binary_diffs"] == 1, pd.NaT)
    cems["shutdowns"] = cems["operating_datetime_utc"].where(
        cems["binary_diffs"] == -1, pd.NaT)
    # drop intermediates
    cems.drop(columns=["binarized", "binary_diffs"], inplace=True)
    return


def _fill_startups_shutdowns(
    cems: pd.DataFrame, boundary_offset_hours: int = 24
) -> None:
    """Create columns with the timestamps of the next shutdown and previous startup for each timestamp."""
    # Start by filling startups forward and shutdowns backward.
    # This leaves NaT values for any uptime periods at the very start/end of the timeseries.
    # The second fillna handles this by assuming the next startup/shutdown is at
    # the edge of the dataset + an offset
    offset = pd.Timedelta(boundary_offset_hours, unit="h")
    units = cems.groupby(level="unit_id_epa")
    cems["startups"] = (
        units["startups"]
        .transform(lambda x: x.ffill().fillna(x.index.get_level_values('operating_datetime_utc')[0] - offset))
    )
    cems["shutdowns"] = (
        units["shutdowns"]
        .transform(lambda x: x.bfill().fillna(x.index.get_level_values('operating_datetime_utc')[-1] + offset))
    )
    return


def _distance_from_downtime(cems: pd.DataFrame) -> None:
    """Calculate two columns: the number of hours 1) to the next shutdown and 2) from the last startup."""
    cems["hours_from_startup"] = (
        cems["operating_datetime_utc"]
        .sub(cems["startups"])
        .dt.total_seconds()
        .div(3600)
        .astype(np.float32)
    )
    # invert sign so distances are all positive
    cems["hours_to_shutdown"] = (
        cems["shutdowns"]
        .sub(cems["operating_datetime_utc"])
        .dt.total_seconds()
        .div(3600)
        .astype(np.float32)
    )
    cems.drop(columns=["startups", "shutdowns"], inplace=True)
    return


def calc_distance_from_downtime(cems: pd.DataFrame) -> None:
    """Add two columns: the number of hours 1) to the next shutdown and 2) from the last startup."""
    add_startup_shutdown_timestamps(cems)
    _fill_startups_shutdowns(cems)
    _distance_from_downtime(cems)
    return


def _filter_retirements(df: pd.DataFrame, year_range: Tuple[int, int]) -> pd.DataFrame:
    """Remove retired or not-yet-existing units that have zero overlap with year_range."""
    min_year = year_range[0]
    max_year = year_range[1]

    not_retired_before_start = df["CAMD_RETIRE_YEAR"].replace(
        0, 3000) >= min_year
    not_built_after_end = (pd.to_datetime(df["CAMD_STATUS_DATE"]).dt.year <= max_year) & df[
        "CAMD_STATUS"
    ].ne("RET")
    return df.loc[not_retired_before_start & not_built_after_end]


def _get_unique_keys(cems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Get unique unit IDs from CEMS data."""
    # The purpose of this function is mostly to resolve the
    # ambiguity between dask and pandas dataframes
    ids = cems[["plant_id_eia", "unitid", "unit_id_epa"]].drop_duplicates()
    if isinstance(cems, dd.DataFrame):
        ids = ids.compute()
    return ids


def _merge_crosswalk_with_cems_ids(crosswalk: pd.DataFrame, unique_cems_ids: pd.DataFrame) -> pd.DataFrame:
    key_map = unique_cems_ids.merge(
        crosswalk,
        left_on=["plant_id_eia", "unitid"],
        right_on=["CAMD_PLANT_ID", "CAMD_UNIT_ID"],
        how="inner",
    )
    return key_map


def _remove_unmatched(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove unmatched or excluded (non-exporting) units."""
    bad = crosswalk["MATCH_TYPE_GEN"].isin({"CAMD Unmatched", "Manual CAMD Excluded"})
    return crosswalk.loc[~bad].copy()


def _remove_boiler_rows(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that represent graph edges between generators and boilers."""
    crosswalk = crosswalk.drop_duplicates(
        subset=["CAMD_PLANT_ID", "CAMD_UNIT_ID", "EIA_GENERATOR_ID"])
    return crosswalk


def _prep_for_networkx(merged: pd.DataFrame) -> pd.DataFrame:
    prepped = merged.copy()
    # networkx can't handle composite keys, so make surrogates
    prepped["combustor_id"] = prepped.groupby(
        by=["CAMD_PLANT_ID", "CAMD_UNIT_ID"]).ngroup()
    # node IDs can't overlap so add (max + 1)
    prepped["generator_id"] = (
        prepped.groupby(by=["CAMD_PLANT_ID", "EIA_GENERATOR_ID"]).ngroup()
        + prepped["combustor_id"].max()
        + 1
    )
    return prepped


def _subplant_ids_from_prepped_crosswalk(prepped: pd.DataFrame) -> pd.DataFrame:
    graph = nx.from_pandas_edgelist(
        prepped,
        source="combustor_id",
        target="generator_id",
        edge_attr=True,
    )
    for i, node_set in enumerate(nx.connected_components(graph)):
        subgraph = graph.subgraph(node_set)
        assert nx.algorithms.bipartite.is_bipartite(
            subgraph
        ), f"non-bipartite: i={i}, node_set={node_set}"
        nx.set_edge_attributes(subgraph, name="subplant_id", values=i)
    return nx.to_pandas_edgelist(graph)


def make_subplant_ids(crosswalk: pd.DataFrame, cems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Analyze crosswalk graph and identify sub-plants."""
    ids = _get_unique_keys(cems)
    filtered_crosswalk = _remove_unmatched(crosswalk)
    filtered_crosswalk = _remove_boiler_rows(filtered_crosswalk)
    key_map = _merge_crosswalk_with_cems_ids(
        crosswalk=filtered_crosswalk, unique_cems_ids=ids)
    column_order = list(key_map.columns)
    edge_list = _prep_for_networkx(key_map)
    edge_list = _subplant_ids_from_prepped_crosswalk(edge_list)
    column_order = ["subplant_id"] + column_order
    return edge_list[column_order]


def _aggregate_subplants(
    xwalk: pd.DataFrame,
    camd_fuel_map: Optional[Dict[str, str]] = None,
    eia_fuel_map: Optional[Dict[str, str]] = None,
    tech_type_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Aggregate categorical variables to sub-plants."""
    if camd_fuel_map is None:
        camd_fuel_map = CAMD_FUEL_MAP
    if eia_fuel_map is None:
        eia_fuel_map = EIA_FUEL_MAP
    if tech_type_map is None:
        tech_type_map = TECH_TYPE_MAP

    aggs = (
        xwalk.groupby("subplant_id")["EIA_UNIT_TYPE"]
        .agg(lambda x: frozenset(x.values.reshape(-1)))
        .to_frame()
        .astype("category")
    )

    for unit in ["CAMD_UNIT_ID", "EIA_GENERATOR_ID"]:
        agency = unit.split("_", maxsplit=1)[0]
        capacity = f"{agency}_NAMEPLATE_CAPACITY"
        aggs[f"capacity_{agency}"] = (
            xwalk.groupby(by=["subplant_id", unit], as_index=False)
            .first()  # avoid double counting
            .groupby("subplant_id")[capacity]
            .sum()
            .replace(0, np.nan)
        )
    for col, mapping in {
        "CAMD_FUEL_TYPE": camd_fuel_map,
        "EIA_FUEL_TYPE": eia_fuel_map,
    }.items():
        nan_count = xwalk[col].isna().sum()
        simple = f"simple_{col}"
        xwalk[simple] = xwalk[col].map(mapping)
        if xwalk[simple].isna().sum() > nan_count:
            raise ValueError(
                f"there is a category in {col} not present in mapping {mapping}")
        aggs[simple + "_via_capacity"] = _assign_by_capacity(xwalk, simple)

    aggs["simple_EIA_UNIT_TYPE"] = aggs["EIA_UNIT_TYPE"].map(tech_type_map)
    aggs = aggs.astype(
        {
            "simple_EIA_UNIT_TYPE": "category",
            "simple_EIA_FUEL_TYPE_via_capacity": "category",
            "simple_CAMD_FUEL_TYPE_via_capacity": "category",
        }
    )
    return aggs


def _assign_by_capacity(xwalk: pd.DataFrame, col: str) -> pd.Series:
    if "CAMD" in col:
        unit = "CAMD_UNIT_ID"
        capacity = "CAMD_NAMEPLATE_CAPACITY"
    elif "EIA" in col:
        unit = "EIA_GENERATOR_ID"
        capacity = "EIA_NAMEPLATE_CAPACITY"
    else:
        raise ValueError(f"{col} must contain 'CAMD' or 'EIA'")

    # assign by category with highest capacity
    grouped = (
        xwalk.groupby(by=["subplant_id", unit], as_index=False)
        .first()  # avoid double counting units
        .groupby(by=["subplant_id", col], as_index=False)[capacity]
        .sum()
        .replace({capacity: 0}, np.nan)
    )
    grouped["max"] = grouped.groupby("subplant_id")[capacity].transform(np.max)
    out = grouped.loc[grouped["max"] == grouped[capacity], ["subplant_id", col]].set_index(
        "subplant_id"
    )
    # break ties by taking first category (alphabetical due to groupby)
    # this is not very principled but it is rare enough to probably not matter
    out = out[~out.index.duplicated(keep="first")]
    return out


def _classify_exclusions(cems: pd.DataFrame, exclusion_map: Optional[Dict[str, int]] = None) -> pd.DataFrame:
    if exclusion_map is None:
        exclusion_map = EXCLUSION_SIZE_HOURS
    cems = cems.copy()
    calc_distance_from_downtime(cems)  # in place
    cems["hours_distance"] = cems[[
        "hours_from_startup", "hours_to_shutdown"]].min(axis=1)
    cems["exclude_ramp"] = cems["hours_distance"] <= cems["simple_EIA_UNIT_TYPE"].map(
        exclusion_map
    ).astype(np.float32)
    return cems


def _agg_units_to_subplants(cems: pd.DataFrame) -> pd.DataFrame:
    """Combine unit timeseries into a single timeseries per subplant."""
    subplant_timeseries = (
        # resolve name collision with index
        cems.drop(columns=["operating_datetime_utc"])
        .groupby(["subplant_id", "operating_datetime_utc"])[["gross_load_mw", "exclude_ramp"]]
        .sum()
    )
    subplant_timeseries["exclude_ramp"] = (
        subplant_timeseries["exclude_ramp"] > 0
    )  # sum() > 0 is like .any()
    # calculate ramp rates
    subplant_timeseries[["ramp"]] = subplant_timeseries.groupby("subplant_id")[
        ["gross_load_mw"]
    ].diff()
    return subplant_timeseries


def _estimate_capacity(cems: pd.DataFrame, subplant_timeseries: pd.DataFrame) -> pd.DataFrame:
    # sum of maxima
    subplant_aggs = (
        cems.drop(columns=["unit_id_epa"])  # resolve name collision with index
        .groupby(["subplant_id", "unit_id_epa"])[["gross_load_mw"]]
        .max()
        .groupby("subplant_id")
        .sum()
        .add_prefix("sum_of_max_")
    )
    # max of sums
    subplant_aggs = subplant_aggs.join(
        subplant_timeseries[["gross_load_mw"]]
        .groupby("subplant_id")
        .max()
        .add_prefix("max_of_sum_"),
    )
    return subplant_aggs


def _summarize_ramps(subplant_timeseries: pd.DataFrame) -> pd.DataFrame:
    ramps = (
        subplant_timeseries.loc[~subplant_timeseries["exclude_ramp"], ["ramp"]]
        .groupby("subplant_id")
        .agg(["max", "min", lambda x: x.idxmax()[1], lambda x: x.idxmin()[1]])
    ).rename(columns={"<lambda_0>": "idxmax", "<lambda_1>": "idxmin"}, level=1)
    for header in ramps.columns.levels[0]:
        # calculate max of absolute value of ramp rates
        ramps.loc[:, (header, "max_abs")] = (
            ramps.loc[:, idx[header, ["max", "min"]]].abs().max(axis=1)
        )
        # associate correct timestamp - note that ties go to idxmax, nans go to idxmin
        condition = ramps.loc[:, (header, "max")
                              ] >= ramps.loc[:, (header, "min")].abs()
        ramps.loc[:, (header, "idxmax_abs")] = ramps.loc[:,
                                                         idx[header, "idxmax"]]
        ramps.loc[:, (header, "idxmax_abs")].where(
            condition, ramps.loc[:, (header, "idxmin")], inplace=True
        )
    # remove multiindex
    ramps.columns = ["_".join(reversed(col)) for col in ramps.columns]
    return ramps


def _add_derived_values(subplant_aggs: pd.DataFrame) -> pd.DataFrame:
    # normalize ramp rates in various ways
    normed = subplant_aggs[["max_abs_ramp"] * 4].div(
        subplant_aggs[
            [
                "capacity_CAMD",
                "capacity_EIA",
                "sum_of_max_gross_load_mw",
                "max_of_sum_gross_load_mw",
            ]
        ].to_numpy()
    )
    normed.columns = ["ramp_factor_" +
                      suf for suf in ["CAMD", "EIA", "sum_max", "max_sum"]]
    return normed


def _process_cems(cems: pd.DataFrame, key_map: pd.DataFrame, subplant_meta: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Top level API to analyze a dataset for subplant-wise max ramp rates.

    Args:
        cems ([pd.DataFrame]): EPA CEMS data from ramprate.load_dataset.load_epacems
        key_map ([pd.DataFrame]): output from make_subplant_ids
        subplant_id_offset (int, optional): used when processing data in chunks to ensure unique IDs. Defaults to 0.

    Returns:
        [Dict[str, pd.DataFrame]]: key:value pairs are as follows:
            "subplant_aggs": subplant-level aggregates like max ramp rates
            "subplant_timeseries": CEMS timeseries aggregated to subplant level
            "cems": CEMS data
    """
    cems = cems.set_index(
        ["unit_id_epa", "operating_datetime_utc"], drop=False)
    cems.sort_index(inplace=True)
    # NOTE: how='inner' drops unmatched units
    cems = cems.join(key_map.groupby("unit_id_epa")[
                     "subplant_id"].first(), how="inner")

    cems = cems.merge(
        subplant_meta["simple_EIA_UNIT_TYPE"], left_on="subplant_id", right_index=True, copy=False
    )
    cems.sort_index(inplace=True)

    # catch empty frames
    if len(cems) == 0:
        return {
            "subplant_aggs": pd.DataFrame(),
            "subplant_timeseries": pd.DataFrame(),
            "cems": cems,
        }

    cems = _classify_exclusions(cems)
    subplant_timeseries = _agg_units_to_subplants(cems)
    subplant_aggs = _estimate_capacity(cems, subplant_timeseries)

    ramps = _summarize_ramps(subplant_timeseries)

    # join all the aggs
    subplant_aggs = subplant_aggs.join([ramps, subplant_meta])

    normed = _add_derived_values(subplant_aggs)
    subplant_aggs = subplant_aggs.join(normed)
    return {
        "subplant_aggs": subplant_aggs,
        "subplant_timeseries": subplant_timeseries,
        "cems": cems,
    }


def _process_cems_parallel(key_map: pd.DataFrame, subplant_meta: pd.DataFrame) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Wrap _process_cems to use a specific key_map and subplant_meta and to return only the 'subplant_aggs' dataframe (to save memory)."""
    # Might be able to use functools.partial instead of this function but
    # I *think* I need to index into the result before wrapping with dask.delayed
    def closure(cems):
        out = _process_cems(cems, key_map, subplant_meta)
        return out['subplant_aggs']
    return closure


def _run_parallel(cems: dd.DataFrame, key_map: pd.DataFrame, subplant_meta: pd.DataFrame) -> List[Delayed]:
    dfs = cems.to_delayed()
    apply_func = _process_cems_parallel(key_map, subplant_meta)
    delayed_results = [dask.delayed(apply_func)(df) for df in dfs]
    return delayed_results


def analyze_ramp_rates(states: Optional[Sequence[str]] = None, years: Optional[Sequence[int]] = None) -> Dict[str, pd.DataFrame]:
    """Analyze ramp rates via EPA CEMS data.

    See pudl.outputs.epacems.epacems for default args
    """
    crosswalk = epa_crosswalk()
    cems = epacems(states=states, years=years)

    key_map = make_subplant_ids(crosswalk, cems)
    subplant_meta = _aggregate_subplants(key_map)

    delayed_results = _run_parallel(cems, key_map, subplant_meta)
    results = dask.compute(*delayed_results)
    results = pd.concat(results)

    results = (results.set_index(
        results['idxmax_ramp'].dt.year.rename('year'), append=True).sort_index())

    return {'crosswalk_with_ids': key_map, 'ramp_rates': results}
