"""Empirical estimation of fossil plant ramp rates via hourly EPA CEMS data."""
from typing import Dict, Optional, Tuple

import networkx as nx
import numpy as np
import pandas as pd

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
    frozenset({"CT"}): "combined_cycle",  # in 2019 about half of solo CTs might be GTs
    # Could classify by operational characteristics but there were only 20 total so didn't bother
    frozenset({"CA"}): "combined_cycle",
    frozenset({"CS"}): "combined_cycle",
    frozenset({"IC"}): "internal_combustion",
    frozenset({"CT", "CA"}): "combined_cycle",
    frozenset({"ST", "GT"}): "combined_cycle",  # I think industrial cogen or mistaken
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


def _binarize(ser: pd.Series):
    """Classify generation values into running / not running."""
    # modularize this in case I want to add smoothing or other methods
    return ser.gt(0).astype(np.int8)


def _find_edges(cems: pd.DataFrame, drop_intermediates=True) -> None:
    """Find timestamps of startups and shutdowns based on transition from zero to non-zero generation."""
    cems["binarized"] = _binarize(cems["gross_load_mw"])
    # for each unit, find change points from zero to non-zero production
    # this could be done with groupby but it is much slower
    # cems.groupby(level='unit_id_epa')['binarized_col'].transform(lambda x: x.diff())
    cems["binary_diffs"] = (
        cems["binarized"].diff().where(cems["unit_id_epa"].diff().eq(0))
    )  # dont take diffs across units
    cems["shutdowns"] = cems["operating_datetime_utc"].where(
        cems["binary_diffs"] == -1, pd.NaT)
    cems["startups"] = cems["operating_datetime_utc"].where(
        cems["binary_diffs"] == 1, pd.NaT)
    if drop_intermediates:
        cems.drop(columns=["binarized", "binary_diffs"], inplace=True)
    return


def _distance_from_downtime(
    cems: pd.DataFrame, drop_intermediates=True, boundary_offset_hours: int = 24
) -> None:
    """Calculate two columns: the number of hours to the next shutdown; and from the last startup."""
    # fill startups forward and shutdowns backward
    # Note that this leaves NaT values for any uptime periods at the very start/end of the timeseries
    # The second fillna handles this by assuming the real boundary is the edge of the dataset + an offset
    offset = pd.Timedelta(boundary_offset_hours, unit="h")
    cems["startups"] = (
        cems["startups"]
        .groupby(level="unit_id_epa")
        .transform(lambda x: x.ffill().fillna(x.index[0][1] - offset))
    )
    cems["shutdowns"] = (
        cems["shutdowns"]
        .groupby(level="unit_id_epa")
        .transform(lambda x: x.bfill().fillna(x.index[-1][1] + offset))
    )

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
    if drop_intermediates:
        cems.drop(columns=["startups", "shutdowns"], inplace=True)
    return None


def calc_distance_from_downtime(
    cems: pd.DataFrame, classify_startup=False, drop_intermediates=True
) -> None:
    """Calculate two columns: the number of hours to the next shutdown; and from the last startup."""
    # in place
    _find_edges(cems, drop_intermediates)
    _distance_from_downtime(cems, drop_intermediates)
    cems["hours_distance"] = cems[[
        "hours_from_startup", "hours_to_shutdown"]].min(axis=1)
    if classify_startup:
        cems["nearest_to_startup"] = cems["hours_from_startup"] < cems["hours_to_shutdown"]
        # randomly allocate midpoints
        rng = np.random.default_rng(seed=42)
        rand_midpoints = (cems["hours_from_startup"] == cems["hours_to_shutdown"]) & rng.choice(
            np.array([True, False]), size=len(cems)
        )
        cems.loc[rand_midpoints, "nearest_to_startup"] = True
    return None


def _filter_retirements(df: pd.DataFrame, year_range: Tuple[int, int]) -> pd.DataFrame:
    """Remove retired or not-yet-existing units that have zero overlap with year_range."""
    min_year = year_range[0]
    max_year = year_range[1]

    not_retired_before_start = df["CAMD_RETIRE_YEAR"].replace(0, 3000) >= min_year
    not_built_after_end = (pd.to_datetime(df["CAMD_STATUS_DATE"]).dt.year <= max_year) & df[
        "CAMD_STATUS"
    ].ne("RET")
    return df.loc[not_retired_before_start & not_built_after_end]


def _remove_irrelevant(df: pd.DataFrame):
    """Remove unmatched or excluded (non-exporting) units."""
    bad = df["MATCH_TYPE_GEN"].isin({"CAMD Unmatched", "Manual CAMD Excluded"})
    return df.loc[~bad]


def _prep_crosswalk_for_networkx(key_map: pd.DataFrame) -> pd.DataFrame:
    filtered = key_map.copy()
    # networkx can't handle composite keys, so make surrogates
    filtered["combustor_id"] = filtered.groupby(
        by=["CAMD_PLANT_ID", "CAMD_UNIT_ID"]).ngroup()
    # node IDs can't overlap so add (max + 1)
    filtered["generator_id"] = (
        filtered.groupby(by=["CAMD_PLANT_ID", "EIA_GENERATOR_ID"]).ngroup()
        + filtered["combustor_id"].max()
        + 1
    )
    return filtered


def _subcomponent_ids_from_prepped_crosswalk(prepped: pd.DataFrame) -> pd.DataFrame:
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
        nx.set_edge_attributes(subgraph, name="component_id", values=i)
    return nx.to_pandas_edgelist(graph)


def _add_key_map(crosswalk: pd.DataFrame, cems: pd.DataFrame) -> pd.DataFrame:
    if "unit_id_epa" not in cems.index.names:
        raise IndexError(
            'cems data must have "unit_id_epa" and "operating_datetime_utc" in index')
    key_map = cems.groupby(level="unit_id_epa")[
        ["plant_id_eia", "unitid", "unit_id_epa"]].first()
    key_map = key_map.merge(
        crosswalk,
        left_on=["plant_id_eia", "unitid"],
        right_on=["CAMD_PLANT_ID", "CAMD_UNIT_ID"],
        how="inner",
    )
    return key_map


def _make_subcomponent_ids(crosswalk: pd.DataFrame, cems: pd.DataFrame) -> pd.DataFrame:
    """Analyze crosswalk graph and identify sub-plants."""
    key_map = _add_key_map(crosswalk=crosswalk, cems=cems)
    column_order = list(key_map.columns)
    edge_list = _prep_crosswalk_for_networkx(key_map)
    edge_list = _subcomponent_ids_from_prepped_crosswalk(edge_list)
    column_order = ["component_id"] + column_order
    return edge_list[column_order]


def aggregate_subcomponents(
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
        xwalk.groupby("component_id")["EIA_UNIT_TYPE"]
        .agg(lambda x: frozenset(x.values.reshape(-1)))
        .to_frame()
        .astype("category")
    )

    for unit in ["CAMD_UNIT_ID", "EIA_GENERATOR_ID"]:
        agency = unit.split("_", maxsplit=1)[0]
        capacity = f"{agency}_NAMEPLATE_CAPACITY"
        aggs[f"capacity_{agency}"] = (
            xwalk.groupby(by=["component_id", unit], as_index=False)
            .first()  # avoid double counting
            .groupby("component_id")[capacity]
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
        xwalk.groupby(by=["component_id", unit], as_index=False)
        .first()  # avoid double counting units
        .groupby(by=["component_id", col], as_index=False)[capacity]
        .sum()
        .replace({capacity: 0}, np.nan)
    )
    grouped["max"] = grouped.groupby("component_id")[capacity].transform(np.max)
    out = grouped.loc[grouped["max"] == grouped[capacity], ["component_id", col]].set_index(
        "component_id"
    )
    # break ties by taking first category (alphabetical due to groupby)
    # this is not very principled but it is rare enough to probably not matter
    out = out[~out.index.duplicated(keep="first")]
    return out


def _cems_index(cems: pd.DataFrame) -> pd.DataFrame:
    cems = cems.set_index(["unit_id_epa", "operating_datetime_utc"], drop=False)
    cems.sort_index(inplace=True)

    return cems


def _classify_exclusions(cems: pd.DataFrame, exclusion_map: Optional[Dict[str, int]] = None) -> pd.DataFrame:
    if exclusion_map is None:
        exclusion_map = EXCLUSION_SIZE_HOURS
    cems = cems.copy()
    calc_distance_from_downtime(cems)  # in place
    cems["exclude_ramp"] = cems["hours_distance"] <= cems["simple_EIA_UNIT_TYPE"].map(
        exclusion_map
    ).astype(np.float32)
    return cems


def _agg_units_to_components(cems: pd.DataFrame) -> pd.DataFrame:
    """Combine unit timeseries into a single timeseries per component."""
    component_timeseries = (
        # resolve name collision with index
        cems.drop(columns=["operating_datetime_utc"])
        .groupby(["component_id", "operating_datetime_utc"])[["gross_load_mw", "exclude_ramp"]]
        .sum()
    )
    component_timeseries["exclude_ramp"] = (
        component_timeseries["exclude_ramp"] > 0
    )  # sum() > 0 is like .any()
    # calculate ramp rates
    component_timeseries[["ramp"]] = component_timeseries.groupby("component_id")[
        ["gross_load_mw"]
    ].diff()
    return component_timeseries


def _estimate_capacity(cems: pd.DataFrame, component_timeseries: pd.DataFrame) -> pd.DataFrame:
    # sum of maxima
    component_aggs = (
        cems.drop(columns=["unit_id_epa"])  # resolve name collision with index
        .groupby(["component_id", "unit_id_epa"])[["gross_load_mw"]]
        .max()
        .groupby("component_id")
        .sum()
        .add_prefix("sum_of_max_")
    )
    # max of sums
    component_aggs = component_aggs.join(
        component_timeseries[["gross_load_mw"]]
        .groupby("component_id")
        .max()
        .add_prefix("max_of_sum_"),
    )
    return component_aggs


def _summarize_ramps(component_timeseries: pd.DataFrame) -> pd.DataFrame:
    ramps = (
        component_timeseries.loc[~component_timeseries["exclude_ramp"], ["ramp"]]
        .groupby("component_id")
        .agg(["max", "min", lambda x: x.idxmax()[1], lambda x: x.idxmin()[1]])
    ).rename(columns={"<lambda_0>": "idxmax", "<lambda_1>": "idxmin"}, level=1)
    for header in ramps.columns.levels[0]:
        # calculate max of absolute value of ramp rates
        ramps.loc[:, (header, "max_abs")] = (
            ramps.loc[:, idx[header, ["max", "min"]]].abs().max(axis=1)
        )
        # associate correct timestamp - note that ties go to idxmax, nans go to idxmin
        condition = ramps.loc[:, (header, "max")] >= ramps.loc[:, (header, "min")].abs()
        ramps.loc[:, (header, "idxmax_abs")] = ramps.loc[:, idx[header, "idxmax"]]
        ramps.loc[:, (header, "idxmax_abs")].where(
            condition, ramps.loc[:, (header, "idxmin")], inplace=True
        )
    # remove multiindex
    ramps.columns = ["_".join(reversed(col)) for col in ramps.columns]
    return ramps


def _add_derived_values(component_aggs: pd.DataFrame) -> pd.DataFrame:
    # normalize ramp rates in various ways
    normed = component_aggs[["max_abs_ramp"] * 4].div(
        component_aggs[
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


def process_subset(cems, crosswalk, component_id_offset=0):
    """Top level API to analyze a dataset for component-wise max ramp rates.

    Args:
        cems ([pd.DataFrame]): EPA CEMS data from ramprate.load_dataset.load_epacems
        crosswalk ([pd.DataFrame]): EPA crosswalk from ramprate.load_dataset.load_epa_crosswalk
        component_id_offset (int, optional): used when processing data in chunks to ensure unique IDs. Defaults to 0.

    Returns:
        [Dict[str, pd.DataFrame]]: key:value pairs are as follows:
        "component_aggs": component-level aggregates like max ramp rates
        "key_map": inner join of crosswalk and CEMS id columns
        "component_timeseries": CEMS timeseries aggregated to component level
        "cems": CEMS data
    """
    key_map = _make_subcomponent_ids(crosswalk, cems)
    if component_id_offset:
        key_map["component_id"] = key_map["component_id"] + component_id_offset

    meta = aggregate_subcomponents(key_map)

    # NOTE: how='inner' drops unmatched units
    cems = cems.join(key_map.groupby("unit_id_epa")[
                     "component_id"].first(), how="inner")

    # aggregate operational data
    cems = cems.merge(
        meta["simple_EIA_UNIT_TYPE"], left_on="component_id", right_index=True, copy=False
    )
    cems.sort_index(inplace=True)

    cems = _classify_exclusions(cems)
    component_timeseries = _agg_units_to_components(cems)
    component_aggs = _estimate_capacity(cems, component_timeseries)

    ramps = _summarize_ramps(component_timeseries)

    # join all the aggs
    component_aggs = component_aggs.join([ramps, meta])

    normed = _add_derived_values(component_aggs)
    component_aggs = component_aggs.join(normed)
    return {
        "component_aggs": component_aggs,
        "key_map": key_map,
        "component_timeseries": component_timeseries,
        "cems": cems,
    }
