"""Empirical estimation of fossil plant ramp rates via hourly EPA CEMS data."""
from typing import Union

import dask.dataframe as dd
import networkx as nx
import pandas as pd


def _get_unique_keys(cems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Get unique unit IDs from CEMS data."""
    # The purpose of this function is mostly to resolve the
    # ambiguity between dask and pandas dataframes
    ids = cems[["plant_id_eia", "unitid", "unit_id_epa"]].drop_duplicates()
    if isinstance(cems, dd.DataFrame):
        ids = ids.compute()
    return ids


def _merge_crosswalk_with_cems_ids(crosswalk: pd.DataFrame, unique_cems_ids: pd.DataFrame) -> pd.DataFrame:
    """Inner join unique CEMS units with the EPA crosswalk.

    This is essentially an empirical filter on EPA units. Instead of filtering by retirement dates
    in the crosswalk (thus assuming they are accurate), use the presence/absence of CEMS data to filter the units.
    """
    key_map = unique_cems_ids.merge(
        crosswalk,
        left_on=["plant_id_eia", "unitid"],
        right_on=["CAMD_PLANT_ID", "CAMD_UNIT_ID"],
        how="inner",
    )
    return key_map


def filter_out_unmatched(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove unmatched or excluded (non-exporting) units."""
    bad = crosswalk["MATCH_TYPE_GEN"].isin({"CAMD Unmatched", "Manual CAMD Excluded"})
    return crosswalk.loc[~bad].copy()


def filter_out_boiler_rows(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that represent graph edges between generators and boilers."""
    crosswalk = crosswalk.drop_duplicates(
        subset=["CAMD_PLANT_ID", "CAMD_UNIT_ID", "EIA_GENERATOR_ID"])
    return crosswalk


def _prep_for_networkx(merged: pd.DataFrame) -> pd.DataFrame:
    """Make surrogate keys for combustors and generators."""
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
    """Apply networkx graph analysis to a preprocessed crosswalk edge list."""
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


def filter_crosswalk(crosswalk: pd.DataFrame, cems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Remove crosswalk rows that do not correspond to an EIA facility or are duplicated due to many-to-many boiler relationships.

    Args:
        crosswalk (pd.DataFrame): The EPA/EIA crosswalk.
        cems (Union[pd.DataFrame, dd.DataFrame]): Emissions data. Must contain columns named ["plant_id_eia", "unitid", "unit_id_epa"]

    Returns:
        pd.DataFrame: An edge list connecting EPA units to EIA generators
    """
    ids = _get_unique_keys(cems)
    filtered_crosswalk = filter_out_unmatched(crosswalk)
    filtered_crosswalk = filter_out_boiler_rows(filtered_crosswalk)
    key_map = _merge_crosswalk_with_cems_ids(
        crosswalk=filtered_crosswalk, unique_cems_ids=ids)
    return key_map


def make_subplant_ids(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Identify sub-plants in the EPA/EIA crosswalk graph. Any row filtering should be done before this step.

    Args:
        crosswalk (pd.DataFrame): The EPA/EIA crosswalk.

    Returns:
        pd.DataFrame: An edge list connecting EPA units to EIA generators, with connected pieces issued a subplant_id
    """
    column_order = list(crosswalk.columns)
    edge_list = _prep_for_networkx(crosswalk)
    edge_list = _subplant_ids_from_prepped_crosswalk(edge_list)
    column_order = ["subplant_id"] + column_order
    return edge_list[column_order]
