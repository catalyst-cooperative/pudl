"""Use the EPA crosswalk to connect EPA units to EIA generators and other data.

A major use case for this dataset is to identify subplants within plant_ids.
Despite the name, plant_id refers to a legal entity that often contains
multiple distinct power plants, even of different technology or fuel types.

EPA CEMS data combines information from several parts of a power plant:
* emissions from smokestacks
* fuel use from combustors
* electricty production from generators
But smokestacks, combustors, and generators can be connected in
complex, many-to-many relationships. This complexity makes attribution difficult for,
as an example, allocating pollution to energy producers.
Furthermore, heterogeneity within plant_ids make aggregation
to the parent entity difficult or inappropriate.

But by analyzing the relationships between combustors and generators,
as provided in the EPA/EIA crosswalk, we can identify distinct power plants.
These are the smallest coherent units of aggregation.

In graph analysis terminology, the crosswalk is a list of edges between
nodes (combustors and generators) in a bipartite graph. The networkx python
package provides functions to analyze this edge list and extract
disjoint subgraphs (groups of combustors and generators that are connected to each other).
These are the distinct power plants. To avoid a name collision
with plant_id, we term these collections 'subplants', and identify them with a subplant_id
that is unique within each plant_id.

Through this analysis, we found that 56% of plant_ids contain multiple distinct subplants,
and 11% contain subplants with different technology types, such as
a gas boiler and gas turbine (not in a combined cycle).

Usage Example:

epacems = pudl.output.epacems.epacems(states=['ID']) # small subset for quick test
epa_crosswalk_df = pudl.output.epacems.epa_crosswalk()
filtered_crosswalk = filter_crosswalk(epa_crosswalk_df, epacems)
crosswalk_with_subplant_ids = make_subplant_ids(filtered_crosswalk)
"""
from typing import Union

import dask.dataframe as dd
import networkx as nx
import pandas as pd


def _get_unique_keys(epacems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Get unique unit IDs from CEMS data.

    Args:
        epacems (Union[pd.DataFrame, dd.DataFrame]): epacems dataset from pudl.output.epacems.epacems

    Returns:
        pd.DataFrame: unique keys from the epacems dataset
    """
    # The purpose of this function is mostly to resolve the
    # ambiguity between dask and pandas dataframes
    ids = epacems[["plant_id_eia", "unitid", "unit_id_epa"]].drop_duplicates()
    if isinstance(epacems, dd.DataFrame):
        ids = ids.compute()
    return ids


def filter_crosswalk_by_epacems(crosswalk: pd.DataFrame, epacems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Inner join unique CEMS units with the EPA crosswalk.

    This is essentially an empirical filter on EPA units. Instead of filtering by construction/retirement dates
    in the crosswalk (thus assuming they are accurate), use the presence/absence of CEMS data to filter the units.

    Args:
        crosswalk (pd.DataFrame): the EPA crosswalk, as from pudl.output.epacems.epa_crosswalk()
        unique_epacems_ids (pd.DataFrame): unique ids from _get_unique_keys

    Returns:
        pd.DataFrame: the inner join of the EPA crosswalk and unique epacems units. Adds the global ID column unit_id_epa.
    """
    unique_epacems_ids = _get_unique_keys(epacems)
    key_map = unique_epacems_ids.merge(
        crosswalk,
        left_on=["plant_id_eia", "unitid"],
        right_on=["CAMD_PLANT_ID", "CAMD_UNIT_ID"],
        how="inner",
    )
    return key_map


def filter_out_unmatched(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove unmatched or excluded (non-exporting) units.

    Args:
        crosswalk (pd.DataFrame): the EPA crosswalk, as from pudl.output.epacems.epa_crosswalk()

    Returns:
        pd.DataFrame: the EPA crosswalk with unmatched units removed
    """
    bad = crosswalk["MATCH_TYPE_GEN"].isin({"CAMD Unmatched", "Manual CAMD Excluded"})
    return crosswalk.loc[~bad].copy()


def filter_out_boiler_rows(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that represent graph edges between generators and boilers.

    Args:
        crosswalk (pd.DataFrame): the EPA crosswalk, as from pudl.output.epacems.epa_crosswalk()

    Returns:
        pd.DataFrame: the EPA crosswalk with boiler rows (many/one-to-many) removed
    """
    crosswalk = crosswalk.drop_duplicates(
        subset=["CAMD_PLANT_ID", "CAMD_UNIT_ID", "EIA_GENERATOR_ID"])
    return crosswalk


def _prep_for_networkx(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Make surrogate keys for combustors and generators.

    Args:
        crosswalk (pd.DataFrame): EPA crosswalk, as from pudl.output.epacems.epa_crosswalk()

    Returns:
        pd.DataFrame: copy of EPA crosswalk with new surrogate ID columns 'combustor_id' and 'generator_id'
    """
    prepped = crosswalk.copy()
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
    """Apply networkx graph analysis to a preprocessed crosswalk edge list.

    Args:
        prepped (pd.DataFrame): an EPA crosswalk that has passed through _prep_for_networkx()

    Returns:
        pd.DataFrame: copy of EPA crosswalk plus new column 'subplant_id'
    """
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


def filter_crosswalk(crosswalk: pd.DataFrame, epacems: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
    """Remove crosswalk rows that do not correspond to an EIA facility or are duplicated due to many-to-many boiler relationships.

    Args:
        crosswalk (pd.DataFrame): The EPA/EIA crosswalk, as from pudl.output.epacems.epa_crosswalk()
        epacems (Union[pd.DataFrame, dd.DataFrame]): Emissions data. Must contain columns named ["plant_id_eia", "unitid", "unit_id_epa"]

    Returns:
        pd.DataFrame: A filtered copy of EPA crosswalk
    """
    filtered_crosswalk = filter_out_unmatched(crosswalk)
    filtered_crosswalk = filter_out_boiler_rows(filtered_crosswalk)
    key_map = filter_crosswalk_by_epacems(
        filtered_crosswalk, epacems)
    return key_map


def make_subplant_ids(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Identify sub-plants in the EPA/EIA crosswalk graph. Any row filtering should be done before this step.

    Usage Example:

    epacems = pudl.output.epacems.epacems(states=['ID']) # small subset for quick test
    epa_crosswalk_df = pudl.output.epacems.epa_crosswalk()
    filtered_crosswalk = filter_crosswalk(epa_crosswalk_df, epacems)
    crosswalk_with_subplant_ids = make_subplant_ids(filtered_crosswalk)

    Args:
        crosswalk (pd.DataFrame): The EPA/EIA crosswalk, as from pudl.output.epacems.epa_crosswalk()

    Returns:
        pd.DataFrame: An edge list connecting EPA units to EIA generators, with connected pieces issued a subplant_id
    """
    column_order = list(crosswalk.columns)
    edge_list = _prep_for_networkx(crosswalk)
    edge_list = _subplant_ids_from_prepped_crosswalk(edge_list)
    column_order = ["subplant_id"] + column_order
    return edge_list[column_order]
