"""Extract EIA AEO data from the bulk JSON."""

import io
import itertools
import re
from collections.abc import Iterable

import networkx as nx
import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetOut,
    Output,
    asset_check,
    multi_asset,
)
from pydantic import BaseModel

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class Category(BaseModel):
    """Describe the structure of how series are related."""

    category_id: int
    parent_category_id: int
    name: str
    notes: str
    childseries: list[str]


class Series(BaseModel):
    """Contain the actual data, and each timeseries' metadata."""

    series_id: str
    name: str
    last_updated: str
    units: str | None = None
    # f: Literal["A"]  - this is just "frequency: annual" afaict
    data: list[tuple[str, str | float]]  # time, value


class AEOTaxonomy:
    """Container for *all* the information in one AEO report."""

    def __init__(self, records: Iterable[str]):
        """Load AEO JSON records into a graph datastructure.

        Args:
            records: the strings that contain an AEO report to parse.

        """
        categories, series = self.__load_records(records)
        self.graph = self.__generate_graph(categories, series)
        self.__sanitize_re = re.compile(r"\W+")

    def __load_records(
        self, records: Iterable[str]
    ) -> tuple[dict[int, Category], dict[str, Series]]:
        # A record can be either a category or a series, so we parse those into
        # two separate mappings.

        all_categories: dict[int, Category] = {}
        all_series: dict[str, Series] = {}
        for record in records:
            if "category_id" in record:
                category = Category.model_validate_json(record)
                all_categories[category.category_id] = category
            elif "series_id" in record:
                series = Series.model_validate_json(record)
                all_series[series.series_id] = series
            else:
                raise ValueError(f"Line had neither series nor category ID: {record}")
        return all_categories, all_series

    def __generate_graph(
        self, categories: dict[int, Category], series: dict[str, Series]
    ) -> nx.DiGraph:
        def get_all_edges(category: Category) -> Iterable[tuple[int, str | int]]:
            # A category can have incoming edges from its parents or outgoing
            # edges to its child series.
            edges: list[tuple[int, str | int]] = [
                (category.parent_category_id, category.category_id)
            ]
            if category.childseries:
                edges += [
                    (category.category_id, child_series_id)
                    for child_series_id in category.childseries
                ]
            return edges

        edges = itertools.chain.from_iterable(
            get_all_edges(category) for category in categories.values()
        )
        graph = nx.DiGraph(incoming_graph_data=edges)

        # TODO (2024-04-03): by setting node attributes like this, we lose the
        # nice type information we got from the parsing. We can work around
        # this by assigning {node_id: {"self": Category | Series}} instead of
        # {node_id: Category | Series}
        nx.set_node_attributes(graph, categories | series)
        return graph

    def __sanitize(self, s: str) -> str:
        return re.sub(self.__sanitize_re, "_", s.lower().strip().replace(" : ", "__"))

    def __series_to_records(
        self, series_id: str, potential_parents: set[int]
    ) -> pd.DataFrame:
        root = 4949903
        series = self.graph.nodes[series_id]
        ancestors = [self.graph.nodes[a] for a in nx.ancestors(self.graph, series_id)]
        cases = [a for a in ancestors if a.get("parent_category_id") == root]
        assert len(cases) == 1
        case = cases[0]["name"]
        parent_names = {
            self.graph.nodes[p]["name"]
            for p in self.graph.predecessors(series_id)
            if p in potential_parents
        }
        assert len(parent_names) == 1
        parent_name = parent_names.pop()

        records = (
            {
                "date": d[0],
                "value": d[1],
                "units": series["units"],
                "series_name": series["name"],
                "category_name": parent_name,
                "case": case,
            }
            for d in series["data"]
        )
        return records

    def get_table(self, table_number: int) -> pd.DataFrame:
        """Get a specific table number as a DataFrame."""
        matching_category_ids = {
            n_id
            for n_id in self.graph
            if self.__sanitize(self.graph.nodes.get(n_id).get("name", "")).startswith(
                f"table_{table_number}_"
            )
        }
        matching_series = set(
            itertools.chain.from_iterable(
                self.graph.nodes[n_id].get("childseries", [])
                for n_id in matching_category_ids
            )
        )

        series_records = itertools.chain.from_iterable(
            self.__series_to_records(series_id, potential_parents=matching_category_ids)
            for series_id in matching_series
        )
        return pd.DataFrame.from_records(series_records)


@multi_asset(
    outs={
        "raw_eia_aeo__electric_power_projections_regional": AssetOut(is_required=False),
        "raw_eia_aeo__renewable_energy_regional": AssetOut(is_required=False),
    },
    can_subset=True,
    required_resource_keys={"datastore"},
)
def raw_eia_aeo(context: AssetExecutionContext):
    """Extract tables from EIA's Annual Energy Outlook.

    We first extract a taxonomy from the AEO JSON blob, which connects
    individual data series to "categories". Some categories are associated with
    a specific table; others are associated with an AEO case or subject.

    The AEO cases are different scenarios such as "High Economic Growth" or
    "High Oil Price." They include "Reference" and "2022 AEO reference case" as
    well.

    The AEO subjects are only used for filtering which tables are relevant to
    which subjects, e.g. "Table 54 is relevant to Energy Prices." So we ignore
    those right now.

    The series each have their own timeseries data, as well as some metadata
    such as a series name and units. Many different dimensions can be inferred
    from the series names, but the data is somewhat heterogeneous so we do not
    try to infer those here and leave that to the transformation step.
    """
    name_to_number = {
        "raw_eia_aeo__electric_power_projections_regional": 54,
        "raw_eia_aeo__renewable_energy_regional": 56,
    }
    ds = context.resources.datastore
    year = 2023
    filename = f"AEO{year}.txt"

    with ds.get_zipfile_resource("eia_aeo", year=year).open(
        filename, mode="r"
    ) as aeo_raw:
        taxonomy = AEOTaxonomy(io.TextIOWrapper(aeo_raw))

    selected = context.op_execution_context.selected_output_names
    for asset_name in selected:
        yield Output(
            value=taxonomy.get_table(name_to_number[asset_name]), output_name=asset_name
        )


@asset_check(asset="raw_eia_aeo__electric_power_projections_regional")
def raw_table_54_invariants(df: pd.DataFrame) -> AssetCheckResult:
    """Check that the AEO Table 54 raw data conforms to *some* assumptions.

    * all values are non-null - i.e. every fact has date, fact name, category
    name, case, and unit
    * we have values from every electricity market module region
    * covers 20 cases and 26 electricity market module regions (25 regions + 1 national)
    """
    assert not df.empty
    assert df.notna().all().all()
    logger.info(df.columns)
    assert len(df.case.value_counts()) == 20
    assert len(df.category_name.value_counts()) == 26
    return AssetCheckResult(passed=True)
