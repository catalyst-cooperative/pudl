"""Extract EIA AEO data from the bulk JSON."""

import io
import itertools
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum

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
from pandera import DataFrameModel, Field
from pydantic import BaseModel

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class AEOCategory(BaseModel):
    """Describe how the AEO data is categorized.

    Categories are the basic way in which metadata that is shared across
    multiple data series is represented.
    """

    category_id: int
    parent_category_id: int
    name: str
    notes: str
    childseries: list[str]


class AEOSeries(BaseModel):
    """Describe actual AEO timeseries data.

    This includes the data itself as well as some timeseries-specific metadata
    that may not be shared across multiple timeseries.
    """

    series_id: str
    name: str
    last_updated: str
    units: str | None = None
    data: list[tuple[str, str | float]]  # time, value


class AEOTable(DataFrameModel):
    """Data schema for a raw AEO table."""

    projection_year: int = Field(coerce=True)
    value: str = Field(coerce=True)
    units: str = Field(coerce=True)
    series_name: str = Field(
        coerce=True,
        description="A human-readable name for the series that this value came "
        "from. Usually contains many fields separated by `` : ``.",
    )
    category_name: str = Field(
        coerce=True,
        description="A human-readable name for the category that the above "
        "series belongs to. Usually contains comma-separated fields.",
    )
    model_case_eiaaeo: str = Field(coerce=True)


class AEOTaxonomy:
    """Container for *all* the information in one AEO report.

    AEO reports are composed of *categories*, which are metadata about multiple
    data series, and *series*, which are the actual data + metadata associated
    with one specific time series.

    The categories and series form a DAG structure with 5 generations: root,
    case, subject, leaf category, and data series.

    The first generation is the root - there is one root node which is nameless
    and which all other nodes descend from.

    The second generation is the "cases." Cases are different scenarios within
    the AEO. These have names like "Reference case," "High Economic Growth",
    "Low Oil and Gas Supply." All direct children of the root node are cases.

    The third generation is the "subjects." These are high-level tags, with
    names like "Energy Prices", "Energy Consumption", etc. These are largely
    used for filtering in the AEO data UI, so we ignore these.

    The fourth generation is the "leaf categories." These are named things like
    "Table 54.  Electric Power Projections by Electricity Market Module Region,
    United States" and have a long list of "child series" which actually
    contain the data. In other words, these leaf categories map the notion of
    an AEO "table" to the actual data.

    The fifth generation is the "data series." These actually contain the data
    points, and have no children. They have names like "Electricity : Electric
    Power Sector : Cumulative Planned Additions : Coal" and "Coal Supply :
    Delivered Prices : Electric Power." As you can see the names imply a bunch
    of different dimensions, which we don't try to make sense of in the extract
    step.

    In the first four generations we see a strictly branching tree, but many
    leaf categories can point at the same data series so the whole taxonomy is
    a DAG. This is because of two reasons:

    * the subject tag doesn't affect data values, but because of the tree
      structure, each leaf category is repeated once for each subject, leading to
      multiple *duplicated* leaf categories pointing at the same data series.
    * some data series are relevant to multiple different tables - so multiple
      different leaf categories point at the same data series. In this case we
      would expect the names of the leaf category to reflect their different
      identities.

    Note, also, that there is no structural notion of a "Table" in the AEO
    data. That information is carried purely by the names of the leaf
    categories.
    """

    class EntityType(Enum):
        """These are the three types of entities in AEO."""

        ROOT = 1001
        CATEGORY = 1002
        SERIES = 1003

    @dataclass
    class CheckSpec:
        """Encapsulate shared checks for the taxonomy structure."""

        generation: str
        typecheck: Callable[[int | str], bool]
        in_degree: Callable[[int], bool]
        out_degree: Callable[[int], bool]

    def __init__(self, records: Iterable[str]):
        """Load AEO JSON records into a graph datastructure.

        Args:
            records: the strings that contain an AEO report to parse.

        """
        categories, series = self.__load_records(records)
        self.graph = self.__generate_graph(categories, series)
        generations = self.__generation_invariants()
        self.__cases = generations[1]
        self.__sanitize_re = re.compile(r"\W+")

    def __load_records(
        self, records: Iterable[str]
    ) -> tuple[dict[int, AEOCategory], dict[str, AEOSeries]]:
        """Read AEO JSON blob into memory.

        A single JSON object can represent either a category or a series, so we
        parse those into two separate mappings.
        """
        all_categories: dict[int, AEOCategory] = {}
        all_series: dict[str, AEOSeries] = {}
        for record in records:
            if "category_id" in record:
                category = AEOCategory.model_validate_json(record)
                all_categories[category.category_id] = category
            elif "series_id" in record:
                series = AEOSeries.model_validate_json(record)
                all_series[series.series_id] = series
            else:
                raise ValueError(f"Line had neither series nor category ID: {record}")
        return all_categories, all_series

    def __generate_graph(
        self, categories: dict[int, AEOCategory], series: dict[str, AEOSeries]
    ) -> nx.DiGraph:
        """Stitch categories and series together into a DAG."""

        def get_all_edges(category: AEOCategory) -> Iterable[tuple[int, str | int]]:
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

        nx.set_node_attributes(graph, categories | series)
        return graph

    def __generation_invariants(self) -> list:  # noqa: C901
        """Check that the graph behaves the way we expect.

        We have a few generic checks for *all* generations - node type,
        in-degree, and out-degree.

        We also have bespoke checks for individual generations as needed.

        Returns the list of generations for further manipulation.
        """

        def _typecheck(node_id: int | str) -> AEOTaxonomy.EntityType:
            category_id = self.graph.nodes[node_id].get("category_id")
            series_id = self.graph.nodes[node_id].get("series_id")
            if category_id is None and series_id is None:
                return AEOTaxonomy.EntityType.ROOT
            if series_id is None and category_id is not None:
                return AEOTaxonomy.EntityType.CATEGORY
            if category_id is None and series_id is not None:
                return AEOTaxonomy.EntityType.SERIES
            raise ValueError(
                f"Found record with both category and series ID: {node_id}"
            )

        def is_root(c):
            return _typecheck(c) == AEOTaxonomy.EntityType.ROOT

        def is_category(c):
            return _typecheck(c) == AEOTaxonomy.EntityType.CATEGORY

        def is_series(c):
            return _typecheck(c) == AEOTaxonomy.EntityType.SERIES

        generations = list(nx.topological_generations(self.graph))

        specs: list[AEOTaxonomy.CheckSpec] = [
            AEOTaxonomy.CheckSpec(
                generation="root",
                typecheck=is_root,
                in_degree=lambda n: n == 0,
                out_degree=lambda n: n >= 1,
            ),
            AEOTaxonomy.CheckSpec(
                generation="case",
                typecheck=is_category,
                in_degree=lambda n: n == 1,
                out_degree=lambda n: n >= 1,
            ),
            AEOTaxonomy.CheckSpec(
                generation="subject",
                typecheck=is_category,
                in_degree=lambda n: n == 1,
                out_degree=lambda n: n >= 1,
            ),
            AEOTaxonomy.CheckSpec(
                generation="leaf_category",
                typecheck=is_category,
                in_degree=lambda n: n == 1,
                out_degree=lambda n: n >= 1,
            ),
            AEOTaxonomy.CheckSpec(
                generation="data_series",
                typecheck=is_series,
                in_degree=lambda n: n >= 1,
                out_degree=lambda n: n == 0,
            ),
        ]

        type_errors = [
            ("wrong_type", spec.generation, node_id)
            for spec, generation in zip(specs, generations, strict=True)
            for node_id in generation
            if not spec.typecheck(node_id)
        ]

        in_degree_errors = [
            ("wrong_in_degree", spec.generation, node_id)
            for spec, generation in zip(specs, generations, strict=True)
            for node_id in generation
            if not spec.in_degree(self.graph.in_degree(node_id))
        ]

        out_degree_errors = [
            ("wrong_out_degree", spec.generation, node_id)
            for spec, generation in zip(specs, generations, strict=True)
            for node_id in generation
            if not spec.out_degree(self.graph.out_degree(node_id))
        ]

        errors = type_errors + in_degree_errors + out_degree_errors

        if len(generations[0]) != 1:
            errors.append(("wrong_length", "root", generations[0]))

        # all leaf categories should be associated with a table
        leaf_cats_no_table_name = [
            c
            for c in generations[3]
            if not self.graph.nodes[c].get("name", "").lower().startswith("table")
        ]
        if len(leaf_cats_no_table_name) != 0:
            errors.append(("no_table_name", "leaf_category", leaf_cats_no_table_name))

        if len(errors) > 0:
            raise RuntimeError(f"Taxonomy graph invariants violated: {errors}")

        return generations

    def __sanitize(self, s: str) -> str:
        return re.sub(self.__sanitize_re, "_", s.lower().strip().replace(" : ", "__"))

    def __series_to_records(
        self, series_id: str, potential_parents: set[int]
    ) -> pd.DataFrame:
        """Turn a data series into records we can feed into a DataFrame.

        This uses graph ancestor data to figure out what case this series
        belongs to.

        This series may be associated with multiple different tables in the
        graph. In that case, we'll need to filter down only to the leaf
        categories that are relevant to the table we're creating a DataFrame
        for. We do that by passing in ``potential_parents`` as a parameter.
        """
        # we don't expect multiple case nodes to overlap in name. If we make
        # this a list, then we will raise an error if we do see the wrong size.
        case_names = [
            self.graph.nodes[a_id]["name"]
            for a_id in nx.ancestors(self.graph, series_id)
            if a_id in self.__cases
        ]
        if len(case_names) != 1:
            raise ValueError(
                f"Found multiple AEO cases for series {series_id}: {case_names}"
            )
        case_name = case_names[0]

        # We do expect the many leaf categories to share a name, so we use a
        # set to automatically deduplicate.
        parent_names = {
            self.graph.nodes[p_id]["name"]
            for p_id in self.graph.predecessors(series_id)
            if p_id in potential_parents
        }
        if len(parent_names) != 1:
            raise ValueError(
                f"Found multiple parents for series {series_id}: {parent_names}"
            )
        parent_name = parent_names.pop()

        # in addition to case_name and parent_name, we get some information
        # from the actual series itself.
        series = self.graph.nodes[series_id]

        # 2024-04-20: we don't sanitize the series/category names here because
        # we want to preserve all sorts of weird information for the
        # transformation step.
        records = (
            {
                "projection_year": d[0],
                "value": d[1],
                "units": series["units"],
                "series_name": series["name"],
                "category_name": parent_name,
                "model_case_eiaaeo": case_name,
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

        # many series belong to more than one category, hence turning this into a set
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
        return AEOTable(pd.DataFrame.from_records(series_records))


@multi_asset(
    outs={
        "raw_eiaaeo__energy_consumption_by_sector_and_source": AssetOut(
            is_required=False
        ),
        "raw_eiaaeo__natural_gas_supply_disposition_and_prices": AssetOut(
            is_required=False
        ),
        "raw_eiaaeo__coal_supply_disposition_and_price": AssetOut(is_required=False),
        "raw_eiaaeo__macroeconomic_indicators": AssetOut(is_required=False),
        "raw_eiaaeo__electric_power_projections_regional": AssetOut(is_required=False),
    },
    can_subset=True,
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eiaaeo(context: AssetExecutionContext):
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
        "raw_eiaaeo__energy_consumption_by_sector_and_source": 2,
        "raw_eiaaeo__natural_gas_supply_disposition_and_prices": 13,
        "raw_eiaaeo__coal_supply_disposition_and_price": 15,
        "raw_eiaaeo__macroeconomic_indicators": 20,
        "raw_eiaaeo__electric_power_projections_regional": 54,
    }
    ds = context.resources.datastore

    # TODO (daz 2024-04-15): one day, we might want the AEO for more than one
    # year. But for now we only take the first year from the settings.
    year = context.resources.dataset_settings.eia.eiaaeo.years[0]
    filename = f"AEO{year}.txt"

    with ds.get_zipfile_resource("eiaaeo", year=year).open(
        filename, mode="r"
    ) as aeo_raw:
        taxonomy = AEOTaxonomy(io.TextIOWrapper(aeo_raw))

    selected = context.op_execution_context.selected_output_names
    for asset_name in selected:
        yield Output(
            value=taxonomy.get_table(name_to_number[asset_name]), output_name=asset_name
        )


@asset_check(
    asset="raw_eiaaeo__electric_power_projections_regional",
    blocking=True,
)
def raw_table_54_invariants(df: pd.DataFrame) -> AssetCheckResult:
    """Check that the AEO Table 54 raw data conforms to *some* assumptions."""
    # all values are non-null - i.e. every fact has date, fact name, category
    # name, case, and unit
    assert not df.empty
    assert df.notna().all().all()
    # covers 20 cases and 26 electricity market module regions (25 regions + 1 national)
    assert len(df.model_case_eiaaeo.value_counts()) == 20
    assert len(df.category_name.value_counts()) == 26
    return AssetCheckResult(passed=True)
