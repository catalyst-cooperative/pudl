"""Scikit-Learn classification pipeline for identifying related FERC 1 plant records.

Sadly FERC doesn't provide any kind of real IDs for the plants that report to them --
all we have is their names (a freeform string) and the data that is reported alongside
them. This is often enough information to be able to recognize which records ought to be
associated with each other year to year to create a continuous time series. However, we
want to do that programmatically, which means using some clustering / categorization
tools from scikit-learn
"""

import numpy as np
import pandas as pd
from dagster import graph, op

from pudl.analysis.ml_tools import experiment_tracking, models
from pudl.analysis.record_linkage import embed_dataframe
from pudl.analysis.record_linkage.link_cross_year import link_ids_cross_year

_FUEL_COLS = [
    "coal_fraction_mmbtu",
    "gas_fraction_mmbtu",
    "nuclear_fraction_mmbtu",
    "oil_fraction_mmbtu",
    "waste_fraction_mmbtu",
]

ferc_dataframe_embedder = embed_dataframe.dataframe_embedder_factory(
    "ferc_embedder",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.NameCleaner(),
                embed_dataframe.TextVectorizer(),
            ],
            weight=2.0,
            columns=["plant_name_ferc1"],
        ),
        "plant_type": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_empty_str"),
                embed_dataframe.CategoricalVectorizer(),
            ],
            weight=2.0,
            columns=["plant_type"],
        ),
        "construction_type": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_empty_str"),
                embed_dataframe.CategoricalVectorizer(),
            ],
            columns=["construction_type"],
        ),
        "capacity_mw": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericalVectorizer(),
            ],
            columns=["capacity_mw"],
        ),
        "construction_year": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="fix_int_na"),
                embed_dataframe.CategoricalVectorizer(),
            ],
            columns=["construction_year"],
        ),
        "utility_id_ferc1": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.CategoricalVectorizer()],
            columns=["utility_id_ferc1"],
        ),
        "fuel_fractions": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericalVectorizer(),
                embed_dataframe.NumericalNormalizer(),
            ],
            columns=_FUEL_COLS,
        ),
    },
)


#: Columns that put FERC 1 steam records in a canonical order. ``record_id`` is the
#: primary key, so it makes the order total and the resulting IDs reproducible.
_CANONICAL_RECORD_ORDER = [
    "report_year",
    "utility_id_ferc1",
    "plant_name_ferc1",
    "record_id",
]


def _canonicalize_plant_ids(labeled_df: pd.DataFrame) -> pd.Series:
    """Replace arbitrary cluster labels with IDs that depend only on the clusters.

    The labels assigned by the clustering models are numbered by the internals of the
    algorithm, so they get permuted by tiny changes in the inputs even when the plants
    they describe are identical. Instead, give every cluster the position of its
    earliest record when all records are sorted by :data:`_CANONICAL_RECORD_ORDER`.
    Because that only depends on the members of a cluster, splitting, merging, or adding
    a plant does not change the IDs of unrelated plants, and diffs between runs
    highlight real changes to the clusters.

    Args:
        labeled_df: The records that were clustered, with their cluster label in a
            ``record_label`` column. Must also contain the columns in
            :data:`_CANONICAL_RECORD_ORDER`.

    Returns:
        A series of plant IDs, indexed like ``labeled_df``.
    """
    ordered = labeled_df[[*_CANONICAL_RECORD_ORDER, "record_label"]].sort_values(
        _CANONICAL_RECORD_ORDER, kind="stable"
    )
    ordered["position"] = np.arange(1, len(ordered) + 1)
    plant_ids = ordered.groupby("record_label")["position"].transform("min")
    return plant_ids.reindex(labeled_df.index).rename("plant_id_ferc1")


def _assign_plant_ids(
    ferc1_steam_df: pd.DataFrame, labeled_df: pd.DataFrame
) -> pd.DataFrame:
    """Add canonical ``plant_id_ferc1`` to the steam table, matching on ``record_id``."""
    plant_ids = _canonicalize_plant_ids(labeled_df)
    plant_ids.index = labeled_df["record_id"]
    return ferc1_steam_df.assign(
        plant_id_ferc1=ferc1_steam_df["record_id"].map(plant_ids)
    )


@op(tags={"dagster/priority": 10})
def assign_plant_ids(
    ferc1_steam_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
) -> pd.DataFrame:
    """Add canonical ``plant_id_ferc1`` values to the steam table.

    Args:
        ferc1_steam_df: A DataFrame of the data from the FERC 1 Steam table.
        labeled_df: The records that were clustered, with a ``record_label`` column
            giving each record's assigned cluster.

    Returns:
        The steam dataframe with a ``plant_id_ferc1`` column added.
    """
    return _assign_plant_ids(ferc1_steam_df, labeled_df)


@op(tags={"dagster/priority": 10})
def merge_steam_fuel_dfs(
    ferc1_steam_df: pd.DataFrame,
    fuel_fractions: pd.DataFrame,
) -> pd.DataFrame:
    """Merge steam plants and fuel dfs to prepare inputs for ferc plant matching."""
    ffc = list(fuel_fractions.filter(regex=".*_fraction_mmbtu$").columns)

    # Grab fuel consumption proportions for use in assigning plant IDs:
    return ferc1_steam_df.merge(
        fuel_fractions[["utility_id_ferc1", "plant_name_ferc1", "report_year"] + ffc],
        on=["utility_id_ferc1", "plant_name_ferc1", "report_year"],
        how="left",
    ).astype({"plant_type": str, "construction_type": str})


@models.pudl_model(
    "_out_ferc1__yearly_steam_plants_sched402_with_plant_ids",
    config_from_yaml=True,
)
@graph
def ferc_to_ferc(
    experiment_tracker: experiment_tracking.ExperimentTracker,
    core_ferc1__yearly_steam_plants_sched402: pd.DataFrame,
    out_ferc1__yearly_steam_plants_fuel_by_plant_sched402: pd.DataFrame,
) -> pd.DataFrame:
    """Assign IDs to the large steam plants."""
    ###########################################################################
    # FERC PLANT ID ASSIGNMENT
    ###########################################################################
    # Now we need to assign IDs to the large steam plants, since FERC doesn't
    # do this for us.
    input_df = merge_steam_fuel_dfs(
        core_ferc1__yearly_steam_plants_sched402,
        out_ferc1__yearly_steam_plants_fuel_by_plant_sched402,
    )
    feature_matrix = ferc_dataframe_embedder(input_df, experiment_tracker)
    labeled_df = link_ids_cross_year(input_df, feature_matrix, experiment_tracker)

    return assign_plant_ids(core_ferc1__yearly_steam_plants_sched402, labeled_df)
