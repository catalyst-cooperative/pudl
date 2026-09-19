"""Scikit-Learn classification pipeline for identifying related FERC 1 plant records.

Sadly FERC doesn't provide any kind of real IDs for the plants that report to them --
all we have is their names (a freeform string) and the data that is reported alongside
them. This is often enough information to be able to recognize which records ought to be
associated with each other year to year to create a continuous time series. However, we
want to do that programmatically, which means using some clustering / categorization
tools from scikit-learn
"""

import mlflow
import numpy as np
import pandas as pd
from dagster import graph, op

import pudl.logging_helpers
from pudl.analysis.ml_tools import experiment_tracking, models
from pudl.analysis.record_linkage import embed_dataframe
from pudl.analysis.record_linkage.link_cross_year import link_ids_cross_year

logger = pudl.logging_helpers.get_logger(__name__)


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


def _canonicalize_plant_ids(
    input_df: pd.DataFrame, record_labels: pd.Series
) -> pd.Series:
    """Replace arbitrary cluster labels with IDs that depend only on the clusters.

    The labels assigned by the clustering models are numbered by the internals of the
    algorithm, so they get permuted by tiny changes in the inputs even when the plants
    they describe are identical. Instead, give every cluster the position of its
    earliest record when all records are sorted by :data:`_CANONICAL_RECORD_ORDER`.
    Because that only depends on the members of a cluster, splitting, merging, or adding
    a plant does not change the IDs of unrelated plants, and diffs between runs
    highlight real changes to the clusters.

    Args:
        input_df: The records that were clustered. Must contain the columns in
            :data:`_CANONICAL_RECORD_ORDER`.
        record_labels: The cluster label of each record, indexed like ``input_df``.

    Returns:
        A series of plant IDs, indexed like ``input_df``.
    """
    ordered = input_df[_CANONICAL_RECORD_ORDER].copy()
    ordered["record_label"] = record_labels.to_numpy()
    ordered = ordered.sort_values(_CANONICAL_RECORD_ORDER, kind="stable")
    ordered["position"] = np.arange(len(ordered))
    plant_ids = ordered.groupby("record_label")["position"].transform("min")
    return plant_ids.reindex(input_df.index).rename("plant_id_ferc1")


def _assign_plant_ids(
    ferc1_steam_df: pd.DataFrame, input_df: pd.DataFrame, record_labels: pd.Series
) -> pd.DataFrame:
    """Add canonical ``plant_id_ferc1`` to the steam table, matching on ``record_id``."""
    plant_ids = _canonicalize_plant_ids(input_df, record_labels)
    plant_ids.index = input_df["record_id"]
    return ferc1_steam_df.assign(
        plant_id_ferc1=ferc1_steam_df["record_id"].map(plant_ids)
    )


@op(tags={"dagster/priority": 10})
def plants_steam_validate_ids(
    ferc_to_ferc_tracker: experiment_tracking.ExperimentTracker,
    ferc1_steam_df: pd.DataFrame,
    input_df: pd.DataFrame,
    label_df: pd.DataFrame,
) -> pd.DataFrame:
    """Tests that plant_id_ferc1 timeseries includes one record per year.

    Args:
        ferc1_steam_df: A DataFrame of the data from the FERC 1 Steam table.
        input_df: The (sorted) records that were clustered to produce ``label_df``.
        label_df: A DataFrame containing column of newly assigned plant labels.

    Returns:
        The steam dataframe with a ``plant_id_ferc1`` column added.
    """
    ferc1_steam_df = _assign_plant_ids(
        ferc1_steam_df, input_df, label_df["record_label"]
    )

    ##########################################################################
    # FERC PLANT ID ERROR CHECKING STUFF
    ##########################################################################

    # Test to make sure that we don't have any plant_id_ferc1 time series
    # which include more than one record from a given year. Warn the user
    # if we find such cases (which... we do, as of writing)
    year_dupes = (
        ferc1_steam_df.groupby(["plant_id_ferc1", "report_year"])
        .size()
        .rename("year_dupes")
        .reset_index()
        .query("year_dupes>1")
    )

    ferc_to_ferc_tracker.execute_logging(
        lambda: mlflow.log_metric("year_duplicates", len(year_dupes))
    )

    if len(year_dupes) > 0:
        for dupe in year_dupes.itertuples():
            logger.error(
                f"Found report_year={dupe.report_year} "
                f"{dupe.year_dupes} times in "
                f"plant_id_ferc1={dupe.plant_id_ferc1}"
            )
    else:
        logger.info("No duplicate years found in any plant_id_ferc1. Hooray!")

    return ferc1_steam_df


@op(tags={"dagster/priority": 10})
def merge_steam_fuel_dfs(
    ferc1_steam_df: pd.DataFrame,
    fuel_fractions: pd.DataFrame,
) -> pd.DataFrame:
    """Merge steam plants and fuel dfs to prepare inputs for ferc plant matching."""
    ffc = list(fuel_fractions.filter(regex=".*_fraction_mmbtu$").columns)

    # Grab fuel consumption proportions for use in assigning plant IDs:
    merged = ferc1_steam_df.merge(
        fuel_fractions[["utility_id_ferc1", "plant_name_ferc1", "report_year"] + ffc],
        on=["utility_id_ferc1", "plant_name_ferc1", "report_year"],
        how="left",
    ).astype({"plant_type": str, "construction_type": str})
    # Clustering results depend on the order of the input rows, so sort by primary key.
    # The clustering ops assume a default RangeIndex, so reset it after sorting.
    return merged.sort_values("record_id", kind="stable").reset_index(drop=True)


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
    label_df = link_ids_cross_year(input_df, feature_matrix, experiment_tracker)

    return plants_steam_validate_ids(
        experiment_tracker, core_ferc1__yearly_steam_plants_sched402, input_df, label_df
    )
