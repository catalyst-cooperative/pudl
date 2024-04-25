"""Scikit-Learn classification pipeline for identifying related FERC 1 plant records.

Sadly FERC doesn't provide any kind of real IDs for the plants that report to them --
all we have is their names (a freeform string) and the data that is reported alongside
them. This is often enough information to be able to recognize which records ought to be
associated with each other year to year to create a continuous time series. However, we
want to do that programmatically, which means using some clustering / categorization
tools from scikit-learn
"""

import mlflow
import pandas as pd
from dagster import graph, op

import pudl
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


@op
def plants_steam_validate_ids(
    ferc_to_ferc_tracker: experiment_tracking.ExperimentTracker,
    ferc1_steam_df: pd.DataFrame,
    label_df: pd.DataFrame,
) -> pd.DataFrame:
    """Tests that plant_id_ferc1 timeseries includes one record per year.

    Args:
        ferc1_steam_df: A DataFrame of the data from the FERC 1 Steam table.
        label_df: A DataFrame containing column of newly assigned plant labels.

    Returns:
        The input dataframe, to enable method chaining.
    """
    # Add column of labels to steam df
    ferc1_steam_df.loc[:, "plant_id_ferc1"] = label_df["record_label"]

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


@op
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
    logger.info("Identifying distinct large FERC plants for ID assignment.")

    input_df = merge_steam_fuel_dfs(
        core_ferc1__yearly_steam_plants_sched402,
        out_ferc1__yearly_steam_plants_fuel_by_plant_sched402,
    )
    feature_matrix = ferc_dataframe_embedder(input_df, experiment_tracker)
    label_df = link_ids_cross_year(input_df, feature_matrix, experiment_tracker)

    return plants_steam_validate_ids(
        experiment_tracker, core_ferc1__yearly_steam_plants_sched402, label_df
    )
