"""Connect FERC1 plant tables to EIA's plant-parts with record linkage.

FERC plant records are reported very non-uniformly. In the same table there are records
that are reported as whole plants, individual generators, and collections of prime
movers. This means portions of EIA plants that correspond to a plant record in FERC
Form 1 are heterogeneous, which complicates using the two data sets together.

The EIA plant data is much cleaner and more uniformly structured. The are generators
with ids and plants with ids reported in *separate* tables. Several generator IDs are
typically grouped under a single plant ID. In :mod:`pudl.analysis.plant_parts_eia`,
we create a large number of synthetic aggregated records representing many possible
slices of a power plant which could in theory be what is actually reported in the FERC
Form 1.

In this module we infer which of the many ``plant_parts_eia`` records is most likely to
correspond to an actually reported FERC Form 1 plant record. This is done with
``splink``, a Python package that implements Fellegi-Sunter's model of record linkage.

We train the parameters of the ``splink`` model using manually labeled training data
that links together several thousand EIA and FERC plant records. This trained model is
used to predict matches on the full dataset (see :func:`get_model_predictions`) using a
threshold match probability to predict if records are a match or not.

The model can return multiple EIA match options for each FERC1 record, so we rank the
matches and choose the one with the highest score. Any matches identified by the model
which are in conflict with our training data are overwritten with the manually
assigned associations (see :func:`override_bad_predictions`). The final match results
are the connections we keep as the matches between FERC1 plant records and EIA
plant-parts.
"""

import importlib
from typing import Literal

import jellyfish
import mlflow
import numpy as np
import pandas as pd
from dagster import Out, graph, op
from splink import DuckDBAPI, Linker, SettingsCreator

import pudl
from pudl.analysis.ml_tools import experiment_tracking, models
from pudl.analysis.record_linkage import embed_dataframe, name_cleaner
from pudl.analysis.record_linkage.eia_ferc1_inputs import (
    InputManager,
    restrict_train_connections_on_date_range,
)
from pudl.analysis.record_linkage.eia_ferc1_model_config import (
    BLOCKING_RULES,
    COMPARISONS,
)
from pudl.metadata.classes import DataSource, Resource

logger = pudl.logging_helpers.get_logger(__name__)

MATCHING_COLS = [
    "plant_name",
    "utility_name",
    "fuel_type_code_pudl",
    "installation_year",
    "construction_year",
    "capacity_mw",
    "net_generation_mwh",
]
# retain these columns either for blocking or validation
# not going to match with these
ID_COL = ["record_id"]
EXTRA_COLS = [
    "report_year",
    "plant_id_pudl",
    "utility_id_pudl",
    "plant_name_mphone",
    "utility_name_mphone",
]

plant_name_cleaner = name_cleaner.CompanyNameCleaner(
    cleaning_rules_list=[
        "remove_word_the_from_the_end",
        "remove_word_the_from_the_beginning",
        "replace_ampersand_by_AND",
        "replace_hyphen_by_space",
        "replace_underscore_by_space",
        "remove_all_punctuation",
        "remove_numbers",
        "remove_math_symbols",
        "add_space_before_opening_parentheses",
        "add_space_after_closing_parentheses",
        "remove_parentheses",
        "remove_brackets",
        "remove_curly_brackets",
        "enforce_single_space_between_words",
    ]
)

col_cleaner = embed_dataframe.dataframe_cleaner_factory(
    "col_cleaner",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.NameCleaner(
                    company_cleaner=plant_name_cleaner, return_as_dframe=True
                )
            ],
            columns=["plant_name"],
        ),
        "utility_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner(return_as_dframe=True)],
            columns=["utility_name"],
        ),
        "fuel_type_code_pudl": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.FuelTypeFiller(
                    fuel_type_col="fuel_type_code_pudl",
                    name_col="plant_name",
                )
            ],
            columns=["fuel_type_code_pudl", "plant_name"],
        ),
        "net_generation_mwh": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="zero_to_null")
            ],
            columns=["net_generation_mwh"],
        ),
        "capacity_mw": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="zero_to_null")
            ],
            columns=["capacity_mw"],
        ),
    },
)


@op
def get_compiled_input_manager(plants_all_ferc1, fbp_ferc1, plant_parts_eia):
    """Get :class:`InputManager` object with compiled inputs for model."""
    inputs = InputManager(plants_all_ferc1, fbp_ferc1, plant_parts_eia)
    # compile/cache inputs upfront. Hopefully we can catch any errors in inputs early.
    inputs.execute()
    return inputs


@op(out={"eia_df": Out(), "ferc_df": Out()})
def get_input_dfs(inputs):
    """Get EIA and FERC inputs for the model."""
    eia_df = (
        inputs.get_plant_parts_eia_true()
        .reset_index()
        .rename(
            columns={
                "record_id_eia": "record_id",
                "plant_name_eia": "plant_name",
                "utility_name_eia": "utility_name",
            }
        )
    )
    ferc_df = (
        inputs.get_plants_ferc1()
        .reset_index()
        .rename(
            columns={
                "record_id_ferc1": "record_id",
                "plant_name_ferc1": "plant_name",
                "utility_name_ferc1": "utility_name",
            }
        )
    )
    return eia_df, ferc_df


@op
def prepare_for_matching(df, transformed_df):
    """Prepare the input dataframes for matching with splink."""

    def _get_metaphone(row, col_name):
        if pd.isnull(row[col_name]):
            return None
        return jellyfish.metaphone(row[col_name])

    # replace old cols with transformed cols
    for col in transformed_df.columns:
        orig_col_name = col.split("__")[1]
        df[orig_col_name] = transformed_df[col]
    df["installation_year"] = pd.to_datetime(df["installation_year"], format="%Y")
    df["construction_year"] = pd.to_datetime(df["construction_year"], format="%Y")
    df["plant_name_mphone"] = df.apply(_get_metaphone, axis=1, args=("plant_name",))
    df["utility_name_mphone"] = df.apply(_get_metaphone, axis=1, args=("utility_name",))
    cols = ID_COL + MATCHING_COLS + EXTRA_COLS
    df = df.loc[:, cols]
    return df


@op
def get_training_data_df(inputs):
    """Get the manually created training data."""
    train_df = inputs.get_train_df().reset_index()
    train_df = train_df[["record_id_ferc1", "record_id_eia"]].rename(
        columns={"record_id_eia": "record_id_l", "record_id_ferc1": "record_id_r"}
    )
    train_df.loc[:, "source_dataset_r"] = "ferc_df"
    train_df.loc[:, "source_dataset_l"] = "eia_df"
    train_df.loc[:, "clerical_match_score"] = (
        1  # this column shows that all these labels are positive labels
    )
    return train_df


@op
def get_model_predictions(eia_df, ferc_df, train_df, experiment_tracker):
    """Train splink model and output predicted matches."""
    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="record_id",
        additional_columns_to_retain=["plant_id_pudl", "utility_id_pudl"],
        comparisons=COMPARISONS,
        blocking_rules_to_generate_predictions=BLOCKING_RULES,
        retain_matching_columns=True,
        retain_intermediate_calculation_columns=True,
        probability_two_random_records_match=(1.0 / len(eia_df)),
    )
    linker = Linker(
        [eia_df, ferc_df],
        settings=settings,
        input_table_aliases=["eia_df", "ferc_df"],
        db_api=DuckDBAPI(),
    )
    linker.table_management.register_table(train_df, "training_labels", overwrite=True)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
    linker.training.estimate_m_from_pairwise_labels("training_labels")
    threshold_prob = 0.9
    experiment_tracker.execute_logging(
        lambda: mlflow.log_params({"threshold match probability": threshold_prob})
    )
    preds_df = linker.inference.predict(threshold_match_probability=threshold_prob)
    return preds_df.as_pandas_dataframe()


@op()
def get_best_matches(
    preds_df,
    inputs,
    experiment_tracker: experiment_tracking.ExperimentTracker,
):
    """Get the best EIA match for each FERC record and log performance metrics."""
    preds_df = (
        preds_df.rename(
            columns={"record_id_l": "record_id_eia", "record_id_r": "record_id_ferc1"}
        )
        .sort_values(by="match_probability", ascending=False)
        .groupby("record_id_ferc1")
        .first()
    )
    preds_df = preds_df.reset_index()
    train_df = inputs.get_train_df().reset_index()
    true_pos = get_true_pos(preds_df, train_df)
    false_pos = get_false_pos(preds_df, train_df)
    false_neg = get_false_neg(preds_df, train_df)
    logger.info(
        "Metrics before overrides:\n"
        f"   True positives:  {true_pos}\n"
        f"   False positives: {false_pos}\n"
        f"   False negatives: {false_neg}\n"
        f"   Precision:       {true_pos / (true_pos + false_pos):.03}\n"
        f"   Recall:          {true_pos / (true_pos + false_neg):.03}\n"
        f"   Accuracy:        {true_pos / len(train_df):.03}\n"
        "Precision = of the training data FERC records that the model predicted a match for, this percentage was correct.\n"
        "A measure of accuracy when the model makes a prediction.\n"
        "Recall = of all of the training data FERC records, the model predicted a match for this percentage.\n"
        "A measure of the coverage of FERC records in the predictions.\n"
        "Accuracy = what percentage of the training data did the model correctly predict.\n"
        "A measure of overall correctness."
    )
    experiment_tracker.execute_logging(
        lambda: mlflow.log_metrics(
            {
                "precision": round(true_pos / (true_pos + false_pos), 3),
                "recall": round(true_pos / (true_pos + false_neg), 3),
                "accuracy": round(true_pos / len(train_df), 3),
            }
        )
    )
    return preds_df


@op(
    out={
        "out_pudl__yearly_assn_eia_ferc1_plant_parts": Out(
            io_manager_key="pudl_io_manager"
        )
    },
    tags={"memory-use": "high"},
)
def get_full_records_with_overrides(best_match_df, inputs, experiment_tracker):
    """Join full dataframe onto matches to make usable and get stats.

    Override the predictions dataframe with the training data, so that all
    known bad predictions are corrected. Then join the EIA and FERC data on
    so that the matches are usable. Drop model parameter and match probability
    columns generated by splink. Log the coverage of the matches on the
    FERC input data.
    """
    best_match_df = override_bad_predictions(best_match_df, inputs.get_train_df())
    connected_df = prettyify_best_matches(
        matches_best=best_match_df,
        plant_parts_eia_true=inputs.get_plant_parts_eia_true(),
        plants_ferc1=inputs.get_plants_ferc1(),
    )
    _log_match_coverage(connected_df, experiment_tracker=experiment_tracker)
    for match_set in ["all", "overrides"]:
        check_match_consistency(
            connected_df,
            inputs.get_train_df(),
            match_set=match_set,
            experiment_tracker=experiment_tracker,
        )
    connected_df = add_null_overrides(
        connected_df
    )  # Override specified values with NA record_id_eia
    return Resource.from_id(
        "out_pudl__yearly_assn_eia_ferc1_plant_parts"
    ).enforce_schema(connected_df)


@models.pudl_model(
    "out_pudl__yearly_assn_eia_ferc1_plant_parts",
    config_from_yaml=False,
)
@graph
def ferc_to_eia(
    experiment_tracker: experiment_tracking.ExperimentTracker,
    out_ferc1__yearly_all_plants: pd.DataFrame,
    out_ferc1__yearly_steam_plants_fuel_by_plant_sched402: pd.DataFrame,
    out_eia__yearly_plant_parts: pd.DataFrame,
) -> pd.DataFrame:
    """Using splink model the connection between FERC1 plants and EIA plant-parts.

    Args:
        out_ferc1__yearly_all_plants: Table of all of the FERC1-reporting plants.
        out_ferc1__yearly_steam_plants_fuel_by_plant_sched402: Table of the fuel
            reported aggregated to the FERC1 plant-level.
        out_eia__yearly_plant_parts: The EIA plant parts list.
    """
    inputs = get_compiled_input_manager(
        out_ferc1__yearly_all_plants,
        out_ferc1__yearly_steam_plants_fuel_by_plant_sched402,
        out_eia__yearly_plant_parts,
    )
    eia_df, ferc_df = get_input_dfs(inputs)
    train_df = get_training_data_df(inputs)
    # apply cleaning transformations to some columns
    transformed_eia_df = col_cleaner(eia_df, experiment_tracker)
    transformed_ferc_df = col_cleaner(ferc_df, experiment_tracker)
    # prepare for matching with splink
    ferc_df = prepare_for_matching(ferc_df, transformed_ferc_df)
    eia_df = prepare_for_matching(eia_df, transformed_eia_df)
    # train model and predict matches
    preds_df = get_model_predictions(
        eia_df=eia_df,
        ferc_df=ferc_df,
        train_df=train_df,
        experiment_tracker=experiment_tracker,
    )
    best_match_df = get_best_matches(
        preds_df=preds_df,
        inputs=inputs,
        experiment_tracker=experiment_tracker,
    )
    ferc1_eia_connected_df = get_full_records_with_overrides(
        best_match_df,
        inputs,
        experiment_tracker=experiment_tracker,
    )

    return ferc1_eia_connected_df


# the correct EIA record is predicted for a FERC record
def get_true_pos(pred_df, train_df):
    """Get the number of correctly predicted matches."""
    return train_df.merge(
        pred_df, how="left", on=["record_id_ferc1", "record_id_eia"], indicator=True
    )._merge.value_counts()["both"]


# an incorrect EIA record is predicted for a FERC record
def get_false_pos(pred_df, train_df):
    """Get the number of incorrectly predicted matches."""
    shared_preds = train_df.merge(
        pred_df, how="inner", on="record_id_ferc1", suffixes=("_true", "_pred")
    )
    return len(
        shared_preds[shared_preds.record_id_eia_true != shared_preds.record_id_eia_pred]
    )


# FERC record is in training data but no prediction made
def get_false_neg(pred_df, train_df):
    """Get the number of matches from the training data where no prediction is made."""
    return train_df.merge(
        pred_df, how="left", on=["record_id_ferc1"], indicator=True
    )._merge.value_counts()["left_only"]


def prettyify_best_matches(
    matches_best: pd.DataFrame,
    plant_parts_eia_true: pd.DataFrame,
    plants_ferc1: pd.DataFrame,
    debug: bool = False,
) -> pd.DataFrame:
    """Make the EIA-FERC best matches usable.

    Use the ID columns from the best matches to merge together both EIA plant-parts data
    and FERC plant data. This removes the comparison vectors (the floats between 0 and 1
    that compare the two columns from each dataset).
    """
    connects_ferc1_eia = (
        # first merge in the EIA plant-parts
        pd.merge(
            matches_best[["record_id_ferc1", "record_id_eia", "match_type"]],
            plant_parts_eia_true.reset_index(),
            how="left",
            on=["record_id_eia"],
            validate="m:1",  # multiple FERC records can have the same EIA match
        )
        # then merge in the FERC data we want the backbone of this table to be
        # the plant records so we have all possible FERC plant records, even
        # the unmapped ones
        .merge(
            plants_ferc1,
            how="outer",
            on=["record_id_ferc1"],
            suffixes=("_eia", "_ferc1"),
            validate="1:1",
            indicator=True,
        )
    )

    # now we have some important cols that have dataset suffixes that we want to condense
    def fill_eia_w_ferc1(x, col):
        return x[f"{col}_eia"].fillna(x[f"{col}_ferc1"])

    condense_cols = ["report_year", "plant_id_pudl", "utility_id_pudl"]
    connects_ferc1_eia = (
        connects_ferc1_eia.assign(
            **{col: fill_eia_w_ferc1(connects_ferc1_eia, col) for col in condense_cols}
        )
        .drop(
            columns=[
                col + dataset for col in condense_cols for dataset in ["_eia", "_ferc1"]
            ]
        )
        .assign(
            report_date=lambda x: pd.to_datetime(
                x.report_year, format="%Y", errors="coerce"
            ),
        )
    )

    no_ferc = connects_ferc1_eia[
        (connects_ferc1_eia._merge == "left_only")
        & (connects_ferc1_eia.record_id_eia.notnull())
        & ~(connects_ferc1_eia.record_id_ferc1.str.contains("_hydro_", na=False))
        & ~(connects_ferc1_eia.record_id_ferc1.str.contains("_gnrt_plant_", na=False))
    ]
    connects_ferc1_eia = connects_ferc1_eia.drop(columns=["_merge"])
    if not no_ferc.empty:
        message = (
            "Help. \nI'm trapped in this computer and I can't get out.\n"
            ".... jk there shouldn't be any matches between FERC and EIA\n"
            "that have EIA matches but aren't in the FERC plant table, but we\n"
            f"got {len(no_ferc)}. Check the training data and "
            "prettyify_best_matches()"
        )
        if debug:
            logger.warning(message)
            return no_ferc
        logger.info(
            "grrrr there are some FERC-EIA matches that aren't in the steam \
            table but this is because they are linked to retired EIA generators."
        )
        logger.warning(message)

    return connects_ferc1_eia


def check_match_consistency(
    connects_ferc1_eia: pd.DataFrame,
    train_df: pd.DataFrame,
    experiment_tracker: experiment_tracking.ExperimentTracker,
    match_set: Literal["all", "overrides"] = "all",
) -> pd.DataFrame:
    """Check how consistent FERC-EIA matches are with FERC-FERC matches.

    We have two record linkage processes: one that links FERC plant records across time,
    and another that links FERC plant records to EIA plant-parts. This function checks
    that the two processes are as consistent with each other as we expect.  Here
    "consistent" means that each FERC plant ID is associated with a single EIA plant
    parts ID across time. The reverse is not necessarily required -- a single EIA plant
    part ID may be associated with various FERC plant IDs across time.

    Args:
        connects_ferc1_eia: Matches of FERC1 to EIA.
        train_df: training data.
        match_set: either ``all`` - to check all of the matches - or ``overrides`` - to
            check just the overrides. Default is ``all``. The overrides are less
            consistent than all of the data, so this argument changes the consistency
            threshold for this check.
    """
    # these are the default
    expected_consistency = 0.73
    expected_uniform_capacity_consistency = 0.85
    mask = connects_ferc1_eia.record_id_eia.notnull()

    if match_set == "overrides":
        expected_consistency = 0.39
        expected_uniform_capacity_consistency = 0.75
        train_ferc1 = train_df.reset_index()
        over_f1 = (
            train_ferc1[train_ferc1.record_id_ferc1.str.contains("_steam_")]
            .set_index("record_id_ferc1")
            .index
        )
        over_ferc1_ids = (
            connects_ferc1_eia.set_index("record_id_ferc1")
            .loc[over_f1]
            .plant_id_ferc1.unique()
        )

        mask = mask & connects_ferc1_eia.plant_id_ferc1.isin(over_ferc1_ids)

    count = (
        connects_ferc1_eia[mask]
        .groupby(["plant_id_ferc1"])[["plant_part_id_eia", "capacity_mw_ferc1"]]
        .nunique()
    )
    actual_consistency = len(count[count.plant_part_id_eia == 1]) / len(count)
    logger.info(
        f"Matches with consistency across years of {match_set} matches is "
        f"{actual_consistency:.1%}"
    )
    if actual_consistency < expected_consistency:
        logger.warning(
            "Inter-year consistency between plant_id_ferc1 and plant_part_id_eia of "
            f"{match_set} matches {actual_consistency:.1%} is less than the expected "
            f"value of {expected_consistency:.1%}."
        )
    actual_uniform_capacity_consistency = (
        len(count)
        - len(count[(count.plant_part_id_eia > 1) & (count.capacity_mw_ferc1 == 1)])
    ) / len(count)
    logger.info(
        "Matches with a uniform FERC 1 capacity have an inter-year consistency between "
        "plant_id_ferc1 and plant_part_id_eia of "
        f"{actual_uniform_capacity_consistency:.1%}"
    )
    if match_set == "all":
        experiment_tracker.execute_logging(
            lambda: mlflow.log_metrics(
                {
                    "plant_id_ferc1 consistency across matches": round(
                        actual_consistency, 2
                    ),
                    "uniform capacity plant_id_ferc1 and plant_part_id_eia consistency": round(
                        actual_uniform_capacity_consistency, 2
                    ),
                }
            )
        )
    if actual_uniform_capacity_consistency < expected_uniform_capacity_consistency:
        logger.warning(
            "Inter-year consistency between plant_id_ferc1 and plant_part_id_eia of "
            "matches with uniform FERC 1 capacity "
            f"{actual_uniform_capacity_consistency:.1%} is less than the expected "
            f"value of {expected_uniform_capacity_consistency:.1%}."
        )
    return count


def override_bad_predictions(
    match_df: pd.DataFrame, train_df: pd.DataFrame
) -> pd.DataFrame:
    """Override incorrect predictions with the correct match from training data.

    Args:
        match_df: A dataframe of the best matches with only one match for each
            FERC1 record.
        train_df: A dataframe of the training data.
    """
    train_df = train_df.reset_index()
    override_df = pd.merge(
        match_df,
        train_df[["record_id_eia", "record_id_ferc1"]],
        on="record_id_ferc1",
        how="outer",
        suffixes=("_pred", "_train"),
        indicator=True,
        validate="1:1",
    )
    # construct new record_id_eia column with incorrect preds overwritten
    override_df["record_id_eia"] = np.where(
        override_df["_merge"] == "left_only",
        override_df["record_id_eia_pred"],
        override_df["record_id_eia_train"],
    )
    # create a the column match_type which indicates whether the match is good
    # based on the training data
    override_rows = (override_df._merge == "both") & (
        override_df.record_id_eia_train != override_df.record_id_eia_pred
    )
    correct_rows = (override_df._merge == "both") & (
        override_df.record_id_eia_train == override_df.record_id_eia_pred
    )
    incorrect_rows = override_df._merge == "right_only"

    override_df.loc[:, "match_type"] = "prediction; not in training data"
    override_df.loc[override_rows, "match_type"] = "incorrect prediction; overwritten"
    override_df.loc[correct_rows, "match_type"] = "correct match"
    override_df.loc[incorrect_rows, "match_type"] = (
        "incorrect prediction; no predicted match"
    )
    # print out stats
    percent_correct = len(override_df[override_df.match_type == "correct match"]) / len(
        train_df
    )
    percent_overwritten = len(
        override_df[override_df.match_type == "incorrect prediction; overridden"]
    ) / len(train_df)
    logger.info(
        "Matches stats:\n"
        f"Percent of training data matches correctly predicted: {percent_correct:.02}\n"
        f"Percent of training data overwritten in matches: {percent_overwritten:.02}\n"
    )
    override_df = override_df.drop(
        columns=["_merge", "record_id_eia_train", "record_id_eia_pred"]
    )
    return override_df


def _log_match_coverage(
    connects_ferc1_eia,
    experiment_tracker: experiment_tracking.ExperimentTracker,
):
    eia_years = DataSource.from_id("eia860").working_partitions["years"]
    # get the matches from just the EIA working years
    matches = connects_ferc1_eia[
        (connects_ferc1_eia.report_date.dt.year.isin(eia_years))
        & (connects_ferc1_eia.record_id_eia.notnull())
    ]
    # get all records from just the EIA working years
    possible_records = connects_ferc1_eia[
        connects_ferc1_eia.report_date.dt.year.isin(eia_years)
    ]
    fuel_type_coverage = len(matches[matches.energy_source_code_1.notnull()]) / len(
        matches
    )
    tech_type_coverage = len(matches[matches.technology_description.notnull()]) / len(
        matches
    )

    def _get_subtable(table_name):
        return possible_records[
            possible_records.record_id_ferc1.str.contains(f"{table_name}")
        ]

    def _get_match_pct(df):
        return len(df[df["record_id_eia"].notna()]) / len(df)

    def _get_capacity_coverage(df):
        return (
            df[df["record_id_eia"].notna()].capacity_mw_ferc1.sum()
            / df.capacity_mw_ferc1.sum()
        )

    steam_cov = _get_match_pct(_get_subtable("steam"))
    steam_cap_cov = _get_capacity_coverage(_get_subtable("steam"))
    small_gen_cov = _get_match_pct(_get_subtable("gnrt_plant"))
    hydro_cov = _get_match_pct(_get_subtable("hydro"))
    pumped_storage_cov = _get_match_pct(_get_subtable("pumped"))
    logger.info(
        "Coverage for matches during EIA working years:\n"
        f"    Fuel type: {fuel_type_coverage:.01%}\n"
        f"    Tech type: {tech_type_coverage:.01%}\n"
        "Coverage for all steam table records during EIA working years:\n"
        f"    EIA matches: {steam_cov:.01%}\n"
        "Coverage for steam table capacity during EIA working years:\n"
        f"    EIA matches: {steam_cap_cov:.01%}\n"
        f"Coverage for all small gen table records during EIA working years:\n"
        f"    EIA matches: {small_gen_cov:.01%}\n"
        f"Coverage for all hydro table records during EIA working years:\n"
        f"    EIA matches: {hydro_cov:.01%}\n"
        f"Coverage for all pumped storage table records during EIA working years:\n"
        f"    EIA matches: {pumped_storage_cov:.01%}"
    )
    experiment_tracker.execute_logging(
        lambda: mlflow.log_metrics(
            {
                "steam table coverage": round(steam_cov, 3),
                "steam table capacity coverage": round(steam_cap_cov, 3),
                "small gen table coverage": round(small_gen_cov, 3),
                "hydro table coverage": round(hydro_cov, 3),
                "pumped storage table coverage": round(pumped_storage_cov, 3),
            }
        )
    )


def add_null_overrides(connects_ferc1_eia):
    """Override known null matches with pd.NA.

    There is no way to indicate in the training data that certain FERC records have no
    proper EIA match. That is to say--you can't specify a blank match or tell the AI
    not to match a given record. Because we've gone through by hand and know for a fact
    that some FERC records have no EIA match (even when you aggregate generators), we
    have to add in these null matches after the fact.

    This function reads in a list of record_id_ferc1 values that are known to have no
    corresponding EIA record match and makes sure they are mapped as NA in the final
    record linkage output. It also updates the match_type field to indicate that this
    value has been overridden.
    """
    logger.info("Overriding specified record_id_ferc1 values with NA record_id_eia")
    # Get record_id_ferc1 values that should be overridden to have no EIA match
    null_overrides = pd.read_csv(
        importlib.resources.files("pudl.package_data.glue") / "eia_ferc1_null.csv"
    ).pipe(
        restrict_train_connections_on_date_range,
        id_col="record_id_ferc1",
        start_date=min(
            connects_ferc1_eia[~(connects_ferc1_eia.record_id_eia.isnull())].report_date
        ),
        end_date=max(
            connects_ferc1_eia[~(connects_ferc1_eia.record_id_eia.isnull())].report_date
        ),
    )
    # Make sure there is content!
    if null_overrides.empty:
        raise AssertionError(
            f"No null overrides found. Consider checking for file at {null_overrides}"
        )
    logger.debug(f"Found {len(null_overrides)} null overrides")
    # List of EIA columns to null. Ideally would like to get this from elsewhere, but
    # compiling this here for now...
    eia_cols_to_null = Resource.from_id("out_eia__yearly_plant_parts").get_field_names()
    # Make all EIA values NA for record_id_ferc1 values in the Null overrides list and
    # make the match_type column say "overridden"
    connects_ferc1_eia.loc[
        connects_ferc1_eia["record_id_ferc1"].isin(null_overrides.record_id_ferc1),
        eia_cols_to_null,
    ] = np.nan
    connects_ferc1_eia.loc[
        connects_ferc1_eia["record_id_ferc1"].isin(null_overrides.record_id_ferc1),
        "match_type",
    ] = "overridden"
    return connects_ferc1_eia
