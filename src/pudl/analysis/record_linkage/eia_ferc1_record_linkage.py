"""Connect FERC1 plant tables to EIA's plant-parts via record linkage.

FERC plant records are reported very non-uniformly. In the same table there are records
that are reported as whole plants, individual generators, and collections of prime
movers. This means portions of EIA plants that correspond to a plant record in FERC
Form 1 are heterogeneous, which complicates using the two data sets together.

The EIA plant data is much cleaner and more uniformly structured. The are generators
with ids and plants with ids reported in *seperate* tables. Several generator IDs are
typically grouped under a single plant ID. In :mod:`pudl.analysis.plant_parts_eia`,
we create a large number of synthetic aggregated records representing many possible
slices of a power plant which could in theory be what is actually reported in the FERC
Form 1.

In this module we infer which of the many ``plant_parts_eia`` records is most likely to
correspond to an actually reported FERC Form 1 plant record. this is done with a
logistic regression model.

We train the logistic regression model using manually labeled training data that links
together several thousand EIA and FERC plant records, and use grid search cross
validation to select a best set of hyperparameters. This trained model is used to
predict matches on the full dataset (see :func:`run_model`). The model can return
multiple EIA match options for each FERC1 record, so we rank the matches and choose the
one with the highest score (see :func:`find_best_matches`). Any matches identified by
the model which are in conflict with our training data are overwritten with the manually
assigned associations (see :func:`overwrite_bad_predictions`). The final match results
are the connections we keep as the matches between FERC1 plant records and EIA
plant-parts.
"""

import importlib.resources
from typing import Literal

import mlflow
import numpy as np
import pandas as pd
from dagster import Out, graph, op
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import GridSearchCV, train_test_split

import pudl
import pudl.helpers
from pudl.analysis.ml_tools import experiment_tracking, models
from pudl.analysis.plant_parts_eia import match_to_single_plant_part
from pudl.analysis.record_linkage import embed_dataframe
from pudl.metadata.classes import DataSource, Resource

logger = pudl.logging_helpers.get_logger(__name__)

pair_vectorizers = embed_dataframe.dataframe_embedder_factory(
    "ferc1_eia_pair_vectorizers",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.NameCleaner(),
                embed_dataframe.StringSimilarityScorer(
                    metric="jaro_winkler",
                    col1="plant_name_ferc1",
                    col2="plant_name_eia",
                    output_name="plant_name",
                ),
            ],
            columns=["plant_name_ferc1", "plant_name_eia"],
        ),
        "utility_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.NameCleaner(),
                embed_dataframe.StringSimilarityScorer(
                    metric="jaro_winkler",
                    col1="utility_name_ferc1",
                    col2="utility_name_eia",
                    output_name="utility_name",
                ),
            ],
            columns=["utility_name_ferc1", "utility_name_eia"],
        ),
        "net_generation_mwh": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="exponential",
                    col1="net_generation_mwh_ferc1",
                    col2="net_generation_mwh_eia",
                    output_name="net_generation_mwh",
                    scale=1000,
                ),
            ],
            columns=["net_generation_mwh_ferc1", "net_generation_mwh_eia"],
        ),
        "capacity_mw": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="exponential",
                    col1="capacity_mw_ferc1",
                    col2="capacity_mw_eia",
                    output_name="capacity_mw",
                    scale=10,
                ),
            ],
            columns=["capacity_mw_ferc1", "capacity_mw_eia"],
        ),
        "total_fuel_cost": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="exponential",
                    col1="total_fuel_cost_ferc1",
                    col2="total_fuel_cost_eia",
                    output_name="total_fuel_cost",
                    scale=10000,
                    offset=2500,
                    missing_value=0.5,
                ),
            ],
            columns=["total_fuel_cost_ferc1", "total_fuel_cost_eia"],
        ),
        "total_mmbtu": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="exponential",
                    col1="total_mmbtu_ferc1",
                    col2="total_mmbtu_eia",
                    output_name="total_mmbtu",
                    scale=100,
                    offset=1,
                    missing_value=0.5,
                ),
            ],
            columns=["total_mmbtu_ferc1", "total_mmbtu_eia"],
        ),
        "capacity_factor": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="linear",
                    col1="capacity_factor_ferc1",
                    col2="capacity_factor_eia",
                    output_name="capacity_factor",
                ),
            ],
            columns=["capacity_factor_ferc1", "capacity_factor_eia"],
        ),
        "fuel_cost_per_mmbtu": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="linear",
                    col1="fuel_cost_per_mmbtu_ferc1",
                    col2="fuel_cost_per_mmbtu_eia",
                    output_name="fuel_cost_per_mmbtu",
                ),
            ],
            columns=["fuel_cost_per_mmbtu_ferc1", "fuel_cost_per_mmbtu_eia"],
        ),
        "heat_rate_mmbtu_mwh": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_zero"),
                embed_dataframe.NumericSimilarityScorer(
                    method="linear",
                    col1="unit_heat_rate_mmbtu_per_mwh_ferc1",
                    col2="unit_heat_rate_mmbtu_per_mwh_eia",
                    output_name="heat_rate_mmbtu_mwh",
                ),
            ],
            columns=[
                "unit_heat_rate_mmbtu_per_mwh_ferc1",
                "unit_heat_rate_mmbtu_per_mwh_eia",
            ],
        ),
        "fuel_type_code_pudl": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.ColumnCleaner(cleaning_function="null_to_empty_str"),
                embed_dataframe.NumericSimilarityScorer(
                    method="exact",
                    col1="fuel_type_code_pudl_ferc1",
                    col2="fuel_type_code_pudl_eia",
                    output_name="fuel_type_code_pudl",
                ),
            ],
            columns=["fuel_type_code_pudl_ferc1", "fuel_type_code_pudl_eia"],
        ),
        "installation_year": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.NumericSimilarityScorer(
                    method="linear",
                    col1="installation_year_ferc1",
                    col2="installation_year_eia",
                    output_name="installation_year",
                )
            ],
            columns=["installation_year_ferc1", "installation_year_eia"],
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


@op(out={"all_pairs_df": Out(), "train_pairs_df": Out()})
def get_pairs_dfs(inputs):
    """Get a dataframe with all possible FERC to EIA record pairs.

    Merge the FERC and EIA records on ``block_col`` to generate possible
    record pairs for the matching model.

    Arguments:
        inputs: :class:`InputManager` object.

    Returns:
        A dataframe with all possible record pairs from all the input
        data and a dataframe with all possible record pairs from the
        training data.
    """
    ferc1_df = inputs.get_plants_ferc1().reset_index()
    eia_df = inputs.get_plant_parts_eia_true().reset_index()
    block_col = "plant_id_report_year_util_id"
    all_pairs_df = ferc1_df.merge(
        eia_df, how="inner", on=block_col, suffixes=("_ferc1", "_eia")
    ).set_index(["record_id_ferc1", "record_id_eia"])
    ferc1_train_df = inputs.get_train_ferc1().reset_index()
    eia_train_df = inputs.get_train_eia().reset_index()
    block_col = "plant_id_report_year_util_id"
    train_pairs_df = ferc1_train_df.merge(
        eia_train_df, how="inner", on=block_col, suffixes=("_ferc1", "_eia")
    ).set_index(["record_id_ferc1", "record_id_eia"])
    return (all_pairs_df, train_pairs_df)


@op
def get_y_label_df(train_pairs_df, inputs):
    """Get the dataframe of y labels.

    For each record pair in ``train_pairs_df``, a 0 if the pair is not
    a match and a 1 if the pair is a match.
    """
    label_df = np.where(
        train_pairs_df.merge(
            inputs.get_train_df(),
            how="left",
            left_index=True,
            right_index=True,
            indicator=True,
        )["_merge"]
        == "both",
        1,
        0,
    )
    return label_df


@op
def get_best_matches_with_overwrites(match_df, inputs):
    """Get dataframe with the best EIA match for each FERC record."""
    return find_best_matches(match_df).pipe(overwrite_bad_predictions, inputs.train_df)


@op
def run_matching_model(features_train, features_all, y_df, experiment_tracker):
    """Run model to match EIA to FERC records."""
    return run_model(
        features_train=features_train,
        features_all=features_all,
        y_df=y_df,
        experiment_tracker=experiment_tracker,
    )


@op(
    out={
        "out_pudl__yearly_assn_eia_ferc1_plant_parts": Out(
            io_manager_key="pudl_io_manager"
        )
    }
)
def get_match_full_records(best_match_df, inputs):
    """Join full dataframe onto matches to make usable and get stats."""
    connected_df = prettyify_best_matches(
        matches_best=best_match_df,
        plant_parts_eia_true=inputs.get_plant_parts_eia_true(),
        plants_ferc1=inputs.get_plants_ferc1(),
        train_df=inputs.get_train_df(),
    ).pipe(add_null_overrides)  # Override specified values with NA record_id_eia
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
    """Coordinate the connection between FERC1 plants and EIA plant-parts.

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
    all_pairs_df, train_pairs_df = get_pairs_dfs(inputs)
    features_all = pair_vectorizers(all_pairs_df, experiment_tracker)
    features_train = pair_vectorizers(train_pairs_df, experiment_tracker)
    y_df = get_y_label_df(train_pairs_df, inputs)
    match_df = run_matching_model(
        features_train=features_train,
        features_all=features_all,
        y_df=y_df,
        experiment_tracker=experiment_tracker,
    )
    # choose one EIA match for each FERC record
    best_match_df = get_best_matches_with_overwrites(match_df, inputs)
    # join EIA and FERC columns back on
    ferc1_eia_connected_df = get_match_full_records(best_match_df, inputs)
    return ferc1_eia_connected_df


class InputManager:
    """Class prepare inputs for linking FERC1 and EIA."""

    def __init__(
        self,
        plants_all_ferc1: pd.DataFrame,
        fbp_ferc1: pd.DataFrame,
        plant_parts_eia: pd.DataFrame,
    ):
        """Initialize inputs manager that gets inputs for linking FERC and EIA.

        Args:
            plants_all_ferc1: Table of all of the FERC1-reporting plants.
            fbp_ferc1: Table of the fuel reported aggregated to the FERC1 plant-level.
            plant_parts_eia: The EIA plant parts list.
            start_date: Start date that cooresponds to the tables passed in.
            end_date: End date that cooresponds to the tables passed in.
        """
        self.plant_parts_eia = plant_parts_eia.set_index("record_id_eia")
        self.plants_all_ferc1 = plants_all_ferc1
        self.fbp_ferc1 = fbp_ferc1

        self.start_date = min(plant_parts_eia.report_date)
        self.end_date = max(plant_parts_eia.report_date)
        if (
            yrs_len := len(yrs_range := range(self.start_date.year, self.end_date.year))
        ) < 3:
            logger.warning(
                f"Your attempting to fit a model with only {yrs_len} years "
                f"({yrs_range}). This will probably result in overfitting. In order to"
                " best judge the results, use more years of data."
            )

        # generate empty versions of the inputs.. this let's this class check
        # whether or not the compiled inputs exist before compilnig
        self.plant_parts_eia_true = None
        self.plants_ferc1 = None
        self.train_df = None
        self.train_ferc1 = None
        self.train_eia = None

    def get_plant_parts_eia_true(self, clobber: bool = False) -> pd.DataFrame:
        """Get the EIA plant-parts with only the unique granularities."""
        if self.plant_parts_eia_true is None or clobber:
            self.plant_parts_eia_true = (
                pudl.analysis.plant_parts_eia.plant_parts_eia_distinct(
                    self.plant_parts_eia
                )
            )
        return self.plant_parts_eia_true

    def get_plants_ferc1(self, clobber: bool = False) -> pd.DataFrame:
        """Prepare FERC1 plants data for record linkage with EIA plant-parts.

        This method grabs two tables (``plants_all_ferc1`` and ``fuel_by_plant_ferc1``,
        accessed originally via :meth:`pudl.output.pudltabl.PudlTabl.plants_all_ferc1`
        and :meth:`pudl.output.pudltabl.PudlTabl.fbp_ferc1` respectively) and ensures
        that the columns the same as their EIA counterparts, because the output of this
        method will be used to link FERC and EIA.

        Returns:
            A cleaned table of FERC1 plants plant records with fuel cost data.
        """
        if clobber or self.plants_ferc1 is None:
            fbp_cols_to_use = [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "utility_id_pudl",
                "fuel_cost",
                "fuel_mmbtu",
                "primary_fuel_by_mmbtu",
            ]

            logger.info("Preparing the FERC1 tables.")
            self.plants_ferc1 = (
                self.plants_all_ferc1.merge(
                    self.fbp_ferc1[fbp_cols_to_use],
                    on=[
                        "report_year",
                        "utility_id_ferc1",
                        "utility_id_pudl",
                        "plant_name_ferc1",
                    ],
                    how="left",
                )
                .pipe(pudl.helpers.convert_cols_dtypes, "ferc1")
                .assign(
                    installation_year=lambda x: (
                        x.installation_year.astype("float")
                    ),  # need for comparison vectors
                    plant_id_report_year=lambda x: (
                        x.plant_id_pudl.astype(str) + "_" + x.report_year.astype(str)
                    ),
                    plant_id_report_year_util_id=lambda x: (
                        x.plant_id_report_year + "_" + x.utility_id_pudl.astype(str)
                    ),
                    fuel_cost_per_mmbtu=lambda x: (x.fuel_cost / x.fuel_mmbtu),
                    unit_heat_rate_mmbtu_per_mwh=lambda x: (
                        x.fuel_mmbtu / x.net_generation_mwh
                    ),
                )
                .rename(
                    columns={
                        "record_id": "record_id_ferc1",
                        "opex_plants": "opex_plant",
                        "fuel_cost": "total_fuel_cost",
                        "fuel_mmbtu": "total_mmbtu",
                        "opex_fuel_per_mwh": "fuel_cost_per_mwh",
                        "primary_fuel_by_mmbtu": "fuel_type_code_pudl",
                    }
                )
                .set_index("record_id_ferc1")
            )
        return self.plants_ferc1

    def get_train_df(self) -> pd.DataFrame:
        """Get the training connections.

        Prepare them if the training data hasn't been connected to FERC data yet.
        """
        if self.train_df is None:
            self.train_df = prep_train_connections(
                ppe=self.plant_parts_eia,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        return self.train_df

    def get_train_records(
        self,
        dataset_df: pd.DataFrame,
        dataset_id_col: Literal["record_id_eia", "record_id_ferc1"],
    ) -> pd.DataFrame:
        """Generate a set of known connections from a dataset using training data.

        This method grabs only the records from the the datasets (EIA or FERC)
        that we have in our training data.

        Args:
            dataset_df: either FERC1 plants table (result of :meth:`get_plants_ferc1`) or
                EIA plant-parts (result of :meth:`get_plant_parts_eia_true`).
            dataset_id_col: Identifying column name. Either ``record_id_eia`` for
                ``plant_parts_eia_true`` or ``record_id_ferc1`` for ``plants_ferc1``.
        """
        known_df = (
            pd.merge(
                dataset_df,
                self.get_train_df().reset_index()[[dataset_id_col]],
                left_index=True,
                right_on=[dataset_id_col],
            )
            .drop_duplicates(subset=[dataset_id_col])
            .set_index(dataset_id_col)
            .astype({"total_fuel_cost": float, "total_mmbtu": float})
        )
        return known_df

    # Note: Is there a way to avoid these little shell methods? I need a
    # standard way to access
    def get_train_eia(self, clobber: bool = False) -> pd.DataFrame:
        """Get the known training data from EIA."""
        if clobber or self.train_eia is None:
            self.train_eia = self.get_train_records(
                self.get_plant_parts_eia_true(), dataset_id_col="record_id_eia"
            )
        return self.train_eia

    def get_train_ferc1(self, clobber: bool = False) -> pd.DataFrame:
        """Get the known training data from FERC1."""
        if clobber or self.train_ferc1 is None:
            self.train_ferc1 = self.get_train_records(
                self.get_plants_ferc1(), dataset_id_col="record_id_ferc1"
            )
        return self.train_ferc1

    def execute(self, clobber: bool = False):
        """Compile all the inputs.

        This method is only run if/when you want to ensure all of the inputs are
        generated all at once. While using :class:`InputManager`, it is preferred to
        access each input dataframe or index via their ``get_`` method instead of
        accessing the attribute.
        """
        # grab the FERC table we are trying
        # to connect to self.plant_parts_eia_true
        self.plants_ferc1 = self.get_plants_ferc1(clobber=clobber)
        self.plant_parts_eia_true = self.get_plant_parts_eia_true(clobber=clobber)

        # we want both the df version and just the index; skl uses just the
        # index and we use the df in merges and such
        self.train_df = self.get_train_df()

        # generate the list of the records in the EIA and FERC records that
        # exist in the training data
        self.train_eia = self.get_train_eia(clobber=clobber)
        self.train_ferc1 = self.get_train_ferc1(clobber=clobber)
        return


def run_model(
    features_train: pd.DataFrame,
    features_all: pd.DataFrame,
    y_df: pd.DataFrame,
    experiment_tracker: experiment_tracking.ExperimentTracker,
) -> pd.DataFrame:
    """Train Logistic Regression model using GridSearch cross validation.

    Search over the parameter grid for the best fit parameters for the
    Logistic Regression estimator on the training data. Predict matches
    on all the input features.

    Args:
        features_train: Dataframe of the feature vectors for the training data.
        features_all: Dataframe of the feature vectors for all the input data.
        y_df: Dataframe with 1 if a pair in ``features_train`` is a match and 0
            if a pair is not a match.

    Returns:
        A dataframe of matches with record_id_ferc1 and record_id_eia as the
        index and a column for the probability of a match.
    """
    param_grid = [
        {
            "solver": ["newton-cg", "lbfgs", "sag"],
            "C": [1000, 1, 10, 100],
            "class_weight": [None, "balanced"],
            "penalty": ["l2"],
        },
        {
            "solver": ["liblinear", "saga"],
            "C": [1000, 1, 10, 100],
            "class_weight": [None, "balanced"],
            "penalty": ["l1", "l2"],
        },
        {
            "solver": ["saga"],
            "C": [1000, 1, 10, 100],
            "class_weight": [None, "balanced"],
            "penalty": ["elasticnet"],
            "l1_ratio": [0.1, 0.3, 0.5, 0.7, 0.9],
        },
    ]
    X = features_train.matrix  # noqa: N806
    X_train, X_test, y_train, y_test = train_test_split(  # noqa: N806
        X, y_df, test_size=0.25, random_state=16
    )
    lrc = LogisticRegression()
    clf = GridSearchCV(estimator=lrc, param_grid=param_grid, verbose=True, n_jobs=-1)
    clf.fit(X=X_train, y=y_train)

    # Log best parameters
    experiment_tracker.execute_logging(lambda: mlflow.log_params(clf.best_params_))

    y_pred = clf.predict(X_test)
    precision, recall, f_score, _ = precision_recall_fscore_support(
        y_test, y_pred, average="binary"
    )
    accuracy = clf.best_score_
    logger.info(
        "Scores from the best model:\n"
        f"    Accuracy:  {accuracy:.02}\n"
        f"    F-Score:   {f_score:.02}\n"
        f"    Precision: {precision:.02}\n"
        f"    Recall:    {recall:.02}\n"
    )
    # Log model metrics
    experiment_tracker.execute_logging(
        lambda: mlflow.log_metrics(
            {
                "accuracy": accuracy,
                "f_score": f_score,
                "precision": precision,
                "recall": recall,
            }
        )
    )

    preds = clf.predict(features_all.matrix)
    probs = clf.predict_proba(features_all.matrix)
    final_df = pd.DataFrame(
        index=features_all.index, data={"match": preds, "prob_of_match": probs[:, 1]}
    )
    match_df = final_df[final_df.match == 1].sort_values(
        by="prob_of_match", ascending=False
    )
    return match_df


def find_best_matches(match_df):
    """Only keep the best EIA match for each FERC record.

    We only want one EIA match for each FERC1 plant record. If there are multiple
    predicted matches for a FERC1 record, the match with the highest
    probability found by the model is chosen.

    Args:
        match_df: A dataframe of matches with record_id_eia and record_id_ferc1
            as the index and a column for the probability of the match.

    Returns:
        Dataframe of matches with one EIA record for each FERC1 record.
    """
    # sort from lowest to highest probability of match
    match_df = match_df.reset_index().sort_values(
        by=["record_id_ferc1", "prob_of_match"]
    )

    best_match_df = match_df.groupby("record_id_ferc1").tail(1)

    return best_match_df


def overwrite_bad_predictions(
    match_df: pd.DataFrame, train_df: pd.DataFrame
) -> pd.DataFrame:
    """Overwrite incorrect predictions with the correct match from training data.

    Args:
        match_df: A dataframe of the best matches with only one match for each
            FERC1 record.
        train_df: A dataframe of the training data.
    """
    train_df = train_df.reset_index()
    overwrite_df = pd.merge(
        match_df,
        train_df[["record_id_eia", "record_id_ferc1"]],
        on="record_id_ferc1",
        how="outer",
        suffixes=("_pred", "_train"),
        indicator=True,
        validate="1:1",
    )
    # construct new record_id_eia column with incorrect preds overwritten
    overwrite_df["record_id_eia"] = np.where(
        overwrite_df["_merge"] == "left_only",
        overwrite_df["record_id_eia_pred"],
        overwrite_df["record_id_eia_train"],
    )
    # create a the column match_type which indicates whether the match is good
    # based on the training data
    overwrite_rows = (overwrite_df._merge == "both") & (
        overwrite_df.record_id_eia_train != overwrite_df.record_id_eia_pred
    )
    correct_rows = (overwrite_df._merge == "both") & (
        overwrite_df.record_id_eia_train == overwrite_df.record_id_eia_pred
    )
    incorrect_rows = overwrite_df._merge == "right_only"

    overwrite_df.loc[:, "match_type"] = "prediction; not in training data"
    overwrite_df.loc[overwrite_rows, "match_type"] = "incorrect prediction; overwritten"
    overwrite_df.loc[correct_rows, "match_type"] = "correct match"
    overwrite_df.loc[
        incorrect_rows, "match_type"
    ] = "incorrect prediction; no predicted match"
    # print out stats
    percent_correct = len(
        overwrite_df[overwrite_df.match_type == "correct match"]
    ) / len(train_df)
    percent_overwritten = len(
        overwrite_df[overwrite_df.match_type == "incorrect prediction; overwritten"]
    ) / len(train_df)
    logger.info(
        "Matches stats:\n"
        f"Percent of training data matches correctly predicted: {percent_correct:.02}\n"
        f"Percent of training data overwritten in matches: {percent_overwritten:.02}\n"
    )
    overwrite_df = overwrite_df.drop(
        columns=["_merge", "record_id_eia_train", "record_id_eia_pred"]
    )
    return overwrite_df


def restrict_train_connections_on_date_range(
    train_df: pd.DataFrame,
    id_col: Literal["record_id_eia", "record_id_ferc1"],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Restrict the training data based on the date ranges of the input tables.

    The training data for this model spans the full PUDL date range. We don't want to
    add training data from dates that are outside of the range of the FERC and EIA data
    we are attempting to match. So this function restricts the training data based on
    start and end dates.

    The training data is only the record IDs, which contain the report year inside them.
    This function compiles a regex using the date range to grab only training records
    which contain the years in the date range followed by and preceeded by ``_`` - in
    the format of ``record_id_eia``and ``record_id_ferc1``. We use that extracted year
    to determine
    """
    # filter training data by year range
    # first get list of all years to grab from training data
    date_range_years_str = "|".join(
        [
            f"{year}"
            for year in pd.date_range(start=start_date, end=end_date, freq="YS").year
        ]
    )
    logger.info(f"Restricting training data on years: {date_range_years_str}")
    train_df = train_df.assign(
        year_in_date_range=lambda x: x[id_col].str.extract(
            r"_{1}" + f"({date_range_years_str})" + "_{1}"
        )
    )
    # pd.drop returns a copy, so no need to copy this portion of train_df
    return train_df.loc[train_df.year_in_date_range.notnull()].drop(
        columns=["year_in_date_range"]
    )


def prep_train_connections(
    ppe: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    """Get and prepare the training connections for the model.

    We have stored training data, which consists of records with ids
    columns for both FERC and EIA. Those id columns serve as a connection
    between ferc1 plants and the EIA plant-parts. These connections
    indicate that a ferc1 plant records is reported at the same granularity
    as the connected EIA plant-parts record.

    Arguments:
        ppe: The EIA plant parts. Records from this dataframe will be connected to the
            training data records. This needs to be the full EIA plant parts, not just
            the distinct/true granularities because the training data could contain
            non-distinct records and this function reassigns those to their distinct
            counterparts.
        start_date: Beginning date for records from the training data. Should match the
            start date of ``ppe``. Default is None and all the training data will be used.
        end_date: Ending date for records from the training data. Should match the end
            date of ``ppe``. Default is None and all the training data will be used.

    Returns:
        A dataframe of training connections which has a MultiIndex of ``record_id_eia``
        and ``record_id_ferc1``.
    """
    ppe_cols = [
        "true_gran",
        "appro_part_label",
        "appro_record_id_eia",
        "plant_part",
        "ownership_dupe",
    ]
    # Read in one_to_many csv and join corresponding plant_match_ferc1 parts to FERC IDs
    one_to_many = (
        pd.read_csv(
            importlib.resources.files("pudl.package_data.glue")
            / "eia_ferc1_one_to_many.csv"
        )
        .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
    )

    # Get the 'm' generator IDs 1:m
    one_to_many_single = match_to_single_plant_part(
        multi_gran_df=ppe.loc[ppe.index.isin(one_to_many.record_id_eia)].reset_index(),
        ppl=ppe.reset_index(),
        part_name="plant_gen",
        cols_to_keep=["plant_part"],
    )[["record_id_eia_og", "record_id_eia"]].rename(
        columns={"record_id_eia": "gen_id", "record_id_eia_og": "record_id_eia"}
    )
    one_to_many = (
        one_to_many.merge(
            one_to_many_single,  # Match plant parts to generators
            on="record_id_eia",
            how="left",
            validate="1:m",
        )
        .drop_duplicates("gen_id")
        .merge(  # Match generators to ferc1_generator_agg_id
            ppe["ferc1_generator_agg_id"].reset_index(),
            left_on="gen_id",
            right_on="record_id_eia",
            how="left",
            validate="1:1",
        )
        .dropna(subset=["ferc1_generator_agg_id"])
        .drop(["record_id_eia_x", "record_id_eia_y"], axis=1)
        .merge(  # Match ferc1_generator_agg_id to new faked plant part record_id_eia
            ppe.loc[
                ppe.plant_part == "plant_match_ferc1",
                ["ferc1_generator_agg_id"],
            ].reset_index(),
            on="ferc1_generator_agg_id",
            how="left",
            validate="m:1",
        )
        .drop(["ferc1_generator_agg_id", "gen_id"], axis=1)
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
        .set_index("record_id_ferc1")
    )

    train_df = (
        pd.read_csv(
            importlib.resources.files("pudl.package_data.glue") / "eia_ferc1_train.csv"
        )
        .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
        .set_index("record_id_ferc1")
    )
    logger.info(f"Updating {len(one_to_many)} training records with 1:m plant parts.")
    train_df.update(one_to_many)  # Overwrite FERC records with faked 1:m parts.
    train_df = (
        # we want to ensure that the records are associated with a
        # "true granularity" - which is a way we filter out whether or
        # not each record in the EIA plant-parts is actually a
        # new/unique collection of plant parts
        # once the true_gran is dealt with, we also need to convert the
        # records which are ownership dupes to reflect their "total"
        # ownership counterparts
        train_df.reset_index()
        .pipe(
            restrict_train_connections_on_date_range,
            id_col="record_id_eia",
            start_date=start_date,
            end_date=end_date,
        )
        .merge(
            ppe[ppe_cols].reset_index(),
            how="left",
            on=["record_id_eia"],
            indicator=True,
        )
    )
    not_in_ppe = train_df[train_df._merge == "left_only"]
    if not not_in_ppe.empty:
        raise AssertionError(
            "Not all training data is associated with EIA records.\n"
            "record_id_ferc1's of bad training data records are: "
            f"{list(not_in_ppe.reset_index().record_id_ferc1)}"
        )
    train_df = (
        train_df.assign(
            plant_part=lambda x: x["appro_part_label"],
            record_id_eia=lambda x: x["appro_record_id_eia"],
        )
        .pipe(pudl.analysis.plant_parts_eia.reassign_id_ownership_dupes)
        .fillna(
            value={
                "record_id_eia": pd.NA,
            }
        )
        .set_index(  # sklearn wants a MultiIndex to do the stuff
            [
                "record_id_ferc1",
                "record_id_eia",
            ]
        )
    )
    train_df = train_df.drop(columns=ppe_cols + ["_merge"])
    return train_df


def prettyify_best_matches(
    matches_best: pd.DataFrame,
    plant_parts_eia_true: pd.DataFrame,
    plants_ferc1: pd.DataFrame,
    train_df: pd.DataFrame,
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

    _log_match_coverage(connects_ferc1_eia)
    for match_set in ["all", "overrides"]:
        check_match_consistency(
            connects_ferc1_eia,
            train_df,
            match_set=match_set,
        )
    return connects_ferc1_eia


def _log_match_coverage(connects_ferc1_eia):
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

    logger.info(
        "Coverage for matches during EIA working years:\n"
        f"    Fuel type: {fuel_type_coverage:.01%}\n"
        f"    Tech type: {tech_type_coverage:.01%}\n"
        "Coverage for all steam table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('steam')):.01%}\n"
        f"Coverage for all small gen table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('gnrt_plant')):.01%}\n"
        f"Coverage for all hydro table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('hydro')):.01%}\n"
        f"Coverage for all pumped storage table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('pumped')):.01%}"
    )


def check_match_consistency(
    connects_ferc1_eia: pd.DataFrame,
    train_df: pd.DataFrame,
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
    expected_consistency = 0.74
    expected_uniform_capacity_consistency = 0.85
    mask = connects_ferc1_eia.record_id_eia.notnull()

    if match_set == "overrides":
        expected_consistency = 0.39
        expected_uniform_capacity_consistency = 0.75
        train_ferc1 = train_df.reset_index()
        # these bbs were missing from connects_ferc1_eia. not totally sure why
        missing = [
            "f1_steam_2018_12_51_0_1",
            "f1_steam_2018_12_45_2_2",
            "f1_steam_2018_12_45_2_1",
            "f1_steam_2018_12_45_1_2",
            "f1_steam_2018_12_45_1_1",
            "f1_steam_2018_12_45_1_5",
            "f1_steam_2018_12_45_1_4",
            "f1_steam_2018_12_56_2_3",
        ]
        over_f1 = (
            train_ferc1[
                train_ferc1.record_id_ferc1.str.contains("_steam_")
                & ~train_ferc1.record_id_ferc1.isin(missing)
            ]
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
        raise AssertionError(
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
    if actual_uniform_capacity_consistency < expected_uniform_capacity_consistency:
        raise AssertionError(
            "Inter-year consistency between plant_id_ferc1 and plant_part_id_eia of "
            "matches with uniform FERC 1 capacity "
            f"{actual_uniform_capacity_consistency:.1%} is less than the expected "
            f"value of {expected_uniform_capacity_consistency:.1%}."
        )
    return count


def add_null_overrides(connects_ferc1_eia):
    """Override known null matches with pd.NA.

    There is no way to indicate in the training data that certain FERC records have no
    proper EIA match. That is to say--you can't specifiy a blank match or tell the AI
    not to match a given record. Because we've gone through by hand and know for a fact
    that some FERC records have no EIA match (even when you aggregate generators), we
    have to add in these null matches after the fact.

    This function reads in a list of record_id_ferc1 values that are known to have no
    cooresponding EIA record match and makes sure they are mapped as NA in the final
    record linkage output. It also updates the match_type field to indicate that this
    value has been overriden.
    """
    logger.info("Overriding specified record_id_ferc1 values with NA record_id_eia")
    # Get record_id_ferc1 values that should be overriden to have no EIA match
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
    # make the match_type column say "overriden"
    connects_ferc1_eia.loc[
        connects_ferc1_eia["record_id_ferc1"].isin(null_overrides.record_id_ferc1),
        eia_cols_to_null,
    ] = np.nan
    connects_ferc1_eia.loc[
        connects_ferc1_eia["record_id_ferc1"].isin(null_overrides.record_id_ferc1),
        "match_type",
    ] = "overridden"
    return connects_ferc1_eia
