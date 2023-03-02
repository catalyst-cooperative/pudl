"""Connect FERC1 plant tables to EIA's plant-parts via record linkage.

FERC plant records are reported... kind of messily. In the same table there are records
that are reported as whole plants, generators, collections of prime movers. So we have
this heterogeneously reported collection of parts of plants in FERC1.

EIA on the other hand is reported in a much cleaner way. The are generators with ids and
plants with ids reported in *seperate* tables. What a joy. In
:mod:`pudl.analysis.plant_parts_eia`, we've generated the EIA plant-parts. The EIA
plant-parts (often referred to as ``plant_parts_eia`` in this module) generated records
for various levels or granularities of plant parts.

For each of the FERC1 plant records we want to figure out which EIA plant-parts record
is the corresponding record. We do this with a record linkage/ scikitlearn logistic
regression model. The recordlinkage package helps us create feature vectors (via
:meth:`Features.make_features`) for each candidate match between FERC and EIA. Feature
vectors contain numbers between 0 and 1 that indicates the closeness for each value we
want to compare.

We use the feature vectors of our known-to-be-connected training data and Grid Search
cross validation to train the logistic regression model. This model is then used to
predict matches on the full dataset (:func:`run_model`). The model can return multiple
EIA match options for each FERC1 record, so we rank the matches and choose the
best/winning match (:func:`find_best_matches`). We then ensure those connections contain
our training data (:func:`overwrite_bad_predictions`). The final match results are the
connections we keep as the matches between FERC1 plant records and the EIA plant-parts.
"""

import importlib.resources
import os
from typing import Literal

import numpy as np
import pandas as pd
import recordlinkage as rl
from recordlinkage.compare import Exact, Numeric, String  # , Date
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import GridSearchCV  # , cross_val_score

import pudl
import pudl.helpers
from pudl.metadata.classes import DataSource

logger = pudl.logging_helpers.get_logger(__name__)
# Silence the recordlinkage logger, which is out of control


def execute(
    plants_all_ferc1: pd.DataFrame,
    fbp_ferc1: pd.DataFrame,
    plant_parts_eia: pd.DataFrame,
) -> pd.DataFrame:
    """Coordinate the connection between FERC1 plants and EIA plant-parts.

    Args:
        plants_all_ferc1: Table of all of the FERC1-reporting plants.
        fbp_ferc1: Table of the fuel reported aggregated to the FERC1 plant-level.
        plant_parts_eia: The EIA plant parts list.
    """
    inputs = InputManager(plants_all_ferc1, fbp_ferc1, plant_parts_eia)
    # compile/cache inputs upfront. Hopefully we can catch any errors in inputs early.
    inputs.execute()
    features_all = Features(feature_type="all", inputs=inputs).get_features(
        clobber=False
    )
    features_train = Features(feature_type="training", inputs=inputs).get_features(
        clobber=False
    )
    match_df = run_model(
        features_train=features_train,
        features_all=features_all,
        train_df=inputs.train_df,
    )
    # choose one EIA match for each FERC record
    best_match_df = find_best_matches(match_df).pipe(
        overwrite_bad_predictions, inputs.train_df
    )
    # join EIA and FERC columns back on
    connects_ferc1_eia = prettyify_best_matches(
        best_match_df,
        train_df=inputs.get_train_df(),
        plant_parts_eia_true=inputs.get_plant_parts_eia_true(),
        plants_ferc1=inputs.get_plants_ferc1(),
    ).pipe(
        add_null_overrides
    )  # Override specified values with NA record_id_eia

    return connects_ferc1_eia


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
        self.plant_parts_eia = plant_parts_eia
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
        self.train_index = None
        self.train_ferc1 = None
        self.train_eia = None

    def get_train_index(self) -> pd.MultiIndex:
        """Get the index for the training data."""
        self.train_index = self.get_train_df().index
        return self.train_index

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

        This method grabs two tables (``plants_all_ferc1`` and ``fuel_by_plant_ferc1`` -
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
                        x.plant_id_pudl.map(str) + "_" + x.report_year.map(str)
                    ),
                    plant_id_report_year_util_id=lambda x: (
                        x.plant_id_report_year + "_" + x.utility_id_pudl.map(str)
                    ),
                    fuel_cost_per_mmbtu=lambda x: (x.fuel_cost / x.fuel_mmbtu),
                    heat_rate_mmbtu_mwh=lambda x: (x.fuel_mmbtu / x.net_generation_mwh),
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
        self.train_index = self.get_train_index()

        # generate the list of the records in the EIA and FERC records that
        # exist in the training data
        self.train_eia = self.get_train_eia(clobber=clobber)
        self.train_ferc1 = self.get_train_ferc1(clobber=clobber)
        return


class Features:
    """Generate feature vectors for connecting FERC and EIA."""

    def __init__(self, feature_type: Literal["training", "all"], inputs: InputManager):
        """Initialize feature generator.

        Args:
            feature_type: Type of features to compile. Either 'training' or 'all'.
            inputs: Instance of :class:`InputManager`.
        """
        self.inputs: InputManager = inputs
        self.features_df: pd.DataFrame | None = None

        if feature_type not in ["all", "training"]:
            raise ValueError(
                f"feature_type {feature_type} not allowable. Must be either "
                "'all' or 'training'"
            )
        self.feature_type = feature_type
        # the input_dict is going to help in standardizing how we generate
        # features. Based on the feature_type (keys), the latter methods will
        # know which dataframes to use as inputs for ``make_features()``
        self.input_dict = {
            "all": {
                "ferc1_df": self.inputs.get_plants_ferc1,
                "eia_df": self.inputs.get_plant_parts_eia_true,
            },
            "training": {
                "ferc1_df": self.inputs.get_train_ferc1,
                "eia_df": self.inputs.get_train_eia,
            },
        }

    def make_features(
        self, ferc1_df: pd.DataFrame, eia_df: pd.DataFrame, block_col: str | None = None
    ) -> pd.DataFrame:
        """Generate comparison features based on defined features.

        The recordlinkage package helps us create feature vectors. For each column that
        we have in both datasets, this method generates a column of feature vecotrs,
        which contain values between 0 and 1 that are measures of the similarity between
        each datapoint the two datasets (1 meaning the two datapoints were exactly the
        same and 0 meaning they were not similar at all).

        For more details see recordlinkage's documentaion:
        https://recordlinkage.readthedocs.io/en/latest/ref-compare.html

        Args:
            ferc1_df: Either training or all records from ferc plants table
                (via :meth:`InputManager.get_train_ferc1` or :meth:`InputManager.get_plants_ferc1`).
            eia_df: Either training or all records from the
                EIA plant-parts (:meth:`InputManager.get_train_eia` or
                `plant_parts_eia_true`).
            block_col:  If you want to restrict possible matches
                between ferc_df and eia_df based on a particular column,
                block_col is the column name of blocking column. Default is
                None. If None, this method will generate features between all
                possible matches.

        Returns:
            a dataframe of feature vectors between FERC and EIA.
        """
        compare_cl = rl.Compare(
            features=[
                String(
                    "plant_name_ferc1",
                    "plant_name_ppe",
                    label="plant_name",
                    method="jarowinkler",
                ),
                Numeric(
                    "net_generation_mwh",
                    "net_generation_mwh",
                    label="net_generation_mwh",
                    method="exp",
                    scale=1000,
                ),
                Numeric(
                    "capacity_mw",
                    "capacity_mw",
                    label="capacity_mw",
                    method="exp",
                    scale=10,
                ),
                Numeric(
                    "total_fuel_cost",
                    "total_fuel_cost",
                    label="total_fuel_cost",
                    method="exp",
                    offset=2500,
                    scale=10000,
                    missing_value=0.5,
                ),
                Numeric(
                    "total_mmbtu",
                    "total_mmbtu",
                    label="total_mmbtu",
                    method="exp",
                    offset=1,
                    scale=100,
                    missing_value=0.5,
                ),
                Numeric("capacity_factor", "capacity_factor", label="capacity_factor"),
                Numeric(
                    "fuel_cost_per_mmbtu",
                    "fuel_cost_per_mmbtu",
                    label="fuel_cost_per_mmbtu",
                ),
                Numeric(
                    "heat_rate_mmbtu_mwh",
                    "heat_rate_mmbtu_mwh",
                    label="heat_rate_mmbtu_mwh",
                ),
                Exact(
                    "fuel_type_code_pudl",
                    "fuel_type_code_pudl",
                    label="fuel_type_code_pudl",
                ),
                Numeric(
                    "installation_year", "installation_year", label="installation_year"
                ),
                # Exact('utility_id_pudl', 'utility_id_pudl',
                #      label='utility_id_pudl'),
            ]
        )

        # generate the index of all candidate features
        indexer = rl.Index()
        indexer.block(block_col)
        feature_index = indexer.index(ferc1_df, eia_df)

        features = compare_cl.compute(feature_index, ferc1_df, eia_df)
        return features

    def get_features(self, clobber=False):
        """Get the feature vectors for the training matches."""
        # generate feature matrixes for known/training data
        if clobber or self.features_df is None:
            self.features_df = self.make_features(
                ferc1_df=self.input_dict[self.feature_type]["ferc1_df"](),
                eia_df=self.input_dict[self.feature_type]["eia_df"](),
                block_col="plant_id_report_year_util_id",
            )
            logger.info(
                f"Generated {len(self.features_df)} {self.feature_type} "
                "candidate features."
            )
        return self.features_df


def run_model(
    features_train: pd.DataFrame, features_all: pd.DataFrame, train_df: pd.DataFrame
) -> pd.DataFrame:
    """Train Logistic Regression model using GridSearch cross validation.

    Search over the parameter grid for the best fit parameters for the
    Logistic Regression estimator on the training data. Predict matches
    on all the input features.

    Args:
        features_train: Dataframe of the feature vectors for the training data.
        features_all: Dataframe of the feature vectors for all the input data.
        train_df: Dataframe of the training data.

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
    x_train = features_train.to_numpy()
    y_train = np.where(
        features_train.merge(
            train_df,
            how="left",
            left_index=True,
            right_index=True,
            indicator=True,
        )["_merge"]
        == "both",
        1,
        0,
    )
    lrc = LogisticRegression()
    clf = GridSearchCV(estimator=lrc, param_grid=param_grid, verbose=True, n_jobs=-1)
    clf.fit(X=x_train, y=y_train)
    y_pred = clf.predict(x_train)
    precision, recall, f_score, _ = precision_recall_fscore_support(
        y_train, y_pred, average="binary"
    )
    accuracy = clf.best_score_
    logger.info(
        "Scores from the best model:\n"
        f"    Accuracy:  {accuracy:.02}\n"
        f"    F-Score:   {f_score:.02}\n"
        f"    Precision: {precision:.02}\n"
        f"    Recall:    {recall:.02}\n"
    )
    preds = clf.predict(features_all.to_numpy())
    probs = clf.predict_proba(features_all.to_numpy())
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


def overwrite_bad_predictions(match_df, train_df):
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
            for year in pd.date_range(start=start_date, end=end_date, freq="AS").year
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
    """Get and prepare the training connections.

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
    train_df = (
        # we want to ensure that the records are associated with a
        # "true granularity" - which is a way we filter out whether or
        # not each record in the EIA plant-parts is actually a
        # new/unique collection of plant parts
        # once the true_gran is dealt with, we also need to convert the
        # records which are ownership dupes to reflect their "total"
        # ownership counterparts
        pd.read_csv(
            importlib.resources.path("pudl.package_data.glue", "ferc1_eia_train.csv"),
        )
        .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
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
        .set_index(  # recordlinkage and sklearn wants MultiIndexs to do the stuff
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
    # if utility_id_pudl is not in the `PPE_COLS`,  we need to include it
    ppe_cols_to_grab = pudl.analysis.plant_parts_eia.PPE_COLS + [
        "plant_id_pudl",
        "total_fuel_cost",
        "fuel_cost_per_mmbtu",
        "net_generation_mwh",
        "capacity_mw",
        "capacity_factor",
        "total_mmbtu",
        "heat_rate_mmbtu_mwh",
        "fuel_type_code_pudl",
        "installation_year",
        "plant_part_id_eia",
    ]
    connects_ferc1_eia = (
        # first merge in the EIA plant-parts
        pd.merge(
            matches_best[["record_id_ferc1", "record_id_eia", "match_type"]],
            # we only want the identifying columns from the PPE
            plant_parts_eia_true.reset_index()[ppe_cols_to_grab],
            how="left",
            on=["record_id_eia"],
            validate="m:1",  # multiple FERC records can have the same EIA match
        )
        # this is necessary in instances where the overrides don't have a record_id_eia
        # i.e., they override to NO MATCH. These get merged in without a report_year,
        # so we need to create one for them from the record_id.
        .assign(
            report_year=lambda x: (
                x.record_id_ferc1.str.extract(r"(\d{4})")[0]
                .astype("float")
                .astype("Int64")
            )
        )
        # then merge in the FERC data we want the backbone of this table to be
        # the plant records so we have all possible FERC plant records, even
        # the unmapped ones
        .merge(
            plants_ferc1,
            how="outer",
            on=["record_id_ferc1", "report_year", "plant_id_pudl", "utility_id_pudl"],
            suffixes=("_eia", "_ferc1"),
            validate="1:1",
            indicator=True,
        ).assign(
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
        else:
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
    """Check how consistent matches are across time.

    Args:
        connects_ferc1_eia: Matches of FERC1 to EIA.
        train_df: training data.
        match_set: either ``all`` - to check all of the matches - or ``overrides`` - to
            check just the overrides. Default is'``all``. The overrides are less
            consistent than all of the data, so this argument changes the consistency
            threshold for this check.
    """
    # these are the default
    consistency = 0.75
    consistency_one_cap_ferc = 0.85
    mask = connects_ferc1_eia.record_id_eia.notnull()

    if match_set == "overrides":
        consistency = 0.39
        consistency_one_cap_ferc = 0.75
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
    consist = len(count[count.plant_part_id_eia == 1]) / len(count)
    logger.info(
        f"Matches with consistency across years of {match_set} matches is "
        f"{consist:.1%}"
    )
    if consist < consistency:
        raise AssertionError(
            f"Consistency of {match_set} matches across years dipped below "
            f"{consistency:.1%} to {consist:.1%}"
        )
    consist_one_cap_ferc = (
        len(count)
        - len(count[(count.plant_part_id_eia > 1) & (count.capacity_mw_ferc1 == 1)])
    ) / len(count)
    logger.info(
        "Matches with completely consistent FERC capacity have a consistency "
        f"of {consist_one_cap_ferc:.1%}"
    )
    if consist_one_cap_ferc < consistency_one_cap_ferc:
        raise AssertionError(
            "Consistency of matches with consistent FERC capcity dipped below "
            f"{consistency_one_cap_ferc:.1%} to {consist_one_cap_ferc:.1%}"
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
        importlib.resources.open_text("pudl.package_data.glue", "ferc1_eia_null.csv")
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
    eia_cols_to_null = [
        "plant_name_new",
        "plant_part",
        "ownership_record_type",
        "generator_id",
        "unit_code_pudl",
        "prime_mover_code",
        "energy_source_code_1",
        "technology_description",
        "true_gran",
        "appro_part_label",
        "record_count",
        "fraction_owned",
        "ownership_dupe",
        "operational_status",
        "operational_status_pudl",
    ] + [x for x in connects_ferc1_eia.columns if x.endswith("eia")]
    # Make all EIA values NA for record_id_ferc1 values in the Null overrides list and
    # make the match_type column say "overriden"
    connects_ferc1_eia.loc[
        connects_ferc1_eia["record_id_ferc1"].isin(null_overrides.record_id_ferc1),
        eia_cols_to_null + ["match_type"],
    ] = [np.nan] * len(eia_cols_to_null) + ["overridden"]

    return connects_ferc1_eia


#######################################################################################
# Create Training Data / Override Tools
#######################################################################################
# """
# Create output spreadsheets used for overriding and checking FERC-EIA record matches.

# The connect_ferc1_to_eia.py module uses machine learning to link records from FERC
# with records from EIA. While this process is waaay better more efficient and logical
# than a human, it requires a set of hand-compiled training data in order to do it's job.
# On top of that, it's not always capable of mapping some of the trickier records that
# humans might be able to make sense of based on past or surrounding records.

# This module creates an output spreadsheet, based on a certain utility, that makes the
# matching and machine-matched human validation process much easier. It also contains
# functions that will read those new/updated/validated matches from the spreadsheet,
# validate them, and incorporate them into the existing training data.

# """

RENAME_COLS_FERC_EIA: dict = {
    "new_1": "verified",
    "new_2": "used_match_record",
    "new_3": "signature_1",
    "new_4": "signature_2",
    "new_5": "notes",
    "new_6": "record_id_override_1",
    "new_7": "record_id_override_2",
    "new_8": "record_id_override_3",
    "new_9": "best_match",
    "record_id_ferc1": "record_id_ferc1",
    "record_id_eia": "record_id_eia",
    "true_gran": "true_gran",
    "report_year": "report_year",
    "match_type": "match_type",
    "plant_part": "plant_part",
    "ownership_record_type": "ownership_record_type",
    "utility_id_eia": "utility_id_eia",
    "utility_id_pudl_ferc1": "utility_id_pudl",
    "utility_name_ferc1": "utility_name_ferc1",
    "utility_name_eia": "utility_name_eia",
    "plant_id_pudl_ferc1": "plant_id_pudl",
    "unit_id_pudl": "unit_id_pudl",
    "generator_id": "generator_id",
    "plant_name_ferc1": "plant_name_ferc1",
    "plant_name_ppe": "plant_name_eia",
    "fuel_type_code_pudl_ferc1": "fuel_type_code_pudl_ferc1",
    "fuel_type_code_pudl_eia": "fuel_type_code_pudl_eia",
    "new_10": "fuel_type_code_pudl_diff",
    "net_generation_mwh_ferc1": "net_generation_mwh_ferc1",
    "net_generation_mwh_eia": "net_generation_mwh_eia",
    "new_11": "net_generation_mwh_pct_diff",
    "capacity_mw_ferc1": "capacity_mw_ferc1",
    "capacity_mw_eia": "capacity_mw_eia",
    "new_12": "capacity_mw_pct_diff",
    "capacity_factor_ferc1": "capacity_factor_ferc1",
    "capacity_factor_eia": "capacity_factor_eia",
    "new_13": "capacity_factor_pct_diff",
    "total_fuel_cost_ferc1": "total_fuel_cost_ferc1",
    "total_fuel_cost_eia": "total_fuel_cost_eia",
    "new_14": "total_fuel_cost_pct_diff",
    "total_mmbtu_ferc1": "total_mmbtu_ferc1",
    "total_mmbtu_eia": "total_mmbtu_eia",
    "new_15": "total_mmbtu_pct_diff",
    "fuel_cost_per_mmbtu_ferc1": "fuel_cost_per_mmbtu_ferc1",
    "fuel_cost_per_mmbtu_eia": "fuel_cost_per_mmbtu_eia",
    "new_16": "fuel_cost_per_mmbtu_pct_diff",
    "installation_year_ferc1": "installation_year_ferc1",
    "installation_year_eia": "installation_year_eia",
    "new_17": "installation_year_diff",
}

RELEVANT_COLS_PPL: list = [
    "record_id_eia",
    "report_year",
    "utility_id_pudl",
    "utility_id_eia",
    "utility_name_eia",  # I add this in from the utils_eia860() table
    "operational_status_pudl",
    "true_gran",
    "plant_part",
    "ownership_dupe",
    "fraction_owned",
    "plant_id_eia",
    "plant_id_pudl",
    "plant_name_ppe",
    "generator_id",
    "capacity_mw",
    "capacity_factor",
    "net_generation_mwh",
    "installation_year",
    "fuel_type_code_pudl",
    "total_fuel_cost",
    "total_mmbtu",
    "fuel_cost_per_mmbtu",
    "heat_rate_mmbtu_mwh",
]

# --------------------------------------------------------------------------------------
# Generate Override Tools
# --------------------------------------------------------------------------------------


def _pct_diff(df, col) -> pd.DataFrame:
    """Calculate percent difference between EIA and FERC versions of a column."""
    # Fed in the _pct_diff column so make sure it is neutral for this analysis
    col = col.replace("_pct_diff", "")
    # Fill in the _pct_diff column with the actual percent difference value
    df.loc[
        (df[f"{col}_eia"] > 0) & (df[f"{col}_ferc1"] > 0), f"{col}_pct_diff"
    ] = round(((df[f"{col}_ferc1"] - df[f"{col}_eia"]) / df[f"{col}_ferc1"] * 100), 2)

    return df


def _is_best_match(
    df, cap_pct_diff=6, net_gen_pct_diff=6, inst_year_diff=3
) -> pd.DataFrame:
    """Fill the best_match column with strings to show cap, net_gen, inst_year match.

    The process of manually checking all of the FERC-EIA matches made by the machine
    learning algorithm is tedius. This function makes it easier to speed through the
    obviously good matches and pay more attention to those that are more questionable.

    By default, a "best match" is comprised of a FERC-EIA match with a capacity percent
    difference of less than 6%, a net generation percent difference of less than 6%,
    and an installation year difference of less than 3 years.
    """
    best_cap = df.capacity_mw_pct_diff < cap_pct_diff
    best_net_gen = df.net_generation_mwh_pct_diff < net_gen_pct_diff
    best_inst_year = df.installation_year_diff < inst_year_diff

    df.loc[:, "best_match"] = df.best_match.fillna("")
    df.loc[best_cap, "best_match"] = "cap"
    df.loc[best_net_gen, "best_match"] = df.best_match + "_net-gen"
    df.loc[best_inst_year, "best_match"] = df.best_match + "_inst_year"
    df.loc[:, "best_match"] = df.best_match.replace(r"^_", "", regex=True)

    return df


def _prep_ferc1_eia(ferc1_eia, pudl_out) -> pd.DataFrame:
    """Prep FERC-EIA for use in override output sheet pre-utility subgroups."""
    logger.debug("Prepping FERC-EIA table")
    # Only want to keep the plant_name_ppe field which replaces plant_name_eia
    ferc1_eia_prep = ferc1_eia.copy().drop(columns="plant_name_eia")

    # Add utility_name_eia - this must happen before renaming the cols or else there
    # will be duplicate utility_name_eia columns.
    utils = pudl_out.utils_eia860().copy()
    utils.loc[:, "report_year"] = utils.report_date.dt.year
    ferc1_eia_prep = pd.merge(
        ferc1_eia_prep,
        utils[["utility_id_eia", "utility_name_eia", "report_year"]],
        on=["utility_id_eia", "report_year"],
        how="left",
        validate="m:1",
    )

    # Add the new columns to the df
    for new_col in [x for x in RENAME_COLS_FERC_EIA.keys() if "new_" in x]:
        ferc1_eia_prep.loc[:, new_col] = pd.NA

    # Rename the columns, and remove unwanted columns from ferc-eia table
    ferc1_eia_prep = ferc1_eia_prep.rename(columns=RENAME_COLS_FERC_EIA)[
        list(RENAME_COLS_FERC_EIA.values())
    ]

    # Add in pct diff values
    for pct_diff_col in [x for x in RENAME_COLS_FERC_EIA.values() if "_pct_diff" in x]:
        ferc1_eia_prep = _pct_diff(ferc1_eia_prep, pct_diff_col)

    # Add in fuel_type_code_pudl diff (qualitative bool)
    ferc1_eia_prep.loc[
        ferc1_eia_prep.fuel_type_code_pudl_eia.notna()
        & ferc1_eia_prep.fuel_type_code_pudl_ferc1.notna(),
        "fuel_type_code_pudl_diff",
    ] = ferc1_eia_prep.fuel_type_code_pudl_eia == (
        ferc1_eia_prep.fuel_type_code_pudl_ferc1
    )

    # Add in installation_year diff (diff vs. pct_diff)
    ferc1_eia_prep.loc[
        :, "installation_year_ferc1"
    ] = ferc1_eia_prep.installation_year_ferc1.astype("Int64")

    ferc1_eia_prep.loc[
        ferc1_eia_prep.installation_year_eia.notna()
        & ferc1_eia_prep.installation_year_ferc1.notna(),
        "installation_year_diff",
    ] = (
        ferc1_eia_prep.installation_year_eia - ferc1_eia_prep.installation_year_ferc1
    )

    # Add best match col
    ferc1_eia_prep = _is_best_match(ferc1_eia_prep)

    return ferc1_eia_prep


def _prep_ppl(ppl, pudl_out) -> pd.DataFrame:
    """Prep PPL table for use in override output sheet pre-utility subgroups."""
    logger.debug("Prepping Plant Parts Table")

    # Add utilty name eia and only take relevant columns
    ppl_out = (
        ppl.reset_index()
        .merge(
            pudl_out.utils_eia860()[
                ["utility_id_eia", "utility_name_eia", "report_date"]
            ].copy(),
            on=["utility_id_eia", "report_date"],
            how="left",
            validate="m:1",
        )[RELEVANT_COLS_PPL]
        .copy()
    )

    return ppl_out


def _prep_deprish(deprish, pudl_out) -> pd.DataFrame:
    """Prep depreciation data for use in override output sheet pre-utility subgroups."""
    logger.debug("Prepping Deprish Data")

    # Get utility_id_eia from EIA
    util_df = pudl_out.utils_eia860()[
        ["utility_id_pudl", "utility_id_eia"]
    ].drop_duplicates()
    deprish.loc[:, "report_year"] = deprish.report_date.dt.year.astype("Int64")
    deprish = deprish.merge(util_df, on=["utility_id_pudl"], how="left")

    return deprish


def _generate_input_dfs(pudl_out, rmi_out) -> dict:
    """Load ferc_eia, ppl, and deprish tables into a dictionary.

    Loading all of these tables once is much faster than loading then repreatedly for
    every utility/year iteration. These tables will be segmented by utility and year
    in _get_util_year_subsets() and loaded as seperate tabs in a spreadsheet in
    _output_override_sheet().

    Returns:
        dict: A dictionary where keys are string names for ferc_eia, ppl, and deprish
            tables and values are the actual tables in full.
    """
    logger.debug("Generating inputs")
    inputs_dict = {
        "ferc_eia": rmi_out.ferc1_to_eia().pipe(_prep_ferc1_eia, pudl_out),
        "ppl": rmi_out.plant_parts_eia().pipe(_prep_ppl, pudl_out),
        "deprish": rmi_out.deprish().pipe(_prep_deprish, pudl_out),
    }

    return inputs_dict


def _get_util_year_subsets(inputs_dict, util_id_eia_list, years) -> dict:
    """Get utility and year subsets for each of the input dfs.

    After generating the dictionary with all of the inputs tables loaded, we'll want to
    create subsets of each of those tables based on the utility and year inputs we're
    given. This function takes the input dict generated in _generate_input_dfs() and
    outputs an updated version with df values pertaining to the utilities in
    util_id_eia_list and years in years.

    Args:
        inputs_dict (dict): The output of running _generation_input_dfs()
        util_id_eia_list (list): A list of the utility_id_eia values you want to
            include in a single spreadsheet output. Generally this is a list of the
            subsidiaries that pertain to a single parent company.
        years (list): A list of the years you'd like to add to the override sheets.

    Returns:
        dict: A subset of the inputs_dict that contains versions of the value dfs that
            pertain only to the utilites and years specified in util_id_eia_list and
            years.
    """
    util_year_subset_dict = {}
    for df_name, df in inputs_dict.items():
        logger.debug(f"Getting utility-year subset for {df_name}")
        subset_df = df[
            df["report_year"].isin(years) & df["utility_id_eia"].isin(util_id_eia_list)
        ].copy()
        # Make sure dfs aren't too big...
        if len(subset_df) > 500000:
            raise AssertionError(
                "Your subset is more than 500,000 rows...this \
                is going to make excel reaaalllllyyy slow. Try entering a smaller utility \
                or year subset"
            )

        if df_name == "ferc_eia":
            # Add column with excel formula to check if the override record id is the
            # same as the AI assigend id. Doing this here instead of prep_ferc_eia
            # because it is based on row index number which is changes when you take a
            # subset of the data.
            subset_df = subset_df.reset_index(drop=True)
            override_col_index = subset_df.columns.get_loc("record_id_override_1")
            record_link_col_index = subset_df.columns.get_loc("record_id_eia")
            subset_df["used_match_record"] = (
                "="
                + chr(ord("a") + override_col_index)
                + (subset_df.index + 2).astype(str)
                + "="
                + chr(ord("a") + record_link_col_index)
                + (subset_df.index + 2).astype(str)
            )

        util_year_subset_dict[f"{df_name}_util_year_subset"] = subset_df

    return util_year_subset_dict


def _output_override_spreadsheet(util_year_subset_dict, util_name) -> None:
    """Output spreadsheet with tabs for ferc-eia, ppl, deprish for one utility.

    Args:
        util_year_subset_dict (dict): The output from _get_util_year_subsets()
        util_name (str): A string indicating the name of the utility that you are
            creating an override sheet for. The string will be used as the suffix for
            the name of the excel file. Ex: for util_name = "BHE", the file name will be
            BHE_fix_FERC-EIA_overrides.xlsx.
    """
    # Enable unique file names and put all files in directory called overrides
    new_output_path = f"pudl/src/pudl/package_data/glue/overrides/{util_name}_fix_FERC-EIA_overrides.xlsx"
    # Output file to a folder called overrides
    logger.info("Outputing table subsets to tabs\n")
    writer = pd.ExcelWriter(new_output_path, engine="xlsxwriter")
    for df_name, df in util_year_subset_dict.items():
        df.to_excel(writer, sheet_name=df_name, index=False)
    writer.save()


def generate_all_override_spreadsheets(pudl_out, rmi_out, util_dict, years) -> None:
    """Output override spreadsheets for all specified utilities and years.

    These manual override files will be output to a folder called "overrides" in the
    output directory.

    Args:
        pudl_out (PudlTabl): the pudl_out object generated in a notebook and passed in.
        rmi_out (Output): the rmi_out object generated in a notebook and passed in.
        util_dict (dict): A dictionary with keys that are the names of utility
            parent companies and values that are lists of subsidiary utility_id_eia
            values. EIA values are used instead of PUDL in this case because PUDL values
            are subject to change.
        years (list): A list of the years you'd like to add to the override sheets.
    """
    # Generate full input tables
    inputs_dict = _generate_input_dfs(pudl_out, rmi_out)

    # Make sure overrides dir exists
    if not os.path.isdir("pudl/src/pudl/package_data/glue/overrides"):
        os.mkdir("pudl/src/pudl/package_data/glue/overrides")

    # For each utility, make an override sheet with the correct input table slices
    for util_name, util_id_eia_list in util_dict.items():
        logger.info(f"Developing outputs for {util_name}")
        util_year_subset_dict = _get_util_year_subsets(
            inputs_dict, util_id_eia_list, years
        )
        _output_override_spreadsheet(util_year_subset_dict, util_name)


# --------------------------------------------------------------------------------------
# Upload Changes to Training Data
# --------------------------------------------------------------------------------------


def _check_id_consistency(
    id_col: Literal["record_id_eia_override_1", "record_id_ferc1"],
    df,
    actual_ids,
    error_message,
) -> None:
    """Check for rogue FERC or EIA ids that don't exist.

    Args:
        id_col (str): The name of either the ferc record id column: record_id_ferc1 or
            the eia record override column: record_id_eia_override_1.
        df (pd.DataFrame): A dataframe of intended overrides.
        actual_ids (list): A list of the ferc or eia ids that are valid and come from
            either the ppl or official ferc-eia record linkage.
        error_message (str): A short string to indicate the type of error you're
            checking for. This could be looking for values that aren't in the official
            list or values that are already in the training data.
    """
    logger.debug(f"Checking {id_col} consistency for {error_message}")

    assert (
        len(bad_ids := df[~df[id_col].isin(actual_ids)][id_col].to_list()) == 0
    ), f"{id_col} {error_message}: {bad_ids}"


def already_in_training(training_data, validated_connections):
    """Blah."""
    logger.info("checking if already in validation or not")
    training_vals = training_data.record_id_eia.dropna().unique().tolist()
    val_vals = validated_connections.record_id_eia_override_1.dropna().unique().tolist()

    not_in_training = [x for x in val_vals if x not in training_vals]
    print(f"Total records: {len(val_vals)}")
    print(f"Records not in training data: {len(not_in_training)}")
    print(not_in_training)
    print("")


def validate_override_fixes(
    validated_connections,
    utils_eia860,
    ppl,
    ferc1_eia,
    training_data,
    expect_override_overrides=False,
    allow_mismatched_utilities=True,
) -> pd.DataFrame:
    """Process the verified and/or fixed matches and look for human error.

    Args:
        validated_connections (pd.DataFrame): A dataframe in the add_to_training
            directory that is ready to be added to be validated and subsumed into the
            training data.
        utils_eia860 (pd.DataFrame): A dataframe resulting from the
            pudl_out.utils_eia860() function.
        ferc1_eia (pd.DataFrame): The current FERC-EIA table
        expect_override_overrides (boolean): Whether you expect the tables to have
            overridden matches already in the training data.

    Raises:
        AssertionError: If there are EIA override id records that aren't in the original
            FERC-EIA connection.
        AssertionError: If there are FERC record ids that aren't in the original
            FERC-EIA connection.
        AssertionError: If there are EIA override ids that are duplicated throughout the
            override document.
        AssertionError: If the utility id in the EIA override id doesn't match the pudl
            id cooresponding with the FERC record.
        AssertionError: If there are EIA override id records that don't correspond to
            the correct report year.
        AssertionError: If you didn't expect to override overrides but the new training
            data implies an override to the existing training data.

    Returns:
        pd.DataFrame: The validated FERC-EIA dataframe you're trying to add to the
            training data.
    """
    logger.info("Validating overrides")
    # When there are NA values in the verified column in the excel doc, it seems that
    # the TRUE values become 1.0 and the column becomes a type float. Let's replace
    # those here and make it a boolean.
    validated_connections["verified"] = validated_connections["verified"].replace(
        {1: True, np.nan: False}
    )
    # Make sure the verified column doesn't contain non-boolean outliers. This will fail
    # if there are bad values.
    validated_connections.astype({"verified": pd.BooleanDtype()})

    # From validated records, get only records with an override
    only_overrides = (
        validated_connections[validated_connections["verified"]]
        .dropna(subset=["record_id_eia_override_1"])
        .reset_index()
        .copy()
    )

    # Make sure that the override EIA ids actually match those in the original FERC-EIA
    # record linkage.
    actual_eia_ids = ppl.record_id_eia.unique()
    _check_id_consistency(
        "record_id_eia_override_1",
        only_overrides,
        actual_eia_ids,
        "values that don't exist",
    )

    # It's unlikely that this changed, but check FERC id too just in case!
    actual_ferc_ids = ferc1_eia.record_id_ferc1.unique()
    _check_id_consistency(
        "record_id_ferc1", only_overrides, actual_ferc_ids, "values that don't exist"
    )

    # Make sure there are no duplicate EIA id overrides
    logger.debug("Checking for duplicate override ids")
    assert (
        len(
            override_dups := only_overrides[
                only_overrides["record_id_eia_override_1"].duplicated(keep=False)
            ]
        )
        == 0
    ), f"Found record_id_eia_override_1 duplicates: \
    {override_dups.record_id_eia_override_1.unique()}"

    if not allow_mismatched_utilities:
        # Make sure the EIA utility id from the override matches the PUDL id from the FERC
        # record. Start by mapping utility_id_eia from PPL onto each
        # record_id_eia_override_1.
        logger.debug("Checking for mismatched utility ids")
        only_overrides = only_overrides.merge(
            ppl[["record_id_eia", "utility_id_eia"]].drop_duplicates(),
            left_on="record_id_eia_override_1",
            right_on="record_id_eia",
            how="left",
            suffixes=("", "_ppl"),
        )
        # Now merge the utility_id_pudl from EIA in so that you can compare it with the
        # utility_id_pudl from FERC that's already in the overrides
        only_overrides = only_overrides.merge(
            utils_eia860[["utility_id_eia", "utility_id_pudl"]].drop_duplicates(),
            left_on="utility_id_eia_ppl",
            right_on="utility_id_eia",
            how="left",
            suffixes=("", "_utils"),
        )
        # Now we can actually compare the two columns
        if (
            len(
                bad_utils := only_overrides["utility_id_pudl"].compare(
                    only_overrides["utility_id_pudl_utils"]
                )
            )
            > 0
        ):
            raise AssertionError(f"Found mismatched utilities: {bad_utils}")

    # Make sure the year in the EIA id overrides match the year in the report_year
    # column.
    logger.debug("Checking that year in override id matches report year")
    only_overrides = only_overrides.merge(
        ppl[["record_id_eia", "report_year"]].drop_duplicates(),
        left_on="record_id_eia_override_1",
        right_on="record_id_eia",
        how="left",
        suffixes=("", "_ppl"),
    )
    if (
        len(
            bad_eia_year := only_overrides["report_year"].compare(
                only_overrides["report_year_ppl"]
            )
        )
        > 0
    ):
        raise AssertionError(
            f"Found record_id_eia_override_1 values that don't correspond to the right \
            report year:\
            {[only_overrides.iloc[x].record_id_eia_override_1 for x in bad_eia_year.index]}"
        )

    # If you don't expect to override values that have already been overridden, make
    # sure the ids you fixed aren't already in the training data.
    if not expect_override_overrides:
        existing_training_eia_ids = training_data.record_id_eia.dropna().unique()
        _check_id_consistency(
            "record_id_eia_override_1",
            only_overrides,
            existing_training_eia_ids,
            "already in training",
        )
        existing_training_ferc_ids = training_data.record_id_ferc1.dropna().unique()
        _check_id_consistency(
            "record_id_ferc1",
            only_overrides,
            existing_training_ferc_ids,
            "already in training",
        )

    # Only return the results that have been verified
    verified_connections = validated_connections[
        validated_connections["verified"]
    ].copy()

    return verified_connections


def _add_to_training(new_overrides, current_training_data_path) -> None:
    """Add the new overrides to the old override sheet."""
    logger.info("Combining all new overrides with existing training data")
    current_training = pd.read_csv(current_training_data_path)
    new_training = (
        new_overrides[
            ["record_id_eia", "record_id_ferc1", "signature_1", "signature_2", "notes"]
        ]
        .copy()
        .drop_duplicates(subset=["record_id_eia", "record_id_ferc1"])
        # .set_index(["record_id_eia", "record_id_ferc1"])
    )
    logger.debug(f"Found {len(new_training)} new overrides")
    # Combine new and old training data
    training_data_out = current_training.append(new_training).drop_duplicates(
        subset=["record_id_eia", "record_id_ferc1"]
    )
    # Output combined training data
    training_data_out.to_csv(current_training_data_path, index=False)


def _add_to_null_overrides(null_matches, current_null_overrides_path) -> None:
    """Take record_id_ferc1 values verified to have no EIA match and add them to csv."""
    logger.info("Adding record_id_ferc1 values with no EIA match to null_overrides csv")
    # Get new null matches
    new_null_matches = null_matches[["record_id_ferc1"]].copy()
    logger.debug(f"Found {len(new_null_matches)} new null matches")
    # Get current null matches
    current_null_matches = pd.read_csv(current_null_overrides_path)
    # Combine new and current record_id_ferc1 values that have no EIA match
    out_null_matches = current_null_matches.append(new_null_matches).drop_duplicates()
    # Write the combined values out to the same location as before
    out_null_matches.to_csv(current_null_overrides_path, index=False)


def validate_and_add_to_training(
    utils_eia860,
    ppl,
    ferc1_eia,
    expect_override_overrides=False,
    allow_mismatched_utilities=True,
) -> None:
    """Validate, combine, and add overrides to the training data.

    Validating and combinging the records so you only have to loop through the files
    once. Runs the validate_override_fixes() function and add_to_training.

    Args:
        pudl_out (PudlTabl): the pudl_out object generated in a notebook and passed in.
        rmi_out (Output): the rmi_out object generated in a notebook and passed in.
        expect_override_overrides (bool): This value is explicitly assigned at the top
            of the notebook.

    Returns:
        pandas.DataFrame: A DataFrame with all of the new overrides combined.
    """
    path_to_new_training = "../../src/pudl/package_data/glue/add_to_ferc1_eia_training/"
    path_to_old_training = "../../src/pudl/package_data/glue/ferc1_eia_train.csv"
    old_training = pd.read_csv(path_to_old_training)
    path_to_null_overrides = "../../src/pudl/package_data/glue/ferc1_eia_null.csv"
    override_cols = [
        "record_id_eia",
        "record_id_ferc1",
        "signature_1",
        "signature_2",
        "notes",
    ]
    null_match_cols = ["record_id_ferc1"]
    all_overrides_list = []
    all_null_matches_list = []

    # Loop through all the files, validate, and combine them.
    all_files = os.listdir(path_to_new_training)
    excel_files = [file for file in all_files if file.endswith(".xlsx")]

    for file in excel_files:
        logger.info(f"Processing fixes in {file}")
        file_df = (
            pd.read_excel(path_to_new_training + file)
            .pipe(
                validate_override_fixes,
                utils_eia860,
                ppl,
                ferc1_eia,
                old_training,
                expect_override_overrides=expect_override_overrides,
                allow_mismatched_utilities=allow_mismatched_utilities,
            )
            .rename(
                columns={
                    "record_id_eia": "record_id_eia_old",
                    "record_id_eia_override_1": "record_id_eia",
                }
            )
        )
        # Get just the overrides and combine them to full list of overrides
        only_overrides = file_df[file_df["record_id_eia"].notna()][override_cols].copy()
        all_overrides_list.append(only_overrides)
        # Get just the null matches and combine them to full list of overrides
        only_null_matches = file_df[file_df["record_id_eia"].isna()][
            null_match_cols
        ].copy()
        all_null_matches_list.append(only_null_matches)

    # Combine all training data and null matches
    all_overrides_df = pd.concat(all_overrides_list)
    all_null_matches_df = pd.concat(all_null_matches_list)

    # Add the records to the training data and null overrides
    _add_to_training(all_overrides_df, path_to_old_training)
    _add_to_null_overrides(all_null_matches_df, path_to_null_overrides)
