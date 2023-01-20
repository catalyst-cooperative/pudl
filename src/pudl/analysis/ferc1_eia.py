"""
Connect FERC1 plants tables to EIA's plant-parts via record linkage.

FERC plant records are reported... kind of messily. In the same table there are
records that are reported as whole plants, generators, collections of prime
movers. So we have this heterogeneously reported collection of parts of plants
in FERC1.

EIA on the other hand is reported in a much cleaner way. The are generators
with ids and plants with ids reported in *seperate* tables. What a joy. In
`pudl.analysis.plant_parts_eia`, we've generated the EIA plant-parts. The EIA
plant-parts (often referred to as `plant_parts_eia` in this module) generated records
for various levels or granularies of plant parts.

For each of the FERC1 plant records we want to figure out which EIA
plant-parts record is the corresponding record. We do this with a record linkage/
scikitlearn machine learning model. The recordlinkage package helps us create
feature vectors (via `make_features`) for each candidate match between FERC
and EIA. Feature vectors are a number between 0 and 1 that indicates the
closeness for each value we want to compare.

We use the feature vectors of our known-to-be-connected training data to cross
validate and tune parameters to choose hyperparameters for scikitlearn models
(via `test_model_parameters`). We choose the "best" model based on the cross
validated results. This best scikitlearn model is then used to generate matches
with the full dataset (`fit_predict_lrc`). The model can return multiple
options for each FERC1 record, so we rank them and choose the best/winning
match (`calc_wins`). We then ensure those connections cointain our training
data (`override_winners_with_training_df`). These "best" results are the
connections we keep as the matches between FERC1 plant records and the EIA
plant-parts.
"""

import importlib.resources
import logging
import statistics
import warnings
from copy import deepcopy
from os import PathLike

import numpy as np
import pandas as pd
import recordlinkage as rl
import scipy
from recordlinkage.compare import Exact, Numeric, String  # , Date
from sklearn.model_selection import KFold  # , cross_val_score

import pudl
import pudl.helpers
from pudl.metadata.classes import DataSource

logger = logging.getLogger(__name__)
# Silence the recordlinkage logger, which is out of control
logging.getLogger("recordlinkage").setLevel(logging.ERROR)
IDX_STEAM = ["utility_id_ferc1", "plant_id_ferc1", "report_date"]
HYPER_PARAMS = {}


def execute(pudl_out: "pudl.output.pudltabl.PudlTabl", plant_parts_eia: pd.DataFrame):
    """Generate the connection between FERC1 plants and EIA plant-parts."""
    inputs = InputManager(
        importlib.resources.path("pudl.package_data.glue", "ferc1_eia_train.csv"),
        pudl_out,
        plant_parts_eia,
    )
    features_all = Features(feature_type="all", inputs=inputs).get_features(
        clobber=False
    )
    features_train = Features(feature_type="training", inputs=inputs).get_features(
        clobber=False
    )
    tuner = ModelTuner(features_train, inputs.get_train_index(), n_splits=10)

    matcher = MatchManager(best=tuner.get_best_fit_model(), inputs=inputs)
    matches_best = matcher.get_best_matches(features_train, features_all)
    connects_ferc1_eia = prettyify_best_matches(
        matches_best,
        train_df=inputs.train_df,
        plant_parts_true_df=inputs.plant_parts_true_df,
        plants_ferc1_df=inputs.plants_ferc1_df,
    )
    # add capex (this should be moved into pudl_out.plants_steam_ferc1)
    connects_ferc1_eia = calc_annual_capital_additions_ferc1(connects_ferc1_eia)
    # Override specified record_id_ferc1 values with NA record_id_eia
    connects_ferc1_eia = add_null_overrides(connects_ferc1_eia)

    return connects_ferc1_eia


class InputManager:
    """Class prepare inputs for linking FERC1 and EIA."""

    def __init__(
        self,
        file_path_training: PathLike,
        pudl_out: "pudl.output.pudltabl.PudlTabl",
        plant_parts_eia: pd.DataFrame,
    ):
        """
        Initialize inputs manager that gets inputs for linking FERC and EIA.

        Args:
            file_path_training (path-like): path to the CSV of training data.
                The training data needs to have at least two columns:
                record_id_eia record_id_ferc1.
            pudl_out (object): instance of `pudl.output.pudltabl.PudlTabl()`.
            plant_parts_eia (pandas.DataFrame)
        """
        self.file_path_training = file_path_training
        self.pudl_out = pudl_out  # remove if we pass in plants_all_ferc1 and fbp_ferc1

        self.start_date = self.pudl_out.start_date
        self.end_date = self.pudl_out.end_date
        self.plant_parts_eia = plant_parts_eia[
            ~plant_parts_eia.index.duplicated(keep="first")
        ]

        # generate empty versions of the inputs.. this let's this class check
        # whether or not the compiled inputs exist before compilnig
        self.plant_parts_true_df = None
        self.plants_ferc1_df = None
        self.train_df = None
        self.train_index = None
        self.plant_parts_train_df = None
        self.plants_ferc1_train_df = None

    def get_plant_parts_true(self, clobber: bool = False) -> pd.DataFrame:
        """Get the EIA plant-parts with only the unique granularities."""
        # We want only the records of the EIA plant-parts that are "true
        # granularies" and those which are not duplicates based on their
        # ownership  so the model doesn't get confused as to which option to
        # pick if there are many records with duplicate data
        if clobber or self.plant_parts_true_df is None:
            plant_parts_eia = self.plant_parts_eia.assign(
                plant_id_report_year_util_id=lambda x: x.plant_id_report_year
                + "_"
                + x.utility_id_pudl.map(str)
            ).astype({"installation_year": "float"})
            self.plant_parts_true_df = plant_parts_eia[
                (plant_parts_eia["true_gran"]) & (~plant_parts_eia["ownership_dupe"])
            ].copy()
        return self.plant_parts_true_df

    def prep_train_connections(self, clobber: bool = False) -> pd.DataFrame:
        """
        Get and prepare the training connections.

        We have stored training data, which consists of records with ids
        columns for both FERC and EIA. Those id columns serve as a connection
        between ferc1 plants and the EIA plant-parts. These connections
        indicate that a ferc1 plant records is reported at the same granularity
        as the connected EIA plant-parts record. These records to train a
        machine learning model.

        Returns:
            pandas.DataFrame: training connections. A dataframe with has a
            MultiIndex with record_id_eia and record_id_ferc1.
        """
        if clobber or self.train_df is None:
            mul_cols = [
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
                    self.file_path_training,
                )
                .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
                .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
                .merge(
                    self.plant_parts_eia.reset_index()[["record_id_eia"] + mul_cols],
                    how="left",
                    on=["record_id_eia"],
                    indicator=True,
                )
                # .assign(
                #     plant_part=lambda x: x["appro_part_label"],
                #     record_id_eia=lambda x: x["appro_record_id_eia"],
                # )
                # # .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
                # # .pipe(pudl.analysis.plant_parts_eia.reassign_id_ownership_dupes)
                # .fillna(
                #     value={
                #         "record_id_eia": pd.NA,
                #     }
                # )
                # # recordlinkage and sklearn wants MultiIndexs to do the stuff
                # .set_index(
                #     [
                #         "record_id_ferc1",
                #         "record_id_eia",
                #     ]
                # )
            )
            # not_in_ppf = train_df[train_df._merge == "left_only"]
            # # # if not not_in_ppf.empty:
            # if len(not_in_ppf) > 12:
            #     self.not_in_ppf = not_in_ppf
            #     raise AssertionError(
            #         "Not all training data is associated with EIA records.\n"
            #         "record_id_ferc1's of bad training data records are: "
            #         f"{list(not_in_ppf.reset_index().record_id_ferc1)}"
            #     )
            # dupe_ferc1 = train_df.reset_index()[
            #     train_df.reset_index()[["record_id_ferc1"]].duplicated(keep=False)
            # ].sort_index()
            # if not dupe_ferc1.empty:
            #     raise AssertionError(
            #         f"You have {len(dupe_ferc1)} duplicate ferc1 training "
            #         f"records: {dupe_ferc1.index}"
            #     )
            self.train_df = train_df  # .drop(columns=mul_cols + ["_merge"])
        return self.train_df

    def get_train_index(self) -> pd.Index:
        """Get the index for the training data."""
        self.train_index = self.prep_train_connections().index
        return self.train_index

    def get_all_ferc1(self, clobber: bool = False) -> pd.DataFrame:
        """
        Prepare FERC1 plants data for record linkage with EIA plant-parts.

        This method grabs two tables from `pudl_out` (`plants_all_ferc1`
        and `fuel_by_plant_ferc1`) and ensures that the columns the same as
        their EIA counterparts, because the output of this method will be used
        to link FERC and EIA.

        Returns:
            pandas.DataFrame: a cleaned table of FERC1 plants plant records
            with fuel cost data.

        """
        if clobber or self.plants_ferc1_df is None:
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
            self.plants_ferc1_df = (
                self.pudl_out.plants_all_ferc1()
                .merge(
                    self.pudl_out.fbp_ferc1()[fbp_cols_to_use],
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
        return self.plants_ferc1_df

    def get_train_records(self, dataset_df, dataset_id_col):
        """
        Generate a set of known connections from a dataset using training data.

        This method grabs only the records from the the datasets (EIA or FERC)
        that we have in our training data.

        Args:
            dataset_df (pandas.DataFrame): either FERC1 plants table (result of
                `get_all_ferc1()`) or EIA plant-parts (result of
                `get_plant_parts_true()`).
            dataset_id_col (string): either `record_id_eia` for
                plant_parts_true_df or `record_id_ferc1` for plants_ferc1_df.

        """
        known_df = (
            pd.merge(
                dataset_df,
                self.prep_train_connections().reset_index()[[dataset_id_col]],
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
    def get_train_eia(self, clobber=False):
        """Get the known training data from EIA."""
        if clobber or self.plant_parts_train_df is None:
            self.plant_parts_train_df = self.get_train_records(
                self.get_plant_parts_true(), dataset_id_col="record_id_eia"
            )
        return self.plant_parts_train_df

    def get_train_ferc1(self, clobber=False):
        """Get the known training data from FERC1."""
        if clobber or self.plants_ferc1_train_df is None:
            self.plants_ferc1_train_df = self.get_train_records(
                self.get_all_ferc1(), dataset_id_col="record_id_ferc1"
            )
        return self.plants_ferc1_train_df

    def execute(self, clobber=False):
        """Compile all the inputs."""
        # grab the main two data tables we are trying to connect
        self.plant_parts_true_df = self.get_plant_parts_true(clobber=clobber)
        self.plants_ferc1_df = self.get_all_ferc1(clobber=clobber)

        # we want both the df version and just the index; skl uses just the
        # index and we use the df in merges and such
        self.train_df = self.prep_train_connections(clobber=clobber)
        self.train_index = self.get_train_index()

        # generate the list of the records in the EIA and FERC records that
        # exist in the training data
        self.plant_parts_train_df = self.get_train_eia(clobber=clobber)
        self.plants_ferc1_train_df = self.get_train_ferc1(clobber=clobber)
        return


class Features:
    """Generate featrue vectors for connecting FERC and EIA."""

    def __init__(self, feature_type, inputs):
        """
        Initialize feature generator.

        Args:
            feature_type (string): either 'training' or 'all'. Type of features
                to compile.
            inputs (instance of class): instance of ``Inputs``

        """
        self.inputs = inputs
        self.features_df = None

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
                "ferc1_df": self.inputs.get_all_ferc1,
                "eia_df": self.inputs.get_plant_parts_true,
            },
            "training": {
                "ferc1_df": self.inputs.get_train_ferc1,
                "eia_df": self.inputs.get_train_eia,
            },
        }

    def make_features(self, ferc1_df, eia_df, block_col=None):
        """
        Generate comparison features based on defined features.

        The recordlinkage package helps us create feature vectors.
        For each column that we have in both datasets, this method generates
        a column of feature vecotrs, which contain values between 0 and 1 that
        are measures of the similarity between each datapoint the two datasets
        (1 meaning the two datapoints were exactly the same and 0 meaning they
        were not similar at all).

        For more details see recordlinkage's documentaion:
        https://recordlinkage.readthedocs.io/en/latest/ref-compare.html

        Args:
            ferc1_df (pandas.DataFrame): Either training or all records from
                ferc plants table (`plants_ferc1_train_df` or
                `plants_ferc1_df`).
            eia_df (pandas.DataFrame): Either training or all records from the
                EIA plant-parts (`plant_parts_train_df` or
                `plant_parts_true_df`).
            block_col (string):  If you want to restrict possible matches
                between ferc_df and eia_df based on a particular column,
                block_col is the column name of blocking column. Default is
                None. If None, this method will generate features between all
                possible matches.

        Returns:
            pandas.DataFrame: a dataframe of feature vectors between FERC and
            EIA.

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


class ModelTuner:
    """A class for tuning scikitlearn model."""

    def __init__(self, features_train, train_index, n_splits=10):
        """
        Initialize model tuner; test hyperparameters with cross validation.

        Initializing this object runs `test_model_parameters()` which runs
        through many options for model hyperparameters and collects scores
        from those model runs.

        Args:
            file_path_training (path-like): path to the CSV of training data.
                The training data needs to have at least two columns:
                record_id_eia record_id_ferc1.
            file_path_mul (pathlib.Path): path to EIA's plant-parts.
            pudl_out (object): instance of `pudl.output.pudltabl.PudlTabl()`.

        """
        self.features_train = features_train
        self.train_index = train_index
        self.n_splits = n_splits
        self.results_options = None
        self.best = None

    @staticmethod
    def kfold_cross_val(n_splits, features_known, known_index, lrc):
        """
        K-fold cross validation for model.

        Args:
            n_splits (int): the number of splits for the cross validation.
                If 5, the known data will be spilt 5 times into testing and
                training sets for example.
            features_known (pandas.DataFrame): a dataframe of comparison
                features. This should be created via `make_features`. This
                will contain all possible combinations of matches between
                your records.
            known_index (pandas.MultiIndex): an index with the known
                matches. The index must be a mutltiindex with record ids
                from both sets of records.

        """
        kf = KFold(n_splits=n_splits)
        fscore = []
        precision = []
        accuracy = []
        result_lrc_complied = pd.DataFrame()
        for train_index, test_index in kf.split(features_known):
            x_train = features_known.iloc[train_index]
            x_test = features_known.iloc[test_index]
            y_train = x_train.index.intersection(known_index)
            y_test = x_test.index.intersection(known_index)
            # Train the classifier
            lrc.fit(x_train, y_train)
            # predict matches for the test
            result_lrc = lrc.predict(x_test)
            # generate and compile the scores and outcomes of the
            # prediction
            fscore.append(rl.fscore(y_test, links_pred=result_lrc))
            precision.append(rl.precision(y_test, links_pred=result_lrc))
            accuracy.append(
                rl.accuracy(y_test, links_pred=result_lrc, total=result_lrc)
            )
            result_lrc_complied = pd.concat(
                [result_lrc_complied, pd.DataFrame(index=result_lrc)]
            )
        return result_lrc_complied, fscore, precision, accuracy

    def fit_predict_option(
        self, solver, c, cw, p, l1, n_splits, multi_class, results_options
    ):
        """
        Test and cross validate with a set of model parameters.

        In this method, we instantiate a model object with a given set of
        hyperparameters (which are selected within `test_model_parameters`)
        and then run k-fold cross vaidation with that model and our training
        data.

        Returns:
            pandas.DataFrame
        """
        logger.debug(f"train: {solver}: c-{c}, cw-{cw}, p-{p}, l1-{l1}")
        lrc = rl.LogisticRegressionClassifier(
            solver=solver,
            C=c,
            class_weight=cw,
            penalty=p,
            l1_ratio=l1,
            random_state=0,
            multi_class=multi_class,
        )
        results, fscore, precision, accuracy = self.kfold_cross_val(
            lrc=lrc,
            n_splits=n_splits,
            features_known=self.features_train,
            known_index=self.train_index,
        )

        # we're going to want to choose the best model so we need to save the
        # results of this model run...
        results_options = pd.concat(
            [
                results_options,
                pd.DataFrame(
                    data={
                        # result scores
                        "precision": [statistics.mean(precision)],
                        "f_score": [statistics.mean(fscore)],
                        "accuracy": [statistics.mean(accuracy)],
                        # info about results
                        "coef": [lrc.coefficients],
                        "interc": [lrc.intercept],
                        "predictions": [len(results)],
                        # info about which model hyperparameters we choose
                        "solver": [solver],
                        "c": [c],
                        "cw": [cw],
                        "penalty": [p],
                        "l1": [l1],
                        "multi_class": [multi_class],
                    },
                ),
            ]
        )
        return results_options

    @staticmethod
    def get_hyperparameters_options():
        """
        Generate a dictionary with sets of options for model hyperparameters.

        Note: The looping over all of the hyperparameters options here feels..
        messy. I investigated scikitlearn's documentaion for a cleaner way to
        do this. I came up empty handed, but I'm still sure I just missed it.

        Returns:
            dictionary: dictionary with autogenerated integers (keys) for each
            dictionary of model
        """
        # we are going to loop through the options for logistic regression
        # hyperparameters
        solvers = ["newton-cg", "lbfgs", "liblinear", "sag", "saga"]
        cs = [1, 10, 100, 1000]
        cws = ["balanced", None]
        ps = {
            "newton-cg": ["l2", "none"],
            "lbfgs": ["l2", "none"],
            "liblinear": ["l1", "l2"],
            "sag": ["l2", "none"],
            "saga": ["l1", "l2", "elasticnet", "none"],
        }
        hyper_options = []
        # we set l1_ratios and multi_classes inside this loop land bc
        for solver in solvers:
            for c in cs:
                for cw in cws:
                    for p in ps[solver]:
                        if p == "elasticnet":
                            l1_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
                        else:
                            l1_ratios = [None]
                        for l1 in l1_ratios:
                            # liblinear solver doesnt allow multinomial
                            # multi_class
                            if solver == "liblinear":
                                multi_classes = ["auto", "ovr"]
                            else:
                                multi_classes = ["auto", "ovr", "multinomial"]
                            for multi_class in multi_classes:
                                hyper_options.append(
                                    {
                                        "solver": solver,
                                        "c": c,
                                        "cw": cw,
                                        "penalty": p,
                                        "l1": l1,
                                        "multi_class": multi_class,
                                    }
                                )
        return hyper_options

    def test_model_parameters(self, clobber=False, update_hyper_params: bool = False):
        """
        Test and corss validate model parameters.

        The method runs `fit_predict_option()` on many options for model
        hyperparameters and saves info about the results for each model run so
        we can later determine which set of hyperparameters works best on
        predicting our training data.

        Args:
            n_splits (int): the number of times we want to split the training
                data during the k-fold cross validation.
        Returns:
            pandas.DataFrame: dataframe in which each record correspondings to
            one model run and contains info like scores of the run (how well
            it predicted our training data).

        """
        if clobber or self.results_options is None:
            logger.info(
                "We are about to test hyper parameters of the model while "
                "doing k-fold cross validation. This takes a few minutes...."
            )
            # it is testing an array of model hyper parameters and
            # cross-vaildating with the training data. It returns a df with
            # losts of result scores to be used to find the best resutls
            hyper_options = self.get_hyperparameters_options()
            # make an empty df to save the results into
            self.results_options = pd.DataFrame()
            for hyper in hyper_options:
                self.results_options = self.fit_predict_option(
                    solver=hyper["solver"],
                    c=hyper["c"],
                    cw=hyper["cw"],
                    p=hyper["penalty"],
                    l1=hyper["l1"],
                    multi_class=hyper["multi_class"],
                    n_splits=self.n_splits,
                    results_options=self.results_options,
                )
        return self.results_options

    def get_best_fit_model(self, clobber=False):
        """Get the best fitting model hyperparameters."""
        if clobber or self.best is None:
            # grab the highest scoring model...the f_score is most encompassing
            # score so we'll lead with that f_score
            self.best = (
                self.test_model_parameters()
                .sort_values(["f_score", "precision", "accuracy"], ascending=False)
                .head(1)
            )
            logger.info(
                "Scores from the best model hyperparameters:\n"
                f"    F-Score:   {self.best.loc[0,'f_score']:.02}\n"
                f"    Precision: {self.best.loc[0,'precision']:.02}\n"
                f"    Accuracy:  {self.best.loc[0,'accuracy']:.02}\n"
            )
        return self.best


class MatchManager:
    """Manages the results of ModelTuner and chooses best matches."""

    def __init__(self, best, inputs):
        """
        Initialize match manager.

        Args:
            best (pandas.DataFrame): one row table with details about the
                hyperparameters of best model option. Result of
                ``ModelTuner.get_best_fit_model()``.
            inputs (instance of ``InputManager``): instance of ``InputManager``
        """
        self.best = best
        self.train_df = inputs.prep_train_connections()
        self.all_ferc1 = inputs.get_all_ferc1()
        # get the # of ferc options within the available eia years.
        self.ferc1_options_len = len(
            self.all_ferc1[
                self.all_ferc1.report_year.isin(
                    inputs.get_plant_parts_true().report_date.dt.year.unique()
                )
            ]
        )

    def _apply_weights(self, features, coefs):
        """
        Apply coefficient weights to each feature.

        Args:
            features (pandas.DataFrame): a dataframe containing features of
                candidate or model matches. The order of the columns
                matters! They must be in the same order as they were fed
                into the model that produced the coefficients.
            coefs (array): array of integers with the same length as the
                columns in features.

        """
        if len(coefs) != len(features.columns):
            raise AssertionError(
                """The number of coeficients (the weight of the importance of the
            columns) should be the same as the number of the columns in the
            candiate matches coefficients."""
            )
        for coef_n in np.array(range(len(coefs))):
            features[features.columns[coef_n]] = features[
                features.columns[coef_n]
            ].multiply(coefs[coef_n])
        return features

    def weight_features(self, features):
        """
        Weight features of candidate (or model) matches with coefficients.

        Args:
            features (pandas.DataFrame): a dataframe containing features of
                candidate or model matches. The order of the columns
                matters! They must be in the same order as they were fed
                into the model that produced the coefficients.
            coefs (array): array of integers with the same length as the
                columns in features.

        """
        df = deepcopy(features)
        return (
            df.pipe(self._apply_weights, self.get_coefs())
            .assign(score=lambda x: x.sum(axis=1))
            .pipe(pudl.helpers.organize_cols, ["score"])
            .sort_values(["score"], ascending=False)
            .sort_index(level="record_id_ferc1")
        )

    def calc_match_stats(self, df):
        """
        Calculate stats needed to judge candidate matches.

        rank: diffs: iqr:

        Args:
            df (pandas.DataFrame): Dataframe of comparison features with
                MultiIndex containing the ferc and eia record ids.

        Returns
            pandas.DataFrame: the input df with the stats.

        """
        df = self.weight_features(df).reset_index()
        gb = df.groupby("record_id_ferc1")[["score"]]

        df = (
            df.sort_values(["record_id_ferc1", "score"])
            .assign(  # calculate differences between scores
                diffs=lambda x: x["score"].diff()
            )
            .merge(  # count grouped records
                pudl.helpers.count_records(df, ["record_id_ferc1"], "count"), how="left"
            )
            # calculate the iqr for each record_id_ferc1 group
            # believe it or not this is faster than .transform(scipy.stats.iqr)
            .merge(
                gb.agg(scipy.stats.iqr).rename(columns={"score": "iqr"}),
                left_on=["record_id_ferc1"],
                right_index=True,
            )
        )
        # rank the scores
        df.loc[:, "rank"] = gb.transform("rank", ascending=0, method="average")
        # assign the first diff of each ferc_id as a nan
        df.loc[df.record_id_ferc1 != df.record_id_ferc1.shift(1), "diffs"] = np.nan

        df = df.set_index(["record_id_ferc1", "record_id_eia"])
        return df

    def calc_murk(self, df, iqr_perc_diff):
        """Calculate the murky model matches."""
        distinction = df["iqr_all"] * iqr_perc_diff
        matches_murk = df[(df["rank"] == 1) & (df["diffs"] < distinction)]
        return matches_murk

    def calc_best_matches(self, df, iqr_perc_diff):
        """
        Find the highest scoring matches and report on match coverage.

        With the matches resulting from a model run, generate "best" matches by
        finding the highest ranking EIA match for each FERC record. If it is
        either the only match or it is different enough from the #2 ranked
        match, we consider it a winner. Also log stats about the coverage of
        the best matches.

        The matches are all of the results from the model prediction. The
        best matches are all of the matches that are distinct enough from it’s
        next closest match. The `murky_df` are the matches that are not
        “distinct enough” from the closes match. Distinct enough means that
        the best match isn’t one iqr away from the second best match.

        Args:
            df (pandas.DataFrame): dataframe with all of the model generate
                matches. This df needs to have been run through
                `calc_match_stats()`.
            iqr_perc_diff (float):

        Returns
            pandas.DataFrame : winning matches. Matches that had the
            highest rank in their record_id_ferc1, by a wide enough margin.

        """
        logger.info("Get the top scoring match for each FERC1 plant record.")
        unique_f = df.reset_index().drop_duplicates(subset=["record_id_ferc1"])
        distinction = df["iqr_all"] * iqr_perc_diff
        # for the best matches, grab the top ranked model match if there is a
        # big enough difference between it and the next highest ranked match
        # diffs is a measure of the difference between each record and the next
        # highest ranked model match
        # the other option here is if a model match is the highest rank and
        # there there is no other model matches
        best_match = df[
            ((df["rank"] == 1) & (df["diffs"] > distinction))
            | ((df["rank"] == 1) & (df["diffs"].isnull()))
        ]
        # we want to know how many of the
        self.murk_df = self.calc_murk(df, iqr_perc_diff)
        self.ties_df = df[df["rank"] == 1.5]

        logger.info(
            "Winning match stats:\n"
            "    matches vs ferc:      "
            f"{len(unique_f)/self.ferc1_options_len:.02%}\n"
            "    best match v ferc:    "
            f"{len(best_match)/self.ferc1_options_len:.02%}\n"
            f"    best match vs matches:{len(best_match)/len(unique_f):.02%}\n"
            f"    murk vs matches:      "
            f"{len(self.murk_df)/len(unique_f):.02%}\n"
            "    ties vs matches:      "
            f"{len(self.ties_df)/2/len(unique_f):.02%}\n"
        )

        # Add a column to show it was a prediction
        best_match.loc[:, "match_type"] = "prediction"

        return best_match

    def override_best_match_with_training_df(self, matches_best_df, train_df):
        """
        Override winning matches with training data matches.

        We want to ensure that all of the matches that we put in the
        training data for the record linkage model actually end up in the
        resutls from the record linkage model.

        Args:
            matches_best_df (pandas.DataFrame): best matches generated via
                `calc_best_matches()`. Matches that had the highest rank in
                their record_id_ferc1, by a wide enough margin.
            train_df (pandas.DataFrame): training data/known matches
                between ferc and the EIA plant-parts. Result of
                `prep_train_connections()`.

        Returns:
            pandas.DataFrame: overridden winning matches. Matches that show
            up in the training data `train_df` or if there was no
            corresponding training data, matches that had the highest rank
            in their record_id_ferc1, by a wide enough margin.
        """
        # create an duplicate column to show exactly where there are and aren't
        # overrides for ferc records. This is necessary because sometimes the
        # override is a blank so we can't just depend on record_id_eia.notnull()
        # when we merge on ferc id below.
        train_df = train_df.reset_index()
        train_df.loc[:, "record_id_ferc1_trn"] = train_df["record_id_ferc1"]

        # we want to override the eia when the training id is
        # different than the "winning" match from the record linkage
        matches_best_df = pd.merge(
            matches_best_df.reset_index(),
            train_df[["record_id_eia", "record_id_ferc1", "record_id_ferc1_trn"]],
            on=["record_id_ferc1"],
            how="outer",
            suffixes=("_rl", "_trn"),
        ).assign(
            record_id_eia=lambda x: np.where(
                x.record_id_ferc1_trn.notnull(), x.record_id_eia_trn, x.record_id_eia_rl
            )
        )

        overwrite_rules = (
            (matches_best_df.record_id_ferc1_trn.notnull())
            & (matches_best_df.record_id_eia_rl.notnull())
            & (matches_best_df.record_id_eia_trn != matches_best_df.record_id_eia_rl)
        )

        correct_match_rules = (  # need to update this
            (matches_best_df.record_id_ferc1_trn.notnull())
            & (matches_best_df.record_id_eia_trn.notnull())
            & (matches_best_df.record_id_eia_rl.notnull())
            & (matches_best_df.record_id_eia_trn == matches_best_df.record_id_eia_rl)
        )

        fill_in_the_blank_rules = (matches_best_df.record_id_eia_trn.notnull()) & (
            matches_best_df.record_id_eia_rl.isnull()
        )

        # check how many records were overridden
        overridden = matches_best_df.loc[overwrite_rules]

        # Add flag
        matches_best_df.loc[overwrite_rules, "match_type"] = "overridden"
        matches_best_df.loc[correct_match_rules, "match_type"] = "correct prediction"
        matches_best_df.loc[
            fill_in_the_blank_rules, "match_type"
        ] = "no prediction; training"

        logger.info(
            f"Overridden records:       {len(overridden)/len(train_df):.01%}\n"
            "New best match v ferc:    "
            f"{len(matches_best_df)/self.ferc1_options_len:.02%}"
        )
        # we don't need these cols anymore...
        matches_best_df = matches_best_df.drop(
            columns=["record_id_eia_trn", "record_id_eia_rl", "record_id_ferc1_trn"]
        )
        return matches_best_df

    @staticmethod
    def fit_predict_lrc(best, features_known, features_all, train_df_ids):
        """Generate, fit and predict model. Wahoo."""
        # prep the model with the hyperparameters
        lrc = rl.LogisticRegressionClassifier(
            solver=best["solver"].values[0],
            C=best["c"].values[0],
            class_weight=best["cw"].values[0],
            penalty=best["penalty"].values[0],
            l1_ratio=best["l1"].values[0],
            random_state=0,
            multi_class=best["multi_class"].values[0],
        )
        # fit the model with all of the
        lrc.fit(features_known, train_df_ids.index)
        # this step is getting preditions on all of the possible matches based
        # on the last run model above
        predict_all = lrc.predict(features_all)
        predict_all_df = pd.merge(
            pd.DataFrame(index=predict_all),
            features_all,
            left_index=True,
            right_index=True,
            how="left",
        )
        return predict_all_df

    def get_coefs(self):
        """
        Get the best models coeficients.

        The coeficients are the measure of relative importance that the model
        determined that each feature vector.
        """
        self.coefs = self.best.loc[0, "coef"]
        return self.coefs

    def get_best_matches(self, features_train, features_all):
        """
        Run logistic regression model and choose highest scoring matches.

        From `TuneModel.test_model_parameters()`, we get a dataframe in which
        each record correspondings to one model run and contains info like
        scores of the run (how well it predicted our training data). This
        method takes the result from `TuneModel.test_model_parameters()` and
        choose the model hyperparameters that scored the highest.

        The logistic regression model this method employs returns all matches
        from the candiate matches in `features_all`. But we only want one match
        for each FERC1 plant record. So this method uses the coeficients from
        the model (which are a measure of the importance of each of the
        features/columns in `features_all`) so weight the feature vecotrs. With
        the sum of the weighted feature vectors for each model match, this
        method selects the hightest scoring match via `calc_best_matches()`.

        Args:
            features_train (pandas.DataFrame): feature vectors between training
                data from FERC plants and EIA plant-parts. Result of
                ``Features.make_features()``.
            features_all (pandas.DataFrame): feature vectors between all data
                from FERC plants and EIA plant-parts. Result of
                ``Features.make_features()``.

        Returns:
            pandas.DataFrame: the best matches between ferc1 plant records and
            the EIA plant-parts. Each ferc1 plant record has a maximum of one
            best match. The dataframe has a MultiIndex with `record_id_eia` and
            `record_id_ferc1`.
        """
        # actually run a model using the "best" model!!
        logger.info("Fit and predict a model w/ the highest scoring hyperparameters.")
        # this returns all matches that the model deems good enough from the
        # candidate matches in the `features_all`
        matches_model = self.fit_predict_lrc(
            self.best, features_train, features_all, self.train_df
        )
        # weight the features of model matches with the coeficients
        # we need a metric of how different each match is
        # merge in the IRQ of the full options
        self.matches_model = pd.merge(
            self.calc_match_stats(matches_model),
            self.calc_match_stats(features_all)[["iqr"]],
            left_index=True,
            right_index=True,
            how="left",
            suffixes=("", "_all"),
        )
        matches_best_df = self.calc_best_matches(self.matches_model, 0.02).pipe(
            self.override_best_match_with_training_df, self.train_df
        )
        return matches_best_df


def prettyify_best_matches(
    matches_best, plant_parts_true_df, plants_ferc1_df, train_df, debug=False
):
    """
    Make the EIA-FERC best matches usable.

    Use the ID columns from the best matches to merge together both EIA
    plant-parts data and FERC plant data. This removes the comparison vectors
    (the floats between 0 and 1 that compare the two columns from each dataset).
    """
    ppe_cols_to_grab = list(
        set(
            pudl.analysis.plant_parts_eia.PPE_COLS
            + [
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
        )
    )
    connects_ferc1_eia = (
        # first merge in the EIA plant-parts
        pd.merge(
            matches_best.reset_index()[
                ["record_id_ferc1", "record_id_eia", "match_type"]
            ],
            # we only want the identifying columns from the PPE
            plant_parts_true_df.reset_index()[ppe_cols_to_grab],
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
            plants_ferc1_df,
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
            warnings.warn(message)
            return no_ferc
        else:
            logger.info(
                "jsuk there are some FERC-EIA matches that aren't in the steam \
                table but this is because they are linked to retired EIA generators."
            )
            # raise AssertionError(message)
            warnings.warn(message)

    _log_match_coverage(connects_ferc1_eia)
    for match_type in ["all", "overrides"]:
        check_match_consistentcy(connects_ferc1_eia, train_df, match_type)

    return connects_ferc1_eia


def _log_match_coverage(connects_ferc1_eia):
    eia_years = DataSource.from_id("eia860").working_partitions["years"]
    # get the matches from just the EIA working years
    m_eia_years = connects_ferc1_eia[
        (connects_ferc1_eia.report_date.dt.year.isin(eia_years))
        & (connects_ferc1_eia.record_id_eia.notnull())
    ]
    # get all records from just the EIA working years
    r_eia_years = connects_ferc1_eia[
        connects_ferc1_eia.report_date.dt.year.isin(eia_years)
    ]

    fuel_type_coverage = len(
        m_eia_years[m_eia_years.energy_source_code_1.notnull()]
    ) / len(m_eia_years)
    tech_type_coverage = len(
        m_eia_years[m_eia_years.technology_description.notnull()]
    ) / len(m_eia_years)

    def _get_subtable(table_name):
        return r_eia_years[r_eia_years.record_id_ferc1.str.contains(f"{table_name}")]

    def _get_match_pct(df):
        return round((len(df[df["record_id_eia"].notna()]) / len(df) * 100), 1)

    logger.info(
        "Coverage for matches during EIA working years:\n"
        f"    Fuel type: {fuel_type_coverage:.01%}\n"
        f"    Tech type: {tech_type_coverage:.01%}\n\n"
        "Coverage for all steam table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('steam'))}\n\n"
        f"Coverage for all small gen table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('gnrt_plant'))}\n\n"
        f"Coverage for all hydro table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('hydro'))}\n\n"
        f"Coverage for all pumped storage table records during EIA working years:\n"
        f"    EIA matches: {_get_match_pct(_get_subtable('pumped'))}"
    )


def check_match_consistentcy(connects_ferc1_eia, train_df, match_type="all"):
    """
    Check how consistent matches are across time.

    Args:
        connects_ferc1_eia (pandas.DataFrame)
        train_df (pandas.DataFrame)
        match_type (string): either 'all' - to check all of the matches - or
            'overrides' - to check just the overrides. Default is 'all'.
    """
    # these are the default
    consistency = 0.75
    consistency_one_cap_ferc = 0.9
    mask = connects_ferc1_eia.record_id_eia.notnull()

    if match_type == "overrides":
        consistency = 0.39
        consistency_one_cap_ferc = 0.83
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
        f"Matches with consistency across years of {match_type} matches is "
        f"{consist:.1%}"
    )
    if consist < consistency:
        raise AssertionError(
            f"Consistency of {match_type} matches across years dipped below "
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


def calc_annual_capital_additions_ferc1(steam_df, window=3):
    """
    Calculate annual capital additions for FERC1 steam records.

    Convert the capex_total column into annual capital additons the
    `capex_total` column is the cumulative capital poured into the plant over
    time. This function takes the annual difference should generate the annual
    capial additions. It also want generates a rolling average, to smooth out
    the big annual fluxuations.

    Args:
        steam_df (pandas.DataFrame): result of `prep_plants_ferc()`

    Returns:
        pandas.DataFrame: augemented version of steam_df with two additional
        columns: `capex_annual_addition` and `capex_annual_addition_rolling`.
    """
    # we need to sort the df so it lines up w/ the groupby
    steam_df = steam_df.sort_values(IDX_STEAM)

    steam_df = steam_df.assign(
        capex_wo_retirement_total=lambda x: x.capex_equipment.fillna(0)
        + x.capex_land.fillna(0)
        + x.capex_structures.fillna(0)
    )
    # we group on everything but the year so the groups are multi-year unique
    # plants the shift happens within these multi-year plant groups
    steam_df["capex_total_shifted"] = steam_df.groupby(
        [x for x in IDX_STEAM if x != "report_date"]
    )[["capex_wo_retirement_total"]].shift()
    steam_df = steam_df.assign(
        capex_annual_addition=lambda x: x.capex_wo_retirement_total
        - x.capex_total_shifted
    )

    addts = pudl.helpers.generate_rolling_avg(
        steam_df,
        group_cols=[x for x in IDX_STEAM if x != "report_date"],
        data_col="capex_annual_addition",
        window=window,
    ).pipe(pudl.helpers.convert_cols_dtypes, "ferc1")

    steam_df_w_addts = pd.merge(
        steam_df,
        addts[
            IDX_STEAM + ["capex_wo_retirement_total", "capex_annual_addition_rolling"]
        ],
        on=IDX_STEAM + ["capex_wo_retirement_total"],
        how="left",
    ).assign(
        capex_annual_per_mwh=lambda x: x.capex_annual_addition
        / x.net_generation_mwh_ferc1,
        capex_annual_per_mw=lambda x: x.capex_annual_addition / x.capacity_mw_ferc1,
        capex_annual_per_kw=lambda x: x.capex_annual_addition
        / x.capacity_mw_ferc1
        / 1000,
        capex_annual_per_mwh_rolling=lambda x: x.capex_annual_addition_rolling
        / x.net_generation_mwh_ferc1,
        capex_annual_per_mw_rolling=lambda x: x.capex_annual_addition_rolling
        / x.capacity_mw_ferc1,
    )

    steam_df_w_addts = add_mean_cap_additions(steam_df_w_addts)
    # bb tests for volumne of negative annual capex
    neg_cap_addts = len(
        steam_df_w_addts[steam_df_w_addts.capex_annual_addition_rolling < 0]
    ) / len(steam_df_w_addts)
    neg_cap_addts_mw = (
        steam_df_w_addts[
            steam_df_w_addts.capex_annual_addition_rolling < 0
        ].net_generation_mwh_ferc1.sum()
        / steam_df_w_addts.net_generation_mwh_ferc1.sum()
    )
    message = (
        f"{neg_cap_addts:.02%} records have negative capitial additions"
        f": {neg_cap_addts_mw:.02%} of capacity"
    )
    if neg_cap_addts > 0.1:
        warnings.warn(message)
    else:
        logger.info(message)
    return steam_df_w_addts


def add_mean_cap_additions(steam_df):
    """Add mean capital additions over lifetime of plant (via `IDX_STEAM`)."""
    idx_steam_no_date = [c for c in IDX_STEAM if c != "report_year"]
    gb_cap_an = steam_df.groupby(idx_steam_no_date)[["capex_annual_addition"]]
    # calcuate the standard deviatoin of each generator's capex over time
    df = (
        steam_df.merge(
            gb_cap_an.std()
            .add_suffix("_gen_std")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="m:1",  # should this really be 1:1?
        )
        .merge(
            gb_cap_an.mean()
            .add_suffix("_gen_mean")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="m:1",  # should this really be 1:1?
        )
        .assign(
            capex_annual_addition_diff_mean=lambda x: x.capex_annual_addition
            - x.capex_annual_addition_gen_mean,
        )
    )
    return df


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
    )
    # Make sure there is content!
    assert ~null_overrides.empty
    logger.debug(f"Found {len(null_overrides)} null overrides")
    # List of EIA columns to null. Ideally would like to get this from elsewhere, but
    # compiling this here for now...
    eia_cols_to_null = [
        "plant_name_new",
        "plant_part",
        "ownership",
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
