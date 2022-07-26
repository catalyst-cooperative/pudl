"""Scikit-Learn classification pipeline for identifying related FERC 1 plant records.

Sadly FERC doesn't provide any kind of real IDs for the plants that report to them --
all we have is their names (a freeform string) and the data that is reported alongside
them. This is often enough information to be able to recognize which records ought to be
associated with each other year to year to create a continuous time series. However, we
want to do that programmatically, which means using some clustering / categorization
tools from scikit-learn

"""

from difflib import SequenceMatcher

import networkx as nx  # Used to knit incomplete ferc plant time series together.
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer

# These modules are required for the FERC Form 1 Plant ID & Time Series
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, Normalizer, OneHotEncoder

import pudl

logger = pudl.helpers.get_logger(__name__)


class FERCPlantClassifier(BaseEstimator, ClassifierMixin):
    """A classifier for identifying FERC plant time series in FERC Form 1 data.

    We want to be able to give the classifier a FERC plant record, and get back the
    group of records(or the ID of the group of records) that it ought to be part of.

    There are hundreds of different groups of records, and we can only know what they
    are by looking at the whole dataset ahead of time. This is the "fitting" step, in
    which the groups of records resulting from a particular set of model parameters(e.g.
    the weights that are attributes of the class) are generated.

    Once we have that set of record categories, we can test how well the classifier
    performs, by checking it against test / training data which we have already
    classified by hand. The test / training set is a list of lists of unique FERC plant
    record IDs(each record ID is the concatenation of: report year, respondent id,
    supplement number, and row number). It could also be stored as a dataframe where
    each column is associated with a year of data(some of which could be empty). Not
    sure what the best structure would be.

    If it's useful, we can assign each group a unique ID that is the time ordered
    concatenation of each of the constituent record IDs. Need to understand what the
    process for checking the classification of an input record looks like.

    To score a given classifier, we can look at what proportion of the records in the
    test dataset are assigned to the same group as in our manual classification of those
    records. There are much more complicated ways to do the scoring too... but for now
    let's just keep it as simple as possible.

    """

    def __init__(self, plants_df: pd.DataFrame, min_sim: float = 0.75) -> None:
        """Initialize the classifier.

        Args:
            plants_df: The entire FERC Form 1 plant table as a dataframe. Needed in
                order to calculate the distance metrics between all of the records so we
                can group the plants in the fit() step, so we can check how well they
                are categorized later...
            min_sim: Number between 0.0 and 1.0, indicating the minimum value of
                cosine similarity that we are willing to accept as indicating two
                records are part of the same plant record time series. All entries in
                the pairwise similarity matrix below this value will be zeroed out.

        """
        self.min_sim = min_sim
        self.plants_df = plants_df
        self._years = self.plants_df.report_year.unique()  # could we list() here?

    def fit(
        self, X, y=None  # noqa: N803 Canonical capital letter...
    ) -> "FERCPlantClassifier":
        """Use weighted FERC plant features to group records into time series.

        The fit method takes the vectorized, normalized, weighted FERC plant
        features (X) as input, calculates the pairwise cosine similarity matrix
        between all records, and groups the records in their best time series.
        The similarity matrix and best time series are stored as data members
        in the object for later use in scoring & predicting.

        This isn't quite the way a fit method would normally work.

        Args:
            X: a sparse matrix of size n_samples x n_features.
            y: Included only for API compatibility.

        """
        self._cossim_df = pd.DataFrame(cosine_similarity(X))
        self._best_of = self._best_by_year()
        # Make the best match indices integers rather than floats w/ NA values.
        self._best_of[list(self._years)] = (
            self._best_of[list(self._years)].fillna(-1).astype(int)
        )

        return self

    def transform(self, X, y=None):  # noqa: N803
        """Passthrough transform method -- just returns self."""
        return self

    def predict(self, X, y=None):  # noqa: N803
        """Identify time series of similar records to input record_ids.

        Given a one-dimensional dataframe X, containing FERC record IDs, return
        a dataframe in which each row corresponds to one of the input record_id
        values (ordered as the input was ordered), with each column
        corresponding to one of the years worth of data. Values in the returned
        dataframe are the FERC record_ids of the record most similar to the
        input record within that year. Some of them may be null, if there was
        no sufficiently good match.

        Row index is the seed record IDs. Column index is years.

        Todo:
        * This method is hideously inefficient. It should be vectorized.
        * There's a line that throws a FutureWarning that needs to be fixed.

        """
        try:
            getattr(self, "_cossim_df")
        except AttributeError:
            raise RuntimeError("You must train classifer before predicting data!")

        tmp_best = pd.concat(
            [
                self._best_of.loc[:, ["record_id"] + list(self._years)],
                pd.DataFrame(data=[""], index=[-1], columns=["record_id"]),
            ]
        )
        out_dfs = []
        # For each record_id we've been given:
        for x in X:
            # Find the index associated with the record ID we are predicting
            # a grouping for:
            idx = tmp_best[tmp_best.record_id == x].index.values[0]

            # Mask the best_of dataframe, keeping only those entries where
            # the index of the chosen record_id appears -- this results in a
            # huge dataframe almost full of NaN values.
            w_m = (
                tmp_best[self._years][tmp_best[self._years] == idx]
                # Grab the index values of the rows in the masked dataframe which
                # are NOT all NaN -- these are the indices of the *other* records
                # which found the record x to be one of their best matches.
                .dropna(how="all").index.values
            )

            # Now look up the indices of the records which were found to be
            # best matches to the record x.
            b_m = tmp_best.loc[idx, self._years].astype(int)

            # Here we require that there is no conflict between the two sets
            # of indices -- that every time a record shows up in a grouping,
            # that grouping is either the same, or a subset of the other
            # groupings that it appears in. When no sufficiently good match
            # is found the "index" in the _best_of array is set to -1, so
            # requiring that the b_m value be >=0 screens out those no-match
            # cases. This is okay -- we're just trying to require that the
            # groupings be internally self-consistent, not that they are
            # completely identical. Being flexible on this dramatically
            # increases the number of records that get assigned a plant ID.
            if np.array_equiv(w_m, b_m[b_m >= 0].values):
                # This line is causing a warning. In cases where there are
                # some years no sufficiently good match exists, and so b_m
                # doesn't contain an index. Instead, it has a -1 sentinel
                # value, which isn't a label for which a record exists, which
                # upsets .loc. Need to find some way around this... but for
                # now it does what we want. We could use .iloc instead, but
                # then the -1 sentinel value maps to the last entry in the
                # dataframe, which also isn't what we want.  Blargh.
                new_grp = tmp_best.loc[b_m, "record_id"]

                # Stack the new list of record_ids on our output DataFrame:
                out_dfs.append(
                    pd.DataFrame(
                        data=new_grp.values.reshape(1, len(self._years)),
                        index=pd.Index(
                            [tmp_best.loc[idx, "record_id"]], name="seed_id"
                        ),
                        columns=self._years,
                    )
                )
        return pd.concat(out_dfs)

    def score(self, X, y=None):  # noqa: N803
        """Scores a collection of FERC plant categorizations.

        For every record ID in X, predict its record group and calculate
        a metric of similarity between the prediction and the "ground
        truth" group that was passed in for that value of X.

        Args:
            X (pandas.DataFrame): an n_samples x 1 pandas dataframe of FERC
                Form 1 record IDs.
            y (pandas.DataFrame): a dataframe of "ground truth" FERC Form 1
                record groups, corresponding to the list record IDs in X

        Returns:
            numpy.ndarray: The average of all the similarity metrics as the
            score.
        """
        scores = []
        for true_group in y:
            true_group = str.split(true_group, sep=",")
            true_group = [s for s in true_group if s != ""]
            predicted_groups = self.predict(pd.DataFrame(true_group))
            for rec_id in true_group:
                sm = SequenceMatcher(None, true_group, predicted_groups.loc[rec_id])
                scores = scores + [sm.ratio()]

        return np.mean(scores)

    def _best_by_year(self):
        """Finds the best match for each plant record in each other year."""
        # only keep similarity matrix entries above our minimum threshold:
        out_df = self.plants_df.copy()
        sim_df = self._cossim_df[self._cossim_df >= self.min_sim]

        # Add a column for each of the years, in which we will store indices
        # of the records which best match the record in question:
        for yr in self._years:
            newcol = yr
            out_df[newcol] = -1

        # seed_yr is the year we are matching *from* -- we do the entire
        # matching process from each year, since it may not be symmetric:
        for seed_yr in self._years:
            seed_idx = self.plants_df.index[self.plants_df.report_year == seed_yr]
            # match_yr is all the other years, in which we are finding the best
            # match
            for match_yr in self._years:
                best_of_yr = match_yr
                match_idx = self.plants_df.index[self.plants_df.report_year == match_yr]
                # For each record specified by seed_idx, obtain the index of
                # the record within match_idx that that is the most similar.
                best_idx = sim_df.iloc[seed_idx, match_idx].idxmax(axis=1)
                out_df.iloc[seed_idx, out_df.columns.get_loc(best_of_yr)] = best_idx

        return out_df


def make_ferc1_clf(
    plants_df,
    ngram_min=2,
    ngram_max=10,
    min_sim=0.75,
    plant_name_ferc1_wt=2.0,
    plant_type_wt=2.0,
    construction_type_wt=1.0,
    capacity_mw_wt=1.0,
    construction_year_wt=1.0,
    utility_id_ferc1_wt=1.0,
    fuel_fraction_wt=1.0,
):
    """Create a FERC Plant Classifier using several weighted features.

    Given a FERC steam plants dataframe plants_df, which also includes fuel consumption
    information, transform a selection of useful columns into features suitable for use
    in calculating inter-record cosine similarities. Individual features are weighted
    according to the keyword arguments.

    Features include:

      * plant_name (via TF-IDF, with ngram_min and ngram_max as parameters)
      * plant_type (OneHot encoded categorical feature)
      * construction_type (OneHot encoded categorical feature)
      * capacity_mw (MinMax scaled numerical feature)
      * construction year (OneHot encoded categorical feature)
      * utility_id_ferc1 (OneHot encoded categorical feature)
      * fuel_fraction_mmbtu (several MinMax scaled numerical columns, which are
        normalized and treated as a single feature.)

    This feature matrix is then used to instantiate a FERCPlantClassifier.

    The combination of the ColumnTransformer and FERCPlantClassifier are combined in a
    sklearn Pipeline, which is returned by the function.

    Arguments:
        ngram_min (int): the minimum lengths to consider in the vectorization of the
            plant_name feature.
        ngram_max (int): the maximum n-gram lengths to consider in the vectorization of
            the plant_name feature.
        min_sim (float): the minimum cosine similarity between two records that can be
            considered a "match" (a number between 0.0 and 1.0).
        plant_name_ferc1_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        plant_type_wt (float): weight used to determine the relative importance of each
            of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        construction_type_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        capacity_mw_wt (float):weight used to determine the relative importance of each
            of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        construction_year_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        utility_id_ferc1_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        fuel_fraction_wt (float): weight used to determine the relative importance of
            each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.

    Returns:
        sklearn.pipeline.Pipeline: an sklearn Pipeline that performs reprocessing and
        classification with a FERCPlantClassifier object.

    """
    # Make a list of all the fuel fraction columns for use as one feature.
    fuel_cols = list(plants_df.filter(regex=".*_fraction_mmbtu$").columns)

    ferc1_pipe = Pipeline(
        [
            (
                "preprocessor",
                ColumnTransformer(
                    transformers=[
                        (
                            "plant_name_ferc1",
                            TfidfVectorizer(
                                analyzer="char", ngram_range=(ngram_min, ngram_max)
                            ),
                            "plant_name_ferc1",
                        ),
                        (
                            "plant_type",
                            OneHotEncoder(categories="auto"),
                            ["plant_type"],
                        ),
                        (
                            "construction_type",
                            OneHotEncoder(categories="auto"),
                            ["construction_type"],
                        ),
                        ("capacity_mw", MinMaxScaler(), ["capacity_mw"]),
                        (
                            "construction_year",
                            OneHotEncoder(categories="auto"),
                            ["construction_year"],
                        ),
                        (
                            "utility_id_ferc1",
                            OneHotEncoder(categories="auto"),
                            ["utility_id_ferc1"],
                        ),
                        (
                            "fuel_fraction_mmbtu",
                            Pipeline(
                                [("scaler", MinMaxScaler()), ("norm", Normalizer())]
                            ),
                            fuel_cols,
                        ),
                    ],
                    transformer_weights={
                        "plant_name_ferc1": plant_name_ferc1_wt,
                        "plant_type": plant_type_wt,
                        "construction_type": construction_type_wt,
                        "capacity_mw": capacity_mw_wt,
                        "construction_year": construction_year_wt,
                        "utility_id_ferc1": utility_id_ferc1_wt,
                        "fuel_fraction_mmbtu": fuel_fraction_wt,
                    },
                ),
            ),
            (
                "classifier",
                FERCPlantClassifier(min_sim=min_sim, plants_df=plants_df),
            ),
        ]
    )
    return ferc1_pipe


def plants_steam_assign_plant_ids(ferc1_steam_df, ferc1_fuel_df):
    """Assign IDs to the large steam plants."""
    ###########################################################################
    # FERC PLANT ID ASSIGNMENT
    ###########################################################################
    # Now we need to assign IDs to the large steam plants, since FERC doesn't
    # do this for us.
    logger.info("Identifying distinct large FERC plants for ID assignment.")

    # scikit-learn still doesn't deal well with NA values (this will be fixed
    # eventually) We need to massage the type and missing data for the
    # Classifier to work.
    ferc1_steam_df = pudl.helpers.fix_int_na(
        ferc1_steam_df, columns=["construction_year"]
    )

    # Grab fuel consumption proportions for use in assigning plant IDs:
    fuel_fractions = pudl.transform.ferc1.fuel_by_plant_ferc1(ferc1_fuel_df)
    ffc = list(fuel_fractions.filter(regex=".*_fraction_mmbtu$").columns)

    ferc1_steam_df = ferc1_steam_df.merge(
        fuel_fractions[["utility_id_ferc1", "plant_name_ferc1", "report_year"] + ffc],
        on=["utility_id_ferc1", "plant_name_ferc1", "report_year"],
        how="left",
    )
    # We need to fill the null values for these numerical feature vectors with
    # zeros. not ideal, but the model requires dealing with nulls
    null_to_zero = ffc + ["capacity_mw"]
    ferc1_steam_df[null_to_zero] = ferc1_steam_df[null_to_zero].fillna(value=0.0)

    # Train the classifier using DEFAULT weights, parameters not listed here.
    ferc1_clf = make_ferc1_clf(ferc1_steam_df).fit_transform(ferc1_steam_df)

    # Use the classifier to generate groupings of similar records:
    record_groups = ferc1_clf.predict(ferc1_steam_df.record_id)
    n_tot = len(ferc1_steam_df)
    n_grp = len(record_groups)
    pct_grp = n_grp / n_tot
    logger.info(
        f"Successfully associated {n_grp} of {n_tot} ({pct_grp:.2%}) "
        f"FERC Form 1 plant records with multi-year plant entities."
    )

    record_groups.columns = record_groups.columns.astype(str)
    cols = record_groups.columns
    record_groups = record_groups.reset_index()

    # Now we are going to create a graph (network) that describes all of the
    # binary relationships between a seed_id and the record_ids that it has
    # been associated with in any other year. Each connected component of that
    # graph is a ferc plant time series / plant_id
    logger.info("Assigning IDs to multi-year FERC plant entities.")
    edges_df = pd.DataFrame(columns=["source", "target"])
    for col in cols:
        new_edges = record_groups[["seed_id", col]]
        new_edges = new_edges.rename({"seed_id": "source", col: "target"}, axis=1)
        edges_df = pd.concat([edges_df, new_edges], sort=True)

    # Drop any records where there's no target ID (no match in a year)
    edges_df = edges_df[edges_df.target != ""]

    # We still have to deal with the orphaned records -- any record which
    # wasn't place in a time series but is still valid should be included as
    # its own independent "plant" for completeness, and use in aggregate
    # analysis.
    orphan_record_ids = np.setdiff1d(
        ferc1_steam_df.record_id.unique(), record_groups.values.flatten()
    )
    logger.info(
        f"Identified {len(orphan_record_ids)} orphaned FERC plant records. "
        f"Adding orphans to list of plant entities."
    )
    orphan_df = pd.DataFrame({"source": orphan_record_ids, "target": orphan_record_ids})
    edges_df = pd.concat([edges_df, orphan_df], sort=True)

    # Use the data frame we've compiled to create a graph
    G = nx.from_pandas_edgelist(  # noqa: N806
        edges_df, source="source", target="target"
    )
    # Find the connected components of the graph
    ferc1_plants = (G.subgraph(c) for c in nx.connected_components(G))

    # Now we'll iterate through the connected components and assign each of
    # them a FERC Plant ID, and pull the results back out into a dataframe:
    plants_w_ids = []
    for plant_id_ferc1, plant in enumerate(ferc1_plants):
        nx.set_edge_attributes(plant, plant_id_ferc1 + 1, name="plant_id_ferc1")
        new_plant_df = nx.to_pandas_edgelist(plant)
        plants_w_ids.append(new_plant_df)
    plants_w_ids = pd.concat(plants_w_ids)
    logger.info(
        f"Successfully Identified {plant_id_ferc1+1-len(orphan_record_ids)} "
        f"multi-year plant entities."
    )

    # Set the construction year back to numeric because it is.
    ferc1_steam_df["construction_year"] = pd.to_numeric(
        ferc1_steam_df["construction_year"], errors="coerce"
    )
    # We don't actually want to save the fuel fractions in this table... they
    # were only here to help us match up the plants.
    ferc1_steam_df = ferc1_steam_df.drop(ffc, axis=1)

    # Now we need a list of all the record IDs, with their associated
    # FERC 1 plant IDs. However, the source-target listing isn't
    # guaranteed to list every one of the nodes in either list, so we
    # need to compile them together to ensure that we get every single
    sources = (
        plants_w_ids.drop("target", axis=1)
        .drop_duplicates()
        .rename({"source": "record_id"}, axis=1)
    )
    targets = (
        plants_w_ids.drop("source", axis=1)
        .drop_duplicates()
        .rename({"target": "record_id"}, axis=1)
    )
    plants_w_ids = (
        pd.concat([sources, targets])
        .drop_duplicates()
        .sort_values(["plant_id_ferc1", "record_id"])
    )
    steam_rids = ferc1_steam_df.record_id.values
    pwids_rids = plants_w_ids.record_id.values
    missing_ids = [rid for rid in steam_rids if rid not in pwids_rids]
    if missing_ids:
        raise AssertionError(
            f"Uh oh, we lost {abs(len(steam_rids)-len(pwids_rids))} FERC "
            f"steam plant record IDs: {missing_ids}"
        )
    ferc1_steam_df = pd.merge(ferc1_steam_df, plants_w_ids, on="record_id")
    return ferc1_steam_df


def plants_steam_validate_ids(ferc1_steam_df):
    """Tests that plant_id_ferc1 times series includes one record per year.

    Args:
        ferc1_steam_df (pandas.DataFrame): A DataFrame of the data from the FERC 1
            Steam table.

    Returns:
        None
    """
    ##########################################################################
    # FERC PLANT ID ERROR CHECKING STUFF
    ##########################################################################

    # Test to make sure that we don't have any plant_id_ferc1 time series
    # which include more than one record from a given year. Warn the user
    # if we find such cases (which... we do, as of writing)
    year_dupes = (
        ferc1_steam_df.groupby(["plant_id_ferc1", "report_year"])["utility_id_ferc1"]
        .count()
        .reset_index()
        .rename(columns={"utility_id_ferc1": "year_dupes"})
        .query("year_dupes>1")
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
