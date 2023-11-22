"""Scikit-Learn classification pipeline for linking records between years.

FERC does not publish plant IDs, so to be able to do any time-series analysis, it is
required to assign IDs ourselves. The pipeline has been generalized to work with any
inter-year record linkage problem.
"""
from typing import Any, Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.cluster import AgglomerativeClustering
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder

import pudl
from pudl.analysis.record_linkage.cleaning_steps import CleaningRules

logger = pudl.logging_helpers.get_logger(__name__)


_TFIDF_DEFAULT_OPTIONS = {
    "analyzer": "char",
    "ngram_range": (2, 10),
}

_ONE_HOT_DEFAULT_OPTIONS = {
    "categories": "auto",
}

_CLEANING_FUNCTIONS = {
    "null_to_zero": lambda df, cols: df[cols].fillna(value=0.0),
    "null_to_empty_str": lambda df, cols: df[cols].fillna(value=""),
    "fix_int_na": lambda df, cols: pudl.helpers.fix_int_na(df, columns=cols)[cols],
}


class ColumnTransform(BaseModel):
    """Configuration for a single column transform in the CrossYearLinker.

    Defines a scikit-learn transformer to be used by a ColumnTransformer. See
    https://scikit-learn.org/stable/modules/generated/sklearn.compose.ColumnTransformer.html#sklearn.compose.ColumnTransformer
    for more information.  The full set of column transforms will transform the
    input DataFrame into a feature matrix that will be to cluster records.

    The 'transformer' should be a string or an initialized scikit-learn BaseEstimator.
    If it is a string, it should be one of the following:

    'string' - Applies a TfidfVectorizer to the column.
    'category' - Applies a OneHotEncoder to the column.
    'number' - Applies a MinMaxScaler to the column.
    """

    step_name: str
    columns: list[str] | str
    transformer: BaseEstimator | Literal["string", "category", "number"]
    transformer_options: dict[str, Any] = {}
    weight: float = 1.0
    cleaning_ops: list[str | CleaningRules] = []

    # This can be handled more elegantly in Pydantic 2.0.
    class Config:
        """To allow a BaseEstimator for 'transformer', arbitrary types must be allowed."""

        arbitrary_types_allowed = True

    def clean_columns(self, df):
        """Perform configurable set of cleaning operations on inputs before pipeline."""
        for cleaning_op in self.cleaning_ops:
            if isinstance(cleaning_op, str):
                df[self.columns] = _CLEANING_FUNCTIONS[cleaning_op](df, self.columns)
            elif isinstance(cleaning_op, CleaningRules):
                df = cleaning_op.clean(df)

        return df

    def as_step(self) -> tuple[str, BaseEstimator, list[str]]:
        """Return tuple formatted as sklearn expects for pipeline step."""
        # Create transform from one of the default types
        if isinstance(self.transformer, str):
            if self.transformer == "string":
                options = _TFIDF_DEFAULT_OPTIONS | self.transformer_options
                transformer = TfidfVectorizer(**options)
            elif self.transformer == "category":
                options = _ONE_HOT_DEFAULT_OPTIONS | self.transformer_options
                transformer = OneHotEncoder(**options)
            elif self.transformer == "number":
                options = self.transformer_options
                transformer = MinMaxScaler(**options)
            else:
                raise RuntimeError(
                    f"Error: {self.transformer} must be either 'string', 'categor', 'number', or a scikit-learn BaseEstimator"
                )
        elif isinstance(self.transformer, BaseEstimator):
            transformer = self.transformer
        else:
            raise RuntimeError(
                f"Error: {self.transformer} must be either 'string', 'categor', 'number', or a scikit-learn BaseEstimator"
            )

        return (self.step_name, transformer, self.columns)


class CrossYearLinker(BaseModel):
    """Model config for a CrossYearLinker, which will link records within the same dataset.

    This model will apply a set of column transforms to create a feature matrix which
    is used to determine distances between records and cluster similar ones. By default
    the linker will apply PCA to reduce the dimensionality of the feature matrix, and
    will apply a significant penalty to records from the same year to avoid matches
    within a year.
    """

    id_column: str
    column_transforms: list[ColumnTransform]
    reduce_dims: int | None
    penalize_same_year: bool = True
    distance_metric: str = "euclidean"
    distance_threshold: float = 1.5
    n_clusters: int | None = None

    class Config:
        """To allow a BaseEstimator for 'transformer', arbitrary types must be allowed."""

        arbitrary_types_allowed = True

    def fit_predict(self, df: pd.DataFrame):
        """Construct scikit-learn Pipeline and apply it to input data to assign IDs."""
        if self.penalize_same_year:
            distance_estimator = DistancePenalizeSameYear(
                np.array(df.report_year), metric=self.distance_metric
            )
        else:
            distance_estimator = PrecomputeDistance(metric=self.distance_metric)

        for transform in self.column_transforms:
            df = transform.clean_columns(df)

        pipeline = Pipeline(
            [
                (
                    "preprocess",
                    ColumnTransformer(
                        transformers=[
                            transform.as_step() for transform in self.column_transforms
                        ],
                        transformer_weights={
                            transform.step_name: transform.weight
                            for transform in self.column_transforms
                        },
                    ),
                ),
                (
                    "to_dense",
                    DenseTransformer(),
                ),
                (
                    "dim_reduction",
                    PCA(copy=False, n_components=self.reduce_dims),
                ),
                ("precompute_dist", distance_estimator),
                (
                    "classifier",
                    AgglomerativeClustering(
                        n_clusters=self.n_clusters,
                        metric="precomputed",
                        linkage="average",
                        distance_threshold=self.distance_threshold,
                        compute_distances=True,
                    ),
                ),
            ]
        )

        df[self.id_column] = pipeline.fit_predict(df)

        return df


class PrecomputeDistance(BaseEstimator, TransformerMixin):
    """Precompute distances which are used during clustering and prediction."""

    def __init__(self, metric="euclidean"):
        """Initialize with distance metric."""
        self.metric = metric

    def fit(self, X, y=None, **fit_params):  # noqa: N803
        """Required by scikit-learn, but does not modify anything."""
        return self

    def transform(self, X, y=None, **fit_params):  # noqa: N803
        """Compute distance between records then add penalty to records from same year."""
        return pairwise_distances(X, metric=self.metric)


class DistancePenalizeSameYear(PrecomputeDistance):
    """Custom estimator to compute distances used to identify clusters of plants."""

    def __init__(self, report_years: np.array, metric="euclidean", penalty=1000):
        """Initialize estimator with configurable parameters.

        Args:
            report_years: Used to find records with same report year and add significant
                distance penalty to these records to avoid matching records.
                from the same year.
            metric: Distance metric to use in computation.
            penalty: Penalty to apply to records with the same report year.
        """
        self.report_years = report_years
        self.metric = metric
        self.penalty = penalty

    def fit(self, X, y=None, **fit_params):  # noqa: N803
        """Required by scikit-learn, but does not modify anything."""
        return self

    def transform(self, X, y=None, **fit_params):  # noqa: N803
        """Compute distance between records then add penalty to records from same year."""
        dist_matrix = super().transform(X)

        # Create penalty matrix
        # Probably not the most elegant way to handle this
        penalty_matrix = pairwise_distances(self.report_years.reshape(-1, 1))
        penalty_matrix += self.penalty
        penalty_matrix[penalty_matrix > self.penalty] = 0

        # distance from node to itself should still be 0
        np.fill_diagonal(penalty_matrix, 0)
        dist_matrix += penalty_matrix
        return dist_matrix


class DenseTransformer(BaseEstimator, TransformerMixin):
    """Convert sparse numpy matrix to dense numpy array."""

    def fit(self, X, y=None, **fit_params):  # noqa: N803
        """No modifications made during fitting."""
        return self

    def transform(self, X, y=None, **fit_params):  # noqa: N803
        """No modifications made during fitting."""
        return np.asarray(np.float32(X.todense()))
