"""Define a record linkage model interface and implement common functionality."""
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict
from sklearn.base import BaseEstimator
from sklearn.cluster import AgglomerativeClustering
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA, IncrementalPCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    MinMaxScaler,
    Normalizer,
    OneHotEncoder,
)

import pudl


class ModelComponent(BaseModel, ABC):
    """A :class:`ModelComponent` is the basic building block of a record linkage model.

    :class:`ModelComponent` defines a simple interface that should be implemented to
    create basic model steps that can be combined and reused at will. This interface
    essentially just says that a :class:`ModelComponent` should take some configuration
    (inherits from :class:`BaseModel`), and should be callable.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @abstractmethod
    def __call__(self, *args, **kwargs):
        """Every model component should be callable."""
        ...


_GENERIC_COLUMN_TRANSFORMS = {
    "function_transforms": {
        "null_to_zero": lambda df: df.fillna(value=0.0),
        "null_to_empty_str": lambda df: df.fillna(value=""),
        "fix_int_na": lambda df: pudl.helpers.fix_int_na(df, columns=list(df.columns)),
    },
    "configurable_transforms": {
        "string": {
            "class": TfidfVectorizer,
            "default_options": {
                "analyzer": "char",
                "ngram_range": (2, 10),
            },
        },
        "category": {
            "class": OneHotEncoder,
            "default_options": {
                "categories": "auto",
            },
        },
        "number": {
            "class": MinMaxScaler,
            "default_options": {},
        },
        "norm": {
            "class": Normalizer,
            "default_options": {},
        },
    },
}


class ColumnTransformation(BaseModel):
    """Define a set of transformations to apply to one or more columns."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    transformations: list[BaseEstimator | str | ModelComponent]
    options: dict[str, Any] = {}
    weight: float = 1.0
    columns: list[str]

    def as_pipeline(self) -> Pipeline:
        """Return a Pipeline to transform columns based on configuration."""
        transform_steps = []
        for i, transform in enumerate(self.transformations):
            if isinstance(transform, BaseEstimator):
                transform_steps.append((f"custom_transform_{i}", transform))
            elif isinstance(transform, ModelComponent):
                transform_steps.append(
                    (f"model_component_{i}", FunctionTransformer(transform))
                )
            elif func := _GENERIC_COLUMN_TRANSFORMS["function_transforms"].get(
                transform
            ):
                transform_steps.append((transform, FunctionTransformer(func)))
            elif config := _GENERIC_COLUMN_TRANSFORMS["configurable_transforms"].get(
                transform
            ):
                options = config["default_options"] | self.options
                transform_steps.append((transform, config["class"](**options)))

        return Pipeline(transform_steps)


class DataFrameEmbedder(ModelComponent):
    """This ModelComponent performs a series of column transformations on a DataFrame.

    Under the hood this uses :class:`sklearn.compose.ColumnTransformer`. As
    configuration it takes as configuration a mapping of column names to a list of
    transformations to apply. Transformations can be specified either by passing an
    instance of a :class:`sklearn.base.BaseEstimator`, or a string to select from
    several common/generic transformers defined by this class. If a string is used, it
    should be one of the following:

    * ``string`` - Applies a TfidfVectorizer to the column.
    * ``category`` - Applies a OneHotEncoder to the column.
    * ``number`` - Applies a MinMaxScaler to the column.
    """

    #: Maps step name to list of transformations.
    transformations: dict[str, ColumnTransformation]

    #: Applying column transformations may produce a sparse matrix.
    #: If this flag is set, the matrix will automatically be made dense before returning.
    make_dense: bool

    def _construct_transformer(self) -> ColumnTransformer:
        """Use configuration to construct :class:`sklearn.compose.ColumnTransformer`."""
        return ColumnTransformer(
            transformers=[
                (name, column_transform.as_pipeline(), column_transform.columns)
                for name, column_transform in self.transformations.items()
            ],
            transformer_weights={
                name: column_transform.weight
                for name, column_transform in self.transformations.items()
            },
        )

    def __call__(self, df: pd.DataFrame):
        """Use :class:`sklearn.compose.ColumnTransformer` to transform input."""
        transformer = self._construct_transformer()
        transformed = transformer.fit_transform(df)

        if self.make_dense:
            transformed = np.array(transformed.todense())

        return transformed


class ReducedDimDataFrameEmbedder(DataFrameEmbedder):
    """:class:`DataFrameEmbedder` subclass that reduces output dimensions using PCA."""

    #: Passed to :class:`sklearn.decomposition.PCA` param n_components
    output_dims: int | float | None = 500
    make_dense: bool = True

    def __call__(self, df: pd.DataFrame):
        """Apply PCA to output of :class:`DataFrameEmbedder`."""
        transformed = super().__call__(df)
        pca = PCA(copy=False, n_components=self.output_dims, batch_size=500)

        return pca.fit_transform(transformed)


class ReducedDimDataFrameEmbedderSparse(DataFrameEmbedder):
    """:class:`DataFrameEmbedder` subclass, using IncrementalPCA to reduce dimensions.

    This class differs from :class:`ReducedDimDataFrameEmbedder` in that it applies
    IncrementalPCA instead of a normal PCA implementation. This implementation is
    an approximation of a true PCA, but it operates with constant memory usage of
    batch_size * n_features (where n_features is the number of columns in the input
    matrix) and it can operate on a sparse input matrix.
    """

    #: Passed to :class:`sklearn.decomposition.PCA` param n_components
    output_dims: int | None = 500
    make_dense: bool = False
    batch_size: int = 500

    def __call__(self, df: pd.DataFrame):
        """Apply PCA to output of :class:`DataFrameEmbedder`."""
        transformed = super().__call__(df)
        pca = IncrementalPCA(
            copy=False, n_components=self.output_dims, batch_size=self.batch_size
        )

        return pca.fit_transform(transformed)


class HierarchicalClusteringClassifier(ModelComponent):
    """Apply agglomerative clustering to distance matrix to classify records."""

    n_clusters: int | None = None
    distance_threshold: float = 1.5

    def __call__(self, distance_matrix: np.ndarray) -> np.ndarray:
        """Use agglomerative clustering algorithm to classify records."""
        classifier = AgglomerativeClustering(
            n_clusters=self.n_clusters,
            metric="precomputed",
            linkage="average",
            distance_threshold=self.distance_threshold,
        )

        return classifier.fit_predict(distance_matrix)


class DistanceCalculator(ModelComponent):
    """Compute distance between records in feature matrix."""

    distance_metric: str | Callable = "euclidean"

    def __call__(self, feature_matrix: np.ndarray) -> np.ndarray:
        """Compute pairwise distance."""
        return pairwise_distances(feature_matrix, metric=self.distance_metric)


class PenalizeReportYearDistanceCalculator(DistanceCalculator):
    """Compute distance between records and add penalty to records from same year."""

    distance_penalty: float = 1000.0

    def __call__(
        self, feature_matrix: np.ndarray, original_df: pd.DataFrame
    ) -> np.ndarray:
        """Create penalty matrix and add to distances."""
        distance_matrix = super().__call__(feature_matrix)

        # First create distance matrix of just report years (same year will be 0)
        report_years = np.array(original_df.report_year)
        penalty_matrix = pairwise_distances(report_years.reshape(-1, 1))
        penalty_matrix = np.isclose(penalty_matrix, 0) * self.distance_penalty

        # Don't add penalty to diagonal (represents distance from record to itself)
        np.fill_diagonal(penalty_matrix, 0)

        return penalty_matrix + distance_matrix


class CrossYearLinker(ModelComponent):
    """Link records within the same dataset between years."""

    embedding_step: DataFrameEmbedder
    distance_calculator: DistanceCalculator = PenalizeReportYearDistanceCalculator()
    classifier: ModelComponent = HierarchicalClusteringClassifier()

    def __call__(self, df: pd.DataFrame):  # noqa: N803
        """Apply model and return column of estimated record labels."""
        feature_matrix = self.embedding_step(df)
        distance_matrix = self.distance_calculator(feature_matrix, df)

        return self.classifier(distance_matrix)
