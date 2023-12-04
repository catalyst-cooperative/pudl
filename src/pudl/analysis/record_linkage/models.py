"""This module defines an interface record linkage models can conform to and implements common functionality."""
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict
from sklearn.base import BaseEstimator
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA, IncrementalPCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    MinMaxScaler,
    Normalizer,
    OneHotEncoder,
)

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


class ModelComponent(BaseModel, ABC):
    """:class:`ModelComponent`s are the basic building blocks of a record linkage model.

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

    Under the hood this uses :class:`sklearn.compose.ColumnTransformer`. As configuration
    it takes as configuration a mapping of column names to a list of transformations to apply.
    Transformations can be specified either by passing an instance of a
    :class:`sklearn.base.BaseEstimator`, or a string to select from several common/generic
    transformers defined by this class. If a string is used, it should be one of the following:

    'string' - Applies a TfidfVectorizer to the column.
    'category' - Applies a OneHotEncoder to the column.
    'number' - Applies a MinMaxScaler to the column.
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
    """Subclass of :class:`DataFrameEmbedder`, which applies PCA to reduce dimensions of the output."""

    #: Passed to :class:`sklearn.decomposition.PCA` param n_components
    output_dims: int | float | None = 500
    make_dense: bool = True

    def __call__(self, df: pd.DataFrame):
        """Apply PCA to output of :class:`DataFrameEmbedder`."""
        transformed = super().__call__(df)
        pca = PCA(copy=False, n_components=self.output_dims, batch_size=500)

        return pca.fit_transform(transformed)


class ReducedDimDataFrameEmbedderSparse(DataFrameEmbedder):
    """Subclass of :class:`DataFrameEmbedder`, which applies IncrementalPCA to reduce dimensions of the output.

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


class ClusteringClassifier(ModelComponent):
    """Apply agglomerative clustering algorithm to distance matrix to classif records."""

    distance_threshold: float = 0.1

    def __call__(self, distance_matrix: np.ndarray) -> np.ndarray:
        """Use agglomerative clustering algorithm to classify records."""
        neighbor_computer = NearestNeighbors(
            radius=self.distance_threshold, metric="precomputed"
        )
        neighbor_computer.fit(distance_matrix)
        neighbor_graph = neighbor_computer.radius_neighbors_graph(mode="distance")

        classifier = DBSCAN(
            metric="precomputed", eps=self.distance_threshold, min_samples=2
        )
        return classifier.fit_predict(neighbor_graph)


def _generate_cluster_ids(max_cluster_id: int) -> int:
    """Get new unique cluster id."""
    while True:
        max_cluster_id += 1
        yield max_cluster_id


class SplitOvermergedClusters(ModelComponent):
    """DBSCAN has a tendency to merge similar clusters together that should be distinct."""

    distance_threshold: float = 1.5

    def __call__(
        self,
        distance_matrix: np.ndarray,
        original_df: pd.DataFrame,
        cluster_id_generator,
    ) -> pd.Series:
        """Apply AgglomerativeClustering to all clusters with more than one record from the same year."""
        classifier = AgglomerativeClustering(
            metric="precomputed",
            linkage="average",
            distance_threshold=self.distance_threshold,
            n_clusters=None,
        )
        duplicated_ids = original_df.loc[
            original_df.duplicated(subset=["report_year", "plant_id_ferc1"]),
            "plant_id_ferc1",
        ]

        for duplicated_id in duplicated_ids.unique():
            # IDs of -1 will be handled seperately
            if duplicated_id == -1:
                continue

            cluster_inds = original_df[
                original_df.plant_id_ferc1 == duplicated_id
            ].index.to_numpy()
            cluster_size = len(cluster_inds)

            dist_inds = np.array(np.meshgrid(cluster_inds, cluster_inds)).T.reshape(
                -1, 2
            )
            cluster_distances = distance_matrix[
                dist_inds[:, 0], dist_inds[:, 1]
            ].reshape((cluster_size, cluster_size))

            new_labels = classifier.fit_predict(cluster_distances)
            for new_label in np.unique(new_labels):
                df_inds = cluster_inds[new_labels == new_label]
                original_df.loc[df_inds, "plant_id_ferc1"] = next(cluster_id_generator)

        return original_df.plant_id_ferc1


def _average_dist_between_clusters(
    distance_matrix: np.ndarray, set_1: list[int], set_2: list[int]
) -> float:
    dist_inds = np.array(np.meshgrid(set_1, set_2)).T.reshape(-1, 2)
    dists = distance_matrix[dist_inds[:, 0], dist_inds[:, 1]]
    return dists.mean()


class MatchOrpahnedRecords(ModelComponent):
    """DBSCAN assigns 'noisy' records a label of '-1', which will be labeled by this step."""

    distance_threshold: float = 0.5

    def __call__(
        self,
        distance_matrix: np.ndarray,
        original_df: pd.DataFrame,
        cluster_id_generator,
    ) -> pd.Series:
        """Compute average distance from orphaned records to existing clusters, and merge."""
        classifier = AgglomerativeClustering(
            metric="precomputed",
            linkage="average",
            distance_threshold=self.distance_threshold,
            n_clusters=None,
        )

        label_inds = original_df.groupby("plant_id_ferc1").indices
        label_groups = [[ind] for ind in label_inds[-1]]
        label_groups += [inds for key, inds in label_inds.items() if key != -1]

        # Prepare a reduced distance matrix
        dist_matrix_size = len(label_groups)
        reduced_dist_matrix = np.zeros((dist_matrix_size, dist_matrix_size))
        for i, x_cluster_inds in enumerate(label_groups):
            for j, y_cluster_inds in enumerate(label_groups[:i]):
                reduced_dist_matrix[i, j] = _average_dist_between_clusters(
                    distance_matrix, x_cluster_inds, y_cluster_inds
                )
                reduced_dist_matrix[j, i] = reduced_dist_matrix[i, j]

        new_labels = classifier.fit_predict(reduced_dist_matrix)
        for inds, label in zip(label_groups, new_labels):
            original_df.loc[inds, "plant_id_ferc1"] = label

        return original_df["plant_id_ferc1"]


class DistanceCalculator(ModelComponent):
    """Compute distance between records in feature matrix."""

    distance_metric: str | Callable = "euclidean"

    def __call__(self, feature_matrix: np.ndarray) -> np.ndarray:
        """Compute pairwise distance."""
        return pairwise_distances(feature_matrix, metric=self.distance_metric)


class PenalizeReportYearDistanceCalculator(DistanceCalculator):
    """Compute distance between records and add penalty to records from same year."""

    distance_penalty: float = 10000.0

    def __call__(
        self, feature_matrix: np.ndarray, original_df: pd.DataFrame
    ) -> np.ndarray:
        """Create penalty matrix and add to distances."""
        distance_matrix = super().__call__(feature_matrix)

        # First create distance matrix of just report years (same year will be 0)
        year_inds = original_df.groupby("report_year").indices
        for inds in year_inds.values():
            matching_year_inds = np.array(np.meshgrid(inds, inds)).T.reshape(-1, 2)
            distance_matrix[
                matching_year_inds[:, 0], matching_year_inds[:, 1]
            ] = self.distance_penalty

        # Don't add penalty to diagonal (represents distance from record to itself)
        np.fill_diagonal(distance_matrix, 0)

        return distance_matrix


class CrossYearLinker(ModelComponent):
    """Link records within the same dataset between years."""

    embedding_step: DataFrameEmbedder
    distance_calculator: DistanceCalculator = PenalizeReportYearDistanceCalculator()
    classifier: ModelComponent = ClusteringClassifier(
        distance_metric=PenalizeReportYearDistanceCalculator()
    )
    split_overmerged_clusters: SplitOvermergedClusters = SplitOvermergedClusters()
    match_orphaned_records: MatchOrpahnedRecords = MatchOrpahnedRecords()

    def __call__(self, df: pd.DataFrame):  # noqa: N803
        """Apply model and return column of estimated record labels."""
        df = df.copy().reset_index()
        feature_matrix = self.embedding_step(df)
        distance_matrix = self.distance_calculator(feature_matrix, df)
        logger.info("Starting classification.")

        # Add column of report years to right of all features to use in distance calcs
        df["plant_id_ferc1"] = self.classifier(distance_matrix)
        id_generator = _generate_cluster_ids(df.plant_id_ferc1.max())
        df["plant_id_ferc1"] = self.split_overmerged_clusters(
            distance_matrix, df, id_generator
        )
        df["plant_id_ferc1"] = self.match_orphaned_records(
            distance_matrix, df, id_generator
        )

        return df["plant_id_ferc1"]
